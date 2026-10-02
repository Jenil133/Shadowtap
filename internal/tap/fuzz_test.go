package tap

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/tap/taptest"
)

func FuzzServiceFromAuthority(f *testing.F) {
	for _, s := range []string{
		"demo-api", "demo-api:8080", "demo-api.shop.svc.cluster.local:8080", "[::1]:80", "a@b:c",
		"x.svc", ".svc", "[", "]:", "UPPER.svc.cluster.local.", "\xff\xfe",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, authority string) {
		got := serviceFromAuthority(authority)
		if got != "" && !validServiceName(got) {
			t.Fatalf("serviceFromAuthority(%q) = %q, not a valid service name", authority, got)
		}
		if name := serviceName("", authority); name != got && !(got == "" && name == unknownService) {
			t.Fatalf("serviceName = %q, serviceFromAuthority = %q", name, got)
		}
	})
}

// The fuzz input for FuzzProcess: a decision byte, an end-of-stream byte,
// then records of [kind, flags, length, payload...].
const (
	fuzzObserve  = 1 << iota // record flag: observability_mode
	fuzzEOS                  // record flag: end_of_stream
	fuzzEOSNoMsg             // record flag: end_of_stream_without_message
)

// fuzzCase is one decoded FuzzProcess input.
type fuzzCase struct {
	decision sampling.Decision
	end      error // returned by Recv after the last message
	msgs     []*extprocv3.ProcessingRequest
}

// decodeFuzz turns fuzz bytes into a decision, the error ending the stream and
// a message sequence. Header payloads are "name=value" lines.
func decodeFuzz(data []byte) fuzzCase {
	if len(data) < 2 {
		return fuzzCase{end: io.EOF}
	}
	decisions := []sampling.Decision{
		sampledDecision(), unsampledDecision(),
		{Reason: sampling.ReasonExcluded}, {Reason: sampling.ReasonRateLimited},
	}
	ends := []error{io.EOF, status.Error(codes.Canceled, "reset"), status.Error(codes.Internal, "broken"), context.DeadlineExceeded}
	fc := fuzzCase{decision: decisions[int(data[0])%len(decisions)], end: ends[int(data[1])%len(ends)]}
	for rest := data[2:]; len(rest) >= 3; {
		kind, flags, n := rest[0]%7, rest[1], int(rest[2])
		rest = rest[3:]
		payload := rest[:min(n, len(rest))]
		rest = rest[len(payload):]
		m := &extprocv3.ProcessingRequest{ObservabilityMode: flags&fuzzObserve != 0}
		eos := flags&fuzzEOS != 0
		headers := &extprocv3.HttpHeaders{Headers: fuzzHeaders(payload), EndOfStream: eos}
		body := &extprocv3.HttpBody{Body: payload, EndOfStream: eos, EndOfStreamWithoutMessage: flags&fuzzEOSNoMsg != 0}
		trailers := &extprocv3.HttpTrailers{Trailers: fuzzHeaders(payload)}
		switch kind {
		case 0:
			m.Request = &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: headers}
		case 1:
			m.Request = &extprocv3.ProcessingRequest_RequestBody{RequestBody: body}
		case 2:
			m.Request = &extprocv3.ProcessingRequest_RequestTrailers{RequestTrailers: trailers}
		case 3:
			m.Request = &extprocv3.ProcessingRequest_ResponseHeaders{ResponseHeaders: headers}
		case 4:
			m.Request = &extprocv3.ProcessingRequest_ResponseBody{ResponseBody: body}
		case 5:
			m.Request = &extprocv3.ProcessingRequest_ResponseTrailers{ResponseTrailers: trailers}
		}
		fc.msgs = append(fc.msgs, m)
	}
	return fc
}

func fuzzHeaders(payload []byte) *corev3.HeaderMap {
	hm := &corev3.HeaderMap{}
	for line := range bytes.SplitSeq(payload, []byte("\n")) {
		k, v, _ := bytes.Cut(line, []byte("="))
		hm.Headers = append(hm.Headers, &corev3.HeaderValue{Key: string(k), RawValue: bytes.Clone(v)})
	}
	return hm
}

// encodeFuzz is the inverse of decodeFuzz for seeds built with taptest.
func encodeFuzz(decision, end byte, msgs []*extprocv3.ProcessingRequest) []byte {
	out := []byte{decision, end}
	for _, m := range msgs {
		var kind, flags byte
		var payload []byte
		if m.GetObservabilityMode() {
			flags |= fuzzObserve
		}
		headerPayload := func(hm *corev3.HeaderMap) []byte {
			var lines [][]byte
			for _, h := range hm.GetHeaders() {
				lines = append(lines, []byte(h.GetKey()+"="+string(h.GetRawValue())))
			}
			return bytes.Join(lines, []byte("\n"))
		}
		switch r := m.GetRequest().(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			kind, payload = 0, headerPayload(r.RequestHeaders.GetHeaders())
			if r.RequestHeaders.GetEndOfStream() {
				flags |= fuzzEOS
			}
		case *extprocv3.ProcessingRequest_RequestBody:
			kind, payload = 1, r.RequestBody.GetBody()
			if r.RequestBody.GetEndOfStream() {
				flags |= fuzzEOS
			}
		case *extprocv3.ProcessingRequest_RequestTrailers:
			kind, payload = 2, headerPayload(r.RequestTrailers.GetTrailers())
		case *extprocv3.ProcessingRequest_ResponseHeaders:
			kind, payload = 3, headerPayload(r.ResponseHeaders.GetHeaders())
			if r.ResponseHeaders.GetEndOfStream() {
				flags |= fuzzEOS
			}
		case *extprocv3.ProcessingRequest_ResponseBody:
			kind, payload = 4, r.ResponseBody.GetBody()
			if r.ResponseBody.GetEndOfStream() {
				flags |= fuzzEOS
			}
		case *extprocv3.ProcessingRequest_ResponseTrailers:
			kind, payload = 5, headerPayload(r.ResponseTrailers.GetTrailers())
		}
		payload = payload[:min(len(payload), 255)]
		out = append(out, kind, flags, byte(len(payload)))
		out = append(out, payload...)
	}
	return out
}

// protocolStream checks the ext_proc reply discipline as Process runs: one
// matching reply per inline message, sent before the next message is read,
// and nothing for observability messages.
type protocolStream struct {
	grpc.ServerStream
	t        *testing.T
	msgs     []*extprocv3.ProcessingRequest
	end      error
	next     int
	answered bool
}

func (p *protocolStream) Context() context.Context { return context.Background() }

func (p *protocolStream) Recv() (*extprocv3.ProcessingRequest, error) {
	if p.next > 0 && !p.msgs[p.next-1].GetObservabilityMode() && !p.answered {
		p.t.Fatalf("message %d (inline) read past without a reply", p.next-1)
	}
	if p.next == len(p.msgs) {
		p.next++
		return nil, p.end
	}
	p.next++
	p.answered = false
	return p.msgs[p.next-1], nil
}

func (p *protocolStream) Send(r *extprocv3.ProcessingResponse) error {
	if p.next == 0 || p.next > len(p.msgs) {
		p.t.Fatal("Send without a pending message")
	}
	req := p.msgs[p.next-1]
	switch {
	case req.GetObservabilityMode():
		p.t.Fatalf("reply %v to observability message %d", r, p.next-1)
	case p.answered:
		p.t.Fatalf("second reply to message %d", p.next-1)
	case !replyMatches(req, r) || !taptest.IsNoop(r):
		p.t.Fatalf("reply %v does not match message %v", r, req)
	}
	p.answered = true
	return nil
}

func FuzzProcess(f *testing.F) {
	ex := postJSON("/fuzz?q=1")
	ex.RequestTrailers = [][2]string{{"x-t", "1"}}
	ex.ResponseTrailers = [][2]string{{"grpc-status", "0"}}
	for i, opt := range []taptest.Options{
		{ChunkSize: 7, SendTrailers: true},
		{Mode: capture.ModeInline, BufferLimit: 9, SendTrailers: true},
		{ChunkSize: 3},
	} {
		msgs, err := taptest.Messages(ex, opt)
		if err != nil {
			f.Fatal(err)
		}
		for d := range byte(4) {
			f.Add(encodeFuzz(d, byte(i), msgs))
			f.Add(encodeFuzz(d, byte(i+1), msgs[:len(msgs)/2]))
		}
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		fc := decodeFuzz(data)
		cfg := testConfig()
		cfg.MaxBodyBytes, cfg.Workers = 16, 1
		sampler := &fakeSampler{decide: func(sampling.Request) sampling.Decision { return fc.decision }}
		scrubber := newFakeScrubber()
		sink := &fakeSink{t: t, scrubber: scrubber}
		s, err := New(cfg, sampler, scrubber, sink, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		perr := s.Process(&protocolStream{t: t, msgs: fc.msgs, end: fc.end})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}

		// Process returns nil, the stream's own error, or InvalidArgument for an
		// inline message it cannot answer; anything else is a recovered panic.
		if perr != nil && perr != fc.end && status.Code(perr) != codes.InvalidArgument {
			t.Fatalf("Process = %v", perr)
		}

		// Every sampled request ends in exactly one outcome.
		switch seen := sampler.requests(); len(seen) {
		case 0:
		case 1:
			label := s.services.label(seen[0].Service)
			var outcomes float64
			for _, o := range []string{outcomePublished, outcomeQueueFull, outcomeIncomplete, outcomeInvalid,
				outcomeSinkError, outcomeShutdown, outcomePanic} {
				outcomes += testutil.ToFloat64(s.metrics.exchanges.WithLabelValues(label, o))
			}
			want := 0.0
			if fc.decision.Sampled {
				want = 1
			}
			if outcomes != want {
				t.Fatalf("%v outcomes for a request sampled=%v", outcomes, fc.decision.Sampled)
			}
		default:
			t.Fatalf("sampler asked %d times for one stream", len(seen))
		}

		for _, x := range sink.published() {
			if err := x.Validate(); err != nil {
				t.Fatalf("published an invalid exchange: %v", err)
			}
			for _, b := range []capture.Body{x.Request.Body, x.Response.Body} {
				data, err := b.Bytes()
				if err != nil {
					t.Fatal(err)
				}
				if len(data) > cfg.MaxBodyBytes || int64(len(data)) > b.Size || (!b.Truncated && int64(len(data)) != b.Size) {
					t.Fatalf("body kept %d bytes, size %d, truncated %v", len(data), b.Size, b.Truncated)
				}
			}
		}
	})
}
