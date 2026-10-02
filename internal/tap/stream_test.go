package tap

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/tap/taptest"
)

func allModes() []string { return []string{capture.ModeObserve, capture.ModeInline} }

func postJSON(path string) taptest.Exchange {
	return taptest.Exchange{
		Method:    "POST",
		Scheme:    "http",
		Authority: "demo-api.shop.svc.cluster.local:8080",
		Path:      path,
		RequestHeaders: [][2]string{
			{"content-type", "application/json"},
			{"traceparent", testTraceparent},
			{"x-request-id", "req-42"},
		},
		RequestBody:     []byte(`{"email":"jane@example.com","name":"Jane"}`),
		Status:          201,
		ResponseHeaders: [][2]string{{"content-type", "application/json"}, {"set-cookie", "a=1"}, {"set-cookie", "b=2"}},
		ResponseBody:    []byte(`{"id":"u_1"}`),
	}
}

func checkBody(t *testing.T, what string, b capture.Body, contentType string, data []byte, size int64, truncated bool) {
	t.Helper()
	got, err := b.Bytes()
	if err != nil {
		t.Fatalf("%s body: %v", what, err)
	}
	if b.ContentType != contentType || b.Size != size || b.Truncated != truncated || !bytes.Equal(got, data) {
		t.Errorf("%s body = {type %q, size %d, truncated %v, %d bytes kept}, want {%q, %d, %v, %d bytes}",
			what, b.ContentType, b.Size, b.Truncated, len(got), contentType, size, truncated, len(data))
	}
}

func onlyExchange(t *testing.T, got []*capture.Exchange) *capture.Exchange {
	t.Helper()
	if len(got) != 1 {
		t.Fatalf("published %d exchanges, want 1", len(got))
	}
	return got[0]
}

func TestPostJSON(t *testing.T) {
	for _, mode := range allModes() {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, testConfig())
			ex := postJSON("/api/users?ref=home")
			res := h.run(ex, taptest.Options{Mode: mode})

			if res.Sent != 4 || res.ModeOverride != nil {
				t.Errorf("result = %+v", res)
			}
			if mode == capture.ModeInline {
				if len(res.Replies) != res.Sent {
					t.Errorf("%d replies for %d messages", len(res.Replies), res.Sent)
				}
				for _, r := range res.Replies {
					if !taptest.IsNoop(r) || r.GetModeOverride() != nil {
						t.Errorf("reply %v changes traffic", r)
					}
				}
			} else if len(res.Replies) != 0 {
				t.Errorf("observe mode got %d replies", len(res.Replies))
			}

			x := onlyExchange(t, h.drain())
			if x.SchemaVersion != capture.SchemaVersion || len(x.ID) != 36 || x.Service != "demo-api" {
				t.Errorf("schema %d id %q service %q", x.SchemaVersion, x.ID, x.Service)
			}
			if x.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || x.ParentSpanID != "00f067aa0ba902b7" || x.RequestID != "req-42" {
				t.Errorf("trace %q parent %q request id %q", x.TraceID, x.ParentSpanID, x.RequestID)
			}
			if want := (capture.Source{Tap: "tap-test", Mode: mode}); x.Source != want {
				t.Errorf("source = %+v, want %+v", x.Source, want)
			}
			if want := (capture.Sampling{Rate: 0.25, Rule: "checkout", KeySource: sampling.KeySourceTraceparent}); x.Sampling != want {
				t.Errorf("sampling = %+v, want %+v", x.Sampling, want)
			}
			req := x.Request
			if req.Method != "POST" || req.Scheme != "http" || req.Authority != ex.Authority || req.Path != "/api/users?ref=home" {
				t.Errorf("request line = %s %s %s %s", req.Method, req.Scheme, req.Authority, req.Path)
			}
			wantReqHeaders := capture.Headers{
				"content-type": {"application/json"}, "traceparent": {testTraceparent}, "x-request-id": {"req-42"},
			}
			if !reflect.DeepEqual(req.Headers, wantReqHeaders) {
				t.Errorf("request headers = %v, want %v", req.Headers, wantReqHeaders)
			}
			checkBody(t, "request", req.Body, "application/json", ex.RequestBody, int64(len(ex.RequestBody)), false)
			resp := x.Response
			wantRespHeaders := capture.Headers{"content-type": {"application/json"}, "set-cookie": {"a=1", "b=2"}}
			if resp.Status != 201 || !reflect.DeepEqual(resp.Headers, wantRespHeaders) || len(resp.Trailers) != 0 {
				t.Errorf("response = %d %v trailers %v", resp.Status, resp.Headers, resp.Trailers)
			}
			checkBody(t, "response", resp.Body, "application/json", ex.ResponseBody, int64(len(ex.ResponseBody)), false)
			tm := x.Timing
			if tm.StartedAt.IsZero() || tm.ResponseStartedAt.Before(tm.StartedAt) || tm.CompletedAt.Before(tm.ResponseStartedAt) ||
				tm.DurationMicros != tm.ResponseStartedAt.Sub(tm.StartedAt).Microseconds() {
				t.Errorf("timing = %+v", tm)
			}
			if err := x.Validate(); err != nil {
				t.Error(err)
			}

			wantSeen := []sampledRequest{{"demo-api", "POST", "/api/users?ref=home", testTraceparent}}
			if got := h.sampler.requests(); !reflect.DeepEqual(got, wantSeen) {
				t.Errorf("sampler saw %+v, want %+v", got, wantSeen)
			}
			if got := testutil.ToFloat64(h.srv.metrics.streams.WithLabelValues(mode)); got != 1 {
				t.Errorf("streams_total{mode=%s} = %v", mode, got)
			}
			if got := h.decisions("demo-api", decisionSampled); got != 1 {
				t.Errorf("sampled decisions = %v", got)
			}
			if got := h.exchanges("demo-api", outcomePublished); got != 1 {
				t.Errorf("published = %v", got)
			}
			if got := histogramCount(t, h.reg, "shadowtap_tap_process_seconds", "", ""); got != 1 {
				t.Errorf("process_seconds count = %d", got)
			}
			wantInline := uint64(0)
			if mode == capture.ModeInline {
				wantInline = 1
			}
			for _, k := range []string{"request_headers", "request_body", "response_headers", "response_body"} {
				if got := histogramCount(t, h.reg, "shadowtap_tap_message_seconds", "type", k); got != wantInline {
					t.Errorf("message_seconds{type=%s} count = %d, want %d", k, got, wantInline)
				}
			}
		})
	}
}

func TestGetWithoutBody(t *testing.T) {
	ex := taptest.Exchange{
		Method: "GET", Scheme: "https", Authority: "demo-api:8443", Path: "/api/users/7",
		RequestHeaders:  [][2]string{{"accept", "application/json"}},
		Status:          204,
		ResponseHeaders: [][2]string{{"x-served-by", "a"}},
	}
	for _, mode := range allModes() {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, testConfig())
			res := h.run(ex, taptest.Options{Mode: mode})
			if res.Sent != 2 {
				t.Errorf("sent %d messages, want 2", res.Sent)
			}
			x := onlyExchange(t, h.drain())
			if x.Request.Method != "GET" || x.Request.Scheme != "https" || x.Response.Status != 204 || x.Service != "demo-api" {
				t.Errorf("exchange = %+v", x)
			}
			if x.TraceID != "" || x.RequestID != "" {
				t.Errorf("trace %q request id %q, want none", x.TraceID, x.RequestID)
			}
			checkBody(t, "request", x.Request.Body, "", nil, 0, false)
			checkBody(t, "response", x.Response.Body, "", nil, 0, false)
			if x.Request.Body.Encoding != "" || x.Response.Body.Encoding != "" {
				t.Error("empty bodies have an encoding")
			}
		})
	}
}

func TestBodyLargerThanMaxBodyBytes(t *testing.T) {
	ex := postJSON("/big")
	ex.RequestBody = []byte(strings.Repeat(`{"k":"v"},`, 500)) // 5000 bytes
	ex.ResponseBody = bytes.Repeat([]byte("r"), 3000)
	for _, mode := range allModes() {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, testConfig()) // MaxBodyBytes 1024
			h.run(ex, taptest.Options{Mode: mode, ChunkSize: 700})
			x := onlyExchange(t, h.drain())
			checkBody(t, "request", x.Request.Body, "application/json", ex.RequestBody[:1024], 5000, true)
			checkBody(t, "response", x.Response.Body, "application/json", ex.ResponseBody[:1024], 3000, true)

			// The scrubber got the raw prefix with the metadata already set.
			in, ok := h.scrubber.input(x.ID)
			if !ok {
				t.Fatal("scrubber never saw the exchange")
			}
			checkBody(t, "scrubber input", in.Request.Body, "application/json", ex.RequestBody[:1024], 5000, true)
		})
	}
}

func TestInlineBufferedPartialOverLimit(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBodyBytes = 64 << 10
	h := newHarness(t, cfg)
	ex := postJSON("/partial")
	ex.RequestBody = bytes.Repeat([]byte("a"), 3000)
	ex.ResponseBody = bytes.Repeat([]byte("b"), 2500)
	h.run(ex, taptest.Options{Mode: capture.ModeInline, BufferLimit: 1000})
	x := onlyExchange(t, h.drain())
	// Envoy sent the first 1000 bytes without end_of_stream: the tap kept all
	// it was sent, yet only a prefix of the body.
	checkBody(t, "request", x.Request.Body, "application/json", ex.RequestBody[:1000], 1000, true)
	checkBody(t, "response", x.Response.Body, "application/json", ex.ResponseBody[:1000], 1000, true)
}

func TestObserveChunkedReassembly(t *testing.T) {
	cfg := testConfig()
	cfg.MaxBodyBytes = 1 << 20
	h := newHarness(t, cfg)
	rng := rand.New(rand.NewPCG(1, 2))
	ex := postJSON("/upload")
	ex.RequestHeaders = [][2]string{{"content-type", "application/octet-stream"}, {"content-length", "102400"}}
	ex.RequestBody = make([]byte, 100<<10)
	for i := range ex.RequestBody {
		ex.RequestBody[i] = byte(rng.Uint32())
	}
	ex.ResponseBody = []byte(strings.Repeat("line of text\n", 4000)) // 52000 bytes
	res := h.run(ex, taptest.Options{ChunkSize: 4 << 10})
	if want := 1 + 25 + 1 + 13; res.Sent != want {
		t.Errorf("sent %d messages, want %d", res.Sent, want)
	}
	x := onlyExchange(t, h.drain())
	checkBody(t, "request", x.Request.Body, "application/octet-stream", ex.RequestBody, int64(len(ex.RequestBody)), false)
	checkBody(t, "response", x.Response.Body, "application/json", ex.ResponseBody, int64(len(ex.ResponseBody)), false)
	if x.Request.Body.Encoding != capture.EncodingBase64 || x.Response.Body.Encoding != capture.EncodingUTF8 {
		t.Errorf("encodings = %q, %q", x.Request.Body.Encoding, x.Response.Body.Encoding)
	}
}

func TestTrailers(t *testing.T) {
	ex := postJSON("/rpc")
	ex.RequestTrailers = [][2]string{{"x-checksum", "abc"}}
	ex.ResponseTrailers = [][2]string{{"grpc-status", "0"}, {"grpc-message", "ok"}}
	// Inline BUFFERED_PARTIAL bodies need a matching content-length before
	// trailers count as their end (see TestInlinePartialBodyBeforeTrailers).
	ex.RequestHeaders = append(ex.RequestHeaders, [2]string{"content-length", strconv.Itoa(len(ex.RequestBody))})
	ex.ResponseHeaders = append(ex.ResponseHeaders, [2]string{"content-length", strconv.Itoa(len(ex.ResponseBody))})
	for _, mode := range allModes() {
		t.Run(mode, func(t *testing.T) {
			h := newHarness(t, testConfig())
			h.run(ex, taptest.Options{Mode: mode, SendTrailers: true, ChunkSize: 8})
			x := onlyExchange(t, h.drain())
			if want := (capture.Headers{"x-checksum": {"abc"}}); !reflect.DeepEqual(x.Request.Trailers, want) {
				t.Errorf("request trailers = %v, want %v", x.Request.Trailers, want)
			}
			if want := (capture.Headers{"grpc-status": {"0"}, "grpc-message": {"ok"}}); !reflect.DeepEqual(x.Response.Trailers, want) {
				t.Errorf("response trailers = %v, want %v", x.Response.Trailers, want)
			}
			// Trailers follow the whole body, so neither body is truncated
			// although no body message carried end_of_stream.
			checkBody(t, "request", x.Request.Body, "application/json", ex.RequestBody, int64(len(ex.RequestBody)), false)
			checkBody(t, "response", x.Response.Body, "application/json", ex.ResponseBody, int64(len(ex.ResponseBody)), false)
		})
	}
	t.Run("not sent", func(t *testing.T) {
		// With trailers in the HTTP message but trailer mode SKIP, Envoy never
		// marks a body complete, so the tap cannot tell it was.
		h := newHarness(t, testConfig())
		h.run(ex, taptest.Options{})
		x := onlyExchange(t, h.drain())
		if len(x.Request.Trailers) != 0 || len(x.Response.Trailers) != 0 {
			t.Errorf("trailers recorded: %v %v", x.Request.Trailers, x.Response.Trailers)
		}
		if !x.Request.Body.Truncated || !x.Response.Body.Truncated {
			t.Errorf("bodies without end_of_stream not marked truncated: %+v %+v", x.Request.Body, x.Response.Body)
		}
	})
}

// In inline BUFFERED_PARTIAL mode a body Envoy cut at its buffer limit is
// followed by the trailers exactly like a complete body, so trailers only end
// it as complete when content-length confirms the size. Otherwise the body is
// marked truncated, which makes the scrubber cut back a PII value split at
// the cut.
func TestInlinePartialBodyBeforeTrailers(t *testing.T) {
	text := strings.Repeat("filler line\n", 82) + "jane.doe@example.com card 4111 1111 1111 1111\n" + strings.Repeat("more\n", 600)
	base := taptest.Exchange{
		Method: "POST", Scheme: "http", Authority: "demo-api:8080", Path: "/notes",
		RequestHeaders:   [][2]string{{"content-type", "text/plain"}},
		RequestBody:      []byte(text),
		RequestTrailers:  [][2]string{{"x-checksum", "abc"}},
		Status:           200,
		ResponseHeaders:  [][2]string{{"content-type", "text/plain"}},
		ResponseBody:     []byte(text),
		ResponseTrailers: [][2]string{{"x-checksum", "def"}},
	}
	cfg := testConfig()
	cfg.MaxBodyBytes = 64 << 10

	t.Run("cut at the buffer limit", func(t *testing.T) {
		h := newHarness(t, cfg)
		h.run(base, taptest.Options{Mode: capture.ModeInline, BufferLimit: 1000, SendTrailers: true})
		x := onlyExchange(t, h.drain())
		checkBody(t, "request", x.Request.Body, "text/plain", base.RequestBody[:1000], 1000, true)
		checkBody(t, "response", x.Response.Body, "text/plain", base.ResponseBody[:1000], 1000, true)
	})
	t.Run("complete without content-length", func(t *testing.T) {
		// Indistinguishable from a cut body, so it fails closed.
		h := newHarness(t, cfg)
		h.run(base, taptest.Options{Mode: capture.ModeInline, SendTrailers: true})
		x := onlyExchange(t, h.drain())
		if !x.Request.Body.Truncated || !x.Response.Body.Truncated {
			t.Errorf("unconfirmed inline bodies not marked truncated: %v %v", x.Request.Body.Truncated, x.Response.Body.Truncated)
		}
	})
	t.Run("complete with content-length", func(t *testing.T) {
		ex := base
		n := strconv.Itoa(len(text))
		ex.RequestHeaders = [][2]string{{"content-type", "text/plain"}, {"content-length", n}}
		ex.ResponseHeaders = [][2]string{{"content-type", "text/plain"}, {"content-length", n}}
		h := newHarness(t, cfg)
		h.run(ex, taptest.Options{Mode: capture.ModeInline, SendTrailers: true})
		x := onlyExchange(t, h.drain())
		checkBody(t, "request", x.Request.Body, "text/plain", ex.RequestBody, int64(len(text)), false)
		checkBody(t, "response", x.Response.Body, "text/plain", ex.ResponseBody, int64(len(text)), false)
	})
	t.Run("observe streams the whole body", func(t *testing.T) {
		h := newHarness(t, cfg)
		h.run(base, taptest.Options{Mode: capture.ModeObserve, SendTrailers: true, ChunkSize: 700})
		x := onlyExchange(t, h.drain())
		checkBody(t, "request", x.Request.Body, "text/plain", base.RequestBody, int64(len(text)), false)
	})
}

// Shutting down an idle tap with an expired context loses nothing, so it is
// not an error.
func TestShutdownIdleWithExpiredContext(t *testing.T) {
	for range 50 {
		srv, err := New(testConfig(), &fakeSampler{}, newFakeScrubber(), &fakeSink{t: t}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown of an idle tap = %v, want nil", err)
		}
	}
}

func TestUnsampledInline(t *testing.T) {
	h := newHarness(t, testConfig(), func(h *harness) {
		h.sampler.decide = func(sampling.Request) sampling.Decision { return unsampledDecision() }
	})
	res := h.run(postJSON("/api/users"), taptest.Options{Mode: capture.ModeInline})
	if !proto.Equal(res.ModeOverride, skipRest()) {
		t.Errorf("mode override = %v, want %v", res.ModeOverride, skipRest())
	}
	// The simulator honoured the override: nothing after the request headers.
	if res.Sent != 1 || len(res.Replies) != 1 || !taptest.IsNoop(res.Replies[0]) {
		t.Errorf("result = %+v", res)
	}
	if got := h.drain(); len(got) != 0 {
		t.Errorf("published %d exchanges", len(got))
	}
	if got := h.decisions("demo-api", decisionUnsampled); got != 1 {
		t.Errorf("unsampled decisions = %v", got)
	}
	if got := testutil.CollectAndCount(h.srv.metrics.exchanges); got != 0 {
		t.Errorf("%d exchanges_total series, want 0", got)
	}
	if got := histogramCount(t, h.reg, "shadowtap_tap_message_seconds", "type", "request_headers"); got != 1 {
		t.Errorf("message_seconds{type=request_headers} count = %d", got)
	}
}

func TestUnsampledInlineOverrideIgnored(t *testing.T) {
	// Without allow_mode_override Envoy keeps sending everything; every
	// message must still get its matching reply, and nothing is captured.
	h := newHarness(t, testConfig(), func(h *harness) {
		h.sampler.decide = func(sampling.Request) sampling.Decision { return unsampledDecision() }
	})
	ex := postJSON("/ignored")
	ex.RequestTrailers = [][2]string{{"x-t", "1"}}
	ex.ResponseTrailers = [][2]string{{"x-t", "2"}}
	msgs := messages(t, ex, taptest.Options{Mode: capture.ModeInline, SendTrailers: true})
	st := &fakeStream{t: t, msgs: msgs}
	if err := h.srv.Process(st); err != nil {
		t.Fatal(err)
	}
	if len(st.sent) != len(msgs) {
		t.Fatalf("%d replies for %d messages", len(st.sent), len(msgs))
	}
	for i, r := range st.sent {
		if !replyMatches(msgs[i], r) || !taptest.IsNoop(r) {
			t.Errorf("reply %d = %v", i, r)
		}
		if (r.GetModeOverride() != nil) != (i == 0) {
			t.Errorf("reply %d mode override = %v", i, r.GetModeOverride())
		}
	}
	if got := h.drain(); len(got) != 0 {
		t.Errorf("published %d exchanges", len(got))
	}
}

func TestUnsampledObserve(t *testing.T) {
	unsampled := func(h *harness) {
		h.sampler.decide = func(sampling.Request) sampling.Decision { return unsampledDecision() }
	}
	t.Run("grpc", func(t *testing.T) {
		h := newHarness(t, testConfig(), unsampled)
		res := h.run(postJSON("/api/users"), taptest.Options{ChunkSize: 4})
		if len(res.Replies) != 0 || res.ModeOverride != nil {
			t.Errorf("result = %+v", res)
		}
		if got := h.drain(); len(got) != 0 {
			t.Errorf("published %d exchanges", len(got))
		}
		if got := h.decisions("demo-api", decisionUnsampled); got != 1 {
			t.Errorf("unsampled decisions = %v", got)
		}
	})
	t.Run("closes after request headers", func(t *testing.T) {
		h := newHarness(t, testConfig(), unsampled)
		st := &fakeStream{t: t, msgs: messages(t, postJSON("/x"), taptest.Options{}), noSend: true}
		if err := h.srv.Process(st); err != nil {
			t.Fatal(err)
		}
		if st.recvs != 1 {
			t.Errorf("Process read %d messages, want 1 (the stream should close)", st.recvs)
		}
	})
}

func TestDecisionsCounted(t *testing.T) {
	tests := []struct {
		decision  sampling.Decision
		label     string
		published int
	}{
		{sampledDecision(), decisionSampled, 1},
		{unsampledDecision(), decisionUnsampled, 0},
		{sampling.Decision{Reason: sampling.ReasonExcluded, Rule: sampling.RuleExcludePath}, decisionExcluded, 0},
		{sampling.Decision{Reason: sampling.ReasonRateLimited, Rule: "default", Rate: 1}, decisionRateLimited, 0},
		{sampling.Decision{Reason: "made-up"}, decisionUnsampled, 0},
	}
	for _, tt := range tests {
		for _, mode := range allModes() {
			t.Run(tt.decision.Reason+"/"+mode, func(t *testing.T) {
				h := newHarness(t, testConfig(), func(h *harness) {
					h.sampler.decide = func(sampling.Request) sampling.Decision { return tt.decision }
				})
				res := h.run(postJSON("/d"), taptest.Options{Mode: mode})
				if got := h.decisions("demo-api", tt.label); got != 1 {
					t.Errorf("decisions{decision=%s} = %v", tt.label, got)
				}
				if got := testutil.CollectAndCount(h.srv.metrics.decisions); got != 1 {
					t.Errorf("%d decision series, want 1", got)
				}
				if mode == capture.ModeInline && (res.ModeOverride != nil) != (tt.published == 0) {
					t.Errorf("mode override = %v", res.ModeOverride)
				}
				if got := len(h.drain()); got != tt.published {
					t.Errorf("published %d, want %d", got, tt.published)
				}
			})
		}
	}
}

func TestHeaderValues(t *testing.T) {
	ex := postJSON("/h")
	ex.RequestHeaders = append(ex.RequestHeaders, [2]string{"x-empty", ""}, [2]string{"x-multi", "1"}, [2]string{"x-multi", "2"})
	want := capture.Headers{
		"content-type": {"application/json"}, "traceparent": {testTraceparent}, "x-request-id": {"req-42"},
		"x-empty": {""}, "x-multi": {"1", "2"},
	}
	for _, legacy := range []bool{false, true} {
		field := "raw_value"
		if legacy {
			field = "value"
		}
		for _, mode := range allModes() {
			t.Run(mode+"/"+field, func(t *testing.T) {
				h := newHarness(t, testConfig())
				h.run(ex, taptest.Options{Mode: mode, LegacyHeaderValues: legacy})
				x := onlyExchange(t, h.drain())
				if !reflect.DeepEqual(x.Request.Headers, want) {
					t.Errorf("headers = %v, want %v", x.Request.Headers, want)
				}
				if x.TraceID == "" || x.Request.Path != "/h" || x.Response.Status != 201 {
					t.Errorf("pseudo-headers or trace context lost: %+v", x)
				}
			})
		}
	}
	t.Run("raw bytes", func(t *testing.T) {
		h := newHarness(t, testConfig())
		ex := postJSON("/bin")
		ex.RequestHeaders = [][2]string{{"x-bin", "\xff\x00\xfe"}}
		h.run(ex, taptest.Options{})
		x := onlyExchange(t, h.drain())
		if got := x.Request.Headers.Get("x-bin"); got != "\xff\x00\xfe" {
			t.Errorf("binary header = %q", got)
		}
	})
}

func TestHeaderNormalization(t *testing.T) {
	// Hand-built headers: mixed-case names, unknown pseudo-headers, duplicates.
	hv := func(k, v string) *corev3.HeaderValue { return &corev3.HeaderValue{Key: k, RawValue: []byte(v)} }
	msgs := []*extprocv3.ProcessingRequest{
		{ObservabilityMode: true, Request: &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{
			EndOfStream: true,
			Headers: &corev3.HeaderMap{Headers: []*corev3.HeaderValue{
				hv(":method", "CONNECT-ish"), hv(":METHOD", "IGNORED"), hv(":path", "/n"), hv(":authority", "svc.example"),
				hv(":scheme", "http"), hv(":protocol", "websocket"), hv("X-Custom", "a"), hv("x-custom", "b"),
				{Key: "x-legacy", Value: "old"}, hv("", "no name"),
			}},
		}}},
		{ObservabilityMode: true, Request: &extprocv3.ProcessingRequest_ResponseHeaders{ResponseHeaders: &extprocv3.HttpHeaders{
			EndOfStream: true,
			Headers:     &corev3.HeaderMap{Headers: []*corev3.HeaderValue{hv(":status", "200"), hv("Content-Type", "text/plain")}},
		}}},
	}
	h := newHarness(t, testConfig())
	if err := h.srv.Process(&fakeStream{t: t, msgs: msgs, noSend: true}); err != nil {
		t.Fatal(err)
	}
	x := onlyExchange(t, h.drain())
	if x.Request.Method != "CONNECT-ish" || x.Request.Path != "/n" || x.Request.Authority != "svc.example" || x.Service != "svc.example" {
		t.Errorf("request = %+v service %q", x.Request, x.Service)
	}
	if want := (capture.Headers{"x-custom": {"a", "b"}, "x-legacy": {"old"}}); !reflect.DeepEqual(x.Request.Headers, want) {
		t.Errorf("request headers = %v, want %v", x.Request.Headers, want)
	}
	if want := (capture.Headers{"content-type": {"text/plain"}}); !reflect.DeepEqual(x.Response.Headers, want) || x.Response.Status != 200 {
		t.Errorf("response = %d %v", x.Response.Status, x.Response.Headers)
	}
}

func TestServiceName(t *testing.T) {
	tests := []struct {
		name      string
		metadata  map[string]string
		authority string
		want      string
	}{
		{"metadata wins", map[string]string{DefaultServiceMetadataKey: "checkout"}, "demo-api.shop.svc:80", "checkout"},
		{"blank metadata ignored", map[string]string{DefaultServiceMetadataKey: "  "}, "demo-api.shop.svc:80", "demo-api"},
		{"other metadata ignored", map[string]string{"x-other": "nope"}, "orders:8080", "orders"},
		{"cluster local", nil, "Demo-API.shop.svc.cluster.local:8080", "demo-api"},
		{"no authority", nil, "", unknownService},
		{"hostile authority", nil, "evil<script>.example", unknownService},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, testConfig())
			ex := postJSON("/s")
			ex.Authority = tt.authority
			h.run(ex, taptest.Options{Metadata: tt.metadata})
			x := onlyExchange(t, h.drain())
			if x.Service != tt.want {
				t.Errorf("service = %q, want %q", x.Service, tt.want)
			}
			if got := h.sampler.requests(); len(got) != 1 || got[0].Service != tt.want {
				t.Errorf("sampler saw %+v", got)
			}
			if got := h.decisions(tt.want, decisionSampled); got != 1 {
				t.Errorf("decisions{service=%s} = %v", tt.want, got)
			}
		})
	}
	t.Run("custom metadata key", func(t *testing.T) {
		cfg := testConfig()
		cfg.ServiceMetadataKey = "X-Mesh-Service"
		h := newHarness(t, cfg)
		h.run(postJSON("/s"), taptest.Options{Metadata: map[string]string{"x-mesh-service": "billing"}})
		if x := onlyExchange(t, h.drain()); x.Service != "billing" {
			t.Errorf("service = %q, want billing", x.Service)
		}
	})
}

func TestServiceLabelCap(t *testing.T) {
	h := newHarness(t, testConfig(), func(h *harness) {
		h.sampler.decide = func(sampling.Request) sampling.Decision { return unsampledDecision() }
	})
	const extra = 5
	for i := range maxServiceLabels + extra {
		ex := postJSON("/")
		ex.Authority = fmt.Sprintf("svc-%d.example", i)
		if err := h.srv.Process(&fakeStream{t: t, msgs: messages(t, ex, taptest.Options{}), noSend: true}); err != nil {
			t.Fatal(err)
		}
	}
	// Unknown services share one fixed label and never use up the cap.
	ex := postJSON("/")
	ex.Authority = ""
	if err := h.srv.Process(&fakeStream{t: t, msgs: messages(t, ex, taptest.Options{}), noSend: true}); err != nil {
		t.Fatal(err)
	}
	if got := testutil.CollectAndCount(h.srv.metrics.decisions); got != maxServiceLabels+2 {
		t.Errorf("%d decision series, want %d", got, maxServiceLabels+2)
	}
	if got := h.decisions(otherService, decisionUnsampled); got != extra {
		t.Errorf("decisions{service=other} = %v, want %d", got, extra)
	}
	if got := h.decisions(unknownService, decisionUnsampled); got != 1 {
		t.Errorf("decisions{service=unknown} = %v, want 1", got)
	}
}

func TestCancelMidRequest(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		h := newHarness(t, testConfig())
		// request headers and request body, then a reset.
		res := h.run(postJSON("/c"), taptest.Options{Mode: capture.ModeInline, CancelAfter: 2})
		if res.Sent != 2 || len(res.Replies) != 2 {
			t.Errorf("result = %+v", res)
		}
		h.waitIdle()
		if got := h.drain(); len(got) != 0 {
			t.Errorf("published %d exchanges", len(got))
		}
		if got := h.exchanges("demo-api", outcomeIncomplete); got != 1 {
			t.Errorf("incomplete = %v", got)
		}
	})
	t.Run("observe", func(t *testing.T) {
		h := newHarness(t, testConfig())
		msgs := messages(t, postJSON("/c"), taptest.Options{ChunkSize: 8})
		st := &fakeStream{t: t, msgs: msgs[:3], endErr: status.Error(codes.Canceled, "client reset"), noSend: true}
		if err := h.srv.Process(st); err != nil {
			t.Fatalf("Process = %v, want nil for a cancelled stream", err)
		}
		if got := h.drain(); len(got) != 0 {
			t.Errorf("published %d exchanges", len(got))
		}
		if got := h.exchanges("demo-api", outcomeIncomplete); got != 1 {
			t.Errorf("incomplete = %v", got)
		}
	})
}

func TestCancelAfterResponseHeaders(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		h := newHarness(t, testConfig())
		ex := postJSON("/c")
		// request headers, request body, response headers, then a reset.
		h.run(ex, taptest.Options{Mode: capture.ModeInline, CancelAfter: 3})
		h.waitIdle()
		x := onlyExchange(t, h.drain())
		checkBody(t, "request", x.Request.Body, "application/json", ex.RequestBody, int64(len(ex.RequestBody)), false)
		checkBody(t, "response", x.Response.Body, "application/json", nil, 0, true)
		if x.Response.Status != 201 || x.Timing.CompletedAt.IsZero() {
			t.Errorf("response %d completed %v", x.Response.Status, x.Timing.CompletedAt)
		}
	})
	t.Run("observe", func(t *testing.T) {
		h := newHarness(t, testConfig())
		ex := postJSON("/c")
		ex.ResponseBody = bytes.Repeat([]byte("z"), 40)
		msgs := messages(t, ex, taptest.Options{ChunkSize: 10}) // headers, 5 request chunks, headers, 4 response chunks
		cut := len(msgs) - 2                                    // two response chunks seen
		st := &fakeStream{t: t, msgs: msgs[:cut], endErr: context.Canceled, noSend: true}
		if err := h.srv.Process(st); err != nil {
			t.Fatalf("Process = %v", err)
		}
		x := onlyExchange(t, h.drain())
		checkBody(t, "response", x.Response.Body, "application/json", ex.ResponseBody[:20], 20, true)
	})
}

func TestObserveNeverSends(t *testing.T) {
	ex := postJSON("/o")
	ex.RequestBody = bytes.Repeat([]byte("q"), 5000)
	ex.RequestTrailers = [][2]string{{"x-t", "1"}}
	ex.ResponseTrailers = [][2]string{{"grpc-status", "0"}}
	full := messages(t, ex, taptest.Options{ChunkSize: 100, SendTrailers: true})
	firstResp := 0
	for i, m := range full {
		if m.GetResponseHeaders() != nil {
			firstResp = i
		}
	}
	empty := &extprocv3.ProcessingRequest{ObservabilityMode: true}
	withEmpty := append([]*extprocv3.ProcessingRequest{full[0], empty}, full[1:]...)

	tests := []struct {
		name      string
		decision  sampling.Decision
		msgs      []*extprocv3.ProcessingRequest
		endErr    error
		published int
	}{
		{"sampled with trailers", sampledDecision(), full, nil, 1},
		{"unsampled", unsampledDecision(), full, nil, 0},
		{"reset mid-request", sampledDecision(), full[:4], status.Error(codes.Canceled, "reset"), 0},
		{"reset after response headers", sampledDecision(), full[:firstResp+1], status.Error(codes.DeadlineExceeded, "timeout"), 1},
		{"no request headers", sampledDecision(), full[1:], nil, 0},
		{"unknown message ignored", sampledDecision(), withEmpty, nil, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, testConfig(), func(h *harness) {
				h.sampler.decide = func(sampling.Request) sampling.Decision { return tt.decision }
			})
			st := &fakeStream{t: t, msgs: tt.msgs, endErr: tt.endErr, noSend: true}
			if err := h.srv.Process(st); err != nil {
				t.Fatalf("Process = %v", err)
			}
			if got := len(h.drain()); got != tt.published {
				t.Errorf("published %d, want %d", got, tt.published)
			}
		})
	}
}

func TestStreamErrors(t *testing.T) {
	msgs := messages(t, postJSON("/e"), taptest.Options{Mode: capture.ModeInline})
	internal := status.Error(codes.Internal, "transport broke")
	tests := []struct {
		name      string
		msgs      []*extprocv3.ProcessingRequest
		endErr    error
		sendErr   error
		wantCode  codes.Code
		published int
		outcome   string
	}{
		{"recv error after response headers", msgs[:3], internal, nil, codes.Internal, 1, outcomePublished},
		{"recv error mid-request", msgs[:2], internal, nil, codes.Internal, 0, outcomeIncomplete},
		{"deadline", msgs[:2], status.Error(codes.DeadlineExceeded, "deadline"), nil, codes.OK, 0, outcomeIncomplete},
		{"send fails", msgs, nil, status.Error(codes.Unavailable, "gone"), codes.Unavailable, 0, outcomeIncomplete},
		{"send cancelled", msgs, nil, status.Error(codes.Canceled, "gone"), codes.OK, 0, outcomeIncomplete},
		{"empty inline message", append(msgs[:1:1], &extprocv3.ProcessingRequest{}), nil, nil, codes.InvalidArgument, 0, outcomeIncomplete},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, testConfig())
			st := &sendFailStream{fakeStream: fakeStream{t: t, msgs: tt.msgs, endErr: tt.endErr}, err: tt.sendErr}
			err := h.srv.Process(st)
			if status.Code(err) != tt.wantCode {
				t.Fatalf("Process = %v, want code %v", err, tt.wantCode)
			}
			if got := len(h.drain()); got != tt.published {
				t.Errorf("published %d, want %d", got, tt.published)
			}
			if got := h.exchanges("demo-api", tt.outcome); got != 1 {
				t.Errorf("exchanges{outcome=%s} = %v", tt.outcome, got)
			}
		})
	}
}

// sendFailStream fails every Send with err (when set).
type sendFailStream struct {
	fakeStream
	err error
}

func (s *sendFailStream) Send(r *extprocv3.ProcessingResponse) error {
	if s.err != nil {
		return s.err
	}
	return s.fakeStream.Send(r)
}

func TestProtocolOddities(t *testing.T) {
	t.Run("duplicate request headers", func(t *testing.T) {
		h := newHarness(t, testConfig())
		first := messages(t, postJSON("/first"), taptest.Options{Mode: capture.ModeInline})
		second := messages(t, postJSON("/second"), taptest.Options{Mode: capture.ModeInline})
		msgs := append([]*extprocv3.ProcessingRequest{first[0], second[0]}, first[1:]...)
		st := &fakeStream{t: t, msgs: msgs}
		if err := h.srv.Process(st); err != nil {
			t.Fatal(err)
		}
		for i, r := range st.sent {
			if !replyMatches(msgs[i], r) {
				t.Errorf("reply %d = %v", i, r)
			}
		}
		if x := onlyExchange(t, h.drain()); x.Request.Path != "/first" {
			t.Errorf("path = %q, want /first", x.Request.Path)
		}
		if got := len(h.sampler.requests()); got != 1 {
			t.Errorf("sampler asked %d times", got)
		}
	})
	t.Run("inline without request headers", func(t *testing.T) {
		h := newHarness(t, testConfig())
		msgs := messages(t, postJSON("/x"), taptest.Options{Mode: capture.ModeInline})[1:]
		st := &fakeStream{t: t, msgs: msgs}
		if err := h.srv.Process(st); err != nil {
			t.Fatal(err)
		}
		if len(st.sent) != len(msgs) {
			t.Errorf("%d replies for %d messages", len(st.sent), len(msgs))
		}
		if got := len(h.drain()); got != 0 {
			t.Errorf("published %d", got)
		}
		if got := testutil.CollectAndCount(h.srv.metrics.decisions); got != 0 {
			t.Errorf("%d decision series", got)
		}
	})
	t.Run("response body without response headers", func(t *testing.T) {
		h := newHarness(t, testConfig())
		msgs := messages(t, postJSON("/x"), taptest.Options{})
		st := &fakeStream{t: t, msgs: []*extprocv3.ProcessingRequest{msgs[0], msgs[1], msgs[3]}, noSend: true}
		if err := h.srv.Process(st); err != nil {
			t.Fatal(err)
		}
		if got := h.exchanges("demo-api", outcomeIncomplete); got != 1 {
			t.Errorf("incomplete = %v", got)
		}
	})
}

func TestTiming(t *testing.T) {
	base := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	clock := &stepClock{t: base, step: 1500 * time.Millisecond}
	h := newHarness(t, testConfig(), func(h *harness) { h.srv.now = clock.now })
	msgs := messages(t, postJSON("/t"), taptest.Options{})
	if err := h.srv.Process(&fakeStream{t: t, msgs: msgs, noSend: true}); err != nil {
		t.Fatal(err)
	}
	x := onlyExchange(t, h.drain())
	want := capture.Timing{
		StartedAt:         base.Add(1500 * time.Millisecond),
		ResponseStartedAt: base.Add(3000 * time.Millisecond),
		CompletedAt:       base.Add(4500 * time.Millisecond),
		DurationMicros:    1_500_000,
	}
	if x.Timing != want {
		t.Errorf("timing = %+v, want %+v", x.Timing, want)
	}
}

func TestMetadataServiceFromContext(t *testing.T) {
	h := newHarness(t, testConfig())
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(DefaultServiceMetadataKey, "payments"))
	msgs := messages(t, postJSON("/m"), taptest.Options{})
	if err := h.srv.Process(&fakeStream{t: t, ctx: ctx, msgs: msgs, noSend: true}); err != nil {
		t.Fatal(err)
	}
	if x := onlyExchange(t, h.drain()); x.Service != "payments" {
		t.Errorf("service = %q", x.Service)
	}
}

func TestDirection(t *testing.T) {
	tests := []struct {
		name      string
		hint      int
		chunks    []string
		eos       bool
		wantData  string
		wantSeen  int64
		truncated bool
	}{
		{"empty complete", 0, nil, true, "", 0, false},
		{"empty cut off", 0, nil, false, "", 0, true},
		{"fits", 0, []string{"ab", "cd"}, true, "abcd", 4, false},
		{"exactly the limit", 0, []string{"abcd", "ef"}, true, "abcdef", 6, false},
		{"over the limit", 0, []string{"abcd", "efgh"}, true, "abcdef", 8, true},
		{"empty chunks", 0, []string{"", "ab", ""}, true, "ab", 2, false},
		{"hint larger than limit", 1 << 20, []string{"abc"}, true, "abc", 3, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := direction{hint: tt.hint}
			for _, c := range tt.chunks {
				d.write([]byte(c), 6)
			}
			d.close(tt.eos)
			d.write([]byte("late"), 6) // ignored once closed
			b := d.body("text/plain")
			if string(d.data) != tt.wantData || b.Size != tt.wantSeen || b.Truncated != tt.truncated || b.ContentType != "text/plain" {
				t.Errorf("data %q body %+v, want %q size %d truncated %v", d.data, b, tt.wantData, tt.wantSeen, tt.truncated)
			}
			if cap(d.data) > 6 {
				t.Errorf("buffer capacity %d exceeds the limit", cap(d.data))
			}
		})
	}
	t.Run("chunk bytes are copied", func(t *testing.T) {
		var d direction
		chunk := []byte("abc")
		d.write(chunk, 10)
		chunk[0] = 'X'
		if string(d.data) != "abc" {
			t.Errorf("data aliases the chunk: %q", d.data)
		}
	})
}
