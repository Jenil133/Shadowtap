package tap

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/tap/taptest"
)

const testTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func sampledDecision() sampling.Decision {
	return sampling.Decision{Sampled: true, Reason: sampling.ReasonSampled, Rule: "checkout", Rate: 0.25, KeySource: sampling.KeySourceTraceparent}
}

func unsampledDecision() sampling.Decision {
	return sampling.Decision{Reason: sampling.ReasonUnsampled, Rule: sampling.RuleDefault, Rate: 0.01, KeySource: sampling.KeySourceRandom}
}

// fakeSampler returns a configurable decision and records what it was asked.
type fakeSampler struct {
	decide func(sampling.Request) sampling.Decision // nil samples everything

	mu   sync.Mutex
	seen []sampledRequest
}

type sampledRequest struct {
	Service, Method, Path, Traceparent string
}

func (f *fakeSampler) Decide(r sampling.Request) sampling.Decision {
	f.mu.Lock()
	f.seen = append(f.seen, sampledRequest{r.Service, r.Method, r.Path, r.Header("traceparent")})
	f.mu.Unlock()
	if f.decide != nil {
		return f.decide(r)
	}
	return sampledDecision()
}

func (f *fakeSampler) requests() []sampledRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]sampledRequest(nil), f.seen...)
}

// fakeScrubber records each exchange as it arrives and marks it scrubbed, so
// the sink can check scrubbing happened before Publish.
type fakeScrubber struct {
	panicOn    string         // request path that makes Exchange panic
	redactions map[string]int // reported for every exchange
	omit       string         // omitted reason set on every response body

	mu     sync.Mutex
	inputs map[string]capture.Exchange // exchange id -> copy as received
	done   map[string]bool
}

func newFakeScrubber() *fakeScrubber {
	return &fakeScrubber{inputs: map[string]capture.Exchange{}, done: map[string]bool{}}
}

func (f *fakeScrubber) Exchange(x *capture.Exchange) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inputs[x.ID] = *x
	if f.panicOn != "" && x.Request.Path == f.panicOn {
		panic("fake scrubber: " + x.Request.Path)
	}
	x.Redactions = maps.Clone(f.redactions)
	if f.omit != "" {
		x.Response.Body.Omit(f.omit)
	}
	f.done[x.ID] = true
}

func (f *fakeScrubber) input(id string) (capture.Exchange, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	x, ok := f.inputs[id]
	return x, ok
}

// inputFor returns the exchange the scrubber received for a request path.
func (f *fakeScrubber) inputFor(t *testing.T, path string) capture.Exchange {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, x := range f.inputs {
		if x.Request.Path == path {
			return x
		}
	}
	t.Fatalf("scrubber never saw %s", path)
	return capture.Exchange{}
}

func (f *fakeScrubber) scrubbed(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.done[id]
}

// fakeSink collects published exchanges. It can fail, panic or block.
type fakeSink struct {
	t        *testing.T
	scrubber *fakeScrubber
	err      error         // returned by Publish when set
	panicOn  string        // request path that makes Publish panic
	release  chan struct{} // when set, Publish waits for it to be closed
	entered  chan string   // when set, receives each request path as Publish starts

	mu  sync.Mutex
	got []*capture.Exchange
}

func (f *fakeSink) Publish(_ context.Context, x *capture.Exchange) error {
	if f.entered != nil {
		f.entered <- x.Request.Path
	}
	if f.release != nil {
		<-f.release
	}
	if !f.scrubber.scrubbed(x.ID) {
		f.t.Errorf("exchange %s (%s) published before it was scrubbed", x.ID, x.Request.Path)
	}
	if f.panicOn != "" && x.Request.Path == f.panicOn {
		panic("fake sink: " + x.Request.Path)
	}
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, x)
	return nil
}

func (f *fakeSink) published() []*capture.Exchange {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*capture.Exchange(nil), f.got...)
}

// stepClock advances by step on every reading.
type stepClock struct {
	mu   sync.Mutex
	t    time.Time
	step time.Duration
}

func (c *stepClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(c.step)
	return c.t
}

// harness is a Server wired to fakes, a private registry and a bufconn client.
type harness struct {
	t        *testing.T
	srv      *Server
	reg      *prometheus.Registry
	sampler  *fakeSampler
	scrubber *fakeScrubber
	sink     *fakeSink
	client   extprocv3.ExternalProcessorClient
}

func testConfig() Config {
	return Config{
		Name:               "tap-test",
		ServiceMetadataKey: DefaultServiceMetadataKey,
		MaxBodyBytes:       1 << 10,
		QueueSize:          256,
		Workers:            2,
	}
}

// newHarness starts a Server with fakes. setup functions run after New and
// before the gRPC server starts, so they can configure the fakes and the
// server (for example its clock) before any stream exists.
func newHarness(t *testing.T, cfg Config, setup ...func(*harness)) *harness {
	t.Helper()
	h := &harness{t: t, reg: prometheus.NewRegistry(), sampler: &fakeSampler{}, scrubber: newFakeScrubber()}
	h.sink = &fakeSink{t: t, scrubber: h.scrubber}
	srv, err := New(cfg, h.sampler, h.scrubber, h.sink, h.reg, testLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	h.srv = srv
	for _, f := range setup {
		f(h)
	}
	t.Cleanup(func() {
		if h.sink.release != nil {
			select {
			case <-h.sink.release:
			default:
				close(h.sink.release)
			}
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	// Registered after the Shutdown cleanup so it runs first: the gRPC server
	// stops (waiting for Process to return) before the queue is drained.
	h.client = taptest.Dial(t, srv)
	return h
}

// run sends ex through the tap and fails the test on a simulator error. When
// it returns (without CancelAfter) Process has finished with the stream.
func (h *harness) run(ex taptest.Exchange, opt taptest.Options) taptest.Result {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(h.t.Context(), 10*time.Second)
	defer cancel()
	res, err := taptest.Run(ctx, h.client, ex, opt)
	if err != nil {
		h.t.Fatalf("taptest.Run: %v", err)
	}
	return res
}

// drain shuts the server down, waiting for queued exchanges, and returns
// everything published.
func (h *harness) drain() []*capture.Exchange {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.srv.Shutdown(ctx); err != nil {
		h.t.Fatalf("Shutdown: %v", err)
	}
	return h.sink.published()
}

func (h *harness) exchanges(service, outcome string) float64 {
	return testutil.ToFloat64(h.srv.metrics.exchanges.WithLabelValues(service, outcome))
}

func (h *harness) decisions(service, decision string) float64 {
	return testutil.ToFloat64(h.srv.metrics.decisions.WithLabelValues(service, decision))
}

// waitIdle waits until no stream is being processed.
func (h *harness) waitIdle() {
	h.t.Helper()
	eventually(h.t, "streams to finish", func() bool {
		return testutil.ToFloat64(h.srv.metrics.streamsActive) == 0
	})
}

// eventually polls cond until it holds or a deadline passes.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// histogramCount returns the sample count of the histogram series of name
// whose label has value (any series when label is empty).
func histogramCount(t *testing.T, g prometheus.Gatherer, name, label, value string) uint64 {
	t.Helper()
	mfs, err := g.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if label == "" {
				return m.GetHistogram().GetSampleCount()
			}
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

// fakeStream drives Process directly with a fixed message sequence.
type fakeStream struct {
	grpc.ServerStream // nil: Process only uses Context, Recv and Send

	t      *testing.T
	ctx    context.Context
	msgs   []*extprocv3.ProcessingRequest
	endErr error // returned once msgs run out; io.EOF when nil
	noSend bool  // observability mode: any Send fails the test

	recvs int
	sent  []*extprocv3.ProcessingResponse
}

func (f *fakeStream) Context() context.Context {
	if f.ctx != nil {
		return f.ctx
	}
	return context.Background()
}

func (f *fakeStream) Recv() (*extprocv3.ProcessingRequest, error) {
	f.recvs++
	if f.recvs <= len(f.msgs) {
		return f.msgs[f.recvs-1], nil
	}
	if f.endErr != nil {
		return nil, f.endErr
	}
	return nil, io.EOF
}

func (f *fakeStream) Send(r *extprocv3.ProcessingResponse) error {
	if f.noSend {
		f.t.Errorf("Send called in observability mode: %v", r)
	}
	f.sent = append(f.sent, r)
	return nil
}

func messages(t *testing.T, ex taptest.Exchange, opt taptest.Options) []*extprocv3.ProcessingRequest {
	t.Helper()
	msgs, err := taptest.Messages(ex, opt)
	if err != nil {
		t.Fatal(err)
	}
	return msgs
}

// replyMatches reports whether resp is the reply type Envoy expects for req.
func replyMatches(req *extprocv3.ProcessingRequest, resp *extprocv3.ProcessingResponse) bool {
	switch req.GetRequest().(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return resp.GetRequestHeaders() != nil
	case *extprocv3.ProcessingRequest_RequestBody:
		return resp.GetRequestBody() != nil
	case *extprocv3.ProcessingRequest_RequestTrailers:
		return resp.GetRequestTrailers() != nil
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return resp.GetResponseHeaders() != nil
	case *extprocv3.ProcessingRequest_ResponseBody:
		return resp.GetResponseBody() != nil
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		return resp.GetResponseTrailers() != nil
	}
	return false
}

// testLogger sends server logs to the test log.
func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSuffix(string(p), "\n"))
	return len(p), nil
}

var errBackpressure = errors.New("fake sink: buffer full")
