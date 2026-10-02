package tap

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/tap/taptest"
)

func paths(xs []*capture.Exchange) []string {
	var out []string
	for _, x := range xs {
		out = append(out, x.Request.Path)
	}
	slices.Sort(out)
	return out
}

func waitEntered(t *testing.T, ch <-chan string, want string) {
	t.Helper()
	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("sink got %s first, want %s", got, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("sink never received %s", want)
	}
}

func TestQueueFull(t *testing.T) {
	cfg := testConfig()
	cfg.Workers, cfg.QueueSize = 1, 1
	h := newHarness(t, cfg, func(h *harness) {
		h.sink.release = make(chan struct{})
		h.sink.entered = make(chan string, 8)
	})
	inline := taptest.Options{Mode: capture.ModeInline}
	h.run(postJSON("/a"), inline)
	waitEntered(t, h.sink.entered, "/a") // the only worker is now stuck in Publish
	h.run(postJSON("/b"), inline)        // fills the queue
	res := h.run(postJSON("/c"), inline) // dropped
	if res.Sent != 4 || len(res.Replies) != 4 {
		t.Errorf("Process stopped answering: %+v", res)
	}
	if got := h.exchanges("demo-api", outcomeQueueFull); got != 1 {
		t.Errorf("queue_full = %v", got)
	}
	if got := testutil.ToFloat64(h.srv.metrics.queueDepth); got != 1 {
		t.Errorf("queue_depth = %v", got)
	}
	close(h.sink.release)
	if got, want := paths(h.drain()), []string{"/a", "/b"}; !slices.Equal(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
	if got := testutil.ToFloat64(h.srv.metrics.queueDepth); got != 0 {
		t.Errorf("queue_depth after drain = %v", got)
	}
}

// The queue is bounded by body bytes as well as by count, so a burst of large
// bodies cannot exhaust memory while the count limit is far away.
func TestQueueBytesLimit(t *testing.T) {
	cfg := testConfig() // MaxBodyBytes 1 KiB
	big := postJSON("/big")
	big.RequestBody = bytes.Repeat([]byte("a"), cfg.MaxBodyBytes)
	big.ResponseBody = bytes.Repeat([]byte("b"), cfg.MaxBodyBytes)
	perJob := int64(2*cfg.MaxBodyBytes) + jobOverhead
	cfg.Workers, cfg.QueueSize = 1, 100
	cfg.MaxQueueBytes = int(2 * perJob) // room for two big exchanges
	h := newHarness(t, cfg, func(h *harness) {
		h.sink.release = make(chan struct{})
		h.sink.entered = make(chan string, 8)
	})
	h.run(big, taptest.Options{})
	waitEntered(t, h.sink.entered, "/big") // in flight: still counted
	h.run(big, taptest.Options{})          // queued: the budget is used up
	h.run(big, taptest.Options{})          // over the byte budget: dropped
	if got := h.exchanges("demo-api", outcomeQueueFull); got != 1 {
		t.Errorf("queue_full = %v, want 1", got)
	}
	if got := h.srv.queued.Load(); got != 2*perJob {
		t.Errorf("queued bytes = %d, want %d", got, 2*perJob)
	}
	close(h.sink.release)
	if got := len(h.drain()); got != 2 {
		t.Errorf("published %d, want 2", got)
	}
	if got := h.srv.queued.Load(); got != 0 {
		t.Errorf("queued bytes after drain = %d", got)
	}
}

func TestSinkError(t *testing.T) {
	h := newHarness(t, testConfig(), func(h *harness) { h.sink.err = errBackpressure })
	h.run(postJSON("/a"), taptest.Options{})
	h.run(postJSON("/b"), taptest.Options{Mode: capture.ModeInline})
	if got := h.drain(); len(got) != 0 {
		t.Errorf("published %d", len(got))
	}
	if got := h.exchanges("demo-api", outcomeSinkError); got != 2 {
		t.Errorf("sink_error = %v", got)
	}
	if got := h.exchanges("demo-api", outcomePublished); got != 0 {
		t.Errorf("published = %v", got)
	}
}

func TestPanicsAreContained(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1 // one worker must survive both panics
	h := newHarness(t, cfg, func(h *harness) {
		h.scrubber.panicOn = "/scrub-panic"
		h.sink.panicOn = "/sink-panic"
	})
	for _, p := range []string{"/scrub-panic", "/sink-panic", "/ok"} {
		h.run(postJSON(p), taptest.Options{})
	}
	if got, want := paths(h.drain()), []string{"/ok"}; !slices.Equal(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
	if got := h.exchanges("demo-api", outcomePanic); got != 2 {
		t.Errorf("panic = %v", got)
	}
	if got := h.exchanges("demo-api", outcomePublished); got != 1 {
		t.Errorf("published = %v", got)
	}
	// The scrubber did see the exchange that made it panic.
	h.scrubber.inputFor(t, "/scrub-panic")
}

func TestInvalidExchange(t *testing.T) {
	h := newHarness(t, testConfig())
	ex := postJSON("/no-status")
	ex.Status = 0 // no :status pseudo-header
	h.run(ex, taptest.Options{})
	if got := h.drain(); len(got) != 0 {
		t.Errorf("published %d", len(got))
	}
	if got := h.exchanges("demo-api", outcomeInvalid); got != 1 {
		t.Errorf("invalid = %v", got)
	}
}

func TestScrubMetrics(t *testing.T) {
	h := newHarness(t, testConfig(), func(h *harness) {
		h.scrubber.redactions = map[string]int{"email": 2, "phone": 1, "card": 0}
		h.scrubber.omit = capture.OmittedBinary
	})
	h.run(postJSON("/a"), taptest.Options{})
	h.run(postJSON("/b"), taptest.Options{Mode: capture.ModeInline})
	got := h.drain()
	if len(got) != 2 {
		t.Fatalf("published %d", len(got))
	}
	for kind, want := range map[string]float64{"email": 4, "phone": 2} {
		if got := testutil.ToFloat64(h.srv.metrics.redactions.WithLabelValues(kind)); got != want {
			t.Errorf("redactions_total{kind=%s} = %v, want %v", kind, got, want)
		}
	}
	if got := testutil.CollectAndCount(h.srv.metrics.redactions); got != 2 {
		t.Errorf("%d redaction series, want 2 (zero counts are skipped)", got)
	}
	if got := testutil.ToFloat64(h.srv.metrics.bodyOmitted.WithLabelValues(capture.OmittedBinary)); got != 2 {
		t.Errorf("body_omitted_total{reason=binary} = %v", got)
	}
	if got := testutil.CollectAndCount(h.srv.metrics.bodyOmitted); got != 1 {
		t.Errorf("%d body_omitted series, want 1", got)
	}
}

func TestShutdownDrainsAndKeepsAnswering(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	h := newHarness(t, cfg, func(h *harness) {
		h.sink.release = make(chan struct{})
		h.sink.entered = make(chan string, 8)
	})
	inline := taptest.Options{Mode: capture.ModeInline}
	for _, p := range []string{"/1", "/2", "/3"} {
		h.run(postJSON(p), inline)
	}
	waitEntered(t, h.sink.entered, "/1")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- h.srv.Shutdown(ctx) }()
	eventually(t, "shutdown to start", func() bool {
		h.srv.mu.RLock()
		defer h.srv.mu.RUnlock()
		return h.srv.closing
	})

	// While draining, inline streams are still answered; their exchanges are
	// dropped.
	res := h.run(postJSON("/during"), inline)
	if res.Sent != 4 || len(res.Replies) != 4 {
		t.Errorf("stream during shutdown: %+v", res)
	}
	close(h.sink.release)
	if err := <-shutdown; err != nil {
		t.Fatalf("Shutdown = %v", err)
	}
	if got, want := paths(h.sink.published()), []string{"/1", "/2", "/3"}; !slices.Equal(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}

	// After Shutdown, too.
	res = h.run(postJSON("/after"), inline)
	if res.Sent != 4 || len(res.Replies) != 4 {
		t.Errorf("stream after shutdown: %+v", res)
	}
	h.run(postJSON("/after-observe"), taptest.Options{})
	if got := h.exchanges("demo-api", outcomeShutdown); got != 3 {
		t.Errorf("shutdown drops = %v, want 3", got)
	}
	if err := h.srv.Shutdown(ctx); err != nil {
		t.Errorf("second Shutdown = %v", err)
	}
	if got := len(h.sink.published()); got != 3 {
		t.Errorf("published %d after shutdown, want 3", got)
	}
}

func TestShutdownDeadline(t *testing.T) {
	cfg := testConfig()
	cfg.Workers = 1
	h := newHarness(t, cfg, func(h *harness) {
		h.sink.release = make(chan struct{})
		h.sink.entered = make(chan string, 8)
	})
	for _, p := range []string{"/1", "/2", "/3"} {
		h.run(postJSON(p), taptest.Options{})
	}
	waitEntered(t, h.sink.entered, "/1")

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.srv.Shutdown(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown = %v, want context.Canceled", err)
	}
	// The exchange in flight finishes; the queued ones are dropped.
	close(h.sink.release)
	ctx, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()
	if err := h.srv.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown = %v", err)
	}
	if got, want := paths(h.sink.published()), []string{"/1"}; !slices.Equal(got, want) {
		t.Errorf("published %v, want %v", got, want)
	}
	if got := h.exchanges("demo-api", outcomeShutdown); got != 2 {
		t.Errorf("shutdown drops = %v, want 2", got)
	}
}

func TestConcurrentStreams(t *testing.T) {
	cfg := testConfig()
	cfg.Workers, cfg.QueueSize = 4, 512
	h := newHarness(t, cfg)
	const n = 200
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			opt := taptest.Options{Mode: capture.ModeObserve, ChunkSize: 16}
			if i%2 == 1 {
				opt = taptest.Options{Mode: capture.ModeInline, BufferLimit: 32}
			}
			ex := postJSON(fmt.Sprintf("/orders/%03d", i))
			ex.RequestBody = []byte(strings.Repeat(fmt.Sprintf(`{"order":%d},`, i), 8))
			if _, err := taptest.Run(ctx, h.client, ex, opt); err != nil {
				errs <- fmt.Errorf("stream %d: %w", i, err)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	got := h.drain()
	if len(got) != n {
		t.Fatalf("published %d, want %d", len(got), n)
	}
	ids := map[string]bool{}
	for _, x := range got {
		ids[x.ID] = true
	}
	want := make([]string, n)
	for i := range n {
		want[i] = fmt.Sprintf("/orders/%03d", i)
	}
	if p := paths(got); len(ids) != n || !slices.Equal(p, want) {
		t.Errorf("%d unique ids; paths %v", len(ids), p)
	}
	if got := h.exchanges("demo-api", outcomePublished); got != n {
		t.Errorf("published = %v", got)
	}
	for _, mode := range allModes() {
		if got := testutil.ToFloat64(h.srv.metrics.streams.WithLabelValues(mode)); got != n/2 {
			t.Errorf("streams_total{mode=%s} = %v", mode, got)
		}
	}
	if got := testutil.ToFloat64(h.srv.metrics.streamsActive); got != 0 {
		t.Errorf("streams_active = %v", got)
	}
}

func TestNewValidation(t *testing.T) {
	sampler, scrubber := &fakeSampler{}, newFakeScrubber()
	sink := &fakeSink{t: t, scrubber: scrubber}
	tests := []struct {
		name     string
		mutate   func(*Config)
		sampler  Sampler
		scrubber Scrubber
		sink     Sink
		want     string
	}{
		{"max body bytes", func(c *Config) { c.MaxBodyBytes = 0 }, sampler, scrubber, sink, "max_body_bytes"},
		{"queue size", func(c *Config) { c.QueueSize = -1 }, sampler, scrubber, sink, "queue_size"},
		{"workers", func(c *Config) { c.Workers = 0 }, sampler, scrubber, sink, "workers"},
		{"metadata key", func(c *Config) { c.ServiceMetadataKey = " " }, sampler, scrubber, sink, "service_metadata_key"},
		{"sampler", func(*Config) {}, nil, scrubber, sink, "sampler"},
		{"scrubber", func(*Config) {}, sampler, nil, sink, "scrubber"},
		{"sink", func(*Config) {}, sampler, scrubber, nil, "sink"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			tt.mutate(&cfg)
			s, err := New(cfg, tt.sampler, tt.scrubber, tt.sink, nil, nil)
			if err == nil {
				_ = s.Shutdown(context.Background())
				t.Fatal("New accepted an invalid config")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error %q does not mention %q", err, tt.want)
			}
		})
	}
	t.Run("all problems reported", func(t *testing.T) {
		_, err := New(Config{}, nil, nil, nil, nil, nil)
		for _, want := range []string{"max_body_bytes", "queue_size", "workers", "service_metadata_key", "sampler", "scrubber", "sink"} {
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Errorf("error %v does not mention %q", err, want)
			}
		}
	})
	t.Run("defaults", func(t *testing.T) {
		cfg := DefaultConfig()
		s, err := New(cfg, sampler, scrubber, sink, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := s.Shutdown(context.Background()); err != nil {
				t.Error(err)
			}
		}()
		host, err := os.Hostname()
		if err != nil {
			t.Skip("no hostname:", err)
		}
		if s.cfg.Name != host {
			t.Errorf("name = %q, want hostname %q", s.cfg.Name, host)
		}
	})
}

func TestMetricsRegistration(t *testing.T) {
	newServer := func(reg prometheus.Registerer) (*Server, error) {
		scrubber := newFakeScrubber()
		s, err := New(testConfig(), &fakeSampler{}, scrubber, &fakeSink{t: t, scrubber: scrubber}, reg, nil)
		if err == nil {
			t.Cleanup(func() { _ = s.Shutdown(context.Background()) })
		}
		return s, err
	}
	t.Run("names", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		if _, err := newServer(reg); err != nil {
			t.Fatal(err)
		}
		// Gauges, pre-created series and histograms are present from the start.
		for _, name := range []string{
			"shadowtap_tap_streams_active", "shadowtap_tap_streams_total", "shadowtap_tap_queue_depth",
			"shadowtap_tap_message_seconds", "shadowtap_tap_process_seconds",
		} {
			if n, err := testutil.GatherAndCount(reg, name); err != nil || n == 0 {
				t.Errorf("%s: %d series, %v", name, n, err)
			}
		}
		if n, _ := testutil.GatherAndCount(reg, "shadowtap_tap_message_seconds"); n != int(numKinds) {
			t.Errorf("message_seconds has %d series, want %d", n, numKinds)
		}
	})
	t.Run("duplicate", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		if _, err := newServer(reg); err != nil {
			t.Fatal(err)
		}
		if _, err := newServer(reg); err == nil || !strings.Contains(err.Error(), "register metrics") {
			t.Fatalf("second New on the same registry = %v", err)
		}
	})
	t.Run("rollback", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		// Same descriptor as the tap's own gauge: the registry keeps a name's
		// help text for good, so only an identical squatter can be removed.
		blocker := prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "shadowtap_tap_queue_depth", Help: "Finished exchanges waiting for a worker.",
		})
		reg.MustRegister(blocker)
		if _, err := newServer(reg); err == nil {
			t.Fatal("New succeeded despite a conflicting collector")
		}
		// Everything registered before the conflict was removed again.
		reg.Unregister(blocker)
		if _, err := newServer(reg); err != nil {
			t.Fatalf("New after removing the conflict: %v", err)
		}
	})
	t.Run("nil registerer", func(t *testing.T) {
		if _, err := newServer(nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMetricsExposition(t *testing.T) {
	h := newHarness(t, testConfig(), func(h *harness) {
		h.sampler.decide = func(r sampling.Request) sampling.Decision {
			if strings.HasPrefix(r.Path, "/skip") {
				return unsampledDecision()
			}
			return sampledDecision()
		}
	})
	h.run(postJSON("/ok"), taptest.Options{})
	h.run(postJSON("/skip"), taptest.Options{Mode: capture.ModeInline})
	h.run(postJSON("/cut"), taptest.Options{Mode: capture.ModeInline, CancelAfter: 1})
	h.waitIdle()
	h.drain()

	const want = `
# HELP shadowtap_tap_decisions_total Sampling decisions by service and decision (sampled, unsampled, excluded, rate_limited).
# TYPE shadowtap_tap_decisions_total counter
shadowtap_tap_decisions_total{decision="sampled",service="demo-api"} 2
shadowtap_tap_decisions_total{decision="unsampled",service="demo-api"} 1
# HELP shadowtap_tap_exchanges_total Sampled exchanges by service and outcome (published, queue_full, incomplete, invalid, sink_error, shutdown, panic).
# TYPE shadowtap_tap_exchanges_total counter
shadowtap_tap_exchanges_total{outcome="incomplete",service="demo-api"} 1
shadowtap_tap_exchanges_total{outcome="published",service="demo-api"} 1
# HELP shadowtap_tap_streams_total ext_proc streams that sent at least one message, by mode (observe or inline).
# TYPE shadowtap_tap_streams_total counter
shadowtap_tap_streams_total{mode="inline"} 2
shadowtap_tap_streams_total{mode="observe"} 1
# HELP shadowtap_tap_streams_active ext_proc streams currently open.
# TYPE shadowtap_tap_streams_active gauge
shadowtap_tap_streams_active 0
# HELP shadowtap_tap_queue_depth Finished exchanges waiting for a worker.
# TYPE shadowtap_tap_queue_depth gauge
shadowtap_tap_queue_depth 0
`
	if err := testutil.GatherAndCompare(h.reg, strings.NewReader(want),
		"shadowtap_tap_decisions_total", "shadowtap_tap_exchanges_total", "shadowtap_tap_streams_total",
		"shadowtap_tap_streams_active", "shadowtap_tap_queue_depth"); err != nil {
		t.Error(err)
	}
}

func TestRegister(t *testing.T) {
	scrubber := newFakeScrubber()
	s, err := New(testConfig(), &fakeSampler{}, scrubber, &fakeSink{t: t, scrubber: scrubber}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Shutdown(context.Background()) }()
	g := grpc.NewServer()
	defer g.Stop()
	s.Register(g)
	if _, ok := g.GetServiceInfo()["envoy.service.ext_proc.v3.ExternalProcessor"]; !ok {
		t.Errorf("ExternalProcessor not registered: %v", g.GetServiceInfo())
	}
}
