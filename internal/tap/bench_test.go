package tap

import (
	"context"
	"runtime"
	"testing"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/tap/taptest"
)

func TestNoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range 20 {
		scrubber := newFakeScrubber()
		cfg := testConfig()
		cfg.Workers = 4
		s, err := New(cfg, &fakeSampler{}, scrubber, &fakeSink{t: t, scrubber: scrubber}, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Process(&fakeStream{t: t, msgs: messages(t, postJSON("/leak"), taptest.Options{}), noSend: true}); err != nil {
			t.Fatal(err)
		}
		if err := s.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "workers to exit", func() bool { return runtime.NumGoroutine() <= before })
}

// benchStream replays msgs on every Process call and discards replies.
type benchStream struct {
	fakeStream
}

func (b *benchStream) reset() { b.recvs = 0 }

func (b *benchStream) Send(*extprocv3.ProcessingResponse) error { return nil }

type discardSink struct{}

func (discardSink) Publish(context.Context, *capture.Exchange) error { return nil }

type noopScrubber struct{}

func (noopScrubber) Exchange(*capture.Exchange) {}

// benchProcess measures Process over the first n messages Envoy sends for a
// POST (all of them when n is 0).
func benchProcess(b *testing.B, decision sampling.Decision, opt taptest.Options, n int) {
	msgs, err := taptest.Messages(postJSON("/api/users?ref=home"), opt)
	if err != nil {
		b.Fatal(err)
	}
	if n > 0 {
		msgs = msgs[:n]
	}
	cfg := testConfig()
	cfg.QueueSize = 1 << 16
	sampler := &constSampler{decision}
	s, err := New(cfg, sampler, noopScrubber{}, discardSink{}, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Shutdown(context.Background()) })
	st := &benchStream{fakeStream{msgs: msgs}}
	b.ReportAllocs()
	for b.Loop() {
		st.reset()
		if err := s.Process(st); err != nil {
			b.Fatal(err)
		}
	}
}

// constSampler is a lock-free Sampler for benchmarks.
type constSampler struct{ d sampling.Decision }

func (c *constSampler) Decide(sampling.Request) sampling.Decision { return c.d }

// The inline critical path in the common case: an unsampled request whose
// mode_override Envoy honours, so only the request headers arrive.
func BenchmarkProcessInlineUnsampled(b *testing.B) {
	benchProcess(b, unsampledDecision(), taptest.Options{Mode: capture.ModeInline}, 1)
}

// An unsampled request on a filter without allow_mode_override: every
// message still needs a reply.
func BenchmarkProcessInlineUnsampledNoOverride(b *testing.B) {
	benchProcess(b, unsampledDecision(), taptest.Options{Mode: capture.ModeInline}, 0)
}

func BenchmarkProcessObserveUnsampled(b *testing.B) {
	benchProcess(b, unsampledDecision(), taptest.Options{Mode: capture.ModeObserve}, 0)
}

func BenchmarkProcessInlineSampled(b *testing.B) {
	benchProcess(b, sampledDecision(), taptest.Options{Mode: capture.ModeInline}, 0)
}

func BenchmarkProcessObserveSampled(b *testing.B) {
	benchProcess(b, sampledDecision(), taptest.Options{Mode: capture.ModeObserve}, 0)
}
