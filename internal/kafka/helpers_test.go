package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

const (
	testTopic      = capture.DefaultTopic
	testPartitions = 3
)

// newCluster starts an in-memory single-broker Kafka that has the captures
// topic with testPartitions partitions.
func newCluster(t *testing.T, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	opts = append([]kfake.Opt{kfake.NumBrokers(1), kfake.SeedTopics(testPartitions, testTopic)}, opts...)
	c, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatalf("start kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// testConfig is the default config pointed at c, without lingering.
func testConfig(c *kfake.Cluster) Config {
	cfg := DefaultConfig()
	cfg.Brokers = c.ListenAddrs()
	cfg.LingerMS = 0
	return cfg
}

// newTestProducer creates a producer that is closed when the test ends.
func newTestProducer(t *testing.T, cfg Config, reg prometheus.Registerer, log *slog.Logger, extra ...kgo.Opt) *Producer {
	t.Helper()
	p, err := NewProducer(cfg, reg, log, extra...)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.Close(ctx)
	})
	return p
}

// closeProducer closes p, failing the test if buffered records are not
// delivered within a generous deadline.
func closeProducer(t *testing.T, p *Producer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// publishAll publishes xs with a producer of its own and waits until they
// are stored.
func publishAll(t *testing.T, cfg Config, xs ...*capture.Exchange) {
	t.Helper()
	p := newTestProducer(t, cfg, nil, nil)
	for _, x := range xs {
		if err := p.Publish(context.Background(), x); err != nil {
			t.Fatalf("Publish %s: %v", x.ID, err)
		}
	}
	closeProducer(t, p)
	if got := testutil.ToFloat64(p.metrics.ok); got != float64(len(xs)) {
		t.Fatalf("stored %v of %d records", got, len(xs))
	}
}

func newTestConsumer(t *testing.T, cfg Config, start string, extra ...kgo.Opt) *Consumer {
	t.Helper()
	c, err := NewConsumer(cfg, ConsumeOptions{Start: start}, extra...)
	if err != nil {
		t.Fatalf("NewConsumer(start %q): %v", start, err)
	}
	t.Cleanup(c.Close)
	return c
}

// readN reads exactly n messages or fails the test.
func readN(t *testing.T, c *Consumer, n int) []Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	msgs := make([]Message, 0, n)
	for len(msgs) < n {
		m, err := c.Next(ctx)
		if err != nil {
			t.Fatalf("Next after %d of %d messages: %v", len(msgs), n, err)
		}
		msgs = append(msgs, m)
	}
	return msgs
}

// expectNoMore fails the test if another message arrives within wait.
func expectNoMore(t *testing.T, c *Consumer, wait time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	m, err := c.Next(ctx)
	switch {
	case err == nil:
		t.Fatalf("unexpected message key %q at partition %d offset %d", m.Key, m.Partition, m.Offset)
	case !errors.Is(err, context.DeadlineExceeded):
		t.Fatalf("Next = %v, want context.DeadlineExceeded", err)
	}
}

// testExchange returns a scrubbed exchange. started should carry no
// monotonic clock reading so that a JSON round trip compares equal.
func testExchange(traceID string, started time.Time) *capture.Exchange {
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            capture.NewID(),
		Service:       "demo-api",
		TraceID:       traceID,
		ParentSpanID:  "00f067aa0ba902b7",
		RequestID:     "req-" + capture.NewID(),
		Source:        capture.Source{Tap: "shadowtap-tap-0", Mode: capture.ModeObserve},
		Sampling:      capture.Sampling{Rate: 0.25, Rule: "default", KeySource: "traceparent"},
		Timing: capture.Timing{
			StartedAt:         started,
			ResponseStartedAt: started.Add(12 * time.Millisecond),
			CompletedAt:       started.Add(13 * time.Millisecond),
			DurationMicros:    12_000,
		},
		Request: capture.Request{
			Method:    "POST",
			Scheme:    "http",
			Authority: "demo-api:8080",
			Path:      "/api/users?q=<b>&page=2",
			Headers:   capture.Headers{"content-type": {"application/json"}},
		},
		Response: capture.Response{
			Status:  201,
			Headers: capture.Headers{"content-type": {"application/json"}},
		},
		Redactions: map[string]int{"email": 1},
	}
	x.Request.Body = capture.Body{ContentType: "application/json", Size: 39}
	x.Request.Body.SetData([]byte(`{"email":"user-1a2b3c@redacted.invalid"}`))
	x.Response.Body = capture.Body{ContentType: "application/json", Size: 11}
	x.Response.Body.SetData([]byte(`{"id":"u1"}`))
	return x
}

// traceID returns a distinct valid trace id for i.
func traceID(i int) string {
	return fmt.Sprintf("4bf92f3577b34da6a3ce929d%08x", i)
}

// stallProduce holds every produce request the cluster receives until the
// returned release function is called or the test ends.
func stallProduce(t *testing.T, c *kfake.Cluster) (release func()) {
	t.Helper()
	ch := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(ch) }) }
	t.Cleanup(release)
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		c.SleepControl(func() { <-ch })
		return nil, nil, false // then let the cluster handle the request
	})
	return release
}

// waitFor polls cond until it holds, failing the test after a deadline.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// histogramSamples returns the observation count and sum of a histogram.
func histogramSamples(t *testing.T, h prometheus.Histogram) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	if err := h.Write(&m); err != nil {
		t.Fatalf("read histogram: %v", err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// syncBuffer is a bytes.Buffer that a log handler may write to while the
// test reads it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
