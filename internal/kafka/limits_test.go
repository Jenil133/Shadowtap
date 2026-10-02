package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

var limitsStart = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

// bigExchange returns an exchange whose bodies are size bytes each.
func bigExchange(trace string, size int) *capture.Exchange {
	x := testExchange(trace, limitsStart)
	x.Request.Body.SetData([]byte(`{"pad":"` + strings.Repeat("a", size) + `"}`))
	x.Response.Body.SetData([]byte(`{"pad":"` + strings.Repeat("b", size) + `"}`))
	return x
}

// A batch of records that each fit max_record_bytes must also fit the topic's
// max.message.bytes: the broker rejects an oversized batch whole.
func TestBatchesFitTopicLimit(t *testing.T) {
	cluster := newCluster(t)
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer adminClient.Close()
	limit := "524288"
	ctx := context.Background()
	if _, err := kadm.NewClient(adminClient).CreateTopic(ctx, 1, 1, map[string]*string{"max.message.bytes": &limit}, "limited"); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig(cluster)
	cfg.Topic = "limited"
	cfg.Compression = "none"
	cfg.MaxRecordBytes = 500_000
	cfg.LingerMS = 200 // let the records batch together
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)
	for range 3 {
		// One trace id: one partition, so the records share batches.
		if err := p.Publish(ctx, bigExchange(traceID(1), 100_000)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	closeProducer(t, p)
	if ok, tooLarge := testutil.ToFloat64(p.metrics.ok), testutil.ToFloat64(p.metrics.tooLarge); ok != 3 || tooLarge != 0 {
		t.Fatalf("ok=%v too_large=%v, want 3 and 0", ok, tooLarge)
	}
	readN(t, newTestConsumer(t, cfg, "earliest"), 3)
}

// The value budget leaves room for the key, headers and framing, so a record
// never exceeds max_record_bytes as a whole.
func TestPublishBudgetsForKeyAndHeaders(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	cfg.MaxRecordBytes = 4_000
	cfg.MaxBufferedBytes = 0
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)

	x := bigExchange(traceID(1), 3_000)
	if err := p.Publish(context.Background(), x); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	value, err := capture.Encode(x)
	if err != nil {
		t.Fatal(err)
	}
	if size := userSize(newRecord(cfg.Topic, x, value)) + recordFraming; size > int64(cfg.MaxRecordBytes) {
		t.Errorf("record of %d bytes exceeds max_record_bytes %d", size, cfg.MaxRecordBytes)
	}
	if x.Response.Body.Omitted != capture.OmittedTooLarge {
		t.Errorf("response body not trimmed: omitted=%q", x.Response.Body.Omitted)
	}

	// Key and headers alone over the limit: too large, never an unlimited Fit.
	huge := testExchange(traceID(2), limitsStart)
	huge.Service = strings.Repeat("s", 4_000)
	if err := p.Publish(context.Background(), huge); !errors.Is(err, capture.ErrTooLarge) {
		t.Errorf("Publish with oversized headers = %v, want capture.ErrTooLarge", err)
	}
}

// A stalled Kafka must not grow the buffer past max_buffered_bytes, however
// few records that is.
func TestBufferIsBoundedByBytes(t *testing.T) {
	cluster := newCluster(t)
	release := stallProduce(t, cluster)
	cfg := testConfig(cluster)
	cfg.MaxBufferedBytes = 2 << 20
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)

	accepted := 0
	for i := range 100 {
		err := p.Publish(context.Background(), bigExchange(traceID(i), 128<<10))
		if errors.Is(err, ErrBackpressure) {
			break
		}
		if err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
		accepted++
	}
	if accepted == 0 || accepted >= 100 {
		t.Fatalf("accepted %d records before backpressure", accepted)
	}
	if buffered := p.client.BufferedProduceBytes(); buffered > int64(cfg.MaxBufferedBytes) {
		t.Errorf("buffered %d bytes, limit %d", buffered, cfg.MaxBufferedBytes)
	}
	release()
	closeProducer(t, p)
	if ok := testutil.ToFloat64(p.metrics.ok); ok != float64(accepted) {
		t.Errorf("ok = %v, want every accepted record (%d) delivered", ok, accepted)
	}
}

// With a healthy Kafka a full buffer is a burst: lingering batches are sent
// at once instead of dropping captures for the whole linger.
func TestFullBufferCutsLingerShort(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	cfg.MaxBufferedRecords = 20
	cfg.LingerMS = 5_000
	cfg.DeliveryTimeoutMS = 30_000
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)
	ctx := context.Background()
	for i := range cfg.MaxBufferedRecords {
		if err := p.Publish(ctx, testExchange(traceID(i), limitsStart)); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	start := time.Now()
	for {
		err := p.Publish(ctx, testExchange(traceID(99), limitsStart))
		if err == nil {
			break
		}
		if !errors.Is(err, ErrBackpressure) {
			t.Fatal(err)
		}
		if time.Since(start) > 2*time.Second {
			t.Fatal("buffer still full after 2s; the linger was not cut short")
		}
		time.Sleep(time.Millisecond)
	}
	t.Logf("buffer drained after %v of a %dms linger", time.Since(start), cfg.LingerMS)
}

// Each Close waits no longer than its own context, even while another Close
// is still flushing.
func TestConcurrentCloseHonoursOwnContext(t *testing.T) {
	cluster := newCluster(t)
	release := stallProduce(t, cluster)
	p := newTestProducer(t, testConfig(cluster), prometheus.NewRegistry(), nil)
	if err := p.Publish(context.Background(), testExchange(traceID(1), limitsStart)); err != nil {
		t.Fatal(err)
	}

	first := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		first <- p.Close(ctx)
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.Close(ctx)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("second Close took %v with a 100ms context", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("second Close = %v, want context.DeadlineExceeded", err)
	}
	release()
	if err := <-first; err != nil {
		t.Errorf("first Close = %v, want a completed flush", err)
	}
}

func TestValidateBufferedBytes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaxBufferedBytes = -1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "max_buffered_bytes") {
		t.Errorf("negative max_buffered_bytes: %v", err)
	}
	cfg.MaxBufferedBytes = cfg.MaxRecordBytes - 1
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "below max_record_bytes") {
		t.Errorf("max_buffered_bytes below max_record_bytes: %v", err)
	}
	cfg.MaxBufferedBytes = 0 // default
	if err := cfg.Validate(); err != nil {
		t.Errorf("zero max_buffered_bytes: %v", err)
	}
}
