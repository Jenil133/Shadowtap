package kafka

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// The tap's Sink, restated so this package does not depend on the tap.
var _ interface {
	Publish(context.Context, *capture.Exchange) error
} = (*Producer)(nil)

func TestProducerConsumerRoundTrip(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	reg := prometheus.NewRegistry()
	p := newTestProducer(t, cfg, reg, nil)

	base := time.Date(2026, 10, 3, 12, 0, 0, 123456789, time.UTC)
	ingress := testExchange("4bf92f3577b34da6a3ce929d0e0e4736", base)
	// A second hop of the same trace must land on the same partition.
	downstream := testExchange(ingress.TraceID, base.Add(40*time.Millisecond))
	downstream.Service = "orders"
	noTrace := testExchange("", base.Add(time.Second))
	noIDs := testExchange("", base.Add(2*time.Second))
	noIDs.RequestID = ""
	sent := []*capture.Exchange{ingress, downstream, noTrace, noIDs}
	wantKeys := map[string]string{
		ingress.ID:    ingress.TraceID,
		downstream.ID: ingress.TraceID,
		noTrace.ID:    noTrace.RequestID,
		noIDs.ID:      noIDs.ID,
	}

	var encodedBytes int
	for _, x := range sent {
		b, err := capture.Encode(x)
		if err != nil {
			t.Fatal(err)
		}
		encodedBytes += len(b)
		if err := p.Publish(context.Background(), x); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	closeProducer(t, p)

	c := newTestConsumer(t, cfg, "earliest")
	got := make(map[string]Message)
	for _, m := range readN(t, c, len(sent)) {
		if m.DecodeErr != nil {
			t.Fatalf("DecodeErr at partition %d offset %d: %v", m.Partition, m.Offset, m.DecodeErr)
		}
		got[m.Exchange.ID] = m
	}
	expectNoMore(t, c, 200*time.Millisecond)

	for _, x := range sent {
		m, ok := got[x.ID]
		if !ok {
			t.Errorf("exchange %s was not consumed", x.ID)
			continue
		}
		if string(m.Key) != wantKeys[x.ID] {
			t.Errorf("%s: key = %q, want %q", x.ID, m.Key, wantKeys[x.ID])
		}
		if want := x.Timing.StartedAt.Truncate(time.Millisecond); !m.Timestamp.Equal(want) {
			t.Errorf("%s: timestamp = %v, want started_at %v", x.ID, m.Timestamp, want)
		}
		wantHeaders := map[string]string{
			capture.HeaderSchemaVersion: "1",
			capture.HeaderContentType:   capture.ContentTypeJSON,
			capture.HeaderService:       x.Service,
		}
		if x.TraceID != "" {
			wantHeaders[capture.HeaderTraceID] = x.TraceID
		}
		if !reflect.DeepEqual(m.Headers, wantHeaders) {
			t.Errorf("%s: headers = %v, want %v", x.ID, m.Headers, wantHeaders)
		}
		if want, _ := capture.Encode(x); !bytes.Equal(m.Raw, want) {
			t.Errorf("%s: value differs from the encoding\n got %s\nwant %s", x.ID, m.Raw, want)
		}
		if !reflect.DeepEqual(m.Exchange, x) {
			t.Errorf("%s: decoded exchange differs\n got %+v\nwant %+v", x.ID, m.Exchange, x)
		}
	}
	if a, b := got[ingress.ID].Partition, got[downstream.ID].Partition; a != b {
		t.Errorf("hops of one trace landed on partitions %d and %d", a, b)
	}

	if v := testutil.ToFloat64(p.metrics.ok); v != float64(len(sent)) {
		t.Errorf("records_total{result=ok} = %v, want %d", v, len(sent))
	}
	if n, _ := histogramSamples(t, p.metrics.produceSeconds); n != uint64(len(sent)) {
		t.Errorf("produce_seconds count = %d, want %d", n, len(sent))
	}
	if n, sum := histogramSamples(t, p.metrics.recordBytes); n != uint64(len(sent)) || sum != float64(encodedBytes) {
		t.Errorf("record_bytes count, sum = %d, %v, want %d, %d", n, sum, len(sent), encodedBytes)
	}
	if v := testutil.ToFloat64(p.metrics.buffered); v != 0 {
		t.Errorf("buffered_records = %v after Close, want 0", v)
	}
}

func TestProducerCompression(t *testing.T) {
	// Codec ids in the record batch attributes.
	tests := []struct {
		compression string
		wantCodecs  []kgo.CompressionCodec
		wantType    uint8
	}{
		{"zstd", []kgo.CompressionCodec{kgo.ZstdCompression(), kgo.NoCompression()}, 4},
		{"snappy", []kgo.CompressionCodec{kgo.SnappyCompression(), kgo.NoCompression()}, 2},
		{"lz4", []kgo.CompressionCodec{kgo.Lz4Compression(), kgo.NoCompression()}, 3},
		{"gzip", []kgo.CompressionCodec{kgo.GzipCompression(), kgo.NoCompression()}, 1},
		{"none", []kgo.CompressionCodec{kgo.NoCompression()}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.compression, func(t *testing.T) {
			cluster := newCluster(t)
			cfg := testConfig(cluster)
			cfg.Compression = tt.compression
			p := newTestProducer(t, cfg, nil, nil)
			if got := p.client.OptValue(kgo.ProducerBatchCompression); !reflect.DeepEqual(got, tt.wantCodecs) {
				t.Errorf("ProducerBatchCompression = %v, want %v", got, tt.wantCodecs)
			}

			x := testExchange(traceID(1), time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
			// Repetitive data so that every codec actually shrinks the batch.
			x.Response.Body.SetData([]byte(strings.Repeat(`{"id":"u1","name":"redacted"},`, 400)))
			if err := p.Publish(context.Background(), x); err != nil {
				t.Fatalf("Publish: %v", err)
			}
			closeProducer(t, p)

			reader, err := kgo.NewClient(
				kgo.SeedBrokers(cfg.Brokers...),
				kgo.ConsumeTopics(testTopic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			fetches := reader.PollFetches(ctx)
			if err := fetches.Err(); err != nil {
				t.Fatalf("fetch: %v", err)
			}
			recs := fetches.Records()
			if len(recs) != 1 {
				t.Fatalf("fetched %d records, want 1", len(recs))
			}
			if got := recs[0].Attrs.CompressionType(); got != tt.wantType {
				t.Errorf("batch compression type = %d, want %d", got, tt.wantType)
			}
			if _, err := capture.Decode(recs[0].Value); err != nil {
				t.Errorf("decode: %v", err)
			}
		})
	}
}

func TestProducerClientOptions(t *testing.T) {
	tests := []struct {
		name            string
		modify          func(*Config)
		extra           []kgo.Opt
		wantBatchBytes  int32
		wantLinger      time.Duration
		wantMaxBuffered int64
		wantDelivery    time.Duration
	}{
		{
			name:            "defaults",
			modify:          func(*Config) {},
			wantBatchBytes:  900_000,
			wantLinger:      20 * time.Millisecond,
			wantMaxBuffered: 10_000,
			wantDelivery:    30 * time.Second,
		},
		{
			name: "large records and zero values",
			modify: func(c *Config) {
				c.MaxRecordBytes = 4 << 20
				c.LingerMS = 0
				c.MaxBufferedRecords = 0
				c.DeliveryTimeoutMS = 0
			},
			wantBatchBytes:  4 << 20,
			wantLinger:      0,
			wantMaxBuffered: 10_000,
			wantDelivery:    0,
		},
		{
			name:            "record just under the default batch limit",
			modify:          func(c *Config) { c.MaxRecordBytes = 1_000_000 },
			wantBatchBytes:  1_000_000,
			wantLinger:      20 * time.Millisecond,
			wantMaxBuffered: 10_000,
			wantDelivery:    30 * time.Second,
		},
		{
			name:            "extra options override the config",
			modify:          func(c *Config) { c.MaxBufferedRecords = 50 },
			extra:           []kgo.Opt{kgo.MaxBufferedRecords(7)},
			wantBatchBytes:  900_000,
			wantLinger:      20 * time.Millisecond,
			wantMaxBuffered: 7,
			wantDelivery:    30 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Brokers = []string{"127.0.0.1:1"} // never dialled
			tt.modify(&cfg)
			p := newTestProducer(t, cfg, nil, nil, tt.extra...)

			checks := []struct {
				name string
				opt  any
				want any
			}{
				{"DefaultProduceTopic", kgo.DefaultProduceTopic, capture.DefaultTopic},
				{"RequiredAcks", kgo.RequiredAcks, kgo.AllISRAcks()},
				{"DisableIdempotentWrite", kgo.DisableIdempotentWrite, false},
				{"ProducerBatchMaxBytes", kgo.ProducerBatchMaxBytes, tt.wantBatchBytes},
				{"ProducerLinger", kgo.ProducerLinger, tt.wantLinger},
				{"MaxBufferedRecords", kgo.MaxBufferedRecords, tt.wantMaxBuffered},
				{"MaxBufferedBytes", kgo.MaxBufferedBytes, int64(256 << 20)},
				// Never set: franz-go would measure it from the request start.
				{"RecordDeliveryTimeout", kgo.RecordDeliveryTimeout, time.Duration(0)},
				{"ClientID", kgo.ClientID, "shadowtap"},
			}
			for _, c := range checks {
				if got := p.client.OptValue(c.opt); !reflect.DeepEqual(got, c.want) {
					t.Errorf("%s = %v, want %v", c.name, got, c.want)
				}
			}
			if p.maxBuffered != tt.wantMaxBuffered {
				t.Errorf("backpressure threshold = %d, want the client's %d", p.maxBuffered, tt.wantMaxBuffered)
			}
			if p.deliveryTimeout != tt.wantDelivery {
				t.Errorf("delivery timeout = %v, want %v", p.deliveryTimeout, tt.wantDelivery)
			}
		})
	}
}

func TestNewProducerRejectsInvalidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Compression = "brotli"
	if _, err := NewProducer(cfg, nil, nil); err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("NewProducer error = %v, want a compression error", err)
	}
}

func TestPublishFitsOversizedExchange(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	cfg.MaxRecordBytes = 64 << 10
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)

	started := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	big := testExchange(traceID(1), started)
	requestBody := big.Request.Body.Data
	big.Response.Body.SetData([]byte(strings.Repeat("x", 200<<10)))
	if err := p.Publish(context.Background(), big); err != nil {
		t.Fatalf("Publish oversized exchange: %v", err)
	}
	// The exchange passed in now matches what was published.
	if big.Response.Body.Omitted != capture.OmittedTooLarge {
		t.Errorf("caller's exchange response body omitted = %q, want %q", big.Response.Body.Omitted, capture.OmittedTooLarge)
	}

	// Headers are never dropped, so nothing can make this one fit.
	impossible := testExchange(traceID(2), started)
	impossible.Request.Headers.Add("x-padding", strings.Repeat("p", 100<<10))
	err := p.Publish(context.Background(), impossible)
	if !errors.Is(err, capture.ErrTooLarge) {
		t.Fatalf("Publish impossible exchange = %v, want capture.ErrTooLarge", err)
	}
	closeProducer(t, p)

	c := newTestConsumer(t, cfg, "earliest")
	m := readN(t, c, 1)[0]
	expectNoMore(t, c, 200*time.Millisecond)
	if len(m.Raw) > cfg.MaxRecordBytes {
		t.Errorf("record value is %d bytes, above max_record_bytes %d", len(m.Raw), cfg.MaxRecordBytes)
	}
	if m.Exchange.ID != big.ID {
		t.Fatalf("consumed exchange %s, want %s", m.Exchange.ID, big.ID)
	}
	if b := m.Exchange.Response.Body; b.Omitted != capture.OmittedTooLarge || b.Data != "" || b.Size != big.Response.Body.Size {
		t.Errorf("response body = %+v, want data dropped as too_large with size kept", b)
	}
	if got := m.Exchange.Request.Body.Data; got != requestBody {
		t.Errorf("request body = %q, want it kept as %q", got, requestBody)
	}

	if v := testutil.ToFloat64(p.metrics.ok); v != 1 {
		t.Errorf("records_total{result=ok} = %v, want 1", v)
	}
	if v := testutil.ToFloat64(p.metrics.tooLarge); v != 1 {
		t.Errorf("records_total{result=too_large} = %v, want 1", v)
	}
	if n, _ := histogramSamples(t, p.metrics.recordBytes); n != 1 {
		t.Errorf("record_bytes count = %d, want 1 (rejected records are not observed)", n)
	}
}

func TestPublishBackpressure(t *testing.T) {
	cluster := newCluster(t)
	release := stallProduce(t, cluster)
	cfg := testConfig(cluster)
	cfg.MaxBufferedRecords = 3
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)
	ctx := context.Background()
	started := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

	for i := range cfg.MaxBufferedRecords {
		if err := p.Publish(ctx, testExchange(traceID(i), started)); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
	}
	if v := testutil.ToFloat64(p.metrics.buffered); v != 3 {
		t.Fatalf("buffered_records = %v, want 3", v)
	}

	const rejected = 20
	for i := range rejected {
		start := time.Now()
		err := p.Publish(ctx, testExchange(traceID(100+i), started))
		elapsed := time.Since(start)
		if !errors.Is(err, ErrBackpressure) {
			t.Fatalf("Publish with a full buffer = %v, want ErrBackpressure", err)
		}
		if elapsed > 50*time.Millisecond {
			t.Errorf("Publish took %v with a full buffer, want it to fail fast", elapsed)
		}
	}
	if v := testutil.ToFloat64(p.metrics.backpressure); v != rejected {
		t.Errorf("records_total{result=backpressure} = %v, want %d", v, rejected)
	}

	release()
	closeProducer(t, p)
	if v := testutil.ToFloat64(p.metrics.ok); v != 3 {
		t.Errorf("records_total{result=ok} = %v, want 3", v)
	}
	if v := testutil.ToFloat64(p.metrics.failed); v != 0 {
		t.Errorf("records_total{result=error} = %v, want 0", v)
	}
	c := newTestConsumer(t, cfg, "earliest")
	readN(t, c, 3)
	expectNoMore(t, c, 200*time.Millisecond)
}

func TestCloseFlushesPendingRecords(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	// Without a flush these records would sit out the linger.
	cfg.LingerMS, cfg.DeliveryTimeoutMS = 60_000, 120_000
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)

	const n = 5
	started := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	for i := range n {
		if err := p.Publish(context.Background(), testExchange(traceID(i), started)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if v := testutil.ToFloat64(p.metrics.buffered); v != n {
		t.Fatalf("buffered_records = %v before Close, want %d", v, n)
	}

	start := time.Now()
	closeProducer(t, p)
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Close took %v; it should flush rather than wait out the linger", elapsed)
	}
	if v := testutil.ToFloat64(p.metrics.ok); v != n {
		t.Errorf("records_total{result=ok} = %v after Close, want %d", v, n)
	}
	c := newTestConsumer(t, cfg, "earliest")
	readN(t, c, n)
}

func TestCloseIsBoundedByContext(t *testing.T) {
	cluster := newCluster(t)
	release := stallProduce(t, cluster)
	cfg := testConfig(cluster)
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)
	started := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	for i := range 2 {
		if err := p.Publish(context.Background(), testExchange(traceID(i), started)); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := p.Close(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("Close took %v with a 100ms context", elapsed)
	}
	// Records the flush could not deliver are failed by closing the client.
	waitFor(t, "undelivered records to be counted as errors", func() bool {
		return testutil.ToFloat64(p.metrics.failed) == 2
	})
	if again := p.Close(context.Background()); !errors.Is(again, context.DeadlineExceeded) {
		t.Errorf("second Close = %v, want the first result", again)
	}
	release()
}

func TestPublishAfterClose(t *testing.T) {
	cluster := newCluster(t)
	p := newTestProducer(t, testConfig(cluster), prometheus.NewRegistry(), nil)
	closeProducer(t, p)

	err := p.Publish(context.Background(), testExchange(traceID(1), time.Now()))
	if !errors.Is(err, kgo.ErrClientClosed) {
		t.Fatalf("Publish after Close = %v, want kgo.ErrClientClosed", err)
	}
	if v := testutil.ToFloat64(p.metrics.failed); v != 1 {
		t.Errorf("records_total{result=error} = %v, want 1", v)
	}
	if err := p.Close(context.Background()); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
	if err := p.Ping(context.Background()); err == nil {
		t.Error("Ping after Close succeeded")
	}
}

func TestPublishRejectsBadInput(t *testing.T) {
	cluster := newCluster(t)
	p := newTestProducer(t, testConfig(cluster), prometheus.NewRegistry(), nil)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Publish(cancelled, testExchange(traceID(1), time.Now())); !errors.Is(err, context.Canceled) {
		t.Errorf("Publish with a cancelled context = %v, want context.Canceled", err)
	}
	if err := p.Publish(context.Background(), nil); err == nil {
		t.Error("Publish(nil) succeeded")
	}
	if v := testutil.ToFloat64(p.metrics.failed); v != 2 {
		t.Errorf("records_total{result=error} = %v, want 2", v)
	}
}

func TestDeliveryOutlivesCallerContext(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	// The record lingers in the buffer, where franz-go would fail it if it
	// carried a context that is cancelled by the time it is sent.
	cfg.LingerMS, cfg.DeliveryTimeoutMS = 60_000, 120_000
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Publish(ctx, testExchange(traceID(1), time.Now())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	cancel()
	closeProducer(t, p)
	if v := testutil.ToFloat64(p.metrics.ok); v != 1 {
		t.Errorf("records_total{result=ok} = %v, want 1", v)
	}
	if v := testutil.ToFloat64(p.metrics.failed); v != 0 {
		t.Errorf("records_total{result=error} = %v, want 0", v)
	}
}

func TestDeliveryTimeoutStartsAtPublish(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	cfg.DeliveryTimeoutMS = 1_000
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)

	// The record timestamp is the request start, an hour before Publish; the
	// delivery timeout must not count that hour.
	x := testExchange(traceID(1), time.Now().Add(-time.Hour))
	if err := p.Publish(context.Background(), x); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	closeProducer(t, p)
	if v := testutil.ToFloat64(p.metrics.ok); v != 1 {
		t.Errorf("records_total{result=ok} = %v, want 1 (error = %v)", v, testutil.ToFloat64(p.metrics.failed))
	}
	c := newTestConsumer(t, cfg, "earliest")
	if m := readN(t, c, 1)[0]; !m.Timestamp.Equal(x.Timing.StartedAt.Truncate(time.Millisecond)) {
		t.Errorf("timestamp = %v, want the request start %v", m.Timestamp, x.Timing.StartedAt)
	}
}

func TestDeliveryTimeoutWhenKafkaIsDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Brokers = []string{ln.Addr().String()}
	cfg.DeliveryTimeoutMS = 1_000
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), slog.New(slog.NewTextHandler(logs, nil)))

	start := time.Now()
	if err := p.Publish(context.Background(), testExchange(traceID(1), time.Now())); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("Publish took %v with Kafka down, want it not to wait", elapsed)
	}
	waitFor(t, "the undeliverable record to time out", func() bool {
		return testutil.ToFloat64(p.metrics.failed) == 1
	})
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Errorf("record failed after %v, before its 1s delivery timeout", elapsed)
	}
	// The promise counts the failure before it logs it.
	waitFor(t, "the delivery timeout to be logged", func() bool {
		return strings.Contains(logs.String(), "timed out")
	})
}

func TestProduceErrorsAreCountedAndLogged(t *testing.T) {
	cluster := newCluster(t)
	cluster.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: testTopic,
		Err:   kerr.InvalidRecord, // not retriable, so every record fails
		Count: -1,
	})
	logs := &syncBuffer{}
	cfg := testConfig(cluster)
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), slog.New(slog.NewTextHandler(logs, nil)))

	const n = 5
	for i := range n {
		if err := p.Publish(context.Background(), testExchange(traceID(i), time.Now())); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	waitFor(t, "produce failures to be counted", func() bool {
		return testutil.ToFloat64(p.metrics.failed) == n
	})
	if got := strings.Count(logs.String(), "kafka produce failed"); got != 1 {
		t.Errorf("logged %d produce failures within 10s, want 1:\n%s", got, logs)
	}
	if !strings.Contains(logs.String(), "INVALID_RECORD") {
		t.Errorf("log does not name the broker error:\n%s", logs)
	}
	if v := testutil.ToFloat64(p.metrics.ok); v != 0 {
		t.Errorf("records_total{result=ok} = %v, want 0", v)
	}

	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !strings.Contains(logs.String(), "count="+strconv.Itoa(n-1)) {
		t.Errorf("Close did not report the %d suppressed failures:\n%s", n-1, logs)
	}
}

func TestLogLimiter(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	l := logLimiter{interval: 10 * time.Second, now: func() time.Time { return now }}
	step := func(advance time.Duration, wantOK bool, wantSuppressed int) {
		t.Helper()
		now = now.Add(advance)
		ok, suppressed := l.allow()
		if ok != wantOK || suppressed != wantSuppressed {
			t.Fatalf("allow() at +%v = %v, %d, want %v, %d", advance, ok, suppressed, wantOK, wantSuppressed)
		}
	}
	step(0, true, 0)
	step(time.Second, false, 0)
	step(time.Second, false, 0)
	step(7*time.Second, false, 0)
	step(time.Second, true, 3) // 10s after the first logged event
	step(0, false, 0)
	if got := l.takeSuppressed(); got != 1 {
		t.Errorf("takeSuppressed() = %d, want 1", got)
	}
	if got := l.takeSuppressed(); got != 0 {
		t.Errorf("takeSuppressed() again = %d, want 0", got)
	}
	step(time.Hour, true, 0)
}

func TestPing(t *testing.T) {
	cluster := newCluster(t)
	p := newTestProducer(t, testConfig(cluster), nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	// A port nothing listens on any more refuses connections.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Brokers = []string{ln.Addr().String()}
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	down := newTestProducer(t, cfg, nil, nil)
	if err := down.Ping(ctx); err == nil {
		t.Fatal("Ping of an unreachable broker succeeded")
	}
}

func TestProducerMetricsRegistration(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Brokers = []string{"127.0.0.1:1"} // never dialled

	regA, regB := prometheus.NewRegistry(), prometheus.NewRegistry()
	a := newTestProducer(t, cfg, regA, nil)
	newTestProducer(t, cfg, regB, nil)
	newTestProducer(t, cfg, nil, nil)

	if _, err := NewProducer(cfg, regA, nil); err == nil {
		t.Error("second producer on the same registry registered its metrics")
	}

	families, err := regA.Gather()
	if err != nil {
		t.Fatal(err)
	}
	types := make(map[string]string)
	for _, f := range families {
		types[f.GetName()] = f.GetType().String()
	}
	want := map[string]string{
		"shadowtap_kafka_records_total":    "COUNTER",
		"shadowtap_kafka_produce_seconds":  "HISTOGRAM",
		"shadowtap_kafka_record_bytes":     "HISTOGRAM",
		"shadowtap_kafka_buffered_records": "GAUGE",
	}
	if !reflect.DeepEqual(types, want) {
		t.Errorf("registered metrics = %v, want %v", types, want)
	}
	if n := testutil.CollectAndCount(a.metrics, "shadowtap_kafka_records_total"); n != 4 {
		t.Errorf("records_total has %d series, want one per result (4)", n)
	}
	problems, err := testutil.CollectAndLint(a.metrics)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range problems {
		t.Errorf("metric lint: %s: %s", p.Metric, p.Text)
	}
}

func TestConcurrentPublishAndClose(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	p := newTestProducer(t, cfg, prometheus.NewRegistry(), nil)

	const workers, perWorker = 8, 200
	var (
		wg       sync.WaitGroup
		accepted atomic.Int64
	)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWorker {
				err := p.Publish(context.Background(), testExchange(traceID(w*perWorker+i), time.Now()))
				switch {
				case err == nil:
					accepted.Add(1)
				case errors.Is(err, kgo.ErrClientClosed):
				default:
					t.Errorf("Publish: %v", err)
				}
			}
		}()
	}
	// Close while records are still being published and delivered.
	waitFor(t, "some records to be accepted", func() bool { return accepted.Load() >= workers })
	closeProducer(t, p)
	wg.Wait()

	n := float64(accepted.Load())
	ok, failed := testutil.ToFloat64(p.metrics.ok), testutil.ToFloat64(p.metrics.failed)
	// Every accepted record was delivered by the flush; every rejected one
	// was counted as an error.
	if ok != n {
		t.Errorf("records_total{result=ok} = %v, want the %v accepted records", ok, n)
	}
	if ok+failed != workers*perWorker {
		t.Errorf("ok %v + error %v != %d publishes", ok, failed, workers*perWorker)
	}
	t.Logf("%v of %d publishes were accepted before Close", n, workers*perWorker)
}

// BenchmarkPublish measures the cost a tap worker pays per capture: encoding
// and handing the record to the client, not delivery.
func BenchmarkPublish(b *testing.B) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(testPartitions, testTopic))
	if err != nil {
		b.Fatal(err)
	}
	defer cluster.Close()
	cfg := testConfig(cluster)
	cfg.LingerMS = 5
	p, err := NewProducer(cfg, nil, nil)
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_ = p.Close(ctx)
	}()
	x := testExchange(traceID(1), time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC))
	ctx := context.Background()

	var dropped int
	b.ReportAllocs()
	// Not a switch: go1.26.7 crashes compiling a tagless switch in a b.Loop
	// body when a later case calls a function.
	for b.Loop() {
		if err := p.Publish(ctx, x); err != nil {
			if !errors.Is(err, ErrBackpressure) {
				b.Fatal(err)
			}
			dropped++
		}
	}
	b.ReportMetric(float64(dropped)/float64(b.N), "dropped/op")
}
