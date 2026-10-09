package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

// tracerName is the instrumentation scope of the producer's spans.
const tracerName = "github.com/Jenil133/Shadowtap/internal/kafka"

// produceSpanName names the span that covers one record from Publish to the
// broker's acknowledgement.
const produceSpanName = "shadowtap.kafka.produce"

// ErrBackpressure is returned by Publish when the producer buffer is full
// because Kafka is slower than capture or unreachable. The exchange is not
// published.
var ErrBackpressure = errors.New("kafka: producer buffer full")

// errClosed is returned by Publish and Ping after Close.
var errClosed = fmt.Errorf("kafka: producer closed: %w", kgo.ErrClientClosed)

const (
	// recordFraming covers the record and batch framing (lengths, varints,
	// the 61-byte batch header) around a record's key, value and headers.
	recordFraming = 256
	// errorLogInterval bounds how often asynchronous produce failures are
	// logged; the metrics count every one of them.
	errorLogInterval = 10 * time.Second
)

// Producer publishes captures to Kafka and is the tap's Sink. Publish never
// waits for Kafka: records are buffered in the client and delivered
// asynchronously, and the outcome of each one is reported through metrics
// and rate-limited logs. The client is idempotent with acks=all, so its
// retries neither duplicate nor reorder records within a partition.
//
// A Producer is safe for concurrent use.
type Producer struct {
	client           *kgo.Client
	topic            string
	maxRecordBytes   int
	maxBuffered      int64
	maxBufferedBytes int64
	linger           time.Duration
	deliveryTimeout  time.Duration
	log              *slog.Logger
	metrics          *producerMetrics
	errLog           logLimiter

	// ctx spans the producer's life. Records are produced under it instead
	// of the caller's context, which may be cancelled as soon as Publish
	// returns.
	ctx    context.Context
	cancel context.CancelFunc

	// mu makes the buffer check and TryProduce a single step and orders
	// Publish against Close.
	mu     sync.Mutex
	closed atomic.Bool

	// inflight counts records whose promise has not run yet.
	inflight sync.WaitGroup

	// unlingering is set while a background flush cuts lingering batches
	// short because the buffer is full; bg tracks that goroutine.
	unlingering atomic.Bool
	bg          sync.WaitGroup

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// NewProducer validates cfg and creates a producer for cfg.Topic. Metrics are
// registered on reg unless it is nil; a registry takes one producer, so
// registering a second one fails. A nil log discards logs. The extra options
// are applied last and override those derived from cfg.
func NewProducer(cfg Config, reg prometheus.Registerer, log *slog.Logger, extra ...kgo.Opt) (*Producer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	opts, err := ClientOptions(cfg)
	if err != nil {
		return nil, err
	}
	opts = append(opts, producerOptions(cfg)...)
	client, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create producer: %w", err)
	}

	// The client's own limits, which extra options may override.
	maxBuffered := int64(maxBufferedRecords(cfg))
	if v, ok := client.OptValue(kgo.MaxBufferedRecords).(int64); ok && v > 0 {
		maxBuffered = v
	}
	maxBytes := int64(maxBufferedBytes(cfg))
	if v, ok := client.OptValue(kgo.MaxBufferedBytes).(int64); ok && v > 0 {
		maxBytes = v
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		client:           client,
		topic:            cfg.Topic,
		maxRecordBytes:   cfg.MaxRecordBytes,
		maxBuffered:      maxBuffered,
		maxBufferedBytes: maxBytes,
		linger:           time.Duration(cfg.LingerMS) * time.Millisecond,
		deliveryTimeout:  time.Duration(cfg.DeliveryTimeoutMS) * time.Millisecond,
		log:              log,
		metrics:          newProducerMetrics(func() float64 { return float64(client.BufferedProduceRecords()) }),
		errLog:           logLimiter{interval: errorLogInterval, now: time.Now},
		ctx:              ctx,
		cancel:           cancel,
		closeDone:        make(chan struct{}),
	}
	if reg != nil {
		if err := reg.Register(p.metrics); err != nil {
			cancel()
			client.Close()
			return nil, fmt.Errorf("kafka: register producer metrics: %w", err)
		}
	}
	return p, nil
}

// producerOptions derives the producer settings from a validated cfg.
//
// DeliveryTimeoutMS is deliberately not passed as kgo.RecordDeliveryTimeout:
// franz-go measures that from the record timestamp, which here is the request
// start, so any capture of a request that began longer ago than the timeout
// would fail at once. produce applies it as a deadline from Publish instead.
func producerOptions(cfg Config) []kgo.Opt {
	codecs := []kgo.CompressionCodec{kgo.NoCompression()}
	if codec, _ := compressionCodec(cfg.Compression); codec != kgo.NoCompression() {
		// none stays as the fallback for brokers too old for the codec.
		codecs = append([]kgo.CompressionCodec{codec}, codecs...)
	}
	return []kgo.Opt{
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ProducerBatchCompression(codecs...),
		kgo.ProducerLinger(time.Duration(cfg.LingerMS) * time.Millisecond),
		kgo.MaxBufferedRecords(maxBufferedRecords(cfg)),
		kgo.MaxBufferedBytes(maxBufferedBytes(cfg)),
		// A batch, not just a record, must fit the topic's max.message.bytes:
		// the broker rejects an oversized batch whole and franz-go does not
		// split it.
		kgo.ProducerBatchMaxBytes(int32(cfg.MaxRecordBytes)),
		// acks=all is what the idempotent producer (on by default) requires.
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
}

// maxBufferedRecords is cfg.MaxBufferedRecords, with zero meaning the default.
func maxBufferedRecords(cfg Config) int {
	if cfg.MaxBufferedRecords > 0 {
		return cfg.MaxBufferedRecords
	}
	return DefaultConfig().MaxBufferedRecords
}

// maxBufferedBytes is cfg.MaxBufferedBytes, with zero meaning the default.
func maxBufferedBytes(cfg Config) int {
	if cfg.MaxBufferedBytes > 0 {
		return cfg.MaxBufferedBytes
	}
	return DefaultConfig().MaxBufferedBytes
}

// valueBudget is how large an exchange's encoding may be so that its whole
// record — key, headers and framing included — stays within MaxRecordBytes.
func (p *Producer) valueBudget(key []byte, headers []capture.RecordHeader) int {
	overhead := len(key) + recordFraming
	for _, h := range headers {
		overhead += len(h.Key) + len(h.Value)
	}
	return p.maxRecordBytes - overhead
}

// Publish encodes x, shrinking it to MaxRecordBytes with x.Fit (which updates
// x to match what is published), and hands the record to the client without
// waiting for Kafka. It returns ErrBackpressure when the producer buffer is
// full, an error wrapping capture.ErrTooLarge when x cannot be made small
// enough, and an error after Close or when ctx is already done. A nil error
// means the record was buffered; whether Kafka stored it is reported by the
// metrics. ctx only gates these checks: delivery continues after Publish
// returns, whatever happens to ctx.
//
// The record is traced: a "shadowtap.kafka.produce" span, child of the span
// in ctx, lasts until the broker acknowledges or fails the record, and the
// record headers carry that span's context (traceparent) so a consumer can
// continue the trace from the record alone.
func (p *Producer) Publish(ctx context.Context, x *capture.Exchange) error {
	start := time.Now()
	if ctx != nil && ctx.Err() != nil {
		p.metrics.failed.Inc()
		return fmt.Errorf("kafka: publish: %w", ctx.Err())
	}
	if x == nil {
		p.metrics.failed.Inc()
		return errors.New("kafka: publish: nil exchange")
	}
	if p.closed.Load() {
		p.metrics.failed.Inc()
		return errClosed
	}
	// Also checked before encoding so that a stalled Kafka does not cost an
	// encode for every capture that is dropped anyway.
	if p.bufferFull(0) {
		p.metrics.backpressure.Inc()
		p.mu.Lock()
		p.unlinger()
		p.mu.Unlock()
		return ErrBackpressure
	}

	key := x.Key()
	ctx, span := p.startProduceSpan(orBackground(ctx), key)
	headers := telemetry.InjectHeaders(ctx, x.RecordHeaders())
	budget := p.valueBudget(key, headers)
	if budget <= 0 {
		// Fit treats a non-positive limit as no limit.
		p.metrics.tooLarge.Inc()
		err := fmt.Errorf("kafka: publish exchange %s: %w: key and headers alone exceed %d bytes", x.ID, capture.ErrTooLarge, p.maxRecordBytes)
		endProduceSpan(span, err)
		return err
	}
	value, err := x.Fit(budget)
	if err != nil {
		if errors.Is(err, capture.ErrTooLarge) {
			p.metrics.tooLarge.Inc()
		} else {
			p.metrics.failed.Inc()
		}
		err = fmt.Errorf("kafka: publish exchange %s: %w", x.ID, err)
		endProduceSpan(span, err)
		return err
	}
	if err := p.produce(newRecord(p.topic, key, headers, value, x.Timing.StartedAt), span, start); err != nil {
		return err
	}
	p.metrics.recordBytes.Observe(float64(len(value)))
	return nil
}

// startProduceSpan starts the produce span for a record with key as a child
// of the span in ctx. The tracer comes from the parent span's own provider
// when the parent is recording, so the child lands in the same pipeline, and
// from the global provider otherwise.
func (p *Producer) startProduceSpan(ctx context.Context, key []byte) (context.Context, trace.Span) {
	tp := otel.GetTracerProvider()
	if parent := trace.SpanFromContext(ctx); parent.IsRecording() {
		tp = parent.TracerProvider()
	}
	return tp.Tracer(tracerName).Start(ctx, produceSpanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingDestinationName(p.topic),
			semconv.MessagingKafkaMessageKey(string(key)),
		),
	)
}

// endProduceSpan ends span, recording err when the record was not stored.
// It also runs inside the delivery promise, so it does nothing slow.
func endProduceSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "record not delivered")
	}
	span.End()
}

// produce buffers r in the client unless the producer is closed or full.
// span is r's produce span, ended here on failure and in the promise
// otherwise; start is when Publish was called.
func (p *Producer) produce(r *kgo.Record, span trace.Span, start time.Time) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed.Load() {
		p.metrics.failed.Inc()
		endProduceSpan(span, errClosed)
		return errClosed
	}
	// TryProduce reports a full buffer only to the promise, after Publish
	// has already returned nil. Only produce adds to the buffer and it holds
	// mu from this check through TryProduce, so the check is exact.
	if p.bufferFull(userSize(r)) {
		p.metrics.backpressure.Inc()
		p.unlinger()
		endProduceSpan(span, ErrBackpressure)
		return ErrBackpressure
	}
	// franz-go fails a batch once its first record's context is done, with
	// the same care for in-flight idempotent batches as its own timeout.
	ctx, cancel := p.ctx, context.CancelFunc(nil)
	if p.deliveryTimeout > 0 {
		ctx, cancel = context.WithTimeout(p.ctx, p.deliveryTimeout)
	}
	p.inflight.Add(1)
	p.client.TryProduce(ctx, r, func(_ *kgo.Record, err error) {
		defer p.inflight.Done()
		if cancel != nil {
			cancel()
		}
		p.delivered(span, err, time.Since(start))
	})
	return nil
}

// bufferFull reports whether a record of size more bytes would exceed either
// buffer limit.
func (p *Producer) bufferFull(size int64) bool {
	return p.client.BufferedProduceRecords() >= p.maxBuffered ||
		p.client.BufferedProduceBytes()+size > p.maxBufferedBytes
}

// userSize is the size franz-go accounts a record at in its buffer.
func userSize(r *kgo.Record) int64 {
	n := len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value)
	}
	return int64(n)
}

// unlinger cuts every lingering batch short when the buffer is full, as
// franz-go's blocking Produce does and TryProduce does not; otherwise a
// healthy Kafka would see captures dropped for a whole linger period.
// p.mu must be held: shutdown sets closed under it before waiting on bg.
func (p *Producer) unlinger() {
	if p.linger <= 0 || p.closed.Load() || !p.unlingering.CompareAndSwap(false, true) {
		return
	}
	p.bg.Add(1)
	go func() {
		defer p.bg.Done()
		defer p.unlingering.Store(false)
		ctx, cancel := context.WithTimeout(p.ctx, p.linger)
		defer cancel()
		_ = p.client.Flush(ctx) // only to unlinger; delivery is tracked by promises
	}()
}

// newRecord builds the record for an exchange encoded as value, with the
// request start (or now, when it is unknown) as the record timestamp.
func newRecord(topic string, key []byte, headers []capture.RecordHeader, value []byte, started time.Time) *kgo.Record {
	r := &kgo.Record{
		Topic:     topic,
		Key:       key,
		Value:     value,
		Headers:   make([]kgo.RecordHeader, len(headers)),
		Timestamp: started,
	}
	for i, h := range headers {
		r.Headers[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}
	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now()
	}
	return r
}

// delivered is the produce promise. franz-go runs promises one at a time on
// its own goroutine, so it has to be quick, and it must never take p.mu: a
// Publish holding p.mu inside TryProduce may be waiting on promise progress.
// It ends the record's produce span, with the error when there is one.
func (p *Producer) delivered(span trace.Span, err error, elapsed time.Duration) {
	switch {
	case err == nil:
		p.metrics.ok.Inc()
		p.metrics.produceSeconds.Observe(elapsed.Seconds())
		span.End()
		return
	case errors.Is(err, kgo.ErrMaxBuffered):
		// Unreachable unless extra options lower the client's limits below
		// the ones bufferFull checks.
		p.metrics.backpressure.Inc()
		endProduceSpan(span, err)
		return
	case errors.Is(err, kerr.MessageTooLarge):
		// The broker's max.message.bytes is below max_record_bytes.
		p.metrics.tooLarge.Inc()
	case errors.Is(err, context.DeadlineExceeded):
		// p.ctx has no deadline, so this is the delivery timeout.
		p.metrics.failed.Inc()
		err = fmt.Errorf("%w: not delivered within %v", kgo.ErrRecordTimeout, p.deliveryTimeout)
	default:
		p.metrics.failed.Inc()
	}
	endProduceSpan(span, err)
	if ok, suppressed := p.errLog.allow(); ok {
		// Logged under the span so the line carries the record's trace id.
		p.log.ErrorContext(trace.ContextWithSpan(context.Background(), span),
			"kafka produce failed", "topic", p.topic, "error", err, "suppressed", suppressed)
	}
}

// Ping checks that a broker answers a metadata request, which exercises TLS
// and SASL when they are configured.
func (p *Producer) Ping(ctx context.Context) error {
	if p.closed.Load() {
		return errClosed
	}
	if err := p.client.Ping(orBackground(ctx)); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}
	return nil
}

// Close stops accepting records, waits until ctx is done for buffered
// records to be delivered, and closes the client. Records still buffered at
// that point fail and are counted as errors, and Close reports that the
// flush did not finish. Close is idempotent: the first call's ctx bounds the
// shutdown, every call returns its result, and each call waits no longer
// than its own ctx.
func (p *Producer) Close(ctx context.Context) error {
	ctx = orBackground(ctx)
	p.closeOnce.Do(func() {
		go func() {
			p.closeErr = p.shutdown(ctx)
			close(p.closeDone)
		}()
	})
	select {
	case <-p.closeDone:
		return p.closeErr
	case <-ctx.Done():
		return fmt.Errorf("kafka: close: %w", ctx.Err())
	}
}

func (p *Producer) shutdown(ctx context.Context) error {
	// Taking mu waits out any Publish inside TryProduce; from here on no
	// record is added and inflight only goes down.
	p.mu.Lock()
	p.closed.Store(true)
	p.mu.Unlock()

	var err error
	if ferr := p.client.Flush(ctx); ferr != nil {
		err = fmt.Errorf("kafka: close: flush: %w (%d records undelivered)", ferr, p.client.BufferedProduceRecords())
	}
	// Close fails whatever is still buffered with kgo.ErrClientClosed.
	p.client.Close()
	p.cancel()
	p.bg.Wait() // a background flush ends with p.ctx

	// Those failures reach the promises asynchronously. Waiting for them
	// makes the metrics final, unless the caller is out of time.
	done := make(chan struct{})
	go func() {
		p.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	if n := p.errLog.takeSuppressed(); n > 0 {
		p.log.Error("kafka produce failures were not logged individually", "count", n)
	}
	return err
}

func orBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// logLimiter lets one event through per interval and counts the rest.
type logLimiter struct {
	interval time.Duration
	now      func() time.Time

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

// allow reports whether an event may be logged now and, if so, how many
// events were suppressed since the last one that was logged.
func (l *logLimiter) allow() (bool, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if !l.last.IsZero() && now.Sub(l.last) < l.interval {
		l.suppressed++
		return false, 0
	}
	n := l.suppressed
	l.last, l.suppressed = now, 0
	return true, n
}

// takeSuppressed returns the number of events suppressed since the last
// logged one and resets it.
func (l *logLimiter) takeSuppressed() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.suppressed
	l.suppressed = 0
	return n
}
