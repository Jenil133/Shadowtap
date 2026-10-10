package index

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/kafka"
)

// scopeName is the instrumentation scope of the indexer's own spans.
const scopeName = "github.com/Jenil133/Shadowtap/internal/index"

// errClientClosed ends the poll loop once the Kafka client is closed.
var errClientClosed = fmt.Errorf("index: kafka client closed: %w", kgo.ErrClientClosed)

const (
	// errorLogInterval bounds how often repeating failures are logged; the
	// metrics count every one of them.
	errorLogInterval = 10 * time.Second
	// maxBatchLinks caps the producer spans a batch span links to; it is
	// the SDK's default link limit, beyond which links are dropped anyway.
	maxBatchLinks = 128
)

// recordKind says what a topic carries.
type recordKind int

const (
	kindUnknown recordKind = iota
	kindCaptures
	kindDiffs
	kindSpans
	kindLogs
)

// Options configures New. Every field is optional.
type Options struct {
	// Registerer receives the indexer's metrics; nil registers nothing.
	Registerer prometheus.Registerer
	// Logger is the process logger; nil discards logs.
	Logger *slog.Logger
	// TracerProvider provides the indexer's own spans: one per batch, linked
	// to the producer spans the records' traceparent headers name, with a
	// child span per bulk request. Nil uses the global provider, which is a
	// no-op until telemetry.Setup installs one.
	TracerProvider trace.TracerProvider
	// Propagator reads the trace context from record headers. Nil uses the
	// global propagator, which telemetry.Setup makes W3C TraceContext.
	Propagator propagation.TextMapPropagator
	// Kafka holds extra franz-go options applied to the consumer last.
	Kafka []kgo.Opt
}

// Indexer consumes the capture, diff, span and log topics as one consumer
// group member and writes their documents to Elasticsearch.
//
// Delivery is at least once: a batch of records is polled, mapped to
// documents, written in bulk until Elasticsearch has acknowledged every
// document (created, or already present from an earlier delivery), its
// failures are written to the dead-letter topic, and only then are its
// offsets committed. Rebalances are blocked while a batch is in flight, so
// offsets are never committed for partitions another member took over.
// A crash between the bulk write and the commit redelivers the batch, whose
// deterministic document ids then conflict instead of duplicating.
type Indexer struct {
	cfg        Config
	name       string
	log        *slog.Logger
	es         *elasticsearch.Client
	transport  *http.Transport
	client     *kgo.Client
	dlq        *kafka.Writer
	bulk       *bulkWriter
	metrics    *metrics
	kinds      map[string]recordKind
	errLog     *logLimiter
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	hooks      hooks

	// uncommitted holds offsets of processed batches whose commit failed,
	// to retry before the partitions are given up.
	mu          sync.Mutex
	uncommitted map[string]map[int32]kgo.EpochOffset

	closeOnce sync.Once
}

// hooks lets tests observe and interrupt the batch cycle.
type hooks struct {
	// beforeCommit runs after a batch is acknowledged and dead-lettered,
	// before its offsets are committed. An error stops the indexer without
	// the commit: a simulated crash.
	beforeCommit func(records []*kgo.Record) error
	// afterBatch runs once a batch is committed.
	afterBatch func(records []*kgo.Record)
}

// New validates cfg and wires an indexer: the Elasticsearch client, the
// consumer group client and the dead-letter producer. Nothing connects
// until Run.
func New(cfg Config, opts Options) (*Indexer, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	name := cfg.Name
	if name == "" {
		name, _ = os.Hostname()
	}
	tp := opts.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	prop := opts.Propagator
	if prop == nil {
		prop = otel.GetTextMapPropagator()
	}
	m := newMetrics()
	if opts.Registerer != nil {
		if err := opts.Registerer.Register(m); err != nil {
			return nil, fmt.Errorf("index: register metrics: %w", err)
		}
	}
	unregister := func() {
		if opts.Registerer != nil {
			opts.Registerer.Unregister(m)
		}
	}

	es, transport, err := newElasticsearchClient(cfg.Elasticsearch)
	if err != nil {
		unregister()
		return nil, err
	}
	x := &Indexer{
		cfg:        cfg,
		name:       name,
		log:        log,
		es:         es,
		transport:  transport,
		metrics:    m,
		errLog:     &logLimiter{interval: errorLogInterval},
		tracer:     tp.Tracer(scopeName),
		propagator: prop,
		kinds: map[string]recordKind{
			cfg.Kafka.Topics.Captures: kindCaptures,
			cfg.Kafka.Topics.Diffs:    kindDiffs,
			cfg.Kafka.Topics.Spans:    kindSpans,
			cfg.Kafka.Topics.Logs:     kindLogs,
		},
		uncommitted: map[string]map[int32]kgo.EpochOffset{},
	}
	x.bulk = &bulkWriter{
		es:      es,
		timeout: time.Duration(cfg.Elasticsearch.RequestTimeoutMS) * time.Millisecond,
		retry:   cfg.Retry,
		metrics: m,
		log:     log,
		errLog:  x.errLog,
		tracer:  x.tracer,
	}

	kopts, err := kafka.ClientOptions(cfg.Kafka.clientConfig(cfg.Kafka.Topics.DLQ))
	if err != nil {
		unregister()
		transport.CloseIdleConnections()
		return nil, err
	}
	start := kgo.NewOffset().AtStart()
	if cfg.Kafka.StartOffset == "latest" {
		start = kgo.NewOffset().AtEnd()
	}
	kopts = append(kopts,
		kgo.ConsumerGroup(cfg.Kafka.Group),
		kgo.ConsumeTopics(cfg.Kafka.Topics.Captures, cfg.Kafka.Topics.Diffs, cfg.Kafka.Topics.Spans, cfg.Kafka.Topics.Logs),
		kgo.ConsumeStartOffset(start),
		// Offsets are committed by process, after Elasticsearch acknowledged
		// the batch; rebalances wait for that so a commit never lands on a
		// partition another member already owns.
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxBytes(int32(min(cfg.Kafka.FetchMaxBytes, math.MaxInt32))),
		kgo.SessionTimeout(time.Duration(cfg.Kafka.SessionTimeoutMS)*time.Millisecond),
		kgo.RebalanceTimeout(time.Duration(cfg.Kafka.RebalanceTimeoutMS)*time.Millisecond),
		kgo.OnPartitionsAssigned(x.onAssigned),
		kgo.OnPartitionsRevoked(x.onRevoked),
		kgo.OnPartitionsLost(x.onLost),
		kgo.WithLogger(kgoLogger{log: log.With("component", "kafka")}),
	)
	client, err := kgo.NewClient(append(kopts, opts.Kafka...)...)
	if err != nil {
		unregister()
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("index: create consumer: %w", err)
	}
	dlq, err := kafka.NewWriter(cfg.Kafka.clientConfig(cfg.Kafka.Topics.DLQ))
	if err != nil {
		client.Close()
		unregister()
		transport.CloseIdleConnections()
		return nil, err
	}
	x.client, x.dlq = client, dlq
	return x, nil
}

// Run bootstraps Elasticsearch (unless disabled), joins the consumer group
// and indexes until ctx is done, then finishes the batch in flight within
// the shutdown timeout, commits it, leaves the group and releases every
// connection. It returns nil after a clean stop and an error when the
// indexer cannot continue safely: a dead-letter record could not be
// delivered, or a test hook asked for a crash.
func (x *Indexer) Run(ctx context.Context) error {
	defer x.Close()
	if x.cfg.Elasticsearch.Bootstrap {
		if err := x.bootstrap(ctx); err != nil {
			return err
		}
	}
	x.log.Info("consuming",
		"group", x.cfg.Kafka.Group,
		"topics", []string{x.cfg.Kafka.Topics.Captures, x.cfg.Kafka.Topics.Diffs, x.cfg.Kafka.Topics.Spans, x.cfg.Kafka.Topics.Logs},
		"dlq", x.cfg.Kafka.Topics.DLQ,
		"elasticsearch", x.cfg.Elasticsearch.URLs,
	)
	for {
		recs, pollErr := x.poll(ctx)
		if len(recs) > 0 {
			bctx, cancel := x.batchContext(ctx)
			err := x.process(bctx, recs)
			cancel()
			if err != nil {
				x.client.AllowRebalance()
				if ctx.Err() != nil {
					// Stopped before the batch was acknowledged: nothing was
					// committed, so the batch is redelivered to the next
					// owner of its partitions.
					x.log.Warn("batch abandoned during shutdown; it will be redelivered", "records", len(recs), "err", err)
					return nil
				}
				return err
			}
		}
		x.client.AllowRebalance()
		if pollErr != nil {
			if ctx.Err() != nil || errors.Is(pollErr, kgo.ErrClientClosed) {
				x.log.Info("stopped consuming")
				return nil
			}
			return pollErr
		}
	}
}

// bootstrap applies the templates, retrying with backoff while
// Elasticsearch is unavailable: the indexer stays up, not ready, until it
// can write.
func (x *Indexer) bootstrap(ctx context.Context) error {
	// Bootstrap makes about a dozen requests; give each the request timeout.
	budget := 12 * time.Duration(x.cfg.Elasticsearch.RequestTimeoutMS) * time.Millisecond
	for attempt := 1; ; attempt++ {
		bctx, cancel := context.WithTimeout(ctx, budget)
		err := Bootstrap(bctx, x.es, x.cfg.Indices)
		cancel()
		if err == nil {
			x.metrics.bootstraps.WithLabelValues("ok").Inc()
			x.log.Info("elasticsearch bootstrapped", "indices", x.cfg.Indices.All())
			return nil
		}
		x.metrics.bootstraps.WithLabelValues("error").Inc()
		if ctx.Err() != nil {
			return fmt.Errorf("index: bootstrap: %w", ctx.Err())
		}
		x.log.Error("elasticsearch bootstrap failed; retrying", "err", err, "attempt", attempt)
		if err := sleepBackoff(ctx, attempt, x.cfg.Retry); err != nil {
			return fmt.Errorf("index: bootstrap: %w", err)
		}
	}
}

// poll gathers one batch: it waits for the first records, then keeps
// polling until the batch holds Batch.MaxRecords records or Batch.MaxBytes
// of record data, or Batch.MaxIntervalMS has passed since the first record.
// Records are returned together with ctx's error when it ends mid-batch, so
// the caller can still flush them.
func (x *Indexer) poll(ctx context.Context) ([]*kgo.Record, error) {
	var (
		recs     []*kgo.Record
		size     int
		deadline time.Time
	)
	maxRecords, maxBytes := x.cfg.Batch.MaxRecords, x.cfg.Batch.MaxBytes
	interval := time.Duration(x.cfg.Batch.MaxIntervalMS) * time.Millisecond
	for {
		pctx, cancel := ctx, context.CancelFunc(func() {})
		if !deadline.IsZero() {
			pctx, cancel = context.WithDeadline(ctx, deadline)
		}
		fetches := x.client.PollRecords(pctx, maxRecords-len(recs))
		cancel()
		if fetches.IsClientClosed() {
			return recs, errClientClosed
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			switch {
			case ctx.Err() != nil && errors.Is(err, ctx.Err()):
				// Stopping; Run handles it.
			case pctx.Err() != nil && errors.Is(err, pctx.Err()):
				// The batch interval elapsed: expected.
			default:
				x.metrics.fetchErrors.Inc()
				if ok, suppressed := x.errLog.allow(time.Now()); ok {
					x.log.Warn("fetch error", "topic", topic, "partition", partition, "err", err, "suppressed_logs", suppressed)
				}
			}
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			n := len(p.Records)
			if n == 0 {
				return
			}
			x.metrics.setLag(p.Topic, p.Partition, p.HighWatermark-(p.Records[n-1].Offset+1))
			for _, r := range p.Records {
				recs = append(recs, r)
				size += len(r.Key) + len(r.Value)
			}
		})
		if err := ctx.Err(); err != nil {
			return recs, err
		}
		if len(recs) == 0 {
			continue
		}
		if deadline.IsZero() {
			deadline = time.Now().Add(interval)
		}
		if len(recs) >= maxRecords || size >= maxBytes || !time.Now().Before(deadline) {
			return recs, nil
		}
	}
}

// batchContext returns the context a batch runs under. It is not cancelled
// when ctx is: a stop request gives the batch the shutdown timeout to finish
// and commit, so a graceful shutdown loses no acknowledged work and
// redelivers as little as possible.
func (x *Indexer) batchContext(ctx context.Context) (context.Context, context.CancelFunc) {
	bctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
			return
		case <-ctx.Done():
		}
		timer := time.NewTimer(time.Duration(x.cfg.ShutdownTimeoutMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			cancel()
		}
	}()
	var once sync.Once
	return bctx, func() {
		once.Do(func() { close(done) })
		cancel()
	}
}

// process indexes one batch: decode, bulk write, dead-letter, commit. The
// batch is one consumer span linked to the producer spans of its records.
func (x *Indexer) process(ctx context.Context, recs []*kgo.Record) (err error) {
	x.metrics.batchRecords.Observe(float64(len(recs)))
	ctx, span := x.startBatchSpan(ctx, recs)
	defer func() { endSpan(span, err) }()

	var (
		docs     []*Document
		failures []failure
		// owner is the record each document came from, so a failure can be
		// dead-lettered with the record's coordinates.
		owner = make(map[*Document]*kgo.Record, len(recs))
	)
	for _, rec := range recs {
		x.metrics.consumed.WithLabelValues(rec.Topic).Inc()
		d, f := x.decodeRecord(rec)
		for _, doc := range d {
			owner[doc] = rec
		}
		docs = append(docs, d...)
		failures = append(failures, f...)
	}
	x.metrics.batchDocuments.Observe(float64(len(docs)))

	created, duplicates := 0, 0
	if len(docs) > 0 {
		outcomes, err := x.bulk.index(ctx, docs)
		if err != nil {
			return err
		}
		for _, o := range outcomes {
			switch o.result {
			case resultCreated:
				created++
			case resultDuplicate:
				duplicates++
			}
			if o.result != "" {
				x.metrics.documents.WithLabelValues(o.doc.Source.Topic, o.doc.Index, o.result).Inc()
				continue
			}
			x.metrics.documents.WithLabelValues(o.doc.Source.Topic, o.doc.Index, resultDLQ).Inc()
			failures = append(failures, failure{
				rec:      owner[o.doc],
				src:      o.doc.Source,
				stage:    StageIndex,
				err:      fmt.Errorf("elasticsearch rejected the document with status %d: %s", o.status, o.errMsg),
				status:   o.status,
				errType:  o.errType,
				attempts: o.attempts,
				index:    o.doc.Index,
				docID:    o.doc.ID,
				body:     o.doc.Body,
			})
		}
	}
	span.SetAttributes(
		attribute.Int("shadowtap.index.documents", len(docs)),
		attribute.Int("shadowtap.index.created", created),
		attribute.Int("shadowtap.index.duplicates", duplicates),
		attribute.Int("shadowtap.index.dead_letters", len(failures)),
	)
	if len(failures) > 0 {
		if err := x.deadLetter(ctx, failures); err != nil {
			return err
		}
	}
	if x.hooks.beforeCommit != nil {
		if err := x.hooks.beforeCommit(recs); err != nil {
			return err
		}
	}
	x.commit(ctx, recs)
	span.AddEvent("committed")
	x.log.DebugContext(ctx, "batch indexed", "records", len(recs), "documents", len(docs), "created", created, "duplicates", duplicates, "dead_letters", len(failures))
	if x.hooks.afterBatch != nil {
		x.hooks.afterBatch(recs)
	}
	return nil
}

// startBatchSpan starts the consumer span of a batch, linked to the spans
// that produced its records (the traceparent record headers the tap, the
// replayer and the Collector write) so the index step shows up next to the
// capture or replay it belongs to.
func (x *Indexer) startBatchSpan(ctx context.Context, recs []*kgo.Record) (context.Context, trace.Span) {
	var links []trace.Link
	seen := map[string]bool{}
	for _, rec := range recs {
		sc := trace.SpanContextFromContext(x.recordContext(context.Background(), headersOf(rec)))
		if !sc.IsValid() {
			continue
		}
		key := sc.TraceID().String() + sc.SpanID().String()
		if seen[key] {
			continue
		}
		seen[key] = true
		links = append(links, trace.Link{SpanContext: sc, Attributes: []attribute.KeyValue{
			semconv.MessagingDestinationName(rec.Topic),
			semconv.MessagingDestinationPartitionID(strconv.Itoa(int(rec.Partition))),
			semconv.MessagingKafkaOffset(int(rec.Offset)),
		}})
		if len(links) == maxBatchLinks {
			break
		}
	}
	return x.tracer.Start(ctx, "shadowtap.index.batch",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithLinks(links...),
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingOperationTypeProcess,
			semconv.MessagingConsumerGroupName(x.cfg.Kafka.Group),
			semconv.MessagingBatchMessageCount(len(recs)),
		))
}

// recordContext returns ctx carrying the trace context of a record's
// headers, read by the propagator; ctx unchanged when there is none. Log
// records written under the result carry the producer's trace id.
func (x *Indexer) recordContext(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return x.propagator.Extract(ctx, propagation.MapCarrier(headers))
}

// decodeRecord maps one record to its documents, by topic.
func (x *Indexer) decodeRecord(rec *kgo.Record) ([]*Document, []failure) {
	src := sourceOf(rec)
	decodeFailure := func(err error) []failure {
		return []failure{{rec: rec, src: src, stage: StageDecode, err: err, value: true}}
	}
	switch x.kinds[rec.Topic] {
	case kindCaptures:
		xch, err := DecodeCapture(rec.Value)
		if err != nil {
			return nil, decodeFailure(err)
		}
		doc, err := CaptureDocument(xch, src, x.cfg.Indices.Captures)
		if err != nil {
			return nil, decodeFailure(err)
		}
		return []*Document{doc}, nil
	case kindDiffs:
		r, err := DecodeDiff(rec.Value)
		if err != nil {
			return nil, decodeFailure(err)
		}
		doc, err := DiffDocument(r, src, x.cfg.Indices.Diffs)
		if err != nil {
			return nil, decodeFailure(err)
		}
		return []*Document{doc}, nil
	case kindSpans:
		req, err := DecodeSpans(rec.Value)
		if err != nil {
			return nil, decodeFailure(err)
		}
		docs, rejected := SpanDocuments(req, src, x.cfg.Indices.Spans)
		return docs, rejections(rec, src, x.cfg.Indices.Spans, rejected)
	case kindLogs:
		req, err := DecodeLogs(rec.Value)
		if err != nil {
			return nil, decodeFailure(err)
		}
		docs, rejected := LogDocuments(req, src, x.cfg.Indices.Logs)
		return docs, rejections(rec, src, x.cfg.Indices.Logs, rejected)
	default:
		return nil, decodeFailure(fmt.Errorf("index: record from unexpected topic %q", rec.Topic))
	}
}

// rejections turns the parts of a record that could not be mapped into
// failures that keep their partial documents.
func rejections(rec *kgo.Record, src Source, index string, rejected []Rejected) []failure {
	if len(rejected) == 0 {
		return nil
	}
	out := make([]failure, 0, len(rejected))
	for _, r := range rejected {
		out = append(out, failure{rec: rec, src: src, stage: StageDecode, err: r.Err, index: index, body: r.Body})
	}
	return out
}

// deadLetter writes every failure to the DLQ topic and waits until the
// broker has them: a failure must be durable before its offset is
// committed, or it would vanish with the commit.
func (x *Indexer) deadLetter(ctx context.Context, failures []failure) error {
	now := time.Now()
	for _, f := range failures {
		value, err := f.dlqRecord(x.name, now, f.valueBudget(x.cfg.Kafka.MaxRecordBytes))
		if err != nil {
			return fmt.Errorf("index: encode dead-letter record: %w", err)
		}
		if err := x.dlq.Write(ctx, f.rec.Key, value, f.headers(), now); err != nil {
			return fmt.Errorf("index: write dead-letter record: %w", err)
		}
		x.metrics.dlq.WithLabelValues(f.src.Topic, f.stage).Inc()
		if ok, suppressed := x.errLog.allow(now); ok {
			// Logged under the record's own trace context, so the line
			// correlates with the request whose record was dropped.
			attrs := []any{
				"topic", f.src.Topic, "partition", f.src.Partition, "offset", f.src.Offset,
				"stage", f.stage, "index", f.index, "document_id", f.docID, "err", f.err, "suppressed_logs", suppressed,
			}
			rctx := x.recordContext(ctx, f.src.Headers)
			if id := f.src.traceID(); id != "" && !trace.SpanContextFromContext(rctx).IsValid() {
				attrs = append(attrs, "trace_id", id)
			}
			x.log.WarnContext(rctx, "record sent to the dead-letter topic", attrs...)
		}
	}
	if err := x.dlq.Flush(ctx); err != nil {
		return fmt.Errorf("index: dead-letter records not delivered: %w", err)
	}
	return nil
}

// commit commits the offsets after recs. A failed commit is not fatal: the
// documents are in Elasticsearch, the next commit covers these offsets too,
// and a redelivery after a crash only produces conflicts. The offsets are
// kept to retry before the partitions are revoked.
func (x *Indexer) commit(ctx context.Context, recs []*kgo.Record) {
	offsets := offsetsAfter(recs)
	if err := x.client.CommitRecords(ctx, recs...); err != nil {
		x.metrics.commits.WithLabelValues("error").Inc()
		x.log.WarnContext(ctx, "offset commit failed; will retry with the next batch", "err", err)
		x.mu.Lock()
		mergeOffsets(x.uncommitted, offsets)
		x.mu.Unlock()
		return
	}
	x.metrics.commits.WithLabelValues("ok").Inc()
	x.mu.Lock()
	forgetCommitted(x.uncommitted, offsets)
	x.mu.Unlock()
}

// offsetsAfter returns, per partition, the offset to commit after recs: one
// past the highest record.
func offsetsAfter(recs []*kgo.Record) map[string]map[int32]kgo.EpochOffset {
	out := map[string]map[int32]kgo.EpochOffset{}
	for _, r := range recs {
		parts := out[r.Topic]
		if parts == nil {
			parts = map[int32]kgo.EpochOffset{}
			out[r.Topic] = parts
		}
		next := kgo.EpochOffset{Epoch: r.LeaderEpoch, Offset: r.Offset + 1}
		if cur, ok := parts[r.Partition]; !ok || cur.Less(next) {
			parts[r.Partition] = next
		}
	}
	return out
}

// mergeOffsets adds src to dst, keeping the higher offset per partition.
func mergeOffsets(dst, src map[string]map[int32]kgo.EpochOffset) {
	for topic, parts := range src {
		if dst[topic] == nil {
			dst[topic] = map[int32]kgo.EpochOffset{}
		}
		for p, o := range parts {
			if cur, ok := dst[topic][p]; !ok || cur.Less(o) {
				dst[topic][p] = o
			}
		}
	}
}

// forgetCommitted drops from pending every offset that committed covers.
func forgetCommitted(pending, committed map[string]map[int32]kgo.EpochOffset) {
	for topic, parts := range committed {
		for p, o := range parts {
			if cur, ok := pending[topic][p]; ok && !o.Less(cur) {
				delete(pending[topic], p)
			}
		}
	}
}

// retryUncommitted commits offsets of processed batches whose commit
// failed, keeping only those for partitions in keep (all when nil).
func (x *Indexer) retryUncommitted(ctx context.Context, cl *kgo.Client, keep map[string][]int32) {
	x.mu.Lock()
	pending := map[string]map[int32]kgo.EpochOffset{}
	for topic, parts := range x.uncommitted {
		for p, o := range parts {
			if keep != nil && !containsPartition(keep, topic, p) {
				continue
			}
			if pending[topic] == nil {
				pending[topic] = map[int32]kgo.EpochOffset{}
			}
			pending[topic][p] = o
		}
	}
	x.mu.Unlock()
	if len(pending) == 0 {
		return
	}
	var commitErr error
	cl.CommitOffsetsSync(ctx, pending, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil && commitErr == nil {
					commitErr = fmt.Errorf("%s partition %d: %w", t.Topic, p.Partition, err)
				}
			}
		}
	})
	if commitErr != nil {
		x.metrics.commits.WithLabelValues("error").Inc()
		x.log.Warn("retrying earlier offset commit failed", "err", commitErr)
		return
	}
	x.metrics.commits.WithLabelValues("ok").Inc()
	x.mu.Lock()
	forgetCommitted(x.uncommitted, pending)
	x.mu.Unlock()
}

func containsPartition(m map[string][]int32, topic string, partition int32) bool {
	for _, p := range m[topic] {
		if p == partition {
			return true
		}
	}
	return false
}

// Group callbacks. With BlockRebalanceOnPoll they only run between batches,
// never while one is being processed.

func (x *Indexer) onAssigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
	x.metrics.rebalances.WithLabelValues(eventAssigned).Inc()
	x.log.Info("partitions assigned", "partitions", assigned)
}

func (x *Indexer) onRevoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	x.metrics.rebalances.WithLabelValues(eventRevoked).Inc()
	x.metrics.forgetLag(revoked)
	// Every processed batch was committed already unless its commit failed;
	// a last attempt lets the next owner start where this member stopped.
	x.retryUncommitted(ctx, cl, revoked)
	x.log.Info("partitions revoked", "partitions", revoked)
}

func (x *Indexer) onLost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
	x.metrics.rebalances.WithLabelValues(eventLost).Inc()
	x.metrics.forgetLag(lost)
	x.mu.Lock()
	for topic, ps := range lost {
		for _, p := range ps {
			delete(x.uncommitted[topic], p)
		}
	}
	x.mu.Unlock()
	x.log.Warn("partitions lost", "partitions", lost)
}

// Ready reports whether the indexer can do its job: it has joined the
// consumer group and Elasticsearch answers. It is cheap enough for a probe
// when cached (admin.NewReadiness).
func (x *Indexer) Ready(ctx context.Context) error {
	if _, generation := x.client.GroupMetadata(); generation < 0 {
		return errors.New("consumer group not joined")
	}
	res, err := x.es.Ping(x.es.Ping.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("elasticsearch: %w", err)
	}
	if status, body := drain(res); status < 200 || status >= 300 {
		return fmt.Errorf("elasticsearch: %w", &ESError{Status: status, Body: body})
	}
	return nil
}

// Close leaves the consumer group, delivers buffered dead-letter records
// and releases every connection. Run calls it; it is idempotent.
func (x *Indexer) Close() {
	x.closeOnce.Do(func() {
		// Close alone would wait for a rebalance this member may be blocking.
		x.client.CloseAllowingRebalance()
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(x.cfg.ShutdownTimeoutMS)*time.Millisecond)
		defer cancel()
		if err := x.dlq.Close(ctx); err != nil {
			x.log.Warn("closing the dead-letter producer", "err", err)
		}
		x.transport.CloseIdleConnections()
	})
}

// headersOf returns the record headers as a map; for a repeated key the
// first value wins.
func headersOf(r *kgo.Record) map[string]string {
	if len(r.Headers) == 0 {
		return nil
	}
	m := make(map[string]string, len(r.Headers))
	for _, h := range r.Headers {
		if _, seen := m[h.Key]; !seen {
			m[h.Key] = string(h.Value)
		}
	}
	return m
}

// kgoLogger routes the Kafka client's logs to slog. The client's info level
// is chatty (every metadata refresh), so it maps to debug.
type kgoLogger struct {
	log *slog.Logger
}

func (l kgoLogger) Level() kgo.LogLevel {
	if l.log.Enabled(context.Background(), slog.LevelDebug) {
		return kgo.LogLevelDebug
	}
	return kgo.LogLevelWarn
}

func (l kgoLogger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var lvl slog.Level
	switch level {
	case kgo.LogLevelError:
		lvl = slog.LevelError
	case kgo.LogLevelWarn:
		lvl = slog.LevelWarn
	default:
		lvl = slog.LevelDebug
	}
	l.log.Log(context.Background(), lvl, msg, keyvals...)
}
