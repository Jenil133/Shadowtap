package index

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/replay/report"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

type attributeKV = attribute.KeyValue

func tlsPair(cert, key string) kafka.TLSConfig {
	return kafka.TLSConfig{Enabled: true, CertFile: cert, KeyFile: key}
}

// traceparentOf renders a W3C traceparent header for the ids.
func traceparentOf(traceHex, spanHex string) string {
	return "00-" + traceHex + "-" + spanHex + "-01"
}

// produceCapture publishes x the way the tap does: keyed by trace id, with
// the capture headers and a traceparent.
func produceCapture(t *testing.T, brokers []string, x *capture.Exchange, traceparent string) *kgo.Record {
	t.Helper()
	headers := map[string]string{}
	for _, h := range x.RecordHeaders() {
		headers[h.Key] = string(h.Value)
	}
	if traceparent != "" {
		headers["traceparent"] = traceparent
	}
	return produce(t, brokers, capture.DefaultTopic, x.Key(), encodeExchange(t, x), headers, x.Timing.StartedAt)
}

// countRecords waits for committed batches until n records are committed and
// returns them.
func countRecords(t *testing.T, committed <-chan []*kgo.Record, n int) []*kgo.Record {
	t.Helper()
	var all []*kgo.Record
	for len(all) < n {
		all = append(all, receive(t, committed, "a committed batch")...)
	}
	if len(all) != n {
		t.Fatalf("committed %d records, want %d", len(all), n)
	}
	return all
}

func TestIndexerIndexesEveryTopic(t *testing.T) {
	cluster, es := newCluster(t), newFakeES(t)
	cfg := testConfig(cluster, es)
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	reg := prometheus.NewRegistry()
	x := newTestIndexer(t, cfg, Options{Registerer: reg, TracerProvider: tp, Propagator: propagation.TraceContext{}})
	committed := batchHooks(x)

	if err := x.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "not joined") {
		t.Errorf("Ready before Run = %v", err)
	}

	brokers := cluster.ListenAddrs()
	xch := testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart)
	produceCapture(t, brokers, xch, traceparentOf(fixtureTraceHex, fixtureParentHex))
	res := testDiffResult()
	diffHeaders := report.RecordHeaders(res)
	diffHeaders["traceparent"] = traceparentOf(fixtureLinkTrace, fixtureLinkSpan)
	produce(t, brokers, diff.DefaultTopic, []byte(res.CaptureID), encodeDiff(t, res), diffHeaders, res.SentAt)
	second := testSpan(t)
	second.SpanId = mustHex(t, fixtureLinkSpan)
	second.ParentSpanId = mustHex(t, fixtureSpanHex)
	produce(t, brokers, DefaultSpansTopic, nil, mustProto(t, spansRequest(&commonv1.InstrumentationScope{Name: "otelhttp"}, testSpan(t), second)), nil, time.Now())
	produce(t, brokers, DefaultLogsTopic, nil, mustProto(t, logsRequest(nil, testLog(t))), nil, time.Now())

	stop := runIndexer(t, x)
	countRecords(t, committed, 4)

	if es.DocCount() != 5 {
		t.Errorf("stored %d documents, want 5", es.DocCount())
	}
	if _, ok := es.Doc(DefaultCapturesIndex, fixtureCaptureID); !ok {
		t.Errorf("capture not indexed: %v", es.Docs(DefaultCapturesIndex))
	}
	if _, ok := es.Doc(DefaultDiffsIndex, fixtureRunID+"-"+fixtureCaptureID); !ok {
		t.Errorf("diff not indexed: %v", es.Docs(DefaultDiffsIndex))
	}
	for _, id := range []string{fixtureTraceHex + "-" + fixtureSpanHex, fixtureTraceHex + "-" + fixtureLinkSpan} {
		if _, ok := es.Doc(DefaultSpansIndex, id); !ok {
			t.Errorf("span %s not indexed", id)
		}
	}
	if logs := es.Docs(DefaultLogsIndex); len(logs) != 1 {
		t.Errorf("logs indexed: %v", logs)
	}
	// The stored capture carries its Kafka provenance and the traceparent.
	raw, _ := es.Doc(DefaultCapturesIndex, fixtureCaptureID)
	doc := decodeDoc(t, raw)
	expectFields(t, doc, map[string]string{"kafka.topic": capture.DefaultTopic, "kafka.offset": "0", "propagation.trace.id": fixtureTraceHex, "trace.id": fixtureTraceHex})

	if err := x.Ready(context.Background()); err != nil {
		t.Errorf("Ready after joining = %v", err)
	}
	// Lag series exist while the partitions are owned; revocation drops them.
	if n := testutil.CollectAndCount(x.metrics, "shadowtap_indexer_consumer_lag"); n == 0 {
		t.Error("no consumer lag series")
	}
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := testutil.CollectAndCount(x.metrics, "shadowtap_indexer_consumer_lag"); n != 0 {
		t.Errorf("%d consumer lag series survive leaving the group", n)
	}

	got := committedOffsets(t, brokers, cfg.Kafka.Group)
	want := nonEmpty(endOffsets(t, brokers, capture.DefaultTopic, diff.DefaultTopic, DefaultSpansTopic, DefaultLogsTopic))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("committed offsets %v, want %v", got, want)
	}

	// Metrics: one record per topic, documents by index, commits and bulks.
	for _, topic := range []string{capture.DefaultTopic, diff.DefaultTopic, DefaultSpansTopic, DefaultLogsTopic} {
		if got := testutil.ToFloat64(x.metrics.consumed.WithLabelValues(topic)); got != 1 {
			t.Errorf("records consumed from %s = %v", topic, got)
		}
	}
	for topic, index := range map[string]string{capture.DefaultTopic: DefaultCapturesIndex, diff.DefaultTopic: DefaultDiffsIndex, DefaultLogsTopic: DefaultLogsIndex} {
		if got := testutil.ToFloat64(x.metrics.documents.WithLabelValues(topic, index, resultCreated)); got != 1 {
			t.Errorf("documents created from %s = %v", topic, got)
		}
	}
	if got := testutil.ToFloat64(x.metrics.documents.WithLabelValues(DefaultSpansTopic, DefaultSpansIndex, resultCreated)); got != 2 {
		t.Errorf("span documents created = %v", got)
	}
	if got := testutil.ToFloat64(x.metrics.commits.WithLabelValues("ok")); got < 1 {
		t.Errorf("commits ok = %v", got)
	}
	if got := testutil.ToFloat64(x.metrics.commits.WithLabelValues("error")); got != 0 {
		t.Errorf("commits error = %v", got)
	}
	if got := testutil.ToFloat64(x.metrics.bulkRequests.WithLabelValues(bulkOK)); got < 1 {
		t.Errorf("bulk ok = %v", got)
	}
	if got := testutil.ToFloat64(x.metrics.bootstraps.WithLabelValues("ok")); got != 1 {
		t.Errorf("bootstraps = %v", got)
	}
	if n := testutil.CollectAndCount(reg, "shadowtap_indexer_records_consumed_total"); n != 4 {
		t.Errorf("registry exposes %d consumed series, want 4", n)
	}
	if n := testutil.CollectAndCount(x.metrics, "shadowtap_indexer_batch_records", "shadowtap_indexer_bulk_seconds", "shadowtap_indexer_bulk_bytes", "shadowtap_indexer_batch_documents"); n != 4 {
		t.Errorf("histograms exposed = %d, want 4", n)
	}

	// Traces: a consumer span per batch, linked to the producers' spans, with
	// a client span per bulk request underneath.
	var batches, bulks int
	var linked []string
	for _, s := range recorder.Ended() {
		switch s.Name() {
		case "shadowtap.index.batch":
			batches++
			if s.SpanKind() != trace.SpanKindConsumer {
				t.Errorf("batch span kind = %v", s.SpanKind())
			}
			for _, l := range s.Links() {
				linked = append(linked, l.SpanContext.TraceID().String()+"/"+l.SpanContext.SpanID().String())
			}
			if !hasAttribute(s.Attributes(), "messaging.consumer.group.name", cfg.Kafka.Group) || !hasAttribute(s.Attributes(), "messaging.system", "kafka") {
				t.Errorf("batch span attributes = %v", s.Attributes())
			}
		case "elasticsearch bulk":
			bulks++
			if s.SpanKind() != trace.SpanKindClient || !hasAttribute(s.Attributes(), "http.response.status_code", int64(200)) {
				t.Errorf("bulk span = kind %v attributes %v", s.SpanKind(), s.Attributes())
			}
		}
	}
	if batches == 0 || bulks == 0 {
		t.Errorf("spans: %d batches, %d bulks", batches, bulks)
	}
	for _, want := range []string{fixtureTraceHex + "/" + fixtureParentHex, fixtureLinkTrace + "/" + fixtureLinkSpan} {
		found := false
		for _, l := range linked {
			found = found || l == want
		}
		if !found {
			t.Errorf("no batch span links to %s; links %v", want, linked)
		}
	}
}

func hasAttribute(attrs []attributeKV, key string, want any) bool {
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.AsInterface() == want {
			return true
		}
	}
	return false
}

func TestIndexerDoesNotCommitBeforeAcknowledgement(t *testing.T) {
	cluster, es := newCluster(t), newFakeES(t)
	cfg := testConfig(cluster, es)
	brokers := cluster.ListenAddrs()
	release := es.Hold()
	x := newTestIndexer(t, cfg, Options{})
	committed := batchHooks(x)
	produceCapture(t, brokers, testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart), "")

	stop := runIndexer(t, x)
	awaitArrival(t, es)
	// Elasticsearch has the request but has not answered: nothing may be
	// committed yet.
	if got := committedOffsets(t, brokers, cfg.Kafka.Group); len(got[capture.DefaultTopic]) != 0 {
		t.Errorf("offsets committed before acknowledgement: %v", got)
	}
	if es.DocCount() != 0 {
		t.Errorf("stored %d documents while held", es.DocCount())
	}
	release()
	countRecords(t, committed, 1)
	if es.DocCount() != 1 {
		t.Errorf("stored %d documents after release", es.DocCount())
	}
	got := committedOffsets(t, brokers, cfg.Kafka.Group)
	want := nonEmpty(endOffsets(t, brokers, capture.DefaultTopic))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("committed offsets %v, want %v", got, want)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexerRedeliveryAfterCrashDoesNotDuplicate(t *testing.T) {
	cluster, es := newCluster(t), newFakeES(t)
	cfg := testConfig(cluster, es)
	brokers := cluster.ListenAddrs()
	ids := []string{"0199b3f0-0000-7000-8000-000000000001", "0199b3f0-0000-7000-8000-000000000002", "0199b3f0-0000-7000-8000-000000000003"}
	for i, id := range ids {
		produceCapture(t, brokers, testExchange(id, strings.Repeat(strconv.Itoa(i+1), 32), fixtureStart.Add(time.Duration(i)*time.Second)), "")
	}

	// The first indexer dies after Elasticsearch acknowledged its first batch
	// but before the commit.
	errCrash := errors.New("simulated crash")
	first := newTestIndexer(t, cfg, Options{})
	first.hooks.beforeCommit = func([]*kgo.Record) error { return errCrash }
	if err := waitRun(t, first); !errors.Is(err, errCrash) {
		t.Fatalf("Run = %v, want the crash", err)
	}
	indexed := es.DocCount()
	if indexed == 0 {
		t.Fatal("the crashed indexer stored nothing")
	}
	if got := committedOffsets(t, brokers, cfg.Kafka.Group); len(got[capture.DefaultTopic]) != 0 {
		t.Errorf("offsets committed despite the crash: %v", got)
	}
	if got := testutil.ToFloat64(first.metrics.commits.WithLabelValues("ok")); got != 0 {
		t.Errorf("crashed indexer counted %v commits", got)
	}

	// The next member gets the batch again; the known ids conflict.
	second := newTestIndexer(t, cfg, Options{})
	committed := batchHooks(second)
	stop := runIndexer(t, second)
	countRecords(t, committed, len(ids))
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if es.DocCount() != len(ids) {
		t.Errorf("stored %d documents, want %d", es.DocCount(), len(ids))
	}
	for _, id := range ids {
		if _, ok := es.Doc(DefaultCapturesIndex, id); !ok {
			t.Errorf("capture %s missing", id)
		}
	}
	dups := testutil.ToFloat64(second.metrics.documents.WithLabelValues(capture.DefaultTopic, DefaultCapturesIndex, resultDuplicate))
	created := testutil.ToFloat64(second.metrics.documents.WithLabelValues(capture.DefaultTopic, DefaultCapturesIndex, resultCreated))
	if int(dups) != indexed || int(created) != len(ids)-indexed {
		t.Errorf("second indexer: %v duplicates and %v created; the first indexed %d", dups, created, indexed)
	}
	got := committedOffsets(t, brokers, cfg.Kafka.Group)
	want := nonEmpty(endOffsets(t, brokers, capture.DefaultTopic))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("committed offsets %v, want %v", got, want)
	}
}

func TestIndexerDeadLetters(t *testing.T) {
	cluster, es := newCluster(t), newFakeES(t)
	cfg := testConfig(cluster, es)
	cfg.Retry.MaxAttempts = 2
	brokers := cluster.ListenAddrs()
	const rejectedID = "0199b3f0-0000-7000-8000-00000000dead"
	const throttledID = "0199b3f0-0000-7000-8000-000000000429"
	es.SetItemStatus(func(_, id string, _ int) int {
		switch id {
		case rejectedID:
			return http.StatusInternalServerError
		case throttledID:
			return http.StatusTooManyRequests
		}
		return 0
	})
	// The process logger of a real deployment: telemetry's handler adds the
	// trace id of the record context the warning is logged under.
	var logs syncBuffer
	providers, err := telemetry.Setup(context.Background(), telemetry.DefaultConfig(), slog.NewJSONHandler(&logs, nil))
	if err != nil {
		t.Fatal(err)
	}
	x := newTestIndexer(t, cfg, Options{Logger: providers.Logger, Propagator: propagation.TraceContext{}})
	x.errLog.interval = 0 // log every dead letter
	committed := batchHooks(x)

	garbage := produce(t, brokers, capture.DefaultTopic, []byte("k1"), []byte("not a capture"), map[string]string{"content-type": "application/json", "trace-id": fixtureLinkTrace}, fixtureStart)
	rejected := produceCapture(t, brokers, testExchange(rejectedID, fixtureTraceHex, fixtureStart), traceparentOf(fixtureTraceHex, fixtureParentHex))
	throttled := produceCapture(t, brokers, testExchange(throttledID, fixtureLinkTrace, fixtureStart), "")
	bad := testSpan(t)
	bad.SpanId = make([]byte, 8)
	spans := produce(t, brokers, DefaultSpansTopic, nil, mustProto(t, spansRequest(nil, bad, testSpan(t))), nil, time.Now())
	fine := produceCapture(t, brokers, testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart), "")

	stop := runIndexer(t, x)
	countRecords(t, committed, 5)
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	// The good documents are in; the failures are dead-lettered and all
	// offsets, failures included, are committed.
	if _, ok := es.Doc(DefaultCapturesIndex, fixtureCaptureID); !ok {
		t.Error("the valid capture was not indexed")
	}
	if _, ok := es.Doc(DefaultSpansIndex, fixtureTraceHex+"-"+fixtureSpanHex); !ok {
		t.Error("the valid span of the partly invalid record was not indexed")
	}
	if es.DocCount() != 2 {
		t.Errorf("stored %d documents, want 2", es.DocCount())
	}
	got := committedOffsets(t, brokers, cfg.Kafka.Group)
	want := nonEmpty(endOffsets(t, brokers, capture.DefaultTopic, DefaultSpansTopic))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("committed offsets %v, want %v", got, want)
	}

	records := readTopic(t, brokers, DefaultDLQTopic, 4)
	byOffset := map[string]*kgo.Record{}
	dlq := map[string]DLQRecord{}
	for _, r := range records {
		var d DLQRecord
		if err := json.Unmarshal(r.Value, &d); err != nil {
			t.Fatalf("DLQ record is not JSON: %v\n%s", err, r.Value)
		}
		key := d.Topic + "/" + strconv.Itoa(int(d.Partition)) + "/" + strconv.FormatInt(d.Offset, 10)
		dlq[key] = d
		byOffset[key] = r
	}
	locate := func(r *kgo.Record) string {
		return r.Topic + "/" + strconv.Itoa(int(r.Partition)) + "/" + strconv.FormatInt(r.Offset, 10)
	}

	d, ok := dlq[locate(garbage)]
	if !ok {
		t.Fatalf("no DLQ record for the garbage record; have %v", dlq)
	}
	if d.Stage != StageDecode || !strings.Contains(d.Error, "capture: decode") || string(d.Value) != "not a capture" || d.Key != "k1" ||
		d.Indexer != "indexer-test" || d.TraceID != fixtureLinkTrace || d.Headers["content-type"] != "application/json" || d.Document != nil || d.SchemaVersion != DLQSchemaVersion {
		t.Errorf("garbage DLQ record = %+v", d)
	}
	r := byOffset[locate(garbage)]
	if string(r.Key) != "k1" {
		t.Errorf("DLQ record key = %q, want the original key", r.Key)
	}
	headers := headersOf(r)
	if headers[HeaderSourceTopic] != capture.DefaultTopic || headers[HeaderSourcePartition] != strconv.Itoa(int(garbage.Partition)) ||
		headers[HeaderSourceOffset] != strconv.FormatInt(garbage.Offset, 10) || headers[HeaderStage] != StageDecode {
		t.Errorf("DLQ record headers = %v", headers)
	}

	d, ok = dlq[locate(rejected)]
	if !ok {
		t.Fatalf("no DLQ record for the rejected document; have %v", dlq)
	}
	if d.Stage != StageIndex || d.Status != 500 || d.ErrorType != "internal_server_error" || !strings.Contains(d.Error, "status 500") ||
		!strings.Contains(d.Error, "simulated internal error") || d.Index != DefaultCapturesIndex || d.DocumentID != rejectedID || d.Attempts != 1 ||
		d.TraceID != fixtureTraceHex || d.Value != nil {
		t.Errorf("rejected DLQ record = %+v", d)
	}
	if doc := decodeDoc(t, d.Document); str(doc, "capture.id") != rejectedID {
		t.Errorf("DLQ record document = %s", d.Document)
	}

	d, ok = dlq[locate(throttled)]
	if !ok {
		t.Fatalf("no DLQ record for the throttled document; have %v", dlq)
	}
	if d.Stage != StageIndex || d.Status != 429 || d.Attempts != 2 || d.ErrorType != "es_rejected_execution_exception" {
		t.Errorf("throttled DLQ record = %+v", d)
	}

	d, ok = dlq[locate(spans)]
	if !ok {
		t.Fatalf("no DLQ record for the invalid span; have %v", dlq)
	}
	if d.Stage != StageDecode || !strings.Contains(d.Error, "no valid span id") || d.Index != DefaultSpansIndex || len(d.Document) == 0 || d.Value != nil {
		t.Errorf("span DLQ record = %+v", d)
	}
	if _, ok := dlq[locate(fine)]; ok {
		t.Error("the valid capture was dead-lettered")
	}

	for stage, want := range map[string]float64{StageDecode: 1, StageIndex: 2} {
		if got := testutil.ToFloat64(x.metrics.dlq.WithLabelValues(capture.DefaultTopic, stage)); got != want {
			t.Errorf("dlq %s from captures = %v, want %v", stage, got, want)
		}
	}
	if got := testutil.ToFloat64(x.metrics.dlq.WithLabelValues(DefaultSpansTopic, StageDecode)); got != 1 {
		t.Errorf("dlq decode from spans = %v", got)
	}
	if got := testutil.ToFloat64(x.metrics.documents.WithLabelValues(capture.DefaultTopic, DefaultCapturesIndex, resultDLQ)); got != 2 {
		t.Errorf("documents dlq = %v", got)
	}
	if got := testutil.ToFloat64(x.metrics.itemFailures.WithLabelValues(DefaultCapturesIndex, "429")); got != 2 {
		t.Errorf("429 item failures = %v, want one per attempt", got)
	}
	// Every dead letter is logged with the trace id of its record: from the
	// traceparent through the log handler for the rejected capture, from
	// the trace-id header as a fallback for the undecodable record.
	out := logs.String()
	if n := strings.Count(out, "record sent to the dead-letter topic"); n != 4 {
		t.Errorf("%d dead-letter warnings, want 4:\n%s", n, out)
	}
	for _, want := range []string{`"trace_id":"` + fixtureTraceHex + `"`, `"span_id":"` + fixtureParentHex + `"`, `"trace_id":"` + fixtureLinkTrace + `"`} {
		if !strings.Contains(out, want) {
			t.Errorf("logs lack %s:\n%s", want, out)
		}
	}
}

func TestIndexerShutdownFlushesAndCommits(t *testing.T) {
	cluster, es := newCluster(t), newFakeES(t)
	cfg := testConfig(cluster, es)
	brokers := cluster.ListenAddrs()
	release := es.Hold()
	var logs syncBuffer
	x := newTestIndexer(t, cfg, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	for i := range 2 {
		produceCapture(t, brokers, testExchange("0199b3f0-0000-7000-8000-00000000000"+strconv.Itoa(i), fixtureTraceHex, fixtureStart), "")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- x.Run(ctx) }()
	awaitArrival(t, es)
	// Stop while the batch waits on Elasticsearch, then let it answer: the
	// batch must be finished and committed, not abandoned.
	cancel()
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return")
	}
	if es.DocCount() != 2 {
		t.Errorf("stored %d documents, want the whole batch", es.DocCount())
	}
	got := committedOffsets(t, brokers, cfg.Kafka.Group)
	want := nonEmpty(endOffsets(t, brokers, capture.DefaultTopic))
	if !reflect.DeepEqual(got, want) {
		t.Errorf("committed offsets %v, want %v", got, want)
	}
	if !strings.Contains(logs.String(), "stopped consuming") || strings.Contains(logs.String(), "abandoned") {
		t.Errorf("logs:\n%s", logs.String())
	}
	// Closing again is harmless.
	x.Close()
}

func TestIndexerAbandonsBatchWhenShutdownTimesOut(t *testing.T) {
	cluster, es := newCluster(t), newFakeES(t)
	cfg := testConfig(cluster, es)
	cfg.ShutdownTimeoutMS = 300
	cfg.Elasticsearch.RequestTimeoutMS = 100
	brokers := cluster.ListenAddrs()
	release := es.Hold()
	defer release()
	var logs syncBuffer
	x := newTestIndexer(t, cfg, Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	produceCapture(t, brokers, testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart), "")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- x.Run(ctx) }()
	awaitArrival(t, es)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil: an abandoned batch is redelivered, not an error", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return")
	}
	if got := committedOffsets(t, brokers, cfg.Kafka.Group); len(got[capture.DefaultTopic]) != 0 {
		t.Errorf("offsets committed for an unacknowledged batch: %v", got)
	}
	if !strings.Contains(logs.String(), "abandoned") {
		t.Errorf("logs lack the abandon warning:\n%s", logs.String())
	}
}

func TestIndexerBootstrapRetriesUntilElasticsearchAnswers(t *testing.T) {
	cluster, es := newCluster(t), newFakeES(t)
	cfg := testConfig(cluster, es)
	// A 500 is not among the statuses the client retries by itself, so each
	// failure reaches the indexer's bootstrap loop.
	failures := 2
	es.SetRequestHook(func(r *http.Request) (int, string) {
		if strings.HasPrefix(r.URL.Path, "/_ilm/") && failures > 0 {
			failures--
			return http.StatusInternalServerError, `{"error":"starting up"}`
		}
		return 0, ""
	})
	x := newTestIndexer(t, cfg, Options{})
	committed := batchHooks(x)
	produceCapture(t, cluster.ListenAddrs(), testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart), "")
	stop := runIndexer(t, x)
	countRecords(t, committed, 1)
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(x.metrics.bootstraps.WithLabelValues("error")); got != 2 {
		t.Errorf("bootstrap errors = %v, want 2", got)
	}
	if got := testutil.ToFloat64(x.metrics.bootstraps.WithLabelValues("ok")); got != 1 {
		t.Errorf("bootstrap ok = %v, want 1", got)
	}
	if streams := es.DataStreams(); len(streams) != 4 {
		t.Errorf("data streams = %v", streams)
	}

	t.Run("bootstrap disabled", func(t *testing.T) {
		es := newFakeES(t)
		cfg := testConfig(cluster, es)
		cfg.Kafka.Group = "no-bootstrap"
		cfg.Elasticsearch.Bootstrap = false
		x := newTestIndexer(t, cfg, Options{})
		committed := batchHooks(x)
		stop := runIndexer(t, x)
		countRecords(t, committed, 1) // the capture produced above, from the start
		if err := stop(); err != nil {
			t.Fatal(err)
		}
		if n := len(es.Requests()); n != 0 {
			t.Errorf("%d bootstrap requests with bootstrap disabled", n)
		}
	})
	t.Run("bootstrap stops with the context", func(t *testing.T) {
		es := newFakeES(t)
		es.SetRequestHook(func(*http.Request) (int, string) { return http.StatusInternalServerError, `{}` })
		cfg := testConfig(cluster, es)
		x := newTestIndexer(t, cfg, Options{})
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if err := x.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Run = %v, want the context error", err)
		}
	})
}

func TestIndexerReadyReportsElasticsearch(t *testing.T) {
	cluster, es := newCluster(t), newFakeES(t)
	cfg := testConfig(cluster, es)
	x := newTestIndexer(t, cfg, Options{})
	committed := batchHooks(x)
	produceCapture(t, cluster.ListenAddrs(), testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart), "")
	stop := runIndexer(t, x)
	countRecords(t, committed, 1)
	if err := x.Ready(context.Background()); err != nil {
		t.Errorf("Ready = %v", err)
	}
	es.SetRequestHook(func(r *http.Request) (int, string) {
		if r.URL.Path == "/" {
			return http.StatusServiceUnavailable, `{"error":"red"}`
		}
		return 0, ""
	})
	if err := x.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "elasticsearch") {
		t.Errorf("Ready with Elasticsearch down = %v", err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
}

func TestNewErrors(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Kafka.Group = ""
	if _, err := New(cfg, Options{}); err == nil {
		t.Error("invalid config accepted")
	}
	cfg = DefaultConfig()
	reg := prometheus.NewRegistry()
	first, err := New(cfg, Options{Registerer: reg})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := New(cfg, Options{Registerer: reg}); err == nil || !strings.Contains(err.Error(), "register") {
		t.Errorf("second registration = %v", err)
	}
	cfg.Elasticsearch.TLS.CAFile = "/nonexistent/ca.pem"
	if _, err := New(cfg, Options{}); err == nil {
		t.Error("missing CA file accepted")
	}
	cfg = DefaultConfig()
	cfg.Kafka.TLS = tlsPair("/nonexistent/c.pem", "/nonexistent/k.pem")
	if _, err := New(cfg, Options{}); err == nil {
		t.Error("missing Kafka client certificate accepted")
	}
}

func TestOffsetBookkeeping(t *testing.T) {
	recs := []*kgo.Record{
		{Topic: "a", Partition: 0, Offset: 5, LeaderEpoch: 1},
		{Topic: "a", Partition: 0, Offset: 7, LeaderEpoch: 1},
		{Topic: "a", Partition: 1, Offset: 2, LeaderEpoch: 1},
		{Topic: "b", Partition: 0, Offset: 0, LeaderEpoch: 2},
	}
	got := offsetsAfter(recs)
	want := map[string]map[int32]kgo.EpochOffset{
		"a": {0: {Epoch: 1, Offset: 8}, 1: {Epoch: 1, Offset: 3}},
		"b": {0: {Epoch: 2, Offset: 1}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("offsetsAfter = %v, want %v", got, want)
	}
	pending := map[string]map[int32]kgo.EpochOffset{"a": {0: {Epoch: 1, Offset: 4}}}
	mergeOffsets(pending, got)
	if pending["a"][0].Offset != 8 || pending["a"][1].Offset != 3 || pending["b"][0].Offset != 1 {
		t.Errorf("mergeOffsets = %v", pending)
	}
	mergeOffsets(pending, map[string]map[int32]kgo.EpochOffset{"a": {0: {Epoch: 1, Offset: 6}}})
	if pending["a"][0].Offset != 8 {
		t.Errorf("mergeOffsets lowered an offset: %v", pending)
	}
	forgetCommitted(pending, map[string]map[int32]kgo.EpochOffset{"a": {0: {Epoch: 1, Offset: 8}, 1: {Epoch: 1, Offset: 2}}})
	if _, ok := pending["a"][0]; ok {
		t.Error("committed offset not forgotten")
	}
	if _, ok := pending["a"][1]; !ok {
		t.Error("a lower commit forgot a higher pending offset")
	}
	if !containsPartition(map[string][]int32{"a": {1, 2}}, "a", 2) || containsPartition(map[string][]int32{"a": {1}}, "b", 1) {
		t.Error("containsPartition")
	}
}

func TestKgoLogger(t *testing.T) {
	var buf syncBuffer
	l := kgoLogger{log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))}
	if l.Level() != kgo.LogLevelWarn {
		t.Errorf("Level = %v, want warn when debug is off", l.Level())
	}
	l.Log(kgo.LogLevelError, "boom", "k", "v")
	l.Log(kgo.LogLevelWarn, "careful")
	l.Log(kgo.LogLevelInfo, "chatty")
	out := buf.String()
	if !strings.Contains(out, "level=ERROR msg=boom k=v") || !strings.Contains(out, "level=WARN msg=careful") || strings.Contains(out, "chatty") {
		t.Errorf("log output:\n%s", out)
	}
	debug := kgoLogger{log: slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))}
	if debug.Level() != kgo.LogLevelDebug {
		t.Errorf("Level = %v, want debug", debug.Level())
	}
}
