package index

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	collogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/index/indextest"
)

// testPartitions is how many partitions every test topic has.
const testPartitions = 2

// testTopics are the topics the indexer consumes plus its dead-letter topic.
var testTopics = []string{capture.DefaultTopic, diff.DefaultTopic, DefaultSpansTopic, DefaultLogsTopic, DefaultDLQTopic}

// Identifiers shared by the fixtures.
const (
	fixtureTraceHex  = "4bf92f3577b34da6a3ce929d0e0e4736"
	fixtureSpanHex   = "00f067aa0ba902b7"
	fixtureParentHex = "b7ad6b7169203331"
	fixtureLinkTrace = "7d3e8a1b2c4f5e6d7a8b9c0d1e2f3a4b"
	fixtureLinkSpan  = "5c6d7e8f90112233"
	fixtureCaptureID = "0199b3f0-1234-7abc-8def-0123456789ab"
	fixtureRunID     = "run-0199b400-5e6f-7a8b-9c0d-ef0123456789"
)

// fixtureStart is a request start with nanosecond precision, so the tests
// see whether precision survives the mapping.
var fixtureStart = time.Date(2026, 10, 3, 9, 0, 0, 123456789, time.UTC)

// newCluster starts an in-memory single-broker Kafka with every test topic.
func newCluster(t *testing.T) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(testPartitions, testTopics...))
	if err != nil {
		t.Fatalf("start kfake cluster: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// newFakeES starts a fake Elasticsearch that is closed when the test ends.
func newFakeES(t *testing.T) *indextest.Server {
	t.Helper()
	s := indextest.NewServer()
	t.Cleanup(s.Close)
	return s
}

// testConfig is the default configuration pointed at c and es, with short
// batch, retry and shutdown timings.
func testConfig(c *kfake.Cluster, es *indextest.Server) Config {
	cfg := DefaultConfig()
	cfg.Name = "indexer-test"
	cfg.Kafka.Brokers = c.ListenAddrs()
	cfg.Elasticsearch.URLs = []string{es.URL()}
	cfg.Elasticsearch.RequestTimeoutMS = 2_000
	cfg.Batch.MaxIntervalMS = 50
	cfg.Retry = RetryConfig{MaxAttempts: 2, InitialBackoffMS: 10, MaxBackoffMS: 50}
	cfg.ShutdownTimeoutMS = 10_000
	return cfg
}

// newTestIndexer creates an indexer that is closed when the test ends.
func newTestIndexer(t *testing.T, cfg Config, opts Options) *Indexer {
	t.Helper()
	x, err := New(cfg, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(x.Close)
	return x
}

// batchHooks wires the indexer's test hooks to channels: committed receives
// the records of every committed batch.
func batchHooks(x *Indexer) (committed <-chan []*kgo.Record) {
	ch := make(chan []*kgo.Record, 64)
	x.hooks.afterBatch = func(recs []*kgo.Record) { ch <- recs }
	return ch
}

// runIndexer runs x in the background and returns a function that stops it
// and returns Run's error. The test fails if Run does not return in time.
func runIndexer(t *testing.T, x *Indexer) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- x.Run(ctx) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(30 * time.Second):
				t.Fatal("Run did not return after cancel")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// waitRun waits for Run to end on its own (a crash hook) and returns its error.
func waitRun(t *testing.T, x *Indexer) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- x.Run(context.Background()) }()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("Run did not return")
		return nil
	}
}

// receive waits for one batch on ch.
func receive(t *testing.T, ch <-chan []*kgo.Record, what string) []*kgo.Record {
	t.Helper()
	select {
	case recs := <-ch:
		return recs
	case <-time.After(30 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

// awaitArrival waits for one bulk request to reach the fake Elasticsearch.
func awaitArrival(t *testing.T, es *indextest.Server) {
	t.Helper()
	select {
	case <-es.Arrived():
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for a bulk request")
	}
}

// newKafkaClient returns a plain client of the cluster.
func newKafkaClient(t *testing.T, brokers []string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(brokers...)}, opts...)...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// produce writes one record and returns where it landed.
func produce(t *testing.T, brokers []string, topic string, key, value []byte, headers map[string]string, ts time.Time) *kgo.Record {
	t.Helper()
	cl := newKafkaClient(t, brokers)
	r := &kgo.Record{Topic: topic, Key: key, Value: value, Timestamp: ts}
	for k, v := range headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
	return r
}

// committedOffsets returns the group's committed offsets per topic and
// partition, as kadm reports them.
func committedOffsets(t *testing.T, brokers []string, group string) map[string]map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(newKafkaClient(t, brokers)).FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("FetchOffsets: %v", err)
	}
	if err := resp.Error(); err != nil {
		t.Fatalf("FetchOffsets: %v", err)
	}
	out := map[string]map[int32]int64{}
	resp.Offsets().Each(func(o kadm.Offset) {
		if o.At < 0 {
			return
		}
		if out[o.Topic] == nil {
			out[o.Topic] = map[int32]int64{}
		}
		out[o.Topic][o.Partition] = o.At
	})
	return out
}

// endOffsets returns the high watermark of every partition of topics.
func endOffsets(t *testing.T, brokers []string, topics ...string) map[string]map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listed, err := kadm.NewClient(newKafkaClient(t, brokers)).ListEndOffsets(ctx, topics...)
	if err != nil {
		t.Fatalf("ListEndOffsets: %v", err)
	}
	if err := listed.Error(); err != nil {
		t.Fatalf("ListEndOffsets: %v", err)
	}
	out := map[string]map[int32]int64{}
	listed.Each(func(o kadm.ListedOffset) {
		if out[o.Topic] == nil {
			out[o.Topic] = map[int32]int64{}
		}
		out[o.Topic][o.Partition] = o.Offset
	})
	return out
}

// nonEmpty drops partitions whose end offset is 0, so an end-offset map
// compares with a committed-offset map that only has committed partitions.
func nonEmpty(m map[string]map[int32]int64) map[string]map[int32]int64 {
	out := map[string]map[int32]int64{}
	for topic, parts := range m {
		for p, o := range parts {
			if o > 0 {
				if out[topic] == nil {
					out[topic] = map[int32]int64{}
				}
				out[topic][p] = o
			}
		}
	}
	return out
}

// readTopic reads n records from the start of topic.
func readTopic(t *testing.T, brokers []string, topic string, n int) []*kgo.Record {
	t.Helper()
	cl := newKafkaClient(t, brokers, kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := cl.PollFetches(ctx)
		if err := fetches.Err0(); err != nil {
			t.Fatalf("reading %s after %d of %d records: %v", topic, len(out), n, err)
		}
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

// testExchange returns a complete capture.
func testExchange(id, traceID string, started time.Time) *capture.Exchange {
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            id,
		Service:       "demo-api",
		TraceID:       traceID,
		ParentSpanID:  fixtureParentHex,
		RequestID:     "req-" + id,
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
			Path:      "/api/users/u_3f2a1b9c0d4e/orders?page=2&q=%3Cb%3E",
			Headers:   capture.Headers{"content-type": {"application/json"}, "accept": {"application/json", "text/plain"}},
		},
		Response: capture.Response{
			Status:  201,
			Headers: capture.Headers{"content-type": {"application/json; charset=utf-8"}},
		},
		Redactions: map[string]int{"email": 2, "phone": 1},
	}
	x.Request.Body = capture.Body{ContentType: "application/json", Size: 39}
	x.Request.Body.SetData([]byte(`{"email":"user-1a2b3c@redacted.invalid"}`))
	x.Response.Body = capture.Body{Size: 11, DecodedFrom: "gzip"}
	x.Response.Body.SetData([]byte(`{"id":"u1"}`))
	return x
}

// encodeExchange encodes x or fails the test.
func encodeExchange(t *testing.T, x *capture.Exchange) []byte {
	t.Helper()
	b, err := capture.Encode(x)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// testDiffResult returns a diff result with reported and suppressed
// differences.
func testDiffResult() *diff.Result {
	return &diff.Result{
		SchemaVersion: diff.SchemaVersion,
		RunID:         fixtureRunID,
		CaptureID:     fixtureCaptureID,
		Service:       "demo-api",
		Route:         "GET /api/users/{id}/orders",
		Method:        "GET",
		Path:          "/api/users/42/orders?page=1",
		Class:         diff.ClassBodyMismatch,
		Status:        diff.Status{Prod: 200, Candidate: 200, Baseline: 200},
		Differences: []diff.Difference{
			{Where: diff.WhereBody, Pointer: "/orders/0/total", Kind: diff.KindChanged, Prod: "12.5", Candidate: "12.50", Baseline: "12.5"},
			{Where: diff.WhereBody, Pointer: "/generated_at", Kind: diff.KindChanged, Prod: "t1", Candidate: "t2", Baseline: "t2", Suppressed: diff.SuppressedBaseline},
			{Where: diff.WhereHeader, Pointer: "content-type", Kind: diff.KindChanged, Prod: "application/json", Candidate: "text/plain", Detail: "media type"},
		},
		Truncated:       true,
		Latency:         diff.Latency{ProdMicros: 23_600, CandidateMicros: 29_000, BaselineMicros: 24_000},
		OriginalTraceID: fixtureTraceHex,
		ReplayTraceID:   fixtureLinkTrace,
		StartedAt:       fixtureStart,
		SentAt:          fixtureStart.Add(time.Hour),
		LagMicros:       1_500,
	}
}

// encodeDiff encodes r as the replayer does.
func encodeDiff(t *testing.T, r *diff.Result) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(r); err != nil {
		t.Fatal(err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

// mustHex decodes hex ids for the OTLP fixtures.
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// kv builds an OTLP attribute from a Go value.
func kv(key string, v any) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: key, Value: anyValueOf(v)}
}

func anyValueOf(v any) *commonv1.AnyValue {
	switch x := v.(type) {
	case string:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: x}}
	case int:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: int64(x)}}
	case int64:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: x}}
	case float64:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_DoubleValue{DoubleValue: x}}
	case bool:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BoolValue{BoolValue: x}}
	case []byte:
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_BytesValue{BytesValue: x}}
	case []any:
		vals := make([]*commonv1.AnyValue, 0, len(x))
		for _, e := range x {
			vals = append(vals, anyValueOf(e))
		}
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_ArrayValue{ArrayValue: &commonv1.ArrayValue{Values: vals}}}
	case map[string]any:
		kvs := make([]*commonv1.KeyValue, 0, len(x))
		for k, e := range x {
			kvs = append(kvs, kv(k, e))
		}
		return &commonv1.AnyValue{Value: &commonv1.AnyValue_KvlistValue{KvlistValue: &commonv1.KeyValueList{Values: kvs}}}
	case nil:
		return &commonv1.AnyValue{}
	}
	panic("anyValueOf: unsupported type")
}

// testResource is the resource of the OTLP fixtures.
func testResource() *resourcev1.Resource {
	return &resourcev1.Resource{Attributes: []*commonv1.KeyValue{
		kv("service.name", "gateway"),
		kv("service.version", "1.2.3"),
		kv("service.namespace", "demo"),
		kv("service.instance.id", "gateway-7c9d8-x2k4p"),
		kv("k8s.pod.name", "gateway-7c9d8-x2k4p"),
	}}
}

// testSpan returns a server span with the stable HTTP attributes, a link, an
// event and nanosecond timestamps.
func testSpan(t *testing.T) *tracev1.Span {
	t.Helper()
	start := fixtureStart.UnixNano()
	return &tracev1.Span{
		TraceId:           mustHex(t, fixtureTraceHex),
		SpanId:            mustHex(t, fixtureSpanHex),
		ParentSpanId:      mustHex(t, fixtureParentHex),
		TraceState:        "vendor=abc",
		Flags:             0x101, // sampled, remote parent known
		Name:              "GET /api/users/{id}/orders",
		Kind:              tracev1.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: uint64(start),
		EndTimeUnixNano:   uint64(start + 23_200_000),
		Attributes: []*commonv1.KeyValue{
			kv("http.request.method", "GET"),
			kv("http.response.status_code", 200),
			kv("http.route", "/api/users/{id}/orders"),
			kv("url.path", "/api/users/42/orders"),
			kv("url.query", "page=1"),
			kv("url.scheme", "http"),
			kv("server.address", "gateway.demo.svc.cluster.local"),
			kv("server.port", 8080),
			kv("user_agent.original", "loadgen/1.0"),
			kv("shadowtap.tags", []any{"a", "b"}),
			kv("shadowtap.nested", map[string]any{"k": "v"}),
			kv("shadowtap.ratio", 0.5),
			kv("shadowtap.bytes", []byte{1, 2}),
			kv("shadowtap.flag", true),
		},
		DroppedAttributesCount: 2,
		Events: []*tracev1.Span_Event{{
			TimeUnixNano: uint64(start + 1_000_000),
			Name:         "exception",
			Attributes:   []*commonv1.KeyValue{kv("exception.type", "timeout")},
		}},
		Links: []*tracev1.Span_Link{{
			TraceId:    mustHex(t, fixtureLinkTrace),
			SpanId:     mustHex(t, fixtureLinkSpan),
			TraceState: "x=1",
			Attributes: []*commonv1.KeyValue{kv("shadowtap.relation", "original")},
		}},
		Status: &tracev1.Status{Code: tracev1.Status_STATUS_CODE_ERROR, Message: "deadline exceeded"},
	}
}

// spansRequest wraps spans in one export request under the test resource.
func spansRequest(scope *commonv1.InstrumentationScope, spans ...*tracev1.Span) *coltracev1.ExportTraceServiceRequest {
	return &coltracev1.ExportTraceServiceRequest{ResourceSpans: []*tracev1.ResourceSpans{{
		Resource:   testResource(),
		ScopeSpans: []*tracev1.ScopeSpans{{Scope: scope, Spans: spans}},
	}}}
}

// testLog returns a log record correlated with the fixture span.
func testLog(t *testing.T) *logsv1.LogRecord {
	t.Helper()
	ts := fixtureStart.UnixNano()
	return &logsv1.LogRecord{
		TimeUnixNano:         uint64(ts),
		ObservedTimeUnixNano: uint64(ts + 500),
		SeverityNumber:       logsv1.SeverityNumber_SEVERITY_NUMBER_WARN,
		SeverityText:         "WARN",
		Body:                 anyValueOf("cache miss for user u-42"),
		Attributes:           []*commonv1.KeyValue{kv("code.function", "lookup"), kv("retry", 2)},
		TraceId:              mustHex(t, fixtureTraceHex),
		SpanId:               mustHex(t, fixtureSpanHex),
		Flags:                1,
		EventName:            "cache.miss",
	}
}

// logsRequest wraps records in one export request under the test resource.
func logsRequest(scope *commonv1.InstrumentationScope, recs ...*logsv1.LogRecord) *collogsv1.ExportLogsServiceRequest {
	return &collogsv1.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource:  testResource(),
		ScopeLogs: []*logsv1.ScopeLogs{{Scope: scope, LogRecords: recs}},
	}}}
}

// mustProto marshals an OTLP message as the Collector's kafka exporter does.
func mustProto(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// testSource is the Kafka provenance of the fixture records.
func testSource(headers map[string]string) Source {
	return Source{Topic: "topic", Partition: 1, Offset: 42, Timestamp: fixtureStart.Add(time.Second), Headers: headers}
}

// decodeDoc parses a document body.
func decodeDoc(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var m map[string]any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		t.Fatalf("decode document: %v\n%s", err, body)
	}
	return m
}

// field returns the value at a dotted path of a nested document, or nil.
func field(doc map[string]any, path string) any {
	cur := any(doc)
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur, ok = m[seg]
		if !ok {
			return nil
		}
	}
	return cur
}

// str returns a field as text: strings as is, numbers and booleans
// formatted, missing as "".
func str(doc map[string]any, path string) string {
	switch v := field(doc, path).(type) {
	case nil:
		return ""
	case string:
		return v
	case json.Number:
		return v.String()
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

// expectFields checks every path of want against doc.
func expectFields(t *testing.T, doc map[string]any, want map[string]string) {
	t.Helper()
	for path, v := range want {
		if got := str(doc, path); got != v {
			t.Errorf("%s = %q, want %q", path, got, v)
		}
	}
}

// expectAbsent checks that none of the paths is set.
func expectAbsent(t *testing.T, doc map[string]any, paths ...string) {
	t.Helper()
	for _, p := range paths {
		if v := field(doc, p); v != nil {
			t.Errorf("%s = %v, want absent", p, v)
		}
	}
}

// syncBuffer is a bytes.Buffer safe for the logger's goroutines.
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
