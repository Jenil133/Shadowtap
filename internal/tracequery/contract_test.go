package tracequery

// The documents this package reads are written by internal/index. These tests
// run real indexer output through the decoders, so a renamed or moved field
// on either side fails here instead of in production, where it would show up
// as a waterfall with blanks in it.

import (
	"encoding/json"
	"testing"
	"time"

	collogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/index"
)

var (
	contractStart  = time.Date(2026, 10, 3, 9, 0, 0, 100_000, time.UTC)
	contractSource = index.Source{Topic: "t", Partition: 1, Offset: 7, Timestamp: contractStart}
)

func hexBytes(t *testing.T, s string) []byte {
	t.Helper()
	b := make([]byte, len(s)/2)
	for i := range b {
		var v byte
		for _, c := range s[2*i : 2*i+2] {
			v <<= 4
			switch {
			case c >= '0' && c <= '9':
				v |= byte(c - '0')
			case c >= 'a' && c <= 'f':
				v |= byte(c-'a') + 10
			default:
				t.Fatalf("not hex: %q", s)
			}
		}
		b[i] = v
	}
	return b
}

func str(k, v string) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: k, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: v}}}
}

func num(k string, v int64) *commonv1.KeyValue {
	return &commonv1.KeyValue{Key: k, Value: &commonv1.AnyValue{Value: &commonv1.AnyValue_IntValue{IntValue: v}}}
}

// indexed decodes the indexer's document the way msearch results are decoded.
func indexed(t *testing.T, d *index.Document) doc {
	t.Helper()
	out, err := decodeDoc(json.RawMessage(d.Body))
	if err != nil {
		t.Fatalf("indexer document does not decode: %v\n%s", err, d.Body)
	}
	return out
}

func TestContractSpanDocument(t *testing.T) {
	start := uint64(contractStart.UnixNano())
	req := &coltracev1.ExportTraceServiceRequest{ResourceSpans: []*tracev1.ResourceSpans{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{str("service.name", "gateway"), str("service.version", "v1"), str("k8s.pod.name", "gateway-1")}},
		ScopeSpans: []*tracev1.ScopeSpans{{Spans: []*tracev1.Span{{
			TraceId:           hexBytes(t, originalTrace),
			SpanId:            hexBytes(t, "b2c3d4e5f6071829"),
			ParentSpanId:      hexBytes(t, "0a1b2c3d4e5f6071"),
			Name:              "GET /api/users/{id}/orders",
			Kind:              tracev1.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: start,
			EndTimeUnixNano:   start + 23_200_000,
			Attributes: []*commonv1.KeyValue{
				str("http.request.method", "GET"), num("http.response.status_code", 503),
				str("http.route", "/api/users/{id}/orders"), str("url.path", "/api/users/42/orders"),
			},
			Status: &tracev1.Status{Code: tracev1.Status_STATUS_CODE_ERROR, Message: "upstream timeout"},
			Links:  []*tracev1.Span_Link{{TraceId: hexBytes(t, replayTrace), SpanId: hexBytes(t, "5c6d7e8f90112233")}},
		}}}},
	}}}
	docs, rejected := index.SpanDocuments(req, contractSource, "traces-shadowtap-default")
	if len(rejected) != 0 || len(docs) != 1 {
		t.Fatalf("docs %d rejected %v", len(docs), rejected)
	}
	s := spanFromDoc(indexed(t, docs[0]))
	want := &Span{
		TraceID: originalTrace, SpanID: "b2c3d4e5f6071829", ParentID: "0a1b2c3d4e5f6071",
		Name: "GET /api/users/{id}/orders", Kind: "SERVER", Service: "gateway",
		Start: contractStart, Duration: 23200 * time.Microsecond,
		Status: "ERROR", StatusMessage: "upstream timeout",
		HTTPMethod: "GET", URLPath: "/api/users/42/orders", HTTPStatus: 503,
		Links: []Link{{TraceID: replayTrace, SpanID: "5c6d7e8f90112233"}},
	}
	for name, got := range map[string][2]any{
		"trace id":  {s.TraceID, want.TraceID},
		"span id":   {s.SpanID, want.SpanID},
		"parent id": {s.ParentID, want.ParentID},
		"name":      {s.Name, want.Name},
		"kind":      {s.Kind, want.Kind},
		"service":   {s.Service, want.Service},
		"start":     {s.Start.Equal(want.Start), true},
		"duration":  {s.Duration, want.Duration},
		"status":    {s.Status, want.Status},
		"message":   {s.StatusMessage, want.StatusMessage},
		"method":    {s.HTTPMethod, want.HTTPMethod},
		"path":      {s.URLPath, want.URLPath},
		"http":      {s.HTTPStatus, want.HTTPStatus},
		"links":     {len(s.Links) == 1 && s.Links[0] == want.Links[0], true},
		"attribute": {s.Attributes["http.route"], "/api/users/{id}/orders"},
	} {
		if got[0] != got[1] {
			t.Errorf("%s = %v, want %v", name, got[0], got[1])
		}
	}
}

func TestContractLogDocument(t *testing.T) {
	req := &collogsv1.ExportLogsServiceRequest{ResourceLogs: []*logsv1.ResourceLogs{{
		Resource: &resourcev1.Resource{Attributes: []*commonv1.KeyValue{str("service.name", "orders")}},
		ScopeLogs: []*logsv1.ScopeLogs{{LogRecords: []*logsv1.LogRecord{{
			TimeUnixNano:   uint64(contractStart.UnixNano()),
			SeverityText:   "WARN",
			SeverityNumber: logsv1.SeverityNumber_SEVERITY_NUMBER_WARN,
			Body:           &commonv1.AnyValue{Value: &commonv1.AnyValue_StringValue{StringValue: "cache timeout"}},
			Attributes:     []*commonv1.KeyValue{str("cache.key", "user-orders:u-42")},
			TraceId:        hexBytes(t, originalTrace),
			SpanId:         hexBytes(t, "293a4b5c6d7e8f90"),
		}}}},
	}}}
	docs, rejected := index.LogDocuments(req, contractSource, "logs-shadowtap-default")
	if len(rejected) != 0 || len(docs) != 1 {
		t.Fatalf("docs %d rejected %v", len(docs), rejected)
	}
	l := logFromDoc(indexed(t, docs[0]))
	if l.TraceID != originalTrace || l.SpanID != "293a4b5c6d7e8f90" || l.Service != "orders" || l.Level != "WARN" || l.Message != "cache timeout" || !l.Time.Equal(contractStart) || l.Attributes["cache.key"] != "user-orders:u-42" {
		t.Errorf("log = %+v", l)
	}
}

func TestContractCaptureDocument(t *testing.T) {
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            captureID,
		Service:       "gateway",
		TraceID:       originalTrace,
		ParentSpanID:  "9f8e7d6c5b4a3928",
		RequestID:     "req-8c1d2e3f",
		Source:        capture.Source{Tap: "tap-0", Mode: capture.ModeObserve},
		Sampling:      capture.Sampling{Rate: 1, Rule: "default", KeySource: "traceparent"},
		Timing:        capture.Timing{StartedAt: contractStart, ResponseStartedAt: contractStart.Add(23600 * time.Microsecond), DurationMicros: 23600},
		Request:       capture.Request{Method: "GET", Path: "/api/users/42/orders?verbose=1", Headers: capture.Headers{"accept": {"*/*"}}},
		Response:      capture.Response{Status: 200},
		Redactions:    map[string]int{"email": 1, "phone": 2},
	}
	x.Request.Body.Size = 5120
	x.Request.Body.Omitted = capture.OmittedTooLarge
	x.Response.Body.ContentType = "application/json"
	x.Response.Body.SetData([]byte(`{"email":"user-0123456789@redacted.invalid"}`))
	x.Response.Body.Size = 44
	x.Response.Body.Truncated = true

	d, err := index.CaptureDocument(x, contractSource, "shadowtap-captures")
	if err != nil {
		t.Fatal(err)
	}
	c := captureFromDoc(indexed(t, d))
	if c.ID != captureID || c.TraceID != originalTrace || c.ParentID != "9f8e7d6c5b4a3928" || c.RequestID != "req-8c1d2e3f" || c.Service != "gateway" {
		t.Errorf("identity = %+v", c)
	}
	if c.Method != "GET" || c.Route != "GET /api/users/{id}/orders" || c.Path != "/api/users/42/orders" || c.Status != 200 || !c.Start.Equal(contractStart) || c.Duration != 23600*time.Microsecond {
		t.Errorf("request = %+v", c)
	}
	if c.Tap != "tap-0" || c.Mode != "observe" || c.Redactions["email"] != 1 || c.Redactions["phone"] != 2 {
		t.Errorf("source/redactions = %+v", c)
	}
	if c.Request.Size != 5120 || c.Request.Omitted != "too_large" || c.Request.Content != "" {
		t.Errorf("request body = %+v", c.Request)
	}
	if c.Response.ContentType != "application/json" || c.Response.Size != 44 || !c.Response.Truncated || c.Response.Content != `{"email":"user-0123456789@redacted.invalid"}` {
		t.Errorf("response body = %+v", c.Response)
	}
}

func TestContractDiffDocument(t *testing.T) {
	sent := contractStart.Add(time.Hour)
	r := &diff.Result{
		SchemaVersion:   diff.SchemaVersion,
		RunID:           "run-1",
		CaptureID:       captureID,
		Service:         "gateway",
		Route:           "GET /api/users/{id}/orders",
		Method:          "GET",
		Path:            "/api/users/42/orders",
		Class:           diff.ClassBodyMismatch,
		Status:          diff.Status{Prod: 200, Candidate: 500, Baseline: 200},
		Differences:     []diff.Difference{{Where: diff.WhereBody, Pointer: "/total", Kind: "changed", Prod: "1", Candidate: "2"}, {Where: diff.WhereBody, Pointer: "/generated_at", Kind: "changed", Prod: "a", Candidate: "b", Suppressed: diff.SuppressedNoise}},
		Truncated:       true,
		Latency:         diff.Latency{ProdMicros: 23600, CandidateMicros: 29000, BaselineMicros: 24100},
		OriginalTraceID: originalTrace,
		ReplayTraceID:   replayTrace,
		StartedAt:       contractStart,
		SentAt:          sent,
		LagMicros:       1200,
	}
	d, err := index.DiffDocument(r, contractSource, "shadowtap-diffs")
	if err != nil {
		t.Fatal(err)
	}
	df := diffFromDoc(indexed(t, d))
	if df.RunID != "run-1" || df.CaptureID != captureID || df.Service != "gateway" || df.Method != "GET" || df.Route != r.Route || df.Path != r.Path || df.Class != "body_mismatch" {
		t.Errorf("identity = %+v", df)
	}
	if df.ProdStatus != 200 || df.CandidateStatus != 500 || df.BaselineStatus != 200 {
		t.Errorf("statuses = %d %d %d", df.ProdStatus, df.CandidateStatus, df.BaselineStatus)
	}
	if df.ProdLatency != 23600*time.Microsecond || df.CandidateLatency != 29*time.Millisecond || df.BaselineLatency != 24100*time.Microsecond || df.ScheduleLag != 1200*time.Microsecond {
		t.Errorf("latencies = %s %s %s lag %s", df.ProdLatency, df.CandidateLatency, df.BaselineLatency, df.ScheduleLag)
	}
	if df.TraceID != originalTrace || df.ReplayTraceID != replayTrace || !df.Start.Equal(contractStart) || !df.SentAt.Equal(sent) || !df.Truncated {
		t.Errorf("traces/times = %+v", df)
	}
	if len(df.Differences) != 2 || len(df.Reported()) != 1 || df.Reported()[0].Pointer != "/total" || df.Differences[1].Suppressed != "noise" {
		t.Errorf("differences = %+v", df.Differences)
	}
}
