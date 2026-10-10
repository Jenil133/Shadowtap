package index

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestSpanDocument(t *testing.T) {
	scope := &commonv1.InstrumentationScope{Name: "otelhttp", Version: "0.72.0"}
	req := spansRequest(scope, testSpan(t))
	src := testSource(map[string]string{"traceparent": "00-" + fixtureTraceHex + "-" + fixtureSpanHex + "-01"})

	docs, rejected := SpanDocuments(req, src, DefaultSpansIndex)
	if len(rejected) != 0 {
		t.Fatalf("rejected: %+v", rejected)
	}
	if len(docs) != 1 {
		t.Fatalf("got %d documents, want 1", len(docs))
	}
	d := docs[0]
	if d.Index != DefaultSpansIndex || d.ID != fixtureTraceHex+"-"+fixtureSpanHex {
		t.Errorf("index %q id %q", d.Index, d.ID)
	}
	if !reflect.DeepEqual(d.Source, src) {
		t.Errorf("source = %+v", d.Source)
	}
	doc := decodeDoc(t, d.Body)
	expectFields(t, doc, map[string]string{
		"@timestamp":                "2026-10-03T09:00:00.123456789Z",
		"event.end":                 "2026-10-03T09:00:00.146656789Z",
		"event.duration":            "23200000",
		"trace.id":                  fixtureTraceHex,
		"trace.state":               "vendor=abc",
		"span.id":                   fixtureSpanHex,
		"span.name":                 "GET /api/users/{id}/orders",
		"span.kind":                 "SERVER",
		"span.flags":                "257",
		"parent.id":                 fixtureParentHex,
		"service.name":              "gateway",
		"service.version":           "1.2.3",
		"service.namespace":         "demo",
		"service.instance.id":       "gateway-7c9d8-x2k4p",
		"scope.name":                "otelhttp",
		"scope.version":             "0.72.0",
		"status.code":               "ERROR",
		"status.message":            "deadline exceeded",
		"http.request.method":       "GET",
		"http.response.status_code": "200",
		"http.route":                "/api/users/{id}/orders",
		"url.path":                  "/api/users/42/orders",
		"url.query":                 "page=1",
		"url.scheme":                "http",
		"server.address":            "gateway.demo.svc.cluster.local",
		"server.port":               "8080",
		"user_agent.original":       "loadgen/1.0",
		"dropped.attributes":        "2",
		"kafka.topic":               "topic",
		"kafka.partition":           "1",
		"kafka.offset":              "42",
		"kafka.timestamp":           "2026-10-03T09:00:01.123456789Z",
	})
	// Attributes keep their dotted keys: the field is flattened in the
	// mapping, so a key never becomes a path.
	attrs, _ := field(doc, "attributes").(map[string]any)
	if attrs["http.request.method"] != "GET" || attrs["shadowtap.flag"] != true {
		t.Errorf("attributes = %v", attrs)
	}
	if got := fmt.Sprint(attrs["shadowtap.tags"]); got != "[a b]" {
		t.Errorf("array attribute = %s", got)
	}
	if nested, _ := attrs["shadowtap.nested"].(map[string]any); nested["k"] != "v" {
		t.Errorf("kvlist attribute = %v", attrs["shadowtap.nested"])
	}
	if attrs["shadowtap.bytes"] != "AQI=" {
		t.Errorf("bytes attribute = %v", attrs["shadowtap.bytes"])
	}
	if attrs["shadowtap.ratio"] != json.Number("0.5") {
		t.Errorf("double attribute = %v", attrs["shadowtap.ratio"])
	}
	res, _ := field(doc, "resource.attributes").(map[string]any)
	if res["k8s.pod.name"] != "gateway-7c9d8-x2k4p" || res["service.name"] != "gateway" {
		t.Errorf("resource.attributes = %v", res)
	}
	links, _ := field(doc, "links").([]any)
	if len(links) != 1 {
		t.Fatalf("links = %v", links)
	}
	link := links[0].(map[string]any)
	expectFields(t, link, map[string]string{"trace.id": fixtureLinkTrace, "span.id": fixtureLinkSpan, "trace.state": "x=1"})
	if la, _ := link["attributes"].(map[string]any); la["shadowtap.relation"] != "original" {
		t.Errorf("link attributes = %v", link["attributes"])
	}
	events, _ := field(doc, "events").([]any)
	if len(events) != 1 {
		t.Fatalf("events = %v", events)
	}
	expectFields(t, events[0].(map[string]any), map[string]string{"name": "exception", "time": "2026-10-03T09:00:00.124456789Z"})
	// Spans never index propagation metadata: their own ids are the trace.
	expectAbsent(t, doc, "propagation")
}

func TestSpanDocumentVariants(t *testing.T) {
	base := func() *tracev1.Span {
		s := testSpan(t)
		s.Attributes, s.Events, s.Links, s.Status, s.TraceState, s.Flags, s.DroppedAttributesCount = nil, nil, nil, nil, "", 0, 0
		return s
	}
	tests := []struct {
		name   string
		span   func() *tracev1.Span
		want   map[string]string
		absent []string
	}{
		{
			name: "pre-1.21 http conventions are promoted",
			span: func() *tracev1.Span {
				s := base()
				s.Kind = tracev1.Span_SPAN_KIND_CLIENT
				s.Attributes = []*commonv1.KeyValue{
					kv("http.method", "POST"),
					kv("http.status_code", "503"), // as a string, which old instrumentation emitted
					kv("http.target", "/api/orders?x=1&y=2"),
					kv("http.url", "http://orders:8080/api/orders?x=1&y=2"),
					kv("http.scheme", "http"),
					kv("net.host.name", "orders"),
					kv("net.host.port", 8080),
					kv("http.user_agent", "curl/8"),
				}
				return s
			},
			want: map[string]string{
				"span.kind":                 "CLIENT",
				"http.request.method":       "POST",
				"http.response.status_code": "503",
				"url.path":                  "/api/orders",
				"url.query":                 "x=1&y=2",
				"url.full":                  "http://orders:8080/api/orders?x=1&y=2",
				"url.scheme":                "http",
				"server.address":            "orders",
				"server.port":               "8080",
				"user_agent.original":       "curl/8",
				"status.code":               "UNSET",
			},
			absent: []string{"http.route", "status.message", "links", "events", "dropped"},
		},
		{
			name: "stable conventions win over old ones",
			span: func() *tracev1.Span {
				s := base()
				s.Attributes = []*commonv1.KeyValue{kv("http.request.method", "GET"), kv("http.method", "HEAD"), kv("url.path", "/new"), kv("http.target", "/old?q=1")}
				return s
			},
			want:   map[string]string{"http.request.method": "GET", "url.path": "/new"},
			absent: []string{"url.query"},
		},
		{
			name: "root span of every kind",
			span: func() *tracev1.Span {
				s := base()
				s.ParentSpanId = nil
				s.Kind = tracev1.Span_SPAN_KIND_PRODUCER
				s.Status = &tracev1.Status{Code: tracev1.Status_STATUS_CODE_OK}
				return s
			},
			want:   map[string]string{"span.kind": "PRODUCER", "status.code": "OK"},
			absent: []string{"parent.id", "attributes", "http", "url"},
		},
		{
			name: "consumer and internal kinds",
			span: func() *tracev1.Span {
				s := base()
				s.Kind = tracev1.Span_SPAN_KIND_CONSUMER
				return s
			},
			want: map[string]string{"span.kind": "CONSUMER"},
		},
		{
			name: "unspecified kind and missing times fall back to the record timestamp",
			span: func() *tracev1.Span {
				s := base()
				s.Kind = tracev1.Span_SPAN_KIND_UNSPECIFIED
				s.StartTimeUnixNano, s.EndTimeUnixNano = 0, 0
				return s
			},
			want:   map[string]string{"span.kind": "UNSPECIFIED", "@timestamp": "2026-10-03T09:00:01.123456789Z"},
			absent: []string{"event.duration", "event.end"},
		},
		{
			name: "end before start yields no duration",
			span: func() *tracev1.Span {
				s := base()
				s.EndTimeUnixNano = s.StartTimeUnixNano - 1
				return s
			},
			absent: []string{"event.duration"},
		},
		{
			name: "resource without service.name is unknown_service",
			span: base,
			want: map[string]string{"service.name": "gateway"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			docs, rejected := SpanDocuments(spansRequest(nil, tt.span()), testSource(nil), DefaultSpansIndex)
			if len(rejected) != 0 || len(docs) != 1 {
				t.Fatalf("docs %d rejected %+v", len(docs), rejected)
			}
			doc := decodeDoc(t, docs[0].Body)
			expectFields(t, doc, tt.want)
			expectAbsent(t, doc, tt.absent...)
			expectAbsent(t, doc, "scope")
		})
	}

	t.Run("missing service name", func(t *testing.T) {
		req := spansRequest(nil, base())
		req.ResourceSpans[0].Resource = nil
		docs, _ := SpanDocuments(req, testSource(nil), DefaultSpansIndex)
		doc := decodeDoc(t, docs[0].Body)
		expectFields(t, doc, map[string]string{"service.name": unknownService})
		expectAbsent(t, doc, "resource")
	})
	t.Run("invalid link ids are dropped from the link", func(t *testing.T) {
		s := base()
		s.Links = []*tracev1.Span_Link{{TraceId: make([]byte, 16), SpanId: []byte{1}, Attributes: []*commonv1.KeyValue{kv("k", "v")}}}
		docs, _ := SpanDocuments(spansRequest(nil, s), testSource(nil), DefaultSpansIndex)
		links, _ := field(decodeDoc(t, docs[0].Body), "links").([]any)
		if len(links) != 1 {
			t.Fatalf("links = %v", links)
		}
		link := links[0].(map[string]any)
		expectAbsent(t, link, "trace", "span")
		if la, _ := link["attributes"].(map[string]any); la["k"] != "v" {
			t.Errorf("link = %v", link)
		}
	})
}

func TestSpanDocumentsRejectInvalidIDs(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*tracev1.Span)
		wantErr string
	}{
		{"zero trace id", func(s *tracev1.Span) { s.TraceId = make([]byte, 16) }, "no valid trace id"},
		{"short trace id", func(s *tracev1.Span) { s.TraceId = s.TraceId[:8] }, "no valid trace id"},
		{"zero span id", func(s *tracev1.Span) { s.SpanId = make([]byte, 8) }, "no valid span id"},
		{"long span id", func(s *tracev1.Span) { s.SpanId = append(s.SpanId, 1) }, "no valid span id"},
		{"both missing", func(s *tracev1.Span) { s.TraceId, s.SpanId = nil, nil }, "no valid trace id and span id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bad := testSpan(t)
			tt.mutate(bad)
			good := testSpan(t)
			good.SpanId[7] ^= 0xff
			req := spansRequest(nil, bad, good)
			docs, rejected := SpanDocuments(req, testSource(nil), DefaultSpansIndex)
			if len(docs) != 1 {
				t.Fatalf("got %d documents, want the valid one", len(docs))
			}
			if len(rejected) != 1 {
				t.Fatalf("rejected = %+v, want one", rejected)
			}
			if !strings.Contains(rejected[0].Err.Error(), tt.wantErr) {
				t.Errorf("error %q, want %q", rejected[0].Err, tt.wantErr)
			}
			// The partial document travels with the rejection so the DLQ
			// keeps the span's data.
			if !json.Valid(rejected[0].Body) {
				t.Errorf("rejected body is not JSON: %s", rejected[0].Body)
			}
		})
	}
	t.Run("nil spans are skipped", func(t *testing.T) {
		req := spansRequest(nil, nil, testSpan(t))
		docs, rejected := SpanDocuments(req, testSource(nil), DefaultSpansIndex)
		if len(docs) != 1 || len(rejected) != 0 {
			t.Errorf("docs %d rejected %d", len(docs), len(rejected))
		}
	})
}

func TestDecodeSpansAndLogs(t *testing.T) {
	if _, err := DecodeSpans([]byte{0xff, 0xff, 0xff}); err == nil {
		t.Error("DecodeSpans accepted garbage")
	}
	if _, err := DecodeLogs([]byte{0xff, 0xff, 0xff}); err == nil {
		t.Error("DecodeLogs accepted garbage")
	}
	req, err := DecodeSpans(mustProto(t, spansRequest(nil, testSpan(t))))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(req.GetResourceSpans()[0].GetScopeSpans()[0].GetSpans()); n != 1 {
		t.Errorf("decoded %d spans", n)
	}
	lreq, err := DecodeLogs(mustProto(t, logsRequest(nil, testLog(t))))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(lreq.GetResourceLogs()[0].GetScopeLogs()[0].GetLogRecords()); n != 1 {
		t.Errorf("decoded %d logs", n)
	}
	// An empty request decodes to no documents and no rejections.
	docs, rejected := SpanDocuments(&coltraceRequestEmpty, testSource(nil), DefaultSpansIndex)
	if len(docs) != 0 || len(rejected) != 0 {
		t.Errorf("empty request: docs %d rejected %d", len(docs), len(rejected))
	}
}

func TestLogDocument(t *testing.T) {
	scope := &commonv1.InstrumentationScope{Name: "otelslog"}
	src := testSource(nil)
	docs, rejected := LogDocuments(logsRequest(scope, testLog(t)), src, DefaultLogsIndex)
	if len(rejected) != 0 || len(docs) != 1 {
		t.Fatalf("docs %d rejected %+v", len(docs), rejected)
	}
	d := docs[0]
	if d.Index != DefaultLogsIndex || len(d.ID) != 32 {
		t.Errorf("index %q id %q", d.Index, d.ID)
	}
	doc := decodeDoc(t, d.Body)
	expectFields(t, doc, map[string]string{
		"@timestamp":             "2026-10-03T09:00:00.123456789Z",
		"log.observed_timestamp": "2026-10-03T09:00:00.123457289Z",
		"log.level":              "WARN",
		"log.severity_number":    "13",
		"log.flags":              "1",
		"event.name":             "cache.miss",
		"message":                "cache miss for user u-42",
		"trace.id":               fixtureTraceHex,
		"span.id":                fixtureSpanHex,
		"service.name":           "gateway",
		"scope.name":             "otelslog",
		"kafka.offset":           "42",
	})
	attrs, _ := field(doc, "attributes").(map[string]any)
	if attrs["code.function"] != "lookup" || attrs["retry"] != json.Number("2") {
		t.Errorf("attributes = %v", attrs)
	}
	expectAbsent(t, doc, "body", "dropped")

	t.Run("deterministic id", func(t *testing.T) {
		again, _ := LogDocuments(logsRequest(scope, testLog(t)), testSource(map[string]string{"x": "y"}), DefaultLogsIndex)
		if again[0].ID != d.ID {
			t.Errorf("same record hashed to %s and %s", d.ID, again[0].ID)
		}
		// Kafka provenance is not part of the id: a redelivery from another
		// offset of the same record must conflict.
		other := testSource(nil)
		other.Offset = 43
		again, _ = LogDocuments(logsRequest(scope, testLog(t)), other, DefaultLogsIndex)
		if again[0].ID != d.ID {
			t.Errorf("offset changed the id: %s vs %s", d.ID, again[0].ID)
		}
	})
	t.Run("distinct records get distinct ids", func(t *testing.T) {
		variants := []func(*logsv1.LogRecord){
			func(r *logsv1.LogRecord) { r.TimeUnixNano++ },
			func(r *logsv1.LogRecord) { r.Body = anyValueOf("other message") },
			func(r *logsv1.LogRecord) { r.SpanId = mustHex(t, fixtureLinkSpan) },
			func(r *logsv1.LogRecord) { r.TraceId = mustHex(t, fixtureLinkTrace) },
			func(r *logsv1.LogRecord) { r.Attributes = append(r.Attributes, kv("extra", 1)) },
			func(r *logsv1.LogRecord) { r.SeverityNumber = logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR },
		}
		seen := map[string]bool{d.ID: true}
		for i, mutate := range variants {
			rec := testLog(t)
			mutate(rec)
			out, _ := LogDocuments(logsRequest(scope, rec), src, DefaultLogsIndex)
			if seen[out[0].ID] {
				t.Errorf("variant %d collides with an earlier id", i)
			}
			seen[out[0].ID] = true
		}
	})
}

func TestLogDocumentVariants(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*logsv1.LogRecord)
		want   map[string]string
		absent []string
	}{
		{
			name: "severity text missing uses the number's range name",
			mutate: func(r *logsv1.LogRecord) {
				r.SeverityText = ""
				r.SeverityNumber = logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR2
			},
			want: map[string]string{"log.level": "ERROR", "log.severity_number": "18"},
		},
		{
			name: "no severity at all",
			mutate: func(r *logsv1.LogRecord) {
				r.SeverityText = ""
				r.SeverityNumber = logsv1.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED
			},
			absent: []string{"log.level", "log.severity_number"},
		},
		{
			name:   "event time missing uses the observed time",
			mutate: func(r *logsv1.LogRecord) { r.TimeUnixNano = 0 },
			want:   map[string]string{"@timestamp": "2026-10-03T09:00:00.123457289Z"},
		},
		{
			name:   "no time at all uses the record timestamp",
			mutate: func(r *logsv1.LogRecord) { r.TimeUnixNano, r.ObservedTimeUnixNano = 0, 0 },
			want:   map[string]string{"@timestamp": "2026-10-03T09:00:01.123456789Z"},
			absent: []string{"log.observed_timestamp"},
		},
		{
			name:   "structured body is kept as text and as a value",
			mutate: func(r *logsv1.LogRecord) { r.Body = anyValueOf(map[string]any{"user": "u-42", "n": 3}) },
			want:   map[string]string{"message": `{"n":3,"user":"u-42"}`, "body.structured.user": "u-42", "body.structured.n": "3"},
		},
		{
			name:   "empty body",
			mutate: func(r *logsv1.LogRecord) { r.Body = nil },
			want:   map[string]string{"message": ""},
			absent: []string{"body"},
		},
		{
			name:   "no trace context",
			mutate: func(r *logsv1.LogRecord) { r.TraceId, r.SpanId = nil, nil },
			absent: []string{"trace", "span"},
		},
		{
			name:   "dropped attributes are counted",
			mutate: func(r *logsv1.LogRecord) { r.DroppedAttributesCount = 3 },
			want:   map[string]string{"dropped.attributes": "3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := testLog(t)
			tt.mutate(rec)
			docs, rejected := LogDocuments(logsRequest(nil, rec), testSource(nil), DefaultLogsIndex)
			if len(rejected) != 0 || len(docs) != 1 {
				t.Fatalf("docs %d rejected %+v", len(docs), rejected)
			}
			doc := decodeDoc(t, docs[0].Body)
			expectFields(t, doc, tt.want)
			expectAbsent(t, doc, tt.absent...)
		})
	}
	t.Run("nil records are skipped", func(t *testing.T) {
		docs, rejected := LogDocuments(logsRequest(nil, nil, testLog(t)), testSource(nil), DefaultLogsIndex)
		if len(docs) != 1 || len(rejected) != 0 {
			t.Errorf("docs %d rejected %d", len(docs), len(rejected))
		}
	})
}

func TestSeverityName(t *testing.T) {
	tests := []struct {
		n    logsv1.SeverityNumber
		want string
	}{
		{logsv1.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED, ""},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_TRACE, "TRACE"},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_TRACE4, "TRACE"},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_DEBUG, "DEBUG"},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_INFO, "INFO"},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_INFO4, "INFO"},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_WARN, "WARN"},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_FATAL, "FATAL"},
		{logsv1.SeverityNumber_SEVERITY_NUMBER_FATAL4, "FATAL"},
		{logsv1.SeverityNumber(25), ""},
		{logsv1.SeverityNumber(-1), ""},
	}
	for _, tt := range tests {
		if got := severityName(tt.n); got != tt.want {
			t.Errorf("severityName(%d) = %q, want %q", tt.n, got, tt.want)
		}
	}
}

func TestNanosField(t *testing.T) {
	ts := time.Date(2026, 10, 3, 9, 0, 0, 1, time.UTC)
	if got, ok := nanosField(uint64(ts.UnixNano())); !ok || got != "2026-10-03T09:00:00.000000001Z" {
		t.Errorf("nanosField = %q, %v", got, ok)
	}
	if _, ok := nanosField(0); ok {
		t.Error("zero is a timestamp")
	}
	if _, ok := nanosField(1 << 63); ok {
		t.Error("out-of-range value is a timestamp")
	}
}
