package index

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

func TestCaptureDocument(t *testing.T) {
	x := testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart)
	tp := "00-" + fixtureTraceHex + "-" + fixtureParentHex + "-01"
	src := testSource(map[string]string{"traceparent": tp, capture.HeaderTraceID: fixtureTraceHex, capture.HeaderService: "demo-api"})

	d, err := CaptureDocument(x, src, DefaultCapturesIndex)
	if err != nil {
		t.Fatal(err)
	}
	if d.Index != DefaultCapturesIndex || d.ID != fixtureCaptureID {
		t.Errorf("index %q id %q", d.Index, d.ID)
	}
	doc := decodeDoc(t, d.Body)
	expectFields(t, doc, map[string]string{
		"@timestamp":                      "2026-10-03T09:00:00.123456789Z",
		"event.start":                     "2026-10-03T09:00:00.123456789Z",
		"event.end":                       "2026-10-03T09:00:00.136456789Z",
		"event.duration":                  "12000000",
		"capture.id":                      fixtureCaptureID,
		"capture.schema_version":          "1",
		"capture.sampling.rate":           "0.25",
		"capture.sampling.rule":           "default",
		"capture.sampling.key_source":     "traceparent",
		"capture.timing.duration_us":      "12000",
		"capture.timing.total_us":         "13000",
		"trace.id":                        fixtureTraceHex,
		"parent.id":                       fixtureParentHex,
		"service.name":                    "demo-api",
		"source.tap":                      "shadowtap-tap-0",
		"source.mode":                     capture.ModeObserve,
		"tap.name":                        "shadowtap-tap-0",
		"tap.mode":                        capture.ModeObserve,
		"http.route":                      "POST /api/users/{id}/orders",
		"http.request.id":                 "req-" + fixtureCaptureID,
		"http.request.method":             "POST",
		"http.request.mime_type":          "application/json",
		"http.request.body.bytes":         "39",
		"http.request.body.encoding":      capture.EncodingUTF8,
		"http.request.body.content":       `{"email":"user-1a2b3c@redacted.invalid"}`,
		"http.response.status_code":       "201",
		"http.response.mime_type":         "application/json; charset=utf-8",
		"http.response.body.bytes":        "11",
		"http.response.body.content":      `{"id":"u1"}`,
		"http.response.body.decoded_from": "gzip",
		"url.path":                        "/api/users/u_3f2a1b9c0d4e/orders",
		"url.query":                       "page=2&q=%3Cb%3E",
		"url.original":                    "/api/users/u_3f2a1b9c0d4e/orders?page=2&q=%3Cb%3E",
		"url.scheme":                      "http",
		"url.domain":                      "demo-api",
		"url.port":                        "8080",
		"redactions.email":                "2",
		"redactions.phone":                "1",
		"redactions_total":                "3",
		"propagation.traceparent":         tp,
		"propagation.trace.id":            fixtureTraceHex,
		"propagation.span.id":             fixtureParentHex,
		"propagation.sampled":             "true",
		"kafka.topic":                     "topic",
		"kafka.partition":                 "1",
		"kafka.offset":                    "42",
	})
	headers, _ := field(doc, "http.request.headers").(map[string]any)
	if got, _ := json.Marshal(headers["accept"]); string(got) != `["application/json","text/plain"]` {
		t.Errorf("request headers = %v", headers)
	}
	expectFields(t, doc, map[string]string{"http.response.body.encoding": capture.EncodingUTF8})
	expectAbsent(t, doc, "http.request.body.truncated", "http.request.body.omitted", "http.response.body.omitted")
	if strings.Contains(string(d.Body), "\\u003c") {
		t.Error("HTML escaping applied to the document")
	}
}

func TestCaptureDocumentVariants(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*capture.Exchange)
		src    Source
		want   map[string]string
		absent []string
	}{
		{
			name: "omitted and truncated bodies keep their metadata only",
			mutate: func(x *capture.Exchange) {
				x.Request.Body.Omit(capture.OmittedTooLarge)
				x.Request.Body.Size = 5120
				x.Response.Body.Truncated = true
			},
			src:    testSource(nil),
			want:   map[string]string{"http.request.body.omitted": capture.OmittedTooLarge, "http.request.body.bytes": "5120", "http.response.body.truncated": "true"},
			absent: []string{"http.request.body.content", "http.request.body.encoding", "propagation"},
		},
		{
			name: "no trace context, authority without port",
			mutate: func(x *capture.Exchange) {
				x.TraceID, x.ParentSpanID, x.RequestID = "", "", ""
				x.Request.Authority = "demo-api"
				x.Request.Scheme = ""
				x.Request.Path = "/healthz"
			},
			src:    testSource(nil),
			want:   map[string]string{"url.domain": "demo-api", "url.path": "/healthz", "http.route": "POST /healthz"},
			absent: []string{"trace", "parent", "http.request.id", "url.port", "url.scheme", "url.query"},
		},
		{
			name: "content type from the header when the body has none",
			mutate: func(x *capture.Exchange) {
				x.Request.Body.ContentType = ""
			},
			src:  testSource(nil),
			want: map[string]string{"http.request.mime_type": "application/json"},
		},
		{
			name: "no redactions",
			mutate: func(x *capture.Exchange) {
				x.Redactions = nil
				x.Timing.DurationMicros = 0
				x.Timing.CompletedAt = time.Time{}
			},
			src:    testSource(nil),
			absent: []string{"redactions", "redactions_total", "event.duration", "event.end", "capture.timing.total_us", "capture.timing.completed_at"},
		},
		{
			name:   "invalid traceparent header yields no propagation",
			mutate: func(*capture.Exchange) {},
			src:    testSource(map[string]string{"traceparent": "00-bad"}),
			absent: []string{"propagation"},
		},
		{
			name: "start time missing falls back to the record timestamp",
			mutate: func(x *capture.Exchange) {
				x.Timing = capture.Timing{}
			},
			src:  testSource(nil),
			want: map[string]string{"@timestamp": "2026-10-03T09:00:01.123456789Z"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart)
			tt.mutate(x)
			d, err := CaptureDocument(x, tt.src, DefaultCapturesIndex)
			if err != nil {
				t.Fatal(err)
			}
			doc := decodeDoc(t, d.Body)
			expectFields(t, doc, tt.want)
			expectAbsent(t, doc, tt.absent...)
		})
	}
}

func TestCaptureDocumentErrors(t *testing.T) {
	if _, err := CaptureDocument(nil, testSource(nil), DefaultCapturesIndex); err == nil {
		t.Error("nil capture accepted")
	}
	x := testExchange("", fixtureTraceHex, fixtureStart)
	if _, err := CaptureDocument(x, testSource(nil), DefaultCapturesIndex); err == nil {
		t.Error("capture without id accepted")
	}
	x = testExchange(fixtureCaptureID, fixtureTraceHex, time.Time{})
	if _, err := CaptureDocument(x, Source{}, DefaultCapturesIndex); err == nil {
		t.Error("capture without any time accepted")
	}
}

func TestDecodeCapture(t *testing.T) {
	x := testExchange(fixtureCaptureID, fixtureTraceHex, fixtureStart)
	got, err := DecodeCapture(encodeExchange(t, x))
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != x.ID || !got.Timing.StartedAt.Equal(x.Timing.StartedAt) {
		t.Errorf("decoded %+v", got)
	}
	for name, raw := range map[string][]byte{
		"garbage":        []byte("{"),
		"wrong schema":   []byte(`{"schema_version":2,"id":"x"}`),
		"missing fields": []byte(`{"schema_version":1,"id":"x"}`),
	} {
		if _, err := DecodeCapture(raw); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
