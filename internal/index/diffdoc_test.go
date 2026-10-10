package index

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/replay/report"
)

func TestDiffDocument(t *testing.T) {
	r := testDiffResult()
	tp := "00-" + fixtureLinkTrace + "-" + fixtureLinkSpan + "-01"
	src := testSource(map[string]string{"traceparent": tp})

	d, err := DiffDocument(r, src, DefaultDiffsIndex)
	if err != nil {
		t.Fatal(err)
	}
	if d.Index != DefaultDiffsIndex || d.ID != fixtureRunID+"-"+fixtureCaptureID {
		t.Errorf("index %q id %q", d.Index, d.ID)
	}
	doc := decodeDoc(t, d.Body)
	expectFields(t, doc, map[string]string{
		"@timestamp":                       "2026-10-03T10:00:00.123456789Z", // sent_at
		"event.start":                      "2026-10-03T09:00:00.123456789Z",
		"event.duration":                   "23600000",
		"replay.run.id":                    fixtureRunID,
		"replay.trace.id":                  fixtureLinkTrace,
		"replay.sent_at":                   "2026-10-03T10:00:00.123456789Z",
		"replay.schedule_lag_us":           "1500",
		"replay.http.response.status_code": "200",
		"replay.event.duration":            "29000000",
		"replay.baseline.status_code":      "200",
		"capture.id":                       fixtureCaptureID,
		"trace.id":                         fixtureTraceHex,
		"service.name":                     "demo-api",
		"http.route":                       "GET /api/users/{id}/orders",
		"http.request.method":              "GET",
		"http.response.status_code":        "200",
		"url.path":                         "/api/users/42/orders",
		"url.query":                        "page=1",
		"diff.schema_version":              "1",
		"diff.class":                       diff.ClassBodyMismatch,
		"diff.differences_truncated":       "true",
		"diff.reported_count":              "2",
		"diff.suppressed_count":            "1",
		"latency.prod_us":                  "23600",
		"latency.candidate_us":             "29000",
		"latency.baseline_us":              "24000",
		"propagation.trace.id":             fixtureLinkTrace,
		"kafka.offset":                     "42",
	})
	if got, _ := json.Marshal(field(doc, "diff.pointers")); string(got) != `["body:/orders/0/total","header:content-type"]` {
		t.Errorf("diff.pointers = %s", got)
	}
	if got, _ := json.Marshal(field(doc, "diff.suppressed_pointers")); string(got) != `["body:/generated_at"]` {
		t.Errorf("diff.suppressed_pointers = %s", got)
	}
	diffs, _ := field(doc, "diff.differences").([]any)
	if len(diffs) != 3 {
		t.Fatalf("diff.differences = %v", diffs)
	}
	expectFields(t, diffs[0].(map[string]any), map[string]string{"where": "body", "pointer": "/orders/0/total", "kind": "changed", "prod": "12.5", "candidate": "12.50", "baseline": "12.5"})
	expectFields(t, diffs[1].(map[string]any), map[string]string{"suppressed": diff.SuppressedBaseline})
	expectFields(t, diffs[2].(map[string]any), map[string]string{"where": "header", "detail": "media type"})
	expectAbsent(t, doc, "diff.reason")
}

func TestDiffDocumentVariants(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*diff.Result)
		want   map[string]string
		absent []string
	}{
		{
			name: "error result",
			mutate: func(r *diff.Result) {
				r.Class, r.Reason = diff.ClassError, "dial tcp: connection refused"
				r.Differences, r.Truncated = nil, false
				r.Status = diff.Status{Prod: 200}
				r.Latency = diff.Latency{ProdMicros: 100}
				r.ReplayTraceID = ""
			},
			want:   map[string]string{"diff.class": diff.ClassError, "diff.reason": "dial tcp: connection refused", "diff.reported_count": "0"},
			absent: []string{"diff.pointers", "diff.differences", "diff.differences_truncated", "replay.trace", "replay.http", "replay.event", "replay.baseline", "latency.baseline_us"},
		},
		{
			name: "skipped result without sent time uses the original start",
			mutate: func(r *diff.Result) {
				r.Class = diff.ClassSkipped
				r.SentAt = time.Time{}
				r.LagMicros = 0
				r.Path = "/api/users"
			},
			want:   map[string]string{"@timestamp": "2026-10-03T09:00:00.123456789Z", "url.path": "/api/users"},
			absent: []string{"replay.sent_at", "replay.schedule_lag_us", "url.query"},
		},
		{
			name: "no times at all fall back to the record timestamp",
			mutate: func(r *diff.Result) {
				r.SentAt, r.StartedAt = time.Time{}, time.Time{}
				r.OriginalTraceID = ""
			},
			want:   map[string]string{"@timestamp": "2026-10-03T09:00:01.123456789Z"},
			absent: []string{"event.start", "trace"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := testDiffResult()
			tt.mutate(r)
			d, err := DiffDocument(r, testSource(nil), DefaultDiffsIndex)
			if err != nil {
				t.Fatal(err)
			}
			doc := decodeDoc(t, d.Body)
			expectFields(t, doc, tt.want)
			expectAbsent(t, doc, tt.absent...)
		})
	}
}

func TestDiffDocumentErrors(t *testing.T) {
	if _, err := DiffDocument(nil, testSource(nil), DefaultDiffsIndex); err == nil {
		t.Error("nil result accepted")
	}
	r := testDiffResult()
	r.RunID = ""
	if _, err := DiffDocument(r, testSource(nil), DefaultDiffsIndex); err == nil {
		t.Error("result without run id accepted")
	}
	r = testDiffResult()
	r.SentAt, r.StartedAt = time.Time{}, time.Time{}
	if _, err := DiffDocument(r, Source{}, DefaultDiffsIndex); err == nil {
		t.Error("result without any time accepted")
	}
}

func TestDecodeDiff(t *testing.T) {
	// The replayer's own encoder is the producer side of this contract.
	raw, err := report.EncodeResult(testDiffResult())
	if err != nil {
		t.Fatal(err)
	}
	r, err := DecodeDiff(raw)
	if err != nil {
		t.Fatal(err)
	}
	if r.RunID != fixtureRunID || r.CaptureID != fixtureCaptureID || len(r.Differences) != 3 || !r.SentAt.Equal(fixtureStart.Add(time.Hour)) {
		t.Errorf("decoded %+v", r)
	}
	for name, raw := range map[string][]byte{
		"garbage":      []byte("["),
		"wrong schema": []byte(`{"schema_version":2,"run_id":"r","capture_id":"c"}`),
		"no run id":    []byte(`{"schema_version":1,"capture_id":"c"}`),
		"no capture":   []byte(`{"schema_version":1,"run_id":"r"}`),
	} {
		if _, err := DecodeDiff(raw); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
