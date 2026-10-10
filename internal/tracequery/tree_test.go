package tracequery

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)

func span(id, parent string, offsetMS, durMS int) *Span {
	return &Span{
		TraceID:  originalTrace,
		SpanID:   id,
		ParentID: parent,
		Name:     "span-" + id,
		Service:  "svc",
		Start:    t0.Add(time.Duration(offsetMS) * time.Millisecond),
		Duration: time.Duration(durMS) * time.Millisecond,
	}
}

// outline renders the tree as "id(children...)" for compact assertions.
func outline(s *Span) string {
	var b strings.Builder
	var rec func(s *Span)
	rec = func(s *Span) {
		switch {
		case s.Synthetic:
			b.WriteString("ROOT")
		case s.SpanID == "":
			b.WriteString("_")
		default:
			b.WriteString(s.SpanID)
		}
		if s.Orphan {
			b.WriteString("?")
		}
		if len(s.Children) == 0 {
			return
		}
		b.WriteString("(")
		for i, c := range s.Children {
			if i > 0 {
				b.WriteString(" ")
			}
			rec(c)
		}
		b.WriteString(")")
	}
	rec(s)
	return b.String()
}

func TestBuildTree(t *testing.T) {
	tests := []struct {
		name  string
		spans []*Span
		logs  []Log
		want  string
		// wantStart/wantDur check the synthetic root's extent.
		wantDur time.Duration
	}{
		{
			name: "no spans",
			want: "",
		},
		{
			name:  "single root",
			spans: []*Span{span("a", "", 0, 10), span("b", "a", 1, 5), span("c", "a", 2, 3)},
			want:  "a(b c)",
		},
		{
			name:  "out of order input sorts children by start",
			spans: []*Span{span("c", "a", 6, 2), span("d", "b", 3, 1), span("b", "a", 2, 3), span("a", "", 0, 10)},
			want:  "a(b(d) c)",
		},
		{
			name:    "missing parent becomes an orphan under a synthetic root",
			spans:   []*Span{span("a", "", 0, 10), span("x", "missing", 4, 2), span("y", "x", 5, 1)},
			want:    "ROOT(a x?(y))",
			wantDur: 10 * time.Millisecond,
		},
		{
			name:    "several roots",
			spans:   []*Span{span("b", "", 5, 10), span("a", "", 0, 3)},
			want:    "ROOT(a b)",
			wantDur: 15 * time.Millisecond,
		},
		{
			name:    "root is the orphan",
			spans:   []*Span{span("a", "client", 0, 10), span("b", "a", 1, 2)},
			want:    "ROOT(a?(b))",
			wantDur: 10 * time.Millisecond,
		},
		{
			name:  "duplicate documents collapse",
			spans: []*Span{span("a", "", 0, 10), span("b", "a", 1, 2), span("b", "a", 1, 2)},
			want:  "a(b)",
		},
		{
			name:    "parent cycle is surfaced, not lost",
			spans:   []*Span{span("r", "", 0, 10), span("p", "q", 1, 2), span("q", "p", 2, 2)},
			want:    "ROOT(r p?(q))",
			wantDur: 10 * time.Millisecond,
		},
		{
			name:  "self parent is a root",
			spans: []*Span{span("a", "a", 0, 10), span("b", "a", 1, 1)},
			want:  "a(b)",
		},
		{
			name:  "span without an id hangs off its parent as a leaf",
			spans: []*Span{span("a", "", 0, 10), span("", "a", 1, 1)},
			want:  "a(_)",
		},
		{
			name:    "span without an id or a known parent is an orphan",
			spans:   []*Span{span("a", "", 0, 10), span("", "missing", 1, 1)},
			want:    "ROOT(a _?)",
			wantDur: 10 * time.Millisecond,
		},
		{
			name:    "equal starts order by id",
			spans:   []*Span{span("b", "", 0, 1), span("a", "", 0, 2)},
			want:    "ROOT(a b)",
			wantDur: 2 * time.Millisecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := buildTree(tt.spans, tt.logs)
			got := ""
			if root != nil {
				got = outline(root)
			}
			if got != tt.want {
				t.Fatalf("tree = %s, want %s", got, tt.want)
			}
			if root != nil && root.Synthetic {
				if root.Duration != tt.wantDur || !root.Start.Equal(t0) {
					t.Errorf("synthetic root extent = %s from %s, want %s from %s", root.Duration, root.Start, tt.wantDur, t0)
				}
			}
		})
	}
}

func TestBuildTreeAttachesLogs(t *testing.T) {
	spans := []*Span{span("a", "", 0, 10), span("b", "a", 1, 2)}
	logs := []Log{
		{SpanID: "b", Message: "in b", Time: t0.Add(time.Millisecond)},
		{SpanID: "a", Message: "in a", Time: t0},
		{SpanID: "zzz", Message: "unknown span"},
		{Message: "no span"},
	}
	root := buildTree(spans, logs)
	if len(root.Logs) != 1 || root.Logs[0].Message != "in a" || len(root.Children[0].Logs) != 1 || root.Children[0].Logs[0].Message != "in b" {
		t.Errorf("logs: root %+v child %+v", root.Logs, root.Children[0].Logs)
	}
	tr := assemble(originalTrace, spans, logs, nil, nil)
	if un := tr.UnattachedLogs(); len(un) != 2 {
		t.Errorf("unattached = %+v", un)
	}
	// Rebuilding is idempotent: no duplicated children or logs.
	root = buildTree(spans, logs)
	if len(root.Children) != 1 || len(root.Logs) != 1 {
		t.Errorf("second build: children %d logs %d", len(root.Children), len(root.Logs))
	}
}

func TestNormalize(t *testing.T) {
	kinds := map[string]string{"": "", "SERVER": "SERVER", "Server": "SERVER", "SPAN_KIND_CLIENT": "CLIENT", "2": "SERVER", "5": "CONSUMER", "0": "", "weird": "WEIRD"}
	for in, want := range kinds {
		if got := normalizeKind(in); got != want {
			t.Errorf("normalizeKind(%q) = %q, want %q", in, got, want)
		}
	}
	statuses := map[string]string{"": "", "Unset": "", "0": "", "Ok": "OK", "STATUS_CODE_OK": "OK", "1": "OK", "Error": "ERROR", "2": "ERROR"}
	for in, want := range statuses {
		if got := normalizeStatus(in); got != want {
			t.Errorf("normalizeStatus(%q) = %q, want %q", in, got, want)
		}
	}
}

// Documents written before the indexer settled on its field names spell the
// tap's identity and the candidate's status and latency differently; both
// generations decode alike.
func TestAlternateSpellings(t *testing.T) {
	current := doc{
		"source": map[string]any{"tap": "tap-0", "mode": "observe"},
		"event":  map[string]any{"duration": json.Number("23600000")},
		"replay": map[string]any{"http": map[string]any{"response": map[string]any{"status_code": json.Number("500")}}, "event": map[string]any{"duration": json.Number("29000000")}},
	}
	earlier := doc{
		"tap":     map[string]any{"name": "tap-0", "mode": "observe"},
		"replay":  map[string]any{"status_code": json.Number("500")},
		"latency": map[string]any{"prod_us": json.Number("23600"), "candidate_us": json.Number("29000")},
	}
	for name, d := range map[string]doc{"current": current, "earlier": earlier} {
		if c := captureFromDoc(d); c.Tap != "tap-0" || c.Mode != "observe" {
			t.Errorf("%s capture: tap %q mode %q", name, c.Tap, c.Mode)
		}
		if df := diffFromDoc(d); df.CandidateStatus != 500 || df.ProdLatency != 23600*time.Microsecond || df.CandidateLatency != 29*time.Millisecond {
			t.Errorf("%s diff: status %d prod %s candidate %s", name, df.CandidateStatus, df.ProdLatency, df.CandidateLatency)
		}
	}
}

func TestDocLookup(t *testing.T) {
	d := doc{
		"trace.id":   "abc",
		"span":       map[string]any{"id": "s1", "kind": "SERVER"},
		"http":       map[string]any{"response.status_code": "200", "request": map[string]any{"method": "GET"}},
		"event":      map[string]any{"duration": "1500000.0"},
		"@timestamp": "2026-10-03T09:00:00.000400Z",
		"ms":         float64(1759482000000),
		"attributes": map[string]any{"a": map[string]any{"b": true}, "c": []any{"x", "y"}},
	}
	if d.str("trace.id") != "abc" || d.str("span.id") != "s1" || d.str("http.request.method") != "GET" {
		t.Errorf("str lookups failed: %q %q %q", d.str("trace.id"), d.str("span.id"), d.str("http.request.method"))
	}
	if d.num("http.response.status_code") != 200 || d.num("event.duration") != 1500000 || d.num("missing") != 0 {
		t.Errorf("num lookups: %d %d", d.num("http.response.status_code"), d.num("event.duration"))
	}
	if got := d.when("@timestamp"); !got.Equal(t0.Add(400 * time.Microsecond)) {
		t.Errorf("when = %s", got)
	}
	if got := d.when("ms"); got.IsZero() || got.Year() != 2025 {
		t.Errorf("epoch millis = %s", got)
	}
	if !d.when("missing").IsZero() || !d.when("span.kind").IsZero() {
		t.Error("unparseable times are not zero")
	}
	flat := d.flatten("attributes")
	if flat["a.b"] != "true" || flat["c"] != `["x","y"]` {
		t.Errorf("flatten = %v", flat)
	}
}
