package tracequery

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var update = flag.Bool("update", false, "rewrite the golden files")

func dur(ns float64) time.Duration { return time.Duration(ns) }

// golden compares got with testdata/<name> and rewrites the file with -update.
func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s differs from the golden file (run with -update to accept):\n--- got ---\n%s\n--- want ---\n%s", name, got, want)
	}
}

func lookup(t *testing.T, q Query) *Result {
	t.Helper()
	_, srv := newFakeES(t)
	res, err := newTestClient(t, srv.URL).Lookup(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func TestRenderGolden(t *testing.T) {
	tests := []struct {
		name string
		q    Query
		opts RenderOptions
	}{
		{name: "trace.golden", q: Query{TraceID: originalTrace}},
		{name: "trace_bodies_follow.golden", q: Query{TraceID: originalTrace, FollowReplay: true}, opts: RenderOptions{ShowBodies: true}},
		{name: "replay.golden", q: Query{TraceID: replayTrace, FollowReplay: true}},
		{name: "capture_without_trace.golden", q: Query{RequestID: "req-notrace"}, opts: RenderOptions{ShowBodies: true}},
		{name: "capture_only.golden", q: Query{RequestID: "req-only-capture"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			if err := Render(&out, lookup(t, tt.q), tt.opts); err != nil {
				t.Fatal(err)
			}
			golden(t, tt.name, out.Bytes())
		})
	}
}

func TestRenderContents(t *testing.T) {
	var out bytes.Buffer
	if err := Render(&out, lookup(t, Query{TraceID: originalTrace, FollowReplay: true}), RenderOptions{}); err != nil {
		t.Fatal(err)
	}
	text := out.String()
	for _, want := range []string{
		"trace " + originalTrace,
		"services 7 (gateway.demo, shadowtap-tap, gateway, users.demo, users, orders.demo, orders)",
		"capture " + captureID,
		"redactions email:1,phone:1",
		"diff run-0199b400-5e6f-7a8b-9c0d-ef0123456789  class body_mismatch",
		"body /orders/0/total changed: prod 12.5 candidate 12.50",
		"[parent 9f8e7d6c5b4a3928 not indexed]",
		"ERROR (deadline exceeded)",
		"log  +19.400ms  WARN   orders  cache timeout, reading store  cache.key=user-orders:u-42",
		"status prod 200 candidate 200 baseline 200  latency prod 23.600ms candidate 29.000ms baseline 24.100ms",
		"[suppressed: noise]",
		"tap tap-0 (observe)",
		"logs without an indexed span",
		"accepted request",
		"replay trace " + replayTrace,
		"[link " + originalTrace + "/b2c3d4e5f6071829]",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("output lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "redacted.invalid") {
		t.Error("bodies printed without ShowBodies")
	}
	// Spans are in tree order: the gateway's users call precedes its orders
	// call, and each sidecar span precedes its app span. The search starts
	// after the waterfall heading, past the capture and diff summaries that
	// mention the same routes.
	spans := text[strings.Index(text, "\nspans\n"):]
	order := []string{"gateway.demo.svc.cluster.local:8080/*", "GET /api/users/{id}/orders", "GET users", "users.demo.svc", "GET /api/users/{id}", "GET orders", "orders.demo.svc", "GET /api/orders", "cache GET"}
	pos := 0
	for _, s := range order {
		i := strings.Index(spans[pos:], s)
		if i < 0 {
			t.Fatalf("%q does not follow the span above it in:\n%s", s, spans)
		}
		pos += i + len(s)
	}
}

func TestRouteLabel(t *testing.T) {
	tests := []struct {
		method, route, path, want string
	}{
		{"GET", "GET /api/users/{id}", "/api/users/42", "GET /api/users/{id}"},
		{"GET", "/api/users/{id}", "/api/users/42", "GET /api/users/{id}"},
		{"GET", "", "/api/users/42", "GET /api/users/42"},
		{"", "GET /api/users/{id}", "", "GET /api/users/{id}"},
		{"POST", "", "", "POST"},
		{"", "", "", "-"},
	}
	for _, tt := range tests {
		if got := routeLabel(tt.method, tt.route, tt.path); got != tt.want {
			t.Errorf("routeLabel(%q, %q, %q) = %q, want %q", tt.method, tt.route, tt.path, got, tt.want)
		}
	}
}

func TestWriteJSONGolden(t *testing.T) {
	var out bytes.Buffer
	if err := WriteJSON(&out, lookup(t, Query{RequestID: "req-8c1d2e3f", FollowReplay: true}), true); err != nil {
		t.Fatal(err)
	}
	var d Document
	if err := json.Unmarshal(out.Bytes(), &d); err != nil {
		t.Fatalf("output is not JSON: %v", err)
	}
	if d.TraceID != originalTrace || d.SpanCount != 11 || d.Spans == nil || !d.Spans.Synthetic || len(d.Spans.Children) != 2 || d.ResolvedBy == nil || d.ResolvedBy.RequestID != "req-8c1d2e3f" {
		t.Errorf("document = %+v", d)
	}
	if len(d.Related) != 1 || d.Related[0].Relation != "replay" || d.Related[0].Trace.Spans.Links[0].TraceID != originalTrace {
		t.Errorf("related = %+v", d.Related)
	}
	if d.Captures[0].Response.Content == "" {
		t.Error("bodies missing with bodies=true")
	}
	golden(t, "trace.json.golden", out.Bytes())

	out.Reset()
	if err := WriteJSON(&out, lookup(t, Query{TraceID: originalTrace}), false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "redacted.invalid") {
		t.Error("bodies included with bodies=false")
	}
}

func TestBar(t *testing.T) {
	total := 10 * 1e6 // 10ms in ns
	tests := []struct {
		offset, dur float64
		want        string
	}{
		{0, total, "|##########|"},
		{0, total / 2, "|#####     |"},
		{total / 2, total / 2, "|     #####|"},
		{0, 0, "|#         |"},                 // zero-length spans still show where they are
		{total, 0, "|         #|"},             // at the very end
		{total / 4, total * 2, "|  ########|"}, // longer than the trace is clipped
	}
	for _, tt := range tests {
		got := bar(dur(tt.offset), dur(tt.dur), t0, dur(total), 10)
		if got != tt.want {
			t.Errorf("bar(%v, %v) = %q, want %q", tt.offset, tt.dur, got, tt.want)
		}
	}
	if got := bar(0, 0, t0, 0, 4); got != "|    |" {
		t.Errorf("bar with zero total = %q", got)
	}
}

func TestFormatting(t *testing.T) {
	if got := fmtDuration(dur(1.5e6)); got != "1.500ms" {
		t.Errorf("fmtDuration = %q", got)
	}
	if got := fmtDuration(dur(12.5e9)); got != "12.500s" {
		t.Errorf("fmtDuration seconds = %q", got)
	}
	if got := fmtOffset(-dur(2e6)); got != "-2.000ms" {
		t.Errorf("fmtOffset negative = %q", got)
	}
	for in, want := range map[string]string{"plain": "plain", "": `""`, "two words": `"two words"`, `q"uote`: `"q\"uote"`} {
		if got := quote(in); got != want {
			t.Errorf("quote(%q) = %s, want %s", in, got, want)
		}
	}
}
