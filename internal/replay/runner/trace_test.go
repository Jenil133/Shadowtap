package runner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/replay/schedule"
)

const (
	originalTrace  = "4bf92f3577b34da6a3ce929d0e0e4736"
	originalParent = "00f067aa0ba902b7"
)

// recordingServer answers every request with the same body and keeps the
// traceparent headers it received.
func recordingServer(t *testing.T) (*url.URL, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("traceparent"))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":1}`))
	}))
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return u, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func TestReplayTraceLinksToOriginal(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	x := exchange(0, "GET", "/api/users/1", 200, `{"id":1}`)
	x.TraceID, x.ParentSpanID = originalTrace, originalParent
	candidate, candidateSeen := recordingServer(t)
	baseline, baselineSeen := recordingServer(t)
	out, err := Run(context.Background(), Config{
		RunID:         "run-trace",
		Candidate:     candidate,
		Baseline:      baseline,
		Engine:        engine(t),
		Schedule:      schedule.Options{Speed: 0, Concurrency: 1},
		NoNoiseProbes: true,
		Tracer:        tp.Tracer("test"),
	}, &sliceSource{xs: []*capture.Exchange{x}})
	if err != nil {
		t.Fatal(err)
	}
	res := out.Results[0]

	spans := map[string]tracetest.SpanStub{}
	for _, s := range exporter.GetSpans() {
		spans[s.Name] = s
	}
	root, ok := spans["shadowtap.replay"]
	if !ok {
		t.Fatalf("no replay span; got %v", names(exporter.GetSpans()))
	}
	// A replay is a new trace that links back to the captured request's.
	if root.SpanContext.TraceID().String() == originalTrace || root.Parent.IsValid() {
		t.Errorf("replay span continues the original trace: %v parent %v", root.SpanContext, root.Parent)
	}
	if len(root.Links) != 1 || root.Links[0].SpanContext.TraceID().String() != originalTrace || root.Links[0].SpanContext.SpanID().String() != originalParent {
		t.Errorf("links = %+v, want one to %s/%s", root.Links, originalTrace, originalParent)
	}
	if root.SpanKind != trace.SpanKindInternal {
		t.Errorf("replay span kind %v", root.SpanKind)
	}

	cand, ok := spans["shadowtap.replay.candidate"]
	if !ok || cand.Parent.SpanID() != root.SpanContext.SpanID() || cand.SpanKind != trace.SpanKindClient {
		t.Fatalf("candidate span = %+v", cand)
	}
	// The candidate request carried the replay trace, never the original.
	sent := candidateSeen()
	if len(sent) != 1 {
		t.Fatalf("candidate saw %d requests", len(sent))
	}
	wantPrefix := "00-" + cand.SpanContext.TraceID().String() + "-" + cand.SpanContext.SpanID().String() + "-"
	if !strings.HasPrefix(sent[0], wantPrefix) {
		t.Errorf("candidate traceparent %q, want prefix %q", sent[0], wantPrefix)
	}
	if strings.Contains(sent[0], originalTrace) {
		t.Error("candidate request carried the original trace id")
	}
	if res.ReplayTraceID != cand.SpanContext.TraceID().String() {
		t.Errorf("ReplayTraceID %q, want %q", res.ReplayTraceID, cand.SpanContext.TraceID())
	}
	base, ok := spans["shadowtap.replay.baseline"]
	if !ok || base.Parent.SpanID() != root.SpanContext.SpanID() {
		t.Fatalf("baseline span = %+v", base)
	}
	if b := baselineSeen(); len(b) != 1 || !strings.Contains(b[0], base.SpanContext.TraceID().String()) {
		t.Errorf("baseline traceparent %v, want trace %s", b, base.SpanContext.TraceID())
	}
	for _, s := range exporter.GetSpans() {
		for _, a := range s.Attributes {
			if v := a.Value.AsString(); strings.Contains(v, "1") && a.Key == "url.full" {
				t.Errorf("span %s carries %s", s.Name, a.Key)
			}
		}
	}
}

// Without a tracer provider the replay behaves as before: random traceparents,
// nothing exported.
func TestReplayWithoutTracer(t *testing.T) {
	x := exchange(0, "GET", "/api/users/1", 200, `{"id":1}`)
	x.TraceID = originalTrace
	candidate, seen := recordingServer(t)
	out, err := Run(context.Background(), Config{
		RunID:     "run-plain",
		Candidate: candidate,
		Engine:    engine(t),
		Schedule:  schedule.Options{Speed: 0, Concurrency: 1},
	}, &sliceSource{xs: []*capture.Exchange{x}})
	if err != nil {
		t.Fatal(err)
	}
	sent := seen()
	if len(sent) != 1 || !strings.HasPrefix(sent[0], "00-") || strings.Contains(sent[0], originalTrace) {
		t.Fatalf("traceparent = %v", sent)
	}
	if out.Results[0].ReplayTraceID == "" || out.Results[0].ReplayTraceID == originalTrace {
		t.Errorf("ReplayTraceID = %q", out.Results[0].ReplayTraceID)
	}
}

func names(spans tracetest.SpanStubs) []string {
	var out []string
	for _, s := range spans {
		out = append(out, s.Name)
	}
	return out
}
