package tap

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/tap/taptest"
)

// newTestTracer returns a provider that hands every finished span to the
// returned exporter as soon as it ends.
func newTestTracer(t *testing.T) (*sdktrace.TracerProvider, *tracetest.InMemoryExporter) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("shutdown tracer provider: %v", err)
		}
	})
	return tp, exp
}

// onlySpan returns the one span exp recorded.
func onlySpan(t *testing.T, exp *tracetest.InMemoryExporter) tracetest.SpanStub {
	t.Helper()
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}
	return spans[0]
}

// attrValues indexes a span's attributes by key.
func attrValues(s tracetest.SpanStub) map[attribute.Key]attribute.Value {
	m := make(map[attribute.Key]attribute.Value, len(s.Attributes))
	for _, kv := range s.Attributes {
		m[kv.Key] = kv.Value
	}
	return m
}

// spanRecorder is a slog handler that counts the records it gets and how
// many of them were logged under a valid span.
type spanRecorder struct {
	slog.Handler
	records, withSpan atomic.Int64
}

func (r *spanRecorder) Handle(ctx context.Context, rec slog.Record) error {
	r.records.Add(1)
	if trace.SpanContextFromContext(ctx).IsValid() {
		r.withSpan.Add(1)
	}
	return r.Handler.Handle(ctx, rec)
}

func TestCaptureSpanJoinsRequestTrace(t *testing.T) {
	for _, mode := range allModes() {
		t.Run(mode, func(t *testing.T) {
			tp, exp := newTestTracer(t)
			h := newHarnessWith(t, testConfig(), []Option{WithTracerProvider(tp)}, nil, func(h *harness) {
				h.scrubber.redactions = map[string]int{"email": 2, "phone": 1, "card": 0}
			})
			h.run(postJSON("/api/users?ref=home"), taptest.Options{Mode: mode})
			x := onlyExchange(t, h.drain())

			span := onlySpan(t, exp)
			if span.Name != captureSpanName || span.SpanKind != trace.SpanKindConsumer {
				t.Errorf("span = %q kind %v, want %q consumer", span.Name, span.SpanKind, captureSpanName)
			}
			if span.InstrumentationScope.Name != tracerName {
				t.Errorf("scope = %q, want %q", span.InstrumentationScope.Name, tracerName)
			}
			// The capture joins the trace of the request it was taken from.
			if got := span.SpanContext.TraceID().String(); got != x.TraceID || x.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
				t.Errorf("trace id = %s, want the request's %s", got, x.TraceID)
			}
			if got := span.Parent.SpanID().String(); got != x.ParentSpanID || x.ParentSpanID != "00f067aa0ba902b7" || !span.Parent.IsRemote() {
				t.Errorf("parent = %s (remote %v), want the request's span %s", got, span.Parent.IsRemote(), x.ParentSpanID)
			}
			if !span.SpanContext.IsSampled() {
				t.Error("capture span is not sampled")
			}
			// The sink was handed the span's context.
			if got := h.sink.spanContext(x.ID); !got.Equal(span.SpanContext) {
				t.Errorf("sink got span context %+v, want %+v", got, span.SpanContext)
			}

			want := map[attribute.Key]attribute.Value{
				attrService:                 attribute.StringValue("demo-api"),
				attrCaptureID:               attribute.StringValue(x.ID),
				attrMode:                    attribute.StringValue(mode),
				attrSamplingRule:            attribute.StringValue("checkout"),
				attrSamplingRate:            attribute.Float64Value(0.25),
				"http.request.method":       attribute.StringValue("POST"),
				"http.response.status_code": attribute.IntValue(201),
				"url.path":                  attribute.StringValue("/api/users?ref=home"),
				attrRedactions:              attribute.IntValue(3),
				attrOutcome:                 attribute.StringValue(outcomePublished),
			}
			got := attrValues(span)
			for k, v := range want {
				if g, ok := got[k]; !ok || g != v {
					t.Errorf("attribute %s = %v (present %v), want %v", k, g.Emit(), ok, v.Emit())
				}
			}
			if len(got) != len(want) {
				t.Errorf("%d attributes, want %d: %v", len(got), len(want), span.Attributes)
			}
			if span.Status.Code != codes.Ok {
				t.Errorf("status = %+v, want ok", span.Status)
			}
			if span.EndTime.Before(span.StartTime) {
				t.Errorf("span not ended: %+v", span)
			}
		})
	}
}

func TestCaptureSpanWithoutTraceContextStartsTrace(t *testing.T) {
	tp, exp := newTestTracer(t)
	h := newHarnessWith(t, testConfig(), []Option{WithTracerProvider(tp)}, nil)
	ex := postJSON("/api/orders")
	ex.RequestHeaders = [][2]string{{"content-type", "application/json"}} // no traceparent
	h.run(ex, taptest.Options{})
	x := onlyExchange(t, h.drain())
	if x.TraceID != "" || x.ParentSpanID != "" {
		t.Fatalf("capture has trace context %s/%s", x.TraceID, x.ParentSpanID)
	}

	span := onlySpan(t, exp)
	if !span.SpanContext.IsValid() {
		t.Errorf("span context %+v is not valid", span.SpanContext)
	}
	if span.Parent.IsValid() {
		t.Errorf("span has parent %+v, want a root", span.Parent)
	}
	if got := attrValues(span); got[attrCaptureID].AsString() != x.ID || got[attrOutcome].AsString() != outcomePublished {
		t.Errorf("attributes = %v", span.Attributes)
	}
}

func TestCaptureSpanOutcomes(t *testing.T) {
	const path = "/api/fail"
	tests := []struct {
		outcome string
		setup   func(*harness)
		mutate  func(*taptest.Exchange)
	}{
		{outcomeSinkError, func(h *harness) { h.sink.err = errBackpressure }, nil},
		{outcomeInvalid, nil, func(ex *taptest.Exchange) { ex.Status = 0 }},
		{outcomePanic, func(h *harness) { h.sink.panicOn = path }, nil},
	}
	for _, tt := range tests {
		t.Run(tt.outcome, func(t *testing.T) {
			tp, exp := newTestTracer(t)
			logs := &spanRecorder{Handler: testLogger(t).Handler()}
			var setup []func(*harness)
			if tt.setup != nil {
				setup = append(setup, tt.setup)
			}
			h := newHarnessWith(t, testConfig(), []Option{WithTracerProvider(tp)}, slog.New(logs), setup...)
			ex := postJSON(path)
			if tt.mutate != nil {
				tt.mutate(&ex)
			}
			h.run(ex, taptest.Options{})
			if got := h.drain(); len(got) != 0 {
				t.Fatalf("published %d exchanges", len(got))
			}
			if got := h.exchanges("demo-api", tt.outcome); got != 1 {
				t.Errorf("exchanges_total{outcome=%s} = %v", tt.outcome, got)
			}

			span := onlySpan(t, exp)
			if got := attrValues(span)[attrOutcome].AsString(); got != tt.outcome {
				t.Errorf("shadowtap.outcome = %q, want %q", got, tt.outcome)
			}
			if span.Status.Code != codes.Error || span.Status.Description == "" {
				t.Errorf("status = %+v, want an error with a description", span.Status)
			}
			// The description is fixed text: nothing from the request.
			if strings.Contains(span.Status.Description, path) || strings.Contains(span.Status.Description, errBackpressure.Error()) {
				t.Errorf("status description %q leaks request data or the sink error", span.Status.Description)
			}
			if span.Parent.SpanID().String() != "00f067aa0ba902b7" {
				t.Errorf("parent = %s, want the request's span", span.Parent.SpanID())
			}
			// The failure was logged under the span, so the line correlates.
			if logs.records.Load() == 0 || logs.withSpan.Load() != logs.records.Load() {
				t.Errorf("%d of %d log records carried the span", logs.withSpan.Load(), logs.records.Load())
			}
		})
	}
}

func TestNoTracerProviderMeansNoSpans(t *testing.T) {
	// Without WithTracerProvider the tap uses the global provider, which
	// nothing in this process sets up: spans cost nothing and record nothing.
	srv, err := New(testConfig(), &fakeSampler{}, newFakeScrubber(), &fakeSink{t: t}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := srv.Shutdown(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	x := &capture.Exchange{ID: capture.NewID(), TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", ParentSpanID: "00f067aa0ba902b7"}
	ctx, span := srv.startCaptureSpan(context.Background(), x)
	defer span.End()
	if span.IsRecording() {
		t.Error("span records without a tracer provider")
	}
	// The request's context still travels, so a sink can forward it.
	if got := trace.SpanContextFromContext(ctx); got.TraceID().String() != x.TraceID {
		t.Errorf("context carries trace %s, want %s", got.TraceID(), x.TraceID)
	}
}
