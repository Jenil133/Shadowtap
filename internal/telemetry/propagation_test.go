package telemetry

import (
	"context"
	"slices"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

func header(k, v string) capture.RecordHeader {
	return capture.RecordHeader{Key: k, Value: []byte(v)}
}

func headersEqual(a, b []capture.RecordHeader) bool {
	return slices.EqualFunc(a, b, func(x, y capture.RecordHeader) bool {
		return x.Key == y.Key && string(x.Value) == string(y.Value)
	})
}

func TestHeaderCarrierSet(t *testing.T) {
	tests := []struct {
		name       string
		start      HeaderCarrier
		key, value string
		want       HeaderCarrier
	}{
		{
			name: "append to empty",
			key:  "traceparent", value: "tp",
			want: HeaderCarrier{header("traceparent", "tp")},
		},
		{
			name:  "append new key",
			start: HeaderCarrier{header("schema-version", "1")},
			key:   "traceparent", value: "tp",
			want: HeaderCarrier{header("schema-version", "1"), header("traceparent", "tp")},
		},
		{
			name:  "replace existing in place",
			start: HeaderCarrier{header("traceparent", "old"), header("service", "api")},
			key:   "traceparent", value: "new",
			want: HeaderCarrier{header("traceparent", "new"), header("service", "api")},
		},
		{
			name: "replace ignoring case and drop duplicates",
			start: HeaderCarrier{
				header("Traceparent", "1"), header("service", "api"),
				header("traceparent", "2"), header("TRACEPARENT", "3"), header("baggage", "b"),
			},
			key: "traceparent", value: "new",
			want: HeaderCarrier{header("traceparent", "new"), header("service", "api"), header("baggage", "b")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := slices.Clone(tt.start)
			c.Set(tt.key, tt.value)
			if !headersEqual(c, tt.want) {
				t.Errorf("Set(%q, %q) = %v, want %v", tt.key, tt.value, c, tt.want)
			}
			if got := c.Get(tt.key); got != tt.value {
				t.Errorf("Get(%q) after Set = %q, want %q", tt.key, got, tt.value)
			}
		})
	}

	var nilCarrier *HeaderCarrier
	nilCarrier.Set("k", "v") // must not panic
}

func TestHeaderCarrierGetKeys(t *testing.T) {
	c := HeaderCarrier{header("Traceparent", "upper"), header("traceparent", "lower"), header("service", "api")}
	tests := []struct {
		key  string
		want string
	}{
		{"traceparent", "lower"}, // exact match wins over a case-insensitive one
		{"TRACEPARENT", "upper"}, // first case-insensitive match
		{"Service", "api"},
		{"missing", ""},
	}
	for _, tt := range tests {
		if got := c.Get(tt.key); got != tt.want {
			t.Errorf("Get(%q) = %q, want %q", tt.key, got, tt.want)
		}
	}
	if got, want := c.Keys(), []string{"Traceparent", "traceparent", "service"}; !slices.Equal(got, want) {
		t.Errorf("Keys() = %v, want %v", got, want)
	}
	if got := HeaderCarrier(nil).Keys(); len(got) != 0 {
		t.Errorf("Keys() of nil = %v", got)
	}
}

func TestInjectExtractHeaders(t *testing.T) {
	otel.SetTextMapPropagator(newPropagator())
	ctx, ok := ContextWithRemoteParent(context.Background(), testTraceID, testSpanID, true)
	if !ok {
		t.Fatal("ContextWithRemoteParent rejected valid ids")
	}
	member, err := baggage.NewMember("tenant", "acme")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx = baggage.ContextWithBaggage(ctx, bag)

	in := []capture.RecordHeader{header(capture.HeaderSchemaVersion, "1"), header("traceparent", "stale")}
	out := InjectHeaders(ctx, in)
	if !headersEqual(in, []capture.RecordHeader{header(capture.HeaderSchemaVersion, "1"), header("traceparent", "stale")}) {
		t.Errorf("InjectHeaders modified its input: %v", in)
	}
	wantTraceparent := "00-" + testTraceID + "-" + testSpanID + "-01"
	if !headersEqual(out, []capture.RecordHeader{
		header(capture.HeaderSchemaVersion, "1"),
		header("traceparent", wantTraceparent),
		header("baggage", "tenant=acme"),
	}) {
		t.Fatalf("InjectHeaders = %v", out)
	}
	if again := InjectHeaders(ctx, out); !headersEqual(again, out) {
		t.Errorf("second InjectHeaders changed the headers: %v", again)
	}

	extracted := ExtractHeaders(context.Background(), out)
	sc := trace.SpanContextFromContext(extracted)
	if sc.TraceID().String() != testTraceID || sc.SpanID().String() != testSpanID || !sc.IsSampled() || !sc.IsRemote() {
		t.Errorf("extracted span context = %+v", sc)
	}
	if got := baggage.FromContext(extracted).Member("tenant").Value(); got != "acme" {
		t.Errorf("extracted baggage tenant = %q, want acme", got)
	}
	if tid, sid, ok := SpanIDs(extracted); !ok || tid != testTraceID || sid != testSpanID {
		t.Errorf("SpanIDs(extracted) = %q, %q, %v", tid, sid, ok)
	}

	// Headers in a different case still extract.
	upper := []capture.RecordHeader{header("Traceparent", wantTraceparent)}
	if sc := trace.SpanContextFromContext(ExtractHeaders(context.Background(), upper)); sc.TraceID().String() != testTraceID {
		t.Errorf("upper-case traceparent not extracted: %+v", sc)
	}

	// Without trace context in ctx nothing is added, and without headers
	// nothing is extracted.
	if got := InjectHeaders(context.Background(), in); !headersEqual(got, in) {
		t.Errorf("InjectHeaders without a span = %v, want %v", got, in)
	}
	if _, _, ok := SpanIDs(ExtractHeaders(context.Background(), nil)); ok {
		t.Error("ExtractHeaders of nil produced a span context")
	}
}

func TestInjectHeadersNoopPropagator(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())
	t.Cleanup(func() { otel.SetTextMapPropagator(newPropagator()) })
	ctx, _ := ContextWithRemoteParent(context.Background(), testTraceID, testSpanID, true)
	in := []capture.RecordHeader{header("service", "api")}
	if got := InjectHeaders(ctx, in); !headersEqual(got, in) {
		t.Errorf("InjectHeaders with a no-op propagator = %v, want %v", got, in)
	}
}

func TestRemoteParentAndLink(t *testing.T) {
	zeros := func(n int) string { return strings.Repeat("0", n) }
	tests := []struct {
		name            string
		traceID, spanID string
		ok              bool
	}{
		{name: "valid", traceID: testTraceID, spanID: testSpanID, ok: true},
		{name: "upper case normalised", traceID: strings.ToUpper(testTraceID), spanID: strings.ToUpper(testSpanID), ok: true},
		{name: "trace id short", traceID: testTraceID[:31], spanID: testSpanID},
		{name: "trace id long", traceID: testTraceID + "0", spanID: testSpanID},
		{name: "span id short", traceID: testTraceID, spanID: testSpanID[:15]},
		{name: "span id long", traceID: testTraceID, spanID: testSpanID + "0"},
		{name: "trace id not hex", traceID: "zz" + testTraceID[2:], spanID: testSpanID},
		{name: "span id not hex", traceID: testTraceID, spanID: "g" + testSpanID[1:]},
		{name: "zero trace id", traceID: zeros(32), spanID: testSpanID},
		{name: "zero span id", traceID: testTraceID, spanID: zeros(16)},
		{name: "empty", traceID: "", spanID: ""},
		{name: "whitespace", traceID: " " + testTraceID[1:], spanID: testSpanID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bg := context.Background()
			for _, sampled := range []bool{true, false} {
				ctx, ok := ContextWithRemoteParent(bg, tt.traceID, tt.spanID, sampled)
				if ok != tt.ok {
					t.Fatalf("ContextWithRemoteParent ok = %v, want %v", ok, tt.ok)
				}
				if !ok {
					if ctx != bg {
						t.Error("rejected ids changed the context")
					}
					if _, _, ok := SpanIDs(ctx); ok {
						t.Error("SpanIDs found a span after rejection")
					}
					continue
				}
				sc := trace.SpanContextFromContext(ctx)
				if sc.TraceID().String() != testTraceID || sc.SpanID().String() != testSpanID {
					t.Errorf("span context ids = %s/%s, want %s/%s", sc.TraceID(), sc.SpanID(), testTraceID, testSpanID)
				}
				if sc.IsSampled() != sampled || !sc.IsRemote() {
					t.Errorf("span context sampled=%v remote=%v, want sampled=%v remote=true", sc.IsSampled(), sc.IsRemote(), sampled)
				}
				if tid, sid, ok := SpanIDs(ctx); !ok || tid != testTraceID || sid != testSpanID {
					t.Errorf("SpanIDs = %q, %q, %v", tid, sid, ok)
				}
			}

			link, ok := LinkTo(tt.traceID, tt.spanID)
			if ok != tt.ok {
				t.Fatalf("LinkTo ok = %v, want %v", ok, tt.ok)
			}
			if !ok {
				if link.SpanContext.IsValid() {
					t.Error("rejected LinkTo returned a valid span context")
				}
				return
			}
			if link.SpanContext.TraceID().String() != testTraceID || link.SpanContext.SpanID().String() != testSpanID {
				t.Errorf("link ids = %s/%s", link.SpanContext.TraceID(), link.SpanContext.SpanID())
			}
			if link.SpanContext.TraceFlags() != 0 || !link.SpanContext.IsRemote() {
				t.Errorf("link flags=%v remote=%v, want clear flags and remote", link.SpanContext.TraceFlags(), link.SpanContext.IsRemote())
			}
		})
	}
}

func TestLinkRecordedOnSpan(t *testing.T) {
	ctx := testContext(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })

	link, ok := LinkTo(testTraceID, testSpanID)
	if !ok {
		t.Fatal("LinkTo rejected valid ids")
	}
	spanCtx, span := tp.Tracer("test").Start(ctx, "replay", trace.WithLinks(link))
	tid, sid, ok := SpanIDs(spanCtx)
	if !ok || tid != span.SpanContext().TraceID().String() || sid != span.SpanContext().SpanID().String() {
		t.Errorf("SpanIDs of recording span = %q, %q, %v", tid, sid, ok)
	}
	if tid == testTraceID {
		t.Error("linked span joined the original trace instead of starting a new one")
	}
	span.End()

	spans := exp.GetSpans()
	if len(spans) != 1 || len(spans[0].Links) != 1 {
		t.Fatalf("exported spans = %+v, want one with one link", spans)
	}
	if got := spans[0].Links[0].SpanContext.TraceID().String(); got != testTraceID {
		t.Errorf("link trace id = %s, want %s", got, testTraceID)
	}
}
