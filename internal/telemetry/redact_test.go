package telemetry

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestRedactingExporter(t *testing.T) {
	inner := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(redactingExporter{inner}))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tracer := tp.Tracer("test")

	_, server := tracer.Start(context.Background(), "GET /api/users/lookup/{email}", traceAttrs(
		attribute.String("http.route", "GET /api/users/lookup/{email}"),
		attribute.String("url.path", "/api/users/lookup/alice@example.com"),
		attribute.String("url.query", "token=s3cret"),
		attribute.Int("http.response.status_code", 200),
	))
	server.End()
	_, client := tracer.Start(context.Background(), "HTTP GET", traceAttrs(
		attribute.String("url.full", "http://demo-users:8080/api/users/lookup/alice@example.com?x=1"),
	))
	client.End()
	_, plain := tracer.Start(context.Background(), "shadowtap.capture", traceAttrs(attribute.String("shadowtap.service", "demo-api")))
	plain.End()

	spans := inner.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("%d spans", len(spans))
	}
	for _, s := range spans {
		for _, kv := range s.Attributes {
			if v := kv.Value.Emit(); strings.Contains(v, "alice") || strings.Contains(v, "s3cret") {
				t.Errorf("span %s exported %s=%q", s.Name, kv.Key, v)
			}
		}
	}
	want := map[string]map[attribute.Key]string{
		"GET /api/users/lookup/{email}": {"url.path": "/api/users/lookup/{email}", "url.query": "", "http.route": "GET /api/users/lookup/{email}"},
		"HTTP GET":                      {"url.full": "http://demo-users:8080" + redactedValue},
	}
	for _, s := range spans {
		for key, v := range want[s.Name] {
			found := false
			for _, kv := range s.Attributes {
				if kv.Key == key {
					found = true
					if kv.Value.AsString() != v {
						t.Errorf("span %s: %s = %q, want %q", s.Name, key, kv.Value.AsString(), v)
					}
				}
			}
			if !found {
				t.Errorf("span %s lost %s", s.Name, key)
			}
		}
		if s.Name == "GET /api/users/lookup/{email}" && len(s.Attributes) != 4 {
			t.Errorf("attribute count changed: %v", s.Attributes)
		}
	}
}

func traceAttrs(kv ...attribute.KeyValue) trace.SpanStartOption { return trace.WithAttributes(kv...) }
