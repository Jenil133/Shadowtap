package telemetry

import (
	"context"
	"net/url"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// urlAttributes are the span attributes that carry a request's path or
// query. Paths and queries hold personal data (an email in a lookup URL),
// so they never leave the process: the exporter reduces them to the route
// template, which carries none, or to the host alone.
var urlAttributes = map[attribute.Key]bool{
	"url.full": true, "url.path": true, "url.query": true,
	"http.url": true, "http.target": true, "http.route_path": true,
}

// redactedValue is what a URL attribute becomes when no route is known.
const redactedValue = "/{redacted}"

// redactingExporter rewrites every span's URL attributes before the
// wrapped exporter sends it.
type redactingExporter struct {
	sdktrace.SpanExporter
}

// ExportSpans rewrites the URL attributes of each span and exports them.
func (e redactingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	out := make([]sdktrace.ReadOnlySpan, len(spans))
	for i, s := range spans {
		out[i] = redactSpan(s)
	}
	return e.SpanExporter.ExportSpans(ctx, out)
}

// redactedSpan is a span whose URL attributes have been replaced.
type redactedSpan struct {
	sdktrace.ReadOnlySpan
	attrs []attribute.KeyValue
}

// Attributes returns the span's attributes with URLs reduced.
func (s redactedSpan) Attributes() []attribute.KeyValue { return s.attrs }

// redactSpan returns s unchanged when it has no URL attribute, otherwise a
// view of it with each URL attribute reduced to the route (http.route when
// the span has one) and, for full URLs, the scheme and host.
func redactSpan(s sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	attrs := s.Attributes()
	touch := false
	route := ""
	for _, kv := range attrs {
		if kv.Key == "http.route" {
			route = kv.Value.AsString()
		}
		if urlAttributes[kv.Key] {
			touch = true
		}
	}
	if !touch {
		return s
	}
	replacement := redactedValue
	if route != "" {
		replacement = route
		if i := strings.IndexByte(route, ' '); i >= 0 {
			replacement = route[i+1:] // "GET /api/users/{id}" -> "/api/users/{id}"
		}
	}
	out := make([]attribute.KeyValue, len(attrs))
	for i, kv := range attrs {
		if !urlAttributes[kv.Key] {
			out[i] = kv
			continue
		}
		switch kv.Key {
		case "url.query":
			out[i] = kv.Key.String("")
		case "url.full", "http.url":
			out[i] = kv.Key.String(hostOnly(kv.Value.AsString()) + replacement)
		default:
			out[i] = kv.Key.String(replacement)
		}
	}
	return redactedSpan{ReadOnlySpan: s, attrs: out}
}

// hostOnly returns the scheme and host of a URL, or "" when it has none.
func hostOnly(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return ""
	}
	return u.Scheme + "://" + u.Host
}
