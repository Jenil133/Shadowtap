package index

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
)

// DecodeCapture parses a record of the captures topic and checks it is a
// complete capture.
func DecodeCapture(value []byte) (*capture.Exchange, error) {
	x, err := capture.Decode(value)
	if err != nil {
		return nil, err
	}
	if err := x.Validate(); err != nil {
		return nil, err
	}
	return x, nil
}

// CaptureDocument maps a capture to a document of index. The document id is
// the capture id.
//
// Document fields: @timestamp (request start), event.start/end,
// event.duration (time to response headers, ns), capture.id,
// capture.schema_version, capture.sampling.*, capture.timing.*, trace.id,
// parent.id (the caller's span), service.name, source.tap and source.mode
// (also as tap.name and tap.mode), http.route (the route template, e.g.
// "GET /api/users/{id}"),
// http.request.{id,method,mime_type,headers,trailers,body.*},
// http.response.{status_code,mime_type,headers,trailers,body.*},
// url.{path,query,original,scheme,domain,port}, redactions.<kind>,
// redactions_total, propagation.* from a traceparent record header and
// kafka.*. Body data is in http.*.body.content, stored but not indexed.
func CaptureDocument(x *capture.Exchange, src Source, index string) (*Document, error) {
	if x == nil {
		return nil, errors.New("index: nil capture")
	}
	if x.ID == "" {
		return nil, errors.New("index: capture has no id")
	}
	started := x.Timing.StartedAt
	if started.IsZero() {
		started = src.Timestamp
	}
	if started.IsZero() {
		return nil, fmt.Errorf("index: capture %s has no start time", x.ID)
	}
	doc := map[string]any{"@timestamp": timeField(started)}
	put(doc, "event.start", timeField(started))
	if !x.Timing.CompletedAt.IsZero() {
		put(doc, "event.end", timeField(x.Timing.CompletedAt))
	}
	if x.Timing.DurationMicros > 0 {
		put(doc, "event.duration", x.Timing.DurationMicros*1000)
	}

	capt := map[string]any{
		"id":             x.ID,
		"schema_version": x.SchemaVersion,
		"sampling": map[string]any{
			"rate":       x.Sampling.Rate,
			"rule":       x.Sampling.Rule,
			"key_source": x.Sampling.KeySource,
		},
	}
	timing := map[string]any{"duration_us": x.Timing.DurationMicros}
	if !x.Timing.ResponseStartedAt.IsZero() {
		timing["response_started_at"] = timeField(x.Timing.ResponseStartedAt)
	}
	if !x.Timing.CompletedAt.IsZero() {
		timing["completed_at"] = timeField(x.Timing.CompletedAt)
	}
	if total := x.Timing.TotalMicros(); total > 0 {
		timing["total_us"] = total
	}
	capt["timing"] = timing
	doc["capture"] = capt

	if x.TraceID != "" {
		put(doc, "trace.id", x.TraceID)
	}
	if x.ParentSpanID != "" {
		put(doc, "parent.id", x.ParentSpanID)
	}
	put(doc, "service.name", x.Service)
	doc["source"] = map[string]any{"tap": x.Source.Tap, "mode": x.Source.Mode}
	// The same under tap.*, which is where the trace CLI reads them.
	doc["tap"] = map[string]any{"name": x.Source.Tap, "mode": x.Source.Mode}

	req := map[string]any{"method": x.Request.Method}
	if x.RequestID != "" {
		req["id"] = x.RequestID
	}
	if mt := mimeType(x.Request.Body, x.Request.Headers); mt != "" {
		req["mime_type"] = mt
	}
	if h := headerFields(x.Request.Headers); h != nil {
		req["headers"] = h
	}
	if h := headerFields(x.Request.Trailers); h != nil {
		req["trailers"] = h
	}
	req["body"] = bodyFields(x.Request.Body)

	resp := map[string]any{"status_code": x.Response.Status}
	if mt := mimeType(x.Response.Body, x.Response.Headers); mt != "" {
		resp["mime_type"] = mt
	}
	if h := headerFields(x.Response.Headers); h != nil {
		resp["headers"] = h
	}
	if h := headerFields(x.Response.Trailers); h != nil {
		resp["trailers"] = h
	}
	resp["body"] = bodyFields(x.Response.Body)

	doc["http"] = map[string]any{
		"route":    diff.RouteTemplate(x.Request.Method, x.Request.Path),
		"request":  req,
		"response": resp,
	}
	doc["url"] = urlFields(x.Request.Scheme, x.Request.Authority, x.Request.Path)

	if len(x.Redactions) > 0 {
		total := 0
		red := make(map[string]any, len(x.Redactions))
		for kind, n := range x.Redactions {
			red[kind] = n
			total += n
		}
		doc["redactions"] = red
		doc["redactions_total"] = total
	}
	if p := src.propagation(); p != nil {
		doc["propagation"] = p
	}
	doc["kafka"] = src.fields()
	return newDocument(index, x.ID, doc, src)
}

// mimeType returns the body's content type, falling back to the header.
func mimeType(b capture.Body, h capture.Headers) string {
	if b.ContentType != "" {
		return b.ContentType
	}
	return h.Get("content-type")
}

// headerFields converts headers to a JSON object for a flattened field, or
// nil when there are none.
func headerFields(h capture.Headers) map[string]any {
	if len(h) == 0 {
		return nil
	}
	m := make(map[string]any, len(h))
	for k, v := range h {
		m[k] = v
	}
	return m
}

// bodyFields describes a body: its size and what happened to it, plus the
// data itself under content, which the mappings keep out of the index.
func bodyFields(b capture.Body) map[string]any {
	m := map[string]any{"bytes": b.Size}
	if b.Truncated {
		m["truncated"] = true
	}
	if b.Omitted != "" {
		m["omitted"] = b.Omitted
	}
	if b.Encoding != "" {
		m["encoding"] = b.Encoding
	}
	if b.DecodedFrom != "" {
		m["decoded_from"] = b.DecodedFrom
	}
	if b.Data != "" {
		m["content"] = b.Data
	}
	return m
}

// urlFields splits a captured :path into url.* fields.
func urlFields(scheme, authority, rawPath string) map[string]any {
	path, query, hasQuery := strings.Cut(rawPath, "?")
	m := map[string]any{"path": path, "original": rawPath}
	if hasQuery && query != "" {
		m["query"] = query
	}
	if scheme != "" {
		m["scheme"] = scheme
	}
	if authority != "" {
		host, port, err := net.SplitHostPort(authority)
		if err != nil {
			host = authority
		}
		m["domain"] = host
		if p, err := strconv.Atoi(port); err == nil {
			m["port"] = p
		}
	}
	return m
}
