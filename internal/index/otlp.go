package index

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	collogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
	logsv1 "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcev1 "go.opentelemetry.io/proto/otlp/resource/v1"
	tracev1 "go.opentelemetry.io/proto/otlp/trace/v1"
)

// OTLP id sizes.
const (
	traceIDBytes = 16
	spanIDBytes  = 8
)

// unknownService is the service.name of telemetry whose resource has none,
// as the OpenTelemetry SDKs spell it.
const unknownService = "unknown_service"

// DecodeSpans parses a record of the spans topic: one OTLP
// ExportTraceServiceRequest in protobuf encoding, as the Collector's Kafka
// exporter writes with encoding otlp_proto.
func DecodeSpans(value []byte) (*coltracev1.ExportTraceServiceRequest, error) {
	var req coltracev1.ExportTraceServiceRequest
	if err := proto.Unmarshal(value, &req); err != nil {
		return nil, fmt.Errorf("index: decode ExportTraceServiceRequest: %w", err)
	}
	return &req, nil
}

// DecodeLogs parses a record of the logs topic: one OTLP
// ExportLogsServiceRequest in protobuf encoding.
func DecodeLogs(value []byte) (*collogsv1.ExportLogsServiceRequest, error) {
	var req collogsv1.ExportLogsServiceRequest
	if err := proto.Unmarshal(value, &req); err != nil {
		return nil, fmt.Errorf("index: decode ExportLogsServiceRequest: %w", err)
	}
	return &req, nil
}

// SpanDocuments maps every span of req to a document of index. A span whose
// trace id or span id is invalid cannot get a deterministic id and is
// returned as rejected with the document that was built for it.
//
// Document fields: @timestamp (start, date_nanos), event.duration (ns),
// event.end, trace.id, trace.state, span.id, span.name, span.kind
// (INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER), span.flags, parent.id,
// service.name/version/namespace/instance.id and the other resource
// attributes under resource.attributes, scope.name/version, status.code
// (UNSET, OK, ERROR) and status.message, http.request.method,
// http.response.status_code, http.route, url.path/query/full/scheme,
// server.address/port and user_agent.original promoted from both the
// stable and the pre-1.21 semantic conventions, every attribute under
// attributes, links[{trace.id, span.id, trace.state, attributes}],
// events[{name, time, attributes}], dropped.{attributes,events,links} and
// kafka.{topic,partition,offset,timestamp}.
func SpanDocuments(req *coltracev1.ExportTraceServiceRequest, src Source, index string) ([]*Document, []Rejected) {
	var (
		docs     []*Document
		rejected []Rejected
	)
	for _, rs := range req.GetResourceSpans() {
		res := newResource(rs.GetResource())
		for _, ss := range rs.GetScopeSpans() {
			scope := scopeFields(ss.GetScope())
			for _, span := range ss.GetSpans() {
				if span == nil {
					continue
				}
				doc, err := spanDocument(span, res, scope, src, index)
				if err != nil {
					rejected = append(rejected, Rejected{Err: err, Body: doc.Body})
					continue
				}
				docs = append(docs, doc)
			}
		}
	}
	return docs, rejected
}

// LogDocuments maps every log record of req to a document of index. Log
// records have no natural id, so the document id is a hash of the record's
// identifying fields (trace and span id, timestamps, severity, body, event
// name, service and attributes); the same record delivered twice hashes the
// same.
//
// Document fields: @timestamp (date_nanos; the observed time, then the
// record timestamp, when the event time is unknown), message (the body as
// text), body.structured (a non-string body), log.level (severity text, or
// the name of the severity number), log.severity_number,
// log.observed_timestamp, log.flags, event.name, trace.id, span.id,
// service.*, resource.attributes, scope.*, attributes, dropped.attributes
// and kafka.*.
func LogDocuments(req *collogsv1.ExportLogsServiceRequest, src Source, index string) ([]*Document, []Rejected) {
	var (
		docs     []*Document
		rejected []Rejected
	)
	for _, rl := range req.GetResourceLogs() {
		res := newResource(rl.GetResource())
		for _, sl := range rl.GetScopeLogs() {
			scope := scopeFields(sl.GetScope())
			for _, rec := range sl.GetLogRecords() {
				if rec == nil {
					continue
				}
				doc, err := logDocument(rec, res, scope, src, index)
				if err != nil {
					rejected = append(rejected, Rejected{Err: err})
					continue
				}
				docs = append(docs, doc)
			}
		}
	}
	return docs, rejected
}

// resource is a decoded OTLP resource: the service identity promoted to
// ECS fields plus the complete attribute map.
type resource struct {
	service    map[string]any
	attributes map[string]any
	dropped    uint32
}

func newResource(r *resourcev1.Resource) resource {
	attrs := attributeMap(r.GetAttributes())
	service := map[string]any{}
	if name, ok := stringAttr(attrs, "service.name"); ok {
		service["name"] = name
	} else {
		service["name"] = unknownService
	}
	if v, ok := stringAttr(attrs, "service.version"); ok {
		service["version"] = v
	}
	if v, ok := stringAttr(attrs, "service.namespace"); ok {
		service["namespace"] = v
	}
	if v, ok := stringAttr(attrs, "service.instance.id"); ok {
		service["instance"] = map[string]any{"id": v}
	}
	return resource{service: service, attributes: attrs, dropped: r.GetDroppedAttributesCount()}
}

// apply writes the resource into doc.
func (r resource) apply(doc map[string]any) {
	doc["service"] = r.service
	if len(r.attributes) > 0 {
		put(doc, "resource.attributes", r.attributes)
	}
	if r.dropped > 0 {
		put(doc, "resource.dropped_attributes", r.dropped)
	}
}

// serviceName returns the promoted service name.
func (r resource) serviceName() string {
	name, _ := r.service["name"].(string)
	return name
}

// instanceID returns the promoted service instance id, or "".
func (r resource) instanceID() string {
	inst, _ := r.service["instance"].(map[string]any)
	id, _ := inst["id"].(string)
	return id
}

// scopeFields returns the scope.* object, or nil for an unknown scope.
func scopeFields(s *commonv1.InstrumentationScope) map[string]any {
	if s == nil || (s.GetName() == "" && s.GetVersion() == "") {
		return nil
	}
	m := map[string]any{}
	if s.GetName() != "" {
		m["name"] = s.GetName()
	}
	if s.GetVersion() != "" {
		m["version"] = s.GetVersion()
	}
	return m
}

// spanDocument builds one span's document. When the ids are invalid the
// document is returned together with the error, so the caller can keep its
// body.
func spanDocument(span *tracev1.Span, res resource, scope map[string]any, src Source, index string) (*Document, error) {
	doc := map[string]any{}
	traceID, traceOK := hexID(span.GetTraceId(), traceIDBytes)
	spanID, spanOK := hexID(span.GetSpanId(), spanIDBytes)
	if traceOK {
		put(doc, "trace.id", traceID)
	}
	if spanOK {
		put(doc, "span.id", spanID)
	}
	if parent, ok := hexID(span.GetParentSpanId(), spanIDBytes); ok {
		put(doc, "parent.id", parent)
	}
	if ts, ok := nanosField(span.GetStartTimeUnixNano()); ok {
		doc["@timestamp"] = ts
	} else if !src.Timestamp.IsZero() {
		doc["@timestamp"] = timeField(src.Timestamp)
	}
	if end, ok := nanosField(span.GetEndTimeUnixNano()); ok {
		put(doc, "event.end", end)
		if span.GetEndTimeUnixNano() >= span.GetStartTimeUnixNano() && span.GetStartTimeUnixNano() > 0 {
			put(doc, "event.duration", span.GetEndTimeUnixNano()-span.GetStartTimeUnixNano())
		}
	}
	put(doc, "span.name", span.GetName())
	put(doc, "span.kind", strings.TrimPrefix(span.GetKind().String(), "SPAN_KIND_"))
	if span.GetFlags() != 0 {
		put(doc, "span.flags", span.GetFlags())
	}
	if span.GetTraceState() != "" {
		put(doc, "trace.state", span.GetTraceState())
	}
	res.apply(doc)
	if scope != nil {
		doc["scope"] = scope
	}
	status := map[string]any{"code": strings.TrimPrefix(span.GetStatus().GetCode().String(), "STATUS_CODE_")}
	if msg := span.GetStatus().GetMessage(); msg != "" {
		status["message"] = msg
	}
	doc["status"] = status

	attrs := attributeMap(span.GetAttributes())
	if len(attrs) > 0 {
		doc["attributes"] = attrs
		promoteHTTP(doc, attrs)
	}
	if links := span.GetLinks(); len(links) > 0 {
		out := make([]any, 0, len(links))
		for _, l := range links {
			link := map[string]any{}
			if id, ok := hexID(l.GetTraceId(), traceIDBytes); ok {
				put(link, "trace.id", id)
			}
			if id, ok := hexID(l.GetSpanId(), spanIDBytes); ok {
				put(link, "span.id", id)
			}
			if l.GetTraceState() != "" {
				put(link, "trace.state", l.GetTraceState())
			}
			if a := attributeMap(l.GetAttributes()); len(a) > 0 {
				link["attributes"] = a
			}
			out = append(out, link)
		}
		doc["links"] = out
	}
	if events := span.GetEvents(); len(events) > 0 {
		out := make([]any, 0, len(events))
		for _, e := range events {
			ev := map[string]any{"name": e.GetName()}
			if ts, ok := nanosField(e.GetTimeUnixNano()); ok {
				ev["time"] = ts
			}
			if a := attributeMap(e.GetAttributes()); len(a) > 0 {
				ev["attributes"] = a
			}
			out = append(out, ev)
		}
		doc["events"] = out
	}
	dropped := map[string]any{}
	if n := span.GetDroppedAttributesCount(); n > 0 {
		dropped["attributes"] = n
	}
	if n := span.GetDroppedEventsCount(); n > 0 {
		dropped["events"] = n
	}
	if n := span.GetDroppedLinksCount(); n > 0 {
		dropped["links"] = n
	}
	if len(dropped) > 0 {
		doc["dropped"] = dropped
	}
	doc["kafka"] = src.fields()

	id := traceID + "-" + spanID
	d, err := newDocument(index, id, doc, src)
	if err != nil {
		return &Document{Index: index, Source: src}, err
	}
	switch {
	case !traceOK && !spanOK:
		return d, errors.New("index: span has no valid trace id and span id")
	case !traceOK:
		return d, fmt.Errorf("index: span %s has no valid trace id", spanID)
	case !spanOK:
		return d, fmt.Errorf("index: span of trace %s has no valid span id", traceID)
	}
	return d, nil
}

// promoteHTTP copies the well-known HTTP and URL attributes to typed ECS
// fields. Both the stable semantic conventions (http.request.method,
// url.path, server.address, ...) and the names instrumentation used before
// semconv 1.21 (http.method, http.target, net.host.name, ...) are read, so
// spans from older sidecars and libraries land in the same fields. The
// attributes map is left complete.
func promoteHTTP(doc, attrs map[string]any) {
	if v, ok := stringAttr(attrs, "http.request.method", "http.method"); ok {
		put(doc, "http.request.method", v)
	}
	if v, ok := intAttr(attrs, "http.response.status_code", "http.status_code"); ok {
		put(doc, "http.response.status_code", v)
	}
	if v, ok := stringAttr(attrs, "http.route"); ok {
		put(doc, "http.route", v)
	}
	path, pathOK := stringAttr(attrs, "url.path")
	query, queryOK := stringAttr(attrs, "url.query")
	if !pathOK {
		if target, ok := stringAttr(attrs, "http.target"); ok {
			p, q, hasQuery := strings.Cut(target, "?")
			path, pathOK = p, p != ""
			if !queryOK && hasQuery && q != "" {
				query, queryOK = q, true
			}
		}
	}
	if pathOK {
		put(doc, "url.path", path)
	}
	if queryOK {
		put(doc, "url.query", query)
	}
	if v, ok := stringAttr(attrs, "url.full", "http.url"); ok {
		put(doc, "url.full", v)
	}
	if v, ok := stringAttr(attrs, "url.scheme", "http.scheme"); ok {
		put(doc, "url.scheme", v)
	}
	if v, ok := stringAttr(attrs, "server.address", "net.host.name"); ok {
		put(doc, "server.address", v)
	}
	if v, ok := intAttr(attrs, "server.port", "net.host.port"); ok {
		put(doc, "server.port", v)
	}
	if v, ok := stringAttr(attrs, "user_agent.original", "http.user_agent"); ok {
		put(doc, "user_agent.original", v)
	}
}

// logDocument builds one log record's document.
func logDocument(rec *logsv1.LogRecord, res resource, scope map[string]any, src Source, index string) (*Document, error) {
	doc := map[string]any{}
	traceID, _ := hexID(rec.GetTraceId(), traceIDBytes)
	spanID, _ := hexID(rec.GetSpanId(), spanIDBytes)
	if traceID != "" {
		put(doc, "trace.id", traceID)
	}
	if spanID != "" {
		put(doc, "span.id", spanID)
	}
	// The event time is best; the Collector always sets the observed time;
	// the record timestamp is the last resort.
	switch ts, ok := nanosField(rec.GetTimeUnixNano()); {
	case ok:
		doc["@timestamp"] = ts
	default:
		if obs, ok := nanosField(rec.GetObservedTimeUnixNano()); ok {
			doc["@timestamp"] = obs
		} else if !src.Timestamp.IsZero() {
			doc["@timestamp"] = timeField(src.Timestamp)
		}
	}
	if obs, ok := nanosField(rec.GetObservedTimeUnixNano()); ok {
		put(doc, "log.observed_timestamp", obs)
	}
	level := rec.GetSeverityText()
	if level == "" {
		level = severityName(rec.GetSeverityNumber())
	}
	if level != "" {
		put(doc, "log.level", level)
	}
	if n := rec.GetSeverityNumber(); n != logsv1.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED {
		put(doc, "log.severity_number", int32(n))
	}
	if rec.GetFlags() != 0 {
		put(doc, "log.flags", rec.GetFlags())
	}
	if rec.GetEventName() != "" {
		put(doc, "event.name", rec.GetEventName())
	}
	message, structured := logBody(rec.GetBody())
	doc["message"] = message
	if structured != nil {
		put(doc, "body.structured", structured)
	}
	res.apply(doc)
	if scope != nil {
		doc["scope"] = scope
	}
	attrs := attributeMap(rec.GetAttributes())
	if len(attrs) > 0 {
		doc["attributes"] = attrs
	}
	if n := rec.GetDroppedAttributesCount(); n > 0 {
		put(doc, "dropped.attributes", n)
	}
	doc["kafka"] = src.fields()

	attrJSON, err := encodeJSON(attrs)
	if err != nil {
		return nil, err
	}
	id := logID(traceID, spanID, rec, message, res.serviceName(), res.instanceID(), attrJSON)
	return newDocument(index, id, doc, src)
}

// logBody renders the body as the message text and, when the body is not a
// plain string, as a structured value too.
func logBody(body *commonv1.AnyValue) (message string, structured any) {
	switch v := anyValue(body).(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	default:
		b, err := encodeJSON(v)
		if err != nil {
			return fmt.Sprint(v), v
		}
		return string(b), v
	}
}

// logID hashes the fields that identify a log record; a redelivered record
// yields the same id while two records that differ in any of them do not.
func logID(traceID, spanID string, rec *logsv1.LogRecord, message, service, instance string, attrs []byte) string {
	h := sha256.New()
	for _, part := range []string{
		traceID, spanID,
		strconv.FormatUint(rec.GetTimeUnixNano(), 10),
		strconv.FormatUint(rec.GetObservedTimeUnixNano(), 10),
		strconv.FormatInt(int64(rec.GetSeverityNumber()), 10),
		rec.GetSeverityText(),
		rec.GetEventName(),
		message,
		service, instance,
	} {
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	h.Write(attrs)
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// severityName maps an OTLP severity number to its range name (TRACE,
// DEBUG, INFO, WARN, ERROR, FATAL), or "" for unspecified.
func severityName(n logsv1.SeverityNumber) string {
	switch {
	case n <= 0 || n > 24:
		return ""
	case n <= 4:
		return "TRACE"
	case n <= 8:
		return "DEBUG"
	case n <= 12:
		return "INFO"
	case n <= 16:
		return "WARN"
	case n <= 20:
		return "ERROR"
	default:
		return "FATAL"
	}
}
