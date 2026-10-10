package tracequery

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"
)

// Indices the indexer writes and this package reads. Spans and logs are data
// streams (one per namespace, hence the wildcard); captures and diffs are
// plain indices.
const (
	IndexTraces   = "traces-shadowtap-*"
	IndexLogs     = "logs-shadowtap-*"
	IndexCaptures = "shadowtap-captures"
	IndexDiffs    = "shadowtap-diffs"
)

// Document field names, following ECS and the OpenTelemetry semantic
// conventions. They are the contract between the indexer (internal/index,
// which nests them: {"trace":{"id":...}}) and this package, which also
// accepts the dotted spelling ({"trace.id":...}) so a document written by
// another tool still resolves.
//
// Fields shared by every document kind.
const (
	FieldTimestamp   = "@timestamp"
	FieldTraceID     = "trace.id"
	FieldSpanID      = "span.id"
	FieldParentID    = "parent.id"
	FieldServiceName = "service.name"
	FieldAttributes  = "attributes"
	FieldHTTPMethod  = "http.request.method"
	FieldHTTPRoute   = "http.route"
	FieldURLPath     = "url.path"
	FieldHTTPStatus  = "http.response.status_code"
	FieldCaptureID   = "capture.id"
)

// Span fields (traces-shadowtap-*).
const (
	FieldSpanName      = "span.name"
	FieldSpanKind      = "span.kind"      // INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER
	FieldDuration      = "event.duration" // nanoseconds
	FieldStatusCode    = "status.code"    // UNSET, OK, ERROR
	FieldStatusMessage = "status.message"
	FieldLinks         = "links" // [{trace.id, span.id}]
)

// Log fields (logs-shadowtap-*).
const (
	FieldLogLevel = "log.level"
	FieldMessage  = "message"
)

// Capture fields (shadowtap-captures). Bodies are kept in _source only. The
// tap's identity is written under source.* and, for readers of the earlier
// spelling, under tap.*; the first present wins.
const (
	FieldRequestID  = "http.request.id"
	FieldTapName    = "source.tap"
	FieldTapNameAlt = "tap.name"
	FieldTapMode    = "source.mode"
	FieldTapModeAlt = "tap.mode"
	FieldRedactions = "redactions" // kind -> count

	FieldRequestBody      = "http.request.body.content"
	FieldRequestBodyBytes = "http.request.body.bytes"
	FieldRequestBodyType  = "http.request.mime_type"
	FieldRequestOmitted   = "http.request.body.omitted"
	FieldRequestTruncated = "http.request.body.truncated"

	FieldResponseBody      = "http.response.body.content"
	FieldResponseBodyBytes = "http.response.body.bytes"
	FieldResponseBodyType  = "http.response.mime_type"
	FieldResponseOmitted   = "http.response.body.omitted"
	FieldResponseTruncated = "http.response.body.truncated"
)

// Diff fields (shadowtap-diffs). @timestamp is when the replayed request was
// sent; the original request's start is event.start. trace.id is the
// original request's trace and replay.trace.id the replay's, so a diff is
// found from either side. The candidate's status and latency are spelled
// like production's under replay.* (http.response.status_code,
// event.duration in ns); the replayer's own microsecond latencies under
// latency.* and replay.status_code are read when those are absent.
const (
	FieldEventStart          = "event.start"
	FieldRunID               = "replay.run.id"
	FieldReplayTraceID       = "replay.trace.id"
	FieldReplaySentAt        = "replay.sent_at"
	FieldReplayLag           = "replay.schedule_lag_us"
	FieldReplayStatus        = "replay.http.response.status_code"
	FieldReplayStatusAlt     = "replay.status_code"
	FieldReplayDuration      = "replay.event.duration" // nanoseconds
	FieldBaselineStatus      = "replay.baseline.status_code"
	FieldDiffClass           = "diff.class"
	FieldDiffReason          = "diff.reason"
	FieldDiffDifferences     = "diff.differences" // [{where, pointer, kind, prod, candidate, detail, suppressed}]
	FieldDiffTruncated       = "diff.differences_truncated"
	FieldDiffReportedCount   = "diff.reported_count"
	FieldDiffSuppressedCount = "diff.suppressed_count"
	FieldLatencyProd         = "latency.prod_us" // microseconds
	FieldLatencyCandidate    = "latency.candidate_us"
	FieldLatencyBaseline     = "latency.baseline_us"
)

// doc is a decoded _source. Lookups take dotted paths and resolve them
// whether the document nests objects or uses dotted keys, so the query side
// does not depend on which shape the indexer chose.
type doc map[string]any

func (d doc) get(path string) (any, bool) {
	if v, ok := d[path]; ok {
		return v, true
	}
	for i := 0; i < len(path); i++ {
		if path[i] != '.' {
			continue
		}
		if sub, ok := d[path[:i]].(map[string]any); ok {
			if v, ok := doc(sub).get(path[i+1:]); ok {
				return v, true
			}
		}
	}
	return nil, false
}

// str returns the field as a string; numbers and booleans are formatted,
// anything else is "".
func (d doc) str(path string) string {
	v, ok := d.get(path)
	if !ok {
		return ""
	}
	switch v := v.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	}
	return ""
}

// firstStr returns the first of the paths that holds a non-empty string.
func (d doc) firstStr(paths ...string) string {
	for _, p := range paths {
		if s := d.str(p); s != "" {
			return s
		}
	}
	return ""
}

// firstNum returns the first of the paths that holds a non-zero number.
func (d doc) firstNum(paths ...string) int64 {
	for _, p := range paths {
		if n := d.num(p); n != 0 {
			return n
		}
	}
	return 0
}

// latency reads a duration stored in nanoseconds at nsPath, falling back to
// microseconds at usPath.
func (d doc) latency(nsPath, usPath string) time.Duration {
	if ns := d.num(nsPath); ns != 0 {
		return time.Duration(ns)
	}
	return time.Duration(d.num(usPath)) * time.Microsecond
}

// num returns the field as an integer; strings holding numbers are accepted.
func (d doc) num(path string) int64 {
	v, ok := d.get(path)
	if !ok {
		return 0
	}
	switch v := v.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return roundInt(f)
		}
	case float64:
		return roundInt(v)
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return roundInt(f)
		}
	}
	return 0
}

func roundInt(f float64) int64 {
	if math.IsNaN(f) || math.IsInf(f, 0) || f > math.MaxInt64 || f < math.MinInt64 {
		return 0
	}
	return int64(math.Round(f))
}

func (d doc) boolean(path string) bool {
	v, ok := d.get(path)
	if !ok {
		return false
	}
	switch v := v.(type) {
	case bool:
		return v
	case string:
		return strings.EqualFold(v, "true")
	}
	return false
}

// when returns the field as a time: RFC 3339 text, or epoch milliseconds
// (Elasticsearch's other common date form). The zero time means absent or
// unparseable.
func (d doc) when(path string) time.Time {
	v, ok := d.get(path)
	if !ok {
		return time.Time{}
	}
	switch v := v.(type) {
	case string:
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			return t.UTC()
		}
	case json.Number:
		if ms, err := v.Int64(); err == nil {
			return time.UnixMilli(ms).UTC()
		}
	case float64:
		return time.UnixMilli(roundInt(v)).UTC()
	}
	return time.Time{}
}

func (d doc) list(path string) []any {
	v, _ := d.get(path)
	l, _ := v.([]any)
	return l
}

func (d doc) object(path string) doc {
	v, _ := d.get(path)
	m, _ := v.(map[string]any)
	return doc(m)
}

// flatten returns the object at path as dotted key -> scalar text, for
// attribute maps that may be nested or flattened.
func (d doc) flatten(path string) map[string]string {
	m := d.object(path)
	if len(m) == 0 {
		return nil
	}
	out := map[string]string{}
	var walk func(prefix string, m map[string]any)
	walk = func(prefix string, m map[string]any) {
		for k, v := range m {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			if sub, ok := v.(map[string]any); ok {
				walk(key, sub)
				continue
			}
			out[key] = scalarString(v)
		}
	}
	walk("", m)
	return out
}

func scalarString(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case nil:
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
