package index

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"

	"github.com/Jenil133/Shadowtap/internal/tracectx"
)

// Source identifies the Kafka record a document was built from. Every
// document carries it under kafka.*, so a document can be traced back to the
// record and a partition can be reindexed by resetting offsets.
type Source struct {
	Topic     string
	Partition int32
	Offset    int64
	// Timestamp is the record timestamp; it stands in for a missing event
	// time.
	Timestamp time.Time
	// Headers holds the record headers; for a repeated key the first value
	// wins. A traceparent header is indexed as propagation.* metadata.
	Headers map[string]string
}

func sourceOf(r *kgo.Record) Source {
	return Source{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp, Headers: headersOf(r)}
}

// fields returns the kafka.* object of a document.
func (s Source) fields() map[string]any {
	m := map[string]any{
		"topic":     s.Topic,
		"partition": s.Partition,
		"offset":    s.Offset,
	}
	if !s.Timestamp.IsZero() {
		m["timestamp"] = timeField(s.Timestamp)
	}
	return m
}

// propagation returns the propagation.* object built from the record's
// traceparent header, or nil when there is none.
func (s Source) propagation() map[string]any {
	tc, ok := s.traceContext()
	if !ok {
		return nil
	}
	return map[string]any{
		"traceparent": strings.TrimSpace(s.Headers[headerTraceparent]),
		"trace":       map[string]any{"id": tc.TraceID},
		"span":        map[string]any{"id": tc.SpanID},
		"sampled":     tc.Sampled,
	}
}

// Record header names the producers write. The traceparent is looked up
// case-insensitively because headers that began life as HTTP headers may
// arrive in any case.
const (
	headerTraceparent = "traceparent"
	headerTraceID     = "trace-id"
)

// traceContext parses the record's traceparent header.
func (s Source) traceContext() (tracectx.Context, bool) {
	raw, ok := s.Headers[headerTraceparent]
	if !ok {
		for k, v := range s.Headers {
			if strings.EqualFold(k, headerTraceparent) {
				raw, ok = v, true
				break
			}
		}
	}
	if !ok {
		return tracectx.Context{}, false
	}
	return tracectx.ParseTraceparent(raw)
}

// traceID returns the trace id the record's headers announce: the W3C
// traceparent first, then the trace-id header that capture and diff records
// carry. It is "" when neither names a valid id.
func (s Source) traceID() string {
	if tc, ok := s.traceContext(); ok {
		return tc.TraceID
	}
	id := strings.ToLower(strings.TrimSpace(s.Headers[headerTraceID]))
	if len(id) != 2*traceIDBytes || !isHex(id) {
		return ""
	}
	return id
}

func isHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// Document is one Elasticsearch document to create.
type Document struct {
	Index string
	// ID is deterministic for the record it came from, so that indexing the
	// same record twice conflicts instead of duplicating.
	ID string
	// Body is the JSON source.
	Body   []byte
	Source Source
}

// Rejected is a record, or one span or log of a record, that could not be
// turned into a document.
type Rejected struct {
	Err error
	// Body is the partial document, when one could be built; it is kept in
	// the DLQ record so the data is not lost.
	Body []byte
}

// newDocument encodes body as the document's source.
func newDocument(index, id string, body map[string]any, src Source) (*Document, error) {
	b, err := encodeJSON(body)
	if err != nil {
		return nil, err
	}
	return &Document{Index: index, ID: id, Body: b, Source: src}, nil
}

// encodeJSON returns the compact JSON encoding of v without HTML escaping.
func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("index: encode document: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// put sets the value at a dotted path, creating intermediate objects:
// put(m, "http.request.method", "GET") yields {"http":{"request":{"method":"GET"}}}.
// A segment that already holds a non-object value is replaced by an object.
func put(m map[string]any, path string, v any) {
	segs := strings.Split(path, ".")
	for _, s := range segs[:len(segs)-1] {
		next, ok := m[s].(map[string]any)
		if !ok {
			next = map[string]any{}
			m[s] = next
		}
		m = next
	}
	m[segs[len(segs)-1]] = v
}

// timeField formats a time for a date or date_nanos field.
func timeField(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

// nanosField formats a Unix nanosecond timestamp for a date_nanos field. It
// reports false for zero (unknown) or out-of-range values.
func nanosField(ns uint64) (string, bool) {
	if ns == 0 || ns > math.MaxInt64 {
		return "", false
	}
	return timeField(time.Unix(0, int64(ns))), true
}

// hexID returns the lowercase hex form of an OTLP id, or false when the id
// does not have the expected size or is all zeroes (invalid per OTLP).
func hexID(b []byte, size int) (string, bool) {
	if len(b) != size {
		return "", false
	}
	zero := true
	for _, c := range b {
		if c != 0 {
			zero = false
			break
		}
	}
	if zero {
		return "", false
	}
	return hex.EncodeToString(b), true
}

// anyValue converts an OTLP attribute value to its JSON form: strings,
// bools and numbers as themselves, bytes as base64, arrays as lists and
// key-value lists as objects. An empty value is nil.
func anyValue(v *commonv1.AnyValue) any {
	switch x := v.GetValue().(type) {
	case *commonv1.AnyValue_StringValue:
		return x.StringValue
	case *commonv1.AnyValue_BoolValue:
		return x.BoolValue
	case *commonv1.AnyValue_IntValue:
		return x.IntValue
	case *commonv1.AnyValue_DoubleValue:
		if math.IsNaN(x.DoubleValue) || math.IsInf(x.DoubleValue, 0) {
			return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64) // JSON has no NaN
		}
		return x.DoubleValue
	case *commonv1.AnyValue_BytesValue:
		return base64.StdEncoding.EncodeToString(x.BytesValue)
	case *commonv1.AnyValue_ArrayValue:
		vals := x.ArrayValue.GetValues()
		out := make([]any, 0, len(vals))
		for _, e := range vals {
			out = append(out, anyValue(e))
		}
		return out
	case *commonv1.AnyValue_KvlistValue:
		return attributeMap(x.KvlistValue.GetValues())
	default:
		return nil
	}
}

// attributeMap converts OTLP attributes to a JSON object. Keys are kept as
// is (dots included): the attributes fields are flattened in the mappings,
// so a key never becomes a mapped path.
func attributeMap(kvs []*commonv1.KeyValue) map[string]any {
	if len(kvs) == 0 {
		return nil
	}
	m := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		if kv == nil || kv.GetKey() == "" {
			continue
		}
		m[kv.GetKey()] = anyValue(kv.GetValue())
	}
	return m
}

// stringAttr returns the first of keys present in m, as a string.
func stringAttr(m map[string]any, keys ...string) (string, bool) {
	for _, k := range keys {
		switch v := m[k].(type) {
		case string:
			if v != "" {
				return v, true
			}
		case int64:
			return strconv.FormatInt(v, 10), true
		case float64:
			return strconv.FormatFloat(v, 'g', -1, 64), true
		case bool:
			return strconv.FormatBool(v), true
		}
	}
	return "", false
}

// intAttr returns the first of keys present in m, as an integer; numeric
// strings are accepted because older instrumentation emitted them.
func intAttr(m map[string]any, keys ...string) (int64, bool) {
	for _, k := range keys {
		switch v := m[k].(type) {
		case int64:
			return v, true
		case float64:
			if v == math.Trunc(v) && math.Abs(v) < math.MaxInt64 {
				return int64(v), true
			}
		case string:
			if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
				return n, true
			}
		}
	}
	return 0, false
}
