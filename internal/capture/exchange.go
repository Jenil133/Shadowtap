// Package capture defines the Shadowtap capture record: one HTTP request and
// response observed at a sidecar, after personal data has been scrubbed.
//
// Records are written to Kafka as JSON (schema version 1). The Kafka key is the
// trace id when present so every hop of a trace lands on the same partition, and
// the record timestamp is the request start so replays can seek by time.
package capture

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

// SchemaVersion is written to every Exchange and checked by Decode.
const SchemaVersion = 1

// DefaultTopic is the Kafka topic captures are published to.
const DefaultTopic = "shadowtap.captures.v1"

// Capture modes, matching how Envoy's ext_proc filter is configured.
const (
	// ModeObserve is ext_proc observability mode: Envoy streams data to the tap
	// and never waits for a reply.
	ModeObserve = "observe"
	// ModeInline is classic ext_proc: Envoy waits for a (no-op) reply to each
	// message.
	ModeInline = "inline"
)

// Reasons a body's data is not stored. The body's metadata is always kept.
const (
	OmittedBinary              = "binary"               // content type we do not scrub
	OmittedMultipart           = "multipart"            // multipart bodies may carry files
	OmittedUnparseable         = "unparseable"          // structured body failed to parse; fail closed
	OmittedUnsupportedEncoding = "unsupported_encoding" // content-encoding we cannot decode
	OmittedTruncatedUnsafe     = "truncated_unsafe"     // truncated body that cannot be scrubbed safely
	OmittedTooLarge            = "too_large"            // dropped so the record fits in Kafka
)

// Body data encodings.
const (
	EncodingUTF8   = "utf8"
	EncodingBase64 = "base64"
)

// Exchange is one captured request/response pair.
type Exchange struct {
	SchemaVersion int            `json:"schema_version"`
	ID            string         `json:"id"`      // UUIDv7, time ordered
	Service       string         `json:"service"` // service that received the request
	TraceID       string         `json:"trace_id,omitempty"`
	ParentSpanID  string         `json:"parent_span_id,omitempty"` // caller's span from the inbound trace context
	RequestID     string         `json:"request_id,omitempty"`     // x-request-id
	Source        Source         `json:"source"`
	Sampling      Sampling       `json:"sampling"`
	Timing        Timing         `json:"timing"`
	Request       Request        `json:"request"`
	Response      Response       `json:"response"`
	Redactions    map[string]int `json:"redactions,omitempty"` // redaction kind -> count; never values
}

// Source records which tap produced the exchange and how.
type Source struct {
	Tap  string `json:"tap"`  // tap instance, usually the pod name
	Mode string `json:"mode"` // ModeObserve or ModeInline
}

// Sampling records why the exchange was captured.
type Sampling struct {
	Rate      float64 `json:"rate"`
	Rule      string  `json:"rule"`
	KeySource string  `json:"key_source"` // header the sampling hash was computed from
}

// Timing is measured at the tap. DurationMicros approximates the time the
// service took to start responding.
type Timing struct {
	StartedAt         time.Time `json:"started_at"`
	ResponseStartedAt time.Time `json:"response_started_at,omitzero"`
	CompletedAt       time.Time `json:"completed_at,omitzero"`
	DurationMicros    int64     `json:"duration_us"`
}

// Request is the inbound request. Path is the scrubbed :path including the
// query string.
type Request struct {
	Method    string  `json:"method"`
	Scheme    string  `json:"scheme,omitempty"`
	Authority string  `json:"authority,omitempty"`
	Path      string  `json:"path"`
	Headers   Headers `json:"headers,omitempty"`
	Trailers  Headers `json:"trailers,omitempty"`
	Body      Body    `json:"body"`
}

// Response is the service's response.
type Response struct {
	Status   int     `json:"status"`
	Headers  Headers `json:"headers,omitempty"`
	Trailers Headers `json:"trailers,omitempty"`
	Body     Body    `json:"body"`
}

// Body holds a (scrubbed) message body and what happened to it.
type Body struct {
	ContentType string `json:"content_type,omitempty"`
	Size        int64  `json:"size"`               // bytes observed by the tap, including any not kept
	Encoding    string `json:"encoding,omitempty"` // EncodingUTF8 or EncodingBase64; empty without data
	Data        string `json:"data,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`    // only a prefix of the body was kept
	Omitted     string `json:"omitted,omitempty"`      // why Data is empty, see Omitted* constants
	DecodedFrom string `json:"decoded_from,omitempty"` // content-encoding removed before scrubbing
}

// SetData stores data as text when it is valid UTF-8 and as base64 otherwise.
// Empty data clears the body's data.
func (b *Body) SetData(data []byte) {
	switch {
	case len(data) == 0:
		b.Data, b.Encoding = "", ""
	case utf8.Valid(data):
		b.Data, b.Encoding = string(data), EncodingUTF8
	default:
		b.Data, b.Encoding = base64.StdEncoding.EncodeToString(data), EncodingBase64
	}
}

// Bytes returns the stored data.
func (b *Body) Bytes() ([]byte, error) {
	switch b.Encoding {
	case "", EncodingUTF8:
		return []byte(b.Data), nil
	case EncodingBase64:
		return base64.StdEncoding.DecodeString(b.Data)
	default:
		return nil, fmt.Errorf("capture: unknown body encoding %q", b.Encoding)
	}
}

// Omit drops the data and records why.
func (b *Body) Omit(reason string) {
	b.Data, b.Encoding, b.Omitted = "", "", reason
}

// Headers maps lowercase header names to their values in arrival order.
type Headers map[string][]string

// Get returns the first value of name, or "".
func (h Headers) Get(name string) string {
	if v := h[strings.ToLower(name)]; len(v) > 0 {
		return v[0]
	}
	return ""
}

// Add appends a value to name.
func (h Headers) Add(name, value string) {
	k := strings.ToLower(name)
	h[k] = append(h[k], value)
}

// Clone returns a deep copy of h.
func (h Headers) Clone() Headers {
	if h == nil {
		return nil
	}
	c := make(Headers, len(h))
	for k, v := range h {
		c[k] = append([]string(nil), v...)
	}
	return c
}

// NewID returns a new time-ordered capture id.
func NewID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}
