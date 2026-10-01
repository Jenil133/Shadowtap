package capture

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var (
	// ErrTooLarge is returned by Fit when an exchange cannot be made small enough.
	ErrTooLarge = errors.New("capture: exchange exceeds max record size")
	// ErrUnsupportedSchema is returned by Decode for unknown schema versions.
	ErrUnsupportedSchema = errors.New("capture: unsupported schema version")
)

// Kafka record header names and values.
const (
	HeaderSchemaVersion = "schema-version"
	HeaderContentType   = "content-type"
	HeaderService       = "service"
	HeaderTraceID       = "trace-id"

	ContentTypeJSON = "application/json"
)

// RecordHeader is a Kafka record header, kept free of any client library type.
type RecordHeader struct {
	Key   string
	Value []byte
}

// Encode returns the compact JSON encoding of x without HTML escaping.
func Encode(x *Exchange) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(x); err != nil {
		return nil, fmt.Errorf("capture: encode: %w", err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// Decode parses an encoded exchange and checks its schema version.
func Decode(data []byte) (*Exchange, error) {
	var x Exchange
	if err := json.Unmarshal(data, &x); err != nil {
		return nil, fmt.Errorf("capture: decode: %w", err)
	}
	if x.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedSchema, x.SchemaVersion)
	}
	return &x, nil
}

// Fit encodes x, dropping body data (response first, then request) until the
// encoding is at most max bytes. x is updated to match what was encoded. A max
// of zero or less disables the limit.
func (x *Exchange) Fit(max int) ([]byte, error) {
	b, err := Encode(x)
	if err != nil || max <= 0 || len(b) <= max {
		return b, err
	}
	for _, body := range []*Body{&x.Response.Body, &x.Request.Body} {
		if body.Data == "" {
			continue
		}
		body.Omit(OmittedTooLarge)
		if b, err = Encode(x); err != nil || len(b) <= max {
			return b, err
		}
	}
	return nil, fmt.Errorf("%w: %d > %d bytes", ErrTooLarge, len(b), max)
}

// Key returns the Kafka record key: trace id, then request id, then capture id.
func (x *Exchange) Key() []byte {
	switch {
	case x.TraceID != "":
		return []byte(x.TraceID)
	case x.RequestID != "":
		return []byte(x.RequestID)
	default:
		return []byte(x.ID)
	}
}

// RecordHeaders returns the Kafka headers that let consumers route records
// without decoding them.
func (x *Exchange) RecordHeaders() []RecordHeader {
	h := []RecordHeader{
		{Key: HeaderSchemaVersion, Value: []byte(strconv.Itoa(SchemaVersion))},
		{Key: HeaderContentType, Value: []byte(ContentTypeJSON)},
		{Key: HeaderService, Value: []byte(x.Service)},
	}
	if x.TraceID != "" {
		h = append(h, RecordHeader{Key: HeaderTraceID, Value: []byte(x.TraceID)})
	}
	return h
}

// Validate reports missing or malformed required fields.
func (x *Exchange) Validate() error {
	var problems []string
	if x.SchemaVersion != SchemaVersion {
		problems = append(problems, fmt.Sprintf("schema_version %d", x.SchemaVersion))
	}
	if x.ID == "" {
		problems = append(problems, "missing id")
	}
	if x.Service == "" {
		problems = append(problems, "missing service")
	}
	if x.Request.Method == "" {
		problems = append(problems, "missing request.method")
	}
	if x.Request.Path == "" {
		problems = append(problems, "missing request.path")
	}
	if x.Timing.StartedAt.IsZero() {
		problems = append(problems, "missing timing.started_at")
	}
	if x.Response.Status < 100 || x.Response.Status > 999 {
		problems = append(problems, fmt.Sprintf("invalid response.status %d", x.Response.Status))
	}
	if len(problems) > 0 {
		return fmt.Errorf("capture: invalid exchange: %s", strings.Join(problems, ", "))
	}
	return nil
}
