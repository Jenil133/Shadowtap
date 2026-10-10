package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/Jenil133/Shadowtap/internal/diff"
)

// EncodeResult renders a result as compact JSON without HTML escaping, the
// format of the diffs topic and NDJSON files.
func EncodeResult(r *diff.Result) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(r); err != nil {
		return nil, fmt.Errorf("report: encode result %s: %w", r.CaptureID, err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// WriteNDJSON writes one result per line.
func WriteNDJSON(w io.Writer, results []*diff.Result) error {
	for _, r := range results {
		b, err := EncodeResult(r)
		if err != nil {
			return err
		}
		if _, err := w.Write(append(b, '\n')); err != nil {
			return err
		}
	}
	return nil
}

// RecordHeaders are the Kafka headers of a result record, so consumers can
// route results without decoding them.
func RecordHeaders(r *diff.Result) map[string]string {
	h := map[string]string{
		"schema-version": strconv.Itoa(diff.SchemaVersion),
		"content-type":   "application/json",
		"run-id":         r.RunID,
		"service":        r.Service,
		"class":          r.Class,
	}
	if r.OriginalTraceID != "" {
		h["trace-id"] = r.OriginalTraceID
	}
	return h
}
