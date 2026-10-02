package api

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
)

const (
	contentTypeJSON = "application/json"
	// gzipMinBytes is the smallest body worth compressing.
	gzipMinBytes = 1 << 10
	maxBodyBytes = 64 << 10
)

// writeJSON writes v as a JSON response, gzip-compressed when the body is at
// least gzipMinBytes and the client accepts gzip.
func (a *API) writeJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := encodeJSON(v)
	if err != nil {
		a.log.ErrorContext(r.Context(), "encode response", "route", r.Pattern, "error", err)
		status, body = http.StatusInternalServerError, []byte(`{"error":"internal server error"}`+"\n")
	}
	h := w.Header()
	h.Set("Content-Type", contentTypeJSON)
	if len(body) >= gzipMinBytes {
		// The representation now depends on Accept-Encoding either way.
		h.Add("Vary", "Accept-Encoding")
		if acceptsGzip(r.Header.Values("Accept-Encoding")) {
			if z, err := a.gzipBytes(body); err == nil {
				body = z
				h.Set("Content-Encoding", "gzip")
			} else {
				a.log.WarnContext(r.Context(), "gzip response", "route", r.Pattern, "error", err)
			}
		}
	}
	h.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	// A failed write means the client went away; there is nobody to tell.
	_, _ = w.Write(body)
}

func (a *API) writeError(w http.ResponseWriter, r *http.Request, status int, msg string) {
	a.writeJSON(w, r, status, ErrorResponse{Error: msg})
}

func encodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, fmt.Errorf("encode JSON: %w", err)
	}
	return buf.Bytes(), nil
}

// gzipBytes compresses b, reusing writers because each one allocates
// sizable compression tables.
func (a *API) gzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw, ok := a.gzipWriters.Get().(*gzip.Writer)
	if ok {
		zw.Reset(&buf)
	} else {
		zw = gzip.NewWriter(&buf)
	}
	defer a.gzipWriters.Put(zw)
	if _, err := zw.Write(b); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	return buf.Bytes(), nil
}

// acceptsGzip reports whether Accept-Encoding header values allow gzip: gzip
// (or x-gzip) listed with a non-zero quality, or, when gzip is not listed, a
// "*" with a non-zero quality. A malformed quality counts as zero.
func acceptsGzip(values []string) bool {
	gzipQ, anyQ := -1.0, -1.0
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			coding, params, _ := strings.Cut(part, ";")
			q := quality(params)
			switch strings.ToLower(strings.TrimSpace(coding)) {
			case "gzip", "x-gzip":
				gzipQ = max(gzipQ, q)
			case "*":
				anyQ = max(anyQ, q)
			}
		}
	}
	if gzipQ >= 0 {
		return gzipQ > 0
	}
	return anyQ > 0
}

// quality returns the q parameter from ";"-separated coding parameters,
// defaulting to 1.
func quality(params string) float64 {
	for _, p := range strings.Split(params, ";") {
		name, value, ok := strings.Cut(strings.TrimSpace(p), "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(name), "q") {
			continue
		}
		q, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		if err != nil || q < 0 || q > 1 {
			return 0
		}
		return q
	}
	return 1
}

// decode reads a single JSON object from the request body into v, rejecting
// unknown fields, trailing data and bodies over maxBodyBytes. On failure it
// writes the error response and returns false.
func (a *API) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil {
		// Anything after the object, other than whitespace, is an error.
		if extra := dec.Decode(&json.RawMessage{}); !errors.Is(extra, io.EOF) {
			err = extra
			if err == nil {
				err = errors.New("unexpected data after the JSON object")
			}
		}
	}
	var tooLarge *http.MaxBytesError
	switch {
	case err == nil:
		return true
	case errors.As(err, &tooLarge):
		a.writeError(w, r, http.StatusRequestEntityTooLarge, "request body exceeds 65536 bytes")
	case errors.Is(err, io.EOF):
		a.writeError(w, r, http.StatusBadRequest, "request body is empty")
	default:
		a.writeError(w, r, http.StatusBadRequest, "invalid JSON body: "+err.Error())
	}
	return false
}
