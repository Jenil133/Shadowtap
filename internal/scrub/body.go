package scrub

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"strings"
	"unicode/utf8"

	"github.com/klauspost/compress/zstd"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

const (
	// maxZstdWindow is the largest zstd window accepted; RFC 9659 limits
	// zstd content-coding windows to 8 MiB.
	maxZstdWindow = 8 << 20
	// maxZstdMemory caps what the zstd decoder may allocate.
	maxZstdMemory = 64 << 20
)

// Body scrubs a message body. Content-Encoding is decoded first (gzip,
// deflate, zstd; at most MaxDecodedBytes of output), then the media type
// picks the scrubber: JSON (order-preserving token walk), NDJSON, form
// encoding, or free text. Anything that cannot be parsed or decoded safely,
// and every other media type, is omitted with a capture.Omitted* reason.
// A truncated body is cut back to its last complete line (text, NDJSON) or
// pair (form), and truncated JSON is omitted, so no value split at the cut
// slips past the detectors. Body fails closed: a panic while scrubbing
// omits the body as unparseable. Redactions are counted only for bodies
// whose data is kept. Unchanged data is returned without copying, so the
// result may share memory with in.Data.
func (s *Scrubber) Body(in BodyInput, rep Report) (res BodyResult) {
	defer func() {
		if recover() != nil {
			res = BodyResult{Omitted: capture.OmittedUnparseable, Truncated: in.Truncated}
		}
	}()
	local := Report{}
	res = s.body(in, local)
	if res.Omitted == "" {
		for k, n := range local {
			rep.Add(k, n)
		}
	}
	return res
}

func (s *Scrubber) body(in BodyInput, rep Report) BodyResult {
	if len(in.Data) == 0 {
		return BodyResult{Truncated: in.Truncated}
	}
	data, from, truncated, omitted := s.decode(in.Data, in.ContentEncoding, in.Truncated)
	res := BodyResult{DecodedFrom: from, Truncated: truncated}
	if omitted == "" && len(data) > 0 {
		res.Data, omitted = s.payload(data, in.ContentType, truncated, rep)
	}
	if omitted != "" {
		res.Data, res.Omitted = nil, omitted
	}
	return res
}

// payload scrubs decoded body data according to its media type and returns
// the result or the reason it is omitted.
func (s *Scrubber) payload(data []byte, contentType string, truncated bool, rep Report) ([]byte, string) {
	typ := s.classify(contentType)
	if (typ == bodyText || typ == bodyForm) && !truncated && looksLikeJSONContainer(data) && json.Valid(data) {
		// JSON sent as text/plain or form data (a CORS-simple request, a
		// misconfigured client) is scrubbed by its structure, not as prose.
		typ = bodyJSON
	}
	switch typ {
	case bodyJSON:
		if truncated {
			return nil, capture.OmittedTruncatedUnsafe
		}
		out, _, err := s.scrubJSON(data, rep)
		if err != nil {
			return nil, capture.OmittedUnparseable
		}
		return out, ""
	case bodyNDJSON:
		if truncated {
			var ok bool
			if data, ok = cutAfterLast(data, '\n'); !ok {
				return nil, capture.OmittedTruncatedUnsafe
			}
		}
		out, err := s.scrubNDJSON(data, rep)
		if err != nil {
			return nil, capture.OmittedUnparseable
		}
		return out, ""
	case bodyForm:
		if truncated {
			var ok bool
			if data, ok = cutAfterLast(data, '&'); !ok {
				return nil, capture.OmittedTruncatedUnsafe
			}
		}
		if !utf8.Valid(data) {
			return nil, capture.OmittedBinary
		}
		return replaceIfChanged(data, s.query(string(data), rep)), ""
	case bodyText:
		if truncated {
			var ok bool
			if data, ok = cutAfterLast(data, '\n'); !ok {
				return nil, capture.OmittedTruncatedUnsafe
			}
		}
		if !utf8.Valid(data) {
			return nil, capture.OmittedBinary
		}
		text := string(data)
		if mediaType(contentType) == "text/html" {
			text = decodeAtEntities(text)
		}
		return replaceIfChanged(data, s.text(text, rep)), ""
	case bodyMultipart:
		return nil, capture.OmittedMultipart
	case bodyBinary:
		return nil, capture.OmittedBinary
	}
	// Unknown or missing media type: keep it only if it is JSON.
	if !looksLikeJSON(data) {
		return nil, capture.OmittedBinary
	}
	if truncated {
		return nil, capture.OmittedTruncatedUnsafe
	}
	out, _, err := s.scrubJSON(data, rep)
	if err != nil {
		return nil, capture.OmittedBinary
	}
	return out, ""
}

func replaceIfChanged(orig []byte, scrubbed string) []byte {
	if scrubbed == string(orig) {
		return orig
	}
	return []byte(scrubbed)
}

// cutAfterLast returns data up to and including the last sep. Keeping the
// separator makes the cut idempotent.
func cutAfterLast(data []byte, sep byte) ([]byte, bool) {
	i := bytes.LastIndexByte(data, sep)
	if i < 0 {
		return nil, false
	}
	return data[:i+1], true
}

// looksLikeJSONContainer reports whether data starts, after whitespace, with
// an object or array.
func looksLikeJSONContainer(data []byte) bool {
	for _, c := range data {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		}
		return c == '{' || c == '['
	}
	return false
}

// atEntities are the HTML character references of '@'.
var atEntities = strings.NewReplacer("&#64;", "@", "&#x40;", "@", "&#X40;", "@", "&commat;", "@")

// decodeAtEntities turns the character references of '@' into '@', so an
// address written as bob&#64;example.org is seen by the email detector. Other
// markup is left alone.
func decodeAtEntities(t string) string {
	if strings.IndexByte(t, '&') < 0 {
		return t
	}
	return atEntities.Replace(t)
}

// looksLikeJSON reports whether data starts, after whitespace, with a byte
// that can begin a JSON value.
func looksLikeJSON(data []byte) bool {
	for _, c := range data {
		switch c {
		case ' ', '\t', '\n', '\r':
			continue
		case '{', '[', '"', '-', 't', 'f', 'n':
			return true
		}
		return isDigit(c)
	}
	return false
}

// scrubNDJSON scrubs newline-delimited JSON line by line. Blank lines are
// kept; any line that is not valid JSON fails the whole body.
func (s *Scrubber) scrubNDJSON(in []byte, rep Report) ([]byte, error) {
	var out []byte
	done := 0
	for start := 0; start < len(in); {
		end := len(in)
		if i := bytes.IndexByte(in[start:], '\n'); i >= 0 {
			end = start + i
		}
		if line := in[start:end]; !isJSONSpace(line) {
			nl, changed, err := s.scrubJSON(line, rep)
			if err != nil {
				return nil, err
			}
			if changed {
				out = append(out, in[done:start]...)
				out = append(out, nl...)
				done = end
			}
		}
		start = end + 1
	}
	if out == nil {
		return in, nil
	}
	return append(out, in[done:]...), nil
}

func isJSONSpace(b []byte) bool {
	for _, c := range b {
		if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
			return false
		}
	}
	return true
}

type bodyType int

const (
	bodyUnknown bodyType = iota
	bodyJSON
	bodyNDJSON
	bodyForm
	bodyText
	bodyMultipart
	bodyBinary
)

func (s *Scrubber) classify(contentType string) bodyType {
	mt := mediaType(contentType)
	switch {
	case mt == "":
		return bodyUnknown
	case strings.HasPrefix(mt, "application/grpc"):
		return bodyBinary // length-prefixed frames, even application/grpc+json
	case mt == "application/json" || mt == "text/json" || strings.HasSuffix(mt, "+json"):
		return bodyJSON
	case mt == "application/x-ndjson" || mt == "application/jsonl" || mt == "application/jsonlines":
		return bodyNDJSON
	case mt == "application/x-www-form-urlencoded":
		return bodyForm
	case has(s.textTypes, mt):
		return bodyText
	case strings.HasPrefix(mt, "multipart/"):
		return bodyMultipart
	case isBinaryType(mt):
		return bodyBinary
	}
	return bodyUnknown
}

// isBinaryType reports media types that are never scrubbed, not even when
// they happen to parse as JSON.
func isBinaryType(mt string) bool {
	if strings.HasSuffix(mt, "/xml") || strings.HasSuffix(mt, "+xml") ||
		strings.Contains(mt, "protobuf") || mt == "application/octet-stream" {
		return true
	}
	top, _, _ := strings.Cut(mt, "/")
	switch top {
	case "image", "audio", "video", "font":
		return true
	}
	return false
}

// mediaType returns the lowercase media type of a Content-Type value
// without parameters, or "".
func mediaType(contentType string) string {
	if mt, _, err := mime.ParseMediaType(contentType); err == nil || mt != "" {
		return mt
	}
	t, _, _ := strings.Cut(contentType, ";")
	return strings.ToLower(strings.TrimSpace(t))
}

// decode removes the content-codings listed in header (applied in order, so
// removed in reverse). It returns the decoded data, the codings removed,
// whether the data is now only a prefix of the body, and an omission reason
// if decoding is impossible.
func (s *Scrubber) decode(data []byte, header string, truncated bool) (out []byte, from string, trunc bool, omitted string) {
	codings := parseCodings(header)
	if len(codings) == 0 {
		return data, "", truncated, ""
	}
	for _, c := range codings {
		switch c {
		case "gzip", "x-gzip", "deflate", "zstd":
		default:
			return nil, "", truncated, capture.OmittedUnsupportedEncoding
		}
	}
	out = data
	for i := len(codings) - 1; i >= 0; i-- {
		var cut bool
		var err error
		if out, cut, err = decodeOne(codings[i], out, s.maxDecoded); err != nil {
			return nil, "", truncated, capture.OmittedUnparseable
		}
		truncated = truncated || cut
	}
	return out, strings.Join(codings, ", "), truncated, ""
}

// parseCodings splits a Content-Encoding value into lowercase codings,
// ignoring identity.
func parseCodings(header string) []string {
	var codings []string
	for c := range strings.SplitSeq(header, ",") {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" && c != "identity" {
			codings = append(codings, c)
		}
	}
	return codings
}

// decodeOne removes one content-coding, keeping at most limit bytes. A
// stream that ends early yields what was decoded, marked as cut.
func decodeOne(coding string, data []byte, limit int) ([]byte, bool, error) {
	switch coding {
	case "gzip", "x-gzip":
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
				return nil, true, nil // cut inside the header
			}
			return nil, false, fmt.Errorf("gzip: %w", err)
		}
		defer zr.Close()
		return readLimited(zr, limit)
	case "deflate":
		// RFC 9110 deflate is zlib-wrapped, but some servers send raw
		// deflate.
		if zr, err := zlib.NewReader(bytes.NewReader(data)); err == nil {
			out, cut, err := readLimited(zr, limit)
			zr.Close()
			if err == nil {
				return out, cut, nil
			}
		}
		fr := flate.NewReader(bytes.NewReader(data))
		defer fr.Close()
		return readLimited(fr, limit)
	case "zstd":
		zr, err := zstd.NewReader(bytes.NewReader(data),
			zstd.WithDecoderConcurrency(1), // decode synchronously: no goroutines
			zstd.WithDecoderLowmem(true),
			zstd.WithDecoderMaxWindow(maxZstdWindow),
			zstd.WithDecoderMaxMemory(maxZstdMemory),
			zstd.WithDecodeBuffersBelow(0), // always stream, so the output cap applies
		)
		if err != nil {
			return nil, false, fmt.Errorf("zstd: %w", err)
		}
		defer zr.Close()
		return readLimited(zr, limit)
	}
	return nil, false, errors.New("unsupported content-coding")
}

// readLimited reads at most limit bytes from r. More data, or a stream that
// ends unexpectedly, sets cut and keeps the prefix; a decompression bomb
// costs no more than limit bytes of output.
func readLimited(r io.Reader, limit int) ([]byte, bool, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(io.LimitReader(r, int64(limit)+1))
	out := buf.Bytes()
	switch {
	case len(out) > limit:
		return out[:limit], true, nil
	case errors.Is(err, io.ErrUnexpectedEOF):
		return out, true, nil
	case err != nil:
		return nil, false, err
	}
	return out, false, nil
}
