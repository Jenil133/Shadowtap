// Package tracectx extracts distributed-trace identifiers from HTTP headers:
// W3C Trace Context (traceparent) and Zipkin B3 (single and multi header).
package tracectx

import "strings"

// Header sources, in the order FromHeaders tries them.
const (
	SourceTraceparent = "traceparent"
	SourceB3          = "b3"
	SourceB3Multi     = "x-b3"
)

// Context is the inbound trace context of a request.
type Context struct {
	TraceID string // 32 lowercase hex characters
	SpanID  string // 16 lowercase hex characters: the caller's span; may be empty for B3 multi
	Sampled bool
	Source  string // SourceTraceparent, SourceB3 or SourceB3Multi
}

// FromHeaders returns the first valid trace context found in traceparent, b3,
// then the X-B3-* headers. get must return the first value of a lowercase
// header name, or "".
func FromHeaders(get func(name string) string) (Context, bool) {
	if c, ok := ParseTraceparent(get("traceparent")); ok {
		return c, true
	}
	if c, ok := ParseB3(get("b3")); ok {
		return c, true
	}
	return parseB3Multi(get)
}

// ParseTraceparent parses a W3C traceparent header
// (version-traceid-parentid-flags). Versions above 00 are parsed per the spec's
// forward-compatibility rules; version ff is invalid.
func ParseTraceparent(v string) (Context, bool) {
	v = strings.TrimSpace(v)
	if len(v) < 55 || v[2] != '-' || v[35] != '-' || v[52] != '-' {
		return Context{}, false
	}
	version, traceID, spanID, flags := v[:2], v[3:35], v[36:52], v[53:55]
	if !isLowerHex(version) || version == "ff" || !isLowerHex(flags) {
		return Context{}, false
	}
	if version == "00" && len(v) != 55 {
		return Context{}, false
	}
	if len(v) > 55 && v[55] != '-' {
		return Context{}, false
	}
	if !isLowerHex(traceID) || isZero(traceID) || !isLowerHex(spanID) || isZero(spanID) {
		return Context{}, false
	}
	return Context{
		TraceID: traceID,
		SpanID:  spanID,
		Sampled: hexNibble(flags[1])&1 == 1,
		Source:  SourceTraceparent,
	}, true
}

// ParseB3 parses the single b3 header ({trace}-{span}[-{sampled}[-{parent}]]).
// A bare sampling flag ("0", "1", "d") carries no ids and is rejected. B3 ids
// are accepted in either case and returned lowercase; 64-bit trace ids are
// left-padded to 128 bits.
func ParseB3(v string) (Context, bool) {
	parts := strings.Split(strings.ToLower(strings.TrimSpace(v)), "-")
	if len(parts) < 2 || len(parts) > 4 {
		return Context{}, false
	}
	traceID, ok := normalizeTraceID(parts[0])
	if !ok || !isSpanID(parts[1]) {
		return Context{}, false
	}
	c := Context{TraceID: traceID, SpanID: parts[1], Source: SourceB3}
	if len(parts) >= 3 {
		switch parts[2] {
		case "1", "d":
			c.Sampled = true
		case "0":
		default:
			return Context{}, false
		}
	}
	if len(parts) == 4 && !isSpanID(parts[3]) {
		return Context{}, false
	}
	return c, true
}

func parseB3Multi(get func(string) string) (Context, bool) {
	traceID, ok := normalizeTraceID(strings.ToLower(strings.TrimSpace(get("x-b3-traceid"))))
	if !ok {
		return Context{}, false
	}
	c := Context{TraceID: traceID, Source: SourceB3Multi}
	if span := strings.ToLower(strings.TrimSpace(get("x-b3-spanid"))); isSpanID(span) {
		c.SpanID = span
	}
	switch strings.ToLower(strings.TrimSpace(get("x-b3-sampled"))) {
	case "1", "true":
		c.Sampled = true
	}
	if strings.TrimSpace(get("x-b3-flags")) == "1" {
		c.Sampled = true
	}
	return c, true
}

func normalizeTraceID(s string) (string, bool) {
	switch {
	case len(s) == 32 && isLowerHex(s) && !isZero(s):
		return s, true
	case len(s) == 16 && isLowerHex(s) && !isZero(s):
		return "0000000000000000" + s, true
	}
	return "", false
}

func isSpanID(s string) bool { return len(s) == 16 && isLowerHex(s) && !isZero(s) }

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

func isZero(s string) bool { return strings.Trim(s, "0") == "" }

func hexNibble(c byte) byte {
	if c >= 'a' {
		return c - 'a' + 10
	}
	return c - '0'
}
