package telemetry

import (
	"context"
	"slices"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// HeaderCarrier adapts Kafka record headers to the propagation API so trace
// context travels on capture and diff records. Lookups are exact first, then
// case-insensitive, because headers that began life as HTTP headers may
// arrive in any case. *HeaderCarrier implements propagation.TextMapCarrier;
// Set needs the pointer to grow the slice.
type HeaderCarrier []capture.RecordHeader

var _ propagation.TextMapCarrier = (*HeaderCarrier)(nil)

// Get returns the value of the first header named key, or "".
func (c HeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	for _, h := range c {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

// Set stores value under key, replacing an existing header of that name
// (compared case-insensitively) and dropping any duplicates of it, so a
// record never carries two traceparents.
func (c *HeaderCarrier) Set(key, value string) {
	if c == nil {
		return
	}
	hs := *c
	i := slices.IndexFunc(hs, func(h capture.RecordHeader) bool { return strings.EqualFold(h.Key, key) })
	if i < 0 {
		*c = append(hs, capture.RecordHeader{Key: key, Value: []byte(value)})
		return
	}
	hs[i] = capture.RecordHeader{Key: key, Value: []byte(value)}
	rest := slices.DeleteFunc(hs[i+1:], func(h capture.RecordHeader) bool { return strings.EqualFold(h.Key, key) })
	*c = hs[:i+1+len(rest)]
}

// Keys lists the header names in order.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
}

// InjectHeaders returns headers plus the trace context of ctx, written by the
// global propagator (traceparent, tracestate and baggage). The input slice is
// not modified.
func InjectHeaders(ctx context.Context, headers []capture.RecordHeader) []capture.RecordHeader {
	c := HeaderCarrier(slices.Clone(headers))
	otel.GetTextMapPropagator().Inject(ctx, &c)
	return []capture.RecordHeader(c)
}

// ExtractHeaders returns ctx with the trace context carried by headers, read
// by the global propagator. Without valid trace headers it returns ctx
// unchanged.
func ExtractHeaders(ctx context.Context, headers []capture.RecordHeader) context.Context {
	c := HeaderCarrier(headers)
	return otel.GetTextMapPropagator().Extract(ctx, &c)
}

// ContextWithRemoteParent returns ctx carrying a remote parent span built
// from the 32-hex trace id and 16-hex span id a capture records from the
// inbound request, so a span started from the result joins the original
// trace. sampled sets the parent's sampled flag, which a parent-based
// sampler follows. It returns ctx and false when either id is invalid.
func ContextWithRemoteParent(ctx context.Context, traceID, spanID string, sampled bool) (context.Context, bool) {
	sc, ok := remoteSpanContext(traceID, spanID, sampled)
	if !ok {
		return ctx, false
	}
	return trace.ContextWithRemoteSpanContext(ctx, sc), true
}

// LinkTo returns a span link to the span identified by the given ids, for
// spans in a new trace (a replay) that refer back to the original one. The
// trace flags stay clear because a capture does not record them. It returns
// false when either id is invalid.
func LinkTo(traceID, spanID string) (trace.Link, bool) {
	sc, ok := remoteSpanContext(traceID, spanID, false)
	if !ok {
		return trace.Link{}, false
	}
	return trace.Link{SpanContext: sc}, true
}

// SpanIDs returns the hex trace and span id of the span in ctx, recording or
// remote, and false when there is none.
func SpanIDs(ctx context.Context) (traceID, spanID string, ok bool) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", "", false
	}
	return sc.TraceID().String(), sc.SpanID().String(), true
}

// remoteSpanContext parses the ids, accepting upper-case hex because ids
// copied from other systems are not always normalised. All-zero ids are
// invalid per the W3C specification.
func remoteSpanContext(traceID, spanID string, sampled bool) (trace.SpanContext, bool) {
	tid, err := trace.TraceIDFromHex(strings.ToLower(traceID))
	if err != nil {
		return trace.SpanContext{}, false
	}
	sid, err := trace.SpanIDFromHex(strings.ToLower(spanID))
	if err != nil {
		return trace.SpanContext{}, false
	}
	var flags trace.TraceFlags
	if sampled {
		flags = trace.FlagsSampled
	}
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: flags,
		Remote:     true,
	}), true
}
