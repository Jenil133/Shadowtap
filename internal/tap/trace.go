package tap

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

// tracerName is the instrumentation scope of the tap's spans.
const tracerName = "github.com/Jenil133/Shadowtap/internal/tap"

// captureSpanName names the span that covers scrubbing, validating and
// publishing one exchange.
const captureSpanName = "shadowtap.capture"

// Span attribute keys specific to Shadowtap. HTTP and URL attributes follow
// the semantic conventions instead.
const (
	attrService      = attribute.Key("shadowtap.service")
	attrCaptureID    = attribute.Key("shadowtap.capture.id")
	attrMode         = attribute.Key("shadowtap.mode")
	attrSamplingRule = attribute.Key("shadowtap.sampling.rule")
	attrSamplingRate = attribute.Key("shadowtap.sampling.rate")
	attrRedactions   = attribute.Key("shadowtap.redactions")
	attrOutcome      = attribute.Key("shadowtap.outcome")
)

// startCaptureSpan starts the capture span for x. When the capture carries
// the trace context of the request it was taken from, the span joins that
// trace as a child of the request's span; otherwise it starts a trace of its
// own. The returned context carries the span for the sink and the logs.
//
// Only attributes that hold no request data are set here; the path is
// added once the scrubber has been over it.
func (s *Server) startCaptureSpan(ctx context.Context, x *capture.Exchange) (context.Context, trace.Span) {
	if x.TraceID != "" {
		// The parent is marked sampled: the capture itself shows the
		// request was kept, and a parent-based sampler would otherwise
		// drop the one span that ties the capture to its trace.
		ctx, _ = telemetry.ContextWithRemoteParent(ctx, x.TraceID, x.ParentSpanID, true)
	}
	return s.tracer.Start(ctx, captureSpanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attrService.String(x.Service),
			attrCaptureID.String(x.ID),
			attrMode.String(x.Source.Mode),
			attrSamplingRule.String(x.Sampling.Rule),
			attrSamplingRate.Float64(x.Sampling.Rate),
			semconv.HTTPRequestMethodKey.String(x.Request.Method),
			semconv.HTTPResponseStatusCode(x.Response.Status),
		),
	)
}

// recordScrubbed adds what is only safe to record after scrubbing: the path
// (the capture keeps :path whole, query included, and the scrubber has
// pseudonymised its values) and how many values were redacted.
func recordScrubbed(span trace.Span, x *capture.Exchange) {
	n := 0
	for _, c := range x.Redactions {
		n += c
	}
	span.SetAttributes(semconv.URLPath(x.Request.Path), attrRedactions.Int(n))
}

// endCaptureSpan records the outcome and ends the span. Failures become an
// error status with a fixed description: the error itself may quote
// captured data and belongs in the logs, not in the trace.
func endCaptureSpan(span trace.Span, outcome string) {
	span.SetAttributes(attrOutcome.String(outcome))
	switch outcome {
	case outcomePublished:
		span.SetStatus(codes.Ok, "")
	case outcomeSinkError:
		span.SetStatus(codes.Error, "sink rejected the exchange")
	case outcomeInvalid:
		span.SetStatus(codes.Error, "exchange failed validation")
	case outcomePanic:
		span.SetStatus(codes.Error, "panic while scrubbing or publishing")
	}
	span.End()
}
