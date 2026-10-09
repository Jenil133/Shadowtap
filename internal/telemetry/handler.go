package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"slices"

	"go.opentelemetry.io/otel/trace"
)

// Attribute keys the process log copy of a record carries when its context
// holds a span. The OTLP copy carries the ids as record fields instead.
const (
	LogKeyTraceID = "trace_id"
	LogKeySpanID  = "span_id"
)

// teeHandler writes each record to the process log handler and to the
// OpenTelemetry bridge. The process copy gets trace_id and span_id as
// top-level attributes whenever the context carries a valid span, so logs
// read from stdout correlate with traces too.
//
// The base handler's level governs both copies: a record the process log
// rejects is not exported either, which keeps "log level" meaning one thing.
//
// Top-level placement of the ids needs the handler chain to be rebuilt from
// the root with the ids applied before any WithGroup, so the handler remembers
// its WithAttrs and WithGroup calls and replays them. Records without a span
// take the pre-built chain and pay nothing extra.
type teeHandler struct {
	root slog.Handler // base as given to Setup; nil discards the process copy
	base slog.Handler // root with ops applied
	otel slog.Handler // bridge to the LoggerProvider; nil when disabled
	ops  []handlerOp
}

// handlerOp is one WithAttrs (attrs set) or WithGroup (group set) call.
type handlerOp struct {
	attrs []slog.Attr
	group string
}

func newTeeHandler(base, otel slog.Handler) *teeHandler {
	return &teeHandler{root: base, base: base, otel: otel}
}

// Enabled follows the base handler, or the bridge when there is no base.
func (h *teeHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h.base != nil {
		return h.base.Enabled(ctx, level)
	}
	return h.otel != nil && h.otel.Enabled(ctx, level)
}

// Handle writes r to both handlers and joins their errors.
func (h *teeHandler) Handle(ctx context.Context, r slog.Record) error {
	if !h.Enabled(ctx, r.Level) {
		return nil
	}
	var baseErr, otelErr error
	if h.base != nil {
		target := h.base
		if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
			if ids := h.missingIDs(sc, r); len(ids) > 0 {
				target = h.withTopLevel(ids)
			}
		}
		baseErr = target.Handle(ctx, r)
	}
	if h.otel != nil && h.otel.Enabled(ctx, r.Level) {
		// The bridge reads the span context from ctx itself.
		otelErr = h.otel.Handle(ctx, r)
	}
	return errors.Join(baseErr, otelErr)
}

// missingIDs returns the span's ids as attributes, leaving out any the record
// already carries at the top level: from a WithAttrs before the first group,
// or on the record itself when no group is open. Code that logs the ids by
// hand must not end up with a line that has a key twice, which Elasticsearch
// rejects; the hand-written value is kept.
func (h *teeHandler) missingIDs(sc trace.SpanContext, r slog.Record) []slog.Attr {
	var haveTrace, haveSpan bool
	note := func(a slog.Attr) bool {
		switch a.Key {
		case LogKeyTraceID:
			haveTrace = true
		case LogKeySpanID:
			haveSpan = true
		}
		return true
	}
	grouped := false
	for _, op := range h.ops {
		if op.group != "" {
			grouped = true
			break
		}
		for _, a := range op.attrs {
			note(a)
		}
	}
	if !grouped {
		r.Attrs(note)
	}
	var ids []slog.Attr
	if !haveTrace {
		ids = append(ids, slog.String(LogKeyTraceID, sc.TraceID().String()))
	}
	if !haveSpan {
		ids = append(ids, slog.String(LogKeySpanID, sc.SpanID().String()))
	}
	return ids
}

// withTopLevel rebuilds the base chain with attrs at the top level, ahead of
// every recorded WithAttrs and WithGroup.
func (h *teeHandler) withTopLevel(attrs []slog.Attr) slog.Handler {
	out := h.root.WithAttrs(attrs)
	for _, op := range h.ops {
		if op.group != "" {
			out = out.WithGroup(op.group)
		} else {
			// Handlers may keep the slice they are given, so each replay
			// hands over a copy.
			out = out.WithAttrs(slices.Clone(op.attrs))
		}
	}
	return out
}

// WithAttrs applies attrs to both handlers.
func (h *teeHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	n := h.derive(handlerOp{attrs: slices.Clone(attrs)})
	if h.base != nil {
		n.base = h.base.WithAttrs(slices.Clone(attrs))
	}
	if h.otel != nil {
		n.otel = h.otel.WithAttrs(slices.Clone(attrs))
	}
	return n
}

// WithGroup opens a group on both handlers.
func (h *teeHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	n := h.derive(handlerOp{group: name})
	if h.base != nil {
		n.base = h.base.WithGroup(name)
	}
	if h.otel != nil {
		n.otel = h.otel.WithGroup(name)
	}
	return n
}

// derive copies h with op appended. The ops slice is cloned so two handlers
// derived from the same parent never share a backing array.
func (h *teeHandler) derive(op handlerOp) *teeHandler {
	return &teeHandler{
		root: h.root,
		base: h.base,
		otel: h.otel,
		ops:  append(slices.Clone(h.ops), op),
	}
}
