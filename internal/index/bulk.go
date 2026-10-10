package index

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// retryableItem reports whether a bulk item status is transient: the node
// shed load (429) or shards were briefly unavailable (503). Anything else a
// retry would not fix, and the document goes to the DLQ at once.
func retryableItem(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable
}

// bulkWriter sends documents to Elasticsearch in bulk requests and sorts out
// what happened to each one.
type bulkWriter struct {
	es      *elasticsearch.Client
	timeout time.Duration
	retry   RetryConfig
	metrics *metrics
	log     *slog.Logger
	errLog  *logLimiter
	tracer  trace.Tracer
}

// outcome is the final result of indexing one document.
type outcome struct {
	doc      *Document
	status   int
	result   string // resultCreated, resultDuplicate or "" when it failed
	errType  string
	errMsg   string
	attempts int
}

// bulkItem is one item of a bulk response.
type bulkItem struct {
	Index  string     `json:"_index"`
	ID     string     `json:"_id"`
	Status int        `json:"status"`
	Result string     `json:"result"`
	Error  *bulkError `json:"error,omitempty"`
}

// bulkError is the error object of a failed bulk item.
type bulkError struct {
	Type     string `json:"type"`
	Reason   string `json:"reason"`
	CausedBy *struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	} `json:"caused_by,omitempty"`
}

// message renders the error for logs and DLQ records.
func (e *bulkError) message() string {
	if e == nil {
		return ""
	}
	msg := e.Type
	if e.Reason != "" {
		msg += ": " + e.Reason
	}
	if e.CausedBy != nil && e.CausedBy.Reason != "" {
		msg += " (caused by " + e.CausedBy.Type + ": " + e.CausedBy.Reason + ")"
	}
	return msg
}

func (e *bulkError) typeName() string {
	if e == nil {
		return ""
	}
	return e.Type
}

type bulkResponse struct {
	Errors bool                  `json:"errors"`
	Items  []map[string]bulkItem `json:"items"`
}

// index writes docs and returns one outcome per document, in order. A
// document Elasticsearch rejects with a transient status is retried with
// backoff up to retry.MaxAttempts times; one rejected for good fails at
// once. The whole request is retried for as long as ctx lives when
// Elasticsearch cannot be reached or rejects the request itself. The only
// error returned is ctx's.
func (w *bulkWriter) index(ctx context.Context, docs []*Document) ([]outcome, error) {
	outcomes := make([]outcome, len(docs))
	pending := make([]int, len(docs))
	for i := range docs {
		pending[i] = i
	}
	for attempt := 1; len(pending) > 0; attempt++ {
		batch := make([]*Document, len(pending))
		for i, p := range pending {
			batch[i] = docs[p]
		}
		items, err := w.send(ctx, batch)
		if err != nil {
			return nil, err
		}
		var retry []int
		failed := 0
		for i, it := range items {
			idx := pending[i]
			o := &outcomes[idx]
			o.doc, o.status, o.attempts = docs[idx], it.Status, attempt
			switch it.Status {
			case http.StatusOK, http.StatusCreated:
				o.result = resultCreated
			case http.StatusConflict:
				// An earlier delivery of the same record already indexed it.
				o.result = resultDuplicate
			default:
				failed++
				w.metrics.itemFailures.WithLabelValues(docs[idx].Index, strconv.Itoa(it.Status)).Inc()
				o.errType, o.errMsg = it.Error.typeName(), it.Error.message()
				if retryableItem(it.Status) && attempt < w.retry.MaxAttempts {
					retry = append(retry, idx)
				}
			}
		}
		if failed > 0 {
			w.metrics.bulkRequests.WithLabelValues(bulkPartial).Inc()
		} else {
			w.metrics.bulkRequests.WithLabelValues(bulkOK).Inc()
		}
		if len(retry) == 0 {
			break
		}
		w.log.WarnContext(ctx, "elasticsearch rejected documents; retrying", "documents", len(retry), "attempt", attempt, "max_attempts", w.retry.MaxAttempts)
		if err := sleepBackoff(ctx, attempt, w.retry); err != nil {
			return nil, err
		}
		pending = retry
	}
	return outcomes, nil
}

// requestError is a bulk response with an error status for the request as a
// whole; no item was processed.
type requestError struct {
	status int
	body   string
}

func (e *requestError) Error() string {
	return (&ESError{Status: e.status, Body: e.body}).Error()
}

// send performs one bulk request for docs, retrying request-level failures
// until it gets a per-item response or ctx ends. A request Elasticsearch
// refuses as too large is split in halves; a single document too large on
// its own fails as an item.
func (w *bulkWriter) send(ctx context.Context, docs []*Document) ([]bulkItem, error) {
	for attempt := 1; ; attempt++ {
		items, err := w.once(ctx, docs)
		if err == nil {
			return items, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var re *requestError
		if errors.As(err, &re) && re.status == http.StatusRequestEntityTooLarge {
			if len(docs) == 1 {
				return []bulkItem{{
					Index:  docs[0].Index,
					ID:     docs[0].ID,
					Status: re.status,
					Error:  &bulkError{Type: "request_entity_too_large", Reason: re.body},
				}}, nil
			}
			mid := len(docs) / 2
			left, err := w.send(ctx, docs[:mid])
			if err != nil {
				return nil, err
			}
			right, err := w.send(ctx, docs[mid:])
			if err != nil {
				return nil, err
			}
			return append(left, right...), nil
		}
		if ok, suppressed := w.errLog.allow(time.Now()); ok {
			w.log.ErrorContext(ctx, "bulk request failed; retrying", "err", err, "attempt", attempt, "documents", len(docs), "suppressed_logs", suppressed)
		}
		if err := sleepBackoff(ctx, attempt, w.retry); err != nil {
			return nil, err
		}
	}
}

// once performs a single bulk request and parses its items.
func (w *bulkWriter) once(ctx context.Context, docs []*Document) (items []bulkItem, err error) {
	body := bulkBody(docs)
	w.metrics.bulkBytes.Observe(float64(len(body)))
	ctx, span := w.tracer.Start(ctx, "elasticsearch bulk",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.DBSystemNameElasticsearch,
			semconv.DBOperationName("bulk"),
			attribute.Int("shadowtap.bulk.documents", len(docs)),
			attribute.Int("shadowtap.bulk.bytes", len(body)),
		))
	defer func() { endSpan(span, err) }()

	rctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	start := time.Now()
	res, err := w.es.Bulk(bytes.NewReader(body),
		w.es.Bulk.WithContext(rctx),
		w.es.Bulk.WithFilterPath("errors", "items.*._index", "items.*._id", "items.*.status", "items.*.error"),
	)
	if err != nil {
		w.metrics.bulkRequests.WithLabelValues(bulkFailed).Inc()
		w.metrics.requestErrors.WithLabelValues("transport").Inc()
		return nil, fmt.Errorf("bulk request: %w", err)
	}
	defer res.Body.Close()
	w.metrics.bulkSeconds.Observe(time.Since(start).Seconds())
	span.SetAttributes(semconv.HTTPResponseStatusCode(res.StatusCode))
	if res.IsError() {
		b, _ := io.ReadAll(io.LimitReader(res.Body, maxErrorBody))
		w.metrics.bulkRequests.WithLabelValues(bulkFailed).Inc()
		w.metrics.requestErrors.WithLabelValues(strconv.Itoa(res.StatusCode)).Inc()
		return nil, &requestError{status: res.StatusCode, body: string(bytes.TrimSpace(b))}
	}
	var resp bulkResponse
	if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
		w.metrics.bulkRequests.WithLabelValues(bulkFailed).Inc()
		w.metrics.requestErrors.WithLabelValues("transport").Inc()
		return nil, fmt.Errorf("bulk response: %w", err)
	}
	if len(resp.Items) != len(docs) {
		w.metrics.bulkRequests.WithLabelValues(bulkFailed).Inc()
		w.metrics.requestErrors.WithLabelValues("transport").Inc()
		return nil, fmt.Errorf("bulk response has %d items for %d documents", len(resp.Items), len(docs))
	}
	items = make([]bulkItem, len(docs))
	for i, actions := range resp.Items {
		// Each item is {"create": {...}}: one action, keyed by its name.
		for _, it := range actions {
			items[i] = it
		}
	}
	return items, nil
}

// bulkBody renders the NDJSON body: a create action with the index and the
// deterministic id, then the document, for every document.
func bulkBody(docs []*Document) []byte {
	var buf bytes.Buffer
	for _, d := range docs {
		action, _ := json.Marshal(map[string]any{"create": map[string]string{"_index": d.Index, "_id": d.ID}})
		buf.Write(action)
		buf.WriteByte('\n')
		buf.Write(d.Body)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// endSpan ends span, recording err as its status when there is one.
func endSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// backoff returns the wait before the next attempt: exponential from the
// initial backoff, capped, with ±20% jitter so replicas do not retry in
// lockstep.
func backoff(attempt int, cfg RetryConfig) time.Duration {
	initial := time.Duration(cfg.InitialBackoffMS) * time.Millisecond
	limit := time.Duration(cfg.MaxBackoffMS) * time.Millisecond
	d := initial << min(max(attempt-1, 0), 20)
	if d > limit || d <= 0 {
		d = limit
	}
	jitter := 0.8 + 0.4*rand.Float64() //nolint:gosec // jitter, not security
	return time.Duration(float64(d) * jitter)
}

// sleepBackoff waits backoff(attempt) or until ctx ends.
func sleepBackoff(ctx context.Context, attempt int, cfg RetryConfig) error {
	timer := time.NewTimer(backoff(attempt, cfg))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// logLimiter lets one event through per interval and counts the rest, so
// an outage does not flood the logs with one line per retry.
type logLimiter struct {
	interval time.Duration

	mu         sync.Mutex
	last       time.Time
	suppressed int
}

// allow reports whether an event at now may be logged and how many were
// suppressed since the last one that was.
func (l *logLimiter) allow(now time.Time) (ok bool, suppressed int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.last.IsZero() && now.Sub(l.last) < l.interval {
		l.suppressed++
		return false, 0
	}
	l.last = now
	suppressed, l.suppressed = l.suppressed, 0
	return true, suppressed
}
