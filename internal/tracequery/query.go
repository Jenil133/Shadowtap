package tracequery

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// Query selects a trace: by trace id, or by the request id or capture id of a
// capture, which the captures index resolves to its trace id.
type Query struct {
	TraceID   string
	RequestID string
	CaptureID string
	// FollowReplay also fetches the traces this one is tied to: the replay
	// traces of an original request (through its diffs and the replay spans'
	// links) or the original of a replay.
	FollowReplay bool
}

// Validate checks that exactly one selector is set and that a trace id is
// hex. Trace ids are normalized to lower case, which is how they are indexed.
func (q *Query) Validate() error {
	n := 0
	for _, s := range []string{q.TraceID, q.RequestID, q.CaptureID} {
		if strings.TrimSpace(s) != "" {
			n++
		}
	}
	switch {
	case n == 0:
		return errors.New("a trace id, --request-id or --capture-id is required")
	case n > 1:
		return errors.New("give one of: a trace id, --request-id, --capture-id")
	}
	q.RequestID = strings.TrimSpace(q.RequestID)
	q.CaptureID = strings.TrimSpace(q.CaptureID)
	if q.TraceID != "" {
		id, err := normalizeTraceID(q.TraceID)
		if err != nil {
			return err
		}
		q.TraceID = id
	}
	return nil
}

// normalizeTraceID accepts a W3C (32 hex) or B3 64-bit (16 hex) trace id.
func normalizeTraceID(s string) (string, error) {
	id := strings.ToLower(strings.TrimSpace(s))
	if len(id) != 32 && len(id) != 16 {
		return "", fmt.Errorf("trace id %q must be 32 (or 16) hex characters", s)
	}
	for _, r := range id {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return "", fmt.Errorf("trace id %q must be hex", s)
		}
	}
	return id, nil
}

// Lookup resolves the query and fetches everything indexed for its trace. It
// returns ErrNotFound (wrapped) when nothing is indexed for the selector.
func (c *Client) Lookup(ctx context.Context, q Query) (*Result, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	res := &Result{Query: q}
	var seed []Capture
	if q.TraceID == "" {
		found, err := c.findCapture(ctx, q)
		if err != nil {
			return nil, err
		}
		if found.TraceID == "" {
			// The capture exists but the request carried no trace context: show
			// what there is.
			res.Trace = assemble("", nil, nil, []Capture{found}, nil)
			return res, nil
		}
		q.TraceID = found.TraceID
		res.Query.TraceID = found.TraceID
		seed = []Capture{found}
	}

	traces, err := c.fetchTraces(ctx, []string{q.TraceID})
	if err != nil {
		return nil, err
	}
	res.Trace = traces[0]
	if res.Trace.Empty() {
		if len(seed) == 0 {
			return nil, fmt.Errorf("trace %s: %w", q.TraceID, ErrNotFound)
		}
		res.Trace.Captures = seed
	}
	if !q.FollowReplay {
		return res, nil
	}
	related := relatedIDs(res.Trace)
	if len(related) == 0 {
		return res, nil
	}
	ids := make([]string, 0, len(related))
	for _, r := range related {
		ids = append(ids, r.id)
	}
	rt, err := c.fetchTraces(ctx, ids)
	if err != nil {
		return nil, err
	}
	for i, t := range rt {
		if !t.Empty() {
			res.Related = append(res.Related, Related{Relation: related[i].relation, Trace: t})
		}
	}
	return res, nil
}

// findCapture looks a capture up by request id or capture id.
func (c *Client) findCapture(ctx context.Context, q Query) (Capture, error) {
	field, value, label := FieldCaptureID, q.CaptureID, "capture"
	if q.RequestID != "" {
		field, value, label = FieldRequestID, q.RequestID, "request"
	}
	body := map[string]any{
		"size":  1,
		"query": map[string]any{"term": map[string]any{field: value}},
		"sort":  []any{sortBy(FieldTimestamp)},
	}
	res, err := c.msearch(ctx, []search{{index: IndexCaptures, body: body}})
	if err != nil {
		return Capture{}, err
	}
	if len(res[0].docs) == 0 {
		return Capture{}, fmt.Errorf("%s id %s: no capture indexed: %w", label, value, ErrNotFound)
	}
	return captureFromDoc(res[0].docs[0]), nil
}

// fetchTraces reads spans, logs, captures and diffs of each trace id with one
// multi-search (four searches per id).
func (c *Client) fetchTraces(ctx context.Context, ids []string) ([]*Trace, error) {
	var searches []search
	for _, id := range ids {
		searches = append(searches,
			search{IndexTraces, map[string]any{"size": maxSpans, "query": termQuery(FieldTraceID, id), "sort": []any{sortBy(FieldTimestamp)}}},
			search{IndexLogs, map[string]any{"size": maxLogs, "query": termQuery(FieldTraceID, id), "sort": []any{sortBy(FieldTimestamp)}}},
			search{IndexCaptures, map[string]any{"size": maxCaptures, "query": termQuery(FieldTraceID, id), "sort": []any{sortBy(FieldTimestamp)}}},
			// A diff belongs to both the original and the replay trace.
			search{IndexDiffs, map[string]any{
				"size": maxDiffs,
				"query": map[string]any{"bool": map[string]any{
					"should":               []any{termQuery(FieldTraceID, id), termQuery(FieldReplayTraceID, id)},
					"minimum_should_match": 1,
				}},
				"sort": []any{sortBy(FieldTimestamp)},
			}},
		)
	}
	res, err := c.msearch(ctx, searches)
	if err != nil {
		return nil, err
	}
	out := make([]*Trace, 0, len(ids))
	for i, id := range ids {
		h := res[i*4 : i*4+4]
		spans := make([]*Span, 0, len(h[0].docs))
		for _, d := range h[0].docs {
			spans = append(spans, spanFromDoc(d))
		}
		logs := make([]Log, 0, len(h[1].docs))
		for _, d := range h[1].docs {
			logs = append(logs, logFromDoc(d))
		}
		captures := make([]Capture, 0, len(h[2].docs))
		for _, d := range h[2].docs {
			captures = append(captures, captureFromDoc(d))
		}
		diffs := make([]Diff, 0, len(h[3].docs))
		for _, d := range h[3].docs {
			diffs = append(diffs, diffFromDoc(d))
		}
		out = append(out, assemble(id, spans, logs, captures, diffs))
	}
	return out, nil
}

func termQuery(field, value string) map[string]any {
	return map[string]any{"term": map[string]any{field: value}}
}

// sortBy sorts on a field, tolerating an index where it is not mapped yet.
func sortBy(field string) map[string]any {
	return map[string]any{field: map[string]any{"order": "asc", "unmapped_type": "date"}}
}

type relatedID struct {
	id       string
	relation string
}

// relatedIDs lists the other traces a trace is tied to: through its diffs
// (original <-> replay) and through span links (a replay span links to the
// span it replays).
func relatedIDs(t *Trace) []relatedID {
	var out []relatedID
	seen := map[string]bool{t.ID: true}
	add := func(id, relation string) {
		if id == "" || seen[id] || len(out) >= maxRelated {
			return
		}
		seen[id] = true
		out = append(out, relatedID{id: id, relation: relation})
	}
	for _, d := range t.Diffs {
		if d.TraceID == t.ID {
			add(d.ReplayTraceID, "replay")
		} else if d.ReplayTraceID == t.ID {
			add(d.TraceID, "original")
		}
	}
	for _, s := range t.Spans {
		for _, l := range s.Links {
			add(l.TraceID, "linked")
		}
	}
	return out
}
