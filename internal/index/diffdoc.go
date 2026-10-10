package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Jenil133/Shadowtap/internal/diff"
)

// DecodeDiff parses a record of the diffs topic: one diff.Result as the
// replayer publishes it (report.EncodeResult).
func DecodeDiff(value []byte) (*diff.Result, error) {
	var r diff.Result
	if err := json.Unmarshal(value, &r); err != nil {
		return nil, fmt.Errorf("index: decode diff result: %w", err)
	}
	if r.SchemaVersion != diff.SchemaVersion {
		return nil, fmt.Errorf("index: unsupported diff schema version %d", r.SchemaVersion)
	}
	if r.RunID == "" || r.CaptureID == "" {
		return nil, errors.New("index: diff result has no run id or capture id")
	}
	return &r, nil
}

// DiffDocument maps a replay diff result to a document of index. The
// document id is "<run id>-<capture id>".
//
// Document fields: @timestamp (when the replayed request was sent, else the
// original request start), event.start, event.duration (production's time
// to response headers, ns), replay.run.id, replay.trace.id (the replayed
// request's trace), replay.sent_at, replay.schedule_lag_us,
// replay.http.response.status_code and replay.event.duration (the
// candidate's status and latency), replay.baseline.status_code, capture.id,
// trace.id (the original request), service.name, http.route,
// http.request.method, http.response.status_code (production),
// url.path/query, diff.class, diff.reason, diff.pointers (the reported
// differences as "where:pointer"), diff.suppressed_pointers,
// diff.differences[{where, pointer, kind, prod, candidate, baseline, detail,
// suppressed}], diff.differences_truncated, diff.reported_count,
// diff.suppressed_count, latency.{prod_us,candidate_us,baseline_us},
// propagation.* from a traceparent record header and kafka.*.
func DiffDocument(r *diff.Result, src Source, index string) (*Document, error) {
	if r == nil {
		return nil, errors.New("index: nil diff result")
	}
	if r.RunID == "" || r.CaptureID == "" {
		return nil, errors.New("index: diff result has no run id or capture id")
	}
	ts := r.SentAt
	if ts.IsZero() {
		ts = r.StartedAt
	}
	if ts.IsZero() {
		ts = src.Timestamp
	}
	if ts.IsZero() {
		return nil, fmt.Errorf("index: diff result %s/%s has no timestamp", r.RunID, r.CaptureID)
	}
	doc := map[string]any{"@timestamp": timeField(ts)}
	if !r.StartedAt.IsZero() {
		put(doc, "event.start", timeField(r.StartedAt))
	}
	if r.Latency.ProdMicros > 0 {
		put(doc, "event.duration", r.Latency.ProdMicros*1000)
	}

	replay := map[string]any{"run": map[string]any{"id": r.RunID}}
	if r.ReplayTraceID != "" {
		replay["trace"] = map[string]any{"id": r.ReplayTraceID}
	}
	if !r.SentAt.IsZero() {
		replay["sent_at"] = timeField(r.SentAt)
	}
	if r.LagMicros != 0 {
		replay["schedule_lag_us"] = r.LagMicros
	}
	// The candidate's side is spelled like production's, under replay.*,
	// so the two compare field for field.
	if r.Status.Candidate != 0 {
		put(replay, "http.response.status_code", r.Status.Candidate)
	}
	if r.Latency.CandidateMicros > 0 {
		put(replay, "event.duration", r.Latency.CandidateMicros*1000)
	}
	if r.Status.Baseline != 0 {
		replay["baseline"] = map[string]any{"status_code": r.Status.Baseline}
	}
	doc["replay"] = replay
	put(doc, "capture.id", r.CaptureID)
	if r.OriginalTraceID != "" {
		put(doc, "trace.id", r.OriginalTraceID)
	}
	put(doc, "service.name", r.Service)

	http := map[string]any{}
	if r.Route != "" {
		http["route"] = r.Route
	}
	if r.Method != "" {
		http["request"] = map[string]any{"method": r.Method}
	}
	if r.Status.Prod != 0 {
		http["response"] = map[string]any{"status_code": r.Status.Prod}
	}
	if len(http) > 0 {
		doc["http"] = http
	}
	if r.Path != "" {
		path, query, hasQuery := strings.Cut(r.Path, "?")
		u := map[string]any{"path": path}
		if hasQuery && query != "" {
			u["query"] = query
		}
		doc["url"] = u
	}

	d := map[string]any{
		"schema_version": r.SchemaVersion,
		"class":          r.Class,
	}
	if r.Reason != "" {
		d["reason"] = r.Reason
	}
	var (
		pointers, suppressed []string
		differences          []any
	)
	for _, df := range r.Differences {
		if df.Suppressed == "" {
			pointers = append(pointers, df.Key())
		} else {
			suppressed = append(suppressed, df.Key())
		}
		diffDoc := map[string]any{"where": df.Where, "pointer": df.Pointer, "kind": df.Kind}
		if df.Prod != "" {
			diffDoc["prod"] = df.Prod
		}
		if df.Candidate != "" {
			diffDoc["candidate"] = df.Candidate
		}
		if df.Baseline != "" {
			diffDoc["baseline"] = df.Baseline
		}
		if df.Detail != "" {
			diffDoc["detail"] = df.Detail
		}
		if df.Suppressed != "" {
			diffDoc["suppressed"] = df.Suppressed
		}
		differences = append(differences, diffDoc)
	}
	if len(pointers) > 0 {
		d["pointers"] = pointers
	}
	if len(suppressed) > 0 {
		d["suppressed_pointers"] = suppressed
	}
	if len(differences) > 0 {
		d["differences"] = differences
	}
	if r.Truncated {
		d["differences_truncated"] = true
	}
	d["reported_count"] = len(pointers)
	d["suppressed_count"] = len(suppressed)
	doc["diff"] = d

	latency := map[string]any{"prod_us": r.Latency.ProdMicros, "candidate_us": r.Latency.CandidateMicros}
	if r.Latency.BaselineMicros != 0 {
		latency["baseline_us"] = r.Latency.BaselineMicros
	}
	doc["latency"] = latency

	if p := src.propagation(); p != nil {
		doc["propagation"] = p
	}
	doc["kafka"] = src.fields()
	return newDocument(index, r.RunID+"-"+r.CaptureID, doc, src)
}
