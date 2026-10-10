package tracequery

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// Document is the JSON form of a Result, for scripting.
type Document struct {
	TraceID   string            `json:"trace_id,omitempty"`
	StartedAt *time.Time        `json:"started_at,omitempty"`
	Duration  int64             `json:"duration_ns"`
	Services  []string          `json:"services"`
	SpanCount int               `json:"span_count"`
	LogCount  int               `json:"log_count"`
	Captures  []CaptureDocument `json:"captures"`
	Diffs     []DiffDocument    `json:"diffs"`
	// Spans is the tree: the root (synthetic when the trace has several
	// roots or orphans) with children nested.
	Spans          *SpanDocument     `json:"spans"`
	UnattachedLogs []LogDocument     `json:"unattached_logs,omitempty"`
	Related        []RelatedDocument `json:"related,omitempty"`
	ResolvedBy     *ResolvedBy       `json:"resolved_by,omitempty"`
}

// ResolvedBy records how a request or capture id was turned into a trace id.
type ResolvedBy struct {
	RequestID string `json:"request_id,omitempty"`
	CaptureID string `json:"capture_id,omitempty"`
}

// RelatedDocument is a trace reached with FollowReplay.
type RelatedDocument struct {
	Relation string   `json:"relation"`
	Trace    Document `json:"trace"`
}

// SpanDocument is a span with its logs and children.
type SpanDocument struct {
	SpanID        string            `json:"span_id,omitempty"`
	ParentID      string            `json:"parent_id,omitempty"`
	Name          string            `json:"name"`
	Kind          string            `json:"kind,omitempty"`
	Service       string            `json:"service,omitempty"`
	StartedAt     *time.Time        `json:"started_at,omitempty"`
	OffsetNS      int64             `json:"offset_ns"`
	DurationNS    int64             `json:"duration_ns"`
	Status        string            `json:"status,omitempty"`
	StatusMessage string            `json:"status_message,omitempty"`
	HTTPMethod    string            `json:"http_method,omitempty"`
	URLPath       string            `json:"url_path,omitempty"`
	HTTPStatus    int               `json:"http_status,omitempty"`
	Links         []Link            `json:"links,omitempty"`
	Attributes    map[string]string `json:"attributes,omitempty"`
	Orphan        bool              `json:"orphan,omitempty"`
	Synthetic     bool              `json:"synthetic,omitempty"`
	Logs          []LogDocument     `json:"logs,omitempty"`
	Children      []*SpanDocument   `json:"children,omitempty"`
}

// LogDocument is one log record.
type LogDocument struct {
	Time       *time.Time        `json:"time,omitempty"`
	SpanID     string            `json:"span_id,omitempty"`
	Service    string            `json:"service,omitempty"`
	Level      string            `json:"level,omitempty"`
	Message    string            `json:"message"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// BodyDocument is a scrubbed body; Content is included only with bodies.
type BodyDocument struct {
	ContentType string `json:"content_type,omitempty"`
	Size        int64  `json:"size"`
	Omitted     string `json:"omitted,omitempty"`
	Truncated   bool   `json:"truncated,omitempty"`
	Content     string `json:"content,omitempty"`
}

// CaptureDocument is a capture summary.
type CaptureDocument struct {
	ID         string           `json:"id,omitempty"`
	TraceID    string           `json:"trace_id,omitempty"`
	ParentID   string           `json:"parent_span_id,omitempty"`
	RequestID  string           `json:"request_id,omitempty"`
	Service    string           `json:"service,omitempty"`
	Method     string           `json:"method,omitempty"`
	Route      string           `json:"route,omitempty"`
	Path       string           `json:"path,omitempty"`
	Status     int              `json:"status"`
	StartedAt  *time.Time       `json:"started_at,omitempty"`
	DurationNS int64            `json:"duration_ns"`
	Tap        string           `json:"tap,omitempty"`
	Mode       string           `json:"mode,omitempty"`
	Redactions map[string]int64 `json:"redactions,omitempty"`
	Request    BodyDocument     `json:"request"`
	Response   BodyDocument     `json:"response"`
}

// DiffDocument is a diff summary.
type DiffDocument struct {
	RunID                string       `json:"run_id,omitempty"`
	CaptureID            string       `json:"capture_id,omitempty"`
	Service              string       `json:"service,omitempty"`
	Method               string       `json:"method,omitempty"`
	Route                string       `json:"route,omitempty"`
	Path                 string       `json:"path,omitempty"`
	Class                string       `json:"class"`
	Reason               string       `json:"reason,omitempty"`
	ProdStatus           int          `json:"prod_status"`
	CandidateStatus      int          `json:"candidate_status"`
	BaselineStatus       int          `json:"baseline_status,omitempty"`
	Differences          []Difference `json:"differences,omitempty"`
	DifferencesTruncated bool         `json:"differences_truncated,omitempty"`
	TraceID              string       `json:"trace_id,omitempty"`
	ReplayTraceID        string       `json:"replay_trace_id,omitempty"`
	StartedAt            *time.Time   `json:"started_at,omitempty"`
	SentAt               *time.Time   `json:"sent_at,omitempty"`
	ScheduleLagNS        int64        `json:"schedule_lag_ns,omitempty"`
	ProdLatencyNS        int64        `json:"prod_latency_ns"`
	CandidateLatencyNS   int64        `json:"candidate_latency_ns"`
	BaselineLatencyNS    int64        `json:"baseline_latency_ns,omitempty"`
}

// WriteJSON writes the result as one indented JSON document. Bodies are
// included when bodies is set.
func WriteJSON(w io.Writer, r *Result, bodies bool) error {
	d := toDocument(r.Trace, bodies)
	if r.Query.RequestID != "" || r.Query.CaptureID != "" {
		d.ResolvedBy = &ResolvedBy{RequestID: r.Query.RequestID, CaptureID: r.Query.CaptureID}
	}
	for _, rel := range r.Related {
		d.Related = append(d.Related, RelatedDocument{Relation: rel.Relation, Trace: toDocument(rel.Trace, bodies)})
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(d); err != nil {
		return fmt.Errorf("tracequery: encode: %w", err)
	}
	return nil
}

func toDocument(t *Trace, bodies bool) Document {
	d := Document{
		TraceID:   t.ID,
		StartedAt: timePtr(t.Start),
		Duration:  int64(t.Duration),
		Services:  t.Services(),
		SpanCount: len(t.Spans),
		LogCount:  len(t.Logs),
		Captures:  []CaptureDocument{},
		Diffs:     []DiffDocument{},
	}
	if d.Services == nil {
		d.Services = []string{}
	}
	for _, c := range t.Captures {
		d.Captures = append(d.Captures, captureDocument(c, bodies))
	}
	for _, df := range t.Diffs {
		d.Diffs = append(d.Diffs, DiffDocument{
			RunID: df.RunID, CaptureID: df.CaptureID, Service: df.Service, Method: df.Method, Route: df.Route, Path: df.Path,
			Class: df.Class, Reason: df.Reason, ProdStatus: df.ProdStatus, CandidateStatus: df.CandidateStatus, BaselineStatus: df.BaselineStatus,
			Differences: df.Differences, DifferencesTruncated: df.Truncated, TraceID: df.TraceID, ReplayTraceID: df.ReplayTraceID,
			StartedAt: timePtr(df.Start), SentAt: timePtr(df.SentAt), ScheduleLagNS: int64(df.ScheduleLag),
			ProdLatencyNS: int64(df.ProdLatency), CandidateLatencyNS: int64(df.CandidateLatency), BaselineLatencyNS: int64(df.BaselineLatency),
		})
	}
	if t.Root != nil {
		d.Spans = spanDocument(t.Root, t.Start)
	}
	for _, l := range t.UnattachedLogs() {
		d.UnattachedLogs = append(d.UnattachedLogs, logDocument(l))
	}
	return d
}

func captureDocument(c Capture, bodies bool) CaptureDocument {
	body := func(b Body) BodyDocument {
		out := BodyDocument{ContentType: b.ContentType, Size: b.Size, Omitted: b.Omitted, Truncated: b.Truncated}
		if bodies {
			out.Content = b.Content
		}
		return out
	}
	return CaptureDocument{
		ID: c.ID, TraceID: c.TraceID, ParentID: c.ParentID, RequestID: c.RequestID, Service: c.Service,
		Method: c.Method, Route: c.Route, Path: c.Path, Status: c.Status, StartedAt: timePtr(c.Start),
		DurationNS: int64(c.Duration), Tap: c.Tap, Mode: c.Mode, Redactions: c.Redactions,
		Request: body(c.Request), Response: body(c.Response),
	}
}

func spanDocument(s *Span, traceStart time.Time) *SpanDocument {
	d := &SpanDocument{
		SpanID: s.SpanID, ParentID: s.ParentID, Name: s.Name, Kind: s.Kind, Service: s.Service,
		StartedAt: timePtr(s.Start), DurationNS: int64(s.Duration), Status: s.Status, StatusMessage: s.StatusMessage,
		HTTPMethod: s.HTTPMethod, URLPath: s.URLPath, HTTPStatus: s.HTTPStatus, Links: s.Links, Attributes: s.Attributes,
		Orphan: s.Orphan, Synthetic: s.Synthetic,
	}
	if !s.Start.IsZero() && !traceStart.IsZero() {
		d.OffsetNS = int64(s.Start.Sub(traceStart))
	}
	for _, l := range s.Logs {
		d.Logs = append(d.Logs, logDocument(l))
	}
	for _, c := range s.Children {
		d.Children = append(d.Children, spanDocument(c, traceStart))
	}
	return d
}

func logDocument(l Log) LogDocument {
	return LogDocument{Time: timePtr(l.Time), SpanID: l.SpanID, Service: l.Service, Level: l.Level, Message: l.Message, Attributes: l.Attributes}
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}
