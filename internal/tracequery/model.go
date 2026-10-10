package tracequery

import (
	"sort"
	"strings"
	"time"
)

// Span is one span document, with its logs and children once the tree is
// built.
type Span struct {
	TraceID       string
	SpanID        string
	ParentID      string
	Name          string
	Kind          string // SERVER, CLIENT, INTERNAL, PRODUCER, CONSUMER or ""
	Service       string
	Start         time.Time
	Duration      time.Duration
	Status        string // OK, ERROR or "" (unset)
	StatusMessage string
	HTTPMethod    string
	URLPath       string
	HTTPStatus    int
	Links         []Link
	Attributes    map[string]string

	Logs     []Log
	Children []*Span

	// Orphan is set when ParentID names a span that is not in the trace, so
	// the span hangs off the synthetic root.
	Orphan bool
	// Synthetic marks the root added when a trace has no single root span.
	Synthetic bool
}

// End is the span's end time.
func (s *Span) End() time.Time { return s.Start.Add(s.Duration) }

// Link points at a span of another (or the same) trace, as a replay span
// links to the request it replays.
type Link struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id,omitempty"`
}

// Log is one log record carrying the trace id.
type Log struct {
	Time       time.Time
	TraceID    string
	SpanID     string
	Service    string
	Level      string
	Message    string
	Attributes map[string]string
}

// Capture is a capture document: the scrubbed exchange the tap recorded.
type Capture struct {
	ID         string
	TraceID    string
	ParentID   string
	RequestID  string
	Service    string
	Method     string
	Route      string
	Path       string
	Status     int
	Start      time.Time
	Duration   time.Duration
	Tap        string
	Mode       string
	Redactions map[string]int64
	Request    Body
	Response   Body
}

// Body is a scrubbed message body as stored with a capture.
type Body struct {
	ContentType string
	Content     string
	Size        int64
	Omitted     string
	Truncated   bool
}

// Diff is a diff document: the comparison of one replayed capture.
type Diff struct {
	RunID           string
	CaptureID       string
	Service         string
	Method          string
	Route           string
	Path            string
	Class           string // match, status_mismatch, header_mismatch, body_mismatch, error, skipped
	Reason          string
	ProdStatus      int
	CandidateStatus int
	BaselineStatus  int // 0 without a baseline
	Differences     []Difference
	// Truncated is set when the replayer kept only the first differences.
	Truncated        bool
	TraceID          string // original request
	ReplayTraceID    string
	Start            time.Time // of the original request
	SentAt           time.Time // of the replayed request
	ScheduleLag      time.Duration
	ProdLatency      time.Duration
	CandidateLatency time.Duration
	BaselineLatency  time.Duration
}

// Reported returns the differences the replayer did not suppress as noise.
func (d Diff) Reported() []Difference {
	var out []Difference
	for _, df := range d.Differences {
		if df.Suppressed == "" {
			out = append(out, df)
		}
	}
	return out
}

// Difference is one place where the candidate differed.
type Difference struct {
	Where     string `json:"where"`
	Pointer   string `json:"pointer"`
	Kind      string `json:"kind"`
	Prod      string `json:"prod,omitempty"`
	Candidate string `json:"candidate,omitempty"`
	Detail    string `json:"detail,omitempty"`
	// Suppressed says why the replayer discounted the difference (noise
	// seen on the baseline, an ignore rule); empty for a real difference.
	Suppressed string `json:"suppressed,omitempty"`
}

// Trace is everything indexed for one trace id.
type Trace struct {
	ID       string
	Spans    []*Span // every span, by start time
	Root     *Span   // the span tree; nil without spans
	Logs     []Log   // every log, by time
	Captures []Capture
	Diffs    []Diff
	Start    time.Time
	Duration time.Duration
}

// Empty reports whether nothing at all was indexed for the trace.
func (t *Trace) Empty() bool {
	return t == nil || len(t.Spans) == 0 && len(t.Logs) == 0 && len(t.Captures) == 0 && len(t.Diffs) == 0
}

// Services lists the distinct service names of the spans, in order of first
// appearance.
func (t *Trace) Services() []string {
	var out []string
	seen := map[string]bool{}
	for _, s := range t.Spans {
		if s.Service != "" && !seen[s.Service] {
			seen[s.Service] = true
			out = append(out, s.Service)
		}
	}
	return out
}

// UnattachedLogs are the logs whose span is not in the trace.
func (t *Trace) UnattachedLogs() []Log {
	ids := map[string]bool{}
	for _, s := range t.Spans {
		ids[s.SpanID] = true
	}
	var out []Log
	for _, l := range t.Logs {
		if l.SpanID == "" || !ids[l.SpanID] {
			out = append(out, l)
		}
	}
	return out
}

// Related is a trace reached from the main one with FollowReplay.
type Related struct {
	Relation string // why it is shown: "replay", "original" or "linked"
	Trace    *Trace
}

// Result is what a Query found.
type Result struct {
	Query   Query
	Trace   *Trace
	Related []Related
}

func spanFromDoc(d doc) *Span {
	s := &Span{
		TraceID:       strings.ToLower(d.str(FieldTraceID)),
		SpanID:        strings.ToLower(d.str(FieldSpanID)),
		ParentID:      strings.ToLower(d.str(FieldParentID)),
		Name:          d.str(FieldSpanName),
		Kind:          normalizeKind(d.str(FieldSpanKind)),
		Service:       d.str(FieldServiceName),
		Start:         d.when(FieldTimestamp),
		Duration:      time.Duration(d.num(FieldDuration)),
		Status:        normalizeStatus(d.str(FieldStatusCode)),
		StatusMessage: d.str(FieldStatusMessage),
		HTTPMethod:    d.str(FieldHTTPMethod),
		URLPath:       d.str(FieldURLPath),
		HTTPStatus:    int(d.num(FieldHTTPStatus)),
		Attributes:    d.flatten(FieldAttributes),
	}
	if s.Duration < 0 {
		s.Duration = 0
	}
	for _, l := range d.list(FieldLinks) {
		m, ok := l.(map[string]any)
		if !ok {
			continue
		}
		link := Link{TraceID: strings.ToLower(doc(m).str(FieldTraceID)), SpanID: strings.ToLower(doc(m).str(FieldSpanID))}
		if link.TraceID == "" {
			// Some exporters flatten link fields with underscores.
			link.TraceID = strings.ToLower(doc(m).str("trace_id"))
			link.SpanID = strings.ToLower(doc(m).str("span_id"))
		}
		if link.TraceID != "" {
			s.Links = append(s.Links, link)
		}
	}
	return s
}

// normalizeKind maps the span kind however it was stored (SERVER,
// SPAN_KIND_SERVER, Server, 2) to the bare upper-case name.
func normalizeKind(k string) string {
	k = strings.ToUpper(strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(k)), "SPAN_KIND_"))
	switch k {
	case "", "0", "UNSPECIFIED":
		return ""
	case "1":
		return "INTERNAL"
	case "2":
		return "SERVER"
	case "3":
		return "CLIENT"
	case "4":
		return "PRODUCER"
	case "5":
		return "CONSUMER"
	}
	return k
}

// normalizeStatus maps a status code however it was stored (Ok, STATUS_CODE_OK,
// 1) to OK, ERROR or "" for unset.
func normalizeStatus(s string) string {
	s = strings.ToUpper(strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(s)), "STATUS_CODE_"))
	switch s {
	case "", "0", "UNSET":
		return ""
	case "1", "OK":
		return "OK"
	case "2", "ERROR":
		return "ERROR"
	}
	return s
}

func logFromDoc(d doc) Log {
	return Log{
		Time:       d.when(FieldTimestamp),
		TraceID:    strings.ToLower(d.str(FieldTraceID)),
		SpanID:     strings.ToLower(d.str(FieldSpanID)),
		Service:    d.str(FieldServiceName),
		Level:      strings.ToUpper(d.str(FieldLogLevel)),
		Message:    d.str(FieldMessage),
		Attributes: d.flatten(FieldAttributes),
	}
}

func captureFromDoc(d doc) Capture {
	c := Capture{
		ID:        d.str(FieldCaptureID),
		TraceID:   strings.ToLower(d.str(FieldTraceID)),
		ParentID:  strings.ToLower(d.str(FieldParentID)),
		RequestID: d.str(FieldRequestID),
		Service:   d.str(FieldServiceName),
		Method:    d.str(FieldHTTPMethod),
		Route:     d.str(FieldHTTPRoute),
		Path:      d.str(FieldURLPath),
		Status:    int(d.num(FieldHTTPStatus)),
		Start:     d.when(FieldTimestamp),
		Duration:  time.Duration(d.num(FieldDuration)),
		Tap:       d.firstStr(FieldTapName, FieldTapNameAlt),
		Mode:      d.firstStr(FieldTapMode, FieldTapModeAlt),
		Request: Body{
			ContentType: d.str(FieldRequestBodyType),
			Content:     d.str(FieldRequestBody),
			Size:        d.num(FieldRequestBodyBytes),
			Omitted:     d.str(FieldRequestOmitted),
			Truncated:   d.boolean(FieldRequestTruncated),
		},
		Response: Body{
			ContentType: d.str(FieldResponseBodyType),
			Content:     d.str(FieldResponseBody),
			Size:        d.num(FieldResponseBodyBytes),
			Omitted:     d.str(FieldResponseOmitted),
			Truncated:   d.boolean(FieldResponseTruncated),
		},
	}
	if red := d.object(FieldRedactions); len(red) > 0 {
		c.Redactions = map[string]int64{}
		for k := range red {
			c.Redactions[k] = red.num(k)
		}
	}
	return c
}

func diffFromDoc(d doc) Diff {
	df := Diff{
		RunID:            d.str(FieldRunID),
		CaptureID:        d.str(FieldCaptureID),
		Service:          d.str(FieldServiceName),
		Method:           d.str(FieldHTTPMethod),
		Route:            d.str(FieldHTTPRoute),
		Path:             d.str(FieldURLPath),
		Class:            d.str(FieldDiffClass),
		Reason:           d.str(FieldDiffReason),
		ProdStatus:       int(d.num(FieldHTTPStatus)),
		CandidateStatus:  int(d.firstNum(FieldReplayStatus, FieldReplayStatusAlt)),
		BaselineStatus:   int(d.num(FieldBaselineStatus)),
		Truncated:        d.boolean(FieldDiffTruncated),
		TraceID:          strings.ToLower(d.str(FieldTraceID)),
		ReplayTraceID:    strings.ToLower(d.str(FieldReplayTraceID)),
		Start:            d.when(FieldEventStart),
		SentAt:           d.when(FieldReplaySentAt),
		ScheduleLag:      time.Duration(d.num(FieldReplayLag)) * time.Microsecond,
		ProdLatency:      d.latency(FieldDuration, FieldLatencyProd),
		CandidateLatency: d.latency(FieldReplayDuration, FieldLatencyCandidate),
		BaselineLatency:  time.Duration(d.num(FieldLatencyBaseline)) * time.Microsecond,
	}
	// @timestamp is the send time, falling back to the original start when
	// the replayer recorded none; a document without event.start is older
	// than that convention, so the one timestamp stands for both.
	if df.Start.IsZero() {
		df.Start = d.when(FieldTimestamp)
	}
	if df.SentAt.IsZero() {
		df.SentAt = d.when(FieldTimestamp)
	}
	for _, v := range d.list(FieldDiffDifferences) {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		dd := doc(m)
		df.Differences = append(df.Differences, Difference{
			Where:      dd.str("where"),
			Pointer:    dd.str("pointer"),
			Kind:       dd.str("kind"),
			Prod:       dd.str("prod"),
			Candidate:  dd.str("candidate"),
			Detail:     dd.str("detail"),
			Suppressed: dd.str("suppressed"),
		})
	}
	return df
}

// assemble turns the hits of one trace id into a Trace with its tree built.
func assemble(id string, spans []*Span, logs []Log, captures []Capture, diffs []Diff) *Trace {
	t := &Trace{ID: id, Spans: spans, Logs: logs, Captures: captures, Diffs: diffs}
	sort.SliceStable(t.Spans, func(i, j int) bool { return spanLess(t.Spans[i], t.Spans[j]) })
	sort.SliceStable(t.Logs, func(i, j int) bool { return t.Logs[i].Time.Before(t.Logs[j].Time) })
	sort.SliceStable(t.Captures, func(i, j int) bool { return t.Captures[i].Start.Before(t.Captures[j].Start) })
	sort.SliceStable(t.Diffs, func(i, j int) bool {
		if !t.Diffs[i].SentAt.Equal(t.Diffs[j].SentAt) {
			return t.Diffs[i].SentAt.Before(t.Diffs[j].SentAt)
		}
		return t.Diffs[i].RunID < t.Diffs[j].RunID
	})
	t.Root = buildTree(t.Spans, t.Logs)
	if t.Root != nil {
		t.Start, t.Duration = t.Root.Start, t.Root.Duration
	} else if len(t.Captures) > 0 {
		t.Start, t.Duration = t.Captures[0].Start, t.Captures[0].Duration
	} else if len(t.Logs) > 0 {
		t.Start = t.Logs[0].Time
	}
	return t
}

// spanLess orders spans by start, then id, so output is deterministic when
// spans share a timestamp.
func spanLess(a, b *Span) bool {
	if !a.Start.Equal(b.Start) {
		return a.Start.Before(b.Start)
	}
	return a.SpanID < b.SpanID
}
