package tracequery

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// RenderOptions controls the text rendering.
type RenderOptions struct {
	// ShowBodies prints the scrubbed request and response bodies of captures.
	ShowBodies bool
	// BarWidth is the width of the timeline bar in cells; 0 means 30.
	BarWidth int
}

const (
	defaultBarWidth = 30
	timeLayout      = "2006-01-02T15:04:05.000000Z"
)

// Render writes the result as text: capture and diff summaries, then the span
// waterfall with each span's logs beneath it, then logs that belong to no
// indexed span, then any related traces.
func Render(w io.Writer, r *Result, opts RenderOptions) error {
	if opts.BarWidth <= 0 {
		opts.BarWidth = defaultBarWidth
	}
	p := &printer{w: w, opts: opts}
	p.trace(r.Trace, "trace")
	for _, rel := range r.Related {
		p.line("")
		p.trace(rel.Trace, rel.Relation+" trace")
	}
	return p.err
}

type printer struct {
	w    io.Writer
	opts RenderOptions
	err  error
}

func (p *printer) line(format string, args ...any) {
	if p.err != nil {
		return
	}
	_, p.err = fmt.Fprintf(p.w, format+"\n", args...)
}

func (p *printer) trace(t *Trace, heading string) {
	switch {
	case t.ID == "":
		p.line("capture without trace context: nothing to correlate")
	default:
		p.line("%s %s", heading, t.ID)
	}
	if !t.Start.IsZero() {
		p.line("  started  %s", t.Start.UTC().Format(timeLayout))
	}
	var parts []string
	if len(t.Spans) > 0 {
		parts = append(parts, fmt.Sprintf("duration %s", fmtDuration(t.Duration)))
		svcs := t.Services()
		parts = append(parts, fmt.Sprintf("services %d (%s)", len(svcs), strings.Join(svcs, ", ")))
		parts = append(parts, fmt.Sprintf("spans %d", len(t.Spans)))
	}
	parts = append(parts, fmt.Sprintf("logs %d", len(t.Logs)), fmt.Sprintf("captures %d", len(t.Captures)), fmt.Sprintf("diffs %d", len(t.Diffs)))
	p.line("  %s", strings.Join(parts, "   "))

	for _, c := range t.Captures {
		p.line("")
		p.capture(c)
	}
	for _, d := range t.Diffs {
		p.line("")
		p.diff(d, t.ID)
	}

	p.line("")
	if t.Root == nil {
		p.line("spans: none indexed for this trace")
	} else {
		p.spans(t)
	}
	if un := t.UnattachedLogs(); len(un) > 0 {
		p.line("")
		p.line("logs without an indexed span")
		for _, l := range un {
			p.log(l, t.Start, "  ")
		}
	}
}

func (p *printer) capture(c Capture) {
	p.line("capture %s", orDash(c.ID))
	var head []string
	if !c.Start.IsZero() {
		head = append(head, c.Start.UTC().Format(timeLayout))
	}
	head = append(head, orDash(c.Service), fmt.Sprintf("%s %s -> %d", orDash(c.Method), orDash(c.Path), c.Status), fmtDuration(c.Duration))
	p.line("  %s", strings.Join(head, "  "))
	var meta []string
	if c.Route != "" {
		meta = append(meta, "route "+quote(c.Route))
	}
	if c.RequestID != "" {
		meta = append(meta, "request-id "+c.RequestID)
	}
	if c.Tap != "" {
		tap := "tap " + c.Tap
		if c.Mode != "" {
			tap += " (" + c.Mode + ")"
		}
		meta = append(meta, tap)
	}
	if len(c.Redactions) > 0 {
		meta = append(meta, "redactions "+formatCounts(c.Redactions))
	}
	if len(meta) > 0 {
		p.line("  %s", strings.Join(meta, "  "))
	}
	p.line("  request body %s   response body %s", bodySummary(c.Request), bodySummary(c.Response))
	if p.opts.ShowBodies {
		p.body("request", c.Request)
		p.body("response", c.Response)
	}
}

func bodySummary(b Body) string {
	switch {
	case b.Omitted != "":
		return fmt.Sprintf("omitted (%s, %d B)", b.Omitted, b.Size)
	case b.Content == "" && b.Size == 0:
		return "none"
	}
	s := fmt.Sprintf("%s %d B", orDash(b.ContentType), b.Size)
	if b.Truncated {
		s += " truncated"
	}
	return s
}

func (p *printer) body(label string, b Body) {
	if b.Content == "" {
		return
	}
	p.line("  %s body:", label)
	for _, l := range strings.Split(b.Content, "\n") {
		p.line("    %s", l)
	}
}

func (p *printer) diff(d Diff, traceID string) {
	p.line("diff %s  class %s", orDash(d.RunID), orDash(d.Class))
	var head []string
	if d.CaptureID != "" {
		head = append(head, "capture "+d.CaptureID)
	}
	head = append(head, routeLabel(d.Method, d.Route, d.Path))
	status := fmt.Sprintf("status prod %d candidate %d", d.ProdStatus, d.CandidateStatus)
	if d.BaselineStatus != 0 {
		status += fmt.Sprintf(" baseline %d", d.BaselineStatus)
	}
	head = append(head, status)
	if d.ProdLatency > 0 || d.CandidateLatency > 0 {
		latency := fmt.Sprintf("latency prod %s candidate %s", fmtDuration(d.ProdLatency), fmtDuration(d.CandidateLatency))
		if d.BaselineLatency > 0 {
			latency += " baseline " + fmtDuration(d.BaselineLatency)
		}
		head = append(head, latency)
	}
	p.line("  %s", strings.Join(head, "  "))
	var links []string
	if !d.SentAt.IsZero() {
		links = append(links, "replayed "+d.SentAt.UTC().Format(timeLayout))
	}
	if d.ScheduleLag != 0 {
		links = append(links, "schedule lag "+fmtOffset(d.ScheduleLag))
	}
	switch {
	case d.ReplayTraceID != "" && d.ReplayTraceID != traceID:
		links = append(links, "replay trace "+d.ReplayTraceID)
	case d.TraceID != "" && d.TraceID != traceID:
		links = append(links, "original trace "+d.TraceID)
	}
	if len(links) > 0 {
		p.line("  %s", strings.Join(links, "  "))
	}
	if d.Reason != "" {
		p.line("  reason %s", d.Reason)
	}
	for _, df := range d.Differences {
		s := fmt.Sprintf("  %s %s %s", orDash(df.Where), orDash(df.Pointer), orDash(df.Kind))
		if df.Prod != "" || df.Candidate != "" {
			s += fmt.Sprintf(": prod %s candidate %s", quote(df.Prod), quote(df.Candidate))
		}
		if df.Detail != "" {
			s += "  (" + df.Detail + ")"
		}
		if df.Suppressed != "" {
			s += "  [suppressed: " + df.Suppressed + "]"
		}
		p.line("%s", s)
	}
	if d.Truncated {
		p.line("  ... more differences not recorded")
	}
}

func (p *printer) spans(t *Trace) {
	width := p.opts.BarWidth
	p.line("spans")
	p.line("  %10s  %10s  %s  %s", "offset", "duration", strings.Repeat(" ", width+2), "service  name  [kind]  status")
	walk(t.Root, func(s *Span, depth int) {
		indent := strings.Repeat("  ", depth)
		if s.Synthetic {
			p.line("  %10s  %10s  %s  %s(trace)  %d root or orphan spans", "+0.000ms", fmtDuration(s.Duration), bar(0, s.Duration, t.Start, t.Duration, width), indent, len(s.Children))
			return
		}
		offset := s.Start.Sub(t.Start)
		desc := fmt.Sprintf("%s  %s", orDash(s.Service), orDash(s.Name))
		if s.Kind != "" {
			desc += "  [" + s.Kind + "]"
		}
		desc += "  " + spanStatus(s)
		if s.HTTPMethod != "" || s.URLPath != "" || s.HTTPStatus != 0 {
			http := strings.TrimSpace(s.HTTPMethod + " " + s.URLPath)
			if s.HTTPStatus != 0 {
				http = strings.TrimSpace(http + " -> " + strconv.Itoa(s.HTTPStatus))
			}
			desc += "  " + http
		}
		if s.Orphan {
			desc += "  [parent " + s.ParentID + " not indexed]"
		}
		for _, l := range s.Links {
			desc += "  [link " + l.TraceID
			if l.SpanID != "" {
				desc += "/" + l.SpanID
			}
			desc += "]"
		}
		p.line("  %10s  %10s  %s  %s%s", fmtOffset(offset), fmtDuration(s.Duration), bar(offset, s.Duration, t.Start, t.Duration, width), indent, desc)
		for _, l := range s.Logs {
			p.log(l, t.Start, "        ")
		}
	})
}

func (p *printer) log(l Log, traceStart time.Time, indent string) {
	when := "-"
	switch {
	case l.Time.IsZero():
	case traceStart.IsZero():
		when = l.Time.UTC().Format(timeLayout)
	default:
		when = fmtOffset(l.Time.Sub(traceStart))
	}
	s := fmt.Sprintf("%slog %10s  %-5s  %s  %s", indent, when, orDash(l.Level), orDash(l.Service), l.Message)
	if len(l.Attributes) > 0 {
		s += "  " + formatAttrs(l.Attributes)
	}
	p.line("%s", s)
}

func spanStatus(s *Span) string {
	switch s.Status {
	case "":
		return "-"
	case "ERROR":
		if s.StatusMessage != "" {
			return "ERROR (" + s.StatusMessage + ")"
		}
		return "ERROR"
	}
	return s.Status
}

// bar draws the span's extent within the trace on a line of width cells.
func bar(offset, dur time.Duration, traceStart time.Time, total time.Duration, width int) string {
	cells := make([]byte, width)
	for i := range cells {
		cells[i] = ' '
	}
	if total > 0 && !traceStart.IsZero() {
		begin := int(float64(offset) / float64(total) * float64(width))
		end := int(float64(offset+dur)/float64(total)*float64(width) + 0.5)
		begin = min(max(begin, 0), width-1)
		end = min(max(end, begin+1), width)
		for i := begin; i < end; i++ {
			cells[i] = '#'
		}
	}
	return "|" + string(cells) + "|"
}

func fmtOffset(d time.Duration) string {
	if d < 0 {
		return "-" + fmtDuration(-d)
	}
	return "+" + fmtDuration(d)
}

func fmtDuration(d time.Duration) string {
	if d >= 10*time.Second {
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
	return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond))
}

func formatAttrs(m map[string]string) string {
	parts := make([]string, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, k+"="+quote(m[k]))
	}
	return strings.Join(parts, " ")
}

func formatCounts(m map[string]int64) string {
	parts := make([]string, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[k]))
	}
	return strings.Join(parts, ",")
}

// routeLabel renders "METHOD route". The replayer's route templates already
// start with the method ("GET /api/users/{id}"), so it is not repeated; a
// document without a route falls back to the path.
func routeLabel(method, route, path string) string {
	if route == "" {
		route = path
	}
	switch {
	case route == "":
		return orDash(method)
	case method == "" || strings.HasPrefix(route, method+" "):
		return route
	}
	return method + " " + route
}

// quote renders a value so spaces and emptiness are visible; plain tokens
// stay bare.
func quote(s string) string {
	if s == "" || strings.ContainsAny(s, " \t\n\"\\") {
		return strconv.Quote(s)
	}
	return s
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
