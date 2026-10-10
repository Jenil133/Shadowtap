package tracequery

import (
	"sort"
	"time"
)

// buildTree links spans to their parents through parent.id and attaches each
// log to its span. Spans without a parent are roots; a span whose parent is
// not in the trace is an orphan. The result is the single root span when the
// trace has exactly one and no orphans, and otherwise a synthetic root that
// spans the whole trace and holds every root and orphan as a child. Spans
// come in any order, duplicates (redelivered documents) collapse on span id,
// and a cycle in parent ids cannot hang the walk: anything the root does not
// reach is attached to it as an orphan.
func buildTree(spans []*Span, logs []Log) *Span {
	if len(spans) == 0 {
		return nil
	}
	byID := make(map[string]*Span, len(spans))
	unique := make([]*Span, 0, len(spans))
	for _, s := range spans {
		s.Children, s.Logs, s.Orphan = nil, nil, false
		if s.SpanID == "" {
			// Without an id nothing can be its child; keep it as a leaf.
			unique = append(unique, s)
			continue
		}
		if _, dup := byID[s.SpanID]; dup {
			continue
		}
		byID[s.SpanID] = s
		unique = append(unique, s)
	}
	for _, l := range logs {
		if s, ok := byID[l.SpanID]; ok && l.SpanID != "" {
			s.Logs = append(s.Logs, l)
		}
	}

	var top []*Span // roots and orphans, in input (start) order
	for _, s := range unique {
		if s.ParentID == "" || s.ParentID == s.SpanID {
			top = append(top, s)
			continue
		}
		parent, ok := byID[s.ParentID]
		if !ok {
			s.Orphan = true
			top = append(top, s)
			continue
		}
		parent.Children = append(parent.Children, s)
	}

	// Spans in a parent cycle are nobody's root and nobody reachable from one;
	// surface them rather than lose them.
	reached := map[*Span]bool{}
	for _, s := range top {
		markReached(s, reached)
	}
	for _, s := range unique {
		if !reached[s] && s.ParentID != "" {
			if p, ok := byID[s.ParentID]; ok {
				p.Children = removeSpan(p.Children, s)
			}
			s.Orphan = true
			top = append(top, s)
			markReached(s, reached)
		}
	}
	for _, s := range unique {
		sort.SliceStable(s.Children, func(i, j int) bool { return spanLess(s.Children[i], s.Children[j]) })
	}
	sort.SliceStable(top, func(i, j int) bool { return spanLess(top[i], top[j]) })

	if len(top) == 1 && !top[0].Orphan {
		return top[0]
	}
	start, end := extent(unique)
	root := &Span{
		TraceID:   unique[0].TraceID,
		Name:      "trace",
		Start:     start,
		Duration:  end.Sub(start),
		Children:  top,
		Synthetic: true,
	}
	return root
}

func markReached(s *Span, reached map[*Span]bool) {
	if reached[s] {
		return
	}
	reached[s] = true
	for _, c := range s.Children {
		markReached(c, reached)
	}
}

func removeSpan(list []*Span, s *Span) []*Span {
	out := list[:0]
	for _, c := range list {
		if c != s {
			out = append(out, c)
		}
	}
	return out
}

// extent returns the earliest start and latest end of the spans.
func extent(spans []*Span) (start, end time.Time) {
	for _, s := range spans {
		if s.Start.IsZero() {
			continue
		}
		if start.IsZero() || s.Start.Before(start) {
			start = s.Start
		}
		if e := s.End(); end.IsZero() || e.After(end) {
			end = e
		}
	}
	return start, end
}

// walk visits the tree depth first, children in start order, with each
// span's depth (the root is 0).
func walk(root *Span, visit func(s *Span, depth int)) {
	var rec func(s *Span, depth int)
	rec = func(s *Span, depth int) {
		visit(s, depth)
		for _, c := range s.Children {
			rec(c, depth+1)
		}
	}
	if root != nil {
		rec(root, 0)
	}
}
