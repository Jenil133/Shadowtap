package diff

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/scrub"
)

// Result classes, from most to least severe.
const (
	ClassError          = "error"           // the candidate could not be reached or timed out
	ClassStatusMismatch = "status_mismatch" // different status code
	ClassHeaderMismatch = "header_mismatch" // same status, a compared header differs
	ClassBodyMismatch   = "body_mismatch"   // same status and headers, the body differs
	ClassMatch          = "match"
	ClassSkipped        = "skipped" // not replayed (filtered or not rebuildable)
)

// Where a difference is.
const (
	WhereStatus = "status"
	WhereHeader = "header"
	WhereBody   = "body"
)

// Reasons a difference is suppressed.
const (
	SuppressedBaseline = "baseline" // the baseline answered like the candidate
	SuppressedNoise    = "noise"    // this place differs between two baseline responses to one request
)

// SchemaVersion is the version of Result on the diffs topic.
const SchemaVersion = 1

// DefaultTopic is the Kafka topic diff results are published to.
const DefaultTopic = "shadowtap.diffs.v1"

// Difference is one place where the candidate's response differs from the
// recorded production response. Values are rendered from scrubbed data.
type Difference struct {
	Where     string `json:"where"`
	Pointer   string `json:"pointer"` // JSON Pointer, header name, or text position
	Kind      string `json:"kind"`
	Prod      string `json:"prod,omitempty"`
	Candidate string `json:"candidate,omitempty"`
	// Baseline is the baseline's value at the same place when a baseline
	// was compared: it equals Candidate when the baseline shares the
	// difference, and is empty when the baseline lacks the place.
	Baseline   string `json:"baseline,omitempty"`
	Detail     string `json:"detail,omitempty"`
	Suppressed string `json:"suppressed,omitempty"`

	template string // Pointer with array indexes as "*", for noise statistics
}

// Key identifies the place of a difference across exchanges of a route.
func (d Difference) Key() string {
	t := d.template
	if t == "" {
		t = d.Pointer
	}
	return d.Where + ":" + t
}

// Status holds the status codes of each side; 0 when a side has none.
type Status struct {
	Prod      int `json:"prod"`
	Candidate int `json:"candidate"`
	Baseline  int `json:"baseline,omitempty"`
}

// Latency holds each side's time to response headers.
type Latency struct {
	ProdMicros      int64 `json:"prod_us"`
	CandidateMicros int64 `json:"candidate_us"`
	BaselineMicros  int64 `json:"baseline_us,omitempty"`
}

// Result is the comparison of one replayed exchange.
type Result struct {
	SchemaVersion   int          `json:"schema_version"`
	RunID           string       `json:"run_id"`
	CaptureID       string       `json:"capture_id"`
	Service         string       `json:"service"`
	Route           string       `json:"route"`
	Method          string       `json:"method"`
	Path            string       `json:"path"`
	Class           string       `json:"class"`
	Reason          string       `json:"reason,omitempty"` // why skipped, or the error
	Status          Status       `json:"status"`
	Differences     []Difference `json:"differences,omitempty"`
	Truncated       bool         `json:"differences_truncated,omitempty"`
	Latency         Latency      `json:"latency"`
	OriginalTraceID string       `json:"original_trace_id,omitempty"`
	ReplayTraceID   string       `json:"replay_trace_id,omitempty"`
	StartedAt       time.Time    `json:"started_at"` // of the original request
	SentAt          time.Time    `json:"sent_at,omitzero"`
	LagMicros       int64        `json:"schedule_lag_us,omitempty"`

	final bool
}

// Reported returns the differences that are not suppressed.
func (r *Result) Reported() []Difference {
	var out []Difference
	for _, d := range r.Differences {
		if d.Suppressed == "" {
			out = append(out, d)
		}
	}
	return out
}

// Side is one normalized response.
type Side struct {
	Status  int
	Headers capture.Headers
	// Body is the decoded, scrubbed body; nil when it is not comparable.
	Body        []byte
	ContentType string
	Size        int64
	// NotComparable says why Body is nil: the body was omitted or cut.
	NotComparable string
}

// Recorded returns the production side of a capture. It is already scrubbed
// and decoded by the tap.
func Recorded(x *capture.Exchange) Side {
	r := x.Response
	s := Side{Status: r.Status, Headers: r.Headers, ContentType: r.Body.ContentType, Size: r.Body.Size}
	if s.ContentType == "" {
		s.ContentType = r.Headers.Get("content-type")
	}
	switch {
	case r.Body.Omitted != "":
		s.NotComparable = r.Body.Omitted
	case r.Body.Truncated:
		s.NotComparable = "truncated"
	default:
		b, err := r.Body.Bytes()
		if err != nil {
			s.NotComparable = capture.OmittedUnparseable
		} else {
			s.Body = b
		}
	}
	return s
}

// Options configures an Engine.
type Options struct {
	// MaxBodyBytes bounds how much of a live response body is read; 0 means
	// 8 MiB.
	MaxBodyBytes int64
}

// Engine normalizes live responses and compares them with recordings. It is
// safe for concurrent use. Results are provisional until Finalize, which
// applies the noise statistics gathered over the whole run.
type Engine struct {
	cfg      *compiled
	scrubber *scrub.Scrubber
	maxBody  int64

	mu      sync.Mutex
	samples map[string]int            // route -> noise probes
	noisy   map[string]map[string]int // route -> difference key -> count
}

// New returns an Engine. scrubber must use the tap's key so that pseudonyms
// in recordings and live responses agree.
func New(cfg Config, scrubber *scrub.Scrubber, opts Options) (*Engine, error) {
	if scrubber == nil {
		return nil, errors.New("diff: scrubber is nil")
	}
	c, err := compile(cfg)
	if err != nil {
		return nil, err
	}
	maxBody := opts.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = 8 << 20
	}
	return &Engine{cfg: c, scrubber: scrubber, maxBody: maxBody, samples: map[string]int{}, noisy: map[string]map[string]int{}}, nil
}

// Normalize reads a live response, decodes its body and scrubs headers and
// body with the tap's scrubber, so it compares with a recording like for
// like. It closes resp.Body.
func (e *Engine) Normalize(resp *http.Response) (Side, error) {
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, e.maxBody+1))
	if err != nil {
		return Side{}, fmt.Errorf("diff: read response body: %w", err)
	}
	truncated := int64(len(raw)) > e.maxBody
	if truncated {
		raw = raw[:e.maxBody]
	}
	headers := capture.Headers{}
	for k, vs := range resp.Header {
		for _, v := range vs {
			headers.Add(k, v)
		}
	}
	s := Side{Status: resp.StatusCode, ContentType: headers.Get("content-type"), Size: int64(len(raw))}
	res := e.scrubber.Body(scrub.BodyInput{
		Data:            raw,
		ContentType:     s.ContentType,
		ContentEncoding: strings.Join(headers["content-encoding"], ","),
		Truncated:       truncated,
	}, nil)
	switch {
	case res.Omitted != "":
		s.NotComparable = res.Omitted
	case res.Truncated:
		s.NotComparable = "truncated"
	default:
		s.Body = res.Data
	}
	s.Headers = e.scrubber.Headers(headers, nil)
	return s, nil
}

// Live is the outcome of sending a replayed request to one target.
type Live struct {
	Side    *Side // nil when Err is set
	Err     error
	Latency time.Duration // to response headers
}

// Compare compares the candidate (and the baseline, if not nil) with the
// recording x. probe, if not nil, is a second baseline response to the same
// request; its differences from the first teach the engine the route's
// noise. The result is provisional until Finalize.
func (e *Engine) Compare(runID string, x *capture.Exchange, cand Live, base, probe *Live) *Result {
	r := &Result{
		SchemaVersion:   SchemaVersion,
		RunID:           runID,
		CaptureID:       x.ID,
		Service:         x.Service,
		Route:           RouteTemplate(x.Request.Method, x.Request.Path),
		Method:          x.Request.Method,
		Path:            x.Request.Path,
		OriginalTraceID: x.TraceID,
		StartedAt:       x.Timing.StartedAt,
		Status:          Status{Prod: x.Response.Status},
		Latency:         Latency{ProdMicros: x.Timing.DurationMicros, CandidateMicros: cand.Latency.Microseconds()},
	}
	if cand.Err != nil {
		r.Class, r.Reason, r.final = ClassError, cand.Err.Error(), true
		return r
	}
	prod := Recorded(x)
	r.Status.Candidate = cand.Side.Status
	r.Differences, r.Truncated = e.compare(r.Route, prod, *cand.Side)

	if base != nil {
		r.Latency.BaselineMicros = base.Latency.Microseconds()
		if base.Err == nil {
			r.Status.Baseline = base.Side.Status
			// A difference the baseline shares comes from the replay, not
			// from the change: suppress it. Either way the baseline's value
			// is kept, so a report can show all three sides.
			bc, _ := e.compare(r.Route, *base.Side, *cand.Side)
			baseline := map[string]string{} // place -> the baseline's value
			differs := map[string]bool{}
			for _, d := range bc {
				k := placeKey(d)
				differs[k] = true
				baseline[k] = d.Prod // bc was compared with the baseline as "prod"
			}
			for i, d := range r.Differences {
				k := placeKey(d)
				if !differs[k] {
					r.Differences[i].Suppressed = SuppressedBaseline
					r.Differences[i].Baseline = d.Candidate
					continue
				}
				r.Differences[i].Baseline = baseline[k]
			}
			if probe != nil && probe.Err == nil {
				noise, _ := e.compare(r.Route, *base.Side, *probe.Side)
				e.learn(r.Route, noise)
			}
		}
	}
	return r
}

// placeKey identifies a place within one exchange. Text differences are one
// place: their line numbers shift with every insertion.
func placeKey(d Difference) string {
	if d.Where == WhereBody && strings.HasPrefix(d.Pointer, "line ") {
		return "text"
	}
	return d.Where + ":" + d.Pointer
}

// learn records which places differ between two baseline responses.
func (e *Engine) learn(route string, diffs []Difference) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.samples[route]++
	m := e.noisy[route]
	if m == nil {
		m = map[string]int{}
		e.noisy[route] = m
	}
	seen := map[string]bool{}
	for _, d := range diffs {
		if k := d.Key(); !seen[k] {
			seen[k] = true
			m[k]++
		}
	}
}

// Noisy reports whether a difference key is noise for route.
func (e *Engine) Noisy(route, key string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.samples[route]
	return n >= MinNoiseSamples && float64(e.noisy[route][key])/float64(n) > e.cfg.threshold
}

// NoiseReport lists, per route, the places that differ between two baseline
// responses in more than the threshold share of probes.
func (e *Engine) NoiseReport() map[string][]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := map[string][]string{}
	for route, m := range e.noisy {
		n := e.samples[route]
		if n < MinNoiseSamples {
			continue
		}
		for k, c := range m {
			if float64(c)/float64(n) > e.cfg.threshold {
				out[route] = append(out[route], k)
			}
		}
		sort.Strings(out[route])
	}
	return out
}

// Finalize applies the noise statistics to r and sets its class. Call it
// once every exchange of the run has been compared. It is idempotent.
func (e *Engine) Finalize(r *Result) {
	if r.final {
		return
	}
	r.final = true
	for i, d := range r.Differences {
		if d.Suppressed == "" && e.Noisy(r.Route, d.Key()) {
			r.Differences[i].Suppressed = SuppressedNoise
		}
	}
	r.Class = ClassMatch
	for _, d := range r.Reported() {
		switch {
		case d.Where == WhereStatus:
			r.Class = ClassStatusMismatch
		case d.Where == WhereHeader && r.Class != ClassStatusMismatch:
			r.Class = ClassHeaderMismatch
		case d.Where == WhereBody && r.Class == ClassMatch:
			r.Class = ClassBodyMismatch
		}
	}
}

// Skipped returns the result of a capture that was not replayed.
func Skipped(runID string, x *capture.Exchange, reason string) *Result {
	return &Result{
		SchemaVersion:   SchemaVersion,
		RunID:           runID,
		CaptureID:       x.ID,
		Service:         x.Service,
		Route:           RouteTemplate(x.Request.Method, x.Request.Path),
		Method:          x.Request.Method,
		Path:            x.Request.Path,
		Class:           ClassSkipped,
		Reason:          reason,
		Status:          Status{Prod: x.Response.Status},
		OriginalTraceID: x.TraceID,
		StartedAt:       x.Timing.StartedAt,
		final:           true,
	}
}

// compare returns the differences of b relative to a.
func (e *Engine) compare(route string, a, b Side) ([]Difference, bool) {
	var diffs []Difference
	if a.Status != b.Status {
		diffs = append(diffs, Difference{Where: WhereStatus, Kind: KindChanged, Prod: fmt.Sprint(a.Status), Candidate: fmt.Sprint(b.Status)})
	}
	ignoreHeaders, ignorePtrs := e.routeRules(route)
	diffs = append(diffs, e.compareHeaders(a.Headers, b.Headers, ignoreHeaders)...)
	body, truncated := e.compareBodies(a, b, ignorePtrs)
	diffs = append(diffs, body...)
	if len(diffs) > e.cfg.maxDiffs {
		diffs, truncated = diffs[:e.cfg.maxDiffs], true
	}
	return diffs, truncated
}

func (e *Engine) routeRules(route string) (map[string]bool, []pattern) {
	headers := e.cfg.ignoreHeaders
	var ptrs []pattern
	for _, r := range e.cfg.routes {
		if !r.re.MatchString(route) {
			continue
		}
		if len(r.ignoreHeaders) > 0 {
			merged := make(map[string]bool, len(headers)+len(r.ignoreHeaders))
			for k := range headers {
				merged[k] = true
			}
			for k := range r.ignoreHeaders {
				merged[k] = true
			}
			headers = merged
		}
		ptrs = append(ptrs, r.ignore...)
	}
	return headers, ptrs
}

// compareHeaders compares the headers whose meaning a release can change:
// content-type (media type), location (path), cache-control (directives),
// set-cookie (cookie names), plus configured headers exactly.
func (e *Engine) compareHeaders(a, b capture.Headers, ignore map[string]bool) []Difference {
	var diffs []Difference
	check := func(name string, norm func([]string) string) {
		if ignore[name] {
			return
		}
		x, y := norm(a[name]), norm(b[name])
		if x == y {
			return
		}
		kind := KindChanged
		switch {
		case len(a[name]) == 0:
			kind = KindAdded
		case len(b[name]) == 0:
			kind = KindRemoved
		}
		diffs = append(diffs, Difference{Where: WhereHeader, Pointer: name, Kind: kind, Prod: clip(x), Candidate: clip(y), template: name})
	}
	check("content-type", mediaType)
	check("location", locationPath)
	check("cache-control", directives)
	check("set-cookie", cookieNames)
	for _, h := range e.cfg.compareHeaders {
		if !slices.Contains([]string{"content-type", "location", "cache-control", "set-cookie"}, h) {
			check(h, func(v []string) string { return strings.Join(v, ", ") })
		}
	}
	return diffs
}

func mediaType(v []string) string {
	if len(v) == 0 {
		return ""
	}
	mt, _, err := mime.ParseMediaType(v[0])
	if err != nil {
		return strings.ToLower(strings.TrimSpace(v[0]))
	}
	return mt
}

func locationPath(v []string) string {
	if len(v) == 0 {
		return ""
	}
	u, err := url.Parse(v[0])
	if err != nil {
		return v[0]
	}
	return u.EscapedPath()
}

func directives(v []string) string {
	var ds []string
	for _, h := range v {
		for _, d := range strings.Split(h, ",") {
			if d = strings.ToLower(strings.TrimSpace(d)); d != "" {
				ds = append(ds, d)
			}
		}
	}
	slices.Sort(ds)
	return strings.Join(ds, ", ")
}

func cookieNames(v []string) string {
	var names []string
	for _, h := range v {
		name, _, _ := strings.Cut(h, "=")
		names = append(names, strings.TrimSpace(name))
	}
	slices.Sort(names)
	return strings.Join(names, ", ")
}

// compareBodies compares two bodies: JSON structurally, text by lines,
// omitted bodies by size when both are binary, and not at all when either
// side was cut or could not be scrubbed.
func (e *Engine) compareBodies(a, b Side, ignore []pattern) ([]Difference, bool) {
	if a.NotComparable != "" || b.NotComparable != "" {
		if a.NotComparable == capture.OmittedBinary && b.NotComparable == capture.OmittedBinary && a.Size != b.Size {
			return []Difference{{Where: WhereBody, Kind: KindChanged, Pointer: "size", template: "size",
				Prod: fmt.Sprint(a.Size), Candidate: fmt.Sprint(b.Size), Detail: "binary bodies differ in size"}}, false
		}
		if (a.NotComparable == "") != (b.NotComparable == "") && !(len(a.Body) == 0 && len(b.Body) == 0) {
			// One side has a comparable body and the other not: report the
			// change of shape, never the values.
			return []Difference{{Where: WhereBody, Kind: KindChanged, Pointer: "", template: "",
				Prod: notComparable(a), Candidate: notComparable(b), Detail: "body not comparable"}}, false
		}
		return nil, false
	}
	if len(a.Body) == 0 && len(b.Body) == 0 {
		return nil, false
	}
	pa, errA := parseJSON(a.Body)
	pb, errB := parseJSON(b.Body)
	switch {
	case errA == nil && errB == nil:
		return e.cfg.diffJSON(pa, pb, ignore)
	case errA == nil || errB == nil:
		if len(a.Body) == 0 || len(b.Body) == 0 {
			kind := KindAdded
			if len(b.Body) == 0 {
				kind = KindRemoved
			}
			return []Difference{{Where: WhereBody, Kind: kind, template: ""}}, false
		}
		return []Difference{{Where: WhereBody, Kind: KindType, template: "",
			Prod: jsonOrText(errA), Candidate: jsonOrText(errB), Detail: "one body is JSON, the other is not"}}, false
	}
	for _, p := range ignore {
		if len(p.segs) == 0 {
			return nil, false // the whole body is ignored
		}
	}
	return diffText(string(a.Body), string(b.Body)), false
}

func notComparable(s Side) string {
	if s.NotComparable != "" {
		return "(" + s.NotComparable + ")"
	}
	return "(comparable)"
}

func jsonOrText(err error) string {
	if err == nil {
		return "(json)"
	}
	return "(text)"
}
