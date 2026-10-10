// Package report summarizes a replay: totals per class, mismatch rates per
// route, the places that differ most often, latency of production, baseline
// and candidate, and schedule lag. It renders the summary as JSON, terminal
// text or a static HTML page, and evaluates the CI gate.
package report

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/replay/runner"
	"github.com/Jenil133/Shadowtap/internal/replay/schedule"
	"github.com/Jenil133/Shadowtap/internal/replay/source"
)

// Latency regression rule: the candidate's p95 is more than
// LatencyRegressionFactor times the reference p95 and at least
// MinLatencyRegression slower. The reference is the baseline when there is
// one (measured the same way), production otherwise.
const (
	LatencyRegressionFactor = 1.2
	MinLatencyRegression    = 5 * time.Millisecond
)

// maxTopDifferences and maxExamples bound the report.
const (
	maxTopDifferences = 25
	maxExamples       = 3
	// MinLatencySamples is how many comparisons a route needs before its
	// latency can be flagged.
	MinLatencySamples = 5
)

// Summary is a replay report.
type Summary struct {
	RunID       string    `json:"run_id"`
	GeneratedAt time.Time `json:"generated_at"`
	Candidate   string    `json:"candidate"`
	Baseline    string    `json:"baseline,omitempty"`
	Speed       string    `json:"speed"`

	Captures int            `json:"captures"` // read from the source
	Replayed int            `json:"replayed"` // sent (match + mismatches + errors)
	Classes  map[string]int `json:"classes"`
	Skipped  map[string]int `json:"skipped,omitempty"` // by reason

	MismatchRate float64 `json:"mismatch_rate"` // mismatches / (matches + mismatches)
	ErrorRate    float64 `json:"error_rate"`    // errors / replayed

	SuppressedBaseline int `json:"suppressed_baseline"`
	SuppressedNoise    int `json:"suppressed_noise"`

	Routes         []RouteSummary      `json:"routes"`
	TopDifferences []DifferenceSummary `json:"top_differences,omitempty"`
	// Noise lists per route the places learned as noise from baseline probes.
	Noise map[string][]string `json:"noise,omitempty"`
	// Volatile lists, in a run without a baseline, the places whose values
	// differ in more than NoiseThreshold of a route's comparisons: likely
	// noise, and candidates for ignore rules.
	Volatile       []VolatileSummary `json:"volatile,omitempty"`
	NoiseThreshold float64           `json:"noise_threshold"`

	Latency LatencySummary `json:"latency"`
	// LatencyRegressions lists routes whose candidate p95 regressed (routes
	// with at least MinLatencySamples comparisons).
	LatencyRegressions []string `json:"latency_regressions,omitempty"`

	Schedule ScheduleSummary `json:"schedule"`
	Source   source.Stats    `json:"source"`

	Gate GateResult `json:"gate"`
}

// RouteSummary is one route's share of the replay.
type RouteSummary struct {
	Route        string         `json:"route"`
	Replayed     int            `json:"replayed"`
	Classes      map[string]int `json:"classes"`
	MismatchRate float64        `json:"mismatch_rate"`
	Latency      LatencySummary `json:"latency"`
}

// DifferenceSummary is a place that differs in many exchanges of a route.
type DifferenceSummary struct {
	Route   string `json:"route"`
	Where   string `json:"where"`
	Pointer string `json:"pointer"` // array indexes as *
	Kind    string `json:"kind"`
	// Count is the number of exchanges with this difference. Occurrences
	// counts the differences themselves: more than Count when the place
	// repeats within one exchange, as one field of every array element does.
	Count       int      `json:"count"`
	Occurrences int      `json:"occurrences"`
	Prod        string   `json:"prod,omitempty"` // first example, scrubbed
	Baseline    string   `json:"baseline,omitempty"`
	Candidate   string   `json:"candidate,omitempty"`
	Examples    []string `json:"examples"` // capture ids
}

// VolatileSummary is a place whose value differs in a large share of a
// route's comparisons. Without a baseline the report cannot tell such noise
// (timestamps, generated ids) from a regression, so it lists these places as
// suggestions; Pointer is usable as an ignore rule.
type VolatileSummary struct {
	Route    string   `json:"route"`
	Where    string   `json:"where"`
	Pointer  string   `json:"pointer"`  // array indexes as *
	Count    int      `json:"count"`    // exchanges of the route with the difference
	Compared int      `json:"compared"` // exchanges of the route that were compared
	Share    float64  `json:"share"`
	Examples []string `json:"examples"` // capture ids
}

// diffKey identifies a place across the exchanges of a route.
type diffKey struct{ route, where, ptr, kind string }

// Percentiles of a latency sample, in microseconds.
type Percentiles struct {
	Count int   `json:"count"`
	P50   int64 `json:"p50_us"`
	P95   int64 `json:"p95_us"`
	P99   int64 `json:"p99_us"`
}

// LatencySummary compares the sides' time to response headers.
type LatencySummary struct {
	Prod       Percentiles `json:"prod"`
	Candidate  Percentiles `json:"candidate"`
	Baseline   Percentiles `json:"baseline,omitzero"`
	Regression bool        `json:"regression"`
}

// ScheduleSummary describes how faithfully the arrival pattern was
// reproduced.
type ScheduleSummary struct {
	Sent        int    `json:"sent"`
	Dropped     int    `json:"dropped"`
	LagP50      int64  `json:"lag_p50_us"`
	LagP95      int64  `json:"lag_p95_us"`
	LagP99      int64  `json:"lag_p99_us"`
	LagMax      int64  `json:"lag_max_us"`
	LagWarning  bool   `json:"lag_warning"`
	SpanMicros  int64  `json:"original_span_us"`
	TookMicros  int64  `json:"took_us"`
	Description string `json:"description,omitempty"`
	// LagHistogram counts the sent captures by schedule lag: one bucket per
	// bound of schedule.LagBuckets plus an open-ended last bucket.
	LagHistogram []LagBucket `json:"lag_histogram,omitempty"`
}

// LagBucket is one bucket of the schedule lag histogram: Count captures were
// handed over later than the previous bucket's bound and at most LE late. LE
// is a duration such as "5ms", or "+Inf" for the open-ended last bucket.
type LagBucket struct {
	LE    string `json:"le"`
	Count int    `json:"count"`
}

// Options carries what the outcome does not know.
type Options struct {
	Candidate, Baseline string
	// Speed is the speed factor, 0 for max.
	Speed float64
	// NoiseThreshold is the share of a route's comparisons above which a
	// place counts as volatile; 0 means diff.DefaultNoiseThreshold.
	NoiseThreshold float64
	Gate           Gate
	Now            func() time.Time
}

// Build summarizes a replay outcome and evaluates the gate.
func Build(out *runner.Outcome, opts Options) *Summary {
	now := time.Now
	if opts.Now != nil {
		now = opts.Now
	}
	s := &Summary{
		RunID:       out.RunID,
		GeneratedAt: now().UTC(),
		Candidate:   opts.Candidate,
		Baseline:    opts.Baseline,
		Speed:       speedName(opts.Speed),
		Captures:    len(out.Results),
		Classes:     map[string]int{},
		Skipped:     map[string]int{},
		Noise:       out.Noise,
		Source:      out.Source,
		Schedule:    scheduleSummary(out.Schedule),
	}
	threshold := opts.NoiseThreshold
	if threshold <= 0 {
		threshold = diff.DefaultNoiseThreshold
	}
	s.NoiseThreshold = threshold
	type routeAcc struct {
		classes             map[string]int
		prod, cand, baseLat []int64
	}
	routes := map[string]*routeAcc{}
	diffs := map[diffKey]*DifferenceSummary{}
	var prod, cand, baseLat []int64

	for _, r := range out.Results {
		s.Classes[r.Class]++
		if r.Class == diff.ClassSkipped {
			s.Skipped[r.Reason]++
			continue
		}
		s.Replayed++
		ra := routes[r.Route]
		if ra == nil {
			ra = &routeAcc{classes: map[string]int{}}
			routes[r.Route] = ra
		}
		ra.classes[r.Class]++
		if r.Class == diff.ClassError {
			continue
		}
		prod, cand = append(prod, r.Latency.ProdMicros), append(cand, r.Latency.CandidateMicros)
		ra.prod, ra.cand = append(ra.prod, r.Latency.ProdMicros), append(ra.cand, r.Latency.CandidateMicros)
		if out.HasBaseline && r.Status.Baseline != 0 {
			baseLat = append(baseLat, r.Latency.BaselineMicros)
			ra.baseLat = append(ra.baseLat, r.Latency.BaselineMicros)
		}
		seen := map[diffKey]bool{} // places already counted for this exchange
		for _, d := range r.Differences {
			switch d.Suppressed {
			case diff.SuppressedBaseline:
				s.SuppressedBaseline++
				continue
			case diff.SuppressedNoise:
				s.SuppressedNoise++
				continue
			}
			k := diffKey{r.Route, d.Where, templateOf(d), d.Kind}
			ds := diffs[k]
			if ds == nil {
				ds = &DifferenceSummary{Route: r.Route, Where: d.Where, Pointer: k.ptr, Kind: d.Kind, Prod: d.Prod, Baseline: d.Baseline, Candidate: d.Candidate}
				diffs[k] = ds
			}
			ds.Occurrences++
			if seen[k] {
				continue
			}
			seen[k] = true
			ds.Count++
			if len(ds.Examples) < maxExamples {
				ds.Examples = append(ds.Examples, r.CaptureID)
			}
		}
	}

	mismatches := s.Classes[diff.ClassStatusMismatch] + s.Classes[diff.ClassHeaderMismatch] + s.Classes[diff.ClassBodyMismatch]
	if compared := mismatches + s.Classes[diff.ClassMatch]; compared > 0 {
		s.MismatchRate = float64(mismatches) / float64(compared)
	}
	if s.Replayed > 0 {
		s.ErrorRate = float64(s.Classes[diff.ClassError]) / float64(s.Replayed)
	}
	s.Latency = latencySummary(prod, cand, baseLat, out.HasBaseline)

	compared := map[string]int{} // route -> exchanges that were compared
	for _, route := range slices.Sorted(maps.Keys(routes)) {
		ra := routes[route]
		rs := RouteSummary{Route: route, Classes: ra.classes, Latency: latencySummary(ra.prod, ra.cand, ra.baseLat, out.HasBaseline)}
		for _, n := range ra.classes {
			rs.Replayed += n
		}
		mm := ra.classes[diff.ClassStatusMismatch] + ra.classes[diff.ClassHeaderMismatch] + ra.classes[diff.ClassBodyMismatch]
		if c := mm + ra.classes[diff.ClassMatch]; c > 0 {
			rs.MismatchRate = float64(mm) / float64(c)
			compared[route] = c
		}
		if rs.Latency.Regression && rs.Latency.Candidate.Count >= MinLatencySamples {
			s.LatencyRegressions = append(s.LatencyRegressions, route)
		}
		s.Routes = append(s.Routes, rs)
	}
	top := slices.Collect(maps.Values(diffs))
	slices.SortFunc(top, func(a, b *DifferenceSummary) int {
		return cmp.Or(cmp.Compare(b.Count, a.Count), cmp.Compare(b.Occurrences, a.Occurrences), strings.Compare(a.Route, b.Route),
			strings.Compare(a.Where, b.Where), strings.Compare(a.Pointer, b.Pointer), strings.Compare(a.Kind, b.Kind))
	})
	for _, d := range top[:min(len(top), maxTopDifferences)] {
		s.TopDifferences = append(s.TopDifferences, *d)
	}
	if !out.HasBaseline {
		// With a baseline, noise is learned from probes and suppressed;
		// without one, the operator gets the most volatile places as
		// suggestions.
		s.Volatile = volatile(diffs, compared, threshold)
	}
	if len(s.Skipped) == 0 {
		s.Skipped = nil
	}
	s.Gate = opts.Gate.Evaluate(s)
	return s
}

// templateOf returns the pointer of d with array indexes collapsed, so one
// field of every array element counts as one place.
func templateOf(d diff.Difference) string {
	k := d.Key()
	_, t, _ := strings.Cut(k, ":")
	return t
}

// volatile returns the places whose values differ in more than threshold of
// their route's comparisons, most volatile first. Only value changes count:
// a place that is added, removed or changes type is a change of shape, which
// noise does not produce.
func volatile(diffs map[diffKey]*DifferenceSummary, compared map[string]int, threshold float64) []VolatileSummary {
	var out []VolatileSummary
	for k, d := range diffs {
		n := compared[k.route]
		if d.Kind != diff.KindChanged || k.where == diff.WhereStatus || n == 0 {
			continue
		}
		share := float64(d.Count) / float64(n)
		if share <= threshold {
			continue
		}
		out = append(out, VolatileSummary{Route: k.route, Where: k.where, Pointer: k.ptr, Count: d.Count, Compared: n, Share: share, Examples: d.Examples})
	}
	slices.SortFunc(out, func(a, b VolatileSummary) int {
		return cmp.Or(cmp.Compare(b.Share, a.Share), cmp.Compare(b.Count, a.Count), strings.Compare(a.Route, b.Route),
			strings.Compare(a.Where, b.Where), strings.Compare(a.Pointer, b.Pointer))
	})
	if len(out) > maxTopDifferences {
		out = out[:maxTopDifferences]
	}
	return out
}

func speedName(speed float64) string {
	if speed == 0 {
		return "max"
	}
	return fmt.Sprintf("%gx", speed)
}

func scheduleSummary(st schedule.Stats) ScheduleSummary {
	s := ScheduleSummary{
		Sent:       st.Sent,
		Dropped:    st.Dropped,
		LagP50:     st.Lag.P50.Microseconds(),
		LagP95:     st.Lag.P95.Microseconds(),
		LagP99:     st.Lag.P99.Microseconds(),
		LagMax:     st.Lag.Max.Microseconds(),
		LagWarning: st.Lag.P99 > schedule.LagWarnThreshold,
		SpanMicros: st.Span.Microseconds(),
		TookMicros: st.Finished.Sub(st.Started).Microseconds(),
	}
	if bounds := schedule.LagBuckets(); len(st.Lag.Histogram) == len(bounds)+1 {
		for i, n := range st.Lag.Histogram {
			le := "+Inf"
			if i < len(bounds) {
				le = bounds[i].String()
			}
			s.LagHistogram = append(s.LagHistogram, LagBucket{LE: le, Count: n})
		}
	}
	return s
}

func latencySummary(prod, cand, base []int64, hasBaseline bool) LatencySummary {
	l := LatencySummary{Prod: percentiles(prod), Candidate: percentiles(cand)}
	ref := l.Prod
	if hasBaseline && len(base) > 0 {
		l.Baseline = percentiles(base)
		ref = l.Baseline
	}
	if l.Candidate.Count > 0 && ref.Count > 0 {
		delta := time.Duration(l.Candidate.P95-ref.P95) * time.Microsecond
		l.Regression = float64(l.Candidate.P95) > float64(ref.P95)*LatencyRegressionFactor && delta >= MinLatencyRegression
	}
	return l
}

func percentiles(v []int64) Percentiles {
	if len(v) == 0 {
		return Percentiles{}
	}
	s := slices.Sorted(slices.Values(v))
	pct := func(p float64) int64 {
		i := int(p*float64(len(s))+0.999999) - 1
		return s[min(max(i, 0), len(s)-1)]
	}
	return Percentiles{Count: len(s), P50: pct(0.5), P95: pct(0.95), P99: pct(0.99)}
}

// Gate is the CI pass/fail rule of a replay.
type Gate struct {
	// MaxMismatchRate fails the run above this mismatch rate; negative
	// disables the check.
	MaxMismatchRate float64
	// MaxErrorRate fails the run above this error rate; negative disables
	// the check.
	MaxErrorRate float64
	// FailOnLatency fails the run on a latency regression.
	FailOnLatency bool
	// MinReplayed fails a run that replayed fewer captures (an empty window
	// passes nothing by accident).
	MinReplayed int
}

// GateResult is the gate's verdict.
type GateResult struct {
	Passed   bool     `json:"passed"`
	Failures []string `json:"failures,omitempty"`
}

// Evaluate applies the gate to s.
func (g Gate) Evaluate(s *Summary) GateResult {
	var fails []string
	if s.Replayed < g.MinReplayed {
		fails = append(fails, fmt.Sprintf("replayed %d captures, need at least %d", s.Replayed, g.MinReplayed))
	}
	if g.MaxMismatchRate >= 0 && s.MismatchRate > g.MaxMismatchRate {
		fails = append(fails, fmt.Sprintf("mismatch rate %.2f%% above %.2f%%", 100*s.MismatchRate, 100*g.MaxMismatchRate))
	}
	if g.MaxErrorRate >= 0 && s.ErrorRate > g.MaxErrorRate {
		fails = append(fails, fmt.Sprintf("error rate %.2f%% above %.2f%%", 100*s.ErrorRate, 100*g.MaxErrorRate))
	}
	if g.FailOnLatency && s.Latency.Regression {
		fails = append(fails, fmt.Sprintf("latency regression: candidate p95 %s", micros(s.Latency.Candidate.P95)))
	}
	if g.FailOnLatency && len(s.LatencyRegressions) > 0 {
		fails = append(fails, fmt.Sprintf("latency regression on %s", strings.Join(s.LatencyRegressions, ", ")))
	}
	return GateResult{Passed: len(fails) == 0, Failures: fails}
}

func micros(us int64) string {
	return (time.Duration(us) * time.Microsecond).Round(10 * time.Microsecond).String()
}
