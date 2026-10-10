package report

import (
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"maps"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/Jenil133/Shadowtap/internal/diff"
)

// WriteJSON writes s as indented JSON.
func WriteJSON(w io.Writer, s *Summary) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	return enc.Encode(s)
}

// classOrder lists result classes from best to worst.
var classOrder = []string{diff.ClassMatch, diff.ClassBodyMismatch, diff.ClassHeaderMismatch, diff.ClassStatusMismatch, diff.ClassError, diff.ClassSkipped}

// WriteText writes a terminal summary.
func WriteText(w io.Writer, s *Summary) error {
	ew := &errWriter{w: w}
	ew.printf("Replay %s → %s", s.RunID, s.Candidate)
	if s.Baseline != "" {
		ew.printf(" (baseline %s)", s.Baseline)
	}
	ew.printf(", speed %s\n", s.Speed)
	ew.printf("%d captures, %d replayed:", s.Captures, s.Replayed)
	for _, c := range classOrder {
		if n := s.Classes[c]; n > 0 {
			ew.printf(" %s=%d", c, n)
		}
	}
	ew.printf("\nmismatch rate %.2f%%, error rate %.2f%%", 100*s.MismatchRate, 100*s.ErrorRate)
	if s.SuppressedBaseline+s.SuppressedNoise > 0 {
		ew.printf(" (suppressed: %d shared with baseline, %d noise)", s.SuppressedBaseline, s.SuppressedNoise)
	}
	ew.printf("\n")
	if len(s.Skipped) > 0 {
		ew.printf("skipped:")
		for _, k := range slices.Sorted(maps.Keys(s.Skipped)) {
			ew.printf(" %s=%d", k, s.Skipped[k])
		}
		ew.printf("\n")
	}

	if len(s.Routes) > 0 {
		ew.printf("\n")
		tw := tabwriter.NewWriter(ew, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ROUTE\tREPLAYED\tMISMATCH\tERRORS\tP95 PROD\tP95 BASE\tP95 CAND\t")
		for _, r := range s.Routes {
			flag := ""
			if slices.Contains(s.LatencyRegressions, r.Route) {
				flag = " slower"
			}
			fmt.Fprintf(tw, "%s\t%d\t%.1f%%\t%d\t%s\t%s\t%s%s\t\n", r.Route, r.Replayed, 100*r.MismatchRate, r.Classes[diff.ClassError],
				micros(r.Latency.Prod.P95), optMicros(r.Latency.Baseline), micros(r.Latency.Candidate.P95), flag)
		}
		tw.Flush()
	}
	if len(s.TopDifferences) > 0 {
		ew.printf("\nTop differences:\n")
		for _, d := range s.TopDifferences {
			ew.printf("  %4d× %s  %s %s %s", d.Count, d.Route, d.Where, d.Kind, orRoot(d.Pointer))
			if d.Occurrences > d.Count {
				ew.printf(" (%d occurrences)", d.Occurrences)
			}
			if d.Prod != "" || d.Baseline != "" || d.Candidate != "" {
				ew.printf("  prod=%s", orDash(d.Prod))
				if s.Baseline != "" {
					ew.printf(" baseline=%s", orDash(d.Baseline))
				}
				ew.printf(" candidate=%s", orDash(d.Candidate))
			}
			ew.printf("  e.g. %s\n", strings.Join(d.Examples, ", "))
		}
	}
	if len(s.Noise) > 0 {
		ew.printf("\nNoise suppressed (differs between two baseline responses to one request):\n")
		for _, route := range slices.Sorted(maps.Keys(s.Noise)) {
			ew.printf("  %s: %s\n", route, strings.Join(s.Noise[route], ", "))
		}
	}
	if len(s.Volatile) > 0 {
		ew.printf("\nVolatile without a baseline (value differs in more than %.0f%% of a route's comparisons; likely noise, candidates for ignore rules):\n", 100*s.NoiseThreshold)
		for _, v := range s.Volatile {
			ew.printf("  %s  %s %s  %d/%d (%.0f%%)  e.g. %s\n", v.Route, v.Where, orRoot(v.Pointer), v.Count, v.Compared, 100*v.Share, strings.Join(v.Examples, ", "))
		}
	}
	ew.printf("\nlatency p95: prod %s", micros(s.Latency.Prod.P95))
	if s.Latency.Baseline.Count > 0 {
		ew.printf(", baseline %s", micros(s.Latency.Baseline.P95))
	}
	ew.printf(", candidate %s", micros(s.Latency.Candidate.P95))
	if s.Latency.Regression || len(s.LatencyRegressions) > 0 {
		ew.printf("  REGRESSION")
	}
	sc := s.Schedule
	ew.printf("\nschedule: sent %d, dropped %d, lag p50 %s p99 %s max %s", sc.Sent, sc.Dropped, micros(sc.LagP50), micros(sc.LagP99), micros(sc.LagMax))
	if sc.LagWarning {
		ew.printf("  WARNING: p99 lag above 50ms, the arrival pattern was not reproduced faithfully")
	}
	ew.printf("\n")
	if len(sc.LagHistogram) > 0 {
		ew.printf("lag histogram (sent, by lag at most):")
		for _, b := range sc.LagHistogram {
			ew.printf(" %s=%d", b.LE, b.Count)
		}
		ew.printf("\n")
	}
	if s.Source.Undecodable > 0 || s.Source.Disordered > 0 {
		ew.printf("source: %d undecodable, %d out of order\n", s.Source.Undecodable, s.Source.Disordered)
	}
	if s.Gate.Passed {
		ew.printf("\nGATE PASSED\n")
	} else {
		ew.printf("\nGATE FAILED: %s\n", strings.Join(s.Gate.Failures, "; "))
	}
	return ew.err
}

func optMicros(p Percentiles) string {
	if p.Count == 0 {
		return "-"
	}
	return micros(p.P95)
}

func orRoot(p string) string {
	if p == "" {
		return "(whole)"
	}
	return p
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

type errWriter struct {
	w   io.Writer
	err error
}

func (e *errWriter) Write(p []byte) (int, error) {
	if e.err != nil {
		return 0, e.err
	}
	n, err := e.w.Write(p)
	e.err = err
	return n, err
}

func (e *errWriter) printf(format string, args ...any) {
	fmt.Fprintf(e, format, args...)
}

// WriteHTML writes a self-contained HTML report (no scripts, no external
// resources). All values are escaped by html/template.
func WriteHTML(w io.Writer, s *Summary) error {
	return htmlTemplate.Execute(w, struct {
		*Summary
		ClassOrder []string
	}{s, classOrder})
}

var htmlTemplate = template.Must(template.New("report").Funcs(template.FuncMap{
	"pct":     func(f float64) string { return fmt.Sprintf("%.2f%%", 100*f) },
	"micros":  micros,
	"opt":     optMicros,
	"root":    orRoot,
	"join":    strings.Join,
	"regress": func(s *Summary, route string) bool { return slices.Contains(s.LatencyRegressions, route) },
	"sorted":  func(m map[string]int) []string { return slices.Sorted(maps.Keys(m)) },
}).Parse(`<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Shadowtap replay {{.RunID}}</title>
<style>
body{font:14px/1.5 system-ui,sans-serif;margin:2rem;color:#1b1f24;background:#fff}
h1{font-size:1.4rem}h2{font-size:1.1rem;margin-top:2rem}
table{border-collapse:collapse;width:100%}
th,td{border-bottom:1px solid #d0d7de;padding:.35rem .6rem;text-align:left;vertical-align:top}
th{background:#f6f8fa}
td.num{text-align:right;font-variant-numeric:tabular-nums}
code{font:12px ui-monospace,monospace;word-break:break-all}
.pass{color:#116329;font-weight:600}.fail{color:#a40e26;font-weight:600}
.visually-hidden{position:absolute;width:1px;height:1px;overflow:hidden;clip:rect(0 0 0 0);white-space:nowrap}
</style>
</head>
<body>
<main>
<h1>Shadowtap replay <code>{{.RunID}}</code></h1>
<p>Candidate <code>{{.Candidate}}</code>{{if .Baseline}}, baseline <code>{{.Baseline}}</code>{{end}}, speed {{.Speed}}, generated {{.GeneratedAt.Format "2006-01-02 15:04:05 UTC"}}.</p>
<p role="status">{{if .Gate.Passed}}<span class="pass">Gate passed</span>{{else}}<span class="fail">Gate failed</span>: {{join .Gate.Failures "; "}}{{end}}</p>

<h2>Totals</h2>
<table>
<caption class="visually-hidden">Results by class</caption>
<thead><tr><th scope="col">Class</th><th scope="col">Count</th></tr></thead>
<tbody>
{{range $c := .ClassOrder}}{{with index $.Classes $c}}<tr><td>{{$c}}</td><td class="num">{{.}}</td></tr>{{end}}{{end}}
</tbody>
</table>
<p>{{.Captures}} captures, {{.Replayed}} replayed. Mismatch rate {{pct .MismatchRate}}, error rate {{pct .ErrorRate}}.
Suppressed differences: {{.SuppressedBaseline}} shared with the baseline, {{.SuppressedNoise}} noise.</p>
{{if .Skipped}}<p>Skipped: {{range $k := sorted .Skipped}}{{$k}} {{index $.Skipped $k}}; {{end}}</p>{{end}}

<h2>Routes</h2>
<table>
<thead><tr><th scope="col">Route</th><th scope="col">Replayed</th><th scope="col">Mismatch rate</th><th scope="col">p95 prod</th><th scope="col">p95 baseline</th><th scope="col">p95 candidate</th></tr></thead>
<tbody>
{{range .Routes}}<tr><td><code>{{.Route}}</code></td><td class="num">{{.Replayed}}</td><td class="num">{{pct .MismatchRate}}</td><td class="num">{{micros .Latency.Prod.P95}}</td><td class="num">{{opt .Latency.Baseline}}</td><td class="num">{{micros .Latency.Candidate.P95}}{{if regress $.Summary .Route}} <span class="fail">slower</span>{{end}}</td></tr>
{{end}}</tbody>
</table>

{{if .TopDifferences}}<h2>Top differences</h2>
<p>Count is the number of exchanges with the difference; occurrences, when higher, count repeats within one exchange (one field of every array element).</p>
<table>
<thead><tr><th scope="col">Exchanges</th><th scope="col">Occurrences</th><th scope="col">Route</th><th scope="col">Where</th><th scope="col">Pointer</th><th scope="col">Kind</th><th scope="col">Production</th>{{if .Baseline}}<th scope="col">Baseline</th>{{end}}<th scope="col">Candidate</th><th scope="col">Examples</th></tr></thead>
<tbody>
{{range .TopDifferences}}<tr><td class="num">{{.Count}}</td><td class="num">{{.Occurrences}}</td><td><code>{{.Route}}</code></td><td>{{.Where}}</td><td><code>{{root .Pointer}}</code></td><td>{{.Kind}}</td><td><code>{{.Prod}}</code></td>{{if $.Baseline}}<td><code>{{.Baseline}}</code></td>{{end}}<td><code>{{.Candidate}}</code></td><td><code>{{join .Examples ", "}}</code></td></tr>
{{end}}</tbody>
</table>{{end}}

{{if .Noise}}<h2>Noise</h2>
<p>Places where two baseline responses to the same request differ in more than the noise threshold of a route's probes; candidate differences there are suppressed.</p>
<ul>{{range $route, $keys := .Noise}}<li><code>{{$route}}</code>: <code>{{join $keys ", "}}</code></li>{{end}}</ul>{{end}}

{{if .Volatile}}<h2>Volatile places</h2>
<p>This run had no baseline. Places whose value differs in more than {{pct .NoiseThreshold}} of a route's comparisons are likely noise (timestamps, generated ids) rather than regressions; each pointer can be used as an ignore rule.</p>
<table>
<thead><tr><th scope="col">Route</th><th scope="col">Where</th><th scope="col">Pointer</th><th scope="col">Share</th><th scope="col">Examples</th></tr></thead>
<tbody>
{{range .Volatile}}<tr><td><code>{{.Route}}</code></td><td>{{.Where}}</td><td><code>{{root .Pointer}}</code></td><td class="num">{{.Count}}/{{.Compared}} ({{pct .Share}})</td><td><code>{{join .Examples ", "}}</code></td></tr>
{{end}}</tbody>
</table>{{end}}

<h2>Latency and schedule</h2>
<p>p95 time to response headers: production {{micros .Latency.Prod.P95}}, {{if .Latency.Baseline.Count}}baseline {{micros .Latency.Baseline.P95}}, {{end}}candidate {{micros .Latency.Candidate.P95}}{{if .Latency.Regression}} <span class="fail">regression</span>{{end}}.</p>
<p>Sent {{.Schedule.Sent}}, dropped {{.Schedule.Dropped}}; schedule lag p50 {{micros .Schedule.LagP50}}, p99 {{micros .Schedule.LagP99}}, max {{micros .Schedule.LagMax}}{{if .Schedule.LagWarning}} <span class="fail">(above 50 ms: arrival pattern not reproduced faithfully)</span>{{end}}.</p>
{{if .Schedule.LagHistogram}}<table>
<caption class="visually-hidden">Schedule lag histogram</caption>
<thead><tr><th scope="col">Lag at most</th><th scope="col">Sent</th></tr></thead>
<tbody>
{{range .Schedule.LagHistogram}}<tr><td>{{.LE}}</td><td class="num">{{.Count}}</td></tr>
{{end}}</tbody>
</table>{{end}}
</main>
</body>
</html>
`))
