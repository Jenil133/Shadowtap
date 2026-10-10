package report

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/replay/runner"
	"github.com/Jenil133/Shadowtap/internal/replay/schedule"
)

func result(i int, route, class string, prodUS, candUS, baseUS int64, diffs ...diff.Difference) *diff.Result {
	return &diff.Result{
		RunID:       "run-1",
		CaptureID:   fmt.Sprintf("cap-%d", i),
		Route:       route,
		Class:       class,
		Differences: diffs,
		Status:      diff.Status{Prod: 200, Candidate: 200, Baseline: 200},
		Latency:     diff.Latency{ProdMicros: prodUS, CandidateMicros: candUS, BaselineMicros: baseUS},
		StartedAt:   time.Date(2026, 10, 3, 12, 0, i, 0, time.UTC),
	}
}

func outcome() *runner.Outcome {
	phone := diff.Difference{Where: diff.WhereBody, Pointer: "/phone", Kind: diff.KindRemoved, Prod: `"tok_x"`, Baseline: `"tok_b"`}
	noise := diff.Difference{Where: diff.WhereBody, Pointer: "/generated_at", Kind: diff.KindChanged, Suppressed: diff.SuppressedNoise}
	var rs []*diff.Result
	for i := range 10 {
		// Two differences at one place within an exchange count once.
		again := phone
		again.Pointer = "/phone"
		rs = append(rs, result(i, "GET /api/users/{id}", diff.ClassBodyMismatch, 1000, 1100, 1000, phone, again, noise))
	}
	for i := 10; i < 20; i++ {
		rs = append(rs, result(i, "GET /api/orders/{id}", diff.ClassMatch, 1000, 60_000, 1200, noise))
	}
	rs = append(rs, result(20, "GET /api/orders/{id}", diff.ClassError, 0, 0, 0))
	skipped := result(21, "POST /api/users", diff.ClassSkipped, 0, 0, 0)
	skipped.Reason = "mutating_method"
	rs = append(rs, skipped)
	return &runner.Outcome{
		RunID:       "run-1",
		Results:     rs,
		HasBaseline: true,
		Noise:       map[string][]string{"GET /api/users/{id}": {"body:/generated_at"}},
		Schedule:    schedule.Stats{Sent: 21, Lag: schedule.LagStats{P99: 80 * time.Millisecond}},
	}
}

func TestBuild(t *testing.T) {
	s := Build(outcome(), Options{Candidate: "http://cand", Baseline: "http://base", Speed: 4,
		Gate: Gate{MaxMismatchRate: 0.01, MaxErrorRate: 0.1, FailOnLatency: true}})
	if s.Captures != 22 || s.Replayed != 21 || s.Skipped["mutating_method"] != 1 {
		t.Errorf("counts: %+v", s)
	}
	if s.MismatchRate != 0.5 || s.ErrorRate != 1.0/21 {
		t.Errorf("rates %v %v", s.MismatchRate, s.ErrorRate)
	}
	if len(s.TopDifferences) != 1 || len(s.TopDifferences[0].Examples) != maxExamples {
		t.Fatalf("top differences %+v", s.TopDifferences)
	}
	if top := s.TopDifferences[0]; top.Count != 10 || top.Occurrences != 20 || top.Baseline != `"tok_b"` {
		t.Errorf("top difference %+v, want 10 exchanges, 20 occurrences and the baseline value", top)
	}
	if s.Volatile != nil || s.NoiseThreshold != diff.DefaultNoiseThreshold {
		t.Errorf("with a baseline: volatile %+v, threshold %v", s.Volatile, s.NoiseThreshold)
	}
	if s.SuppressedNoise != 20 {
		t.Errorf("suppressed noise %d", s.SuppressedNoise)
	}
	if len(s.LatencyRegressions) != 1 || s.LatencyRegressions[0] != "GET /api/orders/{id}" {
		t.Errorf("latency regressions %v", s.LatencyRegressions)
	}
	if !s.Schedule.LagWarning || s.Speed != "4x" {
		t.Errorf("schedule %+v speed %s", s.Schedule, s.Speed)
	}
	if s.Gate.Passed || len(s.Gate.Failures) != 3 {
		t.Errorf("gate %+v", s.Gate)
	}

	pass := Build(outcome(), Options{Gate: Gate{MaxMismatchRate: -1, MaxErrorRate: -1}})
	if !pass.Gate.Passed {
		t.Errorf("disabled gate failed: %+v", pass.Gate)
	}
	empty := Build(&runner.Outcome{RunID: "e"}, Options{Gate: Gate{MaxMismatchRate: 0, MinReplayed: 1}})
	if empty.Gate.Passed {
		t.Error("an empty replay passed a gate needing one capture")
	}
}

func TestRender(t *testing.T) {
	s := Build(outcome(), Options{Candidate: "http://cand", Baseline: "http://base<script>", Gate: Gate{MaxMismatchRate: 0.01}})
	var js bytes.Buffer
	if err := WriteJSON(&js, s); err != nil {
		t.Fatal(err)
	}
	var back Summary
	if err := json.Unmarshal(js.Bytes(), &back); err != nil || back.Replayed != s.Replayed {
		t.Errorf("JSON round trip: %v", err)
	}
	var txt bytes.Buffer
	if err := WriteText(&txt, s); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"GET /api/users/{id}", "body removed /phone", "(20 occurrences)", `baseline="tok_b"`, "GATE FAILED", "Noise suppressed", "WARNING"} {
		if !strings.Contains(txt.String(), want) {
			t.Errorf("text report lacks %q:\n%s", want, txt.String())
		}
	}
	var page bytes.Buffer
	if err := WriteHTML(&page, s); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(page.String(), "<script>") || !strings.Contains(page.String(), "&lt;script&gt;") {
		t.Error("HTML report does not escape values")
	}
	if !strings.Contains(page.String(), "Gate failed") || !strings.Contains(page.String(), `lang="en"`) {
		t.Error("HTML report incomplete")
	}
}

func TestVolatileWithoutBaseline(t *testing.T) {
	generated := diff.Difference{Where: diff.WhereBody, Pointer: "/generated_at", Kind: diff.KindChanged, Prod: `"1"`, Candidate: `"2"`}
	phone := diff.Difference{Where: diff.WhereBody, Pointer: "/phone", Kind: diff.KindRemoved, Prod: `"tok_x"`}
	count := diff.Difference{Where: diff.WhereBody, Pointer: "/count", Kind: diff.KindChanged, Prod: "1", Candidate: "2"}
	status := diff.Difference{Where: diff.WhereStatus, Kind: diff.KindChanged, Prod: "200", Candidate: "500"}
	var rs []*diff.Result
	for i := range 10 {
		ds := []diff.Difference{generated, phone, status}
		if i == 0 {
			ds = append(ds, count)
		}
		rs = append(rs, result(i, "GET /api/users", diff.ClassStatusMismatch, 1000, 1000, 0, ds...))
	}
	rs = append(rs, result(10, "GET /api/users", diff.ClassMatch, 1000, 1000, 0))
	out := &runner.Outcome{RunID: "run-1", Results: rs}

	s := Build(out, Options{Candidate: "http://cand"})
	// Only generated_at is a value change in more than the threshold share
	// of the route's 11 comparisons: phone is removed (a change of shape),
	// count differs once, and status changes are never noise.
	if len(s.Volatile) != 1 {
		t.Fatalf("volatile = %+v", s.Volatile)
	}
	if v := s.Volatile[0]; v.Route != "GET /api/users" || v.Pointer != "/generated_at" || v.Count != 10 || v.Compared != 11 || v.Share != 10.0/11 || len(v.Examples) != maxExamples {
		t.Errorf("volatile = %+v", v)
	}
	if s := Build(out, Options{NoiseThreshold: 0.95}); len(s.Volatile) != 0 || s.NoiseThreshold != 0.95 {
		t.Errorf("above the threshold: %+v", s.Volatile)
	}
	var txt bytes.Buffer
	if err := WriteText(&txt, s); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(txt.String(), "Volatile without a baseline") || !strings.Contains(txt.String(), "/generated_at  10/11 (91%)") {
		t.Errorf("text report lacks the volatile list:\n%s", txt.String())
	}
	var page bytes.Buffer
	if err := WriteHTML(&page, s); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(page.String(), "Volatile places") || strings.Contains(page.String(), "<th scope=\"col\">Baseline</th>") {
		t.Error("HTML report: volatile section missing or baseline column shown without a baseline")
	}

	out.HasBaseline = true
	if s := Build(out, Options{}); s.Volatile != nil {
		t.Errorf("with a baseline the volatile list must be empty: %+v", s.Volatile)
	}
}

func TestScheduleHistogram(t *testing.T) {
	bounds := schedule.LagBuckets()
	hist := make([]int, len(bounds)+1)
	hist[0], hist[4], hist[len(hist)-1] = 7, 2, 1
	out := &runner.Outcome{RunID: "run-1", Schedule: schedule.Stats{Sent: 10, Lag: schedule.LagStats{Histogram: hist}}}
	s := Build(out, Options{})
	if len(s.Schedule.LagHistogram) != len(hist) {
		t.Fatalf("histogram = %+v", s.Schedule.LagHistogram)
	}
	if b := s.Schedule.LagHistogram[0]; b.LE != "1ms" || b.Count != 7 {
		t.Errorf("first bucket %+v", b)
	}
	if b := s.Schedule.LagHistogram[4]; b.LE != "50ms" || b.Count != 2 {
		t.Errorf("warn-threshold bucket %+v", b)
	}
	if b := s.Schedule.LagHistogram[len(hist)-1]; b.LE != "+Inf" || b.Count != 1 {
		t.Errorf("open-ended bucket %+v", b)
	}
	var txt bytes.Buffer
	if err := WriteText(&txt, s); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(txt.String(), "lag histogram") || !strings.Contains(txt.String(), "+Inf=1") {
		t.Errorf("text report lacks the histogram:\n%s", txt.String())
	}
	var page bytes.Buffer
	if err := WriteHTML(&page, s); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(page.String(), "Schedule lag histogram") {
		t.Error("HTML report lacks the histogram")
	}
	// Without sent captures there is no histogram to show.
	if s := Build(&runner.Outcome{RunID: "e"}, Options{}); s.Schedule.LagHistogram != nil {
		t.Errorf("empty run has a histogram: %+v", s.Schedule.LagHistogram)
	}
}

func TestNDJSON(t *testing.T) {
	var buf bytes.Buffer
	o := outcome()
	if err := WriteNDJSON(&buf, o.Results); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != len(o.Results) {
		t.Fatalf("%d lines", len(lines))
	}
	var r diff.Result
	if err := json.Unmarshal([]byte(lines[0]), &r); err != nil || r.CaptureID != "cap-0" {
		t.Errorf("line 0: %v %+v", err, r)
	}
	if h := RecordHeaders(o.Results[0]); h["class"] != diff.ClassBodyMismatch || h["run-id"] != "run-1" {
		t.Errorf("headers %v", h)
	}
}
