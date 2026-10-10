package integration

import (
	"context"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/replay/report"
	"github.com/Jenil133/Shadowtap/internal/replay/request"
	"github.com/Jenil133/Shadowtap/internal/replay/runner"
	"github.com/Jenil133/Shadowtap/internal/replay/schedule"
	"github.com/Jenil133/Shadowtap/internal/replay/source"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/scrub"
)

// demoTarget serves a fresh demo API of the given version and returns its
// validated base URL.
func demoTarget(t *testing.T, version string) *url.URL {
	t.Helper()
	a, err := api.New(api.Options{Version: version})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	u, err := request.ValidateTarget(srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// replay sends the captured window to candidate, with a baseline when one is
// named (fresh deployments each), and summarizes the run.
func replay(t *testing.T, kcfg kafka.Config, candidate, baseline string) (*runner.Outcome, *report.Summary) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	src, err := source.NewKafka(ctx, kcfg, source.KafkaOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	scrubber, err := scrub.New(scrub.Config{Key: []byte(scrubKey)})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := diff.New(diff.Config{}, scrubber, diff.Options{})
	if err != nil {
		t.Fatal(err)
	}
	cfg := runner.Config{
		RunID:     "run-" + candidate,
		Candidate: demoTarget(t, candidate),
		Engine:    engine,
		// Every captured method is replayed, in order and one capture at a
		// time, so baseline and candidate build up the same state.
		Filter:   source.Filter{AllowMutating: true},
		Schedule: schedule.Options{Speed: 0, Concurrency: 1},
	}
	if baseline != "" {
		cfg.Baseline = demoTarget(t, baseline)
	}
	out, err := runner.Run(ctx, cfg, src)
	if err != nil {
		t.Fatal(err)
	}
	sum := report.Build(out, report.Options{Candidate: candidate, Baseline: baseline,
		Gate: report.Gate{MaxMismatchRate: 0, MaxErrorRate: 0, FailOnLatency: true, MinReplayed: 1}})
	return out, sum
}

// difference finds one place among the report's top differences.
func difference(s *report.Summary, route, where, pointer, kind string) (report.DifferenceSummary, bool) {
	i := slices.IndexFunc(s.TopDifferences, func(d report.DifferenceSummary) bool {
		return d.Route == route && d.Where == where && d.Pointer == pointer && d.Kind == kind
	})
	if i < 0 {
		return report.DifferenceSummary{}, false
	}
	return s.TopDifferences[i], true
}

func hasDifference(s *report.Summary, route, where, pointer, kind string) bool {
	_, ok := difference(s, route, where, pointer, kind)
	return ok
}

// route returns the report's summary of one route template.
func route(t *testing.T, s *report.Summary, name string) report.RouteSummary {
	t.Helper()
	i := slices.IndexFunc(s.Routes, func(r report.RouteSummary) bool { return r.Route == name })
	if i < 0 {
		t.Fatalf("route %s missing from the report: %+v", name, s.Routes)
	}
	return s.Routes[i]
}

func textReport(s *report.Summary) string {
	var b strings.Builder
	report.WriteText(&b, s)
	return b.String()
}

// Demo API routes the generated traffic reaches, and which of them VersionV2
// changes.
var (
	allRoutes = []string{"GET /api/orders/{id}", "GET /api/users", "GET /api/users/lookup/{id}", "GET /api/users/{id}",
		"POST /api/login", "POST /api/orders", "POST /api/users"}
	// v2 renames phone in user responses (visible in list responses, which
	// carry the seeded users), answers invalid orders with 400 and is 40 ms
	// slower on /api/orders.
	unchangedRoutes = []string{"GET /api/users/lookup/{id}", "GET /api/users/{id}", "POST /api/login", "POST /api/users", "GET /api/orders/{id}"}
)

func TestReplayFindsRegressionsWithoutFalsePositives(t *testing.T) {
	const n = 300
	p := newPipeline(t, sampling.Config{DefaultRate: 1}, scrubKey)
	drive(t, p, n, 11)
	if got := len(p.collect(t, n, 3*time.Second)); got != n {
		t.Fatalf("captured %d of %d exchanges", got, n)
	}

	t.Run("v1 against v1", func(t *testing.T) {
		out, s := replay(t, p.kafka, api.VersionV1, api.VersionV1)
		if s.Captures != n || s.Replayed != n {
			t.Fatalf("captures %d replayed %d, want %d (skipped %v)", s.Captures, s.Replayed, n, s.Skipped)
		}
		if s.MismatchRate != 0 || s.ErrorRate != 0 || !s.Gate.Passed {
			t.Fatalf("v1 vs v1 reports mismatches:\n%s", textReport(s))
		}
		for _, name := range allRoutes {
			if r := route(t, s, name); r.Classes[diff.ClassMatch] != r.Replayed {
				t.Errorf("%s: classes %v, want every exchange to match", name, r.Classes)
			}
		}
		// generated_at differs on every list response: the baseline exposes
		// it as noise instead of a regression.
		if !slices.Contains(out.Noise["GET /api/users"], "body:/generated_at") {
			t.Errorf("generated_at not learned as noise: %v", out.Noise)
		}
		if s.SuppressedNoise == 0 || s.SuppressedBaseline == 0 {
			t.Errorf("suppressed: noise %d baseline %d", s.SuppressedNoise, s.SuppressedBaseline)
		}
		if s.Volatile != nil {
			t.Errorf("volatile suggestions in a run with a baseline: %+v", s.Volatile)
		}
	})

	t.Run("v1 against v1 without baseline", func(t *testing.T) {
		out, s := replay(t, p.kafka, api.VersionV1, "")
		if len(out.Noise) != 0 || s.SuppressedNoise != 0 || s.SuppressedBaseline != 0 {
			t.Errorf("suppression without a baseline: noise %v, suppressed %d/%d", out.Noise, s.SuppressedNoise, s.SuppressedBaseline)
		}
		// Static rules only: generated_at is reported on every list
		// response, and the report flags it as volatile.
		users := route(t, s, "GET /api/users")
		if users.Classes[diff.ClassBodyMismatch] != users.Replayed {
			t.Errorf("GET /api/users: classes %v, want every exchange to mismatch on generated_at", users.Classes)
		}
		for _, r := range out.Results {
			if r.Route != "GET /api/users" {
				continue
			}
			if !slices.ContainsFunc(r.Reported(), func(d diff.Difference) bool { return d.Pointer == "/generated_at" && d.Kind == diff.KindChanged }) {
				t.Errorf("capture %s: generated_at not reported: %+v", r.CaptureID, r.Differences)
			}
		}
		i := slices.IndexFunc(s.Volatile, func(v report.VolatileSummary) bool {
			return v.Route == "GET /api/users" && v.Where == diff.WhereBody && v.Pointer == "/generated_at"
		})
		if i < 0 {
			t.Fatalf("generated_at not listed as volatile: %+v", s.Volatile)
		}
		if v := s.Volatile[i]; v.Share != 1 || v.Count != users.Replayed || v.Compared != users.Replayed || len(v.Examples) == 0 {
			t.Errorf("volatile generated_at = %+v, want every exchange of the route", v)
		}
		if !strings.Contains(textReport(s), "Volatile without a baseline") {
			t.Error("text report lacks the volatile section")
		}
	})

	t.Run("v2 against v1", func(t *testing.T) {
		_, s := replay(t, p.kafka, api.VersionV2, api.VersionV1)
		t.Logf("report:\n%s", textReport(s))
		if s.Gate.Passed || s.ErrorRate != 0 {
			t.Fatal("gate passed a release with regressions")
		}
		// phone renamed to phone_number: body mismatches on the list route.
		users := route(t, s, "GET /api/users")
		if users.Classes[diff.ClassBodyMismatch] == 0 || users.Classes[diff.ClassStatusMismatch] != 0 {
			t.Errorf("GET /api/users: classes %v, want body mismatches only", users.Classes)
		}
		if !hasDifference(s, "GET /api/users", diff.WhereBody, "/users/*/phone_number", diff.KindAdded) ||
			!hasDifference(s, "GET /api/users", diff.WhereBody, "/users/*/phone", diff.KindRemoved) {
			t.Error("renamed field not reported")
		}
		// List order reversed.
		if !hasDifference(s, "GET /api/users", diff.WhereBody, "/users/*/id", diff.KindChanged) {
			t.Error("reordered list not reported")
		}
		// 400 instead of 422 for invalid orders: every replayed order is
		// invalid on both targets (card numbers are tokenized), so every one
		// is a status mismatch, reported with all three statuses.
		orders := route(t, s, "POST /api/orders")
		if orders.Classes[diff.ClassStatusMismatch] != orders.Replayed {
			t.Errorf("POST /api/orders: classes %v, want every exchange status_mismatch", orders.Classes)
		}
		if d, ok := difference(s, "POST /api/orders", diff.WhereStatus, "", diff.KindChanged); !ok || d.Baseline != "422" || d.Candidate != "400" {
			t.Errorf("status difference %+v, want baseline 422 and candidate 400", d)
		}
		// 40 ms slower on /api/orders.
		for _, name := range []string{"POST /api/orders", "GET /api/orders/{id}"} {
			if !slices.Contains(s.LatencyRegressions, name) {
				t.Errorf("latency regression on %s not flagged: %v", name, s.LatencyRegressions)
			}
		}
		// Noise stays suppressed in a run with real regressions.
		if hasDifference(s, "GET /api/users", diff.WhereBody, "/generated_at", diff.KindChanged) {
			t.Error("generated_at reported as a regression")
		}
		// Routes v2 did not change stay clean: no false positives.
		for _, name := range unchangedRoutes {
			if r := route(t, s, name); r.MismatchRate != 0 {
				t.Errorf("%s: mismatch rate %.2f with classes %v, want 0", name, r.MismatchRate, r.Classes)
			}
		}
	})
}
