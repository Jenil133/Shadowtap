package runner

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/replay/request"
	"github.com/Jenil133/Shadowtap/internal/replay/schedule"
	"github.com/Jenil133/Shadowtap/internal/replay/source"
	"github.com/Jenil133/Shadowtap/internal/scrub"
)

var base = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

// sliceSource yields fixed captures.
type sliceSource struct {
	xs []*capture.Exchange
	i  int
	// onEOF, if set, runs once when the captures are exhausted.
	onEOF func()
	once  sync.Once
}

func (s *sliceSource) Next(ctx context.Context) (*capture.Exchange, error) {
	if s.i == len(s.xs) {
		if s.onEOF != nil {
			s.once.Do(s.onEOF)
		}
		return nil, io.EOF
	}
	s.i++
	return s.xs[s.i-1], nil
}
func (s *sliceSource) Stats() source.Stats { return source.Stats{Read: s.i} }
func (s *sliceSource) Close() error        { return nil }

func exchange(i int, method, path string, status int, body string) *capture.Exchange {
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            fmt.Sprintf("cap-%d", i),
		Service:       "demo-api",
		TraceID:       fmt.Sprintf("4bf92f3577b34da6a3ce929d%08x", i),
		Timing:        capture.Timing{StartedAt: base.Add(time.Duration(i) * 10 * time.Millisecond), DurationMicros: 1000},
		Request:       capture.Request{Method: method, Path: path, Headers: capture.Headers{"accept": {"application/json"}}},
		Response:      capture.Response{Status: status, Headers: capture.Headers{"content-type": {"application/json"}}},
	}
	x.Response.Body = capture.Body{ContentType: "application/json", Size: int64(len(body))}
	x.Response.Body.SetData([]byte(body))
	return x
}

func engine(t *testing.T) *diff.Engine {
	t.Helper()
	s, err := scrub.New(scrub.Config{Key: []byte("runner-test-key-0123456789")})
	if err != nil {
		t.Fatal(err)
	}
	e, err := diff.New(diff.Config{}, s, diff.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func server(t *testing.T, version string, hits *atomic.Int64) *url.URL {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get(request.HeaderCaptureID) == "" || r.Header.Get(request.HeaderRun) != "run-1" {
			http.Error(w, "missing replay markers", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/redirect":
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
		case r.URL.Path == "/slow":
			time.Sleep(200 * time.Millisecond)
			fmt.Fprint(w, `{}`)
		case version == "v2":
			fmt.Fprint(w, `{"id":1,"phone_number":"x"}`)
		default:
			fmt.Fprint(w, `{"id":1,"phone":"x"}`)
		}
	}))
	t.Cleanup(srv.Close)
	u, err := request.ValidateTarget(srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func byID(out *Outcome) map[string]*diff.Result {
	m := map[string]*diff.Result{}
	for _, r := range out.Results {
		m[r.CaptureID] = r
	}
	return m
}

func TestRun(t *testing.T) {
	var candHits, baseHits atomic.Int64
	src := &sliceSource{xs: []*capture.Exchange{
		exchange(0, "GET", "/api/users/1", 200, `{"id":1,"phone":"x"}`),
		exchange(1, "POST", "/api/users", 201, `{}`),
		exchange(2, "GET", "/redirect", 302, ``),
		exchange(3, "GET", "/slow", 200, `{}`),
	}}
	src.xs[2].Response.Headers = capture.Headers{"location": {"/elsewhere"}, "content-type": {"application/json"}}
	cfg := Config{
		RunID:     "run-1",
		Candidate: server(t, "v2", &candHits),
		Baseline:  server(t, "v1", &baseHits),
		Engine:    engine(t),
		Schedule:  schedule.Options{Speed: 0, Concurrency: 4},
		Timeout:   100 * time.Millisecond,
	}
	out, err := Run(context.Background(), cfg, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Results) != 4 || !out.HasBaseline {
		t.Fatalf("%d results", len(out.Results))
	}
	res := byID(out)
	if r := res["cap-0"]; r.Class != diff.ClassBodyMismatch || len(r.Reported()) != 2 || r.ReplayTraceID == "" || r.Status.Baseline != 200 {
		t.Errorf("regression: %+v", r)
	}
	if r := res["cap-1"]; r.Class != diff.ClassSkipped || r.Reason != source.SkipMutating {
		t.Errorf("POST: %+v", r)
	}
	if r := res["cap-2"]; r.Status.Candidate != http.StatusFound {
		t.Errorf("redirect followed: %+v", r)
	}
	if r := res["cap-3"]; r.Class != diff.ClassError || !strings.Contains(r.Reason, "deadline") {
		t.Errorf("timeout: %+v", r)
	}
	// The baseline also gets a noise probe for each safe request it
	// answered (not for /slow, which timed out).
	if candHits.Load() != 3 || baseHits.Load() != 5 {
		t.Errorf("hits candidate %d baseline %d, want 3 and 5", candHits.Load(), baseHits.Load())
	}
	for i := 1; i < len(out.Results); i++ {
		if out.Results[i].StartedAt.Before(out.Results[i-1].StartedAt) {
			t.Error("results not in request-start order")
		}
	}
}

func TestDryRunSendsNothing(t *testing.T) {
	var hits atomic.Int64
	src := &sliceSource{xs: []*capture.Exchange{exchange(0, "GET", "/a", 200, `{}`)}}
	out, err := Run(context.Background(), Config{RunID: "run-1", Candidate: server(t, "v1", &hits), Engine: engine(t), DryRun: true}, src)
	if err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 0 || out.Results[0].Reason != SkipDryRun {
		t.Errorf("dry run: %d hits, %+v", hits.Load(), out.Results[0])
	}
}

func TestRunValidates(t *testing.T) {
	src := &sliceSource{}
	u, _ := url.Parse("http://localhost:1")
	for _, cfg := range []Config{
		{Engine: engine(t)},
		{Candidate: u},
		{Candidate: u, Engine: engine(t), Filter: source.Filter{Methods: []string{"DELETE"}}},
		{Candidate: u, Engine: engine(t), Schedule: schedule.Options{Speed: -1}},
	} {
		if _, err := Run(context.Background(), cfg, src); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
}

// TestRunReproducesSpacing drives whole replays on a virtual clock: captures
// 10 ms apart are sent exactly 10 ms apart at 1x, 2.5 ms apart at 4x and all
// at once at max speed, every one of them is sent with no schedule lag, and a
// candidate that answers like production matches once both sides are
// scrubbed with the tap's key.
func TestRunReproducesSpacing(t *testing.T) {
	scrubber, err := scrub.New(scrub.Config{Key: []byte("runner-test-key-0123456789")})
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	tests := []struct {
		name  string
		speed float64
		gap   time.Duration // between consecutive sends
	}{
		{"1x", 1, 10 * time.Millisecond},
		{"4x", 4, 2500 * time.Microsecond},
		{"max", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hits atomic.Int64
			var xs []*capture.Exchange
			for i := range n {
				x := exchange(i, "GET", fmt.Sprintf("/api/users/%d", i), 200, `{"id":1,"phone":"x"}`)
				scrubber.Exchange(x) // as the tap stores it: phone becomes a pseudonym
				xs = append(xs, x)
			}
			clock := schedule.NewVirtualClock(base.Add(time.Hour))
			out, err := Run(context.Background(), Config{
				RunID:     "run-1",
				Candidate: server(t, "v1", &hits),
				Engine:    engine(t), // same key: the live phone scrubs to the same pseudonym
				Schedule:  schedule.Options{Speed: tt.speed, Concurrency: 1, Clock: clock},
			}, &sliceSource{xs: xs})
			if err != nil {
				t.Fatal(err)
			}
			if len(out.Results) != n || out.Schedule.Sent != n || hits.Load() != n {
				t.Fatalf("results %d, sent %d, hits %d, want %d", len(out.Results), out.Schedule.Sent, hits.Load(), n)
			}
			for i, r := range out.Results {
				if r.Class != diff.ClassMatch || r.LagMicros != 0 {
					t.Errorf("capture %d: class %s lag %dµs", i, r.Class, r.LagMicros)
				}
				if want := out.Results[0].SentAt.Add(time.Duration(i) * tt.gap); !r.SentAt.Equal(want) {
					t.Errorf("capture %d sent at %v, want %v", i, r.SentAt, want)
				}
			}
			if out.Schedule.Span != 70*time.Millisecond || out.Schedule.Lag.Max != 0 {
				t.Errorf("schedule stats %+v", out.Schedule)
			}
		})
	}
}

// TestOverloadDropIsReported holds the candidate's first response until the
// source is exhausted: with one slot every later capture is due while the
// slot is taken, so OverloadDrop skips it, and the outcome records each one
// as skipped rather than losing it.
func TestOverloadDropIsReported(t *testing.T) {
	release := make(chan struct{})
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		<-release
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{}`)
	}))
	t.Cleanup(srv.Close)
	u, err := request.ValidateTarget(srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	const n = 5
	var xs []*capture.Exchange
	for i := range n {
		xs = append(xs, exchange(i, "GET", "/a", 200, `{}`))
	}
	src := &sliceSource{xs: xs, onEOF: func() { close(release) }}
	out, err := Run(context.Background(), Config{
		RunID:     "run-1",
		Candidate: u,
		Engine:    engine(t),
		Schedule:  schedule.Options{Speed: 1, Concurrency: 1, OnOverload: schedule.OverloadDrop, Clock: schedule.NewVirtualClock(base)},
	}, src)
	if err != nil {
		t.Fatal(err)
	}
	if out.Schedule.Sent != 1 || out.Schedule.Dropped != n-1 || hits.Load() != 1 {
		t.Fatalf("sent %d dropped %d hits %d, want 1, %d, 1", out.Schedule.Sent, out.Schedule.Dropped, hits.Load(), n-1)
	}
	if len(out.Results) != n {
		t.Fatalf("%d results, want one per capture (%d)", len(out.Results), n)
	}
	dropped := 0
	for _, r := range out.Results {
		switch {
		case r.Class == diff.ClassSkipped && r.Reason == SkipOverloadDrop:
			dropped++
		case r.Class != diff.ClassMatch:
			t.Errorf("capture %s: %s %s", r.CaptureID, r.Class, r.Reason)
		}
	}
	if dropped != n-1 {
		t.Errorf("%d results skipped as %s, want %d", dropped, SkipOverloadDrop, n-1)
	}
}
