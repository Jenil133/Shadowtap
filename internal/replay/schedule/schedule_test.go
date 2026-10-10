package schedule

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

var base = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

// captures returns a source of captures that started at the given offsets.
func captures(offsets ...time.Duration) func(context.Context) (*capture.Exchange, error) {
	i := 0
	return func(ctx context.Context) (*capture.Exchange, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if i == len(offsets) {
			return nil, io.EOF
		}
		x := &capture.Exchange{ID: fmt.Sprint(i), Timing: capture.Timing{StartedAt: base.Add(offsets[i])}}
		i++
		return x, nil
	}
}

type recorder struct {
	mu   sync.Mutex
	jobs []Job
}

func (r *recorder) send(_ context.Context, j Job) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs = append(r.jobs, j)
}

func (r *recorder) sorted() []Job {
	r.mu.Lock()
	defer r.mu.Unlock()
	sort.Slice(r.jobs, func(a, b int) bool { return r.jobs[a].Seq < r.jobs[b].Seq })
	return r.jobs
}

func TestSpacing(t *testing.T) {
	offsets := []time.Duration{0, 100 * time.Millisecond, 100 * time.Millisecond, 1 * time.Second, 3500 * time.Millisecond}
	// The virtual clock moves exactly to each target, so the spacing must be
	// the original gap divided by the speed, with no tolerance.
	for _, speed := range []float64{1, 2, 4, 10} {
		t.Run(fmt.Sprintf("%vx", speed), func(t *testing.T) {
			clock := NewVirtualClock(base.Add(time.Hour))
			var r recorder
			st, err := Run(context.Background(), Options{Speed: speed, Clock: clock}, captures(offsets...), r.send)
			if err != nil {
				t.Fatal(err)
			}
			jobs := r.sorted()
			if st.Sent != len(offsets) || len(jobs) != len(offsets) {
				t.Fatalf("sent %d/%d", st.Sent, len(jobs))
			}
			start := jobs[0].Sent
			for i, j := range jobs {
				want := time.Duration(float64(offsets[i]) / speed)
				if got := j.Sent.Sub(start); got != want {
					t.Errorf("capture %d sent at +%v, want +%v", i, got, want)
				}
			}
			if st.Lag.Max != 0 || st.Span != 3500*time.Millisecond {
				t.Errorf("stats = %+v", st)
			}
		})
	}
}

func TestMaxSpeedIgnoresTiming(t *testing.T) {
	clock := NewVirtualClock(base)
	var r recorder
	st, err := Run(context.Background(), Options{Speed: 0, Concurrency: 2, Clock: clock}, captures(0, time.Hour, 2*time.Hour), r.send)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range r.sorted() {
		if !j.Sent.Equal(base) {
			t.Errorf("capture %d sent at %v, want at once", j.Seq, j.Sent)
		}
	}
	if st.Sent != 3 || st.Dropped != 0 {
		t.Errorf("stats = %+v", st)
	}
}

func TestDisorderedCaptureIsSentAtOnce(t *testing.T) {
	clock := NewVirtualClock(base)
	var r recorder
	if _, err := Run(context.Background(), Options{Speed: 1, Clock: clock}, captures(time.Second, 0), r.send); err != nil {
		t.Fatal(err)
	}
	jobs := r.sorted()
	if !jobs[1].Target.Equal(jobs[0].Target) {
		t.Errorf("a capture older than the first is due at the start: %v vs %v", jobs[1].Target, jobs[0].Target)
	}
}

func TestOverloadDrop(t *testing.T) {
	clock := NewVirtualClock(base)
	started, release := make(chan struct{}, 1), make(chan struct{})
	var r recorder
	send := func(ctx context.Context, j Job) {
		r.send(ctx, j)
		started <- struct{}{}
		<-release
	}
	inner := captures(0, 10*time.Millisecond, 20*time.Millisecond, 30*time.Millisecond)
	n := 0
	next := func(ctx context.Context) (*capture.Exchange, error) {
		if n == 1 {
			<-started // the first send now holds the only slot
		}
		n++
		x, err := inner(ctx)
		if errors.Is(err, io.EOF) {
			close(release)
		}
		return x, err
	}
	var dropped []string
	onDrop := func(x *capture.Exchange) { dropped = append(dropped, x.ID) }
	st, err := Run(context.Background(), Options{Speed: 1, Concurrency: 1, OnOverload: OverloadDrop, OnDrop: onDrop, Clock: clock}, next, send)
	if err != nil {
		t.Fatal(err)
	}
	if st.Sent != 1 || st.Dropped != 3 || st.Scheduled != 4 {
		t.Errorf("stats = %+v, want 4 scheduled, 1 sent, 3 dropped", st)
	}
	// Every dropped capture is handed to OnDrop, in order, so the caller
	// can report it.
	if want := []string{"1", "2", "3"}; !slices.Equal(dropped, want) {
		t.Errorf("OnDrop saw %v, want %v", dropped, want)
	}
}

func TestOverloadQueueShowsLag(t *testing.T) {
	clock := NewVirtualClock(base)
	// Each send takes 50ms of virtual time while captures are 10ms apart:
	// with one slot they queue up and fall behind.
	var mu sync.Mutex
	send := func(_ context.Context, j Job) {
		mu.Lock()
		defer mu.Unlock()
		clock.Advance(50 * time.Millisecond)
	}
	st, err := Run(context.Background(), Options{Speed: 1, Concurrency: 1, OnOverload: OverloadQueue, Clock: clock},
		captures(0, 10*time.Millisecond, 20*time.Millisecond, 30*time.Millisecond), send)
	if err != nil {
		t.Fatal(err)
	}
	if st.Sent != 4 || st.Dropped != 0 {
		t.Fatalf("stats = %+v", st)
	}
	if st.Lag.Max <= 0 {
		t.Errorf("queued sends show no lag: %+v", st.Lag)
	}
}

func TestStopsOnContextAndSourceErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var r recorder
	if _, err := Run(ctx, Options{Speed: 1, Clock: NewVirtualClock(base)}, captures(0, time.Second), r.send); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	boom := errors.New("boom")
	failing := func(context.Context) (*capture.Exchange, error) { return nil, boom }
	if _, err := Run(context.Background(), Options{}, failing, r.send); !errors.Is(err, boom) {
		t.Errorf("err = %v, want the source error", err)
	}
}

func TestValidate(t *testing.T) {
	for _, o := range []Options{{Speed: -1}, {Concurrency: -1}, {OnOverload: "panic"}} {
		if o.Validate() == nil {
			t.Errorf("%+v validated", o)
		}
	}
}

func TestRealClockWaits(t *testing.T) {
	start := time.Now()
	if err := (RealClock{}).WaitUntil(context.Background(), start.Add(20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 20*time.Millisecond {
		t.Error("returned early")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (RealClock{}).WaitUntil(ctx, time.Now().Add(time.Hour)); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v", err)
	}
}

func TestLagStats(t *testing.T) {
	var lags []time.Duration
	for i := 1; i <= 100; i++ {
		lags = append(lags, time.Duration(i)*time.Millisecond)
	}
	got := lagStats(lags)
	if got.P50 != 50*time.Millisecond || got.P95 != 95*time.Millisecond || got.P99 != 99*time.Millisecond || got.Max != 100*time.Millisecond {
		t.Errorf("lagStats = %+v", got)
	}
	// Lags of 1..100 ms over the bounds 1, 5, 10, 25, 50, 100, 250 and
	// 1000 ms plus the open-ended bucket; a bound belongs to its own bucket.
	if want := []int{1, 4, 5, 15, 25, 50, 0, 0, 0}; !slices.Equal(got.Histogram, want) {
		t.Errorf("histogram = %v, want %v", got.Histogram, want)
	}
	if len(got.Histogram) != len(LagBuckets())+1 {
		t.Errorf("histogram has %d buckets for %d bounds", len(got.Histogram), len(LagBuckets()))
	}
	if empty := lagStats(nil); empty.Histogram != nil {
		t.Errorf("histogram without jobs = %v", empty.Histogram)
	}
}
