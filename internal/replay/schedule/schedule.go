// Package schedule sends captures open loop: each one at its original offset
// from the first capture, divided by a speed factor, regardless of how fast
// the target answers. A slow target therefore does not slow the arrival rate
// (no coordinated omission); when too many requests are in flight the
// overload policy queues or drops the next one, and either way it is counted.
package schedule

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// Overload policies: what happens to a capture that is due while
// Concurrency requests are in flight.
const (
	// OverloadQueue waits for a free slot; the capture is sent late and the
	// delay shows up as schedule lag.
	OverloadQueue = "queue"
	// OverloadDrop skips the capture and counts it.
	OverloadDrop = "drop"
)

// LagWarnThreshold is the p99 schedule lag above which a replay no longer
// reproduces the original arrival pattern faithfully.
const LagWarnThreshold = 50 * time.Millisecond

// Clock is the time source; tests use a virtual one.
type Clock interface {
	Now() time.Time
	// WaitUntil blocks until t or until ctx is done.
	WaitUntil(ctx context.Context, t time.Time) error
}

// RealClock is the wall clock.
type RealClock struct{}

// Now returns time.Now().
func (RealClock) Now() time.Time { return time.Now() }

// WaitUntil sleeps until t.
func (RealClock) WaitUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Options configures a run.
type Options struct {
	// Speed divides the original gaps between captures: 1 replays at the
	// original pace, 4 four times faster. 0 (max) ignores timing and sends as
	// fast as Concurrency allows.
	Speed float64
	// Concurrency bounds requests in flight; 0 means 64.
	Concurrency int
	// OnOverload is OverloadQueue (default) or OverloadDrop. With Speed 0
	// captures always wait for a slot.
	OnOverload string
	// OnDrop is called, from the scheduling goroutine, with every capture
	// OverloadDrop skips, so the caller can account for it (Stats.Dropped
	// counts them either way). Optional.
	OnDrop func(*capture.Exchange)
	// Clock defaults to RealClock.
	Clock Clock
}

// DefaultConcurrency is the default Options.Concurrency.
const DefaultConcurrency = 64

// Validate reports invalid options.
func (o Options) Validate() error {
	var errs []error
	if o.Speed < 0 {
		errs = append(errs, fmt.Errorf("schedule: speed %v must be positive, or 0 for max", o.Speed))
	}
	if o.Concurrency < 0 {
		errs = append(errs, fmt.Errorf("schedule: concurrency %d must be positive", o.Concurrency))
	}
	switch o.OnOverload {
	case "", OverloadQueue, OverloadDrop:
	default:
		errs = append(errs, fmt.Errorf("schedule: on-overload %q must be %s or %s", o.OnOverload, OverloadQueue, OverloadDrop))
	}
	return errors.Join(errs...)
}

// Job is one capture handed to the send function.
type Job struct {
	Exchange *capture.Exchange
	Seq      int       // 0-based position in the replay
	Target   time.Time // when the schedule wanted it sent
	Sent     time.Time // when it was handed over
}

// Lag is how late the job was handed over.
func (j Job) Lag() time.Duration { return j.Sent.Sub(j.Target) }

// Stats summarizes a run.
type Stats struct {
	Scheduled int // captures read from the source
	Sent      int // handed to the send function
	Dropped   int // skipped by OverloadDrop
	Lag       LagStats
	Started   time.Time
	Finished  time.Time
	// Span is the original time between the first and last capture.
	Span time.Duration
}

// LagStats summarize the schedule lag of the sent jobs: percentiles and a
// histogram over LagBuckets.
type LagStats struct {
	P50, P95, P99, Max time.Duration
	// Histogram has one count per bound of LagBuckets plus a final count
	// for lags above the last bound: Histogram[i] is the number of jobs
	// handed over more than LagBuckets()[i-1] and at most LagBuckets()[i]
	// late. Nil when no job was sent.
	Histogram []int
}

// LagBuckets returns the upper bounds of the schedule lag histogram in
// ascending order; LagStats.Histogram has one more, open-ended, bucket. The
// bounds bracket LagWarnThreshold so a report shows how much of a run was
// late, and by how much.
func LagBuckets() []time.Duration {
	return []time.Duration{
		time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond,
		LagWarnThreshold, 100 * time.Millisecond, 250 * time.Millisecond, time.Second,
	}
}

// Run reads captures from next until io.EOF and calls send for each at its
// scheduled time, with at most Concurrency sends running at once. It returns
// when every send has returned. An error from next (other than io.EOF) or a
// done ctx stops scheduling; sends already started still finish.
func Run(ctx context.Context, opts Options, next func(context.Context) (*capture.Exchange, error), send func(context.Context, Job)) (Stats, error) {
	if err := opts.Validate(); err != nil {
		return Stats{}, err
	}
	clock := opts.Clock
	if clock == nil {
		clock = RealClock{}
	}
	concurrency := opts.Concurrency
	if concurrency == 0 {
		concurrency = DefaultConcurrency
	}
	drop := opts.OnOverload == OverloadDrop && opts.Speed > 0

	var (
		st    Stats
		lags  []time.Duration
		wg    sync.WaitGroup
		slots = make(chan struct{}, concurrency)
		first time.Time
		last  time.Time
		err   error
	)
	st.Started = clock.Now()
	for seq := 0; ; seq++ {
		var x *capture.Exchange
		x, err = next(ctx)
		if errors.Is(err, io.EOF) {
			err = nil
			break
		}
		if err != nil {
			break
		}
		st.Scheduled++
		started := x.Timing.StartedAt
		if seq == 0 {
			first = started
		}
		if started.After(last) {
			last = started
		}
		target := st.Started
		if opts.Speed > 0 {
			if offset := started.Sub(first); offset > 0 {
				target = st.Started.Add(time.Duration(float64(offset) / opts.Speed))
			}
			if err = clock.WaitUntil(ctx, target); err != nil {
				break
			}
		}
		if drop {
			select {
			case slots <- struct{}{}:
			default:
				st.Dropped++
				if opts.OnDrop != nil {
					opts.OnDrop(x)
				}
				continue
			}
		} else {
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				err = ctx.Err()
			}
			if err != nil {
				break
			}
		}
		j := Job{Exchange: x, Seq: seq, Target: target, Sent: clock.Now()}
		if opts.Speed == 0 {
			j.Target = j.Sent // no schedule to lag behind
		}
		lags = append(lags, max(j.Lag(), 0))
		st.Sent++
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slots }()
			send(ctx, j)
		}()
	}
	wg.Wait()
	st.Finished = clock.Now()
	if !first.IsZero() {
		st.Span = last.Sub(first)
	}
	st.Lag = lagStats(lags)
	return st, err
}

func lagStats(lags []time.Duration) LagStats {
	if len(lags) == 0 {
		return LagStats{}
	}
	buckets := LagBuckets()
	st := LagStats{Histogram: make([]int, len(buckets)+1)}
	for _, l := range lags {
		// The first bucket whose bound is not below the lag; past the last
		// bound, the open-ended bucket.
		i, _ := slices.BinarySearch(buckets, l)
		st.Histogram[i]++
	}
	slices.Sort(lags)
	pct := func(p float64) time.Duration {
		i := int(p*float64(len(lags))+0.999999) - 1 // nearest rank
		return lags[min(max(i, 0), len(lags)-1)]
	}
	st.P50, st.P95, st.P99, st.Max = pct(0.50), pct(0.95), pct(0.99), lags[len(lags)-1]
	return st
}

// VirtualClock is a Clock that never sleeps: WaitUntil moves it forward to
// the target at once. Advance simulates time spent elsewhere. Safe for
// concurrent use.
type VirtualClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewVirtualClock starts a virtual clock at t.
func NewVirtualClock(t time.Time) *VirtualClock { return &VirtualClock{now: t} }

// Now returns the virtual time.
func (c *VirtualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// WaitUntil moves the clock to t if t is later.
func (c *VirtualClock) WaitUntil(ctx context.Context, t time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.After(c.now) {
		c.now = t
	}
	return nil
}

// Advance moves the clock forward by d.
func (c *VirtualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
