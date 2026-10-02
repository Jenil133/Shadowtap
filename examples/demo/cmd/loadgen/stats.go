package main

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"
)

// maxErrorKinds bounds the distinct transport errors a summary lists.
const maxErrorKinds = 10

// recorder aggregates outcomes from concurrent senders.
type recorder struct {
	mu         sync.Mutex
	requests   int
	statuses   map[int]int
	unexpected int
	errors     map[string]int // description -> count
	failures   int
	latencies  []time.Duration // completed requests only
}

func newRecorder() *recorder {
	return &recorder{statuses: map[int]int{}, errors: map[string]int{}}
}

func (r *recorder) record(o outcome) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.requests++
	if o.err != nil {
		r.failures++
		kind := describe(o.err)
		if _, ok := r.errors[kind]; !ok && len(r.errors) >= maxErrorKinds {
			kind = "other errors"
		}
		r.errors[kind]++
		return
	}
	r.statuses[o.status]++
	if o.expected != 0 && o.status != o.expected {
		r.unexpected++
	}
	r.latencies = append(r.latencies, o.latency)
}

func (r *recorder) summary(elapsed time.Duration) *summary {
	r.mu.Lock()
	defer r.mu.Unlock()
	lat := slices.Clone(r.latencies)
	slices.Sort(lat)
	s := &summary{
		requests:        r.requests,
		elapsed:         elapsed,
		statuses:        maps.Clone(r.statuses),
		unexpected:      r.unexpected,
		transportErrors: r.failures,
		errorKinds:      maps.Clone(r.errors),
		p50:             percentile(lat, 50),
		p95:             percentile(lat, 95),
	}
	if len(lat) > 0 {
		s.max = lat[len(lat)-1]
	}
	return s
}

// describe summarizes a transport error without the request URL, which can
// carry personal data (emails in lookup paths and search queries).
func describe(err error) string {
	var uerr *url.Error
	if errors.As(err, &uerr) {
		if uerr.Timeout() {
			return uerr.Op + ": timeout"
		}
		return uerr.Op + ": " + uerr.Err.Error()
	}
	return err.Error()
}

// percentile returns the nearest-rank p-th percentile of sorted durations.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	return sorted[min(max(rank, 1), len(sorted))-1]
}

// summary describes a finished run.
type summary struct {
	requests        int
	elapsed         time.Duration
	statuses        map[int]int
	unexpected      int // responses whose status differs from the generator's prediction
	transportErrors int
	errorKinds      map[string]int
	p50, p95, max   time.Duration
	ledgerPath      string
	ledgerValues    int
}

func (s *summary) transportErrorRate() float64 {
	if s.requests == 0 {
		return 0
	}
	return float64(s.transportErrors) / float64(s.requests)
}

func (s *summary) write(w io.Writer) {
	perSecond := 0.0
	if s.elapsed > 0 {
		perSecond = float64(s.requests) / s.elapsed.Seconds()
	}
	var codes []string
	for _, code := range slices.Sorted(maps.Keys(s.statuses)) {
		codes = append(codes, fmt.Sprintf("%d=%d", code, s.statuses[code]))
	}
	if len(codes) == 0 {
		codes = []string{"none"}
	}
	fmt.Fprintf(w, "requests:          %d in %s (%.1f/s)\n", s.requests, s.elapsed.Round(time.Millisecond), perSecond)
	fmt.Fprintf(w, "status codes:      %s\n", strings.Join(codes, " "))
	// Some are normal under concurrency or against an API with earlier data.
	fmt.Fprintf(w, "unexpected status: %d (status differs from the generator's model)\n", s.unexpected)
	fmt.Fprintf(w, "transport errors:  %d (%.1f%%)\n", s.transportErrors, 100*s.transportErrorRate())
	for _, kind := range slices.Sorted(maps.Keys(s.errorKinds)) {
		fmt.Fprintf(w, "  %6d  %s\n", s.errorKinds[kind], kind)
	}
	fmt.Fprintf(w, "latency:           p50 %s  p95 %s  max %s\n", round(s.p50), round(s.p95), round(s.max))
	if s.ledgerPath != "" {
		fmt.Fprintf(w, "pii ledger:        %d values written to %s\n", s.ledgerValues, s.ledgerPath)
	}
}

func round(d time.Duration) time.Duration {
	if d >= time.Millisecond {
		return d.Round(10 * time.Microsecond)
	}
	return d.Round(time.Microsecond)
}
