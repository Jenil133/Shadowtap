package kafka

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// readWindow reads r to io.EOF and returns the capture ids in read order.
func readWindow(t *testing.T, r *WindowReader) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var ids []string
	for {
		m, err := r.Next(ctx)
		if errors.Is(err, io.EOF) {
			return ids
		}
		if err != nil {
			t.Fatalf("Next after %d records: %v", len(ids), err)
		}
		if m.DecodeErr != nil {
			t.Fatalf("decode: %v", m.DecodeErr)
		}
		ids = append(ids, m.Exchange.ID)
	}
}

func TestWindowReader(t *testing.T) {
	c := newCluster(t)
	cfg := testConfig(c)
	base := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	var xs []*capture.Exchange
	for i := range 30 { // one exchange a second, spread over every partition
		xs = append(xs, testExchange(traceID(i), base.Add(time.Duration(i)*time.Second)))
	}
	// Written out of timestamp order, as a tap does when a later request
	// finishes first.
	xs[10], xs[11] = xs[11], xs[10]
	publishAll(t, cfg, xs...)

	idsBetween := func(from, to time.Time) []string {
		var ids []string
		for _, x := range xs {
			if !x.Timing.StartedAt.Before(from) && x.Timing.StartedAt.Before(to) {
				ids = append(ids, x.ID)
			}
		}
		slices.Sort(ids)
		return ids
	}

	tests := []struct {
		name     string
		w        WindowOptions
		from, to time.Time
	}{
		{"everything", WindowOptions{}, base, base.Add(time.Hour)},
		{"bounded", WindowOptions{From: base.Add(5 * time.Second), To: base.Add(15 * time.Second)}, base.Add(5 * time.Second), base.Add(15 * time.Second)},
		{"bounded without skew", WindowOptions{From: base.Add(5 * time.Second), To: base.Add(15 * time.Second), Skew: -1}, base.Add(5 * time.Second), base.Add(15 * time.Second)},
		{"open end", WindowOptions{From: base.Add(25 * time.Second)}, base.Add(25 * time.Second), base.Add(time.Hour)},
		{"open start", WindowOptions{To: base.Add(3 * time.Second)}, base, base.Add(3 * time.Second)},
		{"after the data", WindowOptions{From: base.Add(time.Hour)}, base.Add(time.Hour), base.Add(2 * time.Hour)},
		// `--to now`: no record is stamped at or after the end, so the end
		// lookup must fall back to each partition's end offset rather than
		// drop the partition.
		{"end after the last record", WindowOptions{From: base.Add(20 * time.Second), To: base.Add(time.Hour)}, base.Add(20 * time.Second), base.Add(time.Hour)},
		{"start before the first record", WindowOptions{From: base.Add(-time.Hour), To: base.Add(3 * time.Second), Skew: -1}, base.Add(-time.Hour), base.Add(3 * time.Second)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, err := NewWindowReader(context.Background(), cfg, tt.w)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			got := readWindow(t, r)
			slices.Sort(got)
			want := idsBetween(tt.from, tt.to)
			if !slices.Equal(got, want) {
				t.Errorf("read %d records, want %d", len(got), len(want))
			}
			if len(r.Pending()) != 0 {
				t.Errorf("pending after EOF: %v", r.Pending())
			}
		})
	}

	t.Run("finite while the topic grows", func(t *testing.T) {
		r, err := NewWindowReader(context.Background(), cfg, WindowOptions{})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		publishAll(t, cfg, testExchange(traceID(99), base.Add(time.Minute)))
		if got := readWindow(t, r); len(got) != len(xs) {
			t.Errorf("read %d records, want the %d present at creation", len(got), len(xs))
		}
	})

	t.Run("reproducible", func(t *testing.T) {
		w := WindowOptions{From: base.Add(2 * time.Second), To: base.Add(20 * time.Second)}
		read := func() []string {
			r, err := NewWindowReader(context.Background(), cfg, w)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			ids := readWindow(t, r)
			slices.Sort(ids)
			return ids
		}
		if a, b := read(), read(); !slices.Equal(a, b) {
			t.Error("two reads of one window differ")
		}
	})
}

func TestWindowReaderErrors(t *testing.T) {
	c := newCluster(t)
	cfg := testConfig(c)
	now := time.Now()
	if _, err := NewWindowReader(context.Background(), cfg, WindowOptions{From: now, To: now}); err == nil {
		t.Error("empty window accepted")
	}
	bad := cfg
	bad.Topic = "no such topic!"
	if _, err := NewWindowReader(context.Background(), bad, WindowOptions{}); err == nil {
		t.Error("invalid topic accepted")
	}
	missing := cfg
	missing.Topic = "missing.topic"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := NewWindowReader(ctx, missing, WindowOptions{}); err == nil {
		t.Error("missing topic accepted")
	}

	r, err := NewWindowReader(context.Background(), cfg, WindowOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(context.Background()); !errors.Is(err, io.EOF) {
		t.Errorf("empty topic: Next = %v, want io.EOF", err)
	}
	r.Close()
	r.Close()
	if _, err := r.Next(context.Background()); err == nil || errors.Is(err, io.EOF) {
		t.Errorf("after Close: Next = %v", err)
	}
}
