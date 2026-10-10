// Package source reads the captures a replay sends: a time window from the
// captures topic, merged across partitions into request-start order, or an
// NDJSON file written by `shadowtap tail --format json`.
package source

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/kafka"
)

// Source yields captures in request-start order and io.EOF after the last.
type Source interface {
	Next(ctx context.Context) (*capture.Exchange, error)
	// Stats reports what was read so far.
	Stats() Stats
	Close() error
}

// Stats counts what a source read.
type Stats struct {
	Read        int // records read, valid or not
	Undecodable int // records that are not valid captures
	// Disordered counts captures emitted earlier than one already emitted:
	// clock skew between taps or a request that outlived the reorder slack.
	Disordered int
	// Forced counts captures emitted before the merge watermark allowed,
	// because the reorder buffer was full.
	Forced int
}

// DefaultReorderSlack is how far behind the slowest partition a capture must
// be before it is emitted. A tap writes an exchange when it completes, so a
// partition is only roughly ordered by request start.
const DefaultReorderSlack = 2 * time.Second

// DefaultMaxBuffered bounds the reorder buffer of a Kafka source.
const DefaultMaxBuffered = 100_000

// partitionReader is what the merge needs from kafka.WindowReader.
type partitionReader interface {
	Next(ctx context.Context) (kafka.Message, error)
	Pending() []int32
	Close()
}

// KafkaOptions configures a Kafka source.
type KafkaOptions struct {
	Window kafka.WindowOptions
	// Slack is the reorder slack; 0 means DefaultReorderSlack.
	Slack time.Duration
	// MaxBuffered bounds buffered captures; 0 means DefaultMaxBuffered.
	MaxBuffered int
}

// NewKafka reads a window of cfg.Topic.
func NewKafka(ctx context.Context, cfg kafka.Config, opts KafkaOptions) (Source, error) {
	r, err := kafka.NewWindowReader(ctx, cfg, opts.Window)
	if err != nil {
		return nil, err
	}
	return newMerge(r, opts.Slack, opts.MaxBuffered), nil
}

// merge is a k-way merge of partitions by request start. Each partition is
// roughly ordered, so a capture is emitted once every partition still being
// read has reached its start plus the slack: nothing older can follow.
type merge struct {
	r           partitionReader
	slack       time.Duration
	maxBuffered int

	h        captureHeap
	lastSeen map[int32]time.Time // latest record timestamp per partition
	eof      bool
	lastOut  time.Time
	stats    Stats
}

func newMerge(r partitionReader, slack time.Duration, maxBuffered int) *merge {
	if slack <= 0 {
		slack = DefaultReorderSlack
	}
	if maxBuffered <= 0 {
		maxBuffered = DefaultMaxBuffered
	}
	return &merge{r: r, slack: slack, maxBuffered: maxBuffered, lastSeen: map[int32]time.Time{}}
}

func (m *merge) Next(ctx context.Context) (*capture.Exchange, error) {
	for {
		if m.h.Len() > 0 {
			if ok, forced := m.ready(); ok {
				if forced {
					m.stats.Forced++
				}
				return m.emit(), nil
			}
		}
		if m.eof {
			return nil, io.EOF
		}
		msg, err := m.r.Next(ctx)
		if errors.Is(err, io.EOF) {
			m.eof = true
			continue
		}
		if err != nil {
			return nil, err
		}
		m.stats.Read++
		if msg.Timestamp.After(m.lastSeen[msg.Partition]) {
			m.lastSeen[msg.Partition] = msg.Timestamp
		}
		if msg.DecodeErr != nil {
			m.stats.Undecodable++
			continue
		}
		heap.Push(&m.h, msg.Exchange)
	}
}

// ready reports whether the oldest buffered capture can be emitted, and
// whether only a full buffer allows it.
func (m *merge) ready() (ok, forced bool) {
	if m.eof {
		return true, false
	}
	top := m.h[0].Timing.StartedAt
	watermark, known := time.Time{}, true
	for _, p := range m.r.Pending() {
		seen, ok := m.lastSeen[p]
		if !ok {
			known = false
			break
		}
		if watermark.IsZero() || seen.Before(watermark) {
			watermark = seen
		}
	}
	if known && (watermark.IsZero() || !top.After(watermark.Add(-m.slack))) {
		// No partition is pending (zero watermark), or all have passed top.
		return true, false
	}
	if m.h.Len() >= m.maxBuffered {
		return true, true
	}
	return false, false
}

func (m *merge) emit() *capture.Exchange {
	x := heap.Pop(&m.h).(*capture.Exchange)
	if x.Timing.StartedAt.Before(m.lastOut) {
		m.stats.Disordered++
	} else {
		m.lastOut = x.Timing.StartedAt
	}
	return x
}

func (m *merge) Stats() Stats { return m.stats }

func (m *merge) Close() error {
	m.r.Close()
	return nil
}

// captureHeap orders captures by request start, then id (UUIDv7, so also
// roughly by time), which makes the order of a window deterministic.
type captureHeap []*capture.Exchange

func (h captureHeap) Len() int { return len(h) }
func (h captureHeap) Less(i, j int) bool {
	a, b := h[i].Timing.StartedAt, h[j].Timing.StartedAt
	if !a.Equal(b) {
		return a.Before(b)
	}
	return h[i].ID < h[j].ID
}
func (h captureHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *captureHeap) Push(x any)   { *h = append(*h, x.(*capture.Exchange)) }
func (h *captureHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return x
}

// MaxFileRecords bounds how many captures a file source loads; a file is
// sorted in memory.
const MaxFileRecords = 1_000_000

// maxLineBytes bounds one NDJSON line: the largest record the tap writes is
// kafka.max_record_bytes, capped at 16 MiB.
const maxLineBytes = 16 << 20

// FileOptions configures a file source.
type FileOptions struct {
	// From and To bound captures by request start (To exclusive); zero
	// values leave that side open.
	From, To time.Time
}

// NewFile reads every capture from NDJSON in r (one capture per line, as
// written by `shadowtap tail --format json`) and returns them sorted by
// request start. Blank lines are skipped; lines that are not valid captures
// are counted as undecodable.
func NewFile(r io.Reader, opts FileOptions) (Source, error) {
	f := &file{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), maxLineBytes)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		f.stats.Read++
		x, err := capture.Decode(line)
		if err == nil {
			err = x.Validate()
		}
		if err != nil {
			f.stats.Undecodable++
			continue
		}
		ts := x.Timing.StartedAt
		if (!opts.From.IsZero() && ts.Before(opts.From)) || (!opts.To.IsZero() && !ts.Before(opts.To)) {
			continue
		}
		if f.h.Len() >= MaxFileRecords {
			return nil, fmt.Errorf("source: file holds more than %d captures; replay a narrower window", MaxFileRecords)
		}
		f.h = append(f.h, x)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("source: read captures: %w", err)
	}
	heap.Init(&f.h)
	return f, nil
}

type file struct {
	h     captureHeap
	stats Stats
}

func (f *file) Next(ctx context.Context) (*capture.Exchange, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.h.Len() == 0 {
		return nil, io.EOF
	}
	return heap.Pop(&f.h).(*capture.Exchange), nil
}

func (f *file) Stats() Stats { return f.stats }
func (f *file) Close() error { return nil }
