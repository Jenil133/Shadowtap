package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultWindowSkew widens the offset range a WindowReader reads on both
// sides of the window. Record timestamps are request starts, but a tap writes
// an exchange when it completes, so within a partition a record can follow
// one that started later. Reading a little wider and filtering on the
// timestamp keeps such records in the window.
const DefaultWindowSkew = 30 * time.Second

// WindowOptions selects the records a WindowReader returns.
type WindowOptions struct {
	// From is the start of the window (inclusive). Zero means the start of
	// every partition.
	From time.Time
	// To is the end of the window (exclusive). Zero means the end of every
	// partition when the reader is created, so the read is finite even while
	// the topic keeps growing.
	To time.Time
	// Skew is how far beyond the window, by record timestamp, offsets are
	// looked up; 0 means DefaultWindowSkew, a negative value none.
	Skew time.Duration
}

// WindowReader reads the records of a topic whose timestamps fall into a
// fixed window, then reports io.EOF. The offsets to read are resolved once,
// from the record timestamps, when the reader is created, so reading the
// same window twice returns the same records. Each partition is returned in
// offset order; partitions are interleaved as fetches arrive.
//
// Next must not be called concurrently; Close may be called from another
// goroutine to stop a blocked Next.
type WindowReader struct {
	client   *kgo.Client
	topic    string
	from, to time.Time // filter bounds on the record timestamp; zero = open

	mu      sync.Mutex
	next    map[int32]int64 // next offset to return per pending partition
	end     map[int32]int64 // end offset (exclusive) per partition
	buf     []*kgo.Record
	bufNext int

	closed    atomic.Bool
	closeOnce sync.Once
}

// NewWindowReader resolves the window to per-partition offsets and starts
// fetching them. Only the connection settings and the topic of cfg are used
// and validated. The extra options are applied last.
func NewWindowReader(ctx context.Context, cfg Config, w WindowOptions, extra ...kgo.Opt) (*WindowReader, error) {
	if err := configError(append(cfg.connectionProblems(), topicProblems(cfg.Topic)...)); err != nil {
		return nil, err
	}
	if !w.From.IsZero() && !w.To.IsZero() && !w.To.After(w.From) {
		return nil, fmt.Errorf("kafka: window end %s is not after its start %s", w.To.Format(time.RFC3339Nano), w.From.Format(time.RFC3339Nano))
	}
	skew := w.Skew
	switch {
	case skew == 0:
		skew = DefaultWindowSkew
	case skew < 0:
		skew = 0
	}
	opts, err := ClientOptions(cfg)
	if err != nil {
		return nil, err
	}
	meta, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create window client: %w", err)
	}
	starts, ends, err := windowOffsets(ctx, kadm.NewClient(meta), cfg.Topic, w, skew)
	meta.Close()
	if err != nil {
		return nil, err
	}

	r := &WindowReader{
		topic: cfg.Topic,
		from:  w.From,
		to:    w.To,
		next:  make(map[int32]int64),
		end:   make(map[int32]int64),
	}
	consume := make(map[int32]kgo.Offset)
	for p, start := range starts {
		end, ok := ends[p]
		if !ok || start >= end {
			continue // nothing of this partition is in the window
		}
		r.next[p], r.end[p] = start, end
		consume[p] = kgo.NewOffset().At(start)
	}
	if len(consume) == 0 {
		return r, nil // an empty window needs no client
	}
	opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{cfg.Topic: consume}))
	r.client, err = kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create window consumer: %w", err)
	}
	return r, nil
}

// windowOffsets returns, per partition, the first offset to read and the end
// offset (exclusive).
func windowOffsets(ctx context.Context, adm *kadm.Client, topic string, w WindowOptions, skew time.Duration) (starts, ends map[int32]int64, err error) {
	list := func(at time.Time, edge func(context.Context, ...string) (kadm.ListedOffsets, error)) (map[int32]int64, error) {
		var (
			listed kadm.ListedOffsets
			err    error
		)
		if at.IsZero() {
			listed, err = edge(ctx, topic)
		} else {
			listed, err = adm.ListOffsetsAfterMilli(ctx, max(at.UnixMilli(), 0), topic)
		}
		if err == nil {
			err = listed.Error()
		}
		if err != nil {
			return nil, fmt.Errorf("kafka: list offsets of %s: %w", topic, err)
		}
		out := make(map[int32]int64)
		listed.Each(func(o kadm.ListedOffset) {
			if o.Partition >= 0 && o.Offset >= 0 {
				out[o.Partition] = o.Offset
			}
		})
		if len(out) == 0 {
			return nil, fmt.Errorf("kafka: topic %s has no partitions", topic)
		}
		return out, nil
	}
	from, to := w.From, w.To
	if !from.IsZero() {
		from = from.Add(-skew)
	}
	if !to.IsZero() {
		to = to.Add(skew)
	}
	if starts, err = list(from, adm.ListStartOffsets); err != nil {
		return nil, nil, err
	}
	if ends, err = list(to, adm.ListEndOffsets); err != nil {
		return nil, nil, err
	}
	return starts, ends, nil
}

// Partitions returns the partitions that hold records in the offset range,
// sorted.
func (r *WindowReader) Partitions() []int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := make([]int32, 0, len(r.end))
	for p := range r.end {
		ps = append(ps, p)
	}
	slices.Sort(ps)
	return ps
}

// Pending returns the partitions not yet read to their end offset, sorted. A
// partition leaves Pending once its last record was returned by Next, so
// callers merging partitions know no older record of it will follow.
func (r *WindowReader) Pending() []int32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := make([]int32, 0, len(r.next))
	for p := range r.next {
		ps = append(ps, p)
	}
	slices.Sort(ps)
	return ps
}

// Next returns the next record in the window, or io.EOF once every partition
// has been read to its end offset. Records whose timestamps fall outside the
// window are skipped. A record that is not a valid capture is returned with
// DecodeErr set.
func (r *WindowReader) Next(ctx context.Context) (Message, error) {
	ctx = orBackground(ctx)
	for {
		if err := ctx.Err(); err != nil {
			return Message{}, err
		}
		if r.closed.Load() {
			return Message{}, errConsumerClosed
		}
		rec, finished, ok := r.take()
		if len(finished) > 0 && r.client != nil {
			// A finished partition would otherwise keep streaming records
			// past the window until every partition is done.
			r.client.PauseFetchPartitions(map[string][]int32{r.topic: finished})
		}
		if ok {
			if r.inWindow(rec.Timestamp) {
				return newMessage(rec), nil
			}
			continue
		}
		if r.done() {
			return Message{}, io.EOF
		}
		if err := r.poll(ctx); err != nil {
			return Message{}, err
		}
	}
}

// take returns the next buffered record that is within its partition's end
// offset, advancing that partition, together with the partitions that
// reached their end meanwhile.
func (r *WindowReader) take() (rec *kgo.Record, finished []int32, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for r.bufNext < len(r.buf) {
		rec := r.buf[r.bufNext]
		r.buf[r.bufNext] = nil
		r.bufNext++
		next, pending := r.next[rec.Partition]
		if !pending || rec.Offset < next {
			continue // already done with this partition
		}
		end := r.end[rec.Partition]
		if rec.Offset >= end {
			// Records arrive in offset order, so nothing below the end can
			// follow: the partition is done even when offsets below the end
			// were never delivered (control batches, compaction).
			delete(r.next, rec.Partition)
			finished = append(finished, rec.Partition)
			continue
		}
		if rec.Offset+1 >= end {
			delete(r.next, rec.Partition)
			finished = append(finished, rec.Partition)
		} else {
			r.next[rec.Partition] = rec.Offset + 1
		}
		return rec, finished, true
	}
	r.buf, r.bufNext = r.buf[:0], 0
	return nil, finished, false
}

func (r *WindowReader) done() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.next) == 0
}

func (r *WindowReader) inWindow(ts time.Time) bool {
	return (r.from.IsZero() || !ts.Before(r.from)) && (r.to.IsZero() || ts.Before(r.to))
}

func (r *WindowReader) poll(ctx context.Context) error {
	fetches := r.client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return errConsumerClosed
	}
	var errs []error
	r.mu.Lock()
	// The tap's producer is idempotent but not transactional and the topic
	// is not compacted, so every offset up to the end is a record. A topic
	// with gaps (control batches, compaction) can leave a partition pending;
	// callers bound Next with a context for that case.
	fetches.EachRecord(func(rec *kgo.Record) { r.buf = append(r.buf, rec) })
	r.mu.Unlock()
	fetches.EachError(func(topic string, partition int32, err error) {
		switch {
		case ctx.Err() != nil && errors.Is(err, ctx.Err()):
		case topic == "":
			errs = append(errs, fmt.Errorf("kafka: fetch: %w", err))
		default:
			errs = append(errs, fmt.Errorf("kafka: fetch %s partition %d: %w", topic, partition, err))
		}
	})
	return errors.Join(errs...)
}

// Close stops the reader and releases its connections. It is idempotent.
func (r *WindowReader) Close() {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		if r.client != nil {
			r.client.Close()
		}
	})
}
