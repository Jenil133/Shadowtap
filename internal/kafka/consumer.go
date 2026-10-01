package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// errConsumerClosed is returned by Next after Close.
var errConsumerClosed = fmt.Errorf("kafka: consumer closed: %w", kgo.ErrClientClosed)

// ParseStart converts a start position into a franz-go offset: "earliest",
// "latest", an RFC 3339 timestamp (fractional seconds allowed), or a negative
// duration relative to now such as "-15m" or "-2h". A time starts each
// partition at its first record stamped at or after that time; records are
// stamped with the request start. Keywords are case-insensitive.
func ParseStart(s string, now time.Time) (kgo.Offset, error) {
	v := strings.TrimSpace(s)
	switch strings.ToLower(v) {
	case "earliest":
		return kgo.NewOffset().AtStart(), nil
	case "latest":
		return kgo.NewOffset().AtEnd(), nil
	}
	if strings.HasPrefix(v, "-") {
		d, err := time.ParseDuration(v)
		if err != nil {
			return kgo.Offset{}, fmt.Errorf("kafka: start %q: %w", s, err)
		}
		if d >= 0 {
			return kgo.Offset{}, fmt.Errorf("kafka: start %q: relative start must be before now", s)
		}
		return afterTime(now.Add(d)), nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return kgo.Offset{}, fmt.Errorf("kafka: start %q: want earliest, latest, an RFC 3339 time or a negative duration such as -15m", s)
	}
	return afterTime(t), nil
}

// afterTime starts at the first record stamped at or after t. Times before
// the Unix epoch are clamped to it: the milliseconds go to the broker as the
// ListOffsets timestamp, where -1 and -2 mean latest and earliest and other
// negative values are rejected.
func afterTime(t time.Time) kgo.Offset {
	return kgo.NewOffset().AfterMilli(max(t.UnixMilli(), 0))
}

// ConsumeOptions controls where a Consumer starts.
type ConsumeOptions struct {
	// Start is where every partition starts, in any form ParseStart accepts.
	// Empty means "latest".
	Start string
}

// Message is one record read by a Consumer.
type Message struct {
	// Exchange is the decoded capture; nil when DecodeErr is set.
	Exchange *capture.Exchange
	// Raw is the record value as stored in Kafka.
	Raw []byte
	// Key is the record key: trace id, request id or capture id.
	Key       []byte
	Partition int32
	Offset    int64
	// Timestamp is the record timestamp, the request start to the
	// millisecond for records written by Producer.
	Timestamp time.Time
	// Headers holds the record headers; for a repeated key the first value
	// wins.
	Headers map[string]string
	// DecodeErr is why Raw is not a valid capture: it does not decode or
	// fails capture.Exchange.Validate.
	DecodeErr error
}

// Consumer reads captures from every partition of a topic. It does not join
// a consumer group, so it commits nothing and never affects other readers.
// Next must not be called concurrently; Close may be called from another
// goroutine to stop a blocked Next.
type Consumer struct {
	client *kgo.Client

	// buf holds fetched records not yet returned, partition by partition in
	// offset order.
	buf  []*kgo.Record
	next int

	closed    atomic.Bool
	closeOnce sync.Once
}

// NewConsumer creates a consumer of cfg.Topic that starts every partition at
// opts.Start. Only the connection settings and the topic of cfg are used and
// validated. The extra options are applied last.
func NewConsumer(cfg Config, opts ConsumeOptions, extra ...kgo.Opt) (*Consumer, error) {
	if err := configError(append(cfg.connectionProblems(), topicProblems(cfg.Topic)...)); err != nil {
		return nil, err
	}
	start := opts.Start
	if strings.TrimSpace(start) == "" {
		start = "latest"
	}
	offset, err := ParseStart(start, time.Now())
	if err != nil {
		return nil, err
	}
	clientOpts, err := ClientOptions(cfg)
	if err != nil {
		return nil, err
	}
	clientOpts = append(clientOpts,
		kgo.ConsumeTopics(cfg.Topic),
		// ConsumeResetOffset keeps franz-go's default (rewind a minute) for
		// broker data loss: resetting to the start position instead would
		// replay a whole log from "earliest" or skip data from "latest".
		kgo.ConsumeStartOffset(offset),
	)
	client, err := kgo.NewClient(append(clientOpts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create consumer: %w", err)
	}
	return &Consumer{client: client}, nil
}

// Next returns the next record, preserving the order of each partition.
// A record that is not a valid capture is returned with DecodeErr set rather
// than as an error. Fetch errors are returned as errors, and the consumer
// remains usable after them: records fetched alongside an error are returned
// by later calls. When ctx is done Next returns ctx.Err(), and after Close it
// returns an error wrapping kgo.ErrClientClosed.
func (c *Consumer) Next(ctx context.Context) (Message, error) {
	ctx = orBackground(ctx)
	for {
		if err := ctx.Err(); err != nil {
			return Message{}, err
		}
		if c.closed.Load() {
			return Message{}, errConsumerClosed
		}
		if c.next < len(c.buf) {
			r := c.buf[c.next]
			c.buf[c.next] = nil // let the record be collected once returned
			c.next++
			return newMessage(r), nil
		}
		c.buf, c.next = c.buf[:0], 0
		if err := c.poll(ctx); err != nil {
			return Message{}, err
		}
	}
}

// poll waits for the next fetch and buffers its records.
func (c *Consumer) poll(ctx context.Context) error {
	fetches := c.client.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return errConsumerClosed
	}
	fetches.EachRecord(func(r *kgo.Record) {
		c.buf = append(c.buf, r)
	})
	var errs []error
	fetches.EachError(func(topic string, partition int32, err error) {
		switch {
		case ctx.Err() != nil && errors.Is(err, ctx.Err()):
			// PollFetches reports the caller's own cancellation as a fetch
			// error; Next returns ctx.Err() itself.
		case topic == "":
			errs = append(errs, fmt.Errorf("kafka: fetch: %w", err))
		default:
			errs = append(errs, fmt.Errorf("kafka: fetch %s partition %d: %w", topic, partition, err))
		}
	})
	return errors.Join(errs...)
}

func newMessage(r *kgo.Record) Message {
	m := Message{
		Raw:       r.Value,
		Key:       r.Key,
		Partition: r.Partition,
		Offset:    r.Offset,
		Timestamp: r.Timestamp,
	}
	if len(r.Headers) > 0 {
		m.Headers = make(map[string]string, len(r.Headers))
		for _, h := range r.Headers {
			if _, seen := m.Headers[h.Key]; !seen {
				m.Headers[h.Key] = string(h.Value)
			}
		}
	}
	x, err := capture.Decode(r.Value)
	if err == nil {
		err = x.Validate()
	}
	if err != nil {
		m.DecodeErr = err
		return m
	}
	m.Exchange = x
	return m
}

// Close stops the consumer and releases its connections. It is idempotent.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.client.Close()
	})
}
