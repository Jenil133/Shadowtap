package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Writer produces arbitrary records to one topic and reports delivery
// failures on Flush. Unlike Producer, which must never block the tap,
// Writer applies back-pressure (Write waits for buffer space): it serves
// batch tools such as the replayer, where every record must arrive.
type Writer struct {
	client *kgo.Client

	mu     sync.Mutex
	failed int
	first  error
}

// NewWriter creates a writer for cfg.Topic. Only the connection settings,
// the topic, compression and the record size limit of cfg are used.
func NewWriter(cfg Config, extra ...kgo.Opt) (*Writer, error) {
	if err := configError(append(cfg.connectionProblems(), topicProblems(cfg.Topic)...)); err != nil {
		return nil, err
	}
	opts, err := ClientOptions(cfg)
	if err != nil {
		return nil, err
	}
	codecs := []kgo.CompressionCodec{kgo.NoCompression()}
	if codec, ok := compressionCodec(cfg.Compression); ok && codec != kgo.NoCompression() {
		codecs = append([]kgo.CompressionCodec{codec}, codecs...)
	}
	opts = append(opts,
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ProducerBatchCompression(codecs...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5*time.Millisecond),
	)
	if cfg.MaxRecordBytes > 0 {
		opts = append(opts, kgo.ProducerBatchMaxBytes(int32(cfg.MaxRecordBytes)))
	}
	client, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: create writer: %w", err)
	}
	return &Writer{client: client}, nil
}

// Write buffers one record. It blocks while the buffer is full or until ctx
// is done.
func (w *Writer) Write(ctx context.Context, key, value []byte, headers map[string]string, ts time.Time) error {
	r := &kgo.Record{Key: key, Value: value, Timestamp: ts}
	for k, v := range headers {
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	w.client.Produce(ctx, r, func(_ *kgo.Record, err error) {
		if err == nil {
			return
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		w.failed++
		if w.first == nil {
			w.first = err
		}
	})
	return nil
}

// Flush waits until every buffered record is delivered and reports how many
// failed since the last Flush.
func (w *Writer) Flush(ctx context.Context) error {
	if err := w.client.Flush(ctx); err != nil {
		return fmt.Errorf("kafka: flush: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed > 0 {
		err := fmt.Errorf("kafka: %d records not delivered: %w", w.failed, w.first)
		w.failed, w.first = 0, nil
		return err
	}
	return nil
}

// Close flushes and closes the writer.
func (w *Writer) Close(ctx context.Context) error {
	err := w.Flush(ctx)
	w.client.Close()
	return err
}
