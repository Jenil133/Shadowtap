package kafka

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

func TestParseStart(t *testing.T) {
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	after := func(t time.Time) kgo.Offset { return kgo.NewOffset().AfterMilli(t.UnixMilli()) }
	tests := []struct {
		in      string
		want    kgo.Offset
		wantErr bool
	}{
		{in: "earliest", want: kgo.NewOffset().AtStart()},
		{in: "latest", want: kgo.NewOffset().AtEnd()},
		{in: " Earliest ", want: kgo.NewOffset().AtStart()},
		{in: "LATEST", want: kgo.NewOffset().AtEnd()},
		{in: "2026-10-03T11:00:00Z", want: after(now.Add(-time.Hour))},
		{in: "2026-10-03T13:00:00+02:00", want: after(now.Add(-time.Hour))},
		{in: "2026-10-03T11:00:00.250Z", want: after(now.Add(-time.Hour + 250*time.Millisecond))},
		{in: "2026-10-03T11:00:00.123456789Z", want: after(now.Add(-time.Hour + 123*time.Millisecond))},
		{in: "2030-01-01T00:00:00Z", want: after(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC))},
		{in: "1960-01-01T00:00:00Z", want: kgo.NewOffset().AfterMilli(0)},
		{in: "-15m", want: after(now.Add(-15 * time.Minute))},
		{in: "-2h", want: after(now.Add(-2 * time.Hour))},
		{in: "-1h30m", want: after(now.Add(-90 * time.Minute))},
		{in: "-1500ms", want: after(now.Add(-1500 * time.Millisecond))},
		{in: "", wantErr: true},
		{in: "   ", wantErr: true},
		{in: "15m", wantErr: true},
		{in: "+15m", wantErr: true},
		{in: "-0s", wantErr: true},
		{in: "-", wantErr: true},
		{in: "-15", wantErr: true},
		{in: "-fifteen minutes", wantErr: true},
		{in: "now", wantErr: true},
		{in: "yesterday", wantErr: true},
		{in: "earliest-1", wantErr: true},
		{in: "2026-10-03", wantErr: true},
		{in: "2026-10-03 11:00:00Z", wantErr: true},
		{in: "2026-10-03T11:00:00", wantErr: true},
		{in: "1759489200", wantErr: true},
	}
	for _, tt := range tests {
		got, err := ParseStart(tt.in, now)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseStart(%q) = %v, want an error", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseStart(%q) error: %v", tt.in, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseStart(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func FuzzParseStart(f *testing.F) {
	for _, s := range []string{"earliest", "LATEST", "-15m", "-1h30m", "2026-10-03T11:00:00Z", "2026-10-03T11:00:00.5+02:00", "", "-", "+1h", "-0s"} {
		f.Add(s)
	}
	now := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, s string) {
		off, err := ParseStart(s, now)
		if err != nil {
			return
		}
		// Every accepted input is a keyword, a negative duration or an
		// RFC 3339 time, and never maps below franz-go's special offsets.
		v := strings.TrimSpace(s)
		switch lower := strings.ToLower(v); {
		case lower == "earliest" || lower == "latest":
		case strings.HasPrefix(v, "-"):
			if d, err := time.ParseDuration(v); err != nil || d >= 0 {
				t.Fatalf("accepted %q, which is not a negative duration", s)
			}
		default:
			if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
				t.Fatalf("accepted %q, which is not an RFC 3339 time: %v", s, err)
			}
		}
		if at := off.EpochOffset().Offset; at < -2 {
			t.Fatalf("ParseStart(%q) = %v, below the start-of-log offset", s, off)
		}
	})
}

// partitionsFetched closes the returned channel once the cluster has received
// fetch requests covering every partition of the topic, which means the
// consumer has resolved its start offsets.
func partitionsFetched(c *kfake.Cluster) <-chan struct{} {
	done := make(chan struct{})
	var (
		mu   sync.Mutex
		seen = make(map[int32]bool)
	)
	c.ControlKey(int16(kmsg.Fetch), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if fetch, ok := req.(*kmsg.FetchRequest); ok {
			mu.Lock()
			for _, topic := range fetch.Topics {
				for _, p := range topic.Partitions {
					seen[p.Partition] = true
				}
			}
			if len(seen) == testPartitions {
				select {
				case <-done:
				default:
					close(done)
				}
			}
			mu.Unlock()
		}
		return nil, nil, false
	})
	return done
}

// traceIDs returns the trace ids of msgs, sorted.
func traceIDs(t *testing.T, msgs []Message) []string {
	t.Helper()
	ids := make([]string, 0, len(msgs))
	for _, m := range msgs {
		if m.DecodeErr != nil {
			t.Fatalf("DecodeErr at partition %d offset %d: %v", m.Partition, m.Offset, m.DecodeErr)
		}
		ids = append(ids, m.Exchange.TraceID)
	}
	slices.Sort(ids)
	return ids
}

func TestConsumerStartOffsets(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	base := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	var xs []*capture.Exchange
	for i := range 6 {
		xs = append(xs, testExchange(traceID(i), base.Add(time.Duration(i)*time.Minute)))
	}
	publishAll(t, cfg, xs...)
	ids := func(is ...int) []string {
		out := make([]string, 0, len(is))
		for _, i := range is {
			out = append(out, traceID(i))
		}
		return out
	}

	tests := []struct {
		name  string
		start string
		want  []string
	}{
		{"earliest", "earliest", ids(0, 1, 2, 3, 4, 5)},
		{"at a record's timestamp", base.Add(3 * time.Minute).Format(time.RFC3339), ids(3, 4, 5)},
		{"between records", base.Add(150 * time.Second).Format(time.RFC3339Nano), ids(3, 4, 5)},
		{"after every record", base.Add(time.Hour).Format(time.RFC3339), nil},
		{"latest", "latest", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestConsumer(t, cfg, tt.start)
			got := traceIDs(t, readN(t, c, len(tt.want)))
			expectNoMore(t, c, 300*time.Millisecond)
			if !slices.Equal(got, tt.want) {
				t.Errorf("consumed %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConsumerRelativeStart(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	// Minutes away from the -15m cutoff, so test timing cannot matter.
	now := time.Now().UTC().Round(0)
	ages := []time.Duration{40 * time.Minute, 20 * time.Minute, 10 * time.Minute, time.Minute}
	var xs []*capture.Exchange
	for i, age := range ages {
		xs = append(xs, testExchange(traceID(i), now.Add(-age)))
	}
	publishAll(t, cfg, xs...)

	c := newTestConsumer(t, cfg, "-15m")
	got := traceIDs(t, readN(t, c, 2))
	expectNoMore(t, c, 300*time.Millisecond)
	if want := []string{traceID(2), traceID(3)}; !slices.Equal(got, want) {
		t.Errorf("consumed %v, want %v", got, want)
	}
}

func TestConsumerLatestSkipsExistingRecords(t *testing.T) {
	for _, start := range []string{"latest", ""} {
		t.Run("start "+strconv.Quote(start), func(t *testing.T) {
			cluster := newCluster(t)
			fetched := partitionsFetched(cluster)
			cfg := testConfig(cluster)
			started := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
			publishAll(t, cfg, testExchange(traceID(0), started), testExchange(traceID(1), started))

			c := newTestConsumer(t, cfg, start)
			// "latest" is resolved when the consumer first loads the
			// partitions; records produced before that would be skipped too.
			select {
			case <-fetched:
			case <-time.After(10 * time.Second):
				t.Fatal("consumer never fetched every partition")
			}
			publishAll(t, cfg, testExchange(traceID(2), started), testExchange(traceID(3), started))

			got := traceIDs(t, readN(t, c, 2))
			expectNoMore(t, c, 200*time.Millisecond)
			if want := []string{traceID(2), traceID(3)}; !slices.Equal(got, want) {
				t.Errorf("consumed %v, want only the records produced after starting %v", got, want)
			}
		})
	}
}

func TestConsumerDecodeErrorsAndOrder(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)

	valid, err := capture.Encode(testExchange(traceID(1), time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	values := [][]byte{
		valid,
		[]byte("not json"),
		[]byte(`{"schema_version":2,"id":"from-the-future"}`),
		valid,
		{},
		[]byte(`{"schema_version":1}`), // decodes but is not a capture
	}
	raw, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	records := make([]*kgo.Record, len(values))
	for i, v := range values {
		records[i] = &kgo.Record{
			Key:       []byte{byte('a' + i)},
			Value:     v,
			Partition: 1,
			Headers:   []kgo.RecordHeader{{Key: "dup", Value: []byte("first")}, {Key: "dup", Value: []byte("second")}},
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := raw.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	c := newTestConsumer(t, cfg, "earliest")
	msgs := readN(t, c, len(values))
	expectNoMore(t, c, 200*time.Millisecond)
	for i, m := range msgs {
		if m.Partition != 1 || m.Offset != int64(i) || string(m.Key) != string(rune('a'+i)) {
			t.Errorf("message %d is partition %d offset %d key %q, want partition 1 offset %d key %q",
				i, m.Partition, m.Offset, m.Key, i, string(rune('a'+i)))
		}
		if m.Headers["dup"] != "first" {
			t.Errorf("message %d header dup = %q, want the first value", i, m.Headers["dup"])
		}
		if string(m.Raw) != string(values[i]) {
			t.Errorf("message %d raw = %q, want %q", i, m.Raw, values[i])
		}
		wantValid := i == 0 || i == 3
		if gotValid := m.DecodeErr == nil && m.Exchange != nil; gotValid != wantValid {
			t.Errorf("message %d decoded: %v (DecodeErr %v), want %v", i, gotValid, m.DecodeErr, wantValid)
		}
		if !wantValid && m.Exchange != nil {
			t.Errorf("message %d has an exchange despite DecodeErr", i)
		}
	}
	if !errors.Is(msgs[2].DecodeErr, capture.ErrUnsupportedSchema) {
		t.Errorf("schema 2 record DecodeErr = %v, want capture.ErrUnsupportedSchema", msgs[2].DecodeErr)
	}
}

func TestConsumerFetchErrors(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	publishAll(t, cfg, testExchange(traceID(1), time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)))
	cluster.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Fetch},
		Topic: testTopic,
		Err:   kerr.TopicAuthorizationFailed,
		Count: 1,
	})

	c := newTestConsumer(t, cfg, "earliest")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.Next(ctx)
	if !errors.Is(err, kerr.TopicAuthorizationFailed) {
		t.Fatalf("Next = %v, want TOPIC_AUTHORIZATION_FAILED", err)
	}
	// The consumer keeps going once the error is reported.
	m, err := c.Next(ctx)
	if err != nil {
		t.Fatalf("Next after a fetch error: %v", err)
	}
	if m.DecodeErr != nil || m.Exchange.TraceID != traceID(1) {
		t.Errorf("Next after a fetch error = %+v", m)
	}
}

func TestConsumerContextAndClose(t *testing.T) {
	cluster := newCluster(t)
	cfg := testConfig(cluster)
	c := newTestConsumer(t, cfg, "latest")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Next(cancelled); err != context.Canceled {
		t.Errorf("Next with a cancelled context = %v, want context.Canceled", err)
	}
	short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelShort()
	if _, err := c.Next(short); err != context.DeadlineExceeded {
		t.Errorf("Next past its deadline = %v, want context.DeadlineExceeded", err)
	}

	// Close unblocks a Next that is waiting for records.
	errc := make(chan error, 1)
	go func() {
		_, err := c.Next(context.Background())
		errc <- err
	}()
	c.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, kgo.ErrClientClosed) {
			t.Errorf("blocked Next after Close = %v, want kgo.ErrClientClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not unblock Next")
	}
	if _, err := c.Next(context.Background()); !errors.Is(err, kgo.ErrClientClosed) {
		t.Errorf("Next after Close = %v, want kgo.ErrClientClosed", err)
	}
	c.Close() // idempotent
}

func TestNewConsumerValidation(t *testing.T) {
	brokers := []string{"127.0.0.1:1"} // never dialled
	tests := []struct {
		name    string
		cfg     Config
		start   string
		wantErr string
	}{
		{"minimal config", Config{Brokers: brokers, Topic: testTopic}, "earliest", ""},
		{"empty start", Config{Brokers: brokers, Topic: testTopic}, "", ""},
		{"no topic", Config{Brokers: brokers}, "earliest", "topic is empty"},
		{"no brokers", Config{Topic: testTopic}, "earliest", "brokers is empty"},
		{"bad start", Config{Brokers: brokers, Topic: testTopic}, "tomorrow", `start "tomorrow"`},
		{"bad sasl", Config{Brokers: brokers, Topic: testTopic, SASL: SASLConfig{Mechanism: "PLAIN"}}, "earliest", "sasl.username"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := NewConsumer(tt.cfg, ConsumeOptions{Start: tt.start})
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("NewConsumer: %v", err)
				}
				c.Close()
				return
			}
			if err == nil {
				c.Close()
				t.Fatalf("NewConsumer succeeded, want an error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("NewConsumer error = %q, want it to contain %q", err, tt.wantErr)
			}
		})
	}
}
