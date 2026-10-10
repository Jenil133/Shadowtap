package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/kafka"
)

var base = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

func exchangeAt(i int, at time.Duration) *capture.Exchange {
	return &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            fmt.Sprintf("id-%04d", i),
		Service:       "demo-api",
		TraceID:       fmt.Sprintf("4bf92f3577b34da6a3ce929d%08x", i),
		Source:        capture.Source{Tap: "tap-0", Mode: capture.ModeObserve},
		Timing:        capture.Timing{StartedAt: base.Add(at)},
		Request:       capture.Request{Method: "GET", Path: "/api/users/1"},
		Response:      capture.Response{Status: 200},
	}
}

// fakeReader replays a fixed sequence of (partition, exchange) messages and
// tracks which partitions are still pending: a partition is done after its
// last message.
type fakeReader struct {
	msgs []kafka.Message
	i    int
}

func (f *fakeReader) Next(context.Context) (kafka.Message, error) {
	if f.i == len(f.msgs) {
		return kafka.Message{}, io.EOF
	}
	m := f.msgs[f.i]
	f.i++
	return m, nil
}

func (f *fakeReader) Pending() []int32 {
	var ps []int32
	for _, m := range f.msgs[f.i:] {
		if !slices.Contains(ps, m.Partition) {
			ps = append(ps, m.Partition)
		}
	}
	return ps
}

func (f *fakeReader) Close() {}

func msg(p int32, x *capture.Exchange) kafka.Message {
	return kafka.Message{Partition: p, Exchange: x, Timestamp: x.Timing.StartedAt}
}

func drain(t *testing.T, s Source) []*capture.Exchange {
	t.Helper()
	var out []*capture.Exchange
	for {
		x, err := s.Next(context.Background())
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, x)
	}
}

func ids(xs []*capture.Exchange) []string {
	out := make([]string, len(xs))
	for i, x := range xs {
		out[i] = x.ID
	}
	return out
}

func TestMergeOrdersPartitions(t *testing.T) {
	// Partition 0 holds even seconds, partition 1 odd ones; fetches arrive
	// a partition at a time, and partition 0 has one record written late.
	var p0, p1 []kafka.Message
	for i := range 20 {
		x := exchangeAt(i, time.Duration(i)*time.Second)
		if i%2 == 0 {
			p0 = append(p0, msg(0, x))
		} else {
			p1 = append(p1, msg(1, x))
		}
	}
	p0[3], p0[4] = p0[4], p0[3] // 6s written after 8s
	var seq []kafka.Message
	for i := 0; i < 10; i += 5 {
		seq = append(seq, p0[i:i+5]...)
		seq = append(seq, p1[i:i+5]...)
	}
	m := newMerge(&fakeReader{msgs: seq}, 3*time.Second, 0)
	got := drain(t, m)
	if len(got) != 20 {
		t.Fatalf("got %d captures", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timing.StartedAt.Before(got[i-1].Timing.StartedAt) {
			t.Fatalf("out of order at %d: %v", i, ids(got))
		}
	}
	if s := m.Stats(); s.Read != 20 || s.Disordered != 0 || s.Forced != 0 {
		t.Errorf("stats = %+v", s)
	}
}

func TestMergeBoundsItsBuffer(t *testing.T) {
	// Partition 1 sends nothing until the end, so the watermark is unknown
	// and only the buffer limit lets captures out.
	var seq []kafka.Message
	for i := range 10 {
		seq = append(seq, msg(0, exchangeAt(i, time.Duration(i)*time.Second)))
	}
	seq = append(seq, msg(1, exchangeAt(10, 0)))
	m := newMerge(&fakeReader{msgs: seq}, time.Second, 4)
	got := drain(t, m)
	if len(got) != 11 {
		t.Fatalf("got %d", len(got))
	}
	if s := m.Stats(); s.Forced == 0 || s.Disordered == 0 {
		t.Errorf("stats = %+v, want forced and disordered emissions", s)
	}
}

func TestMergeCountsUndecodable(t *testing.T) {
	seq := []kafka.Message{
		msg(0, exchangeAt(0, 0)),
		{Partition: 0, DecodeErr: errors.New("bad"), Timestamp: base},
		msg(0, exchangeAt(1, time.Second)),
	}
	m := newMerge(&fakeReader{msgs: seq}, 0, 0)
	if got := drain(t, m); len(got) != 2 {
		t.Fatalf("got %d", len(got))
	}
	if s := m.Stats(); s.Read != 3 || s.Undecodable != 1 {
		t.Errorf("stats = %+v", s)
	}
}

func TestKafkaSource(t *testing.T) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(4, capture.DefaultTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cfg := kafka.DefaultConfig()
	cfg.Brokers = c.ListenAddrs()
	cfg.LingerMS = 0
	p, err := kafka.NewProducer(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		x := exchangeAt(i, time.Duration(i)*250*time.Millisecond)
		x.ID = capture.NewID()
		if err := p.Publish(context.Background(), x); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}

	s, err := NewKafka(ctx, cfg, KafkaOptions{Window: kafka.WindowOptions{From: base.Add(2 * time.Second), To: base.Add(7 * time.Second)}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got := drain(t, s)
	if len(got) != 20 { // 2s..6.75s at 4 per second
		t.Fatalf("got %d captures, want 20", len(got))
	}
	for i := 1; i < len(got); i++ {
		if got[i].Timing.StartedAt.Before(got[i-1].Timing.StartedAt) {
			t.Fatal("captures out of request-start order")
		}
	}
}

func TestFileSource(t *testing.T) {
	var lines []string
	for _, i := range []int{3, 1, 2, 0} {
		b, err := capture.Encode(exchangeAt(i, time.Duration(i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		lines = append(lines, string(b))
	}
	lines = append(lines, "", "not json", `{"schema_version":2}`)
	s, err := NewFile(strings.NewReader(strings.Join(lines, "\n")), FileOptions{From: base.Add(time.Second)})
	if err != nil {
		t.Fatal(err)
	}
	got := ids(drain(t, s))
	if want := []string{"id-0001", "id-0002", "id-0003"}; !slices.Equal(got, want) {
		t.Errorf("order %v, want %v", got, want)
	}
	if st := s.Stats(); st.Read != 6 || st.Undecodable != 2 {
		t.Errorf("stats = %+v", st)
	}
}

func TestFilter(t *testing.T) {
	get := exchangeAt(0, 0)
	post := exchangeAt(1, 0)
	post.Request.Method = "POST"
	post.Request.Body = capture.Body{Size: 10}
	post.Request.Body.SetData([]byte(`{"a":1234}`))
	cut := exchangeAt(2, 0)
	cut.Request.Method = "POST"
	cut.Request.Body = capture.Body{Size: 5000, Truncated: true}
	omitted := exchangeAt(3, 0)
	omitted.Request.Method, omitted.Request.Body = "PUT", capture.Body{Size: 10, Omitted: capture.OmittedBinary}
	other := exchangeAt(4, 0)
	other.Service, other.Response.Status, other.Request.Path = "orders", 503, "/api/orders/7?x=1"

	tests := []struct {
		name string
		f    Filter
		x    *capture.Exchange
		want string
	}{
		{"default replays GET", Filter{}, get, ""},
		{"default skips POST", Filter{}, post, SkipMutating},
		{"POST allowed", Filter{Methods: []string{"get", "post"}, AllowMutating: true}, post, ""},
		{"allow mutating without a list replays every method", Filter{AllowMutating: true}, post, ""},
		{"allow mutating without a list still needs the body", Filter{AllowMutating: true}, cut, SkipBodyUnavailable},
		{"allow mutating with a list keeps the list", Filter{Methods: []string{"GET"}, AllowMutating: true}, post, SkipMethod},
		{"method not listed", Filter{Methods: []string{"HEAD"}}, get, SkipMethod},
		{"truncated body", Filter{Methods: []string{"POST"}, AllowMutating: true}, cut, SkipBodyUnavailable},
		{"omitted body", Filter{Methods: []string{"PUT"}, AllowMutating: true}, omitted, SkipBodyUnavailable},
		{"service", Filter{Services: []string{"demo-api"}}, other, SkipService},
		{"path regex ignores query", Filter{PathRegex: regexp.MustCompile(`^/api/orders/\d+$`)}, other, ""},
		{"path regex", Filter{PathRegex: regexp.MustCompile(`^/api/orders`)}, get, SkipPath},
		{"status class", Filter{Status: []string{"5xx"}}, other, ""},
		{"status code", Filter{Status: []string{"201", "4xx"}}, get, SkipStatus},
	}
	for _, tt := range tests {
		if got := tt.f.Skip(tt.x); got != tt.want {
			t.Errorf("%s: Skip = %q, want %q", tt.name, got, tt.want)
		}
	}
	if err := (Filter{Methods: []string{"POST"}}).Validate(); err == nil {
		t.Error("POST without AllowMutating validated")
	}
	if err := (Filter{Status: []string{"6xx"}}).Validate(); err == nil {
		t.Error("bad status validated")
	}
	if err := (Filter{Methods: []string{"GET", "OPTIONS"}, Status: []string{"2xx", "404"}}).Validate(); err != nil {
		t.Error(err)
	}
	if err := (Filter{AllowMutating: true}).Validate(); err != nil {
		t.Error(err)
	}
}

// TestKafkaSourceMergesSkewedPartitions places records in partitions by
// hand, each partition's log out of request-start order within a bounded
// skew (a later request finished first), and expects one ordered stream.
func TestKafkaSourceMergesSkewedPartitions(t *testing.T) {
	const partitions, n = 3, 60
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, capture.DefaultTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.DefaultProduceTopic(capture.DefaultTopic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	recs := make([]*kgo.Record, 0, n)
	for i := range n {
		x := exchangeAt(i, time.Duration(i)*100*time.Millisecond)
		b, err := capture.Encode(x)
		if err != nil {
			t.Fatal(err)
		}
		recs = append(recs, &kgo.Record{Key: []byte(x.TraceID), Value: b, Timestamp: x.Timing.StartedAt, Partition: int32(i % partitions)})
	}
	// Every other pair of neighbours within a partition is written in the
	// wrong order: 300 ms of skew, within the reorder slack.
	for i := 0; i+partitions < n; i += 2 * partitions {
		recs[i], recs[i+partitions] = recs[i+partitions], recs[i]
	}
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}

	cfg := kafka.DefaultConfig()
	cfg.Brokers = c.ListenAddrs()
	s, err := NewKafka(ctx, cfg, KafkaOptions{
		Window: kafka.WindowOptions{From: base.Add(time.Second), To: base.Add(5 * time.Second)},
		Slack:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got := drain(t, s)
	if len(got) != 40 { // 1.0s..4.9s at 10 per second
		t.Fatalf("got %d captures, want 40", len(got))
	}
	for i, x := range got {
		if want := base.Add(time.Second + time.Duration(i)*100*time.Millisecond); !x.Timing.StartedAt.Equal(want) {
			t.Fatalf("capture %d started at %v, want %v: %v", i, x.Timing.StartedAt, want, ids(got))
		}
	}
	if st := s.Stats(); st.Read != 40 || st.Disordered != 0 || st.Forced != 0 || st.Undecodable != 0 {
		t.Errorf("stats = %+v", st)
	}
}
