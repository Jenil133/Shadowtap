package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWriter(t *testing.T) {
	const topic = "shadowtap.diffs.v1"
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	cfg := DefaultConfig()
	cfg.Brokers, cfg.Topic = c.ListenAddrs(), topic
	w, err := NewWriter(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ts := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, k := range []string{"a", "b", "c"} {
		if err := w.Write(ctx, []byte(k), []byte(`{"k":"`+k+`"}`), map[string]string{"class": "match"}, ts); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var got []*kgo.Record
	for len(got) < 3 {
		f := cl.PollFetches(ctx)
		if err := f.Err0(); err != nil {
			t.Fatal(err)
		}
		got = append(got, f.Records()...)
	}
	for _, r := range got {
		if !r.Timestamp.Equal(ts) || len(r.Headers) != 1 || r.Headers[0].Key != "class" {
			t.Errorf("record %s: ts %v headers %v", r.Key, r.Timestamp, r.Headers)
		}
	}

	bad := cfg
	bad.Topic = "bad topic"
	if _, err := NewWriter(bad); err == nil {
		t.Error("invalid topic accepted")
	}
}
