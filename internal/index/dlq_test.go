package index

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDLQRecord(t *testing.T) {
	now := time.Date(2026, 10, 3, 9, 30, 0, 0, time.UTC)
	rec := &kgo.Record{Topic: "shadowtap.captures.v1", Partition: 1, Offset: 42, Key: []byte(fixtureTraceHex), Value: []byte("not json"), Timestamp: fixtureStart}
	src := sourceOf(rec)
	src.Headers = map[string]string{"trace-id": fixtureTraceHex, "content-type": "application/json"}

	t.Run("decode failure keeps the raw value", func(t *testing.T) {
		f := failure{rec: rec, src: src, stage: StageDecode, err: errors.New("capture: decode: invalid character"), value: true}
		b, err := f.dlqRecord("indexer-0", now, 0)
		if err != nil {
			t.Fatal(err)
		}
		var r DLQRecord
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		if r.SchemaVersion != DLQSchemaVersion || r.Indexer != "indexer-0" || !r.FailedAt.Equal(now) ||
			r.Topic != rec.Topic || r.Partition != 1 || r.Offset != 42 || !r.Timestamp.Equal(fixtureStart) ||
			r.Key != fixtureTraceHex || r.TraceID != fixtureTraceHex || r.Stage != StageDecode ||
			r.Error != "capture: decode: invalid character" || string(r.Value) != "not json" || r.Document != nil || r.Truncated {
			t.Errorf("record = %+v", r)
		}
		if r.Headers["content-type"] != "application/json" {
			t.Errorf("headers = %v", r.Headers)
		}
		h := f.headers()
		if h[HeaderSourceTopic] != rec.Topic || h[HeaderSourcePartition] != "1" || h[HeaderSourceOffset] != "42" || h[HeaderStage] != StageDecode || h[HeaderContentType] != "application/json" {
			t.Errorf("headers = %v", h)
		}
	})
	t.Run("index failure keeps the document", func(t *testing.T) {
		f := failure{
			rec: rec, src: src, stage: StageIndex,
			err:    errors.New("elasticsearch rejected the document with status 400: mapper_parsing_exception"),
			status: 400, errType: "mapper_parsing_exception", attempts: 1,
			index: DefaultCapturesIndex, docID: fixtureCaptureID, body: []byte(`{"capture":{"id":"x"}}`),
		}
		b, err := f.dlqRecord("indexer-0", now, 0)
		if err != nil {
			t.Fatal(err)
		}
		var r DLQRecord
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		if r.Stage != StageIndex || r.Status != 400 || r.ErrorType != "mapper_parsing_exception" || r.Attempts != 1 ||
			r.Index != DefaultCapturesIndex || r.DocumentID != fixtureCaptureID || string(r.Document) != `{"capture":{"id":"x"}}` || r.Value != nil {
			t.Errorf("record = %+v", r)
		}
	})
	t.Run("payload dropped to fit the budget", func(t *testing.T) {
		f := failure{rec: rec, src: src, stage: StageIndex, err: errors.New("x"), body: []byte(`{"big":"` + strings.Repeat("x", 2000) + `"}`)}
		b, err := f.dlqRecord("indexer-0", now, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if len(b) > 1000 {
			t.Errorf("record is %d bytes, over the budget", len(b))
		}
		var r DLQRecord
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatal(err)
		}
		if !r.Truncated || r.Document != nil || r.Error != "x" || r.Offset != 42 {
			t.Errorf("record = %+v", r)
		}
	})
	t.Run("binary key is base64", func(t *testing.T) {
		bin := &kgo.Record{Topic: "t", Key: []byte{0xff, 0xfe}}
		f := failure{rec: bin, src: sourceOf(bin), stage: StageDecode, err: errors.New("x")}
		b, err := f.dlqRecord("", now, 0)
		if err != nil {
			t.Fatal(err)
		}
		var r DLQRecord
		_ = json.Unmarshal(b, &r)
		if r.Key != "" || r.KeyBase64 != "//4=" || r.Indexer != "" || r.TraceID != "" {
			t.Errorf("record = %+v", r)
		}
	})
	t.Run("budget leaves room for key, headers and framing", func(t *testing.T) {
		f := failure{rec: rec, src: src, stage: StageDecode}
		if got := f.valueBudget(0); got != 0 {
			t.Errorf("no limit budget = %d", got)
		}
		budget := f.valueBudget(900_000)
		if budget >= 900_000-len(rec.Key)-recordFraming || budget <= 800_000 {
			t.Errorf("budget = %d", budget)
		}
		if got := f.valueBudget(10); got != 1 {
			t.Errorf("tiny limit budget = %d, want 1", got)
		}
	})
}
