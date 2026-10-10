package index

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/Jenil133/Shadowtap/internal/index/indextest"
)

// newBulkWriter wires a bulk writer against es with fast retries.
func newBulkWriter(t *testing.T, es *indextest.Server, retry RetryConfig) *bulkWriter {
	t.Helper()
	client, err := NewElasticsearchClient(ESConfig{URLs: []string{es.URL()}, Compress: true})
	if err != nil {
		t.Fatal(err)
	}
	return &bulkWriter{
		es:      client,
		timeout: 2 * time.Second,
		retry:   retry,
		metrics: newMetrics(),
		log:     slog.New(slog.DiscardHandler),
		errLog:  &logLimiter{interval: time.Hour},
		tracer:  noop.NewTracerProvider().Tracer("test"),
	}
}

func fastRetry() RetryConfig {
	return RetryConfig{MaxAttempts: 3, InitialBackoffMS: 1, MaxBackoffMS: 5}
}

// docs builds n documents of index with ids d0..dn-1.
func docs(index string, n int) []*Document {
	out := make([]*Document, 0, n)
	for i := range n {
		out = append(out, &Document{Index: index, ID: "d" + string(rune('0'+i)), Body: []byte(`{"n":` + string(rune('0'+i)) + `}`), Source: testSource(nil)})
	}
	return out
}

func results(outcomes []outcome) []string {
	var out []string
	for _, o := range outcomes {
		r := o.result
		if r == "" {
			r = "failed"
		}
		out = append(out, r)
	}
	return out
}

func TestBulkIndexCreatesAndTreatsConflictsAsSuccess(t *testing.T) {
	es := newFakeES(t)
	w := newBulkWriter(t, es, fastRetry())
	ctx := context.Background()

	first, err := w.index(ctx, docs("traces-shadowtap-default", 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(results(first), ","); got != "created,created,created" {
		t.Errorf("first results = %s", got)
	}
	if es.DocCount() != 3 {
		t.Errorf("stored %d documents", es.DocCount())
	}
	// A redelivery conflicts on every id: acknowledged, nothing duplicated.
	again, err := w.index(ctx, docs("traces-shadowtap-default", 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(results(again), ","); got != "duplicate,duplicate,duplicate" {
		t.Errorf("redelivery results = %s", got)
	}
	for _, o := range again {
		if o.status != http.StatusConflict || o.attempts != 1 {
			t.Errorf("outcome %+v", o)
		}
	}
	if es.DocCount() != 3 || es.BulkRequests() != 2 {
		t.Errorf("stored %d documents in %d bulk requests", es.DocCount(), es.BulkRequests())
	}
	if got := testutil.ToFloat64(w.metrics.bulkRequests.WithLabelValues(bulkOK)); got != 2 {
		t.Errorf("bulk ok = %v", got)
	}
}

func TestBulkIndexRetriesTransientItems(t *testing.T) {
	es := newFakeES(t)
	es.SetItemStatus(func(_, id string, attempt int) int {
		switch {
		case id == "d1" && attempt == 1:
			return http.StatusTooManyRequests
		case id == "d2" && attempt <= 2:
			return http.StatusServiceUnavailable
		}
		return 0
	})
	w := newBulkWriter(t, es, fastRetry())
	outcomes, err := w.index(context.Background(), docs("logs-shadowtap-default", 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(results(outcomes), ","); got != "created,created,created" {
		t.Errorf("results = %s", got)
	}
	if outcomes[0].attempts != 1 || outcomes[1].attempts != 2 || outcomes[2].attempts != 3 {
		t.Errorf("attempts = %d %d %d", outcomes[0].attempts, outcomes[1].attempts, outcomes[2].attempts)
	}
	if es.DocCount() != 3 || es.BulkRequests() != 3 {
		t.Errorf("stored %d documents in %d bulk requests", es.DocCount(), es.BulkRequests())
	}
	if got := testutil.ToFloat64(w.metrics.itemFailures.WithLabelValues("logs-shadowtap-default", "429")); got != 1 {
		t.Errorf("429 failures = %v", got)
	}
	if got := testutil.ToFloat64(w.metrics.itemFailures.WithLabelValues("logs-shadowtap-default", "503")); got != 2 {
		t.Errorf("503 failures = %v", got)
	}
	if got := testutil.ToFloat64(w.metrics.bulkRequests.WithLabelValues(bulkPartial)); got != 2 {
		t.Errorf("partial bulk requests = %v", got)
	}
}

func TestBulkIndexGivesUpAfterMaxAttempts(t *testing.T) {
	es := newFakeES(t)
	es.SetItemStatus(func(_, id string, _ int) int {
		if id == "d0" {
			return http.StatusTooManyRequests
		}
		return 0
	})
	w := newBulkWriter(t, es, RetryConfig{MaxAttempts: 2, InitialBackoffMS: 1, MaxBackoffMS: 2})
	outcomes, err := w.index(context.Background(), docs("shadowtap-captures", 2))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(results(outcomes), ","); got != "failed,created" {
		t.Errorf("results = %s", got)
	}
	o := outcomes[0]
	if o.status != http.StatusTooManyRequests || o.attempts != 2 || o.errType != "es_rejected_execution_exception" || !strings.Contains(o.errMsg, "rejected execution") {
		t.Errorf("outcome = %+v", o)
	}
	if es.BulkRequests() != 2 {
		t.Errorf("%d bulk requests, want 2", es.BulkRequests())
	}
}

func TestBulkIndexDoesNotRetryPermanentFailures(t *testing.T) {
	es := newFakeES(t)
	es.SetItemStatus(func(_, id string, _ int) int {
		switch id {
		case "d0":
			return http.StatusInternalServerError
		case "d1":
			return http.StatusBadRequest
		}
		return 0
	})
	w := newBulkWriter(t, es, fastRetry())
	outcomes, err := w.index(context.Background(), docs("shadowtap-diffs", 3))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(results(outcomes), ","); got != "failed,failed,created" {
		t.Errorf("results = %s", got)
	}
	if outcomes[0].status != 500 || !strings.Contains(outcomes[0].errMsg, "internal_server_error: simulated internal error") {
		t.Errorf("500 outcome = %+v", outcomes[0])
	}
	if outcomes[1].status != 400 || outcomes[1].errType != "document_parsing_exception" {
		t.Errorf("400 outcome = %+v", outcomes[1])
	}
	if es.BulkRequests() != 1 {
		t.Errorf("%d bulk requests, want 1 (no retry)", es.BulkRequests())
	}
}

func TestBulkIndexRetriesRequestFailures(t *testing.T) {
	t.Run("whole request rejected then accepted", func(t *testing.T) {
		es := newFakeES(t)
		failures := 2
		es.SetRequestHook(func(r *http.Request) (int, string) {
			if strings.HasSuffix(r.URL.Path, "/_bulk") && failures > 0 {
				failures--
				return http.StatusInternalServerError, `{"error":{"type":"exception","reason":"node down"}}`
			}
			return 0, ""
		})
		w := newBulkWriter(t, es, fastRetry())
		outcomes, err := w.index(context.Background(), docs("shadowtap-captures", 2))
		if err != nil {
			t.Fatal(err)
		}
		if got := strings.Join(results(outcomes), ","); got != "created,created" {
			t.Errorf("results = %s", got)
		}
		if got := testutil.ToFloat64(w.metrics.requestErrors.WithLabelValues("500")); got < 2 {
			t.Errorf("500 request errors = %v, want at least 2", got)
		}
	})
	t.Run("dropped connections", func(t *testing.T) {
		es := newFakeES(t)
		// The client retries a dropped connection esMaxRetries times on its
		// own; one more drop reaches the writer's retry loop.
		es.AbortConnections(esMaxRetries + 2)
		w := newBulkWriter(t, es, fastRetry())
		outcomes, err := w.index(context.Background(), docs("shadowtap-captures", 1))
		if err != nil {
			t.Fatal(err)
		}
		if results(outcomes)[0] != "created" {
			t.Errorf("results = %v", results(outcomes))
		}
		if got := testutil.ToFloat64(w.metrics.requestErrors.WithLabelValues("transport")); got < 1 {
			t.Errorf("transport errors = %v", got)
		}
	})
	t.Run("context ends the retries", func(t *testing.T) {
		es := newFakeES(t)
		es.SetRequestHook(func(r *http.Request) (int, string) {
			return http.StatusBadGateway, `{"error":"gateway"}`
		})
		w := newBulkWriter(t, es, RetryConfig{MaxAttempts: 3, InitialBackoffMS: 50, MaxBackoffMS: 50})
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		_, err := w.index(ctx, docs("shadowtap-captures", 1))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("index = %v, want the context error", err)
		}
		if es.DocCount() != 0 {
			t.Error("documents stored despite the failures")
		}
	})
	t.Run("stalled server", func(t *testing.T) {
		es := newFakeES(t)
		release := es.Hold()
		w := newBulkWriter(t, es, fastRetry())
		w.timeout = 100 * time.Millisecond
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		_, err := w.index(ctx, docs("shadowtap-captures", 1))
		release()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("index = %v, want the context error", err)
		}
	})
}

func TestBulkSplitsRequestsTooLarge(t *testing.T) {
	es := newFakeES(t)
	es.SetRequestHook(func(r *http.Request) (int, string) {
		if strings.HasSuffix(r.URL.Path, "/_bulk") && r.ContentLength > 120 {
			return http.StatusRequestEntityTooLarge, "request too large"
		}
		return 0, ""
	})
	w := newBulkWriter(t, es, fastRetry())
	w.es, _ = NewElasticsearchClient(ESConfig{URLs: []string{es.URL()}}) // uncompressed, so sizes are predictable
	outcomes, err := w.index(context.Background(), docs("shadowtap-captures", 4))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(results(outcomes), ","); got != "created,created,created,created" {
		t.Errorf("results = %s", got)
	}
	if es.DocCount() != 4 {
		t.Errorf("stored %d", es.DocCount())
	}

	// One document that is too large on its own fails as an item.
	es.SetRequestHook(func(r *http.Request) (int, string) {
		if strings.HasSuffix(r.URL.Path, "/_bulk") {
			return http.StatusRequestEntityTooLarge, "request too large"
		}
		return 0, ""
	})
	outcomes, err = w.index(context.Background(), []*Document{{Index: "shadowtap-captures", ID: "huge", Body: []byte(`{}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if outcomes[0].result != "" || outcomes[0].status != http.StatusRequestEntityTooLarge || outcomes[0].errType != "request_entity_too_large" {
		t.Errorf("outcome = %+v", outcomes[0])
	}
}

func TestBulkBody(t *testing.T) {
	body := bulkBody([]*Document{{Index: "i", ID: "a", Body: []byte(`{"x":1}`)}, {Index: "j", ID: "b", Body: []byte(`{"y":"<b>"}`)}})
	want := `{"create":{"_id":"a","_index":"i"}}` + "\n" + `{"x":1}` + "\n" + `{"create":{"_id":"b","_index":"j"}}` + "\n" + `{"y":"<b>"}` + "\n"
	if string(body) != want {
		t.Errorf("bulk body:\n%s\nwant:\n%s", body, want)
	}
}

func TestBackoff(t *testing.T) {
	cfg := RetryConfig{InitialBackoffMS: 100, MaxBackoffMS: 1000}
	within := func(d time.Duration, base time.Duration) bool {
		return d >= time.Duration(float64(base)*0.8) && d <= time.Duration(float64(base)*1.2)
	}
	for attempt, base := range map[int]time.Duration{1: 100 * time.Millisecond, 2: 200 * time.Millisecond, 3: 400 * time.Millisecond, 4: 800 * time.Millisecond, 5: time.Second, 50: time.Second, 0: 100 * time.Millisecond} {
		for range 20 {
			if d := backoff(attempt, cfg); !within(d, base) {
				t.Errorf("backoff(%d) = %v, want about %v", attempt, d, base)
			}
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepBackoff(ctx, 1, cfg); !errors.Is(err, context.Canceled) {
		t.Errorf("sleepBackoff with a cancelled context = %v", err)
	}
}

func TestLogLimiter(t *testing.T) {
	l := &logLimiter{interval: time.Minute}
	now := time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)
	if ok, n := l.allow(now); !ok || n != 0 {
		t.Errorf("first = %v, %d", ok, n)
	}
	for range 3 {
		if ok, _ := l.allow(now.Add(time.Second)); ok {
			t.Error("event within the interval allowed")
		}
	}
	if ok, n := l.allow(now.Add(2 * time.Minute)); !ok || n != 3 {
		t.Errorf("after the interval = %v, %d", ok, n)
	}
}

func TestBulkErrorMessage(t *testing.T) {
	var nilErr *bulkError
	if nilErr.message() != "" || nilErr.typeName() != "" {
		t.Error("nil error renders text")
	}
	e := &bulkError{Type: "mapper_parsing_exception", Reason: "failed to parse"}
	if e.message() != "mapper_parsing_exception: failed to parse" {
		t.Errorf("message = %q", e.message())
	}
	e.CausedBy = &struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	}{Type: "illegal_argument_exception", Reason: "bad value"}
	if !strings.Contains(e.message(), "(caused by illegal_argument_exception: bad value)") {
		t.Errorf("message = %q", e.message())
	}
}
