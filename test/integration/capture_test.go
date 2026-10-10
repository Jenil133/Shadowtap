// Package integration runs the capture path in-process: demo API
// traffic → simulated Envoy ext_proc stream (observe and inline modes) → tap →
// sampler → scrubber → Kafka (kfake) → consumer, and checks that no planted
// personal data survives anywhere in what was stored.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
	"github.com/Jenil133/Shadowtap/examples/demo/traffic"
	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/scrub"
	"github.com/Jenil133/Shadowtap/internal/tap"
	"github.com/Jenil133/Shadowtap/internal/tap/taptest"
)

const (
	scrubKey      = "integration-test-scrub-key-0123456789"
	service       = "demo-api"
	serviceHeader = "x-shadowtap-service"
)

// pipeline is one tap wired to a fake Kafka cluster.
type pipeline struct {
	client extprocv3.ExternalProcessorClient
	kafka  kafka.Config
	reg    *prometheus.Registry
}

func newPipeline(t *testing.T, sc sampling.Config, key string) *pipeline {
	t.Helper()
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(6, capture.DefaultTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cluster.Close)

	kcfg := kafka.DefaultConfig()
	kcfg.Brokers = cluster.ListenAddrs()
	kcfg.ClientID = "integration"
	kcfg.LingerMS = 1

	scrubber, err := scrub.New(scrub.Config{Key: []byte(key)})
	if err != nil {
		t.Fatal(err)
	}
	sampler, err := sampling.New(sc)
	if err != nil {
		t.Fatal(err)
	}
	reg := prometheus.NewRegistry()
	producer, err := kafka.NewProducer(kcfg, reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	tcfg := tap.DefaultConfig()
	tcfg.Name = "tap-integration"
	tcfg.Workers = 4
	server, err := tap.New(tcfg, sampler, scrubber, producer, reg, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("tap shutdown: %v", err)
		}
		if err := producer.Close(ctx); err != nil {
			t.Errorf("producer close: %v", err)
		}
	})
	// Registered after the shutdown cleanup so the gRPC server stops first.
	client := taptest.Dial(t, server)
	return &pipeline{client: client, kafka: kcfg, reg: reg}
}

// boundedCtx returns a context that expires long after any healthy run, so a
// tap that stops answering fails the test instead of hanging it (taptest also
// bounds each awaited reply).
func boundedCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	t.Cleanup(cancel)
	return ctx
}

// collect reads records until want have arrived or quiet passes without one.
func (p *pipeline) collect(t *testing.T, want int, quiet time.Duration) []kafka.Message {
	t.Helper()
	consumer, err := kafka.NewConsumer(p.kafka, kafka.ConsumeOptions{Start: "earliest"})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	var msgs []kafka.Message
	for want <= 0 || len(msgs) < want {
		ctx, cancel := context.WithTimeout(context.Background(), quiet)
		msg, err := consumer.Next(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if msg.DecodeErr != nil {
			t.Fatalf("partition %d offset %d: %v", msg.Partition, msg.Offset, msg.DecodeErr)
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

// sent records what went through the simulated sidecar.
type sent struct {
	byMode   map[string]int
	ledger   *traffic.Ledger
	gzipped  int // responses that were gzip-encoded on the wire
	statuses map[int]int
}

// drive sends n generated requests to a real demo API server and replays each
// exchange through the simulated sidecar, alternating capture modes.
func drive(t *testing.T, p *pipeline, n int, seed int64) sent {
	t.Helper()
	demo, err := api.New(api.Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(demo.Handler())
	defer srv.Close()
	// Keep encoded bodies exactly as a sidecar would see them.
	httpClient := &http.Client{Transport: &http.Transport{DisableCompression: true}, Timeout: 10 * time.Second}

	s := sent{byMode: map[string]int{}, ledger: traffic.NewLedger(), statuses: map[int]int{}}
	gen := traffic.NewGenerator(seed, traffic.People(seed, 40), s.ledger)
	ctx := boundedCtx(t, 2*time.Minute)
	for i := range n {
		req, err := gen.Next(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		var reqBody []byte
		if req.Body != nil {
			if reqBody, err = io.ReadAll(req.Body); err != nil {
				t.Fatal(err)
			}
			req.Body = io.NopCloser(bytes.NewReader(reqBody))
			req.ContentLength = int64(len(reqBody))
		}
		if req.Header.Get("Accept-Encoding") == "" {
			req.Header.Set("Accept-Encoding", "gzip")
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		s.statuses[resp.StatusCode]++
		if resp.Header.Get("Content-Encoding") == "gzip" {
			s.gzipped++
		}

		ex := taptest.FromHTTP(req, reqBody, resp.StatusCode, resp.Header, respBody)
		ex.Authority = "demo-api.demo.svc.cluster.local:8080"
		mode := capture.ModeObserve
		if i%2 == 1 {
			mode = capture.ModeInline
		}
		opts := taptest.Options{
			Mode:      mode,
			ChunkSize: 700, // several STREAMED chunks for larger bodies
			Metadata:  map[string]string{serviceHeader: service},
		}
		if _, err := taptest.Run(ctx, p.client, ex, opts); err != nil {
			t.Fatalf("request %d (%s %s, %s): %v", i, req.Method, req.URL.Path, mode, err)
		}
		s.byMode[mode]++
	}
	return s
}

func TestCaptureLeaksNoPersonalData(t *testing.T) {
	const n = 400
	p := newPipeline(t, sampling.Config{DefaultRate: 1}, scrubKey)
	s := drive(t, p, n, 7)
	msgs := p.collect(t, n, 5*time.Second)
	if len(msgs) != n {
		t.Fatalf("captured %d of %d exchanges", len(msgs), n)
	}
	secrets := s.ledger.Values()
	if len(secrets) < 50 {
		t.Fatalf("ledger only holds %d values; generator coverage too low", len(secrets))
	}
	t.Logf("%d exchanges, %d planted PII values, %d gzip responses, statuses %v", n, len(secrets), s.gzipped, s.statuses)

	byMode := map[string]int{}
	kinds := map[string]int{}
	var traced, decoded int
	for _, msg := range msgs {
		x := msg.Exchange
		if err := x.Validate(); err != nil {
			t.Errorf("capture %s: %v", x.ID, err)
		}
		if x.Service != service || x.Source.Tap != "tap-integration" {
			t.Errorf("capture %s: service %q tap %q", x.ID, x.Service, x.Source.Tap)
		}
		byMode[x.Source.Mode]++
		if x.TraceID != "" {
			traced++
			if string(msg.Key) != x.TraceID {
				t.Errorf("capture %s keyed %q, want trace id", x.ID, msg.Key)
			}
		}
		if x.Response.Body.DecodedFrom == "gzip" {
			decoded++
		}
		for k, v := range x.Redactions {
			kinds[k] += v
		}

		views := views(t, msg)
		haystack := strings.ToLower(strings.Join(views, "\n"))
		for _, secret := range secrets {
			if strings.Contains(haystack, strings.ToLower(secret)) {
				t.Errorf("capture %s (%s %s): planted PII value of length %d survived", x.ID, x.Request.Method, x.Request.Path, len(secret))
			}
		}
		for _, v := range views {
			for _, m := range scrub.Detect(v) {
				t.Errorf("capture %s: detector %s still matches stored data", x.ID, m.Kind)
			}
		}
	}

	if !maps.Equal(byMode, s.byMode) {
		t.Errorf("captures by mode %v, sent %v", byMode, s.byMode)
	}
	if traced < n*8/10 {
		t.Errorf("only %d of %d captures carry a trace id", traced, n)
	}
	if s.gzipped > 0 && decoded != s.gzipped {
		t.Errorf("%d gzip responses sent, %d decoded before scrubbing", s.gzipped, decoded)
	}
	if s.gzipped == 0 {
		t.Error("demo traffic produced no gzip responses; decoding path untested")
	}
	for _, kind := range []string{scrub.KindEmail, scrub.KindCard, scrub.KindPhone, scrub.KindSSN, scrub.KindIPv4, scrub.KindCredential, scrub.KindSensitiveKey} {
		if kinds[kind] == 0 {
			t.Errorf("no %s redactions across %d captures (got %v)", kind, n, kinds)
		}
	}
	t.Logf("redactions by kind: %v", kinds)
}

// views returns every stored string of a capture in raw and decoded forms
// (URL-unescaped, base64-decoded, JSON-unescaped), so a leak cannot hide
// behind an encoding.
func views(t *testing.T, msg kafka.Message) []string {
	t.Helper()
	x := msg.Exchange
	out := []string{string(msg.Raw)}
	add := func(s string) {
		out = append(out, s)
		if u, err := url.PathUnescape(s); err == nil && u != s {
			out = append(out, u)
		}
		if u, err := url.QueryUnescape(s); err == nil && u != s {
			out = append(out, u)
		}
	}
	add(x.Request.Path)
	for _, h := range []capture.Headers{x.Request.Headers, x.Request.Trailers, x.Response.Headers, x.Response.Trailers} {
		for _, k := range slices.Sorted(maps.Keys(h)) {
			for _, v := range h[k] {
				add(v)
			}
		}
	}
	for _, b := range []capture.Body{x.Request.Body, x.Response.Body} {
		data, err := b.Bytes()
		if err != nil {
			t.Errorf("capture %s: body: %v", x.ID, err)
			continue
		}
		add(string(data))
		out = append(out, jsonStrings(data)...)
	}
	return out
}

// jsonStrings returns the decoded string tokens of a JSON document (or of
// every document in an NDJSON stream); nothing for non-JSON data.
func jsonStrings(data []byte) []string {
	var out []string
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	for {
		tok, err := dec.Token()
		if err != nil {
			return out
		}
		switch v := tok.(type) {
		case string:
			out = append(out, v)
		case json.Number:
			out = append(out, v.String())
		}
	}
}

func TestUnsampledTrafficIsNeverCaptured(t *testing.T) {
	p := newPipeline(t, sampling.Config{DefaultRate: 0}, scrubKey)
	ctx := boundedCtx(t, time.Minute)
	ex := taptest.Exchange{
		Method: "POST", Scheme: "http", Authority: "demo-api:8080", Path: "/api/users",
		RequestHeaders:  [][2]string{{"content-type", "application/json"}, {"x-request-id", "r"}},
		RequestBody:     []byte(`{"email":"never@example.com"}`),
		Status:          201,
		ResponseHeaders: [][2]string{{"content-type", "application/json"}},
		ResponseBody:    []byte(`{"id":"u_1"}`),
	}
	for i := range 40 {
		mode := []string{capture.ModeObserve, capture.ModeInline}[i%2]
		res, err := taptest.Run(ctx, p.client, ex, taptest.Options{Mode: mode, Metadata: map[string]string{serviceHeader: service}})
		if err != nil {
			t.Fatal(err)
		}
		if mode == capture.ModeInline && res.ModeOverride == nil {
			t.Fatal("unsampled inline request got no mode_override")
		}
	}
	if msgs := p.collect(t, 0, time.Second); len(msgs) != 0 {
		t.Fatalf("%d unsampled exchanges reached Kafka", len(msgs))
	}
	if got := counterSum(t, p.reg, "shadowtap_tap_decisions_total", "decision", sampling.ReasonUnsampled); got != 40 {
		t.Errorf("unsampled decisions = %v, want 40", got)
	}
}

// counterSum adds up the series of a counter family whose label has value.
func counterSum(t *testing.T, reg *prometheus.Registry, name, label, value string) float64 {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var sum float64
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, m := range f.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == label && l.GetValue() == value {
					sum += m.GetCounter().GetValue()
				}
			}
		}
	}
	return sum
}

func TestSamplingIsConsistentAcrossServices(t *testing.T) {
	const traces = 300
	p := newPipeline(t, sampling.Config{DefaultRate: 0.3}, scrubKey)
	ctx := boundedCtx(t, time.Minute)
	for i := range traces {
		traceID := fmt.Sprintf("%016x%016x", uint64(i)*0x9e3779b97f4a7c15+1, uint64(i)+1)
		for _, svc := range []string{"gateway", "orders"} {
			ex := taptest.Exchange{
				Method: "GET", Scheme: "http", Authority: svc + ":8080", Path: "/api/x",
				RequestHeaders: [][2]string{{"traceparent", "00-" + traceID + "-00f067aa0ba902b7-01"}},
				Status:         200,
			}
			if _, err := taptest.Run(ctx, p.client, ex, taptest.Options{Metadata: map[string]string{serviceHeader: svc}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	msgs := p.collect(t, 0, 2*time.Second)
	byService := map[string]map[string]bool{"gateway": {}, "orders": {}}
	for _, m := range msgs {
		byService[m.Exchange.Service][m.Exchange.TraceID] = true
	}
	gw, orders := byService["gateway"], byService["orders"]
	if !maps.Equal(gw, orders) {
		t.Fatalf("services captured different traces: gateway %d, orders %d", len(gw), len(orders))
	}
	if len(gw) < traces*2/10 || len(gw) > traces*4/10 {
		t.Errorf("captured %d of %d traces at rate 0.3", len(gw), traces)
	}
}

func TestPseudonymsAgreeAcrossTapReplicas(t *testing.T) {
	ex := taptest.Exchange{
		Method: "POST", Scheme: "http", Authority: "demo-api:8080", Path: "/api/users?email=bob%40example.com",
		RequestHeaders: [][2]string{
			{"content-type", "application/json"},
			{"x-forwarded-for", "203.0.113.9"},
		},
		RequestBody:     []byte(`{"full_name":"Bob Jones","email":"bob@example.com","phone":"(415) 555-0142","card_number":"5555 5555 5555 4444"}`),
		Status:          201,
		ResponseHeaders: [][2]string{{"content-type", "application/json"}},
		ResponseBody:    []byte(`{"id":"u_9","email":"bob@example.com"}`),
	}
	ctx := boundedCtx(t, time.Minute)
	capture1 := func(key string) *capture.Exchange {
		p := newPipeline(t, sampling.Config{DefaultRate: 1}, key)
		if _, err := taptest.Run(ctx, p.client, ex, taptest.Options{Metadata: map[string]string{serviceHeader: service}}); err != nil {
			t.Fatal(err)
		}
		msgs := p.collect(t, 1, 5*time.Second)
		if len(msgs) != 1 {
			t.Fatalf("got %d captures", len(msgs))
		}
		return msgs[0].Exchange
	}
	a, b := capture1(scrubKey), capture1(scrubKey)
	other := capture1("a-different-scrub-key-0123456789")

	same := func(x, y *capture.Exchange) bool {
		return x.Request.Path == y.Request.Path &&
			x.Request.Body.Data == y.Request.Body.Data &&
			x.Response.Body.Data == y.Response.Body.Data &&
			slices.Equal(x.Request.Headers["x-forwarded-for"], y.Request.Headers["x-forwarded-for"])
	}
	if !same(a, b) {
		t.Errorf("replicas with one key disagree:\n%s\n%s", a.Request.Body.Data, b.Request.Body.Data)
	}
	if same(a, other) {
		t.Error("a different key produced identical pseudonyms")
	}
	// The email in the query and in both bodies maps to one pseudonym.
	var req map[string]any
	if err := json.Unmarshal([]byte(a.Request.Body.Data), &req); err != nil {
		t.Fatal(err)
	}
	pseudo, _ := req["email"].(string)
	if !scrub.IsPseudonym(pseudo) || !strings.Contains(a.Response.Body.Data, pseudo) {
		t.Errorf("email pseudonym %q not reused in response %s", pseudo, a.Response.Body.Data)
	}
	if q, _ := url.QueryUnescape(a.Request.Path); !strings.Contains(q, pseudo) {
		t.Errorf("email pseudonym %q not reused in query %s", pseudo, a.Request.Path)
	}
}
