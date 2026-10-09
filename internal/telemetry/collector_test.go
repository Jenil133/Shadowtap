package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	sdklog "go.opentelemetry.io/otel/sdk/log"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// collector is an in-process OTLP gRPC collector on a loopback listener. It
// records everything the exporters send and the metadata they send it with.
type collector struct {
	addr string

	mu       sync.Mutex
	spans    []*tracepb.ResourceSpans
	logs     []*logspb.ResourceLogs
	metrics  []*metricspb.ResourceMetrics
	metadata []metadata.MD
}

// The three OTLP services each have an Export method with its own signature,
// so one struct cannot implement all three; each is a thin wrapper.
type (
	traceService struct {
		coltracepb.UnimplementedTraceServiceServer
		c *collector
	}
	logsService struct {
		collogspb.UnimplementedLogsServiceServer
		c *collector
	}
	metricsService struct {
		colmetricspb.UnimplementedMetricsServiceServer
		c *collector
	}
)

func startCollector(t *testing.T) *collector {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	c := &collector{addr: ln.Addr().String()}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, traceService{c: c})
	collogspb.RegisterLogsServiceServer(srv, logsService{c: c})
	colmetricspb.RegisterMetricsServiceServer(srv, metricsService{c: c})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = srv.Serve(ln)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return c
}

func (c *collector) record(ctx context.Context) {
	md, _ := metadata.FromIncomingContext(ctx)
	c.metadata = append(c.metadata, md)
}

func (s traceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.record(ctx)
	s.c.spans = append(s.c.spans, req.GetResourceSpans()...)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (s logsService) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.record(ctx)
	s.c.logs = append(s.c.logs, req.GetResourceLogs()...)
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func (s metricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.c.record(ctx)
	s.c.metrics = append(s.c.metrics, req.GetResourceMetrics()...)
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

// exportedSpans flattens every received span with its resource attributes.
func (c *collector) exportedSpans() []exportedSpan {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []exportedSpan
	for _, rs := range c.spans {
		res := attrMap(rs.GetResource().GetAttributes())
		for _, ss := range rs.GetScopeSpans() {
			for _, s := range ss.GetSpans() {
				out = append(out, exportedSpan{Span: s, resource: res})
			}
		}
	}
	return out
}

type exportedSpan struct {
	*tracepb.Span
	resource map[string]string
}

// exportedLogs flattens every received log record with its resource
// attributes.
func (c *collector) exportedLogs() []exportedLog {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []exportedLog
	for _, rl := range c.logs {
		res := attrMap(rl.GetResource().GetAttributes())
		for _, sl := range rl.GetScopeLogs() {
			for _, r := range sl.GetLogRecords() {
				out = append(out, exportedLog{LogRecord: r, resource: res})
			}
		}
	}
	return out
}

type exportedLog struct {
	*logspb.LogRecord
	resource map[string]string
}

// metricNames lists the names of every received metric.
func (c *collector) metricNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var names []string
	for _, rm := range c.metrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				names = append(names, m.GetName())
			}
		}
	}
	return names
}

// requestMetadata returns, per export request, the first value of key.
func (c *collector) requestMetadata(key string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, md := range c.metadata {
		v := md.Get(key)
		if len(v) == 0 {
			out = append(out, "")
			continue
		}
		out = append(out, v[0])
	}
	return out
}

// attrMap renders OTLP attributes as strings; nested values use their JSON.
func attrMap(kvs []*commonpb.KeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.GetKey()] = anyValueString(kv.GetValue())
	}
	return m
}

func anyValueString(v *commonpb.AnyValue) string {
	switch x := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case nil:
		return ""
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// memLogExporter keeps records in memory for handler tests.
type memLogExporter struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (e *memLogExporter) Export(_ context.Context, records []sdklog.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.records = append(e.records, records...)
	return nil
}

func (e *memLogExporter) Shutdown(context.Context) error   { return nil }
func (e *memLogExporter) ForceFlush(context.Context) error { return nil }

func (e *memLogExporter) all() []sdklog.Record {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdklog.Record(nil), e.records...)
}

// syncBuffer is a bytes.Buffer safe for the SDK's error handler, which may
// log from export goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// raw returns each line written so far, unparsed, for checks a JSON decoder
// would hide such as a key appearing twice.
func (b *syncBuffer) raw() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := strings.TrimSpace(b.buf.String())
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// lines parses each JSON log line written so far.
func (b *syncBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(b.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		out = append(out, m)
	}
	return out
}

// lookup follows a dotted path through nested JSON objects.
func lookup(m map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var cur any = m
	for _, p := range parts {
		obj, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		if cur, ok = obj[p]; !ok {
			return nil, false
		}
	}
	return cur, true
}

// waitForGoroutines waits for the goroutine count to fall back to baseline.
// Providers.Shutdown returns before gRPC's connection goroutines have exited,
// so the count is polled with a deadline rather than read once.
func waitForGoroutines(t *testing.T, baseline int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for runtime.NumGoroutine() > baseline {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			t.Fatalf("%d goroutines, baseline %d:\n%s", runtime.NumGoroutine(), baseline, buf[:n])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
