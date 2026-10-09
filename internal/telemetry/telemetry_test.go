package telemetry

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

// enabledConfig points export at the collector over plaintext.
func enabledConfig(addr string) Config {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.Endpoint = addr
	cfg.Insecure = true
	cfg.ServiceName = "shadowtap-test"
	cfg.ServiceVersion = "1.2.3"
	cfg.TimeoutMS = 5000
	return cfg
}

func shutdown(t *testing.T, p *Providers) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

func TestSetupDisabled(t *testing.T) {
	ctx := testContext(t)
	var buf syncBuffer
	base := slog.NewJSONHandler(&buf, nil)
	before := otel.GetTracerProvider()
	goroutines := runtime.NumGoroutine()

	p, err := Setup(ctx, DefaultConfig(), base)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if got := runtime.NumGoroutine(); got > goroutines {
		t.Errorf("disabled Setup grew goroutines from %d to %d", goroutines, got)
	}
	if otel.GetTracerProvider() != before {
		t.Error("disabled Setup replaced the global TracerProvider")
	}
	fields := otel.GetTextMapPropagator().Fields()
	if !slices.Contains(fields, "traceparent") || !slices.Contains(fields, "baggage") {
		t.Errorf("propagator fields = %v, want traceparent and baggage", fields)
	}

	_, span := p.Tracer("test").Start(ctx, "op")
	if span.IsRecording() || span.SpanContext().IsValid() {
		t.Error("disabled Tracer produced a real span")
	}
	span.End()

	p.Logger.Info("plain", "k", "v")
	parentCtx, ok := ContextWithRemoteParent(ctx, testTraceID, testSpanID, true)
	if !ok {
		t.Fatal("ContextWithRemoteParent rejected valid ids")
	}
	p.Logger.InfoContext(parentCtx, "traced")

	lines := buf.lines(t)
	if len(lines) != 2 {
		t.Fatalf("got %d log lines, want 2: %v", len(lines), lines)
	}
	if lines[0]["msg"] != "plain" || lines[0]["k"] != "v" {
		t.Errorf("first line = %v", lines[0])
	}
	if _, has := lines[0][LogKeyTraceID]; has {
		t.Errorf("line without span carries trace_id: %v", lines[0])
	}
	if lines[1][LogKeyTraceID] != testTraceID || lines[1][LogKeySpanID] != testSpanID {
		t.Errorf("line with remote parent = %v, want trace_id %s span_id %s", lines[1], testTraceID, testSpanID)
	}

	for i := range 2 {
		if err := p.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown #%d = %v", i+1, err)
		}
	}
}

func TestSetupNilBase(t *testing.T) {
	ctx := testContext(t)
	p, err := Setup(ctx, DefaultConfig(), nil)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if p.Logger.Enabled(ctx, slog.LevelError) {
		t.Error("logger without base and export is enabled")
	}
	p.Logger.Error("dropped")
	shutdown(t, p)
}

func TestSetupInvalidConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Enabled = true
	cfg.SampleRatio = 2
	p, err := Setup(testContext(t), cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "sample_ratio") {
		t.Fatalf("Setup = %v, %v; want sample_ratio error", p, err)
	}
	if p != nil {
		t.Error("Setup returned providers with an error")
	}
}

func TestSetupExportsTracesAndLogs(t *testing.T) {
	ctx := testContext(t)
	col := startCollector(t)
	t.Setenv(EnvPodName, "tap-0")
	t.Setenv(EnvPodNamespace, "shadowtap")
	t.Setenv(EnvNodeName, "node-a")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment.name=test")
	t.Setenv("OTEL_SERVICE_NAME", "from-env")

	var buf syncBuffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	cfg := enabledConfig(col.addr)
	cfg.Headers = map[string]string{"x-api-key": "secret"}
	goroutines := runtime.NumGoroutine()

	p, err := Setup(ctx, cfg, base)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if _, ok := otel.GetTracerProvider().(*sdktrace.TracerProvider); !ok {
		t.Errorf("global TracerProvider is %T, want the SDK provider", otel.GetTracerProvider())
	}
	if _, ok := global.GetLoggerProvider().(*sdklog.LoggerProvider); !ok {
		t.Errorf("global LoggerProvider is %T, want the SDK provider", global.GetLoggerProvider())
	}

	spanCtx, span := p.Tracer("test").Start(ctx, "shadowtap.test")
	if !span.IsRecording() || !span.SpanContext().IsSampled() {
		t.Fatal("span from enabled provider is not recording and sampled")
	}
	wantTrace := span.SpanContext().TraceID().String()
	wantSpan := span.SpanContext().SpanID().String()
	p.Logger.InfoContext(spanCtx, "hello", "k", "v")
	p.Logger.DebugContext(spanCtx, "below level")
	span.End()
	shutdown(t, p)
	if err := p.Shutdown(ctx); err != nil {
		t.Errorf("second Shutdown = %v", err)
	}

	spans := col.exportedSpans()
	if len(spans) != 1 || spans[0].GetName() != "shadowtap.test" {
		t.Fatalf("collector spans = %v, want one shadowtap.test", spans)
	}
	if got := hex.EncodeToString(spans[0].GetTraceId()); got != wantTrace {
		t.Errorf("exported trace id %s, want %s", got, wantTrace)
	}
	wantResource := map[string]string{
		"service.name":                "shadowtap-test",
		"service.version":             "1.2.3",
		"service.instance.id":         "tap-0",
		"k8s.pod.name":                "tap-0",
		"k8s.namespace.name":          "shadowtap",
		"k8s.node.name":               "node-a",
		"deployment.environment.name": "test",
		"telemetry.sdk.language":      "go",
	}
	for k, want := range wantResource {
		if got := spans[0].resource[k]; got != want {
			t.Errorf("span resource %s = %q, want %q", k, got, want)
		}
	}

	logs := col.exportedLogs()
	if len(logs) != 1 {
		t.Fatalf("collector logs = %v, want one record", logs)
	}
	rec := logs[0]
	if got := anyValueString(rec.GetBody()); got != "hello" {
		t.Errorf("log body = %q, want hello", got)
	}
	if got := hex.EncodeToString(rec.GetTraceId()); got != wantTrace {
		t.Errorf("log trace id %s, want %s", got, wantTrace)
	}
	if got := hex.EncodeToString(rec.GetSpanId()); got != wantSpan {
		t.Errorf("log span id %s, want %s", got, wantSpan)
	}
	if got := attrMap(rec.GetAttributes())["k"]; got != "v" {
		t.Errorf("log attribute k = %q, want v", got)
	}
	if got := rec.resource["service.name"]; got != "shadowtap-test" {
		t.Errorf("log resource service.name = %q", got)
	}
	if got := rec.GetSeverityText(); got != "INFO" {
		t.Errorf("log severity text = %q, want INFO", got)
	}

	for i, v := range col.requestMetadata("x-api-key") {
		if v != "secret" {
			t.Errorf("export request %d lacked the x-api-key header: %q", i, v)
		}
	}
	if names := col.metricNames(); len(names) != 0 {
		t.Errorf("metrics exported without cfg.Metrics: %v", names)
	}

	lines := buf.lines(t)
	if len(lines) != 1 {
		t.Fatalf("process log lines = %v, want one", lines)
	}
	if lines[0]["msg"] != "hello" || lines[0][LogKeyTraceID] != wantTrace || lines[0][LogKeySpanID] != wantSpan {
		t.Errorf("process log line = %v, want hello with trace %s span %s", lines[0], wantTrace, wantSpan)
	}

	// The logger keeps working after shutdown: the process copy is written,
	// the exported copy is dropped.
	p.Logger.Info("after shutdown")
	if lines := buf.lines(t); len(lines) != 2 || lines[1]["msg"] != "after shutdown" {
		t.Errorf("process log after shutdown = %v", lines)
	}
	waitForGoroutines(t, goroutines)
}

func TestSetupExportsMetrics(t *testing.T) {
	ctx := testContext(t)
	col := startCollector(t)
	cfg := enabledConfig(col.addr)
	cfg.Metrics = true
	p, err := Setup(ctx, cfg, slog.DiscardHandler)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	counter, err := otel.Meter("test").Int64Counter("shadowtap.test.requests")
	if err != nil {
		t.Fatalf("counter: %v", err)
	}
	counter.Add(ctx, 3)
	shutdown(t, p)
	if names := col.metricNames(); !slices.Contains(names, "shadowtap.test.requests") {
		t.Errorf("collector metrics = %v, want shadowtap.test.requests", names)
	}
}

func TestSetupEnvironment(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string // values of "ADDR" are replaced by the collector address
		cfg  func(addr string) Config
		// wantHeader is a metadata key and value every export must carry.
		wantHeader [2]string
	}{
		{
			name: "env endpoint when config has none",
			env:  map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://ADDR"},
			cfg: func(string) Config {
				cfg := enabledConfig("")
				cfg.Insecure = false
				return cfg
			},
		},
		{
			name: "config endpoint wins over env",
			env:  map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1"},
			cfg:  enabledConfig,
		},
		{
			name:       "env headers when config has none",
			env:        map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "x-env-key=from-env"},
			cfg:        enabledConfig,
			wantHeader: [2]string{"x-env-key", "from-env"},
		},
		{
			name: "config headers win over env",
			env:  map[string]string{"OTEL_EXPORTER_OTLP_HEADERS": "x-api-key=from-env"},
			cfg: func(addr string) Config {
				cfg := enabledConfig(addr)
				cfg.Headers = map[string]string{"x-api-key": "from-config"}
				return cfg
			},
			wantHeader: [2]string{"x-api-key", "from-config"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testContext(t)
			col := startCollector(t)
			for k, v := range tt.env {
				t.Setenv(k, strings.ReplaceAll(v, "ADDR", col.addr))
			}
			p, err := Setup(ctx, tt.cfg(col.addr), slog.DiscardHandler)
			if err != nil {
				t.Fatalf("Setup: %v", err)
			}
			_, span := p.Tracer("test").Start(ctx, "env")
			span.End()
			shutdown(t, p)
			if spans := col.exportedSpans(); len(spans) != 1 {
				t.Fatalf("collector received %d spans, want 1", len(spans))
			}
			if tt.wantHeader[0] == "" {
				return
			}
			values := col.requestMetadata(tt.wantHeader[0])
			if len(values) == 0 {
				t.Fatal("no export requests recorded")
			}
			for i, v := range values {
				if v != tt.wantHeader[1] {
					t.Errorf("request %d header %s = %q, want %q", i, tt.wantHeader[0], v, tt.wantHeader[1])
				}
			}
		})
	}
}

func TestNewResource(t *testing.T) {
	ctx := testContext(t)
	tests := []struct {
		name string
		cfg  Config
		env  map[string]string
		// resourceEnv is the value of OTEL_RESOURCE_ATTRIBUTES, which the
		// SDK reads from the process environment.
		resourceEnv string
		want        map[string]string
		// absent lists keys that must not be set.
		absent []string
	}{
		{
			name: "configured instance wins over pod name",
			cfg:  Config{ServiceName: "svc", ServiceVersion: "2", ServiceInstance: "inst"},
			env:  map[string]string{EnvPodName: "pod-1"},
			want: map[string]string{"service.name": "svc", "service.version": "2", "service.instance.id": "inst", "k8s.pod.name": "pod-1"},
		},
		{
			name: "pod name is the instance when unset",
			cfg:  Config{ServiceName: "svc"},
			env:  map[string]string{EnvPodName: "pod-1", EnvPodNamespace: "ns", EnvNodeName: "node"},
			want: map[string]string{"service.instance.id": "pod-1", "k8s.pod.name": "pod-1", "k8s.namespace.name": "ns", "k8s.node.name": "node"},
		},
		{
			name:   "no kubernetes attributes outside a pod",
			cfg:    Config{ServiceName: "svc"},
			absent: []string{"k8s.pod.name", "k8s.namespace.name", "k8s.node.name", "service.version"},
		},
		{
			name:        "environment instance id kept outside a pod",
			cfg:         Config{ServiceName: "svc"},
			resourceEnv: "service.instance.id=from-env,k8s.pod.name=env-pod",
			want:        map[string]string{"service.name": "svc", "service.instance.id": "from-env", "k8s.pod.name": "env-pod"},
		},
		{
			name:        "pod name wins over environment instance id",
			cfg:         Config{ServiceName: "svc"},
			env:         map[string]string{EnvPodName: "pod-1"},
			resourceEnv: "service.instance.id=from-env,k8s.pod.name=env-pod",
			want:        map[string]string{"service.instance.id": "pod-1", "k8s.pod.name": "pod-1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.resourceEnv)
			getenv := func(k string) string { return tt.env[k] }
			res, err := newResource(ctx, tt.cfg, getenv)
			if err != nil {
				t.Fatalf("newResource: %v", err)
			}
			attrs := make(map[string]string)
			for _, kv := range res.Attributes() {
				attrs[string(kv.Key)] = kv.Value.Emit()
			}
			for k, want := range tt.want {
				if attrs[k] != want {
					t.Errorf("%s = %q, want %q", k, attrs[k], want)
				}
			}
			for _, k := range tt.absent {
				if v, ok := attrs[k]; ok {
					t.Errorf("%s = %q, want unset", k, v)
				}
			}
			if attrs["service.instance.id"] == "" {
				t.Error("service.instance.id is empty; want the hostname fallback")
			}
			if attrs["telemetry.sdk.name"] != "opentelemetry" {
				t.Errorf("default resource attributes missing: %v", attrs)
			}
			if res.SchemaURL() == "" {
				t.Error("merged resource lost its schema URL")
			}
		})
	}
}

func TestSamplerParentBased(t *testing.T) {
	ctx := testContext(t)
	remote := func(sampled bool) context.Context {
		parent, ok := ContextWithRemoteParent(ctx, testTraceID, testSpanID, sampled)
		if !ok {
			t.Fatal("ContextWithRemoteParent rejected valid ids")
		}
		return parent
	}
	tests := []struct {
		name        string
		ratio       float64
		parent      context.Context
		wantSampled bool
	}{
		{name: "root at ratio 0 dropped", ratio: 0, parent: ctx},
		{name: "root at ratio 1 sampled", ratio: 1, parent: ctx, wantSampled: true},
		{name: "sampled remote parent kept at ratio 0", ratio: 0, parent: remote(true), wantSampled: true},
		{name: "unsampled remote parent dropped at ratio 1", ratio: 1, parent: remote(false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exp := tracetest.NewInMemoryExporter()
			tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(newSampler(tt.ratio)), sdktrace.WithSyncer(exp))
			defer func() { _ = tp.Shutdown(ctx) }()

			_, span := tp.Tracer("test").Start(tt.parent, "op")
			span.End()
			if got := span.SpanContext().IsSampled(); got != tt.wantSampled {
				t.Errorf("sampled = %v, want %v", got, tt.wantSampled)
			}
			spans := exp.GetSpans()
			if (len(spans) == 1) != tt.wantSampled {
				t.Fatalf("exported %d spans, want sampled=%v", len(spans), tt.wantSampled)
			}
			if tt.wantSampled && tt.parent != ctx {
				if got := spans[0].SpanContext.TraceID().String(); got != testTraceID {
					t.Errorf("child trace id %s, want the remote parent's %s", got, testTraceID)
				}
				if got := spans[0].Parent.SpanID().String(); got != testSpanID || !spans[0].Parent.IsRemote() {
					t.Errorf("child parent = %v, want remote %s", spans[0].Parent, testSpanID)
				}
			}
		})
	}
}

func TestNilProviders(t *testing.T) {
	var p *Providers
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("nil Shutdown = %v", err)
	}
	_, span := p.Tracer("x").Start(context.Background(), "op")
	if span.IsRecording() {
		t.Error("nil Providers produced a recording span")
	}
	span.End()
}

func TestZeroValuesDoNotPanic(t *testing.T) {
	ctx := testContext(t)
	var buf syncBuffer
	p, err := Setup(ctx, Config{}, slog.NewJSONHandler(&buf, nil))
	if err != nil {
		t.Fatalf("Setup with a zero Config: %v", err)
	}
	spanCtx, _ := ContextWithRemoteParent(ctx, testTraceID, testSpanID, true)
	p.Logger.InfoContext(spanCtx, "x")
	p.Logger.With("a", 1).WithGroup("g").ErrorContext(spanCtx, "y")
	if lines := buf.lines(t); len(lines) != 2 || lines[0][LogKeyTraceID] != testTraceID || lines[1][LogKeyTraceID] != testTraceID {
		t.Errorf("process log lines = %v", lines)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown = %v", err)
	}

	var zero Providers
	_, span := zero.Tracer("x").Start(ctx, "op")
	span.End()
	if span.IsRecording() {
		t.Error("zero Providers produced a recording span")
	}
	if err := zero.Shutdown(ctx); err != nil {
		t.Errorf("zero Providers Shutdown = %v", err)
	}

	var carrier HeaderCarrier
	carrier.Set("k", "v")
	if carrier.Get("K") != "v" {
		t.Errorf("carrier after Set = %v", carrier)
	}
	if HeaderCarrier(nil).Get("x") != "" || len(InjectHeaders(ctx, nil)) != 0 {
		t.Error("nil headers were not handled as empty")
	}
	if _, _, ok := SpanIDs(ExtractHeaders(ctx, nil)); ok {
		t.Error("ExtractHeaders of nil produced a span context")
	}
}

func TestShutdownConcurrentCallers(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	p := &Providers{shutdowns: []shutdownStep{{"blocking", func(ctx context.Context) error {
		close(entered)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}}}

	first := make(chan error, 1)
	go func() { first <- p.Shutdown(context.Background()) }()
	<-entered

	// A caller whose context has ended does not wait for the first call.
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Shutdown(expired); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown with an expired context = %v, want context.Canceled", err)
	}

	// A caller with time waits for the first call and reports nothing.
	waiting := make(chan error, 1)
	go func() { waiting <- p.Shutdown(context.Background()) }()
	close(release)
	if err := <-first; err != nil {
		t.Errorf("first Shutdown = %v", err)
	}
	if err := <-waiting; err != nil {
		t.Errorf("overlapping Shutdown = %v", err)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown after completion = %v", err)
	}
}

func TestShutdownJoinsErrors(t *testing.T) {
	errA, errB := errors.New("a failed"), errors.New("b failed")
	calls := 0
	p := &Providers{shutdowns: []shutdownStep{
		{"first", func(context.Context) error { calls++; return errA }},
		{"second", func(context.Context) error { calls++; return nil }},
		{"third", func(context.Context) error { calls++; return errB }},
	}}
	err := p.Shutdown(context.Background())
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("Shutdown = %v, want both %v and %v", err, errA, errB)
	}
	for _, want := range []string{"shutdown first", "shutdown third"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Shutdown error %q does not name %q", err, want)
		}
	}
	if calls != 3 {
		t.Errorf("shutdown steps run = %d, want all 3 despite failures", calls)
	}
	if err := p.Shutdown(context.Background()); err != nil {
		t.Errorf("second Shutdown = %v, want nil", err)
	}
}

func TestSetupDiscardBaseStillExports(t *testing.T) {
	ctx := testContext(t)
	col := startCollector(t)
	p, err := Setup(ctx, enabledConfig(col.addr), slog.DiscardHandler)
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if !p.Logger.Enabled(ctx, slog.LevelDebug) {
		t.Error("logger with a discarded process copy is not enabled for export")
	}
	spanCtx, span := p.Tracer("test").Start(ctx, "op")
	p.Logger.ErrorContext(spanCtx, "exported only")
	span.End()
	shutdown(t, p)

	logs := col.exportedLogs()
	if len(logs) != 1 || anyValueString(logs[0].GetBody()) != "exported only" {
		t.Fatalf("collector logs = %v, want the one record", logs)
	}
	if got := hex.EncodeToString(logs[0].GetTraceId()); got != span.SpanContext().TraceID().String() {
		t.Errorf("log trace id = %s, want %s", got, span.SpanContext().TraceID())
	}
}

func TestCheckEnvEndpoints(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		env  map[string]string
		// wantErr is a substring of the error; empty means valid.
		wantErr string
	}{
		{name: "nothing set"},
		{name: "generic url", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel-collector:4317"}},
		{name: "generic https url without port", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel-collector"}},
		{name: "blank variable ignored", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "  "}},
		{name: "generic host and port only", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "otel-collector:4317"}, wantErr: `OTEL_EXPORTER_OTLP_ENDPOINT="otel-collector:4317" must be a URL`},
		{name: "generic ip and port only", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "127.0.0.1:4317"}, wantErr: "must be a URL"},
		{name: "traces host and port only", env: map[string]string{"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "otel:4317"}, wantErr: "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"},
		{name: "logs host and port only", env: map[string]string{"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT": "otel:4317"}, wantErr: "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"},
		{name: "metrics variable ignored without metrics", env: map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "otel:4317"}},
		{name: "metrics host and port only with metrics", cfg: Config{Metrics: true}, env: map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "otel:4317"}, wantErr: "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"},
		{name: "configured endpoint overrides a bad variable", cfg: Config{Endpoint: "otel:4317"}, env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "otel:4317"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkEnvEndpoints(tt.cfg, func(k string) string { return tt.env[k] })
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("checkEnvEndpoints = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("checkEnvEndpoints = %v, want error containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestSetupRejectsEnvEndpointWithoutScheme(t *testing.T) {
	ctx := testContext(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "otel-collector:4317")
	before := otel.GetTracerProvider()
	goroutines := runtime.NumGoroutine()

	p, err := Setup(ctx, enabledConfig(""), slog.DiscardHandler)
	if err == nil || !strings.Contains(err.Error(), "OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Fatalf("Setup = %v, %v; want an endpoint error", p, err)
	}
	if p != nil {
		t.Error("Setup returned providers with an error")
	}
	if otel.GetTracerProvider() != before {
		t.Error("failed Setup replaced the global TracerProvider")
	}
	if got := runtime.NumGoroutine(); got > goroutines {
		t.Errorf("failed Setup grew goroutines from %d to %d", goroutines, got)
	}
}

func TestSetupConcurrentLoggingDuringShutdown(t *testing.T) {
	ctx := testContext(t)
	col := startCollector(t)
	var buf syncBuffer
	p, err := Setup(ctx, enabledConfig(col.addr), slog.NewJSONHandler(&buf, nil))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	const workers, perWorker = 8, 50
	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := p.Logger.With("worker", i).WithGroup("req")
			for n := range perWorker {
				spanCtx, span := p.Tracer("test").Start(ctx, "op")
				l.InfoContext(spanCtx, "hit", "n", n)
				span.End()
			}
		}()
	}
	// Shutdown overlaps the workers. The process copy of every record is
	// written whether or not export is still running.
	shutdown(t, p)
	wg.Wait()
	if got := len(buf.lines(t)); got != workers*perWorker {
		t.Errorf("process log lines = %d, want %d", got, workers*perWorker)
	}
}
