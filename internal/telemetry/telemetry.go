// Package telemetry configures OpenTelemetry for Shadowtap processes: a
// TracerProvider, LoggerProvider and optional MeterProvider exporting over
// OTLP gRPC, a resource that names the service and its pod, W3C trace context
// propagation (including over Kafka record headers), and a slog handler that
// writes every record to the process log and to the OTLP log exporter with
// trace ids on both copies.
//
// Setup is called once per process. Everything it returns is safe to use
// after Shutdown; records and spans are then dropped.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"go.opentelemetry.io/contrib/bridges/otelslog"
)

// scopeName is the instrumentation scope of log records bridged from slog.
const scopeName = "github.com/Jenil133/Shadowtap/internal/telemetry"

// Providers is what Setup hands back: the process logger and the tracer
// factory, plus a single Shutdown for everything behind them.
type Providers struct {
	// Logger writes every record to the base handler given to Setup and,
	// when export is enabled, to the OTLP log exporter. Both copies carry
	// the trace and span id of any span in the record's context.
	Logger *slog.Logger

	tracerProvider trace.TracerProvider
	shutdowns      []shutdownStep

	mu   sync.Mutex
	done chan struct{} // non-nil once Shutdown has started; closed when it finished
}

// shutdownStep names a provider so a shutdown error says which one failed.
type shutdownStep struct {
	name string
	fn   func(context.Context) error
}

// Tracer returns a tracer from the configured TracerProvider; a no-op one
// when telemetry is disabled.
func (p *Providers) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	if p == nil || p.tracerProvider == nil {
		return noop.NewTracerProvider().Tracer(name, opts...)
	}
	return p.tracerProvider.Tracer(name, opts...)
}

// TracerProvider returns the configured provider, a no-op one when telemetry
// is disabled, for components that take a provider rather than a tracer.
func (p *Providers) TracerProvider() trace.TracerProvider {
	if p == nil || p.tracerProvider == nil {
		return noop.NewTracerProvider()
	}
	return p.tracerProvider
}

// Shutdown flushes queued spans, logs and metrics and stops the exporters,
// bounded by ctx. Errors from every provider are joined. The first call does
// the work and reports its errors; a call that overlaps it waits for it to
// finish, or returns ctx's error if ctx ends first, and a call after it
// returns nil.
func (p *Providers) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.done != nil {
		done := p.done
		p.mu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return fmt.Errorf("telemetry: shutdown: %w", ctx.Err())
		}
	}
	done := make(chan struct{})
	p.done = done
	p.mu.Unlock()
	defer close(done)

	var errs []error
	for _, s := range p.shutdowns {
		if err := s.fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("telemetry: shutdown %s: %w", s.name, err))
		}
	}
	return errors.Join(errs...)
}

// Setup validates cfg and builds the providers. base is the process log
// handler (usually the JSON handler on stdout); nil or slog.DiscardHandler
// drops the process copy, leaving the OTLP copy as the only log output.
//
// When cfg.Enabled is false the global TracerProvider is left untouched (a
// no-op unless something else set it), no goroutine is started, and the
// returned Logger writes to base only, still adding trace_id and span_id when
// the context carries a span (for example one built by
// ContextWithRemoteParent).
//
// When enabled, Setup installs the SDK TracerProvider, the LoggerProvider and,
// with cfg.Metrics, the MeterProvider as the OpenTelemetry globals, and routes
// the SDK's own export errors to base at warn level. Exporter settings the
// configuration leaves empty come from the OTEL_EXPORTER_OTLP_* environment
// variables, read by the exporters themselves; an endpoint variable that is
// not a URL is an error, because the exporters would otherwise ignore it and
// export to localhost. In both modes the global propagator becomes W3C
// TraceContext plus Baggage, so trace headers are forwarded through every
// component even when export is off.
func Setup(ctx context.Context, cfg Config, base slog.Handler) (*Providers, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Enabled {
		if err := checkEnvEndpoints(cfg, os.Getenv); err != nil {
			return nil, err
		}
	}
	if base == slog.DiscardHandler {
		// The explicit way of asking for no process log means the same as
		// nil; left as is, its always-false Enabled would silence the OTLP
		// copy too.
		base = nil
	}
	otel.SetTextMapPropagator(newPropagator())
	if !cfg.Enabled {
		return &Providers{
			Logger:         slog.New(newTeeHandler(base, nil)),
			tracerProvider: noop.NewTracerProvider(),
		}, nil
	}

	res, err := newResource(ctx, cfg, os.Getenv)
	if err != nil {
		return nil, fmt.Errorf("telemetry: resource: %w", err)
	}

	// Each exporter dials its own connection so the OTEL_EXPORTER_OTLP_*
	// variables keep working; a shared connection would bypass them.
	traceExporter, err := otlptracegrpc.New(ctx, traceOptions(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: trace exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(newSampler(cfg.SampleRatio)),
		// URL attributes are reduced to the route before export: paths and
		// queries carry personal data.
		sdktrace.WithBatcher(redactingExporter{traceExporter}),
	)
	steps := []shutdownStep{{"tracer provider", tp.Shutdown}}
	// cleanup undoes what was built if a later step fails, so Setup never
	// leaks a batch processor's goroutine.
	cleanup := func() {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		for _, s := range steps {
			_ = s.fn(shutdownCtx)
		}
	}

	logExporter, err := otlploggrpc.New(ctx, logOptions(cfg)...)
	if err != nil {
		cleanup()
		return nil, fmt.Errorf("telemetry: log exporter: %w", err)
	}
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
	)
	steps = append(steps, shutdownStep{"logger provider", lp.Shutdown})

	var mp *sdkmetric.MeterProvider
	if cfg.Metrics {
		metricExporter, err := otlpmetricgrpc.New(ctx, metricOptions(cfg)...)
		if err != nil {
			cleanup()
			return nil, fmt.Errorf("telemetry: metric exporter: %w", err)
		}
		mp = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		)
		steps = append(steps, shutdownStep{"meter provider", mp.Shutdown})
	}

	otel.SetTracerProvider(tp)
	global.SetLoggerProvider(lp)
	if mp != nil {
		otel.SetMeterProvider(mp)
	}
	otel.SetErrorHandler(newErrorHandler(base))

	otelHandler := otelslog.NewHandler(scopeName, otelslog.WithLoggerProvider(lp))
	return &Providers{
		Logger:         slog.New(newTeeHandler(base, otelHandler)),
		tracerProvider: tp,
		shutdowns:      steps,
	}, nil
}

// newPropagator is the propagator every component uses: W3C traceparent and
// tracestate, plus baggage.
func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

// newSampler samples the given fraction of root traces and follows the parent
// decision otherwise, so a capture span parented to a sampled request is
// always kept.
func newSampler(ratio float64) sdktrace.Sampler {
	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
}

// newErrorHandler reports the SDK's export failures through the process log.
// It deliberately bypasses the OTLP log exporter: an unreachable collector
// must not generate records destined for that same collector.
func newErrorHandler(base slog.Handler) otel.ErrorHandler {
	if base == nil {
		base = slog.DiscardHandler
	}
	log := slog.New(base)
	return otel.ErrorHandlerFunc(func(err error) {
		log.Warn("telemetry export failed", "error", err)
	})
}

// envEndpointVars lists the endpoint variables the exporters Setup builds
// read: the generic one and each signal's override.
func envEndpointVars(cfg Config) []string {
	vars := []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
	}
	if cfg.Metrics {
		vars = append(vars, "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
	}
	return vars
}

// checkEnvEndpoints rejects endpoint variables the exporters cannot use. They
// need a URL ("http://otel-collector:4317"); handed the bare host:port that
// Kubernetes manifests usually carry, they log the parse error once and
// export to localhost, which loses every span and log silently. The check
// only matters without a configured endpoint, which overrides the variables.
func checkEnvEndpoints(cfg Config, getenv func(string) string) error {
	if cfg.Endpoint != "" {
		return nil
	}
	for _, name := range envEndpointVars(cfg) {
		v := strings.TrimSpace(getenv(name))
		if v == "" {
			continue
		}
		u, err := url.Parse(v)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return fmt.Errorf("telemetry: %s=%q must be a URL such as http://host:4317, or set endpoint in the configuration", name, v)
		}
	}
	return nil
}

// newResource describes this process: the SDK defaults, then
// OTEL_RESOURCE_ATTRIBUTES, then the configured identity and the pod
// placement from the downward API, each layer overriding the one before.
func newResource(ctx context.Context, cfg Config, getenv func(string) string) (*resource.Resource, error) {
	// resource.Default reads the environment once per process, so the
	// environment is merged explicitly to pick up the current values.
	res, err := mergeResources(resource.Default(), resource.EnvironmentWithContext(ctx))
	if err != nil {
		return nil, err
	}

	attrs := []attribute.KeyValue{semconv.ServiceName(cfg.ServiceName)}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.ServiceVersion))
	}
	_, envHasInstance := res.Set().Value(semconv.ServiceInstanceIDKey)
	if id := instanceID(cfg, getenv, envHasInstance); id != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(id))
	}
	if v := getenv(EnvPodName); v != "" {
		attrs = append(attrs, semconv.K8SPodName(v))
	}
	if v := getenv(EnvPodNamespace); v != "" {
		attrs = append(attrs, semconv.K8SNamespaceName(v))
	}
	if v := getenv(EnvNodeName); v != "" {
		attrs = append(attrs, semconv.K8SNodeName(v))
	}
	return mergeResources(res, resource.NewWithAttributes(semconv.SchemaURL, attrs...))
}

// instanceID picks service.instance.id: the configured value, the pod name,
// then whatever OTEL_RESOURCE_ATTRIBUTES set (reported by envHasInstance and
// already in the resource, so "" keeps it), then the hostname.
func instanceID(cfg Config, getenv func(string) string, envHasInstance bool) string {
	if cfg.ServiceInstance != "" {
		return cfg.ServiceInstance
	}
	if v := getenv(EnvPodName); v != "" {
		return v
	}
	if envHasInstance {
		return ""
	}
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return host
}

// mergeResources merges b over a. Differing schema URLs only cost the merged
// resource its schema URL, which is not worth failing startup over.
func mergeResources(a, b *resource.Resource) (*resource.Resource, error) {
	merged, err := resource.Merge(a, b)
	if err != nil && !errors.Is(err, resource.ErrSchemaURLConflict) {
		return nil, err
	}
	return merged, nil
}

// exporterOptions builds one exporter's options from the configuration. The
// three OTLP exporters have distinct option types with the same constructors,
// so each supplies its constructors and shares the rule: only configured
// values become options, leaving the rest to the OTEL_EXPORTER_OTLP_*
// environment variables that the exporters read themselves.
type exporterOptions[O any] struct {
	endpoint func(string) O
	insecure func() O
	headers  func(map[string]string) O
	timeout  func(time.Duration) O
}

func (e exporterOptions[O]) build(cfg Config) []O {
	var opts []O
	if cfg.Endpoint != "" {
		opts = append(opts, e.endpoint(cfg.Endpoint))
	}
	if cfg.Insecure {
		opts = append(opts, e.insecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, e.headers(maps.Clone(cfg.Headers)))
	}
	if d := cfg.Timeout(); d > 0 {
		opts = append(opts, e.timeout(d))
	}
	return opts
}

func traceOptions(cfg Config) []otlptracegrpc.Option {
	return exporterOptions[otlptracegrpc.Option]{
		endpoint: otlptracegrpc.WithEndpoint,
		insecure: otlptracegrpc.WithInsecure,
		headers:  otlptracegrpc.WithHeaders,
		timeout:  otlptracegrpc.WithTimeout,
	}.build(cfg)
}

func logOptions(cfg Config) []otlploggrpc.Option {
	return exporterOptions[otlploggrpc.Option]{
		endpoint: otlploggrpc.WithEndpoint,
		insecure: otlploggrpc.WithInsecure,
		headers:  otlploggrpc.WithHeaders,
		timeout:  otlploggrpc.WithTimeout,
	}.build(cfg)
}

func metricOptions(cfg Config) []otlpmetricgrpc.Option {
	return exporterOptions[otlpmetricgrpc.Option]{
		endpoint: otlpmetricgrpc.WithEndpoint,
		insecure: otlpmetricgrpc.WithInsecure,
		headers:  otlpmetricgrpc.WithHeaders,
		timeout:  otlpmetricgrpc.WithTimeout,
	}.build(cfg)
}
