// Package runner replays captures: it reads them from a source, filters
// them, sends each to the candidate (and the baseline, if any) on the
// open-loop schedule, and compares every response with the recording.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/replay/request"
	"github.com/Jenil133/Shadowtap/internal/replay/schedule"
	"github.com/Jenil133/Shadowtap/internal/replay/source"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

// TracerName is the instrumentation scope of the replayer's spans.
const TracerName = "github.com/Jenil133/Shadowtap/internal/replay/runner"

// DefaultTimeout bounds one replayed request, body included.
const DefaultTimeout = 10 * time.Second

// Skip reasons the runner adds to the source filter's.
const (
	// SkipDryRun is the skip reason of every capture in a dry run.
	SkipDryRun = "dry_run"
	// SkipOverloadDrop is the skip reason of a capture the scheduler dropped
	// because Concurrency requests were already in flight (OverloadDrop).
	SkipOverloadDrop = "overload_drop"
	// SkipUnbuildable is the skip reason of a capture no request could be
	// rebuilt from (for example a path that is not absolute).
	SkipUnbuildable = "unbuildable"
)

// Config configures a replay.
type Config struct {
	RunID string
	// Candidate is the build under test; Baseline, optional, a deployment
	// of the current production build. Both are validated targets.
	Candidate *url.URL
	Baseline  *url.URL
	Request   request.Options
	Schedule  schedule.Options
	Filter    source.Filter
	// Timeout bounds each request, response body included; 0 means
	// DefaultTimeout.
	Timeout time.Duration
	// DryRun rebuilds every request but sends none.
	DryRun bool
	// NoNoiseProbes stops sending safe requests (GET, HEAD, OPTIONS) to the
	// baseline a second time. The two baseline responses show which places
	// of a route change between identical requests: noise to suppress.
	// Mutating requests are never probed, since a repeat would change the
	// baseline's state; noisy fields in their responses need ignore rules.
	NoNoiseProbes bool
	Engine        *diff.Engine
	// Client overrides the HTTP client (tests); it must not follow
	// redirects.
	Client *http.Client
	Log    *slog.Logger
	// Tracer produces the replay spans; nil uses the global tracer provider.
	// Each replayed capture becomes a new trace linked to the captured
	// request's trace, and the candidate request carries that new trace's
	// traceparent, so the candidate's own spans nest under it.
	Tracer trace.Tracer
}

// Outcome is everything a replay produced.
type Outcome struct {
	RunID string
	// Results holds one finalized result per capture read, in request-start
	// order.
	Results  []*diff.Result
	Schedule schedule.Stats
	Source   source.Stats
	// Noise lists, per route, the places suppressed as noise.
	Noise map[string][]string
	// HasBaseline reports whether a baseline was compared.
	HasBaseline bool
}

// NewClient returns an HTTP client for replays: no redirects followed, no
// transparent compression (the captured Accept-Encoding is sent as is, so
// the target encodes like it did for production), and enough idle
// connections per host for the concurrency.
func NewClient(concurrency int) *http.Client {
	if concurrency <= 0 {
		concurrency = schedule.DefaultConcurrency
	}
	t := &http.Transport{
		Proxy:                 nil, // never route replayed traffic through an environment proxy
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          2 * concurrency,
		MaxIdleConnsPerHost:   concurrency,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true,
		ForceAttemptHTTP2:     true,
	}
	return &http.Client{
		Transport:     t,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// Run replays src and returns the outcome. It returns early, with the
// results so far, when ctx is done or the source fails.
func Run(ctx context.Context, cfg Config, src source.Source) (*Outcome, error) {
	if cfg.Candidate == nil {
		return nil, errors.New("runner: no candidate target")
	}
	if cfg.Engine == nil {
		return nil, errors.New("runner: no diff engine")
	}
	if err := cfg.Filter.Validate(); err != nil {
		return nil, err
	}
	if err := cfg.Schedule.Validate(); err != nil {
		return nil, err
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultTimeout
	}
	if cfg.Client == nil {
		cfg.Client = NewClient(cfg.Schedule.Concurrency)
	}
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	cfg.Request.RunID = cfg.RunID

	r := &run{cfg: cfg}
	// A dropped capture is still accounted for: it appears in the outcome
	// as skipped, so the report's totals add up to what was read.
	onDrop := cfg.Schedule.OnDrop
	cfg.Schedule.OnDrop = func(x *capture.Exchange) {
		r.add(diff.Skipped(cfg.RunID, x, SkipOverloadDrop))
		if onDrop != nil {
			onDrop(x)
		}
	}
	next := func(ctx context.Context) (*capture.Exchange, error) {
		for {
			x, err := src.Next(ctx)
			if err != nil {
				return nil, err
			}
			if reason := cfg.Filter.Skip(x); reason != "" {
				r.add(diff.Skipped(cfg.RunID, x, reason))
				continue
			}
			return x, nil
		}
	}
	stats, err := schedule.Run(ctx, cfg.Schedule, next, r.send)
	for _, res := range r.results {
		cfg.Engine.Finalize(res)
	}
	slices.SortStableFunc(r.results, func(a, b *diff.Result) int { return a.StartedAt.Compare(b.StartedAt) })
	out := &Outcome{
		RunID:       cfg.RunID,
		Results:     r.results,
		Schedule:    stats,
		Source:      src.Stats(),
		Noise:       cfg.Engine.NoiseReport(),
		HasBaseline: cfg.Baseline != nil && !cfg.DryRun,
	}
	if err != nil {
		return out, fmt.Errorf("runner: %w", err)
	}
	return out, nil
}

type run struct {
	cfg     Config
	mu      sync.Mutex
	results []*diff.Result
}

func (r *run) add(res *diff.Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.results = append(r.results, res)
}

// send replays one capture to the candidate and the baseline in parallel,
// under a span of its own that links back to the captured request's trace.
func (r *run) send(ctx context.Context, j schedule.Job) {
	x := j.Exchange
	ctx, cancel := context.WithTimeout(ctx, r.cfg.Timeout)
	defer cancel()
	ctx, span := r.startSpan(ctx, x)
	defer span.End()

	cand, err := r.request(ctx, "candidate", r.cfg.Candidate, x)
	if err != nil {
		reason := SkipUnbuildable
		if errors.Is(err, request.ErrBodyUnavailable) {
			reason = source.SkipBodyUnavailable
		}
		r.skip(span, x, reason)
		return
	}
	var base outbound
	if r.cfg.Baseline != nil {
		if base, err = r.request(ctx, "baseline", r.cfg.Baseline, x); err != nil {
			cand.end(nil)
			r.skip(span, x, SkipUnbuildable)
			return
		}
	}
	if r.cfg.DryRun {
		cand.end(nil)
		base.end(nil)
		r.skip(span, x, SkipDryRun)
		return
	}

	var (
		baseLive, probeLive *diff.Live
		wg                  sync.WaitGroup
	)
	if base.req != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l := r.do(base.req)
			base.end(&l)
			baseLive = &l
			if r.cfg.NoNoiseProbes || !safeMethod(x.Request.Method) || l.Err != nil {
				return
			}
			probe, err := r.request(ctx, "probe", r.cfg.Baseline, x)
			if err != nil {
				return
			}
			p := r.do(probe.req)
			probe.end(&p)
			probeLive = &p
		}()
	}
	candLive := r.do(cand.req)
	cand.end(&candLive)
	wg.Wait()

	res := r.cfg.Engine.Compare(r.cfg.RunID, x, candLive, baseLive, probeLive)
	res.ReplayTraceID = cand.traceID
	res.SentAt = j.Sent
	res.LagMicros = j.Lag().Microseconds()
	span.SetAttributes(attribute.String("shadowtap.class", fmt.Sprint(res.Class)))
	r.add(res)
}

func (r *run) tracer() trace.Tracer {
	if r.cfg.Tracer != nil {
		return r.cfg.Tracer
	}
	return otel.Tracer(TracerName)
}

// startSpan opens the trace of one replayed capture. It is a new trace, not a
// continuation of the captured request's: the original is finished, and a
// link lets a trace viewer jump between the two.
func (r *run) startSpan(ctx context.Context, x *capture.Exchange) (context.Context, trace.Span) {
	path, _, _ := strings.Cut(x.Request.Path, "?")
	opts := []trace.SpanStartOption{
		trace.WithNewRoot(),
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("shadowtap.run.id", r.cfg.RunID),
			attribute.String("shadowtap.capture.id", x.ID),
			attribute.String("shadowtap.service", x.Service),
			attribute.String("shadowtap.original.trace_id", x.TraceID),
			attribute.String("http.request.method", x.Request.Method),
			attribute.String("url.path", path),
		),
	}
	if link, ok := telemetry.LinkTo(x.TraceID, x.ParentSpanID); ok {
		opts = append(opts, trace.WithLinks(link))
	}
	return r.tracer().Start(ctx, "shadowtap.replay", opts...)
}

// skip records a capture that was not replayed.
func (r *run) skip(span trace.Span, x *capture.Exchange, reason string) {
	span.SetAttributes(attribute.String("shadowtap.skip", reason))
	r.add(diff.Skipped(r.cfg.RunID, x, reason))
}

// outbound is one request to a target together with the span it is sent
// under.
type outbound struct {
	req     *http.Request
	traceID string
	span    trace.Span
}

// request builds the request to send to target for x under a client span.
// When the span is recording, its trace context becomes the request's
// traceparent, so the target's spans join the replay trace; otherwise the
// request gets a random traceparent as before.
func (r *run) request(ctx context.Context, role string, target *url.URL, x *capture.Exchange) (outbound, error) {
	ctx, span := r.tracer().Start(ctx, "shadowtap.replay."+role,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			attribute.String("shadowtap.target", role),
			attribute.String("server.address", target.Hostname()),
		))
	opts := r.cfg.Request
	if sc := span.SpanContext(); sc.IsValid() {
		opts.NewTraceparent = func() string { return traceparent(sc) }
	}
	built, err := request.Build(ctx, target, x, opts)
	if err != nil {
		span.SetStatus(codes.Error, "request could not be built")
		span.End()
		return outbound{}, err
	}
	return outbound{req: built.Request, traceID: built.TraceID, span: span}, nil
}

// end closes the request's span with the outcome of l (nil when the request
// was never sent).
func (o outbound) end(l *diff.Live) {
	if o.span == nil {
		return
	}
	if l != nil {
		o.span.SetAttributes(attribute.Int64("shadowtap.latency_us", l.Latency.Microseconds()))
		if l.Err != nil {
			// The error text is kept out of the span: it may quote the
			// request URL.
			o.span.SetStatus(codes.Error, "request failed")
		}
	}
	o.span.End()
}

// traceparent formats sc as a W3C traceparent header value.
func traceparent(sc trace.SpanContext) string {
	return fmt.Sprintf("00-%s-%s-%s", sc.TraceID(), sc.SpanID(), sc.TraceFlags())
}

func safeMethod(m string) bool {
	switch strings.ToUpper(m) {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}
	return false
}

// do sends req and normalizes the response.
func (r *run) do(req *http.Request) diff.Live {
	start := time.Now()
	resp, err := r.cfg.Client.Do(req)
	latency := time.Since(start)
	if err != nil {
		return diff.Live{Err: redactURL(err), Latency: latency}
	}
	side, err := r.cfg.Engine.Normalize(resp)
	if err != nil {
		return diff.Live{Err: err, Latency: latency}
	}
	return diff.Live{Side: &side, Latency: latency}
}

// redactURL drops the request URL from client errors: it holds the
// captured path, which is scrubbed but needlessly verbose in reports.
func redactURL(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Errorf("%s: %w", ue.Op, ue.Err)
	}
	return err
}
