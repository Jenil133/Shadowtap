package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/config"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/replay/report"
	"github.com/Jenil133/Shadowtap/internal/replay/request"
	"github.com/Jenil133/Shadowtap/internal/replay/runner"
	"github.com/Jenil133/Shadowtap/internal/replay/schedule"
	"github.com/Jenil133/Shadowtap/internal/replay/source"
	"github.com/Jenil133/Shadowtap/internal/scrub"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

// exitGateFailed is replay's exit status when the gate fails.
const exitGateFailed = 1

// listFlag collects a repeatable flag; each use may also hold a
// comma-separated list.
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error {
	*l = append(*l, splitList(v)...)
	return nil
}

// repeatFlag collects a repeatable flag verbatim (values may hold commas).
type repeatFlag []string

func (r *repeatFlag) String() string { return strings.Join(*r, " ") }
func (r *repeatFlag) Set(v string) error {
	*r = append(*r, v)
	return nil
}

type replayOptions struct {
	conn           connFlags
	topic          string
	file           string
	from, to       string
	target         string
	baseline       string
	allowTargets   listFlag
	speed          string
	concurrency    int
	onOverload     string
	services       listFlag
	methods        listFlag
	allowMutating  bool
	pathRegex      string
	statuses       listFlag
	setHeaders     repeatFlag
	dropHeaders    listFlag
	timeout        time.Duration
	dryRun         bool
	noProbes       bool
	tapConfig      string
	scrubKeyFile   string
	diffConfig     string
	runID          string
	format         string
	reportJSON     string
	reportHTML     string
	diffsOut       string
	publishDiffs   bool
	diffsTopic     string
	maxMismatch    float64
	maxErrors      float64
	failOnLatency  bool
	minReplayed    int
	maxBodyBytes   int64
	logLevel       string
	otel           bool
	otelEndpoint   string
	otelTLS        bool
	reorderSlack   time.Duration
	windowSkew     time.Duration
	sourceDeadline time.Duration
}

func runReplay(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	var o replayOptions
	fs := flag.NewFlagSet("shadowtap replay", flag.ContinueOnError)
	fs.SetOutput(stderr)
	o.conn.register(fs, getenv)
	fs.StringVar(&o.topic, "topic", capture.DefaultTopic, "captures topic")
	fs.StringVar(&o.file, "file", "", "read captures from this NDJSON file (shadowtap tail --format json) instead of Kafka; - for stdin")
	fs.StringVar(&o.from, "from", "", "window start: RFC3339 time or a negative duration such as -10m (default: start of the topic)")
	fs.StringVar(&o.to, "to", "", "window end (exclusive): RFC3339 time, negative duration or now (default: the end when the replay starts)")
	fs.StringVar(&o.target, "target", "", "base URL of the candidate build (required)")
	fs.StringVar(&o.baseline, "baseline", "", "base URL of a deployment of the current production build, for noise suppression")
	fs.Var(&o.allowTargets, "allow-target", "host pattern a target may match besides loopback and cluster-local names (repeatable; a host labelled prod must be named exactly)")
	fs.StringVar(&o.speed, "speed", "1", "replay speed: 1 for the original pace, 4 for four times faster, max to ignore timing")
	fs.IntVar(&o.concurrency, "concurrency", schedule.DefaultConcurrency, "requests in flight at most")
	fs.StringVar(&o.onOverload, "on-overload", schedule.OverloadQueue, "when concurrency is exhausted: queue (send late) or drop")
	fs.Var(&o.services, "service", "only these services (repeatable or comma-separated)")
	fs.Var(&o.methods, "method", "only these methods (default GET,HEAD; every method with --allow-mutating)")
	fs.BoolVar(&o.allowMutating, "allow-mutating", false, "allow methods that change state (POST, PUT, PATCH, DELETE); without --method, every method is replayed")
	fs.StringVar(&o.pathRegex, "path-regex", "", "only paths matching this regular expression")
	fs.Var(&o.statuses, "status", "only captures with these recorded statuses: codes or classes like 2xx")
	fs.Var(&o.setHeaders, "set-header", "name=value set on every replayed request, e.g. a test credential (repeatable)")
	fs.Var(&o.dropHeaders, "drop-header", "captured header not to send (repeatable)")
	fs.DurationVar(&o.timeout, "timeout", runner.DefaultTimeout, "timeout of each replayed request, body included")
	fs.BoolVar(&o.dryRun, "dry-run", false, "read and rebuild every request but send none")
	fs.BoolVar(&o.noProbes, "no-noise-probes", false, "do not send safe requests to the baseline twice to learn noise")
	fs.StringVar(&o.tapConfig, "tap-config", "", "the tap's tap.yaml: replay scrubs with its scrub settings and key (key from its key_env/key_file)")
	fs.StringVar(&o.scrubKeyFile, "scrub-key-file", "", "file holding the tap's scrub key (default: $"+config.EnvScrubKey+")")
	fs.StringVar(&o.diffConfig, "diff-config", "", "diff.yaml with ignore rules")
	fs.StringVar(&o.runID, "run-id", "", "replay run id (default: generated)")
	fs.StringVar(&o.format, "format", "text", "report on stdout: text or json")
	fs.StringVar(&o.reportJSON, "report-json", "", "also write the JSON report to this file")
	fs.StringVar(&o.reportHTML, "report-html", "", "also write a static HTML report to this file")
	fs.StringVar(&o.diffsOut, "diffs-out", "", "write per-exchange results as NDJSON to this file")
	fs.BoolVar(&o.publishDiffs, "publish-diffs", false, "publish per-exchange results to Kafka (--diffs-topic)")
	fs.StringVar(&o.diffsTopic, "diffs-topic", diff.DefaultTopic, "topic for --publish-diffs")
	fs.Float64Var(&o.maxMismatch, "max-mismatch-rate", 0.01, "fail (exit 1) above this mismatch rate; negative disables")
	fs.Float64Var(&o.maxErrors, "max-error-rate", 0.01, "fail (exit 1) above this error rate; negative disables")
	fs.BoolVar(&o.failOnLatency, "fail-on-latency", false, "fail (exit 1) on a latency regression")
	fs.IntVar(&o.minReplayed, "min-replayed", 1, "fail (exit 1) when fewer captures were replayed")
	fs.Int64Var(&o.maxBodyBytes, "max-body-bytes", 8<<20, "read at most this much of each live response body")
	fs.StringVar(&o.logLevel, "log-level", "warn", "log level on stderr: debug, info, warn or error")
	fs.BoolVar(&o.otel, "otel", false, "export a trace per replayed capture (linked to the captured request's trace) and the logs over OTLP")
	fs.StringVar(&o.otelEndpoint, "otel-endpoint", "", "OTLP gRPC collector host:port (default $OTEL_EXPORTER_OTLP_ENDPOINT or localhost:4317)")
	fs.BoolVar(&o.otelTLS, "otel-tls", false, "use TLS to the collector")
	fs.DurationVar(&o.reorderSlack, "reorder-slack", source.DefaultReorderSlack, "how far behind the slowest partition a capture is held to restore request-start order")
	fs.DurationVar(&o.windowSkew, "window-skew", kafka.DefaultWindowSkew, "how far beyond the window offsets are looked up, for out-of-order records")
	fs.DurationVar(&o.sourceDeadline, "read-deadline", 0, "give up reading Kafka after this long (0 = no limit)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	usage := func(format string, args ...any) int {
		fmt.Fprintf(stderr, "shadowtap replay: "+format+"\n", args...)
		return exitUsage
	}
	if fs.NArg() > 0 {
		return usage("unexpected arguments %q", fs.Args())
	}
	if o.target == "" {
		return usage("--target is required")
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(o.logLevel)); err != nil {
		return usage("--log-level %q must be debug, info, warn or error", o.logLevel)
	}
	log := slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: level}))
	tel := telemetry.DefaultConfig()
	tel.Enabled, tel.Endpoint, tel.Insecure, tel.ServiceName = o.otel, o.otelEndpoint, !o.otelTLS, "shadowtap-replay"
	providers, err := telemetry.Setup(ctx, tel, log.Handler())
	if err != nil {
		return usage("%v", err)
	}
	defer func() {
		// Flush the replay spans before exit; the traces are the point.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(stderr, "shadowtap replay: telemetry: %v\n", err)
		}
	}()
	log = providers.Logger

	cand, err := request.ValidateTarget(o.target, o.allowTargets)
	if err != nil {
		return usage("--target: %v", err)
	}
	var base *url.URL
	if o.baseline != "" {
		if base, err = request.ValidateTarget(o.baseline, o.allowTargets); err != nil {
			return usage("--baseline: %v", err)
		}
	}
	speed, err := parseSpeed(o.speed)
	if err != nil {
		return usage("%v", err)
	}
	filter := source.Filter{Services: o.services, Methods: o.methods, AllowMutating: o.allowMutating, Status: o.statuses}
	if o.pathRegex != "" {
		if filter.PathRegex, err = regexp.Compile(o.pathRegex); err != nil {
			return usage("--path-regex: %v", err)
		}
	}
	if err := filter.Validate(); err != nil {
		return usage("%v (add --allow-mutating)", err)
	}
	var setHeaders [][2]string
	for _, h := range o.setHeaders {
		k, v, ok := strings.Cut(h, "=")
		if !ok || strings.TrimSpace(k) == "" {
			return usage("--set-header %q must be name=value", h)
		}
		setHeaders = append(setHeaders, [2]string{strings.TrimSpace(k), v})
	}
	if o.format != "text" && o.format != "json" {
		return usage("--format %q must be text or json", o.format)
	}
	now := time.Now()
	from, err := parseWindowTime("--from", o.from, now)
	if err != nil {
		return usage("%v", err)
	}
	to, err := parseWindowTime("--to", o.to, now)
	if err != nil {
		return usage("%v", err)
	}

	scrubCfg, err := o.scrubConfig(getenv)
	if err != nil {
		return usage("%v", err)
	}
	scrubber, err := scrub.New(scrubCfg)
	if err != nil {
		fmt.Fprintf(stderr, "shadowtap replay: %v (replay must scrub with the tap's key: set %s, --scrub-key-file or --tap-config)\n", err, config.EnvScrubKey)
		return exitUsage
	}
	var diffCfg diff.Config
	if o.diffConfig != "" {
		if diffCfg, err = diff.LoadConfig(o.diffConfig); err != nil {
			return usage("%v", err)
		}
	}
	engine, err := diff.New(diffCfg, scrubber, diff.Options{MaxBodyBytes: o.maxBodyBytes})
	if err != nil {
		return usage("%v", err)
	}
	if o.runID == "" {
		o.runID = "run-" + capture.NewID()
	}

	kcfg := kafka.DefaultConfig()
	if err := o.conn.apply(&kcfg, getenv); err != nil {
		return usage("%v", err)
	}
	kcfg.ClientID = "shadowtap-replay"

	// Source.
	var src source.Source
	srcCtx := ctx
	if o.sourceDeadline > 0 {
		var cancel context.CancelFunc
		srcCtx, cancel = context.WithTimeout(ctx, o.sourceDeadline)
		defer cancel()
	}
	if o.file != "" {
		in := io.Reader(os.Stdin)
		if o.file != "-" {
			f, err := os.Open(o.file)
			if err != nil {
				fmt.Fprintf(stderr, "shadowtap replay: %v\n", err)
				return exitError
			}
			defer f.Close()
			in = f
		}
		if src, err = source.NewFile(in, source.FileOptions{From: from, To: to}); err != nil {
			fmt.Fprintf(stderr, "shadowtap replay: %v\n", err)
			return exitError
		}
	} else {
		capCfg := kcfg
		capCfg.Topic = o.topic
		src, err = source.NewKafka(srcCtx, capCfg, source.KafkaOptions{
			Window: kafka.WindowOptions{From: from, To: to, Skew: o.windowSkew},
			Slack:  o.reorderSlack,
		})
		if err != nil {
			fmt.Fprintf(stderr, "shadowtap replay: %v\n", err)
			return exitError
		}
	}
	defer src.Close()

	log.Info("replay starting", "run_id", o.runID, "target", cand.Redacted(), "speed", speedLabel(speed), "dry_run", o.dryRun)
	out, runErr := runner.Run(srcCtx, runner.Config{
		RunID:         o.runID,
		Candidate:     cand,
		Baseline:      base,
		Request:       request.Options{SetHeaders: setHeaders, DropHeaders: o.dropHeaders},
		Schedule:      schedule.Options{Speed: speed, Concurrency: o.concurrency, OnOverload: o.onOverload},
		Filter:        filter,
		Timeout:       o.timeout,
		DryRun:        o.dryRun,
		NoNoiseProbes: o.noProbes,
		Engine:        engine,
		Log:           log,
		Tracer:        providers.Tracer(runner.TracerName),
	}, src)
	if out == nil {
		fmt.Fprintf(stderr, "shadowtap replay: %v\n", runErr)
		return exitUsage
	}
	if runErr != nil {
		// The results so far are still reported, and the run fails.
		fmt.Fprintf(stderr, "shadowtap replay: %v; reporting the %d results so far\n", runErr, len(out.Results))
	}

	baseName := ""
	if base != nil {
		baseName = base.Redacted()
	}
	sum := report.Build(out, report.Options{
		Candidate:      cand.Redacted(),
		Baseline:       baseName,
		Speed:          speed,
		NoiseThreshold: diffCfg.NoiseThreshold,
		Gate:           report.Gate{MaxMismatchRate: o.maxMismatch, MaxErrorRate: o.maxErrors, FailOnLatency: o.failOnLatency, MinReplayed: o.minReplayed},
	})
	if sum.Schedule.LagWarning {
		log.Warn("schedule lag above threshold: the arrival pattern was not reproduced faithfully; lower --speed or raise --concurrency",
			"lag_p99", time.Duration(sum.Schedule.LagP99)*time.Microsecond, "threshold", schedule.LagWarnThreshold)
	}
	failed := runErr != nil
	if err := o.writeOutputs(ctx, out, sum, kcfg); err != nil {
		fmt.Fprintf(stderr, "shadowtap replay: %v\n", err)
		failed = true
	}
	if o.format == "json" {
		err = report.WriteJSON(stdout, sum)
	} else {
		err = report.WriteText(stdout, sum)
	}
	if err != nil {
		fmt.Fprintf(stderr, "shadowtap replay: write report: %v\n", err)
		failed = true
	}
	switch {
	case failed:
		return exitError
	case !sum.Gate.Passed:
		return exitGateFailed
	}
	return exitOK
}

func (o *replayOptions) scrubConfig(getenv func(string) string) (scrub.Config, error) {
	if o.tapConfig != "" {
		tc, err := config.LoadTap(o.tapConfig, getenv)
		if err != nil {
			return scrub.Config{}, err
		}
		if o.scrubKeyFile != "" {
			if tc.Scrub.Key, err = readKey(o.scrubKeyFile); err != nil {
				return scrub.Config{}, err
			}
		}
		return tc.Scrub.Config, nil
	}
	cfg := scrub.Config{Key: []byte(getenv(config.EnvScrubKey))}
	if o.scrubKeyFile != "" {
		key, err := readKey(o.scrubKeyFile)
		if err != nil {
			return cfg, err
		}
		cfg.Key = key
	}
	return cfg, nil
}

func readKey(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("--scrub-key-file: %w", err)
	}
	return []byte(strings.TrimRight(string(b), "\r\n")), nil
}

// writeOutputs writes the report files and the per-exchange results.
func (o *replayOptions) writeOutputs(ctx context.Context, out *runner.Outcome, sum *report.Summary, kcfg kafka.Config) error {
	var errs []error
	write := func(path string, f func(io.Writer) error) {
		if path == "" {
			return
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if err != nil {
			errs = append(errs, err)
			return
		}
		errs = append(errs, f(file), file.Close())
	}
	write(o.reportJSON, func(w io.Writer) error { return report.WriteJSON(w, sum) })
	write(o.reportHTML, func(w io.Writer) error { return report.WriteHTML(w, sum) })
	write(o.diffsOut, func(w io.Writer) error { return report.WriteNDJSON(w, out.Results) })
	if o.publishDiffs && !o.dryRun {
		errs = append(errs, publishResults(ctx, kcfg, o.diffsTopic, out))
	}
	return errors.Join(errs...)
}

func publishResults(ctx context.Context, kcfg kafka.Config, topic string, out *runner.Outcome) error {
	kcfg.Topic = topic
	w, err := kafka.NewWriter(kcfg)
	if err != nil {
		return err
	}
	for _, r := range out.Results {
		b, err := report.EncodeResult(r)
		if err != nil {
			return err
		}
		ts := r.SentAt
		if ts.IsZero() {
			ts = r.StartedAt
		}
		if err := w.Write(ctx, []byte(r.CaptureID), b, report.RecordHeaders(r), ts); err != nil {
			return err
		}
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := w.Close(cctx); err != nil {
		return fmt.Errorf("publish diffs: %w", err)
	}
	return nil
}

func parseSpeed(s string) (float64, error) {
	v := strings.ToLower(strings.TrimSpace(s))
	if v == "max" {
		return 0, nil
	}
	v = strings.TrimSuffix(v, "x")
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f <= 0 || f > 1000 {
		return 0, fmt.Errorf("--speed %q must be a factor in (0, 1000] such as 1, 4 or 0.5, or max", s)
	}
	return f, nil
}

func speedLabel(f float64) string {
	if f == 0 {
		return "max"
	}
	return strconv.FormatFloat(f, 'g', -1, 64) + "x"
}

// parseWindowTime parses an RFC3339 time, a negative duration relative to
// now, or "now"; empty means unbounded.
func parseWindowTime(flagName, s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	switch {
	case s == "":
		return time.Time{}, nil
	case strings.EqualFold(s, "now"):
		return now, nil
	case strings.HasPrefix(s, "-"):
		d, err := time.ParseDuration(s)
		if err != nil {
			return time.Time{}, fmt.Errorf("%s %q: %w", flagName, s, err)
		}
		return now.Add(d), nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s %q must be an RFC3339 time, a negative duration such as -10m, or now", flagName, s)
	}
	return t, nil
}
