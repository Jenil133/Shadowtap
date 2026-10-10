// Command shadowtap-indexer consumes captures, replay diffs and
// OpenTelemetry spans and logs from Kafka and indexes them in Elasticsearch,
// keyed by trace id, so one request can be followed across every service it
// touched.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Jenil133/Shadowtap/internal/admin"
	"github.com/Jenil133/Shadowtap/internal/index"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

var version = "dev"

// Shutdown budget. The indexer gets most of the pod's grace period to
// finish and commit its batch and leave the group; the admin server goes
// next so probes report draining until the end, and the telemetry flush
// comes last so the shutdown's own spans and logs are exported.
const (
	readinessTTL      = 5 * time.Second
	adminStopTimeout  = 2 * time.Second
	telemetryShutdown = 5 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "shadowtap-indexer: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("shadowtap-indexer", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "path to indexer.yaml; built-in defaults apply when empty")
	printConfig := fs.Bool("print-config", false, "print the effective configuration (secrets omitted) and exit")
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if *showVersion {
		_, err := fmt.Fprintln(stdout, version)
		return err
	}

	cfg, err := index.Load(*configPath, getenv)
	if err != nil {
		return err
	}
	if *printConfig {
		out, err := cfg.YAML()
		if err != nil {
			return err
		}
		_, err = stdout.Write(out)
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	handler, err := cfg.Log.Handler(stderr)
	if err != nil {
		return err
	}
	// The process is identified by its build version unless the file says
	// otherwise.
	tcfg := cfg.Telemetry
	if tcfg.ServiceVersion == "" {
		tcfg.ServiceVersion = version
	}
	providers, err := telemetry.Setup(ctx, tcfg, handler)
	if err != nil {
		return err
	}
	defer func() {
		// Flush with a fresh context: ctx is already done after a signal.
		sctx, cancel := context.WithTimeout(context.Background(), telemetryShutdown)
		defer cancel()
		if err := providers.Shutdown(sctx); err != nil {
			providers.Logger.Warn("telemetry shutdown", "err", err)
		}
	}()

	a, err := newApp(cfg, providers.Logger)
	if err != nil {
		return err
	}
	httpLn, err := net.Listen("tcp", cfg.Listen.HTTP)
	if err != nil {
		a.indexer.Close()
		return fmt.Errorf("listen http: %w", err)
	}
	return a.serve(ctx, httpLn)
}

// app is a wired indexer process: everything except the listener, so tests
// can serve it on a port of their choosing.
type app struct {
	cfg     index.Config
	log     *slog.Logger
	indexer *index.Indexer
	ready   *admin.Readiness
	admin   *admin.Server
}

func newApp(cfg index.Config, log *slog.Logger, kafkaOpts ...kgo.Opt) (*app, error) {
	reg := prometheus.NewRegistry()
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shadowtap_build_info",
		Help: "Build information of the running shadowtap-indexer.",
	}, []string{"version", "goversion"})
	buildInfo.WithLabelValues(version, runtime.Version()).Set(1)
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		buildInfo,
	)
	ix, err := index.New(cfg, index.Options{Registerer: reg, Logger: log, Kafka: kafkaOpts})
	if err != nil {
		return nil, err
	}
	ready := admin.NewReadiness(ix.Ready, readinessTTL)
	return &app{
		cfg:     cfg,
		log:     log,
		indexer: ix,
		ready:   ready,
		admin:   admin.New(reg, ready.Check, log),
	}, nil
}

// serve runs the admin server and the indexer until ctx is done or either
// fails, then shuts down: the indexer first (it flushes and commits its
// batch and leaves the group), then the admin server.
func (a *app) serve(ctx context.Context, httpLn net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	adminErr := make(chan error, 1)
	go func() {
		if err := a.admin.Serve(httpLn); err != nil {
			adminErr <- fmt.Errorf("admin server: %w", err)
		}
	}()
	indexErr := make(chan error, 1)
	go func() { indexErr <- a.indexer.Run(ctx) }()
	a.log.Info("shadowtap-indexer started",
		"version", version,
		"http", httpLn.Addr().String(),
		"group", a.cfg.Kafka.Group,
		"elasticsearch", a.cfg.Elasticsearch.URLs,
	)

	var serveErr error
	select {
	case <-ctx.Done():
		a.log.Info("shutting down", "reason", context.Cause(ctx).Error())
	case serveErr = <-adminErr:
		a.log.Error("server failed", "err", serveErr)
	case serveErr = <-indexErr:
		if serveErr != nil {
			a.log.Error("indexer failed", "err", serveErr)
		} else {
			a.log.Error("indexer stopped")
		}
		indexErr <- serveErr // let shutdown read it again
	}
	return errors.Join(serveErr, a.shutdown(cancel, indexErr))
}

// shutdown stops the indexer and then the admin server.
func (a *app) shutdown(cancel context.CancelFunc, indexErr <-chan error) error {
	a.ready.SetDraining()
	cancel()
	var errs []error
	// Run bounds its own shutdown with the configured timeout; wait a little
	// longer for it, then give up on a stuck group leave.
	timeout := time.Duration(a.cfg.ShutdownTimeoutMS)*time.Millisecond + 5*time.Second
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-indexErr:
		if err != nil {
			errs = append(errs, fmt.Errorf("indexer: %w", err))
		}
	case <-timer.C:
		errs = append(errs, errors.New("indexer did not stop in time"))
	}
	stopCtx, stopCancel := context.WithTimeout(context.Background(), adminStopTimeout)
	defer stopCancel()
	if err := a.admin.Shutdown(stopCtx); err != nil {
		errs = append(errs, fmt.Errorf("stop admin server: %w", err))
	}
	a.log.Info("shutdown complete")
	return errors.Join(errs...)
}
