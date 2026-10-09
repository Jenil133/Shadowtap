// Command shadowtap-tap is the Envoy External Processing (ext_proc) server
// that receives sidecar traffic, samples it, scrubs personal data and
// publishes the captures to Kafka.
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

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/Jenil133/Shadowtap/internal/admin"
	"github.com/Jenil133/Shadowtap/internal/config"
	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/scrub"
	"github.com/Jenil133/Shadowtap/internal/tap"
)

var version = "dev"

// Shutdown budget. Each step gets its own deadline, so a slow step cannot
// leave the next one with an expired context (a queue drain that runs out
// the clock must still leave Kafka time to flush what was drained). Together
// they stay under the pod's 30 s grace period.
const (
	grpcStopTimeout  = 12 * time.Second
	drainTimeout     = 7 * time.Second
	flushTimeout     = 7 * time.Second
	adminStopTimeout = 2 * time.Second
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintf(os.Stderr, "shadowtap-tap: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("shadowtap-tap", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "", "path to tap.yaml; built-in defaults apply when empty")
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

	cfg, err := config.LoadTap(*configPath, getenv)
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
	logger, err := cfg.Log.Logger(stderr)
	if err != nil {
		return err
	}

	a, err := newApp(cfg, logger)
	if err != nil {
		return err
	}
	grpcLn, err := net.Listen("tcp", cfg.Listen.GRPC)
	if err != nil {
		return errors.Join(fmt.Errorf("listen grpc: %w", err), a.shutdown())
	}
	httpLn, err := net.Listen("tcp", cfg.Listen.HTTP)
	if err != nil {
		grpcLn.Close()
		return errors.Join(fmt.Errorf("listen http: %w", err), a.shutdown())
	}
	return a.serve(ctx, grpcLn, httpLn)
}

// app is a wired tap process: everything except the listeners, so tests can
// serve it on ports of their choosing.
type app struct {
	cfg      config.Tap
	log      *slog.Logger
	reg      *prometheus.Registry
	producer *kafka.Producer
	tap      *tap.Server
	grpc     *grpc.Server
	health   *health.Server
	ready    *admin.Readiness
	admin    *admin.Server
}

func newApp(cfg config.Tap, log *slog.Logger, kafkaOpts ...kgo.Opt) (*app, error) {
	reg := prometheus.NewRegistry()
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "shadowtap_build_info",
		Help: "Build information of the running shadowtap-tap.",
	}, []string{"version", "goversion"})
	buildInfo.WithLabelValues(version, runtime.Version()).Set(1)
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		buildInfo,
	)

	scrubber, err := scrub.New(cfg.Scrub.Config)
	if err != nil {
		return nil, err
	}
	sampler, err := sampling.New(cfg.Sampling)
	if err != nil {
		return nil, err
	}
	producer, err := kafka.NewProducer(cfg.Kafka, reg, log, kafkaOpts...)
	if err != nil {
		return nil, err
	}
	tapServer, err := tap.New(cfg.Tap, sampler, scrubber, producer, reg, log)
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
		defer cancel()
		return nil, errors.Join(err, producer.Close(ctx))
	}

	g := grpc.NewServer(
		// An inline-mode body arrives as one message as large as Envoy's
		// buffer limit; leave room for operators who raise that limit past
		// gRPC's 4 MiB default.
		grpc.MaxRecvMsgSize(16<<20),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}),
	)
	tapServer.Register(g)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(g, hs)
	hs.SetServingStatus(extprocv3.ExternalProcessor_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)

	ready := admin.NewReadiness(producer.Ping, 5*time.Second)
	return &app{
		cfg:      cfg,
		log:      log,
		reg:      reg,
		producer: producer,
		tap:      tapServer,
		grpc:     g,
		health:   hs,
		ready:    ready,
		admin:    admin.New(reg, ready.Check, log),
	}, nil
}

// serve runs until ctx is done or a server fails, then shuts down.
func (a *app) serve(ctx context.Context, grpcLn, httpLn net.Listener) error {
	errc := make(chan error, 2)
	go func() {
		if err := a.grpc.Serve(grpcLn); err != nil {
			errc <- fmt.Errorf("grpc server: %w", err)
		}
	}()
	go func() {
		if err := a.admin.Serve(httpLn); err != nil {
			errc <- fmt.Errorf("admin server: %w", err)
		}
	}()
	a.log.Info("shadowtap-tap started",
		"version", version,
		"grpc", grpcLn.Addr().String(),
		"http", httpLn.Addr().String(),
		"tap", a.cfg.Tap.Name,
		"topic", a.cfg.Kafka.Topic,
		"default_rate", a.cfg.Sampling.DefaultRate,
	)

	var serveErr error
	select {
	case <-ctx.Done():
		a.log.Info("shutting down", "reason", context.Cause(ctx).Error())
	case serveErr = <-errc:
		a.log.Error("server failed", "err", serveErr)
	}
	return errors.Join(serveErr, a.shutdown())
}

// shutdown stops taking traffic, lets in-flight streams finish, drains
// queued captures and flushes Kafka, in that order.
func (a *app) shutdown() error {
	a.ready.SetDraining()
	a.health.Shutdown()

	stopped := make(chan struct{})
	go func() {
		a.grpc.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(grpcStopTimeout):
		a.log.Warn("graceful gRPC stop timed out; closing remaining streams")
		a.grpc.Stop()
		<-stopped
	}

	var errs []error
	step := func(name string, timeout time.Duration, f func(context.Context) error) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		if err := f(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
		}
	}
	step("drain captures", drainTimeout, a.tap.Shutdown)
	step("flush kafka", flushTimeout, a.producer.Close)
	step("stop admin server", adminStopTimeout, a.admin.Shutdown)
	a.log.Info("shutdown complete")
	return errors.Join(errs...)
}
