// Command demo-api serves the Shadowtap demo application (package api): an
// in-memory users, orders and login JSON API full of fake personal data, for
// Shadowtap to capture and scrub.
//
// Usage:
//
//	demo-api [--addr :8080] [--version v1] [--log-level info] [--shutdown-timeout 10s]
//
// Logs are JSON on stderr. SIGINT or SIGTERM stops accepting connections and
// waits for in-flight requests before exiting.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

// version is the build version, set with -ldflags "-X main.version=...".
var version = "dev"

type config struct {
	addr            string
	apiVersion      string
	logLevel        slog.Level
	shutdownTimeout time.Duration
	role            api.Role
	usersURL        string
	ordersURL       string
	serviceName     string
	telemetry       telemetry.Config
}

// errUsage marks command-line errors; flag has already explained them.
var errUsage = errors.New("usage error")

func main() {
	os.Exit(realMain(os.Args[1:], os.Stderr))
}

func realMain(args []string, stderr io.Writer) int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// After the first signal, restore default handling so a second one
	// terminates a stuck shutdown.
	context.AfterFunc(ctx, stop)

	err := run(ctx, args, stderr)
	switch {
	case err == nil, errors.Is(err, flag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		return 2
	default:
		fmt.Fprintf(stderr, "demo-api: %v\n", err)
		return 1
	}
}

func run(ctx context.Context, args []string, stderr io.Writer) error {
	cfg, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	providers, err := telemetry.Setup(ctx, cfg.telemetry, slog.NewJSONHandler(stderr, &slog.HandlerOptions{Level: cfg.logLevel}))
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := providers.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(stderr, "demo-api: telemetry: %v\n", err)
		}
	}()
	logger := providers.Logger
	a, err := api.New(api.Options{
		Version:        cfg.apiVersion,
		Logger:         logger,
		Role:           cfg.role,
		UsersURL:       cfg.usersURL,
		OrdersURL:      cfg.ordersURL,
		TracerProvider: providers.TracerProvider(),
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger.Info("demo-api listening",
		"addr", ln.Addr().String(), "role", string(cfg.role), "api_version", a.Version(), "build_version", version)
	return serve(ctx, ln, a.Handler(), logger, cfg.shutdownTimeout)
}

func parseFlags(args []string, output io.Writer) (config, error) {
	fs := flag.NewFlagSet("demo-api", flag.ContinueOnError)
	fs.SetOutput(output)
	cfg := config{}
	var level string
	fs.StringVar(&cfg.addr, "addr", ":8080", "listen address")
	fs.StringVar(&cfg.apiVersion, "version", api.VersionV1, "API behaviour version: v1, or v2 (a candidate with deliberate regressions)")
	fs.StringVar(&level, "log-level", "info", "log level: debug, info, warn or error")
	fs.DurationVar(&cfg.shutdownTimeout, "shutdown-timeout", 10*time.Second, "how long to wait for in-flight requests on shutdown")
	var role string
	fs.StringVar(&role, "role", "all", "part of the application to serve: all, gateway, users or orders")
	fs.StringVar(&cfg.usersURL, "users-url", "", "base URL of the users service (gateway and orders roles)")
	fs.StringVar(&cfg.ordersURL, "orders-url", "", "base URL of the orders service (gateway role)")
	fs.StringVar(&cfg.serviceName, "service-name", "", "service.name of this process's spans and logs (default demo-<role>)")
	fs.BoolVar(&cfg.telemetry.Enabled, "otel", false, "export spans and logs over OTLP")
	fs.StringVar(&cfg.telemetry.Endpoint, "otel-endpoint", "", "OTLP gRPC collector host:port (default $OTEL_EXPORTER_OTLP_ENDPOINT or localhost:4317)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return config{}, err
		}
		return config{}, errUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(output, "unexpected arguments: %q\n", fs.Args())
		return config{}, errUsage
	}
	if err := cfg.logLevel.UnmarshalText([]byte(level)); err != nil {
		fmt.Fprintf(output, "invalid --log-level %q: want debug, info, warn or error\n", level)
		return config{}, errUsage
	}
	if cfg.shutdownTimeout <= 0 {
		fmt.Fprintln(output, "--shutdown-timeout must be positive")
		return config{}, errUsage
	}
	r, err := api.ParseRole(role)
	if err != nil {
		fmt.Fprintf(output, "invalid --role: %v\n", err)
		return config{}, errUsage
	}
	cfg.role = r
	if cfg.serviceName == "" {
		cfg.serviceName = "demo-" + string(r)
		if r == api.RoleAll {
			cfg.serviceName = "demo-api"
		}
	}
	cfg.telemetry.ServiceName = cfg.serviceName
	cfg.telemetry.SampleRatio = 1
	cfg.telemetry.Insecure = true
	return cfg, nil
}

// serve runs an HTTP server on ln until ctx is done, then shuts it down
// gracefully, waiting up to shutdownTimeout for in-flight requests.
func serve(ctx context.Context, ln net.Listener, h http.Handler, logger *slog.Logger, shutdownTimeout time.Duration) error {
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	select {
	case err := <-errc:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
	}
	logger.Info("shutting down", "timeout", shutdownTimeout.String())
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		// Requests are still running past the deadline: cut them off.
		srv.Close()
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	if shutdownErr != nil {
		return fmt.Errorf("graceful shutdown: %w", shutdownErr)
	}
	logger.Info("stopped")
	return nil
}
