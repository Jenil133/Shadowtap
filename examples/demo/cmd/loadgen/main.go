// Command loadgen sends a seeded mix of personal-data-heavy requests to the
// demo API, normally through the Envoy sidecar stand-in, and can write every
// personal-data string it sent to a file so leak checks can search captures
// for them.
//
// Usage:
//
//	loadgen [--target http://localhost:10000] [--rps 20] [--requests 200]
//	        [--duration 0] [--concurrency 8] [--seed 1] [--people 1000]
//	        [--timeout 10s] [--pii-out pii.txt]
//
// With --requests 0 it runs for --duration. It prints a summary and exits 1
// when more than 5% of requests fail at the transport level (no HTTP
// response), or 2 on usage errors. SIGINT stops sending, lets in-flight
// requests finish and still prints the summary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/time/rate"

	"github.com/Jenil133/Shadowtap/examples/demo/traffic"
)

// version is the build version, set with -ldflags "-X main.version=...".
var version = "dev"

// maxTransportErrorRate is the share of transport failures above which the
// run fails.
const maxTransportErrorRate = 0.05

// errUsage marks command-line errors that have already been reported.
var errUsage = errors.New("usage error")

type config struct {
	target      string
	rps         float64
	requests    int
	duration    time.Duration
	concurrency int
	seed        int64
	people      int
	timeout     time.Duration
	piiOut      string
}

func main() {
	os.Exit(realMain(os.Args[1:], os.Stdout, os.Stderr))
}

func realMain(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseFlags(args, stderr)
	switch {
	case errors.Is(err, flag.ErrHelp):
		return 0
	case err != nil:
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	context.AfterFunc(ctx, stop) // a second signal kills a stuck run

	sum, err := run(ctx, cfg)
	if sum != nil {
		sum.write(stdout)
	}
	if err != nil {
		fmt.Fprintf(stderr, "loadgen: %v\n", err)
		return 1
	}
	if r := sum.transportErrorRate(); r > maxTransportErrorRate {
		fmt.Fprintf(stderr, "loadgen: %.1f%% of requests failed in transport (limit %.0f%%)\n", 100*r, 100*maxTransportErrorRate)
		return 1
	}
	return 0
}

func parseFlags(args []string, output io.Writer) (config, error) {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	fs.SetOutput(output)
	var cfg config
	fs.StringVar(&cfg.target, "target", "http://localhost:10000", "base URL of the demo API, normally the Envoy listener")
	fs.Float64Var(&cfg.rps, "rps", 20, "requests per second; 0 sends as fast as --concurrency allows")
	fs.IntVar(&cfg.requests, "requests", 200, "requests to send; 0 means run for --duration")
	fs.DurationVar(&cfg.duration, "duration", 0, "how long to send; with --requests, whichever limit comes first")
	fs.IntVar(&cfg.concurrency, "concurrency", 8, "maximum requests in flight")
	fs.Int64Var(&cfg.seed, "seed", 1, "seed for the fake people and the request mix")
	fs.IntVar(&cfg.people, "people", 1000, "number of fake people to draw from")
	fs.DurationVar(&cfg.timeout, "timeout", 10*time.Second, "per-request timeout")
	fs.StringVar(&cfg.piiOut, "pii-out", "", "write every personal-data string sent to this file, one per line")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return config{}, err
		}
		return config{}, errUsage
	}

	var problems []string
	if fs.NArg() > 0 {
		problems = append(problems, fmt.Sprintf("unexpected arguments %q", fs.Args()))
	}
	if u, err := url.Parse(cfg.target); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		problems = append(problems, fmt.Sprintf("--target %q must be an http(s) URL", cfg.target))
	}
	if cfg.rps < 0 || math.IsNaN(cfg.rps) || math.IsInf(cfg.rps, 0) {
		problems = append(problems, "--rps must be a finite number >= 0")
	}
	if cfg.requests < 0 || cfg.duration < 0 {
		problems = append(problems, "--requests and --duration must not be negative")
	}
	if cfg.requests == 0 && cfg.duration == 0 {
		problems = append(problems, "--requests 0 needs a --duration")
	}
	if cfg.concurrency < 1 || cfg.people < 1 {
		problems = append(problems, "--concurrency and --people must be at least 1")
	}
	if cfg.timeout <= 0 {
		problems = append(problems, "--timeout must be positive")
	}
	if len(problems) > 0 {
		for _, p := range problems {
			fmt.Fprintf(output, "loadgen: %s\n", p)
		}
		return config{}, errUsage
	}
	return cfg, nil
}

// run sends traffic until the request count or duration is reached or ctx
// is done, then waits for in-flight requests. It returns a summary even when
// writing the ledger fails.
func run(ctx context.Context, cfg config) (*summary, error) {
	ledger := traffic.NewLedger()
	gen := traffic.NewGenerator(cfg.seed, traffic.People(cfg.seed, cfg.people), ledger)

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = cfg.concurrency
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		// Report redirects as they are rather than following them.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	if cfg.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.duration)
		defer cancel()
	}
	limit := rate.Inf
	if cfg.rps > 0 {
		limit = rate.Limit(cfg.rps)
	}
	limiter := rate.NewLimiter(limit, 1)

	reqs := make(chan *http.Request)
	rec := newRecorder()
	var wg sync.WaitGroup
	for range cfg.concurrency {
		wg.Go(func() {
			for req := range reqs {
				rec.record(send(client, req, cfg.timeout))
			}
		})
	}
	start := time.Now()
	genErr := dispatch(ctx, cfg, gen, limiter, reqs)
	close(reqs)
	wg.Wait()

	sum := rec.summary(time.Since(start))
	if genErr != nil {
		return sum, genErr
	}
	if cfg.piiOut != "" {
		if err := ledger.WriteFile(cfg.piiOut); err != nil {
			return sum, err
		}
		sum.ledgerPath, sum.ledgerValues = cfg.piiOut, ledger.Len()
	}
	return sum, nil
}

// dispatch generates paced requests until the limit is reached or ctx is
// done. Generation happens on this one goroutine, so a seed always yields
// the same request sequence whatever the concurrency.
func dispatch(ctx context.Context, cfg config, gen *traffic.Generator, limiter *rate.Limiter, reqs chan<- *http.Request) error {
	for n := 0; cfg.requests == 0 || n < cfg.requests; n++ {
		if limiter.Wait(ctx) != nil {
			return nil // duration elapsed or interrupted
		}
		req, err := gen.Next(cfg.target)
		if err != nil {
			return err
		}
		req.Header.Set("User-Agent", "shadowtap-loadgen/"+version)
		select {
		case reqs <- req:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

// outcome is the result of one request. status is 0 when no complete
// response arrived.
type outcome struct {
	status   int
	expected int
	latency  time.Duration
	err      error
}

func send(client *http.Client, req *http.Request, timeout time.Duration) outcome {
	exp, _ := traffic.ExpectationOf(req)
	// Derive from the request's context to keep the expectation, and not
	// from the run's, so that stopping lets in-flight requests finish.
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	start := time.Now()
	resp, err := client.Do(req.WithContext(ctx))
	if err != nil {
		return outcome{expected: exp.Status, latency: time.Since(start), err: err}
	}
	_, err = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if err != nil {
		return outcome{expected: exp.Status, latency: time.Since(start), err: err}
	}
	return outcome{status: resp.StatusCode, expected: exp.Status, latency: time.Since(start)}
}
