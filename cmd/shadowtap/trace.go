package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/Jenil133/Shadowtap/internal/tracequery"
)

// Environment variables trace reads for the Elasticsearch connection. They
// are the ones the indexer reads, so one shell profile serves both.
const (
	envESURL      = "SHADOWTAP_ES_URL"
	envESUsername = "SHADOWTAP_ES_USERNAME"
	envESPassword = "SHADOWTAP_ES_PASSWORD"
	envESAPIKey   = "SHADOWTAP_ES_API_KEY"
)

// exitNotFound is trace's exit status when nothing is indexed for the id, so
// scripts can tell "not there (yet)" from a broken connection (exitError).
const exitNotFound = 4

const traceUsage = `Usage: shadowtap trace [flags] <trace-id>
       shadowtap trace [flags] --request-id <x-request-id>
       shadowtap trace [flags] --capture-id <capture-id>

Shows everything indexed for one request: its capture and replay diffs, then
the span waterfall across every service it touched with each span's logs
beneath it. --request-id and --capture-id resolve the trace through the
captures index.

Exit status: 0 found, 1 error (Elasticsearch unreachable or failing), 2 usage,
4 nothing indexed for the id.

Flags:
`

type traceOptions struct {
	es       string
	username string
	password string
	apiKey   string
	caFile   string
	insecure bool
	timeout  time.Duration

	requestID    string
	captureID    string
	json         bool
	showBodies   bool
	followReplay bool
	width        int
}

func runTrace(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	var o traceOptions
	fs := flag.NewFlagSet("shadowtap trace", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprint(stderr, traceUsage)
		fs.PrintDefaults()
	}
	defaultES := getenv(envESURL)
	if defaultES == "" {
		defaultES = tracequery.DefaultURL
	}
	fs.StringVar(&o.es, "es", defaultES, "Elasticsearch URL(s), comma-separated (default $"+envESURL+")")
	fs.StringVar(&o.username, "es-username", getenv(envESUsername), "basic auth user (default $"+envESUsername+")")
	fs.StringVar(&o.password, "es-password", getenv(envESPassword), "basic auth password; prefer $"+envESPassword+" so it stays out of the shell history")
	fs.StringVar(&o.apiKey, "es-api-key", getenv(envESAPIKey), "API key (base64 id:key), wins over basic auth (default $"+envESAPIKey+")")
	fs.StringVar(&o.caFile, "es-ca", "", "PEM CA bundle to verify the Elasticsearch certificate (default: system roots)")
	fs.BoolVar(&o.insecure, "es-insecure", false, "do not verify the Elasticsearch certificate (testing only)")
	fs.DurationVar(&o.timeout, "timeout", tracequery.DefaultTimeout, "give up on a query after this long")
	fs.StringVar(&o.requestID, "request-id", "", "find the trace through the capture with this x-request-id")
	fs.StringVar(&o.captureID, "capture-id", "", "find the trace through the capture with this id")
	fs.BoolVar(&o.json, "json", false, "print one JSON document instead of text")
	fs.BoolVar(&o.showBodies, "show-bodies", false, "include the scrubbed request and response bodies of captures")
	fs.BoolVar(&o.followReplay, "follow-replay", false, "also show the replay traces of this request, or the original request of a replay trace")
	fs.IntVar(&o.width, "width", 0, "width of the waterfall bars in characters (default 30)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}

	q := tracequery.Query{RequestID: o.requestID, CaptureID: o.captureID, FollowReplay: o.followReplay}
	switch fs.NArg() {
	case 0:
	case 1:
		q.TraceID = fs.Arg(0)
	default:
		fmt.Fprintf(stderr, "shadowtap trace: one trace id expected, got %q\n", fs.Args())
		return exitUsage
	}
	if err := q.Validate(); err != nil {
		fmt.Fprintf(stderr, "shadowtap trace: %v\n", err)
		return exitUsage
	}
	if o.timeout <= 0 {
		fmt.Fprintln(stderr, "shadowtap trace: --timeout must be positive")
		return exitUsage
	}

	client, err := tracequery.New(tracequery.Config{
		URL:      o.es,
		Username: o.username,
		Password: o.password,
		APIKey:   o.apiKey,
		CAFile:   o.caFile,
		Insecure: o.insecure,
		Timeout:  o.timeout,
	})
	if err != nil {
		fmt.Fprintf(stderr, "shadowtap trace: %v\n", err)
		return exitUsage
	}

	res, err := client.Lookup(ctx, q)
	switch {
	case errors.Is(err, tracequery.ErrNotFound):
		fmt.Fprintf(stderr, "shadowtap trace: %v (nothing indexed yet, or the wrong cluster at %s)\n", err, client.URL())
		return exitNotFound
	case errors.Is(err, context.Canceled):
		fmt.Fprintln(stderr, "shadowtap trace: interrupted")
		return exitError
	case err != nil:
		fmt.Fprintf(stderr, "shadowtap trace: %v\n", err)
		return exitError
	}

	if o.json {
		err = tracequery.WriteJSON(stdout, res, o.showBodies)
	} else {
		err = tracequery.Render(stdout, res, tracequery.RenderOptions{ShowBodies: o.showBodies, BarWidth: o.width})
	}
	if err != nil {
		fmt.Fprintf(stderr, "shadowtap trace: %v\n", err)
		return exitError
	}
	return exitOK
}
