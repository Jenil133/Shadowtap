// Command shadowtap is the Shadowtap operator CLI.
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

var version = "dev"

// Exit codes.
const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2
	exitPII   = 3 // tail --check-pii found unscrubbed personal data
	// replay exits with exitGateFailed (1) when the gate fails; trace exits
	// with exitNotFound (4) when nothing is indexed for the id.
)

const usage = `Usage: shadowtap <command> [flags]

Commands:
  tail      print captures from Kafka, or audit them for unscrubbed PII
  replay    send a window of captures to a candidate build and diff its responses
  trace     follow one request across every service it touched, from Elasticsearch
  version   print the version

Run "shadowtap <command> -h" for the command's flags.
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return exitUsage
	}
	switch args[0] {
	case "tail":
		return runTail(ctx, args[1:], getenv, stdout, stderr)
	case "replay":
		return runReplay(ctx, args[1:], getenv, stdout, stderr)
	case "trace":
		return runTrace(ctx, args[1:], getenv, stdout, stderr)
	case "version", "--version":
		fmt.Fprintln(stdout, version)
		return exitOK
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return exitOK
	default:
		fmt.Fprintf(stderr, "shadowtap: unknown command %q\n\n%s", args[0], usage)
		return exitUsage
	}
}
