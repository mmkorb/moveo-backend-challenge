// Command producer is the REST to Kafka ingestion service. This package is
// the composition root: it is the only place that names concrete adapters
// (ADR 0003).
//
// Usage:
//
//	producer [run]        serve the API (default)
//	producer healthcheck  probe GET /readyz on loopback; exit 0 or 1
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// healthcheckTimeout bounds the probe made by the healthcheck subcommand.
const healthcheckTimeout = 2 * time.Second

func main() {
	if err := dispatch(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "producer:", err)
		os.Exit(1)
	}
}

// dispatch runs the subcommand named by args.
func dispatch(args []string) error {
	cmd := "run"
	if len(args) > 0 {
		cmd = args[0]
	}
	switch cmd {
	case "run":
		return serve()
	case "healthcheck":
		ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
		defer cancel()
		return healthcheck(ctx)
	default:
		return fmt.Errorf("unknown command %q (want run or healthcheck)", cmd)
	}
}

// serve runs the service until SIGINT or SIGTERM.
func serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	context.AfterFunc(ctx, stop) // a second signal now terminates immediately (ADR 0012)
	err := run(ctx, os.Stdout)
	stop()
	if err != nil {
		return fmt.Errorf("run: %w", err)
	}
	return nil
}
