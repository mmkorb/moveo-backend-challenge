// Command consumer is the composition root of the consumer service (ADR 0003,
// 0012). Subcommands: run (default), migrate and healthcheck.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
)

// errUsage marks a command line mistake; main exits with status 2 for it.
var errUsage = errors.New("usage error")

const usage = `usage: consumer [run|migrate|healthcheck]

  run          consume incoming.user_activity and maintain activity_aggregates (default)
  migrate      apply the embedded database migrations (needs the owner credentials)
  healthcheck  GET /readyz on the local health server; exit 0 when ready
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	context.AfterFunc(ctx, stop) // a second signal now terminates immediately (ADR 0012)
	err := dispatch(ctx, os.Args[1:], os.Stdout)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "consumer:", err)
		if errors.Is(err, errUsage) {
			fmt.Fprint(os.Stderr, usage)
			os.Exit(2)
		}
		os.Exit(1)
	}
}

// dispatch runs the subcommand named by args[0] ("run" when there is none).
func dispatch(ctx context.Context, args []string, stdout io.Writer) error {
	cmd := "run"
	if len(args) > 0 {
		cmd = args[0]
		args = args[1:]
	}
	if len(args) > 0 {
		return fmt.Errorf("%w: %s takes no arguments", errUsage, cmd)
	}
	switch cmd {
	case "run":
		return run(ctx, stdout)
	case "migrate":
		return migrate(ctx, stdout)
	case "healthcheck":
		return healthcheck(ctx)
	default:
		return fmt.Errorf("%w: unknown command %q", errUsage, cmd)
	}
}
