package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/retry"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// startupBackoff is the retry policy for dependencies at startup (ADR 0011).
type startupBackoff struct {
	Timeout   time.Duration // total budget (STARTUP_TIMEOUT)
	Base, Max time.Duration // exponential backoff with full jitter (RETRY_BASE_DELAY, RETRY_MAX_DELAY)
}

// retryUnavailable retries only transient failures. Anything else (a wrong password,
// a missing grant) fails the startup immediately instead of burning the budget.
func retryUnavailable(err error) bool { return errors.Is(err, ports.ErrUnavailable) }

// retryStartup runs op with retry.Do until it succeeds, fails with an error that
// retryable rejects, or the STARTUP_TIMEOUT budget ends or ctx is done (ADR 0011
// "Startup dependencies"). Every error is prefixed with the dependency's name.
func retryStartup(ctx context.Context, b startupBackoff, logger *slog.Logger, dependency string, retryable func(error) bool, op func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, b.Timeout)
	defer cancel()
	onRetry := func(attempt int, wait time.Duration, err error) {
		if attempt == 1 || attempt%10 == 0 {
			logger.WarnContext(ctx, "dependency not available yet, retrying",
				"dependency", dependency, "attempt", attempt, "backoff", wait.String(), "error", err)
		}
	}
	if err := retry.Do(ctx, retry.Policy{Base: b.Base, Max: b.Max}, retryable, onRetry, op); err != nil {
		return fmt.Errorf("%s: %w", dependency, err)
	}
	return nil
}
