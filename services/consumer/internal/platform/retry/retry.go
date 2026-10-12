// Package retry is the consumer's single backoff helper (ADR 0011): exponential
// backoff with full jitter, no attempt limit, and waits that end as soon as the
// context is done. It never calls time.Sleep.
package retry

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

// Policy is the backoff policy: the ceiling of attempt n (0-based) is
// min(Base·2ⁿ, Max), and each wait is uniform in [0, ceiling].
type Policy struct{ Base, Max time.Duration }

// Validate reports whether the policy is usable.
func (p Policy) Validate() error {
	if p.Base <= 0 || p.Max < p.Base {
		return errors.New("retry: policy needs 0 < Base <= Max")
	}
	return nil
}

// Do runs op until it succeeds, op's error is not retryable, or ctx is done.
// retryable and onRetry may be nil (retry everything; no callback).
//
// onRetry is called before every wait with the 1-based number of the attempt
// that just failed, the wait that follows and that attempt's error. When ctx is
// done, Do returns an error wrapping both ctx.Err() and the last error of op.
func Do(ctx context.Context, p Policy, retryable func(error) bool, onRetry func(attempt int, wait time.Duration, err error), op func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		err := op(ctx)
		if err == nil {
			return nil
		}
		if retryable != nil && !retryable(err) {
			return err
		}
		if ctx.Err() != nil {
			return fmt.Errorf("retry aborted after %d attempts: %w (last error: %w)", attempt+1, ctx.Err(), err)
		}
		wait := rand.N(ceiling(p, attempt) + 1) //nolint:gosec // G404: jitter for backoff, not security. Full jitter: uniform in [0, ceiling]
		if onRetry != nil {
			onRetry(attempt+1, wait, err)
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("retry aborted after %d attempts: %w (last error: %w)", attempt+1, ctx.Err(), err)
		case <-t.C:
		}
	}
}

// ceiling returns min(Base·2ⁿ, Max) for the 0-based attempt n without ever
// overflowing time.Duration: it doubles step by step and stops at Max.
func ceiling(p Policy, attempt int) time.Duration {
	d := p.Base
	for range attempt {
		if d >= p.Max || d > p.Max/2 {
			return p.Max
		}
		d *= 2
	}
	return min(d, p.Max)
}
