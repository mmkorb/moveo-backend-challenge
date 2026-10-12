package retry

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"
)

var errTransient = errors.New("transient")

func TestDo(t *testing.T) {
	t.Parallel()
	errPermanent := errors.New("permanent")
	policy := Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second}
	tests := []struct {
		name         string
		failures     int   // op fails this many times before succeeding
		failWith     error // error returned while failing
		wantAttempts int
		wantErr      error
	}{
		{name: "should return nil after one attempt when op succeeds", wantAttempts: 1},
		{name: "should retry until op succeeds", failures: 3, failWith: errTransient, wantAttempts: 4},
		{name: "should stop at once when the error is not retryable", failures: 5, failWith: errPermanent,
			wantAttempts: 1, wantErr: errPermanent},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				attempts, retries := 0, 0
				err := Do(t.Context(), policy,
					func(err error) bool { return !errors.Is(err, errPermanent) },
					func(attempt int, wait time.Duration, err error) {
						retries++
						if attempt != retries {
							t.Errorf("onRetry attempt = %d, want %d", attempt, retries)
						}
						if ceil := min(policy.Base<<(attempt-1), policy.Max); wait < 0 || wait > ceil {
							t.Errorf("wait %v outside [0, %v]", wait, ceil)
						}
					},
					func(context.Context) error {
						attempts++
						if attempts <= tt.failures {
							return tt.failWith
						}
						return nil
					})
				if !errors.Is(err, tt.wantErr) || (tt.wantErr == nil && err != nil) {
					t.Errorf("Do() error = %v, want %v", err, tt.wantErr)
				}
				if attempts != tt.wantAttempts {
					t.Errorf("attempts = %d, want %d", attempts, tt.wantAttempts)
				}
			})
		})
	}
}

func TestDoGivesUpWhenTheDeadlineExpires(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		start := time.Now()
		err := Do(ctx, Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second}, nil, nil,
			func(context.Context) error { return errTransient })
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errTransient) {
			t.Errorf("Do() error = %v, want DeadlineExceeded wrapping the last error", err)
		}
		if elapsed := time.Since(start); elapsed > 60*time.Second {
			t.Errorf("Do() returned after %v, want <= 60s", elapsed)
		}
	})
}

func TestDoReturnsPromptlyWhenCancelledDuringBackoff(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		start := time.Now()
		err := Do(ctx, Policy{Base: time.Hour, Max: time.Hour}, nil,
			func(int, time.Duration, error) { time.AfterFunc(time.Second, cancel) },
			func(context.Context) error { return errTransient })
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Do() error = %v, want context.Canceled", err)
		}
		// The wait is drawn from [0, 1h]; cancellation must cut it short.
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("Do() returned after %v, want <= 1s", elapsed)
		}
	})
}

func TestPolicyValidate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		p       Policy
		wantErr bool
	}{
		{"should accept base below max", Policy{Base: time.Millisecond, Max: time.Second}, false},
		{"should reject a zero base", Policy{Max: time.Second}, true},
		{"should reject max below base", Policy{Base: time.Second, Max: time.Millisecond}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if err := tt.p.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCeiling(t *testing.T) {
	maxDur := time.Duration(math.MaxInt64)
	tests := []struct {
		name    string
		p       Policy
		attempt int
		want    time.Duration
	}{
		{"first attempt is base", Policy{time.Second, time.Minute}, 0, time.Second},
		{"doubles each attempt", Policy{time.Second, time.Minute}, 3, 8 * time.Second},
		{"caps at max", Policy{time.Second, time.Minute}, 10, time.Minute},
		{"huge base does not overflow", Policy{time.Duration(1) << 62, maxDur}, 5, maxDur},
		{"huge base and many attempts", Policy{time.Duration(1) << 40, time.Duration(1) << 62}, 1000, time.Duration(1) << 62},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ceiling(tt.p, tt.attempt); got != tt.want || got <= 0 {
				t.Errorf("ceiling(%v, %d) = %v, want %v", tt.p, tt.attempt, got, tt.want)
			}
		})
	}
}
