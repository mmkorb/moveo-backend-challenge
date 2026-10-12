package retry

import (
	"context"
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"
)

var errBoom = errors.New("boom")

func always(error) bool { return true }

func TestDo(t *testing.T) {
	t.Run("should return nil without retrying when op succeeds first", func(t *testing.T) {
		calls := 0
		err := Do(t.Context(), Policy{Base: time.Millisecond, Max: time.Second}, always,
			func(int, time.Duration, error) { t.Fatal("onRetry must not be called") },
			func(context.Context) error { calls++; return nil })
		if err != nil || calls != 1 {
			t.Fatalf("Do() = %v after %d calls, want nil after 1", err, calls)
		}
	})

	t.Run("should return a non-retryable error immediately", func(t *testing.T) {
		calls := 0
		err := Do(t.Context(), Policy{Base: time.Millisecond, Max: time.Second},
			func(error) bool { return false },
			func(int, time.Duration, error) { t.Fatal("onRetry must not be called") },
			func(context.Context) error { calls++; return errBoom })
		if !errors.Is(err, errBoom) || calls != 1 {
			t.Fatalf("Do() = %v after %d calls, want errBoom after 1", err, calls)
		}
	})

	t.Run("should retry until success with jittered waits bounded by the ceiling", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := Policy{Base: 100 * time.Millisecond, Max: 400 * time.Millisecond}
			var attempts []int
			calls := 0
			err := Do(t.Context(), p, always,
				func(attempt int, wait time.Duration, err error) {
					attempts = append(attempts, attempt)
					ceil := min(p.Base<<(attempt-1), p.Max)
					if wait < 0 || wait > ceil {
						t.Errorf("attempt %d: wait %v outside [0, %v]", attempt, wait, ceil)
					}
					if !errors.Is(err, errBoom) {
						t.Errorf("attempt %d: err = %v, want errBoom", attempt, err)
					}
				},
				func(context.Context) error {
					calls++
					if calls < 6 {
						return errBoom
					}
					return nil
				})
			if err != nil {
				t.Fatalf("Do() = %v, want nil", err)
			}
			if want := []int{1, 2, 3, 4, 5}; len(attempts) != len(want) {
				t.Fatalf("onRetry attempts = %v, want %v", attempts, want)
			}
		})
	})

	t.Run("should return promptly when the context is cancelled during a backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			p := Policy{Base: time.Hour, Max: time.Hour}
			done := make(chan error, 1)
			go func() {
				done <- Do(ctx, p, always, func(int, time.Duration, error) {},
					func(context.Context) error { return errBoom })
			}()
			synctest.Wait() // Do is now blocked in its backoff wait
			start := time.Now()
			cancel()
			err := <-done
			if took := time.Since(start); took != 0 {
				t.Errorf("Do returned after %v of fake time, want immediately", took)
			}
			if !errors.Is(err, context.Canceled) || !errors.Is(err, errBoom) {
				t.Fatalf("Do() = %v, want it to wrap context.Canceled and errBoom", err)
			}
		})
	})

	t.Run("should cap the ceiling at Max for large attempt numbers", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			p := Policy{Base: time.Millisecond, Max: 50 * time.Millisecond}
			calls := 0
			err := Do(t.Context(), p, always,
				func(attempt int, wait time.Duration, _ error) {
					if wait > p.Max {
						t.Errorf("attempt %d: wait %v > Max %v", attempt, wait, p.Max)
					}
				},
				func(context.Context) error {
					calls++
					if calls < 40 {
						return errBoom
					}
					return nil
				})
			if err != nil {
				t.Fatalf("Do() = %v, want nil", err)
			}
		})
	})
}

func TestDoAcceptsNilCallbacks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		err := Do(t.Context(), Policy{Base: time.Millisecond, Max: time.Second}, nil, nil,
			func(context.Context) error {
				calls++
				if calls < 3 {
					return errBoom
				}
				return nil
			})
		if err != nil || calls != 3 {
			t.Fatalf("Do() = %v after %d calls, want nil after 3", err, calls)
		}
	})
}

func TestDoGivesUpWhenTheDeadlineExpires(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		defer cancel()
		start := time.Now()
		err := Do(ctx, Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second}, nil, nil,
			func(context.Context) error { return errBoom })
		if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, errBoom) {
			t.Errorf("Do() error = %v, want DeadlineExceeded wrapping the last error", err)
		}
		if elapsed := time.Since(start); elapsed > 60*time.Second {
			t.Errorf("Do() returned after %v, want <= 60s", elapsed)
		}
	})
}

func TestDoDoesNotRetryAfterContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	calls := 0
	err := Do(ctx, Policy{Base: time.Millisecond, Max: time.Second}, always,
		func(int, time.Duration, error) { t.Error("onRetry must not be called once ctx is done") },
		func(context.Context) error { calls++; cancel(); return errBoom })
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("Do() = %v after %d calls, want Canceled wrapping errBoom after 1", err, calls)
	}
}

func TestPolicyValidate(t *testing.T) {
	tests := []struct {
		name    string
		p       Policy
		wantErr bool
	}{
		{"should accept base below max", Policy{Base: time.Millisecond, Max: time.Second}, false},
		{"should accept base equal to max", Policy{Base: time.Second, Max: time.Second}, false},
		{"should reject a zero base", Policy{Max: time.Second}, true},
		{"should reject max below base", Policy{Base: time.Second, Max: time.Millisecond}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
