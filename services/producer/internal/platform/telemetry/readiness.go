package telemetry

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrNotReady is returned by Readiness.Check once MarkNotReady was called.
var ErrNotReady = errors.New("not ready: shutting down")

// Readiness answers /readyz. Dependency checks are cached for a short TTL so
// frequent probes do not hammer Kafka, and MarkNotReady flips it to failing
// for good at the first shutdown signal (ADR 0012, stage 1).
type Readiness struct {
	check func(context.Context) error
	ttl   time.Duration
	now   func() time.Time

	notReady atomic.Bool

	mu        sync.Mutex
	checkedAt time.Time
	last      error
}

// NewReadiness returns a Readiness that runs check at most once per ttl.
func NewReadiness(check func(context.Context) error, ttl time.Duration) *Readiness {
	return &Readiness{check: check, ttl: ttl, now: time.Now}
}

// MarkNotReady makes every later Check fail with ErrNotReady.
func (r *Readiness) MarkNotReady() { r.notReady.Store(true) }

// Check reports whether the service can do useful work. It returns the cached
// result of the dependency check when it is younger than the TTL.
func (r *Readiness) Check(ctx context.Context) error {
	if r.notReady.Load() {
		return ErrNotReady
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.checkedAt.IsZero() && r.now().Sub(r.checkedAt) < r.ttl {
		return r.last
	}
	err := r.check(ctx)
	if ctx.Err() == nil { // do not cache a result caused by the caller giving up
		r.last, r.checkedAt = err, r.now()
	}
	return err
}
