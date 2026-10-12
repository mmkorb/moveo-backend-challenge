// Package kafka is the consumer's Kafka adapter (ADR 0006, 0007, 0008, 0011,
// 0012, 0013): group members with one worker goroutine per assigned partition,
// per-partition micro-batching, the dead-letter publisher and the startup ping.
// It is the only package of the consumer that imports franz-go.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/retry"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

const (
	// defaultDrainTimeout bounds the shutdown stages 2-4 of ADR 0012 when
	// Config.DrainTimeout is zero.
	defaultDrainTimeout = 15 * time.Second
	// defaultLagInterval is how often consumer_lag_records is refreshed (ADR 0013).
	defaultLagInterval = 15 * time.Second
	// commitTimeout bounds every explicit CommitMarkedOffsets call.
	commitTimeout = 5 * time.Second
	// stopGrace is how long a stopper still waits for a worker whose flush
	// context has already been cancelled.
	stopGrace = 2 * time.Second
	// defaultFlushTimeout bounds one flush attempt when Config.FlushTimeout is
	// zero.
	defaultFlushTimeout = 10 * time.Second
	// defaultSessionTimeout is the group session timeout when
	// Config.SessionTimeout is zero (ADR 0012).
	defaultSessionTimeout = 30 * time.Second
	// leaveTimeout bounds the explicit LeaveGroup of a static member when the
	// drain deadline has already passed.
	leaveTimeout = 2 * time.Second
)

// Config holds everything the Kafka adapter needs. cmd maps the platform
// config onto it; the zero value of an optional field selects its default.
type Config struct {
	Brokers            []string
	Topic, DLQTopic    string
	Group              string
	FlushInterval      time.Duration // FLUSH_INTERVAL
	FlushMaxRecords    int           // FLUSH_MAX_RECORDS
	RevokeFlushTimeout time.Duration // REVOKE_FLUSH_TIMEOUT
	Retry              retry.Policy  // RETRY_BASE_DELAY / RETRY_MAX_DELAY

	// DrainTimeout bounds flush + commit + leave on shutdown (DRAIN_TIMEOUT,
	// ADR 0012). Optional: zero means 15s.
	DrainTimeout time.Duration
	// LagInterval is the refresh period of consumer_lag_records. Optional:
	// zero means 15s.
	LagInterval time.Duration
	// FlushTimeout bounds one flush attempt (FLUSH_TIMEOUT, ADR 0011): a
	// dependency that does not answer in time fails the attempt with a
	// transient error, and the worker retries. Optional: zero means 10s.
	FlushTimeout time.Duration
	// InstanceID turns on static group membership (CONSUMER_INSTANCE_ID,
	// ADR 0012): member i joins as "<InstanceID>-<i>", so a restarted
	// replica takes its partitions back without waiting for the session
	// timeout. Optional: empty means dynamic membership.
	InstanceID string
	// SessionTimeout is the group session timeout (CONSUMER_SESSION_TIMEOUT).
	// Optional: zero means 30s.
	SessionTimeout time.Duration
}

// BatchProcessor runs one flush attempt for a partition batch: dead letters
// first, then the folded aggregate deltas together with the move of the
// partition's stored position from cp.From to cp.To (ADR 0008). An error
// wrapping ports.ErrFenced means another owner moved the position. It is
// declared here, where it is consumed, and satisfied by
// *application.ProcessActivity.
type BatchProcessor interface {
	Process(ctx context.Context, cp ports.Checkpoint, events []domain.ActivityEvent, rejected []ports.DeadLetter) error
}

// PositionLoader returns the stored next offset of each given partition;
// partitions without a stored position are absent (ADR 0008). It is satisfied
// by *application.ResumePositions.
type PositionLoader interface {
	NextOffsets(ctx context.Context, group, topic string, partitions []int32) (map[int32]int64, error)
}

func (c Config) validateClient() error {
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("no brokers"))
	}
	if c.Topic == "" {
		errs = append(errs, errors.New("empty topic"))
	}
	if c.DLQTopic == "" {
		errs = append(errs, errors.New("empty dlq topic"))
	}
	if c.Topic != "" && c.Topic == c.DLQTopic {
		errs = append(errs, errors.New("topic and dlq topic must differ"))
	}
	return errors.Join(errs...)
}

func (c Config) validateConsumer() error {
	errs := []error{c.validateClient()}
	if c.Group == "" {
		errs = append(errs, errors.New("empty group"))
	}
	if c.FlushInterval <= 0 {
		errs = append(errs, fmt.Errorf("flush interval %v must be > 0", c.FlushInterval))
	}
	if c.FlushMaxRecords <= 0 {
		errs = append(errs, fmt.Errorf("flush max records %d must be > 0", c.FlushMaxRecords))
	}
	if c.RevokeFlushTimeout <= 0 {
		errs = append(errs, fmt.Errorf("revoke flush timeout %v must be > 0", c.RevokeFlushTimeout))
	}
	if c.Retry.Base <= 0 || c.Retry.Max < c.Retry.Base {
		errs = append(errs, fmt.Errorf("retry policy needs 0 < base (%v) <= max (%v)", c.Retry.Base, c.Retry.Max))
	}
	if c.DrainTimeout < 0 || c.LagInterval < 0 || c.FlushTimeout < 0 || c.SessionTimeout < 0 {
		errs = append(errs, errors.New("drain, flush and session timeouts and the lag interval must not be negative"))
	}
	return errors.Join(errs...)
}

func (c Config) withDefaults() Config {
	if c.DrainTimeout == 0 {
		c.DrainTimeout = defaultDrainTimeout
	}
	if c.LagInterval == 0 {
		c.LagInterval = defaultLagInterval
	}
	if c.FlushTimeout == 0 {
		c.FlushTimeout = defaultFlushTimeout
	}
	if c.SessionTimeout == 0 {
		c.SessionTimeout = defaultSessionTimeout
	}
	return c
}
