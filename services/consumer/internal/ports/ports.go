// Package ports declares the interfaces the consumer's application layer needs
// from the outside world (ADR 0003, 0004). Only domain and the stdlib are imported.
package ports

import (
	"context"
	"errors"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
)

// ErrUnavailable marks a transient failure (ADR 0011). Adapters wrap retriable
// driver errors with it: fmt.Errorf("%w: %w", ports.ErrUnavailable, err).
var ErrUnavailable = errors.New("dependency unavailable")

// ErrDataException marks a failure the database reports as a data exception
// (SQLSTATE class 22: NUL in text, invalid UTF-8, numeric overflow...). It is a
// defense in depth behind domain validation: the batch is aggregated, so the
// offending record cannot be identified and the batch is not dead-lettered.
// The worker still retries it, but labels it apart from "unknown" (ADR 0011).
var ErrDataException = errors.New("database rejected the data")

// ErrorClass is the permanent-error class of a dead-lettered record (ADR 0011).
type ErrorClass string

const (
	ErrorClassDecode     ErrorClass = "decode"
	ErrorClassValidation ErrorClass = "validation"
)

// Header is one Kafka record header, copied unchanged from the original record.
type Header struct {
	Key   string
	Value []byte
}

// DeadLetter is a record that can never be processed. It carries the original
// bytes and coordinates, never the error text (ADR 0011, 0002).
type DeadLetter struct {
	Key       []byte
	Value     []byte
	Headers   []Header
	Topic     string
	Partition int32
	Offset    int64
	Class     ErrorClass
}

// ErrFenced reports that a Checkpoint did not match the stored position of its
// partition: another owner advanced it (or created it) first. Nothing of the
// batch was written. The caller has lost the partition and must drop the batch
// without retrying it (ADR 0008).
var ErrFenced = errors.New("partition position fenced by another owner")

// NoPosition is Checkpoint.From when no position is stored for the partition
// yet: the write then creates it, and fails with ErrFenced if it already exists.
const NoPosition int64 = -1

// Checkpoint moves the stored position of one partition of a consumer group
// from From to To (ADR 0008). Positions are "next offset to read": To is the
// offset of the last record of the batch plus one. From is the position the
// owner loaded or last wrote, or NoPosition; it acts as a fencing token, so a
// stale owner can never apply a batch twice.
type Checkpoint struct {
	Group     string
	Topic     string
	Partition int32
	From      int64
	To        int64
}

// Validate checks the invariants every adapter relies on: non-empty names, a
// non-negative partition, From = NoPosition or From >= 0, and To > From.
func (c Checkpoint) Validate() error {
	switch {
	case c.Group == "" || c.Topic == "":
		return errors.New("checkpoint: empty group or topic")
	case c.Partition < 0:
		return errors.New("checkpoint: negative partition")
	case c.From < NoPosition:
		return errors.New("checkpoint: from below NoPosition")
	case c.To <= c.From || c.To <= 0:
		return errors.New("checkpoint: to must be > from and > 0")
	}
	return nil
}

// AggregateRepository adds deltas to activity_aggregates AND advances the
// partition's stored position in ONE transaction (ADR 0009, 0008, 0010), so a
// batch is applied exactly once per position. Deltas have unique
// (ActivityDate, UserID) keys and may be empty (a batch of dead letters still
// moves the position). If the stored position is not cp.From, nothing is
// written and the error wraps ErrFenced.
type AggregateRepository interface {
	ApplyDeltas(ctx context.Context, cp Checkpoint, deltas []domain.DailyUserAggregate) error
}

// OffsetStore reads the stored positions of a consumer group (ADR 0008). The
// result maps partition to next offset; partitions without a stored position
// are absent.
type OffsetStore interface {
	NextOffsets(ctx context.Context, group, topic string, partitions []int32) (map[int32]int64, error)
}

// DeadLetterPublisher parks records on the DLQ topic. It returns only after the
// broker acknowledged every record; a failure is transient (ADR 0011).
type DeadLetterPublisher interface {
	Publish(ctx context.Context, letters []DeadLetter) error
}
