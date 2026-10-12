// Package application holds the consumer's use cases (ADR 0003, 0004). It depends
// only on domain and ports, never on adapters or platform code, and it never logs.
package application

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// ProcessActivity folds a batch of accepted ActivityEvents into DailyUserAggregate
// deltas and persists them; records that failed decoding or validation are parked
// on the dead-letter queue first (ADR 0008, 0011).
type ProcessActivity struct {
	repo ports.AggregateRepository
	dlq  ports.DeadLetterPublisher
}

// NewProcessActivity returns the use case wired to its two outbound ports.
func NewProcessActivity(repo ports.AggregateRepository, dlq ports.DeadLetterPublisher) *ProcessActivity {
	return &ProcessActivity{repo: repo, dlq: dlq}
}

// Process runs ONE flush attempt for one partition batch (ADR 0008):
//
//  1. publish rejected records (if any) to the DLQ;
//  2. fold events into one delta per (activity date, user), sorted by that key;
//  3. apply all deltas AND move the partition's stored position from cp.From to
//     cp.To with a single AggregateRepository call (one transaction).
//
// Step 3 runs even when every record of the batch was rejected: dead-lettered
// records advance the position too. It does not retry: the partition worker
// retries the whole flush (ADR 0011). If the DLQ publish fails, nothing is
// written to the repository. Errors keep the ports' sentinels
// (ports.ErrUnavailable, ports.ErrFenced) in their chain. A batch without
// events and dead letters is a no-op.
func (p *ProcessActivity) Process(ctx context.Context, cp ports.Checkpoint, events []domain.ActivityEvent, rejected []ports.DeadLetter) error {
	if len(events) == 0 && len(rejected) == 0 {
		return nil
	}
	if err := cp.Validate(); err != nil {
		return err
	}
	if len(rejected) > 0 {
		if err := p.dlq.Publish(ctx, rejected); err != nil {
			return fmt.Errorf("publish %d dead letters: %w", len(rejected), err)
		}
	}
	deltas, err := fold(events)
	if err != nil {
		return err
	}
	if err := p.repo.ApplyDeltas(ctx, cp, deltas); err != nil {
		return fmt.Errorf("apply %d aggregate deltas up to offset %d: %w", len(deltas), cp.To, err)
	}
	return nil
}

// ResumePositions tells a new partition owner where to resume (ADR 0008): the
// positions stored with the aggregates are the source of truth, not the
// offsets committed to Kafka.
type ResumePositions struct {
	store ports.OffsetStore
}

// NewResumePositions returns the use case wired to its outbound port.
func NewResumePositions(store ports.OffsetStore) *ResumePositions {
	return &ResumePositions{store: store}
}

// NextOffsets returns the stored next offset of each given partition of
// (group, topic); partitions without a stored position are absent. An empty
// partition list is a no-op.
func (r *ResumePositions) NextOffsets(ctx context.Context, group, topic string, partitions []int32) (map[int32]int64, error) {
	if len(partitions) == 0 {
		return map[int32]int64{}, nil
	}
	next, err := r.store.NextOffsets(ctx, group, topic, partitions)
	if err != nil {
		return nil, fmt.Errorf("load stored positions of %d partitions: %w", len(partitions), err)
	}
	return next, nil
}

// aggregateKey identifies one row of activity_aggregates. The date is kept as a
// Unix second of UTC midnight because time.Time is not a reliable map key.
type aggregateKey struct {
	day    int64
	userID string
}

// fold returns one delta per (ActivityDate, UserID), sorted by date then user so
// that the repository always takes row locks in the same order (ADR 0009).
func fold(events []domain.ActivityEvent) ([]domain.DailyUserAggregate, error) {
	byKey := make(map[aggregateKey]*domain.DailyUserAggregate)
	for _, e := range events {
		date := e.ActivityDate()
		k := aggregateKey{day: date.Unix(), userID: e.UserID}
		agg, ok := byKey[k]
		if !ok {
			agg = &domain.DailyUserAggregate{ActivityDate: date, UserID: e.UserID}
			byKey[k] = agg
		}
		if err := agg.Apply(e); err != nil {
			return nil, fmt.Errorf("fold activity event: %w", err)
		}
	}
	deltas := make([]domain.DailyUserAggregate, 0, len(byKey))
	for _, agg := range byKey {
		deltas = append(deltas, *agg)
	}
	slices.SortFunc(deltas, compareKeys)
	return deltas, nil
}

func compareKeys(a, b domain.DailyUserAggregate) int {
	return cmp.Or(a.ActivityDate.Compare(b.ActivityDate), cmp.Compare(a.UserID, b.UserID))
}
