package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// upsertDeltas is the batched delta upsert of ADR 0009: one statement, one round
// trip, any batch size. It is NOT idempotent: running it twice adds the deltas
// twice. ApplyDeltas therefore only runs it in the transaction that also moves
// the partition's stored position (ADR 0008).
const upsertDeltas = `
INSERT INTO activity_aggregates AS a
    (activity_date, user_id, activity_count, page_view_count)
SELECT d, u, ac, pv
FROM unnest($1::date[], $2::text[], $3::bigint[], $4::bigint[]) AS t(d, u, ac, pv)
ON CONFLICT (activity_date, user_id) DO UPDATE
SET activity_count  = a.activity_count  + EXCLUDED.activity_count,
    page_view_count = a.page_view_count + EXCLUDED.page_view_count,
    updated_at      = now()`

// Position statements (ADR 0008, migration 00002). advanceOffset is a
// compare-and-set: it matches no row unless the stored position is still the
// one the caller expects. Under READ COMMITTED a concurrent writer of the same
// row waits for the first one and then re-checks the WHERE clause against the
// committed row, so only one of two owners can move a position.
const (
	advanceOffset = `
UPDATE consumer_offsets
SET next_offset = $5, updated_at = now()
WHERE consumer_group = $1 AND topic = $2 AND partition = $3 AND next_offset = $4`

	insertOffset = `
INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset)
VALUES ($1, $2, $3, $4)
ON CONFLICT (consumer_group, topic, partition) DO NOTHING`

	selectOffset = `
SELECT next_offset FROM consumer_offsets
WHERE consumer_group = $1 AND topic = $2 AND partition = $3`

	selectOffsets = `
SELECT partition, next_offset FROM consumer_offsets
WHERE consumer_group = $1 AND topic = $2 AND partition = ANY($3::int[])`
)

// AggregateRepository adds daily per-user deltas to activity_aggregates and
// stores the Kafka position they cover in consumer_offsets, atomically.
type AggregateRepository struct{ pool *pgxpool.Pool }

var (
	_ ports.AggregateRepository = (*AggregateRepository)(nil)
	_ ports.OffsetStore         = (*AggregateRepository)(nil)
)

// NewAggregateRepository returns a repository on top of pool. The pool belongs to
// the caller, which closes it.
func NewAggregateRepository(pool *pgxpool.Pool) *AggregateRepository {
	return &AggregateRepository{pool: pool}
}

// ApplyDeltas moves the position of cp's partition from cp.From to cp.To and
// adds the deltas, in one transaction (ADR 0009, 0008):
//
//  1. compare-and-set the position (insert it when cp.From is ports.NoPosition);
//  2. if that matched no row, roll back: the error wraps ports.ErrFenced, unless
//     the stored position already is cp.To — then an earlier attempt of this
//     very batch committed (for example the COMMIT reply was lost) and
//     ApplyDeltas returns nil without writing anything;
//  3. run the batched upsert (skipped for an empty delta list) and commit.
//
// Each (ActivityDate, UserID) key must appear at most once; a duplicate makes
// PostgreSQL reject the statement (SQLSTATE 21000), which is returned as an
// unclassified error. Keys are sorted here as well, so row locks are always taken
// in the same order and concurrent flushes cannot deadlock (ADR 0009).
//
// Transient failures wrap ports.ErrUnavailable. ApplyDeltas never retries.
func (r *AggregateRepository) ApplyDeltas(ctx context.Context, cp ports.Checkpoint, deltas []domain.DailyUserAggregate) error {
	if err := cp.Validate(); err != nil {
		return err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return wrap("begin flush transaction", err)
	}
	// After a successful Commit, Rollback is a no-op returning pgx.ErrTxClosed.
	defer func() { _ = tx.Rollback(ctx) }()

	moved, err := advance(ctx, tx, cp)
	if err != nil {
		return err
	}
	if !moved {
		return fenced(ctx, tx, cp)
	}
	if len(deltas) > 0 {
		dates, users, activities, pageViews := columns(deltas)
		if _, err := tx.Exec(ctx, upsertDeltas, dates, users, activities, pageViews); err != nil {
			return wrap("upsert aggregates", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return wrap("commit flush transaction", err)
	}
	return nil
}

// advance runs the compare-and-set of step 1 and reports whether it matched.
func advance(ctx context.Context, tx pgx.Tx, cp ports.Checkpoint) (bool, error) {
	if cp.From == ports.NoPosition {
		tag, err := tx.Exec(ctx, insertOffset, cp.Group, cp.Topic, cp.Partition, cp.To)
		if err != nil {
			return false, wrap("insert stored position", err)
		}
		return tag.RowsAffected() == 1, nil
	}
	tag, err := tx.Exec(ctx, advanceOffset, cp.Group, cp.Topic, cp.Partition, cp.From, cp.To)
	if err != nil {
		return false, wrap("advance stored position", err)
	}
	return tag.RowsAffected() == 1, nil
}

// fenced is step 2: it tells "already applied" from "another owner moved the
// position". The transaction is rolled back by the caller either way.
func fenced(ctx context.Context, tx pgx.Tx, cp ports.Checkpoint) error {
	var stored int64
	err := tx.QueryRow(ctx, selectOffset, cp.Group, cp.Topic, cp.Partition).Scan(&stored)
	switch {
	case err == nil && stored == cp.To:
		return nil
	case err == nil:
		return fmt.Errorf("%w: %s partition %d: stored next offset %d, expected %d",
			ports.ErrFenced, cp.Topic, cp.Partition, stored, cp.From)
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: %s partition %d: no stored position, expected %d",
			ports.ErrFenced, cp.Topic, cp.Partition, cp.From)
	default:
		return wrap("read stored position", err)
	}
}

// columns sorts a copy of deltas by key and splits it into the four arrays of
// the unnest upsert.
func columns(deltas []domain.DailyUserAggregate) ([]time.Time, []string, []int64, []int64) {
	sorted := slices.Clone(deltas)
	slices.SortFunc(sorted, func(a, b domain.DailyUserAggregate) int {
		return cmp.Or(a.ActivityDate.Compare(b.ActivityDate), cmp.Compare(a.UserID, b.UserID))
	})
	n := len(sorted)
	dates, users := make([]time.Time, n), make([]string, n)
	activities, pageViews := make([]int64, n), make([]int64, n)
	for i, d := range sorted {
		dates[i], users[i], activities[i], pageViews[i] = d.ActivityDate, d.UserID, d.ActivityCount, d.PageViewCount
	}
	return dates, users, activities, pageViews
}

// NextOffsets returns the stored next offset of each given partition of
// (group, topic) with one primary-key lookup; partitions without a row are
// absent from the result (ADR 0008). Failures are classified like ApplyDeltas.
func (r *AggregateRepository) NextOffsets(ctx context.Context, group, topic string, partitions []int32) (map[int32]int64, error) {
	out := make(map[int32]int64, len(partitions))
	if len(partitions) == 0 {
		return out, nil
	}
	rows, err := r.pool.Query(ctx, selectOffsets, group, topic, partitions)
	if err != nil {
		return nil, wrap("select stored positions", err)
	}
	var (
		partition int32
		next      int64
	)
	_, err = pgx.ForEachRow(rows, []any{&partition, &next}, func() error {
		out[partition] = next
		return nil
	})
	if err != nil {
		return nil, wrap("read stored positions", err)
	}
	return out, nil
}

// Ping checks that the database answers. Failures are classified like ApplyDeltas.
func (r *AggregateRepository) Ping(ctx context.Context) error {
	return wrap("ping postgres", r.pool.Ping(ctx))
}
