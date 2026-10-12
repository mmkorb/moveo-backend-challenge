//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

func checkpoint(partition int32, from, to int64) ports.Checkpoint {
	return ports.Checkpoint{Group: testGroup, Topic: testTopic, Partition: partition, From: from, To: to}
}

func TestCheckpointInsertsThenAdvances(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	jul1 := day(2024, 7, 1)

	if err := repo.ApplyDeltas(t.Context(), checkpoint(1, ports.NoPosition, 10), []domain.DailyUserAggregate{delta(jul1, "u", 2, 1)}); err != nil {
		t.Fatalf("first ApplyDeltas() error = %v", err)
	}
	var firstUpdate, secondUpdate string
	if err := ownerDB.QueryRow(t.Context(), "SELECT updated_at::text FROM consumer_offsets WHERE partition = 1").Scan(&firstUpdate); err != nil {
		t.Fatal(err)
	}
	if err := repo.ApplyDeltas(t.Context(), checkpoint(1, 10, 25), []domain.DailyUserAggregate{delta(jul1, "u", 3, 0)}); err != nil {
		t.Fatalf("second ApplyDeltas() error = %v", err)
	}
	if err := ownerDB.QueryRow(t.Context(), "SELECT updated_at::text FROM consumer_offsets WHERE partition = 1").Scan(&secondUpdate); err != nil {
		t.Fatal(err)
	}

	if got := storedOffsets(t); !cmp.Equal(got, map[int32]int64{1: 25}) {
		t.Errorf("stored positions = %v, want {1: 25}", got)
	}
	if got := rows(t); len(got) != 1 || got[0].ActivityCount != 5 || got[0].PageViewCount != 1 {
		t.Errorf("rows = %+v, want one row (5, 1)", got)
	}
	if firstUpdate == secondUpdate {
		t.Errorf("updated_at not refreshed by the advance: %s", secondUpdate)
	}
}

func TestCheckpointMismatchIsFencedAndRollsBack(t *testing.T) {
	jul1 := day(2024, 7, 1)
	tests := []struct {
		name   string
		stored map[int32]int64 // positions before the call
		cp     ports.Checkpoint
	}{
		{"should fence an advance from a stale position", map[int32]int64{2: 40}, checkpoint(2, 30, 50)},
		{"should fence an advance from a position ahead of the stored one", map[int32]int64{2: 40}, checkpoint(2, 45, 50)},
		{"should fence the creation of a position that already exists", map[int32]int64{2: 40}, checkpoint(2, ports.NoPosition, 50)},
		{"should fence an advance of a position that does not exist", nil, checkpoint(2, 30, 50)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetTable(t)
			for p, next := range tt.stored {
				if _, err := ownerDB.Exec(t.Context(),
					"INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset) VALUES ($1, $2, $3, $4)",
					testGroup, testTopic, p, next); err != nil {
					t.Fatal(err)
				}
			}
			repo := newRepo(t, appCfg)

			err := repo.ApplyDeltas(t.Context(), tt.cp, []domain.DailyUserAggregate{delta(jul1, "u", 1, 1)})

			if !errors.Is(err, ports.ErrFenced) {
				t.Fatalf("ApplyDeltas() error = %v, want ports.ErrFenced", err)
			}
			if errors.Is(err, ports.ErrUnavailable) {
				t.Errorf("a fence is not an outage: %v", err)
			}
			if got := rows(t); len(got) != 0 {
				t.Errorf("rows = %+v, want none: a fenced batch writes nothing", got)
			}
			want := tt.stored
			if want == nil {
				want = map[int32]int64{}
			}
			if got := storedOffsets(t); !cmp.Equal(got, want) {
				t.Errorf("stored positions = %v, want them unchanged %v", got, want)
			}
		})
	}
}

func TestCheckpointAlreadyAppliedIsANoop(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	batch := []domain.DailyUserAggregate{delta(day(2024, 7, 1), "u", 4, 2)}

	for _, cp := range []ports.Checkpoint{checkpoint(0, ports.NoPosition, 5), checkpoint(0, 5, 9)} {
		// The second call of each pair stands in for a retry after a lost COMMIT reply.
		for attempt := range 2 {
			if err := repo.ApplyDeltas(t.Context(), cp, batch); err != nil {
				t.Fatalf("ApplyDeltas(%+v) attempt %d error = %v", cp, attempt+1, err)
			}
		}
	}

	if got := rows(t); len(got) != 1 || got[0].ActivityCount != 8 || got[0].PageViewCount != 4 {
		t.Errorf("rows = %+v, want (8, 4): two batches, each applied once", got)
	}
	if got := storedOffsets(t); !cmp.Equal(got, map[int32]int64{0: 9}) {
		t.Errorf("stored positions = %v, want {0: 9}", got)
	}
}

func TestCheckpointIsAtomicWithTheUpsert(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	jul1 := day(2024, 7, 1)
	bad := []domain.DailyUserAggregate{delta(jul1, "good", 1, 1), delta(jul1, "zbad", 1, 5)} // CHECK violation

	if err := repo.ApplyDeltas(t.Context(), checkpoint(3, ports.NoPosition, 7), bad); sqlState(err) != "23514" {
		t.Fatalf("ApplyDeltas(create) error = %v, want SQLSTATE 23514", err)
	}
	if got := storedOffsets(t); len(got) != 0 {
		t.Errorf("stored positions = %v, want none: the failed batch must not create the position", got)
	}

	if err := repo.ApplyDeltas(t.Context(), checkpoint(3, ports.NoPosition, 7), []domain.DailyUserAggregate{delta(jul1, "good", 1, 1)}); err != nil {
		t.Fatalf("ApplyDeltas() error = %v", err)
	}
	if err := repo.ApplyDeltas(t.Context(), checkpoint(3, 7, 9), bad); sqlState(err) != "23514" {
		t.Fatalf("ApplyDeltas(advance) error = %v, want SQLSTATE 23514", err)
	}
	if got := storedOffsets(t); !cmp.Equal(got, map[int32]int64{3: 7}) {
		t.Errorf("stored positions = %v, want {3: 7}: the failed batch must not move the position", got)
	}
	if got := rows(t); len(got) != 1 || got[0].ActivityCount != 1 {
		t.Errorf("rows = %+v, want only the good batch", got)
	}
}

// TestConcurrentOwnersApplyEachPositionOnce runs two owners of the same
// partition (separate pools, as two processes) that flush batches of different
// sizes from the same position at the same time: exactly one of them may win
// each round, and only its batch is counted.
func TestConcurrentOwnersApplyEachPositionOnce(t *testing.T) {
	resetTable(t)
	owners := []*struct {
		repo interface {
			ApplyDeltas(context.Context, ports.Checkpoint, []domain.DailyUserAggregate) error
		}
		size  int64 // records per batch
		count int64 // activities per batch
		wins  int64
	}{
		{repo: newRepo(t, appCfg), size: 10, count: 1},
		{repo: newRepo(t, appCfg), size: 7, count: 1000},
	}
	jul1 := day(2024, 7, 1)
	const rounds = 30

	from := ports.NoPosition
	var wantActivities int64
	for i := range rounds {
		var (
			wg   sync.WaitGroup
			errs = make([]error, len(owners))
			cps  = make([]ports.Checkpoint, len(owners))
		)
		for j, o := range owners {
			cps[j] = checkpoint(5, from, max(from, 0)+o.size)
			batch := []domain.DailyUserAggregate{delta(jul1, "u", o.count, 0)}
			wg.Go(func() { errs[j] = o.repo.ApplyDeltas(t.Context(), cps[j], batch) })
		}
		wg.Wait()

		winners := 0
		for j, err := range errs {
			switch {
			case err == nil:
				winners++
				owners[j].wins++
				wantActivities += owners[j].count
				from = cps[j].To
			case !errors.Is(err, ports.ErrFenced):
				t.Fatalf("round %d: ApplyDeltas() error = %v, want nil or ErrFenced", i, err)
			}
		}
		if winners != 1 {
			t.Fatalf("round %d: %d owners won, want exactly 1 (errors %v)", i, winners, errs)
		}
	}

	if got := rows(t); len(got) != 1 || got[0].ActivityCount != wantActivities {
		t.Errorf("rows = %+v, want activity_count %d (only the winners' batches)", got, wantActivities)
	}
	if got := storedOffsets(t); !cmp.Equal(got, map[int32]int64{5: from}) {
		t.Errorf("stored positions = %v, want {5: %d}", got, from)
	}
	t.Logf("wins: %d / %d", owners[0].wins, owners[1].wins)
}

func TestNextOffsets(t *testing.T) {
	resetTable(t)
	seed := []struct {
		group, topic string
		partition    int32
		next         int64
	}{
		{testGroup, testTopic, 0, 100},
		{testGroup, testTopic, 2, 7},
		{"other-group", testTopic, 1, 55},
		{testGroup, "other.topic", 1, 66},
	}
	for _, r := range seed {
		if _, err := ownerDB.Exec(t.Context(),
			"INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset) VALUES ($1, $2, $3, $4)",
			r.group, r.topic, r.partition, r.next); err != nil {
			t.Fatal(err)
		}
	}
	repo := newRepo(t, appCfg)

	got, err := repo.NextOffsets(t.Context(), testGroup, testTopic, []int32{0, 1, 2, 3})

	if err != nil {
		t.Fatalf("NextOffsets() error = %v", err)
	}
	if want := map[int32]int64{0: 100, 2: 7}; !cmp.Equal(got, want) {
		t.Errorf("NextOffsets() = %v, want %v (other groups and topics excluded, missing partitions absent)", got, want)
	}
}

func TestConsumerOffsetsChecks(t *testing.T) {
	tests := []struct {
		name, stmt, state string
	}{
		{"should reject a negative next_offset", "INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset) VALUES ('g', 't', 0, -1)", "23514"},
		{"should reject a negative partition", "INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset) VALUES ('g', 't', -1, 0)", "23514"},
		{"should reject a null next_offset", "INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset) VALUES ('g', 't', 0, NULL)", "23502"},
		{"should reject a second row for one partition", "INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset) VALUES ('g', 't', 0, 1), ('g', 't', 0, 2)", "23505"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetTable(t)

			_, err := ownerDB.Exec(t.Context(), tt.stmt)

			if got := sqlState(err); got != tt.state {
				t.Errorf("SQLSTATE = %q (err %v), want %q", got, err, tt.state)
			}
		})
	}
}

func TestMigratedConsumerOffsetsSchema(t *testing.T) {
	type column struct {
		Name, Type, Nullable string
	}
	r, err := ownerDB.Query(t.Context(), `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = 'consumer_offsets' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("query columns: %v", err)
	}
	defer r.Close()
	var got []column
	for r.Next() {
		var c column
		if err := r.Scan(&c.Name, &c.Type, &c.Nullable); err != nil {
			t.Fatalf("scan column: %v", err)
		}
		got = append(got, c)
	}
	if err := r.Err(); err != nil {
		t.Fatalf("columns: %v", err)
	}
	want := []column{
		{"consumer_group", "text", "NO"},
		{"topic", "text", "NO"},
		{"partition", "integer", "NO"},
		{"next_offset", "bigint", "NO"},
		{"updated_at", "timestamp with time zone", "NO"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("columns mismatch (-want +got):\n%s", diff)
	}
	var pk string
	if err := ownerDB.QueryRow(t.Context(), `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conrelid = 'consumer_offsets'::regclass AND contype = 'p'`).Scan(&pk); err != nil {
		t.Fatalf("primary key: %v", err)
	}
	if pk != "PRIMARY KEY (consumer_group, topic, partition)" {
		t.Errorf("primary key = %q", pk)
	}
}
