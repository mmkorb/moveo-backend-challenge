//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

func day(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func delta(date time.Time, user string, activities, pageViews int64) domain.DailyUserAggregate {
	return domain.DailyUserAggregate{ActivityDate: date, UserID: user, ActivityCount: activities, PageViewCount: pageViews}
}

// sqlState returns the SQLSTATE found in err's chain, or "" if there is none.
func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

var ignoreUpdatedAt = cmpopts.IgnoreFields(row{}, "UpdatedAt")

func TestApplyDeltasInsertsThenAdds(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	jul1 := day(2024, 7, 1)

	if err := apply(t.Context(), repo, []domain.DailyUserAggregate{delta(jul1, "u1", 3, 1), delta(jul1, "u2", 1, 0)}); err != nil {
		t.Fatalf("first ApplyDeltas() error = %v", err)
	}
	first := rows(t)
	if err := apply(t.Context(), repo, []domain.DailyUserAggregate{delta(jul1, "u1", 2, 2), delta(day(2024, 7, 2), "u1", 1, 1)}); err != nil {
		t.Fatalf("second ApplyDeltas() error = %v", err)
	}
	second := rows(t)

	want := []row{
		{Date: "2024-07-01", UserID: "u1", ActivityCount: 5, PageViewCount: 3},
		{Date: "2024-07-01", UserID: "u2", ActivityCount: 1, PageViewCount: 0},
		{Date: "2024-07-02", UserID: "u1", ActivityCount: 1, PageViewCount: 1},
	}
	if diff := cmp.Diff(want, second, ignoreUpdatedAt); diff != "" {
		t.Errorf("rows mismatch (-want +got):\n%s", diff)
	}
	if !second[0].UpdatedAt.After(first[0].UpdatedAt) {
		t.Errorf("updated_at of u1 = %v, want it after %v: the conflict clause must refresh it", second[0].UpdatedAt, first[0].UpdatedAt)
	}
	if !second[1].UpdatedAt.Equal(first[1].UpdatedAt) {
		t.Errorf("updated_at of the untouched u2 changed: %v -> %v", first[1].UpdatedAt, second[1].UpdatedAt)
	}
}

func TestApplyDeltasStoresTheUTCDateWhateverTheLocation(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)

	if err := apply(t.Context(), repo, []domain.DailyUserAggregate{delta(day(2024, 12, 31), "u1", 1, 0), delta(day(2025, 1, 1), "u1", 1, 0)}); err != nil {
		t.Fatalf("ApplyDeltas() error = %v", err)
	}

	got := rows(t)
	if len(got) != 2 || got[0].Date != "2024-12-31" || got[1].Date != "2025-01-01" {
		t.Errorf("dates = %+v, want 2024-12-31 and 2025-01-01", got)
	}
}

func TestApplyDeltasWithoutDeltasOnlyMovesThePosition(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	cp := ports.Checkpoint{Group: testGroup, Topic: testTopic, Partition: 0, From: ports.NoPosition, To: 3}

	if err := repo.ApplyDeltas(t.Context(), cp, nil); err != nil {
		t.Fatalf("ApplyDeltas(nil) error = %v", err)
	}

	if got := rows(t); len(got) != 0 {
		t.Errorf("rows = %+v, want none", got)
	}
	if got := storedOffsets(t); !cmp.Equal(got, map[int32]int64{0: 3}) {
		t.Errorf("stored positions = %v, want {0: 3}: dead-letter-only batches move the position too", got)
	}
}

func TestApplyDeltasLargeUnsortedBatch(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	const users, days = 400, 5
	batch := make([]domain.DailyUserAggregate, 0, users*days)
	for d := range days {
		for u := range users {
			batch = append(batch, delta(day(2024, 7, 1+d), fmt.Sprintf("user-%04d", u), int64(u%7)+1, int64(u%2)))
		}
	}
	rand.Shuffle(len(batch), func(i, j int) { batch[i], batch[j] = batch[j], batch[i] })

	if err := apply(t.Context(), repo, batch); err != nil {
		t.Fatalf("ApplyDeltas() error = %v", err)
	}

	got := rows(t)
	if len(got) != users*days {
		t.Fatalf("rows = %d, want %d", len(got), users*days)
	}
	var activities, pageViews int64
	for _, r := range got {
		activities += r.ActivityCount
		pageViews += r.PageViewCount
	}
	var wantA, wantP int64
	for _, d := range batch {
		wantA += d.ActivityCount
		wantP += d.PageViewCount
	}
	if activities != wantA || pageViews != wantP {
		t.Errorf("totals = (%d, %d), want (%d, %d)", activities, pageViews, wantA, wantP)
	}
}

func TestApplyDeltasDoesNotMutateTheCallersSlice(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	jul1 := day(2024, 7, 1)
	batch := []domain.DailyUserAggregate{delta(jul1, "b", 1, 0), delta(jul1, "a", 1, 0)}

	if err := apply(t.Context(), repo, batch); err != nil {
		t.Fatalf("ApplyDeltas() error = %v", err)
	}

	if batch[0].UserID != "b" || batch[1].UserID != "a" {
		t.Errorf("caller's slice was reordered: %+v", batch)
	}
}

func TestConcurrentUpsertsSumExactly(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	jul1 := day(2024, 7, 1)
	keys := []string{"k1", "k2", "k3", "k4", "k5"}
	const workers, iterations = 8, 25

	var wg sync.WaitGroup
	errs := make(chan error, workers*iterations)
	for w := range workers {
		wg.Go(func() {
			for range iterations {
				// Every worker sends the same keys in a different input order: the
				// repository's sort keeps row locks ordered, so no deadlock (40P01).
				batch := make([]domain.DailyUserAggregate, 0, len(keys))
				for _, k := range keys {
					batch = append(batch, delta(jul1, k, 1, 1))
				}
				if w%2 == 1 {
					for i, j := 0, len(batch)-1; i < j; i, j = i+1, j-1 {
						batch[i], batch[j] = batch[j], batch[i]
					}
				}
				if err := apply(t.Context(), repo, batch); err != nil {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Errorf("concurrent ApplyDeltas() error = %v", err)
	}
	got := rows(t)
	if len(got) != len(keys) {
		t.Fatalf("rows = %d, want %d", len(got), len(keys))
	}
	for _, r := range got {
		if r.ActivityCount != workers*iterations || r.PageViewCount != workers*iterations {
			t.Errorf("row %s = (%d, %d), want (%d, %d): increments were lost", r.UserID, r.ActivityCount, r.PageViewCount, workers*iterations, workers*iterations)
		}
	}
}

func TestChecksRejectInconsistentRows(t *testing.T) {
	tests := []struct {
		name  string
		stmt  string
		check string // expected SQLSTATE
	}{
		{"should reject a negative activity_count", "INSERT INTO activity_aggregates (activity_date, user_id, activity_count, page_view_count) VALUES ('2024-07-01', 'u', -1, 0)", "23514"},
		{"should reject a negative page_view_count", "INSERT INTO activity_aggregates (activity_date, user_id, activity_count, page_view_count) VALUES ('2024-07-01', 'u', 1, -1)", "23514"},
		{"should reject more page views than activities", "INSERT INTO activity_aggregates (activity_date, user_id, activity_count, page_view_count) VALUES ('2024-07-01', 'u', 1, 2)", "23514"},
		{"should reject a null user_id", "INSERT INTO activity_aggregates (activity_date, user_id) VALUES ('2024-07-01', NULL)", "23502"},
		{"should reject a duplicate primary key", "INSERT INTO activity_aggregates (activity_date, user_id) VALUES ('2024-07-01', 'dup'), ('2024-07-01', 'dup')", "23505"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetTable(t)

			_, err := ownerDB.Exec(t.Context(), tt.stmt)

			if got := sqlState(err); got != tt.check {
				t.Errorf("SQLSTATE = %q (err %v), want %q", got, err, tt.check)
			}
		})
	}
}

func TestApplyDeltasCheckViolationIsUnclassifiedAndAtomic(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	jul1 := day(2024, 7, 1)

	err := apply(t.Context(), repo, []domain.DailyUserAggregate{
		delta(jul1, "good", 1, 1),
		delta(jul1, "zbad", 1, 5), // more page views than activities
	})

	if sqlState(err) != "23514" {
		t.Fatalf("ApplyDeltas() error = %v, want SQLSTATE 23514", err)
	}
	if errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("a CHECK violation must stay unclassified, got ErrUnavailable: %v", err)
	}
	if got := rows(t); len(got) != 0 {
		t.Errorf("rows = %+v, want none: a failed flush applies no row", got)
	}
}

func TestApplyDeltasRejectsDuplicateKeys(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	jul1 := day(2024, 7, 1)

	err := apply(t.Context(), repo, []domain.DailyUserAggregate{delta(jul1, "u", 1, 0), delta(jul1, "u", 1, 0)})

	if sqlState(err) != "21000" {
		t.Fatalf("ApplyDeltas() error = %v, want SQLSTATE 21000 (row affected twice)", err)
	}
	if errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("a duplicate key is a bug, not an outage: %v", err)
	}
	if got := rows(t); len(got) != 0 {
		t.Errorf("rows = %+v, want none", got)
	}
}

func TestRuntimeRoleHasDMLOnly(t *testing.T) {
	resetTable(t)
	app := newPool(t, appCfg)

	var super bool
	if err := app.QueryRow(t.Context(), "SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&super); err != nil {
		t.Fatalf("query role: %v", err)
	}
	if super {
		t.Fatal("analytics_app is a superuser")
	}

	allowed := []string{
		"SELECT count(*) FROM activity_aggregates",
		"INSERT INTO activity_aggregates (activity_date, user_id, activity_count) VALUES ('2024-07-01', 'x', 1)",
		"UPDATE activity_aggregates SET activity_count = 2 WHERE user_id = 'x'",
		"SELECT count(*) FROM consumer_offsets",
		"INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset) VALUES ('g', 't', 0, 1)",
		"UPDATE consumer_offsets SET next_offset = 2 WHERE consumer_group = 'g'",
	}
	for _, stmt := range allowed {
		if _, err := app.Exec(t.Context(), stmt); err != nil {
			t.Errorf("%q: error = %v, want it allowed", stmt, err)
		}
	}
	denied := []string{
		"DELETE FROM activity_aggregates",
		"TRUNCATE activity_aggregates",
		"DELETE FROM consumer_offsets",
		"TRUNCATE consumer_offsets",
		"DROP TABLE activity_aggregates",
		"ALTER TABLE activity_aggregates ADD COLUMN extra int",
		"CREATE TABLE intruder (id int)",
		"CREATE INDEX intruder_idx ON activity_aggregates (updated_at)",
		"SELECT * FROM goose_db_version",
	}
	for _, stmt := range denied {
		_, err := app.Exec(t.Context(), stmt)
		if sqlState(err) != "42501" {
			t.Errorf("%q: SQLSTATE = %q (err %v), want 42501 insufficient_privilege", stmt, sqlState(err), err)
		}
	}
}

func TestMigrationsAreIdempotent(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)

	// TestMain already applied them once.
	if err := postgres.Migrate(t.Context(), ownerCfg, logger); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}

	var versions int
	if err := ownerDB.QueryRow(t.Context(), "SELECT count(*) FROM goose_db_version WHERE version_id > 0").Scan(&versions); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if versions != migrationCount {
		t.Errorf("applied migrations = %d, want %d", versions, migrationCount)
	}
}

func TestConcurrentMigrationsSerializeAndGrantTheRuntimeRole(t *testing.T) {
	name := scratchDatabase(t)
	cfg := ownerCfg
	cfg.Database = name
	logger := slog.New(slog.DiscardHandler)

	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for range 3 {
		wg.Go(func() { errs <- postgres.Migrate(t.Context(), cfg, logger) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent Migrate() error = %v", err)
		}
	}

	scratch := newPool(t, cfg)
	var versions int
	if err := scratch.QueryRow(t.Context(), "SELECT count(*) FROM goose_db_version WHERE version_id > 0").Scan(&versions); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if versions != migrationCount {
		t.Errorf("applied migrations = %d, want exactly %d despite 3 concurrent runs", versions, migrationCount)
	}
	privileges := map[string]bool{"SELECT": true, "INSERT": true, "UPDATE": true, "DELETE": false, "TRUNCATE": false}
	for _, table := range []string{"activity_aggregates", "consumer_offsets"} {
		for priv, want := range privileges {
			var got bool
			if err := scratch.QueryRow(t.Context(), "SELECT has_table_privilege('analytics_app', $1, $2)", table, priv).Scan(&got); err != nil {
				t.Fatalf("has_table_privilege %s %s: %v", table, priv, err)
			}
			if got != want {
				t.Errorf("analytics_app %s on %s = %v, want %v", priv, table, got, want)
			}
		}
		var owner string
		if err := scratch.QueryRow(t.Context(), "SELECT tableowner FROM pg_tables WHERE tablename = $1", table).Scan(&owner); err != nil {
			t.Fatalf("table owner of %s: %v", table, err)
		}
		if owner != ownerUser {
			t.Errorf("owner of %s = %q, want %q", table, owner, ownerUser)
		}
	}
}

func TestMigratedSchemaMatchesADR0009(t *testing.T) {
	type column struct {
		Name, Type, Nullable string
	}
	r, err := ownerDB.Query(t.Context(), `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = 'activity_aggregates' ORDER BY ordinal_position`)
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
		{"activity_date", "date", "NO"},
		{"user_id", "text", "NO"},
		{"activity_count", "bigint", "NO"},
		{"page_view_count", "bigint", "NO"},
		{"updated_at", "timestamp with time zone", "NO"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("columns mismatch (-want +got):\n%s", diff)
	}

	var pk, userIdx string
	if err := ownerDB.QueryRow(t.Context(), `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conrelid = 'activity_aggregates'::regclass AND contype = 'p'`).Scan(&pk); err != nil {
		t.Fatalf("primary key: %v", err)
	}
	if pk != "PRIMARY KEY (activity_date, user_id)" {
		t.Errorf("primary key = %q", pk)
	}
	if err := ownerDB.QueryRow(t.Context(), `
		SELECT indexdef FROM pg_indexes WHERE indexname = 'idx_activity_aggregates_user'`).Scan(&userIdx); err != nil {
		t.Fatalf("user index: %v", err)
	}
	if want := "CREATE INDEX idx_activity_aggregates_user ON public.activity_aggregates USING btree (user_id)"; userIdx != want {
		t.Errorf("user index = %q, want %q", userIdx, want)
	}
}

func TestMigrateNeedsTheOwnerRole(t *testing.T) {
	name := scratchDatabase(t)
	cfg := appCfg
	cfg.Database = name

	err := postgres.Migrate(t.Context(), cfg, slog.New(slog.DiscardHandler))

	if err == nil {
		t.Fatal("Migrate() as analytics_app succeeded, want a privilege error")
	}
	if errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("a privilege error is not transient: %v", err)
	}
}

func TestMigrateWithWrongPasswordIsNotTransient(t *testing.T) {
	cfg := ownerCfg
	cfg.Password = "wrong"

	err := postgres.Migrate(t.Context(), cfg, slog.New(slog.DiscardHandler))

	if err == nil {
		t.Fatal("Migrate() with a wrong password succeeded")
	}
	if errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("an authentication failure must not be retried as an outage: %v", err)
	}
}

func TestPingSucceeds(t *testing.T) {
	repo := newRepo(t, appCfg)

	if err := repo.Ping(t.Context()); err != nil {
		t.Errorf("Ping() error = %v", err)
	}
}

func TestStatementTimeoutIsUnavailable(t *testing.T) {
	resetTable(t)
	jul1 := day(2024, 7, 1)
	seed := newRepo(t, appCfg)
	if err := apply(t.Context(), seed, []domain.DailyUserAggregate{delta(jul1, "locked", 1, 0)}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cfg := appCfg
	cfg.StatementTimeout = 300 * time.Millisecond
	repo := newRepo(t, cfg)

	// Another session holds the row lock, so the upsert waits and hits statement_timeout.
	blocker, err := ownerDB.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = blocker.Rollback(t.Context()) }()
	if _, err := blocker.Exec(t.Context(), "SELECT 1 FROM activity_aggregates WHERE user_id = 'locked' FOR UPDATE"); err != nil {
		t.Fatalf("lock row: %v", err)
	}

	err = apply(t.Context(), repo, []domain.DailyUserAggregate{delta(jul1, "locked", 1, 0)})

	if !errors.Is(err, ports.ErrUnavailable) {
		t.Fatalf("ApplyDeltas() error = %v, want ports.ErrUnavailable", err)
	}
	if got := sqlState(err); got != "57014" {
		t.Errorf("SQLSTATE = %q, want 57014 query_canceled", got)
	}
	if got := rows(t); len(got) != 1 || got[0].ActivityCount != 1 {
		t.Errorf("rows = %+v, want the seed row untouched", got)
	}
}

func TestTerminatedConnectionIsUnavailableThenPoolRecovers(t *testing.T) {
	resetTable(t)
	cfg := appCfg
	cfg.MaxConns = 1
	repo := newRepo(t, cfg)
	jul1 := day(2024, 7, 1)
	if err := apply(t.Context(), repo, []domain.DailyUserAggregate{delta(jul1, "u", 1, 0)}); err != nil {
		t.Fatalf("warm up: %v", err)
	}

	// Stands in for a database restart: the server kills the pooled connection.
	if _, err := ownerDB.Exec(t.Context(), "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = $1", appUser); err != nil {
		t.Fatalf("terminate backend: %v", err)
	}
	err := apply(t.Context(), repo, []domain.DailyUserAggregate{delta(jul1, "u", 1, 0)})

	if !errors.Is(err, ports.ErrUnavailable) {
		t.Fatalf("ApplyDeltas() on a killed connection error = %v, want ports.ErrUnavailable", err)
	}
	if err := apply(t.Context(), repo, []domain.DailyUserAggregate{delta(jul1, "u", 1, 0)}); err != nil {
		t.Fatalf("ApplyDeltas() after the pool reconnected error = %v", err)
	}
	// The failed attempt wrote nothing: warm-up + recovered call = 2.
	if got := rows(t); len(got) != 1 || got[0].ActivityCount != 2 {
		t.Errorf("rows = %+v, want one row with activity_count 2", got)
	}
}

func TestMissingTableIsUnclassified(t *testing.T) {
	name := scratchDatabase(t) // no migrations applied
	cfg := appCfg
	cfg.Database = name
	repo := newRepo(t, cfg)

	err := apply(t.Context(), repo, []domain.DailyUserAggregate{delta(day(2024, 7, 1), "u", 1, 0)})

	if sqlState(err) != "42P01" {
		t.Fatalf("ApplyDeltas() error = %v, want SQLSTATE 42P01", err)
	}
	if errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("a missing table (migrations not applied) is a configuration error, not an outage: %v", err)
	}
}

func TestCanceledContextStopsTheUpsert(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()

	err := apply(canceled, repo, []domain.DailyUserAggregate{delta(day(2024, 7, 1), "u", 1, 0)})

	if err == nil {
		t.Fatal("ApplyDeltas() with a canceled context succeeded")
	}
	if got := rows(t); len(got) != 0 {
		t.Errorf("rows = %+v, want none", got)
	}
}
