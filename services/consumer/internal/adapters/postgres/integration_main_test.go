//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// One PostgreSQL container (the image of compose.yaml, limited to 512 MiB and one
// CPU) is shared by every test of this package. It is initialised with the real
// deploy/postgres/init.sql, so the roles match the Compose stack: analytics_owner
// is the bootstrap superuser, analytics_app is created by the script.
const (
	postgresImage = "postgres:18.6"
	database      = "analytics"
	ownerUser     = "analytics_owner"
	ownerPassword = "owner-password"
	appUser       = "analytics_app"
	appPassword   = "app-password"
	initScript    = "../../../../../deploy/postgres/init.sql"
)

var (
	host     string
	port     int
	ownerCfg postgres.Config // analytics_owner on the analytics database
	appCfg   postgres.Config // analytics_app on the analytics database
	ownerDB  *pgxpool.Pool   // direct SQL for fixtures and assertions
	scratchN atomic.Int64
	// containerID lets a test freeze the server (docker pause).
	containerID string
)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	ctr, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase(database),
		tcpostgres.WithUsername(ownerUser),
		tcpostgres.WithPassword(ownerPassword),
		tcpostgres.WithInitScripts(initScript),
		testcontainers.WithFiles(testcontainers.ContainerFile{
			Reader:            strings.NewReader(appPassword),
			ContainerFilePath: "/run/secrets/analytics_app_password",
			FileMode:          0o644,
		}),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Memory = 512 << 20
			hc.NanoCPUs = 1_000_000_000
		}),
		tcpostgres.BasicWaitStrategies(),
	)
	if ctr != nil {
		defer func() {
			if err := testcontainers.TerminateContainer(ctr); err != nil {
				fmt.Fprintln(os.Stderr, "terminate postgres container:", err)
			}
		}()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres container:", err)
		return 1
	}
	containerID = ctr.GetContainerID()
	endpoint, err := ctr.PortEndpoint(ctx, "5432/tcp", "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres endpoint:", err)
		return 1
	}
	h, p, err := net.SplitHostPort(endpoint)
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres endpoint:", err)
		return 1
	}
	host = h
	if port, err = strconv.Atoi(p); err != nil {
		fmt.Fprintln(os.Stderr, "postgres port:", err)
		return 1
	}
	ownerCfg = postgres.Config{Host: host, Port: port, Database: database, User: ownerUser, Password: ownerPassword, SSLMode: "disable", MaxConns: 4, MinConns: 0}
	appCfg = postgres.Config{Host: host, Port: port, Database: database, User: appUser, Password: appPassword, SSLMode: "disable", MaxConns: 8, MinConns: 0, StatementTimeout: 5 * time.Second}

	if err := postgres.Migrate(ctx, ownerCfg, slog.New(slog.DiscardHandler)); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		return 1
	}
	ownerDB, err = postgres.NewPool(ctx, ownerCfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "owner pool:", err)
		return 1
	}
	defer ownerDB.Close()

	return m.Run()
}

// newRepo returns a repository connected as cfg's user; the pool closes with the test.
func newRepo(t *testing.T, cfg postgres.Config) *postgres.AggregateRepository {
	t.Helper()
	pool := newPool(t, cfg)
	return postgres.NewAggregateRepository(pool)
}

func newPool(t *testing.T, cfg postgres.Config) *pgxpool.Pool {
	t.Helper()
	pool, err := postgres.NewPool(t.Context(), cfg)
	if err != nil {
		t.Fatalf("NewPool(%s) error = %v", cfg, err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// migrationCount is the number of files in services/consumer/migrations.
const migrationCount = 2

// Coordinates of the stored positions written by these tests.
const (
	testGroup = "analytics-consumer"
	testTopic = "incoming.user_activity"
)

// resetTable empties both tables before a test.
func resetTable(t *testing.T) {
	t.Helper()
	if _, err := ownerDB.Exec(t.Context(), "TRUNCATE activity_aggregates, consumer_offsets"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// streams gives every apply call sequence its own partition (see apply).
var streams sync.Map // *postgres.AggregateRepository -> *stream

// stream is the stored position of one partition, as its single owner sees it.
type stream struct {
	mu        sync.Mutex
	partition int32
	next      int64
}

var streamN atomic.Int32

// apply calls ApplyDeltas with the next checkpoint of a partition reserved for
// repo, as one owner flushing one batch of len(deltas)+1 records would: tests
// about the aggregates do not have to track positions themselves.
func apply(ctx context.Context, repo *postgres.AggregateRepository, deltas []domain.DailyUserAggregate) error {
	v, _ := streams.LoadOrStore(repo, &stream{partition: streamN.Add(1) - 1, next: ports.NoPosition})
	st := v.(*stream)
	st.mu.Lock()
	defer st.mu.Unlock()
	cp := ports.Checkpoint{Group: testGroup, Topic: testTopic, Partition: st.partition, From: st.next}
	cp.To = max(st.next, 0) + int64(len(deltas)) + 1
	err := repo.ApplyDeltas(ctx, cp, deltas)
	if err == nil {
		st.next = cp.To
	}
	return err
}

// storedOffsets returns every stored position of (testGroup, testTopic).
func storedOffsets(t *testing.T) map[int32]int64 {
	t.Helper()
	r, err := ownerDB.Query(t.Context(),
		"SELECT partition, next_offset FROM consumer_offsets WHERE consumer_group = $1 AND topic = $2", testGroup, testTopic)
	if err != nil {
		t.Fatalf("select positions: %v", err)
	}
	out := map[int32]int64{}
	var (
		p    int32
		next int64
	)
	if _, err := pgx.ForEachRow(r, []any{&p, &next}, func() error { out[p] = next; return nil }); err != nil {
		t.Fatalf("read positions: %v", err)
	}
	return out
}

type row struct {
	Date          string
	UserID        string
	ActivityCount int64
	PageViewCount int64
	UpdatedAt     time.Time
}

// rows returns the table content ordered by key, the date rendered by PostgreSQL
// so the host time zone cannot influence the assertion.
func rows(t *testing.T) []row {
	t.Helper()
	r, err := ownerDB.Query(t.Context(),
		"SELECT activity_date::text, user_id, activity_count, page_view_count, updated_at FROM activity_aggregates ORDER BY activity_date, user_id")
	if err != nil {
		t.Fatalf("select rows: %v", err)
	}
	out, err := pgx.CollectRows(r, pgx.RowToStructByPos[row])
	if err != nil {
		t.Fatalf("collect rows: %v", err)
	}
	return out
}

// scratchDatabase creates an empty database (no migrations) and returns its name.
func scratchDatabase(t *testing.T) string {
	t.Helper()
	name := fmt.Sprintf("scratch_%d", scratchN.Add(1))
	ident := pgx.Identifier{name}.Sanitize()
	if _, err := ownerDB.Exec(t.Context(), "CREATE DATABASE "+ident); err != nil {
		t.Fatalf("create database %s: %v", name, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := ownerDB.Exec(ctx, "DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
	})
	return name
}
