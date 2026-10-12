//go:build integration

package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/application"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// One limited postgres:18.6 container (512 MiB, one CPU), initialised with the
// real deploy/postgres/init.sql, like the adapter's integration tests.
const (
	database      = "analytics"
	ownerUser     = "analytics_owner"
	ownerPassword = "owner-password"
	appUser       = "analytics_app"
	appPassword   = "app-password"
	initScript    = "../../../../deploy/postgres/init.sql"
)

var pgHost, pgPort string

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := tcpostgres.Run(ctx, "postgres:18.6",
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
	endpoint, err := ctr.PortEndpoint(ctx, "5432/tcp", "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "postgres endpoint:", err)
		return 1
	}
	if pgHost, pgPort, err = net.SplitHostPort(endpoint); err != nil {
		fmt.Fprintln(os.Stderr, "postgres endpoint:", err)
		return 1
	}
	return m.Run()
}

// dbEnv points the service at the container as the given role.
func dbEnv(t *testing.T, user, password string, extra ...string) {
	t.Helper()
	setEnv(t, password, append([]string{
		"DATABASE_HOST", pgHost, "DATABASE_PORT", pgPort, "DATABASE_NAME", database, "DATABASE_USER", user,
		"STARTUP_TIMEOUT", "30s",
	}, extra...)...)
}

func ownerPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	port, err := strconv.Atoi(pgPort)
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	pool, err := postgres.NewPool(t.Context(), postgres.Config{Host: pgHost, Port: port, Database: database, User: ownerUser, Password: ownerPassword, SSLMode: "disable", MaxConns: 2})
	if err != nil {
		t.Fatalf("owner pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// safeBuffer is a bytes.Buffer that the service goroutines and the test can share.
type safeBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestMigrateSubcommandAsOwner(t *testing.T) {
	dbEnv(t, ownerUser, ownerPassword)
	var out bytes.Buffer

	if err := dispatch(t.Context(), []string{"migrate"}, &out); err != nil {
		t.Fatalf("migrate error = %v\noutput: %s", err, out.String())
	}
	first := out.String()
	out.Reset()
	if err := dispatch(t.Context(), []string{"migrate"}, &out); err != nil {
		t.Fatalf("second migrate error = %v", err)
	}

	if !strings.Contains(first, `"msg":"migration applied"`) || !strings.Contains(first, `"service":"consumer"`) {
		t.Errorf("first run output = %s, want a JSON 'migration applied' record", first)
	}
	if !strings.Contains(out.String(), "database schema is up to date") {
		t.Errorf("second run output = %s, want it to report an up-to-date schema", out.String())
	}
	if strings.Contains(first+out.String(), ownerPassword) {
		t.Error("the owner password leaked into the migrate output")
	}
	for _, table := range []string{"activity_aggregates", "consumer_offsets"} {
		var canWrite, canDelete bool
		err := ownerPool(t).QueryRow(t.Context(),
			"SELECT has_table_privilege('analytics_app', $1, 'INSERT'), has_table_privilege('analytics_app', $1, 'DELETE')", table).
			Scan(&canWrite, &canDelete)
		if err != nil || !canWrite || canDelete {
			t.Errorf("grants for analytics_app on %s: insert=%v delete=%v err=%v, want insert only", table, canWrite, canDelete, err)
		}
	}
}

func TestMigrateSubcommandAsRuntimeRoleIsRefused(t *testing.T) {
	// A scratch database where the runtime role may connect but owns nothing.
	owner := ownerPool(t)
	if _, err := owner.Exec(t.Context(), "CREATE DATABASE scratch_cmd"); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() { _, _ = owner.Exec(context.Background(), "DROP DATABASE IF EXISTS scratch_cmd WITH (FORCE)") })
	dbEnv(t, appUser, appPassword, "DATABASE_NAME", "scratch_cmd")

	err := dispatch(t.Context(), []string{"migrate"}, io.Discard)

	if err == nil {
		t.Fatal("migrate as analytics_app succeeded, want a privilege failure")
	}
}

// fakeDeadLetters stands in for the Kafka DLQ publisher, which is not part of this build.
type fakeDeadLetters struct{}

func (fakeDeadLetters) Publish(context.Context, []ports.DeadLetter) error { return nil }

func TestRunWiresConfigPoolUseCaseAndHealthServer(t *testing.T) {
	// Schema first, as the consumer-migrate service does in Compose.
	dbEnv(t, ownerUser, ownerPassword)
	if err := dispatch(t.Context(), []string{"migrate"}, io.Discard); err != nil {
		t.Fatalf("migrate error = %v", err)
	}
	owner := ownerPool(t)
	if _, err := owner.Exec(t.Context(), "TRUNCATE activity_aggregates, consumer_offsets"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	httpAddr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	dbEnv(t, appUser, appPassword, "HTTP_ADDR", httpAddr)

	var closed atomic.Bool
	start := func(ctx context.Context, d pipelineDeps) (pipeline, error) {
		process := application.NewProcessActivity(d.repo, fakeDeadLetters{})
		positions := application.NewResumePositions(d.offsets)
		cp := ports.Checkpoint{Group: "it-group", Topic: "it-topic", Partition: 0, From: ports.NoPosition, To: 3}
		ts := time.Date(2024, 7, 1, 23, 30, 0, 0, time.FixedZone("BRT", -3*3600)) // 2024-07-02 UTC
		var events []domain.ActivityEvent
		for _, typ := range []domain.ActivityType{domain.ActivityPageView, domain.ActivityPageView, "click"} {
			e, err := domain.NewActivityEvent("u1", typ, ts, nil)
			if err != nil {
				return pipeline{}, err
			}
			events = append(events, e)
		}
		return pipeline{
			members: []func(context.Context) error{func(ctx context.Context) error {
				if err := process.Process(ctx, cp, events, nil); err != nil {
					return err
				}
				next, err := positions.NextOffsets(ctx, cp.Group, cp.Topic, []int32{0, 1})
				if err != nil {
					return err
				}
				if len(next) != 1 || next[0] != cp.To {
					return fmt.Errorf("stored positions = %v, want {0: %d}", next, cp.To)
				}
				<-ctx.Done()
				return nil
			}},
			close: func() { closed.Store(true) },
		}, nil
	}

	var out safeBuffer
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runWith(ctx, &out, start) }()

	waitFor(t, 20*time.Second, "GET /readyz to answer 200", func() bool { return httpStatus(httpAddr, "/readyz") == http.StatusOK })
	if got := httpStatus(httpAddr, "/healthz"); got != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", got)
	}
	if body := httpBody(httpAddr, "/metrics"); !strings.Contains(body, "go_goroutines") {
		t.Errorf("/metrics lacks the Go collector:\n%.300s", body)
	}
	var date string
	var activities, pageViews int64
	waitFor(t, 10*time.Second, "the aggregate row to appear", func() bool {
		err := owner.QueryRow(t.Context(),
			"SELECT activity_date::text, activity_count, page_view_count FROM activity_aggregates WHERE user_id = 'u1'").
			Scan(&date, &activities, &pageViews)
		return err == nil
	})
	if date != "2024-07-02" || activities != 3 || pageViews != 2 {
		t.Errorf("row = (%s, %d, %d), want (2024-07-02, 3, 2): UTC date, 3 activities, 2 page views", date, activities, pageViews)
	}
	// The healthcheck subcommand sees the same server.
	t.Setenv("HTTP_ADDR", httpAddr)
	if err := dispatch(t.Context(), []string{"healthcheck"}, io.Discard); err != nil {
		t.Errorf("healthcheck while running error = %v", err)
	}

	cancel() // SIGTERM
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run error = %v\noutput:\n%s", err, out.String())
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not return after the context was canceled")
	}

	if !closed.Load() {
		t.Error("pipeline.close was not called")
	}
	if status := httpStatus(httpAddr, "/readyz"); status != 0 {
		t.Errorf("/readyz after shutdown = %d, want the listener closed", status)
	}
	logs := out.String()
	for _, want := range []string{`"msg":"starting"`, `"msg":"postgres reachable"`, `"stage":"health_server"`, `"service":"consumer"`} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs lack %s:\n%s", want, logs)
		}
	}
	if strings.Contains(logs, appPassword) {
		t.Error("the runtime password leaked into the logs")
	}
}

func TestRunFailsFastOnAWrongPassword(t *testing.T) {
	dbEnv(t, appUser, "wrong-password")
	start := time.Now()

	err := dispatch(t.Context(), []string{"run"}, io.Discard)

	if err == nil || !strings.Contains(err.Error(), "connect postgres") {
		t.Fatalf("run error = %v, want a connect postgres failure", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("run took %v: an authentication failure must not be retried until STARTUP_TIMEOUT", took)
	}
	if strings.Contains(err.Error(), "wrong-password") {
		t.Errorf("error leaks the password: %v", err)
	}
}

func httpStatus(addr, path string) int {
	resp, err := httpGet(addr, path)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func httpBody(addr, path string) string {
	resp, err := httpGet(addr, path)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

func httpGet(addr, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return nil, err
	}
	c := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	return c.Do(req)
}

func waitFor(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", limit, what)
}
