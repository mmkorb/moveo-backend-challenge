package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestNewLoggerStampsServiceAndHonorsLevel(t *testing.T) {
	t.Parallel()
	buf := &bytes.Buffer{}
	logger := NewLogger(buf, "producer", slog.LevelWarn)

	logger.Info("dropped")
	logger.Warn("kept", "k", "v")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("got %d records, want 1: %s", len(lines), buf)
	}
	var rec map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("log line is not JSON: %v", err)
	}
	if rec["service"] != "producer" || rec["msg"] != "kept" || rec["level"] != "WARN" {
		t.Errorf("record = %v", rec)
	}
}

func TestMetricsCountAndExposeFixedLabels(t *testing.T) {
	t.Parallel()
	reg, err := NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	m, err := NewMetrics(reg)
	if err != nil {
		t.Fatal(err)
	}

	m.ActivityPublished(ResultOK)
	m.ActivityPublished(ResultOK)
	m.ActivityPublished(ResultInvalid)
	m.Retry(DependencyKafka)

	want := `
# HELP activities_published_total Activity publish requests by outcome.
# TYPE activities_published_total counter
activities_published_total{result="invalid"} 1
activities_published_total{result="kafka_error"} 0
activities_published_total{result="ok"} 2
# HELP retries_total Retries of a dependency call by dependency.
# TYPE retries_total counter
retries_total{dependency="kafka"} 1
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), MetricActivitiesPublished, MetricRetries); err != nil {
		t.Error(err)
	}
}

func TestNewMetricsRejectsDuplicateRegistration(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	if _, err := NewMetrics(reg); err != nil {
		t.Fatal(err)
	}
	if _, err := NewMetrics(reg); err == nil {
		t.Error("second NewMetrics on the same registry succeeded, want an error")
	}
}

func TestNewRegistryHasRuntimeCollectors(t *testing.T) {
	t.Parallel()
	reg, err := NewRegistry()
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	Handler(reg).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "go_goroutines") {
		t.Errorf("metrics = %d, lacks go_goroutines", rec.Code)
	}
}

func TestReadinessCachesTheDependencyCheck(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	failNext := false
	r := NewReadiness(func(context.Context) error {
		calls.Add(1)
		if failNext {
			return errors.New("kafka down")
		}
		return nil
	}, 2*time.Second)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return now }
	ctx := context.Background()

	if err := r.Check(ctx); err != nil {
		t.Fatalf("first check: %v", err)
	}
	failNext = true
	if err := r.Check(ctx); err != nil {
		t.Fatalf("cached check must still be ok: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("dependency checked %d times within the TTL, want 1", got)
	}

	now = now.Add(3 * time.Second)
	if err := r.Check(ctx); err == nil {
		t.Fatal("check after the TTL must see the failure")
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("dependency checked %d times, want 2", got)
	}
}

func TestReadinessDoesNotCacheACancelledCheck(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	r := NewReadiness(func(ctx context.Context) error { calls.Add(1); return ctx.Err() }, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.Check(ctx); err == nil {
		t.Fatal("cancelled check should fail")
	}
	if err := r.Check(context.Background()); err != nil {
		t.Errorf("a later check must not see the cancelled result: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("checks = %d, want 2", got)
	}
}

func TestReadinessMarkNotReadyWins(t *testing.T) {
	t.Parallel()
	r := NewReadiness(func(context.Context) error { return nil }, time.Hour)
	if err := r.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.MarkNotReady()
	if err := r.Check(context.Background()); !errors.Is(err, ErrNotReady) {
		t.Errorf("err = %v, want ErrNotReady", err)
	}
}
