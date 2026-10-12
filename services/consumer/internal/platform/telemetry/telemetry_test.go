package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestNewLogger(t *testing.T) {
	t.Run("should write JSON records carrying the service name", func(t *testing.T) {
		var buf bytes.Buffer

		NewLogger(&buf, slog.LevelInfo).Info("hello", "topic", "t", "partition", 3)

		var rec map[string]any
		if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
			t.Fatalf("record is not JSON: %v: %s", err, buf.String())
		}
		if rec["service"] != "consumer" || rec["msg"] != "hello" || rec["level"] != "INFO" || rec["topic"] != "t" || rec["partition"] != float64(3) {
			t.Errorf("record = %v", rec)
		}
	})
	t.Run("should honour the level", func(t *testing.T) {
		var buf bytes.Buffer
		logger := NewLogger(&buf, slog.LevelWarn)

		logger.Info("quiet")
		logger.Debug("quiet")
		logger.Warn("loud")

		if got := strings.Count(buf.String(), "\n"); got != 1 || !strings.Contains(buf.String(), "loud") {
			t.Errorf("output = %q, want only the warning", buf.String())
		}
	})
	t.Run("should not replace the default logger", func(t *testing.T) {
		before := slog.Default()

		NewLogger(io.Discard, slog.LevelInfo)

		if slog.Default() != before {
			t.Error("NewLogger changed slog.Default()")
		}
	})
}

func TestNewRegistryHasRuntimeCollectors(t *testing.T) {
	reg := NewRegistry()

	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}

	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	for _, want := range []string{"go_goroutines", "process_open_fds"} {
		// process_open_fds is Linux/Windows only; accept either process metric.
		if want == "process_open_fds" && (names[want] || names["process_cpu_seconds_total"]) {
			continue
		}
		if !names[want] {
			t.Errorf("registry lacks %s", want)
		}
	}
	if NewRegistry() == reg {
		t.Error("NewRegistry() must return a fresh registry each time")
	}
}

func TestEndpoints(t *testing.T) {
	reg := NewRegistry()
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "records_consumed_total", Help: "test"})
	reg.MustRegister(counter)
	counter.Add(7)
	var ready atomic.Bool
	ready.Store(true)
	s := NewServer(":0", reg, discardLogger(), ReadinessCheck{Name: "postgres", Check: func(context.Context) error {
		if ready.Load() {
			return nil
		}
		return errors.New("password authentication failed for user analytics_app")
	}})
	h := s.Handler()

	t.Run("should answer liveness with 200", func(t *testing.T) {
		if rec := get(t, h, http.MethodGet, "/healthz"); rec.Code != http.StatusOK {
			t.Errorf("/healthz = %d, want 200", rec.Code)
		}
	})
	t.Run("should answer readiness with 200 when every check passes", func(t *testing.T) {
		if rec := get(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
			t.Errorf("/readyz = %d, want 200", rec.Code)
		}
	})
	t.Run("should expose registered metrics", func(t *testing.T) {
		rec := get(t, h, http.MethodGet, "/metrics")

		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "records_consumed_total 7") {
			t.Errorf("/metrics = %d %q, want 200 with the counter", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "go_goroutines") {
			t.Error("/metrics lacks the Go collector")
		}
	})
	t.Run("should reject other methods and unknown paths", func(t *testing.T) {
		if rec := get(t, h, http.MethodPost, "/healthz"); rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST /healthz = %d, want 405", rec.Code)
		}
		if rec := get(t, h, http.MethodGet, "/debug/pprof/"); rec.Code != http.StatusNotFound {
			t.Errorf("GET /debug/pprof/ = %d, want 404", rec.Code)
		}
	})
}

func TestReadinessFailureNamesTheCheckButNotTheError(t *testing.T) {
	var logs bytes.Buffer
	s := NewServer(":0", NewRegistry(), slog.New(slog.NewJSONHandler(&logs, nil)),
		ReadinessCheck{Name: "postgres", Check: func(context.Context) error { return errors.New("secret detail") }},
		ReadinessCheck{Name: "kafka", Check: func(context.Context) error { return nil }},
	)

	rec := get(t, s.Handler(), http.MethodGet, "/readyz")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", rec.Code)
	}
	if body := rec.Body.String(); !strings.Contains(body, "postgres") || strings.Contains(body, "kafka") || strings.Contains(body, "secret detail") {
		t.Errorf("body = %q, want only the failing check name", body)
	}
	if !strings.Contains(logs.String(), "secret detail") || !strings.Contains(logs.String(), `"level":"WARN"`) {
		t.Errorf("logs = %q, want the cause logged at WARN", logs.String())
	}
}

func TestReadinessIsCachedForOneSecond(t *testing.T) {
	var calls atomic.Int32
	s := NewServer(":0", NewRegistry(), discardLogger(), ReadinessCheck{Name: "db", Check: func(context.Context) error {
		calls.Add(1)
		return nil
	}})
	clock := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return clock }
	h := s.Handler()

	get(t, h, http.MethodGet, "/readyz")
	clock = clock.Add(900 * time.Millisecond)
	get(t, h, http.MethodGet, "/readyz")
	if got := calls.Load(); got != 1 {
		t.Fatalf("checks ran %d times within the TTL, want 1", got)
	}
	clock = clock.Add(200 * time.Millisecond)
	get(t, h, http.MethodGet, "/readyz")

	if got := calls.Load(); got != 2 {
		t.Errorf("checks ran %d times after the TTL, want 2", got)
	}
}

func TestAddReadinessCheckInvalidatesTheCache(t *testing.T) {
	s := NewServer(":0", NewRegistry(), discardLogger())
	h := s.Handler()
	if rec := get(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz without checks = %d, want 200", rec.Code)
	}

	s.AddReadinessCheck(ReadinessCheck{Name: "kafka", Check: func(context.Context) error { return errors.New("down") }})

	if rec := get(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz after adding a failing check = %d, want 503 without waiting for the TTL", rec.Code)
	}
}

func TestMarkNotReady(t *testing.T) {
	var calls atomic.Int32
	s := NewServer(":0", NewRegistry(), discardLogger(), ReadinessCheck{Name: "db", Check: func(context.Context) error {
		calls.Add(1)
		return nil
	}})
	h := s.Handler()
	if rec := get(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200 before shutdown", rec.Code)
	}

	s.MarkNotReady()

	if rec := get(t, h, http.MethodGet, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz after MarkNotReady = %d, want 503 even though the cached result is ready", rec.Code)
	}
	if rec := get(t, h, http.MethodGet, "/healthz"); rec.Code != http.StatusOK {
		t.Errorf("/healthz after MarkNotReady = %d, want 200: liveness must survive the drain", rec.Code)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("checks ran %d times, want 1: a shutting-down server stops probing dependencies", got)
	}
}

func TestMarkNotReadyDoesNotWaitForASlowCheck(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	s := NewServer(":0", NewRegistry(), discardLogger(), ReadinessCheck{Name: "db", Check: func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}})
	var wg sync.WaitGroup
	wg.Go(func() { get(t, s.Handler(), http.MethodGet, "/readyz") })
	<-started

	done := make(chan struct{})
	go func() { s.MarkNotReady(); close(done) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Error("MarkNotReady blocked behind a running readiness check")
	}
	close(release)
	wg.Wait()
}

func TestReadinessCheckIsBoundedByATimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := NewServer(":0", NewRegistry(), discardLogger(), ReadinessCheck{Name: "kafka", Check: func(ctx context.Context) error {
			<-ctx.Done() // a dependency that never answers
			return ctx.Err()
		}})

		start := time.Now()
		rec := get(t, s.Handler(), http.MethodGet, "/readyz")

		if rec.Code != http.StatusServiceUnavailable {
			t.Errorf("/readyz = %d, want 503", rec.Code)
		}
		if took := time.Since(start); took != readinessCheckTimeout {
			t.Errorf("check took %v, want exactly the %v timeout", took, readinessCheckTimeout)
		}
	})
}

func TestReadinessLogsOnlyTransitions(t *testing.T) {
	var logs bytes.Buffer
	var fail atomic.Bool
	fail.Store(true)
	s := NewServer(":0", NewRegistry(), slog.New(slog.NewJSONHandler(&logs, nil)), ReadinessCheck{Name: "db", Check: func(context.Context) error {
		if fail.Load() {
			return errors.New("down")
		}
		return nil
	}})
	clock := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	s.now = func() time.Time { return clock }
	tick := func() { clock = clock.Add(2 * readinessCacheTTL); get(t, s.Handler(), http.MethodGet, "/readyz") }

	tick()
	tick()
	tick()
	fail.Store(false)
	tick()
	tick()

	if got := strings.Count(logs.String(), "readiness changed"); got != 2 {
		t.Errorf("transition records = %d, want 2 (down, then up):\n%s", got, logs.String())
	}
}

func TestServeAndShutdown(t *testing.T) {
	s := NewServer("127.0.0.1:0", NewRegistry(), discardLogger())
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Serve(l) }()

	if err := Probe(t.Context(), l.Addr().String()); err != nil {
		t.Errorf("Probe() against the live server error = %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Serve() = %v, want http.ErrServerClosed", err)
	}
	if err := Probe(t.Context(), l.Addr().String()); err == nil {
		t.Error("Probe() after Shutdown succeeded, want a connection error")
	}
}

func TestListenAndServeReportsAnUnusableAddress(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	s := NewServer(l.Addr().String(), NewRegistry(), discardLogger())

	err = s.ListenAndServe()

	if err == nil || errors.Is(err, http.ErrServerClosed) {
		t.Errorf("ListenAndServe() on a busy port = %v, want a listen error", err)
	}
}

func TestServerSetsTimeouts(t *testing.T) {
	s := NewServer(":0", NewRegistry(), discardLogger())

	srv := s.srv

	if srv.ReadHeaderTimeout == 0 || srv.ReadTimeout == 0 || srv.WriteTimeout == 0 || srv.IdleTimeout == 0 || srv.MaxHeaderBytes == 0 {
		t.Errorf("server timeouts/limits unset: %+v", srv)
	}
}

func TestProbe(t *testing.T) {
	status := atomic.Int32{}
	status.Store(http.StatusOK)
	var path atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path.Store(r.URL.Path)
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	t.Run("should succeed on 200 and call /readyz", func(t *testing.T) {
		if err := Probe(t.Context(), addr); err != nil {
			t.Fatalf("Probe() error = %v", err)
		}
		if path.Load() != "/readyz" {
			t.Errorf("probed path = %v, want /readyz", path.Load())
		}
	})
	t.Run("should fail on 503", func(t *testing.T) {
		status.Store(http.StatusServiceUnavailable)
		defer status.Store(http.StatusOK)

		err := Probe(t.Context(), addr)

		if err == nil || !strings.Contains(err.Error(), "503") {
			t.Errorf("Probe() error = %v, want a 503 error", err)
		}
	})
	t.Run("should probe loopback for a wildcard address", func(t *testing.T) {
		_, port, _ := net.SplitHostPort(addr)

		for _, wildcard := range []string{":" + port, "0.0.0.0:" + port} {
			if err := Probe(t.Context(), wildcard); err != nil {
				t.Errorf("Probe(%q) error = %v", wildcard, err)
			}
		}
	})
	t.Run("should fail on a malformed address", func(t *testing.T) {
		if err := Probe(t.Context(), "no-port"); err == nil {
			t.Error("Probe() error = nil, want an address error")
		}
	})
}

func TestProbeTimesOutOnAHangingServer(t *testing.T) {
	hang := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-hang }))
	defer srv.Close()
	defer close(hang)

	start := time.Now()
	err := Probe(t.Context(), srv.Listener.Addr().String())

	if err == nil {
		t.Fatal("Probe() against a hanging server succeeded")
	}
	if took := time.Since(start); took > 4*time.Second {
		t.Errorf("Probe() took %v, want about 2s", took)
	}
}
