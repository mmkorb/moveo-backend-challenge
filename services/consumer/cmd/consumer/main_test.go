package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/kafka"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/config"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/retry"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// setEnv clears every variable the service reads, then applies kv (key, value, ...)
// and points DATABASE_PASSWORD_FILE at a file holding password.
func setEnv(t *testing.T, password string, kv ...string) {
	t.Helper()
	for _, k := range []string{
		"LOG_LEVEL", "HTTP_ADDR", "KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_DLQ_TOPIC", "CONSUMER_GROUP",
		"CONSUMER_WORKERS", "FLUSH_INTERVAL", "FLUSH_MAX_RECORDS", "REVOKE_FLUSH_TIMEOUT",
		"RETRY_BASE_DELAY", "RETRY_MAX_DELAY", "STARTUP_TIMEOUT", "DRAIN_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT",
		"DATABASE_HOST", "DATABASE_PORT", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD_FILE",
		"DATABASE_SSLMODE", "DATABASE_MAX_CONNS", "DATABASE_MIN_CONNS", "DATABASE_STATEMENT_TIMEOUT",
	} {
		t.Setenv(k, "")
	}
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte(password), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	t.Setenv("DATABASE_PASSWORD_FILE", path)
	for i := 0; i < len(kv); i += 2 {
		t.Setenv(kv[i], kv[i+1])
	}
}

func unusedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	_, port, _ := net.SplitHostPort(l.Addr().String())
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	return port
}

func TestDispatchRejectsBadCommandLines(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"should reject an unknown command", []string{"serve"}},
		{"should reject extra arguments", []string{"migrate", "--force"}},
		{"should reject arguments to the default command", []string{"run", "x"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := dispatch(t.Context(), tt.args, io.Discard)

			if !errors.Is(err, errUsage) {
				t.Errorf("dispatch(%v) = %v, want errUsage", tt.args, err)
			}
		})
	}
}

func TestHealthcheck(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer srv.Close()

	t.Run("should exit cleanly when ready", func(t *testing.T) {
		t.Setenv("HTTP_ADDR", srv.Listener.Addr().String())

		if err := dispatch(t.Context(), []string{"healthcheck"}, io.Discard); err != nil {
			t.Errorf("healthcheck error = %v", err)
		}
	})
	t.Run("should fail when not ready", func(t *testing.T) {
		t.Setenv("HTTP_ADDR", srv.Listener.Addr().String())
		status.Store(http.StatusServiceUnavailable)
		defer status.Store(http.StatusOK)

		if err := dispatch(t.Context(), []string{"healthcheck"}, io.Discard); err == nil {
			t.Error("healthcheck error = nil, want a failure for 503")
		}
	})
	t.Run("should fail when nothing listens", func(t *testing.T) {
		t.Setenv("HTTP_ADDR", "127.0.0.1:"+unusedPort(t))

		if err := dispatch(t.Context(), []string{"healthcheck"}, io.Discard); err == nil {
			t.Error("healthcheck error = nil, want a connection failure")
		}
	})
	t.Run("should not need secrets or other settings", func(t *testing.T) {
		setEnv(t, "x", "HTTP_ADDR", srv.Listener.Addr().String(), "DATABASE_PASSWORD_FILE", filepath.Join(t.TempDir(), "absent"), "KAFKA_BROKERS", "garbage")

		if err := dispatch(t.Context(), []string{"healthcheck"}, io.Discard); err != nil {
			t.Errorf("healthcheck error = %v", err)
		}
	})
	t.Run("should reject an invalid HTTP_ADDR", func(t *testing.T) {
		t.Setenv("HTTP_ADDR", "nonsense")

		if err := dispatch(t.Context(), []string{"healthcheck"}, io.Discard); err == nil {
			t.Error("healthcheck error = nil, want a configuration error")
		}
	})
}

func TestPostgresConfigMapsEveryField(t *testing.T) {
	in := config.Postgres{Host: "h", Port: 6543, Name: "analytics", User: "u", Password: "pw", SSLMode: "require", MaxConns: 9, MinConns: 2, StatementTimeout: 3 * time.Second}

	got := postgresConfig(in)

	want := postgres.Config{Host: "h", Port: 6543, Database: "analytics", User: "u", Password: "pw", SSLMode: "require", MaxConns: 9, MinConns: 2, StatementTimeout: 3 * time.Second}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("postgresConfig mismatch (-want +got):\n%s", diff)
	}
}

func TestKafkaConfigMapsEveryField(t *testing.T) {
	in := config.Config{
		Kafka: config.Kafka{
			Brokers: []string{"k:9092"}, Topic: "t", DLQTopic: "t.dlq", Group: "g",
			InstanceID: "host-1", SessionTimeout: 40 * time.Second,
		},
		FlushInterval: time.Second, FlushMaxRecords: 7, FlushTimeout: 8 * time.Second,
		RevokeFlushTimeout: 9 * time.Second, RetryBaseDelay: time.Millisecond, RetryMaxDelay: time.Second,
		DrainTimeout: 12 * time.Second,
	}

	got := kafkaConfig(in)

	want := kafka.Config{
		Brokers: []string{"k:9092"}, Topic: "t", DLQTopic: "t.dlq", Group: "g",
		FlushInterval: time.Second, FlushMaxRecords: 7, RevokeFlushTimeout: 9 * time.Second,
		Retry:        retry.Policy{Base: time.Millisecond, Max: time.Second},
		DrainTimeout: 12 * time.Second, FlushTimeout: 8 * time.Second,
		InstanceID: "host-1", SessionTimeout: 40 * time.Second,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("kafkaConfig mismatch (-want +got):\n%s", diff)
	}
}

func TestMigrateFailsFastOnBadConfig(t *testing.T) {
	setEnv(t, "pw", "DATABASE_PORT", "not-a-port")

	err := dispatch(t.Context(), []string{"migrate"}, io.Discard)

	if err == nil || !strings.Contains(err.Error(), "DATABASE_PORT") {
		t.Errorf("migrate error = %v, want a configuration error naming DATABASE_PORT", err)
	}
}

func TestMigrateGivesUpWhenPostgresStaysDown(t *testing.T) {
	const secret = "s3cr3t-owner-value"
	setEnv(t, secret,
		"DATABASE_HOST", "127.0.0.1", "DATABASE_PORT", unusedPort(t),
		"STARTUP_TIMEOUT", "400ms", "RETRY_BASE_DELAY", "5ms", "RETRY_MAX_DELAY", "20ms")
	var out bytes.Buffer

	start := time.Now()
	err := dispatch(t.Context(), []string{"migrate"}, &out)

	if !errors.Is(err, ports.ErrUnavailable) || !strings.Contains(err.Error(), "retry aborted") {
		t.Errorf("migrate error = %v, want it to give up with ErrUnavailable", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("migrate took %v, want it bounded by STARTUP_TIMEOUT", took)
	}
	if strings.Contains(out.String(), secret) || strings.Contains(err.Error(), secret) {
		t.Errorf("the password leaked into the output or the error: %s / %v", out.String(), err)
	}
}

func TestRunFailsFastOnBadConfig(t *testing.T) {
	setEnv(t, "pw", "CONSUMER_WORKERS", "0")

	err := dispatch(t.Context(), []string{"run"}, io.Discard)

	if err == nil || !strings.Contains(err.Error(), "CONSUMER_WORKERS must be > 0") {
		t.Errorf("run error = %v, want a configuration error", err)
	}
}

func TestRunGivesUpWhenPostgresStaysDown(t *testing.T) {
	setEnv(t, "pw",
		"DATABASE_HOST", "127.0.0.1", "DATABASE_PORT", unusedPort(t),
		"STARTUP_TIMEOUT", "400ms", "RETRY_BASE_DELAY", "5ms", "RETRY_MAX_DELAY", "20ms")

	err := dispatch(t.Context(), []string{"run"}, io.Discard)

	if !errors.Is(err, ports.ErrUnavailable) || !strings.Contains(err.Error(), "connect postgres") {
		t.Errorf("run error = %v, want a postgres startup failure", err)
	}
}

func TestRunExitsCleanlyWhenInterruptedDuringStartup(t *testing.T) {
	setEnv(t, "pw",
		"DATABASE_HOST", "127.0.0.1", "DATABASE_PORT", unusedPort(t),
		"STARTUP_TIMEOUT", "30s", "RETRY_BASE_DELAY", "5ms", "RETRY_MAX_DELAY", "20ms")
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	err := dispatch(ctx, []string{"run"}, io.Discard)

	if err != nil {
		t.Errorf("run error = %v, want nil: a signal during startup is not a failure", err)
	}
}

func TestRetryStartup(t *testing.T) {
	policy := startupBackoff{Timeout: time.Minute, Base: 100 * time.Millisecond, Max: 2 * time.Second}
	transient := fmtUnavailable("db down")

	t.Run("should retry transient errors until the operation succeeds", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			attempts := 0

			err := retryStartup(t.Context(), policy, discardLogger(), "postgres", retryUnavailable, func(context.Context) error {
				attempts++
				if attempts < 5 {
					return transient
				}
				return nil
			})

			if err != nil || attempts != 5 {
				t.Errorf("err = %v after %d attempts, want success on the 5th", err, attempts)
			}
		})
	})
	t.Run("should not retry an error the predicate rejects", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			attempts := 0
			boom := errors.New("password authentication failed")
			start := time.Now()

			err := retryStartup(t.Context(), policy, discardLogger(), "postgres", retryUnavailable, func(context.Context) error {
				attempts++
				return boom
			})

			if !errors.Is(err, boom) || attempts != 1 || time.Since(start) != 0 {
				t.Errorf("err = %v, attempts = %d, waited %v; want an immediate failure", err, attempts, time.Since(start))
			}
		})
	})
	t.Run("should give up when the budget is spent and report the last error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			start := time.Now()

			err := retryStartup(t.Context(), policy, discardLogger(), "postgres", retryUnavailable, func(context.Context) error { return transient })

			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, ports.ErrUnavailable) {
				t.Errorf("err = %v, want the deadline and the last error in the chain", err)
			}
			if took := time.Since(start); took != policy.Timeout {
				t.Errorf("gave up after %v, want exactly STARTUP_TIMEOUT (%v)", took, policy.Timeout)
			}
		})
	})
	t.Run("should never wait longer than Max between attempts", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var last time.Time
			var longest time.Duration
			retryStartup(t.Context(), policy, discardLogger(), "postgres", retryUnavailable, func(context.Context) error { //nolint:errcheck // only the timing matters
				if !last.IsZero() {
					longest = max(longest, time.Since(last))
				}
				last = time.Now()
				return transient
			})

			if longest > policy.Max {
				t.Errorf("longest wait = %v, want <= %v", longest, policy.Max)
			}
		})
	})
	t.Run("should stop promptly when the context is canceled during a backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			slow := startupBackoff{Timeout: time.Hour, Base: time.Hour, Max: time.Hour}
			start := time.Now()
			go func() { time.Sleep(10 * time.Millisecond); cancel() }()

			err := retryStartup(ctx, slow, discardLogger(), "postgres", retryUnavailable, func(context.Context) error { return transient })

			if !errors.Is(err, context.Canceled) {
				t.Errorf("err = %v, want context.Canceled", err)
			}
			if took := time.Since(start); took > time.Second {
				t.Errorf("returned after %v, want it right after the cancel", took)
			}
		})
	})
	t.Run("should not overflow with a huge base delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			huge := startupBackoff{Timeout: time.Hour, Base: 1000 * time.Hour, Max: 2000 * time.Hour}
			attempts := 0

			err := retryStartup(t.Context(), huge, discardLogger(), "postgres", retryUnavailable, func(context.Context) error {
				attempts++
				if attempts < 3 {
					return transient
				}
				return nil
			})

			// The waits are jittered in [0, ceil], so only completion within the
			// budget or a clean give-up is acceptable; a negative wait would panic.
			if err != nil && !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("err = %v", err)
			}
		})
	})
}

func fmtUnavailable(msg string) error {
	return errors.Join(ports.ErrUnavailable, errors.New(msg))
}

// fakeHealth records the order of health server calls.
type fakeHealth struct {
	mu          sync.Mutex
	events      []string
	listenErr   error
	shutdownErr error
	closed      chan struct{}
	once        sync.Once
	deadline    time.Duration
	ctxErr      error
}

func newFakeHealth() *fakeHealth { return &fakeHealth{closed: make(chan struct{})} }

func (f *fakeHealth) record(e string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
}

func (f *fakeHealth) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.events...)
}

func (f *fakeHealth) MarkNotReady() { f.record("not_ready") }

func (f *fakeHealth) ListenAndServe() error {
	if f.listenErr != nil {
		return f.listenErr
	}
	<-f.closed
	return http.ErrServerClosed
}

func (f *fakeHealth) Shutdown(ctx context.Context) error {
	f.record("health_shutdown")
	if dl, ok := ctx.Deadline(); ok {
		f.deadline = time.Until(dl)
	}
	f.ctxErr = ctx.Err()
	f.once.Do(func() { close(f.closed) })
	return f.shutdownErr
}

// member returns a group member that runs until ctx ends, takes `drain` to stop and
// then records "member_<name>_done", like a real member flushing and leaving.
func member(f *fakeHealth, name string, drain time.Duration) func(context.Context) error {
	return func(ctx context.Context) error {
		<-ctx.Done()
		time.Sleep(drain)
		f.record("member_" + name + "_done")
		return nil
	}
}

func serveConfig() config.Config {
	return config.Config{HTTPShutdownTimeout: 5 * time.Second}
}

func TestServeShutdownOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newFakeHealth()
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- serve(ctx, serveConfig(), discardLogger(), h, []func(context.Context) error{
				member(h, "a", 3*time.Second), member(h, "b", 8*time.Second),
			})
		}()
		synctest.Wait()
		if got := h.snapshot(); len(got) != 0 {
			t.Fatalf("events before the signal = %v, want none", got)
		}

		cancel() // SIGTERM
		err := <-done

		if err != nil {
			t.Fatalf("serve() = %v, want nil after a graceful stop", err)
		}
		got := h.snapshot()
		if len(got) != 4 || got[0] != "not_ready" || got[3] != "health_shutdown" {
			t.Errorf("events = %v, want not_ready first, health_shutdown last, both members in between", got)
		}
		if !strings.Contains(strings.Join(got[1:3], ","), "member_a_done") || !strings.Contains(strings.Join(got[1:3], ","), "member_b_done") {
			t.Errorf("events = %v, want both members finished before the health server stops", got)
		}
		if h.deadline != 5*time.Second {
			t.Errorf("health shutdown deadline = %v, want HTTP_SHUTDOWN_TIMEOUT (5s)", h.deadline)
		}
		if h.ctxErr != nil {
			t.Errorf("health shutdown context was already done (%v): it must be detached from the canceled parent", h.ctxErr)
		}
	})
}

func TestServeStopsEverythingWhenAMemberFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newFakeHealth()
		boom := errors.New("member failed")

		err := serve(t.Context(), serveConfig(), discardLogger(), h, []func(context.Context) error{
			func(context.Context) error { return boom },
			member(h, "other", time.Second),
		})

		if !errors.Is(err, boom) {
			t.Errorf("serve() = %v, want the member's error", err)
		}
		got := h.snapshot()
		if len(got) != 3 || got[0] != "not_ready" || got[2] != "health_shutdown" {
			t.Errorf("events = %v, want readiness off, the other member stopped, then the health server", got)
		}
	})
}

func TestServeStopsMembersWhenTheHealthServerFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newFakeHealth()
		h.listenErr = errors.New("address already in use")

		err := serve(t.Context(), serveConfig(), discardLogger(), h, []func(context.Context) error{member(h, "a", time.Second)})

		if err == nil || !strings.Contains(err.Error(), "health server: address already in use") {
			t.Errorf("serve() = %v, want the listen error", err)
		}
		got := strings.Join(h.snapshot(), ",")
		if !strings.Contains(got, "member_a_done") {
			t.Errorf("events = %q, want the member stopped", got)
		}
	})
}

func TestServeReportsAHealthShutdownFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newFakeHealth()
		h.shutdownErr = errors.New("shutdown timed out")
		ctx, cancel := context.WithCancel(t.Context())
		cancel()

		err := serve(ctx, serveConfig(), discardLogger(), h, []func(context.Context) error{member(h, "a", 0)})

		if err == nil || !strings.Contains(err.Error(), "shutdown timed out") {
			t.Errorf("serve() = %v, want the shutdown error", err)
		}
	})
}
