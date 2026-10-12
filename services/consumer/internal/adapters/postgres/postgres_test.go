package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

func TestTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"should retry connection exceptions (08)", &pgconn.PgError{Code: "08006"}, true},
		{"should retry serialization failures (40001)", &pgconn.PgError{Code: "40001"}, true},
		{"should retry deadlocks (40P01)", &pgconn.PgError{Code: "40P01"}, true},
		{"should retry insufficient resources (53300)", &pgconn.PgError{Code: "53300"}, true},
		{"should retry admin shutdown (57P01)", &pgconn.PgError{Code: "57P01"}, true},
		{"should retry statement timeout (57014)", &pgconn.PgError{Code: "57014"}, true},
		{"should not classify a CHECK violation (23514)", &pgconn.PgError{Code: "23514"}, false},
		{"should not classify a missing grant (42501)", &pgconn.PgError{Code: "42501"}, false},
		{"should not classify a missing table (42P01)", &pgconn.PgError{Code: "42P01"}, false},
		{"should not classify a double-touched row (21000)", &pgconn.PgError{Code: "21000"}, false},
		{"should not classify an empty SQLSTATE", &pgconn.PgError{}, false},
		{"should find the PgError inside wrapping", fmt.Errorf("outer: %w", &pgconn.PgError{Code: "57P01"}), true},
		{"should retry network errors", &net.OpError{Op: "read", Err: errors.New("connection reset by peer")}, true},
		{"should retry unexpected EOF", fmt.Errorf("read: %w", io.ErrUnexpectedEOF), true},
		{"should retry EOF", io.EOF, true},
		{"should retry deadline exceeded", context.DeadlineExceeded, true},
		{"should not classify arbitrary errors", errors.New("boom"), false},
		{"should not classify nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := transient(tt.err); got != tt.want {
				t.Errorf("transient(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestWrap(t *testing.T) {
	t.Run("should add ErrUnavailable and keep the driver error for a transient failure", func(t *testing.T) {
		pgErr := &pgconn.PgError{Code: "57P01", Message: "terminating connection due to administrator command"}

		err := wrap("upsert aggregates", pgErr)

		if !errors.Is(err, ports.ErrUnavailable) {
			t.Errorf("errors.Is(err, ErrUnavailable) = false for %v", err)
		}
		var got *pgconn.PgError
		if !errors.As(err, &got) || got.Code != "57P01" {
			t.Errorf("errors.As(err, *PgError) = %v, want code 57P01", got)
		}
		if !strings.Contains(err.Error(), "upsert aggregates") {
			t.Errorf("err = %q, want the operation as context", err)
		}
	})
	t.Run("should leave an unknown failure unclassified", func(t *testing.T) {
		err := wrap("upsert aggregates", &pgconn.PgError{Code: "23514"})

		if errors.Is(err, ports.ErrUnavailable) {
			t.Errorf("errors.Is(err, ErrUnavailable) = true for %v", err)
		}
	})
	t.Run("should mark a data exception (class 22) and keep the driver error", func(t *testing.T) {
		for _, code := range []string{"22021", "22P05", "22003"} {
			err := wrap("upsert aggregates", &pgconn.PgError{Code: code})

			if !errors.Is(err, ports.ErrDataException) {
				t.Errorf("%s: errors.Is(err, ErrDataException) = false for %v", code, err)
			}
			if errors.Is(err, ports.ErrUnavailable) {
				t.Errorf("%s: errors.Is(err, ErrUnavailable) = true for %v", code, err)
			}
			var got *pgconn.PgError
			if !errors.As(err, &got) || got.Code != code {
				t.Errorf("%s: errors.As(err, *PgError) = %v", code, got)
			}
		}
	})
	t.Run("should not mark other classes as data exceptions", func(t *testing.T) {
		for _, code := range []string{"23514", "42501", "40001", "02000", ""} {
			if err := wrap("x", &pgconn.PgError{Code: code}); errors.Is(err, ports.ErrDataException) {
				t.Errorf("%q: errors.Is(err, ErrDataException) = true", code)
			}
		}
	})
	t.Run("should return nil for nil", func(t *testing.T) {
		if err := wrap("x", nil); err != nil {
			t.Errorf("wrap(nil) = %v, want nil", err)
		}
	})
}

func TestConnStringKeepsSpecialPasswordCharactersIntact(t *testing.T) {
	cfg := Config{
		Host: "db.internal", Port: 6543, Database: "analytics", User: "analytics_app",
		Password: `p@ss:w/ord?#&% "x"`, SSLMode: "verify-full", StatementTimeout: 2500 * time.Millisecond,
	}

	cc, err := pgx.ParseConfig(cfg.connString())

	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	if cc.Password != cfg.Password || cc.User != cfg.User || cc.Host != cfg.Host || cc.Port != 6543 || cc.Database != cfg.Database {
		t.Errorf("parsed config = user %q host %q port %d db %q password ok=%v", cc.User, cc.Host, cc.Port, cc.Database, cc.Password == cfg.Password)
	}
	if got := cc.RuntimeParams["statement_timeout"]; got != "2500" {
		t.Errorf("statement_timeout = %q, want 2500 (milliseconds)", got)
	}
	if got := cc.RuntimeParams["application_name"]; got != "consumer" {
		t.Errorf("application_name = %q, want consumer", got)
	}
	if cc.TLSConfig == nil {
		t.Error("TLSConfig = nil, want TLS for sslmode=verify-full")
	}
}

func TestConnStringOmitsStatementTimeoutWhenZero(t *testing.T) {
	cfg := Config{Host: "h", Port: 5432, Database: "d", User: "u", Password: "p", SSLMode: "disable"}

	cc, err := pgx.ParseConfig(cfg.connString())

	if err != nil {
		t.Fatalf("ParseConfig() error = %v", err)
	}
	if v, ok := cc.RuntimeParams["statement_timeout"]; ok {
		t.Errorf("statement_timeout = %q, want it unset", v)
	}
}

func TestConfigNeverPrintsThePassword(t *testing.T) {
	cfg := Config{Host: "h", Port: 5432, Database: "d", User: "u", Password: "hunter2", SSLMode: "disable"}

	var logs bytes.Buffer
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("connect", "config", cfg)

	for name, out := range map[string]string{"String": cfg.String(), "slog": logs.String()} {
		if strings.Contains(out, "hunter2") {
			t.Errorf("%s output leaks the password: %s", name, out)
		}
	}
}

func TestNewPoolErrorsDoNotLeakThePassword(t *testing.T) {
	cfg := Config{Host: "h", Port: 5432, Database: "d", User: "u", Password: "hunter2", SSLMode: "no-such-mode", MaxConns: 1}

	_, err := NewPool(context.Background(), cfg)

	if err == nil {
		t.Fatal("NewPool() error = nil, want an invalid sslmode error")
	}
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("NewPool() error leaks the password: %v", err)
	}
}

func TestApplyDeltasRejectsAnInvalidCheckpointWithoutTouchingThePool(t *testing.T) {
	repo := NewAggregateRepository(nil)

	err := repo.ApplyDeltas(context.Background(), ports.Checkpoint{Group: "g", Topic: "t", From: 5, To: 5}, nil)

	if err == nil || errors.Is(err, ports.ErrUnavailable) || errors.Is(err, ports.ErrFenced) {
		t.Errorf("ApplyDeltas(invalid checkpoint) = %v, want an unclassified error", err)
	}
}

func TestNextOffsetsWithoutPartitionsDoesNotTouchThePool(t *testing.T) {
	repo := NewAggregateRepository(nil)

	got, err := repo.NextOffsets(context.Background(), "g", "t", nil)

	if err != nil || len(got) != 0 {
		t.Errorf("NextOffsets(nil) = %v, %v, want empty, nil", got, err)
	}
}

// unusedPort returns a loopback port nothing listens on.
func unusedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err := l.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	return port
}

func TestConnectionRefusedIsUnavailable(t *testing.T) {
	cfg := Config{Host: "127.0.0.1", Port: unusedPort(t), Database: "d", User: "u", Password: "p", SSLMode: "disable", MaxConns: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPool() error = %v", err)
	}
	defer pool.Close()
	repo := NewAggregateRepository(pool)

	err = repo.Ping(ctx)
	if !errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("Ping() error = %v, want it to wrap ports.ErrUnavailable", err)
	}

	err = repo.ApplyDeltas(ctx, ports.Checkpoint{Group: "g", Topic: "t", From: ports.NoPosition, To: 1}, nil)
	if !errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("ApplyDeltas() error = %v, want it to wrap ports.ErrUnavailable", err)
	}

	_, err = repo.NextOffsets(ctx, "g", "t", []int32{0})
	if !errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("NextOffsets() error = %v, want it to wrap ports.ErrUnavailable", err)
	}
}

func TestMigrateConnectionRefusedIsUnavailable(t *testing.T) {
	cfg := Config{Host: "127.0.0.1", Port: unusedPort(t), Database: "d", User: "u", Password: "p", SSLMode: "disable"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := Migrate(ctx, cfg, slog.New(slog.DiscardHandler))

	if !errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("Migrate() error = %v, want it to wrap ports.ErrUnavailable", err)
	}
	if strings.Contains(err.Error(), "127.0.0.1:"+strconv.Itoa(cfg.Port)+"/d?") {
		t.Errorf("Migrate() error leaks the connection URL: %v", err)
	}
}
