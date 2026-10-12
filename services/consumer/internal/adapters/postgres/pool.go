// Package postgres is the PostgreSQL adapter of the consumer (ADR 0009, 0008,
// 0011, 0010): the pgx pool, the AggregateRepository with its flush transaction
// (stored-position compare-and-set plus batched unnest upsert), the goose
// migrations and the translation of driver errors into ports.ErrUnavailable and
// ports.ErrFenced.
// It never retries; the partition worker owns the only retry loop (ADR 0011).
package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	applicationName = "consumer"
	maxConnLifetime = 30 * time.Minute
	maxConnIdleTime = 5 * time.Minute
)

// Config holds the connection settings. Password is the secret itself: Config
// prints and logs without it, but do not marshal the struct field by field.
type Config struct {
	Host     string
	Port     int
	Database string
	User     string
	Password string
	// SSLMode is a libpq sslmode: disable locally, verify-full in production.
	SSLMode string
	// MaxConns and MinConns bound the pool (ADR 0010: MaxConns >= partitions).
	MaxConns int32
	MinConns int32
	// StatementTimeout turns a stuck statement into a transient 57014 error.
	// Zero means no limit (Migrate always runs without one).
	StatementTimeout time.Duration
}

// String implements fmt.Stringer without the password.
func (c Config) String() string {
	return fmt.Sprintf("postgres %s@%s/%s sslmode=%s", c.User, net.JoinHostPort(c.Host, strconv.Itoa(c.Port)), c.Database, c.SSLMode)
}

// LogValue implements slog.LogValuer without the password.
func (c Config) LogValue() slog.Value { return slog.StringValue(c.String()) }

// connString builds the connection URL with url.UserPassword, never by string
// concatenation, so special characters in the password cannot break parsing
// (ADR 0002). pgx redacts the password in its own parse errors.
func (c Config) connString() string {
	q := url.Values{}
	q.Set("sslmode", c.SSLMode)
	q.Set("application_name", applicationName)
	if c.StatementTimeout > 0 {
		// pgx forwards unknown query parameters as server runtime parameters.
		q.Set("statement_timeout", strconv.FormatInt(max(c.StatementTimeout.Milliseconds(), 1), 10))
	}
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.User, c.Password),
		Host:     net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		Path:     "/" + c.Database,
		RawQuery: q.Encode(),
	}
	return u.String()
}

// NewPool builds the pgx pool (ADR 0010). It does not wait for a connection:
// call AggregateRepository.Ping to check reachability. The caller closes the pool
// once, last, during shutdown (ADR 0012).
func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.connString())
	if err != nil {
		return nil, fmt.Errorf("parse postgres config for %s: %w", cfg, err)
	}
	pc.MaxConns = cfg.MaxConns
	pc.MinConns = cfg.MinConns
	pc.MaxConnLifetime = maxConnLifetime
	pc.MaxConnIdleTime = maxConnIdleTime
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool for %s: %w", cfg, err)
	}
	return pool, nil
}
