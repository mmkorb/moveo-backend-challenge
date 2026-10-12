package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/migrations"
)

// Migrate applies every pending embedded migration (ADR 0010). cfg must carry the
// owner credentials (analytics_owner): the migrations run DDL and GRANT. A Postgres
// session advisory lock serializes concurrent runs, so replicas that start at the
// same time cannot interleave. No statement timeout is applied to DDL.
//
// Connection problems wrap ports.ErrUnavailable so the caller can retry them;
// anything else (bad SQL, missing role) is returned unclassified.
func Migrate(ctx context.Context, cfg Config, logger *slog.Logger) error {
	cfg.StatementTimeout = 0
	cc, err := pgx.ParseConfig(cfg.connString())
	if err != nil {
		return fmt.Errorf("parse postgres config for %s: %w", cfg, err)
	}
	db := stdlib.OpenDB(*cc)
	defer func() { _ = db.Close() }() // the migration result is already decided; a failed close changes nothing

	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("migration locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS, goose.WithSessionLocker(locker))
	if err != nil {
		return wrap("goose provider", err)
	}
	results, err := p.Up(ctx)
	for _, r := range results {
		logger.InfoContext(ctx, "migration applied", "version", r.Source.Version, "file", r.Source.Path, "took", r.Duration.String())
	}
	if err != nil {
		return wrap("migrate up", err)
	}
	if len(results) == 0 {
		logger.InfoContext(ctx, "database schema is up to date")
	}
	return nil
}
