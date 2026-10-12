package main

import (
	"context"
	"fmt"
	"io"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/config"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/telemetry"
)

// postgresConfig maps the platform config to the adapter's own config type; the
// platform package never imports adapters (ADR 0003).
func postgresConfig(p config.Postgres) postgres.Config {
	return postgres.Config{
		Host:             p.Host,
		Port:             p.Port,
		Database:         p.Name,
		User:             p.User,
		Password:         p.Password.Value(),
		SSLMode:          p.SSLMode,
		MaxConns:         p.MaxConns,
		MinConns:         p.MinConns,
		StatementTimeout: p.StatementTimeout,
	}
}

// migrate applies the embedded migrations (ADR 0010). It runs as the one-shot
// Compose service consumer-migrate with the owner credentials. Connection
// failures are retried within STARTUP_TIMEOUT; anything else fails at once.
func migrate(ctx context.Context, stdout io.Writer) error {
	cfg, err := config.LoadMigrate()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := telemetry.NewLogger(stdout, cfg.LogLevel)
	pg := postgresConfig(cfg.Postgres)
	logger.InfoContext(ctx, "migrating database", "target", pg.String())

	backoff := startupBackoff{Timeout: cfg.StartupTimeout, Base: cfg.RetryBaseDelay, Max: cfg.RetryMaxDelay}
	err = retryStartup(ctx, backoff, logger, "postgres", retryUnavailable, func(ctx context.Context) error {
		return postgres.Migrate(ctx, pg, logger)
	})
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

// healthcheck is the image HEALTHCHECK: GET /readyz on the local health server,
// exit status 0 only for a 200 (ADR 0013). It reads HTTP_ADDR and nothing else, so
// it needs no secrets.
func healthcheck(ctx context.Context) error {
	addr, err := config.LoadHTTPAddr()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return telemetry.Probe(ctx, addr)
}
