package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/config"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/telemetry"
)

// run is the composition root of `consumer run` (ADR 0003, 0012). Shutdown follows
// ADR 0012: readiness off -> members flush, commit and leave -> health server ->
// DLQ publisher -> pool, which is closed last.
func run(ctx context.Context, stdout io.Writer) error {
	return runWith(ctx, stdout, startPipeline)
}

// pipelineStarter builds the Kafka side; production uses startPipeline, tests a fake.
type pipelineStarter func(ctx context.Context, d pipelineDeps) (pipeline, error)

func runWith(ctx context.Context, stdout io.Writer, start pipelineStarter) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := telemetry.NewLogger(stdout, cfg.LogLevel)
	reg := telemetry.NewRegistry()
	logger.InfoContext(ctx, "starting",
		"workers", cfg.ConsumerWorkers, "topic", cfg.Kafka.Topic, "group", cfg.Kafka.Group,
		"instance_id", cfg.Kafka.InstanceID, "http_addr", cfg.HTTPAddr)

	backoff := startupBackoff{Timeout: cfg.StartupTimeout, Base: cfg.RetryBaseDelay, Max: cfg.RetryMaxDelay}

	pg := postgresConfig(cfg.Postgres)
	pool, err := postgres.NewPool(ctx, pg)
	if err != nil {
		return fmt.Errorf("create postgres pool: %w", err)
	}
	defer pool.Close() // stage 6b: last
	repo := postgres.NewAggregateRepository(pool)
	err = retryStartup(ctx, backoff, logger, "postgres", retryUnavailable, repo.Ping)
	if err != nil {
		return interrupted(ctx, logger, "connect postgres", err)
	}
	logger.InfoContext(ctx, "postgres reachable", "target", pg.String())

	health := telemetry.NewServer(cfg.HTTPAddr, reg, logger,
		telemetry.ReadinessCheck{Name: "postgres", Check: repo.Ping})

	pipe, err := start(ctx, pipelineDeps{cfg: cfg, logger: logger, reg: reg, repo: repo, offsets: repo, health: health, backoff: backoff})
	if err != nil {
		return interrupted(ctx, logger, "start pipeline", err)
	}
	defer func() { // stage 6a: DLQ publisher, before the pool
		if pipe.close != nil {
			pipe.close()
		}
	}()

	return serve(ctx, cfg, logger, health, pipe.members)
}

// interrupted turns a startup failure caused by a signal into a clean exit, and
// wraps every other failure with what was being done.
func interrupted(ctx context.Context, logger *slog.Logger, doing string, err error) error {
	if ctx.Err() != nil {
		logger.InfoContext(ctx, "interrupted during startup", "while", doing)
		return nil
	}
	return fmt.Errorf("%s: %w", doing, err)
}

// healthServer is what serve needs from telemetry.Server; tests substitute a fake.
type healthServer interface {
	MarkNotReady()
	ListenAndServe() error
	Shutdown(ctx context.Context) error
}

var _ healthServer = (*telemetry.Server)(nil)

// serve runs the group members and the health server until ctx is done or one of
// them fails, then shuts down in the order of ADR 0012. Each member's Run performs
// stages 2-4 itself (flush, commit, leave the group) and returns when finished.
func serve(ctx context.Context, cfg config.Config, logger *slog.Logger, health healthServer, members []func(context.Context) error) error {
	g, gctx := errgroup.WithContext(ctx)

	consumers, cctx := errgroup.WithContext(gctx)
	// Stage 1: readiness flips as soon as the members start stopping, whether
	// because of a signal, a failed member or a failed health server.
	context.AfterFunc(cctx, health.MarkNotReady)
	for _, runMember := range members {
		consumers.Go(func() error { return runMember(cctx) })
	}

	g.Go(func() error {
		err := consumers.Wait() // every member has flushed, committed and left
		// Stage 5: the health server goes after the Kafka clients.
		shCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.HTTPShutdownTimeout)
		defer cancel()
		start := time.Now()
		shErr := health.Shutdown(shCtx)
		logger.InfoContext(ctx, "shutdown stage", "stage", "health_server", "took", time.Since(start).String())
		return errors.Join(err, shErr)
	})
	g.Go(func() error {
		if err := health.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("health server: %w", err)
		}
		return nil
	})
	return g.Wait()
}
