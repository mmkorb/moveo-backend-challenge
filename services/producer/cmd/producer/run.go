package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
	"uuid"

	"golang.org/x/sync/errgroup"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/adapters/httpapi"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/adapters/kafka"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/application"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/config"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/retry"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/telemetry"
)

const (
	serviceName = "producer"

	// readinessTTL caches the Kafka ping behind /readyz so frequent probes do
	// not hammer the broker (ADR 0013).
	readinessTTL = 1500 * time.Millisecond

	// logEveryNthRetry rate-limits the startup retry log: the first attempt,
	// then every 10th (ADR 0011).
	logEveryNthRetry = 10
)

// startupBackoff is the retry policy of the startup topic check (ADR 0011).
var startupBackoff = retry.Policy{Base: 200 * time.Millisecond, Max: 5 * time.Second}

var errStarting = errors.New("startup check not finished")

// run wires the service and serves until ctx is cancelled or a component
// fails. It stops in the order of ADR 0012: readiness off, in-flight requests
// finish, buffered records are flushed, then the Kafka client closes.
func run(ctx context.Context, stdout io.Writer) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := telemetry.NewLogger(stdout, serviceName, cfg.LogLevel)

	reg, err := telemetry.NewRegistry()
	if err != nil {
		return fmt.Errorf("create metrics registry: %w", err)
	}
	metrics, err := telemetry.NewMetrics(reg)
	if err != nil {
		return fmt.Errorf("create metrics: %w", err)
	}

	publisher, err := kafka.NewPublisher(kafka.Config{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		PublishTimeout: cfg.PublishTimeout,
	}, reg)
	if err != nil {
		return fmt.Errorf("create kafka publisher: %w", err)
	}
	// Stages 3 and 4: deliver anything still buffered, then release the
	// client. Deferred so it also runs when a later step fails.
	defer func() {
		drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.DrainTimeout)
		defer cancel()
		start := time.Now()
		if err := publisher.Flush(drainCtx); err != nil {
			logger.Error("flush kafka client", "error", err)
		}
		logStage(logger, "flush", start)
		publisher.Close()
		logger.Info("producer stopped")
	}()

	registerActivity, err := application.NewRegisterActivity(publisher, uuid.NewV7)
	if err != nil {
		return fmt.Errorf("create register activity use case: %w", err)
	}

	var topicChecked atomic.Bool
	readiness := telemetry.NewReadiness(func(ctx context.Context) error {
		if !topicChecked.Load() {
			return errStarting
		}
		return publisher.Ping(ctx)
	}, readinessTTL)

	router, err := httpapi.NewRouter(httpapi.Deps{
		Registrar:      registerActivity,
		Metrics:        metrics,
		Ready:          readiness.Check,
		MetricsHandler: telemetry.Handler(reg),
		Logger:         logger,
		PublishTimeout: cfg.PublishTimeout,
	})
	if err != nil {
		return fmt.Errorf("create router: %w", err)
	}
	srv := newServer(cfg.HTTPAddr, router, logger)

	g, gctx := errgroup.WithContext(ctx)
	context.AfterFunc(gctx, readiness.MarkNotReady) // stage 1

	g.Go(func() error {
		logger.Info("producer listening", "addr", cfg.HTTPAddr, "topic", cfg.KafkaTopic)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})
	g.Go(func() error { // stage 2: finish in-flight requests, each waits for its Kafka ack
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.HTTPShutdownTimeout)
		defer cancel()
		start := time.Now()
		err := srv.Shutdown(shutdownCtx)
		logStage(logger, "http_drain", start)
		if err != nil {
			return fmt.Errorf("shut down http server: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		return checkTopic(gctx, cfg.StartupTimeout, publisher, metrics, logger, &topicChecked)
	})

	return g.Wait()
}

// checkTopic retries the Kafka topic check until it passes, StartupTimeout
// expires or ctx ends. A shutdown signal during startup is not an error.
func checkTopic(ctx context.Context, timeout time.Duration, publisher *kafka.Publisher,
	metrics *telemetry.Metrics, logger *slog.Logger, done *atomic.Bool) error {
	startCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	onRetry := func(attempt int, wait time.Duration, err error) {
		metrics.Retry(telemetry.DependencyKafka)
		if attempt == 1 || attempt%logEveryNthRetry == 0 {
			logger.Warn("kafka topic check failed, retrying",
				"dependency", telemetry.DependencyKafka, "attempt", attempt, "backoff", wait, "error", err)
		}
	}
	err := retry.Do(startCtx, startupBackoff, nil, onRetry, publisher.EnsureTopic)
	switch {
	case err == nil:
		done.Store(true)
		logger.Info("kafka topic check passed")
		return nil
	case ctx.Err() != nil:
		return nil // shutting down before startup finished
	default:
		return fmt.Errorf("kafka startup check (waited %s): %w", timeout, err)
	}
}

// newServer returns the HTTP server with the timeouts required by ADR 0005
// and ADR 0002.
func newServer(addr string, handler http.Handler, logger *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
}

func logStage(logger *slog.Logger, stage string, start time.Time) {
	logger.Info("shutdown stage", "stage", stage, "took", time.Since(start))
}
