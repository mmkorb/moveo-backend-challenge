package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/kafka"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/application"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/config"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/retry"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/telemetry"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// pipelineDeps is everything startPipeline may need from the composition root.
type pipelineDeps struct {
	cfg     config.Config
	logger  *slog.Logger
	reg     *prometheus.Registry
	repo    ports.AggregateRepository
	offsets ports.OffsetStore
	health  *telemetry.Server
	backoff startupBackoff
}

// pipeline is the Kafka side of the service: the group members (each Run performs
// ADR 0012 stages 2-4 and returns after a graceful stop) and the cleanup that
// closes the DLQ publisher (stage 6a), which runs before the pool is closed.
type pipeline struct {
	members []func(ctx context.Context) error
	close   func()
}

// errNoMemberJoined is the /readyz failure until a member has been assigned
// partitions at least once (ADR 0013).
var errNoMemberJoined = errors.New("no consumer group member has joined yet")

// kafkaConfig maps the platform config onto the adapter's own config type; the
// platform package never imports adapters (ADR 0003).
func kafkaConfig(cfg config.Config) kafka.Config {
	return kafka.Config{
		Brokers:            cfg.Kafka.Brokers,
		Topic:              cfg.Kafka.Topic,
		DLQTopic:           cfg.Kafka.DLQTopic,
		Group:              cfg.Kafka.Group,
		FlushInterval:      cfg.FlushInterval,
		FlushMaxRecords:    cfg.FlushMaxRecords,
		RevokeFlushTimeout: cfg.RevokeFlushTimeout,
		Retry:              retry.Policy{Base: cfg.RetryBaseDelay, Max: cfg.RetryMaxDelay},
		DrainTimeout:       cfg.DrainTimeout,
		FlushTimeout:       cfg.FlushTimeout,
		InstanceID:         cfg.Kafka.InstanceID,
		SessionTimeout:     cfg.Kafka.SessionTimeout,
	}
}

// startPipeline waits for Kafka (brokers reachable, both topics exist), builds the
// DLQ publisher and the group members, registers the readiness check and returns
// the functions `serve` runs. Readiness needs at least one joined member
// (ADR 0013); the Postgres half is registered by the composition root.
func startPipeline(ctx context.Context, d pipelineDeps) (pipeline, error) {
	kcfg := kafkaConfig(d.cfg)

	// Every Ping failure is retried within STARTUP_TIMEOUT: a missing topic is
	// usually kafka-init still running, and the config was validated by Load.
	err := retryStartup(ctx, d.backoff, d.logger, "kafka", func(error) bool { return true },
		func(ctx context.Context) error { return kafka.Ping(ctx, kcfg) })
	if err != nil {
		return pipeline{}, err
	}
	d.logger.InfoContext(ctx, "kafka reachable", "topic", kcfg.Topic, "dlq_topic", kcfg.DLQTopic)

	dlq, err := kafka.NewDeadLetterPublisher(kcfg, d.logger, d.reg)
	if err != nil {
		return pipeline{}, fmt.Errorf("create dlq publisher: %w", err)
	}
	process := application.NewProcessActivity(d.repo, dlq)
	positions := application.NewResumePositions(d.offsets)
	members, err := kafka.NewGroupMembers(kcfg, d.cfg.ConsumerWorkers, process, positions, d.logger, d.reg)
	if err != nil {
		dlq.Close()
		return pipeline{}, fmt.Errorf("create group members: %w", err)
	}

	d.health.AddReadinessCheck(telemetry.ReadinessCheck{
		Name: "kafka group",
		Check: func(context.Context) error {
			for _, m := range members {
				if m.Joined() {
					return nil
				}
			}
			return errNoMemberJoined
		},
	})

	runs := make([]func(context.Context) error, len(members))
	for i, m := range members {
		runs[i] = m.Run
	}
	return pipeline{members: runs, close: dlq.Close}, nil
}
