# ADR 0003: System decomposition, repository layout, hexagonal architecture, manual DI and configuration

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** both

## Context

- The challenge asks for a **producer** (REST → Kafka) and a **consumer** (Kafka → PostgreSQL), "a Docker image for both", and horizontal scaling of consumers ([ADR 0000](0000-scope-interpretation-and-assumptions.md)).
- The two services have different dependencies (HTTP + Kafka producer vs Kafka consumer + pgx), different scaling needs and different release cadences.
- The only thing they share is the **event contract** on `incoming.user_activity`.
- A layout with both binaries in one module and `internal/domain` plus `internal/ports` is tempting, but it creates an **import cycle** if `domain` builds services from `ports` while `ports` references `domain` types. Go refuses to compile that.
- Reviewers will read the code. A predictable layout and explicit wiring matter more here than abstraction.

## Decision

### 1. Two services, two images, one repository, two independent Go modules

- `services/producer` and `services/consumer` are separate Go modules. Each has its own `go.mod`, `Dockerfile` and `.dockerignore`, and each directory is its own image build context.
- Module paths: `github.com/mmkorb/moveo-backend-challenge/services/producer` and `github.com/mmkorb/moveo-backend-challenge/services/consumer`.
- **No shared Go code.** The single shared artifact is `contracts/user_activity.v1.schema.json` ([ADR 0006](0006-event-contract-and-json-serialization.md)).
- `go.work` at the root is a **developer convenience only** (gopls, running both test suites). Images build with `GOWORK=off`, so each module must build on its own.

Root level:

```text
moveo_ai/
├── contracts/            # user_activity.v1.schema.json — the only shared artifact
├── deploy/               # kafka/create-topics.sh, postgres/init.sql, secrets/*.example
├── docs/                 # challenge_description.md, real_time_analytics_challenge_summary.md, adrs/
├── loadtest/             # k6 load & crash tests, run.sh, report/ (ADR 0016)
├── scripts/              # check-boundaries.sh
├── services/             # producer/ and consumer/ (independent Go modules)
├── .env.example
├── .gitattributes        # *.sh eol=lf
├── .gitignore
├── .golangci.yml         # shared lint config
├── compose.yaml          # kafka, kafka-init, postgres, consumer-migrate, producer, consumer
├── go.work               # dev convenience only
├── Makefile
└── README.md
```

Per service:

```text
services/producer/
├── go.mod
├── cmd/producer/main.go          # composition root (+ "healthcheck" subcommand)
├── internal/domain/              # ActivityEvent, ActivityType, validation rules (thin)
├── internal/application/         # RegisterActivity use case
├── internal/ports/               # ActivityPublisher
├── internal/adapters/httpapi/    # handlers, request/response DTOs, middleware
├── internal/adapters/kafka/      # ActivityPublisher impl + wire DTO (mirrors contracts/)
├── internal/platform/config/     # env loading and validation
├── internal/platform/telemetry/  # slog, Prometheus registry, health server
├── internal/platform/tracectx/   # W3C traceparent/tracestate parse + format (ADR 0013)
├── api/openapi.yaml
├── Dockerfile
└── .dockerignore

services/consumer/
├── go.mod
├── cmd/consumer/main.go          # composition root; subcommands: run | migrate | healthcheck
├── internal/domain/              # ActivityEvent, ActivityType, DailyUserAggregate (delta rules)
├── internal/application/         # ProcessActivity use case (batch of events → aggregate deltas)
├── internal/ports/               # AggregateRepository, DeadLetterPublisher
├── internal/adapters/kafka/      # group consumer, DLQ publisher, wire DTO
├── internal/adapters/postgres/   # AggregateRepository impl (pgx)
├── internal/platform/config/
├── internal/platform/telemetry/
├── internal/platform/tracectx/   # traceparent parser, a reduced copy of the producer's package (no shared code)
├── migrations/                   # goose SQL files, embedded with embed.FS
├── Dockerfile
└── .dockerignore
```

### 2. Hexagonal architecture inside each service

Import direction (an arrow means "is imported by"):

```text
domain  ←  ports  ←  application  ←  adapters        cmd/<svc> imports everything and wires it
```

- `domain` is a leaf: it imports only the standard library. It has no struct tags ([ADR 0004](0004-domain-model-and-ubiquitous-language.md)).
- `ports` holds interfaces that use `domain` types. Only `domain` and the stdlib may be imported there.
- `application` holds the use cases. It depends on `domain` and `ports` and never on an adapter. This fixes the old cycle: use cases live in `application`, not in `domain`.
- `adapters` translate between the outside world and the core. An adapter MUST NOT import another adapter. An inbound adapter (HTTP handler, Kafka consumer) depends on a small interface declared next to it, which the use case satisfies.
- `platform` holds cross-cutting infrastructure (config, logger, metrics registry). It never imports `domain`, `application` or `adapters`.
- **Ports exist only where a test needs a fake:** `ActivityPublisher` (producer), and `AggregateRepository` + `DeadLetterPublisher` (consumer). We add no speculative interfaces.
- Every implementation has a compile-time assertion, e.g. `var _ ports.AggregateRepository = (*postgres.AggregateRepository)(nil)`.

### 3. Manual dependency injection in `cmd/`

Constructors take their dependencies as parameters; `cmd/<svc>/main.go` is the only place that names concrete adapters. We use no DI container. Sketch of the consumer's composition root (constructor names are illustrative; real signatures are fixed during scaffolding):

```go
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/kafka"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/application"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/config"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "consumer:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := telemetry.NewLogger(os.Stdout, cfg.LogLevel)
	reg := prometheus.NewRegistry()

	pool, err := postgres.NewPool(ctx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	dlq, err := kafka.NewDeadLetterPublisher(ctx, cfg.Kafka, logger)
	if err != nil {
		return fmt.Errorf("create dlq publisher: %w", err)
	}
	defer dlq.Close()

	repo := postgres.NewAggregateRepository(pool)
	process := application.NewProcessActivity(repo, dlq)

	// CONSUMER_WORKERS group members, each a goroutine in the same consumer group (ADR 0008).
	members, err := kafka.NewGroupMembers(cfg.Kafka, cfg.ConsumerWorkers, process, logger, reg)
	if err != nil {
		return fmt.Errorf("create group members: %w", err)
	}
	health := telemetry.NewServer(cfg.HTTPAddr, reg, logger)

	g, gctx := errgroup.WithContext(ctx)
	for _, m := range members {
		g.Go(func() error { return m.Run(gctx) })
	}
	g.Go(func() error { return health.Serve(gctx) })
	return g.Wait()
}
```

This sketch shows **what** gets wired. The `migrate` and `healthcheck` subcommands are dispatched from `main` before `run`. The staged shutdown (readiness off → workers drain → commit → leave group → HTTP server → pool) is in the full skeleton in [ADR 0012](0012-lifecycle-and-graceful-shutdown.md), which is authoritative for **how** it stops.

### 4. Configuration

- `internal/platform/config` reads the environment with stdlib `os.LookupEnv` and small typed helpers (`stringVar`, `intVar`, `durationVar`, `listVar`), each with a documented default.
- Secrets support the `*_FILE` convention: when `DATABASE_PASSWORD_FILE` is set, the value is read from that file (Compose `secrets:`). See [ADR 0002](0002-security-baseline-and-threat-model.md).
- `Load()` validates everything and returns **all** problems at once via `errors.Join`. `main` fails fast with exit code 1 before opening any connection.
- `Config` is a plain struct passed down explicitly. `.env.example` lists every variable with its default.

```go
func intVar(key string, def int, errs *[]error) int {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not an integer", key, raw))
		return def
	}
	return v
}
```

#### Environment variables (single source of truth)

All names use one prefix per concern. `.env.example` mirrors this table. Defaults are for local Compose.

| Variable | Service | Default | Defined by |
|---|---|---|---|
| `LOG_LEVEL` | both | `info` | [0013](0013-observability.md) |
| `HTTP_ADDR` | both | producer `:8080`, consumer `:8081` (not published) | [0005](0005-ingestion-http-api.md), [0013](0013-observability.md) |
| `KAFKA_BROKERS` | both | `kafka:9092` | [0007](0007-kafka-client-and-topic-topology.md) |
| `KAFKA_TOPIC` | both | `incoming.user_activity` | [0007](0007-kafka-client-and-topic-topology.md) |
| `KAFKA_DLQ_TOPIC` | consumer | `incoming.user_activity.dlq` | [0011](0011-failure-handling-retry-and-dlq.md) |
| `PUBLISH_TIMEOUT` | producer | `5s` | [0005](0005-ingestion-http-api.md) |
| `CONSUMER_GROUP` | consumer | `analytics-consumer` | [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) |
| `CONSUMER_WORKERS` | consumer | `3` | [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) |
| `CONSUMER_INSTANCE_ID` | consumer | the hostname (one per replica; member `i` joins as `<id>-<i>`) | [0012](0012-lifecycle-and-graceful-shutdown.md) |
| `CONSUMER_SESSION_TIMEOUT` | consumer | `30s` (`6s`..`30m`) | [0012](0012-lifecycle-and-graceful-shutdown.md) |
| `FLUSH_INTERVAL` / `FLUSH_MAX_RECORDS` | consumer | `1s` / `500` | [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) |
| `FLUSH_TIMEOUT` | consumer | `10s` (`>= DATABASE_STATEMENT_TIMEOUT`) | [0011](0011-failure-handling-retry-and-dlq.md) |
| `REVOKE_FLUSH_TIMEOUT` | consumer | `10s` | [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) |
| `RETRY_BASE_DELAY` / `RETRY_MAX_DELAY` | consumer | `100ms` / `10s` | [0011](0011-failure-handling-retry-and-dlq.md) |
| `STARTUP_TIMEOUT` | both | `60s` | [0011](0011-failure-handling-retry-and-dlq.md) |
| `DRAIN_TIMEOUT` / `HTTP_SHUTDOWN_TIMEOUT` | consumer | `15s` / `5s` |
| `DRAIN_TIMEOUT` / `HTTP_SHUTDOWN_TIMEOUT` | producer | `5s` / `12s` (`HTTP_SHUTDOWN_TIMEOUT` must exceed `PUBLISH_TIMEOUT`) | [0012](0012-lifecycle-and-graceful-shutdown.md) |
| `DATABASE_HOST` / `DATABASE_PORT` / `DATABASE_NAME` | consumer | `postgres` / `5432` / `analytics` | [0010](0010-postgresql-access-and-migrations.md) |
| `DATABASE_USER` / `DATABASE_PASSWORD_FILE` | consumer | `analytics_app` / `/run/secrets/analytics_app_password` (`migrate` needs `DATABASE_USER=analytics_owner` and `DATABASE_PASSWORD_FILE=/run/secrets/postgres_owner_password`: Compose sets them for `consumer-migrate`, but the defaults are the runtime role, which has no DDL rights) | [0010](0010-postgresql-access-and-migrations.md), [0002](0002-security-baseline-and-threat-model.md) |
| `DATABASE_SSLMODE` | consumer | `disable` (production: `verify-full`) | [0002](0002-security-baseline-and-threat-model.md) |
| `DATABASE_MAX_CONNS` / `DATABASE_MIN_CONNS` | consumer | `8` / `1` | [0010](0010-postgresql-access-and-migrations.md) |
| `DATABASE_STATEMENT_TIMEOUT` | consumer | `5s` | [0010](0010-postgresql-access-and-migrations.md) |

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| One module, two `cmd/` binaries | Rejected | Every image would pull every dependency, and services couldn't be versioned or built independently. |
| Two separate repositories | Rejected | The challenge asks for one GitHub repo; the contract and compose file belong next to both services. |
| Shared Go module (`pkg/event`) | Rejected | It couples releases; a JSON Schema plus contract tests decouples them ([ADR 0006](0006-event-contract-and-json-serialization.md)). |
| Standard feature-package layout (one package per feature, no ports) | Rejected | It's viable, but the consumer has real rules (deltas, error classes) and two integrations, so isolating the core pays off in tests. |
| `uber-go/fx`, `uber-go/dig` (reflection DI) | Rejected | Wiring errors show up at runtime and the graph is hidden; two small graphs don't need it. |
| `google/wire` (codegen DI) | Rejected | Adds a generation step for about 10 constructors. |
| `spf13/viper`, `caarlos0/env`, `kelseyhightower/envconfig` | Rejected | Env-only config with about 15 keys fits in a 100-line stdlib package; fewer dependencies ([ADR 0001](0001-versions-and-upgrade-policy.md)). |
| `domain` + `ports` with use cases inside `domain` | Rejected | It causes the import cycle. |

## Consequences

**Positive**
- Each image contains only its service's code and dependencies, and builds from its own directory.
- The core is testable with fakes and has no I/O. The cycle is impossible by construction.
- Wiring is plain Go: easy to read, and checked by the compiler.
- A config mistake produces one complete error report at startup.

**Negative / trade-offs**
- `ActivityEvent`/`ActivityType` exist twice (once per module). Mitigation: both are small, and the contract tests against `contracts/` catch drift.
- Two `go.mod` files to keep up to date. Mitigation: both modules pin the same dependency versions, listed in [ADR 0001](0001-versions-and-upgrade-policy.md).
- `main.go` grows with each dependency. Mitigation: helper functions in `cmd/<svc>/`, never a container.

## Implementation rules

- `domain` MUST import only the standard library and MUST NOT carry struct tags.
- `ports` MUST import only the stdlib and `domain`; `application` only the stdlib, `domain` and `ports`.
- An adapter MUST NOT import another adapter; `platform` MUST NOT import `domain`, `application` or `adapters`.
- Concrete adapters MUST be constructed only in `cmd/<svc>/`. No `init()` wiring.
- Package-level mutable variables (globals, `promauto`, default registries, `http.DefaultServeMux`) MUST NOT be used.
- `log.Fatal`/`os.Exit` MUST NOT appear outside `main`; everything else returns errors.
- Every goroutine MUST be owned by an `errgroup` or `WaitGroup` and stopped by a context. No fire-and-forget `go f()`, and no `select {}` to block `main`.
- Every port implementation MUST have a `var _ ports.X = (*T)(nil)` assertion.
- Config MUST be validated in `Load()` with `errors.Join`; secrets MUST support `*_FILE`.
- Each module MUST build and test with `GOWORK=off`.
- `scripts/check-boundaries.sh` (a `go list` based check per module) MUST pass (`make boundaries`). It enforces the import rules above and the "no tags in domain" rule.

## Related

- [ADR 0004](0004-domain-model-and-ubiquitous-language.md) — what lives in `domain` and `application`.
- [ADR 0006](0006-event-contract-and-json-serialization.md) — the shared contract instead of shared code.
- [ADR 0012](0012-lifecycle-and-graceful-shutdown.md) — signal handling and shutdown order in `main`.
- [ADR 0014](0014-testing-strategy-and-quality-tooling.md) — fakes, contract tests and the boundary script.
- [ADR 0015](0015-containerization-and-local-deployment.md) — per-service Dockerfiles and build contexts.
- [ADR 0002](0002-security-baseline-and-threat-model.md) — secrets via `*_FILE`.
- [ADR 0016](0016-load-and-crash-testing.md) — what lives in `loadtest/`.

## References

- [1] Alistair Cockburn, "Hexagonal architecture" (Ports and Adapters), 2005, revised on the author's site. https://alistair.cockburn.us/hexagonal-architecture/. The ports-and-adapters structure of each service.
- [2] The Go Project, "Organizing a Go module". https://go.dev/doc/modules/layout. `cmd/` for commands and `internal/` for private packages, the layout used in each module.
- [3] The Go Project, "Go Modules Reference", sections on workspaces and `internal` directories. https://go.dev/ref/mod. `go.work` as a developer convenience with `GOWORK=off` for builds, and independent modules.
- [4] The Go Project, "Go Code Review Comments", section "Interfaces". https://go.dev/wiki/CodeReviewComments. Interfaces belong in the consuming package, as in the consumer-declared ports.
- [5] Martin Fowler, "Inversion of Control Containers and the Dependency Injection pattern", 2004. https://martinfowler.com/articles/injection.html. Constructor injection, which `cmd/<svc>/main.go` performs by hand.
- [6] Neal Ford, Mark Richards, Pramod Sadalage and Zhamak Dehghani, *Software Architecture: The Hard Parts*, O'Reilly Media, 2021, ISBN 978-1-4920-8689-5, ch. 8, "Reuse Patterns". The trade-off between code replication and a shared library, behind "no shared Go code".
- [7] The Twelve-Factor App, III. "Config". https://12factor.net/config. Configuration from environment variables, validated at startup.
- [8] The Go Project, Go 1.20 release notes (https://go.dev/doc/go1.20, `errors.Join`) and Go 1.22 release notes (https://go.dev/doc/go1.22, per-iteration loop variables), plus `errgroup` at v0.24.0 (https://pkg.go.dev/golang.org/x/sync@v0.24.0/errgroup). `Load()` returns all problems with `errors.Join`, and the composition-root sketch captures the loop variable in `g.Go`.
