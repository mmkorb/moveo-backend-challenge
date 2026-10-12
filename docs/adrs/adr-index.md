# Architecture Decision Records — Index

Decisions for the **Real-Time Analytics** challenge: two Go services that live in one repository and ship as separate images. The **producer** takes HTTP requests and publishes them to Kafka. The **consumer** reads from Kafka and writes aggregates to PostgreSQL.

- Challenge: [`../challenge_description.md`](../challenge_description.md)
- Our summary of the challenge. It records our reading of ambiguous points; the challenge text wins where it is explicit: [`../real_time_analytics_challenge_summary.md`](../real_time_analytics_challenge_summary.md)
- How to run the system: [`../../README.md`](../../README.md). How to run and read the load and crash tests: [`../load-testing.md`](../load-testing.md). Measured results: [`../load-test-results.md`](../load-test-results.md).

## Records

| # | Title | Scope | Status | Decision in one line |
|---|---|---|---|---|
| [0000](0000-scope-interpretation-and-assumptions.md) | Scope, interpretation and assumptions | both | Accepted | Build a correct, reproducible end-to-end pipeline. Record every interpretation as an assumption. |
| [0001](0001-versions-and-upgrade-policy.md) | Versions and upgrade policy | both | Accepted | Newest release line still under upstream support. Libraries must be at least 14 days old. Everything pinned. |
| [0002](0002-security-baseline-and-threat-model.md) | Security baseline and threat model | both | Accepted | Input limits, no PII in logs, least-privilege DB role, secrets via files, hardened containers, supply-chain checks. |
| [0003](0003-system-decomposition-and-layout.md) | System decomposition, repository layout, hexagonal architecture, manual DI and configuration | both | Accepted | One repo with two independent Go modules (`services/producer`, `services/consumer`). Hexagonal layout inside each, wiring only in `cmd/`, configuration from env. |
| [0004](0004-domain-model-and-ubiquitous-language.md) | Domain model and ubiquitous language (lightweight tactical DDD) | both | Accepted | Explicit domain types and use cases (`RegisterActivity`, `ProcessActivity`). No strategic DDD, no DDD Aggregates. |
| [0005](0005-ingestion-http-api.md) | Ingestion HTTP API on `net/http` | producer | Accepted | `POST /v1/activities` returns 202 only after the Kafka ack. Plain `ServeMux`; no router framework. |
| [0006](0006-event-contract-and-json-serialization.md) | Event contract and JSON serialization | both | Accepted | JSON payload as in the challenge, versioned by a JSON Schema in `contracts/`, encoded with stdlib `encoding/json`. `event-id` header is for tracing only. |
| [0007](0007-kafka-client-and-topic-topology.md) | Kafka client and topic topology | both | Accepted | franz-go. `incoming.user_activity` has 6 partitions and is keyed by `user_id`, plus a DLQ topic. Retention is explicit and auto-create is off. |
| [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) | Consumer concurrency, batching and delivery semantics | consumer | Accepted (amended 2026-10-12) | N group members as goroutines, one worker per partition, micro-batched upserts. Option A: the Kafka position is stored in PostgreSQL in the same transaction as the upsert (compare-and-set), so delivery is effectively once per partition. A fenced partition is resumed in place from the stored position. |
| [0008a](0008a-separate-offset-table-justification.md) | Justification for a separate offset tracking table | consumer | Accepted | Store positions in `consumer_offsets` (option A), not as columns of `activity_aggregates` (option B): constant-time recovery, no metadata bloat, domain isolation, simple topology changes. |
| [0009](0009-aggregation-data-model.md) | Aggregation data model | consumer | Accepted (amended 2026-10-12) | One table, `activity_aggregates`, with one row per (UTC date, user). Counters are incremented atomically by an upsert. Kafka positions live in a separate 6-row table, `consumer_offsets`. |
| [0010](0010-postgresql-access-and-migrations.md) | PostgreSQL access and migrations | consumer | Accepted (amended 2026-10-12) | pgx/v5 + `pgxpool` with hand-written SQL. goose runs as the `consumer migrate` one-shot job (`00001` aggregates, `00002` `consumer_offsets`). The runtime role has DML rights only. One transaction per flush. |
| [0011](0011-failure-handling-retry-and-dlq.md) | Failure handling: classification, retry, DLQ | both | Accepted (amended 2026-10-12) | Permanent errors go to the DLQ. Transient errors get context-aware backoff and are never sent to the DLQ. Retry happens in one layer only; each flush attempt has a deadline (`FLUSH_TIMEOUT`). |
| [0012](0012-lifecycle-and-graceful-shutdown.md) | Lifecycle and graceful shutdown | both | Accepted (amended 2026-10-12) | `signal.NotifyContext` + `errgroup`. Teardown happens in stages within a 20 s budget; compose waits 30 s before killing. Consumers are static group members (fast restart) and leave explicitly on a clean stop. |
| [0013](0013-observability.md) | Observability: slog, minimal Prometheus, health | both | Accepted (amended 2026-10-12) | JSON `slog`, a Prometheus registry injected at startup, and health/readiness endpoints. No monitoring stack. `offset_fence_rejections_total` counts fenced batches, `offset_fence_resyncs_total` their in-place recovery. |
| [0014](0014-testing-strategy-and-quality-tooling.md) | Testing strategy and quality tooling | both | Accepted | stdlib `testing` + `go-cmp` + fakes, `testcontainers-go` for integration tests, `goleak`, golangci-lint v2 and `govulncheck`. |
| [0015](0015-containerization-and-local-deployment.md) | Containerization: Dockerfiles, Compose, Kafka and Postgres containers | both | Accepted | Multi-stage distroless images. `compose.yaml` with Kafka (KRaft), topic init, Postgres 18, the migrate job and both services. |
| [0016](0016-load-and-crash-testing.md) | Load and crash testing with k6 and an end-of-run report | both | Accepted (amended 2026-10-12) | k6 in an isolated Compose project (`moveo-loadtest`) with hard limits on every container. Scenarios from smoke to crash; every run ends with a PASS/FAIL report. Zero overcount in every scenario, and DB = input topic − DLQ. No monitoring stack. |
| [0017](0017-scaling-and-capacity.md) | Scaling and capacity | both | Accepted | Compose stays local-only. Producers scale as stateless replicas, consumers up to the partition count (lag-driven), Kafka with brokers, RF 3 and reassignment, PostgreSQL as one writer with bounded connections. Measured bottleneck order: Kafka, producer, consumer, PostgreSQL ([results](../load-test-results.md)). |

## Numbering

The numbers follow the order in which the decisions build on each other: scope, then stack/versions and security constraints, then decomposition, domain and the entry API, then the event contract and Kafka topology, then delivery semantics (they need the topology and drive the data model), then the data model and PostgreSQL access, then failure handling/DLQ and lifecycle, and last the cross-cutting concerns (observability, testing, containers, load tests). An addendum to an ADR uses the `NNNNa-` file-name convention.

## Conventions

- **When to write an ADR:** for any decision that is expensive to reverse, or that a reviewer would ask "why?" about. Examples: a library choice, a data model, delivery semantics, an interface contract or a deployment shape.
- **File name:** `NNNN-kebab-case-title.md`. The number is sequential and never reused. An addendum to ADR `NNNN` is named `NNNNa-kebab-case-title.md` (for example `0008a-separate-offset-table-justification.md`).
- **Status lifecycle:** `Proposed` → `Accepted` → (`Deprecated` | `Superseded by NNNN`). An accepted ADR is not rewritten when its decision changes: add a new ADR that supersedes the old one, and update this index. Small amendments (a correction, a clarification, a refinement that keeps the decision) are made in place and noted in the ADR's header (`Amended: <date> — <what>`).
- **Exact versions** appear only in [0001](0001-versions-and-upgrade-policy.md). Other ADRs name a component and link to 0001.
- **Security rules** live in [0002](0002-security-baseline-and-threat-model.md). Other ADRs link to it instead of repeating it.
- **Vocabulary:** use the terms from [0004](0004-domain-model-and-ubiquitous-language.md) (`ActivityEvent`, `DailyUserAggregate`, `ProcessActivity`, …) consistently in ADRs, code and tests.

## Template

```markdown
# ADR NNNN: <Title>

- **Status:** Proposed | Accepted | Deprecated | Superseded by NNNN
- **Date:** YYYY-MM-DD
- **Scope:** producer | consumer | both

## Context
What forces are at play (requirements, constraints, the challenge text). Facts, not opinions.

## Decision
What we do, stated in the active voice ("We use …").

## Alternatives considered
| Option | Verdict | Reason |
|---|---|---|

## Consequences
**Positive**
- …

**Negative / trade-offs**
- … (and how each is mitigated)

## Implementation rules
Concrete, checkable rules for code and review ("MUST" / "MUST NOT").

## Related
- ADR NNNN — why it is related
```

## References

Each ADR ends with a numbered `## References` section listing its sources: specifications and official documentation first (at the versions pinned in [0001](0001-versions-and-upgrade-policy.md)), then books and articles.

- [1] Michael Nygard, "Documenting Architecture Decisions", Cognitect blog, 15 November 2011. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions. The record format (title, status, context, decision, consequences) that the template above extends.
