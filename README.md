# moveo_ai: real-time user activity analytics

Two Go services in one repository, shipped as two Docker images.

## Reviewer guide

**10-minute path.**

1. Create the two secret files and run `docker compose up -d --build --wait` ([Run the system](#run-the-system), steps 1-2; the first build takes a few minutes).
2. Send the example event with `curl` (step 4), then send a few more with other `user_id` and `timestamp` values.
3. Run the per-day and date-range queries in step 5. They answer what the challenge asks: users and `page_view` activities per day.
4. Stop with `docker compose down -v` (step 7).

**Files that carry the core logic.**

- `services/producer/internal/adapters/httpapi/handler.go`: validation and the `202` only after the Kafka acknowledgement.
- `services/consumer/internal/application/process_activity.go`: folds a batch of events into per-user, per-UTC-day deltas and sends rejected records to the DLQ.
- `services/consumer/internal/adapters/postgres/repository.go`: `ApplyDeltas`: the upsert and the position compare-and-set in one transaction.
- `services/consumer/internal/adapters/kafka/member.go` and `worker.go`: group member, one worker per partition, micro-batching, rebalance handling, retry.
- [ADR 0008](docs/adrs/0008-consumer-concurrency-batching-and-delivery-semantics.md): the delivery semantics those files implement.

**Optional extras beyond the challenge:** the k6 load and crash harness (`loadtest/`, results in [docs/load-test-results.md](docs/load-test-results.md)), W3C trace-context propagation (`tracectx`), and SBOM and provenance attestations for the images (`docker buildx build --sbom=true --provenance=mode=max`, see [ADR 0002](docs/adrs/0002-security-baseline-and-threat-model.md)).

- **producer** (`services/producer`): `POST /v1/activities` validates an event and publishes it to the Kafka topic `incoming.user_activity` (routes in `internal/adapters/httpapi/router.go`).
- **consumer** (`services/consumer`): reads that topic and keeps per-user, per-UTC-day counters in the PostgreSQL table `activity_aggregates`, plus its Kafka position per partition in `consumer_offsets`. Invalid records go to `incoming.user_activity.dlq`.

## Stack

- Go 1.27 (`go 1.27.0`, toolchain `go1.27.2`): two independent modules plus the `loadtest/report` module.
- Apache Kafka 4.3.1 (KRaft, one broker), PostgreSQL 18.6, Docker Compose (`compose.yaml`). Versions: [ADR 0001](docs/adrs/0001-versions-and-upgrade-policy.md).

## Documentation

### Where to find what

- [docs/architecture.md](docs/architecture.md): packages and layer dependencies, delivery semantics, external integrations.
- [docs/domain.md](docs/domain.md): business rules, the event, the tables, validations and counters.
- [docs/security.md](docs/security.md): input validation, logs, personal data, secrets, DB roles and ports.
- [docs/load-testing.md](docs/load-testing.md): running the load and crash tests and reading their report.
- [docs/adrs/adr-index.md](docs/adrs/adr-index.md): the "why" behind each decision.
- [docs/real_time_analytics_challenge_summary.md](docs/real_time_analytics_challenge_summary.md): our summary of the challenge. It records our reading of ambiguous points; the [challenge text](docs/challenge_description.md) wins where it is explicit.

## Run the system

### Prerequisites

- Docker with Compose v2 (tested on Docker Desktop). The stack is capped at about 5 CPUs and 2 GiB of memory (limits in [`compose.yaml`](compose.yaml)); give Docker at least 4 GiB.
- Free host ports `8080` (producer) and `29092` (Kafka, for host tools), both bound to `127.0.0.1`.
- The commands are bash. On Windows use Git Bash or WSL; `curl` ships with Windows 11.

### 1. Create the local secrets

The Postgres passwords are Compose secrets read from files. The files are gitignored; the example values are fine for a local run.

```bash
cp deploy/secrets/postgres_owner_password.txt.example deploy/secrets/postgres_owner_password.txt
cp deploy/secrets/analytics_app_password.txt.example  deploy/secrets/analytics_app_password.txt
```

### 2. Start the ecosystem

```bash
docker compose up -d --build --wait
```

This builds the two images and starts Kafka (KRaft), `kafka-init` (topic and DLQ), Postgres 18, `consumer-migrate` (tables and grants), the producer and the consumer, in dependency order. `--wait` returns when every long-running service is healthy; the first build takes a few minutes.

### 3. Check that it is healthy

```bash
docker compose ps -a                      # kafka, postgres, producer, consumer: healthy; kafka-init and consumer-migrate: Exited (0)
curl -i http://127.0.0.1:8080/readyz      # 200 when the producer can reach Kafka
docker compose logs --tail 20 producer consumer
```

The consumer serves `/healthz`, `/readyz` and `/metrics` on port 8081 inside the Compose network only; it is not published.

### 4. Send an event

```bash
curl -i -X POST http://127.0.0.1:8080/v1/activities \
  -H "Content-Type: application/json" \
  -d '{"user_id":"12345","activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z","metadata":{"page_url":"https://example.com/home","referrer":"https://google.com"}}'
```

The answer is `202 Accepted`, sent only after Kafka acknowledged the record. The API contract is [`services/producer/api/openapi.yaml`](services/producer/api/openapi.yaml): 202, 400 (invalid event), 413 (over 64 KiB), 415, 503 (Kafka unavailable, with `Retry-After`). Probes: `/healthz`, `/readyz`, `/metrics`.

### 5. Inspect the aggregates

The consumer flushes at least once per second.

```bash
docker compose exec postgres psql -U analytics_owner -d analytics \
  -c "SELECT * FROM activity_aggregates ORDER BY activity_date, user_id;"
```

After step 4 this shows `activity_date = 2024-07-01`, `user_id = 12345`, `activity_count = 1`, `page_view_count = 1`. Sending the same event again increments both counters: events are counted, not deduplicated.

Per day, the number of users and the number of `page_view` activities (`unique_users` is valid per day only):

```bash
docker compose exec postgres psql -U analytics_owner -d analytics -c \
  "SELECT activity_date,
          COUNT(*)             AS unique_users,
          SUM(page_view_count) AS page_views,
          SUM(activity_count)  AS activity_volume
   FROM activity_aggregates
   GROUP BY activity_date
   ORDER BY activity_date;"
```

Distinct users over a date range (never add up the daily user counts: a user active on two days would be counted twice):

```bash
docker compose exec postgres psql -U analytics_owner -d analytics -c \
  "SELECT COUNT(DISTINCT user_id) AS unique_users,
          COALESCE(SUM(page_view_count), 0) AS page_views
   FROM activity_aggregates
   WHERE activity_date BETWEEN '2024-07-01' AND '2024-07-31';"
```

To see the consumer's stored Kafka positions and its lag:

```bash
docker compose exec postgres psql -U analytics_owner -d analytics -c "SELECT * FROM consumer_offsets;"
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 --describe --group analytics-consumer
```

In Git Bash, prefix the `docker compose exec kafka /opt/kafka/...` command with `MSYS_NO_PATHCONV=1`, otherwise Git Bash rewrites the container path.

### 6. Scale the consumers

```bash
docker compose up -d --scale consumer=3
```

Every replica joins the group `analytics-consumer` and runs `CONSUMER_WORKERS` group members as goroutines (default 3). The topic has 6 partitions, so at most 6 members own work in total; the rest are idle standbys ([ADR 0007](docs/adrs/0007-kafka-client-and-topic-topology.md), [ADR 0008](docs/adrs/0008-consumer-concurrency-batching-and-delivery-semantics.md)).

### 7. Stop and clean up

```bash
docker compose down        # stop and remove the containers, keep the data
docker compose down -v     # also delete the Kafka and Postgres volumes (needed after editing deploy/postgres/init.sql)
```

### Configuration

All settings are environment variables with defaults, listed in [`.env.example`](.env.example) (specification: [ADR 0003](docs/adrs/0003-system-decomposition-and-layout.md)). Passwords are never env values. Compose substitutes only `LOG_LEVEL` and `CONSUMER_WORKERS` from the environment or `.env`; edit the service's `environment:` block in [`compose.yaml`](compose.yaml) to change another.

## Tests

Each service is its own Go module: run Go commands from the service directory with `GOWORK=off`. Examples for the consumer (use `services/producer` for the other):

```bash
cd services/consumer
GOWORK=off go test -count=1 ./...                          # unit tests, no Docker
GOWORK=off go test -count=1 -p 1 -tags=integration ./...   # needs Docker: Testcontainers starts Kafka and Postgres
```

Import rules: `bash scripts/check-boundaries.sh`. Contract examples in `contracts/examples/` are checked by both services. The `Makefile` wraps these checks (needs `make`); strategy in [ADR 0014](docs/adrs/0014-testing-strategy-and-quality-tooling.md).

## Load and crash tests

`bash loadtest/run.sh <scenario>` runs the whole stack in its own Compose project (`moveo-loadtest`, about 6 CPUs and 3.7 GiB; Docker needs at least 8 GiB), drives it with k6, optionally kills or pauses a component, and ends with a PASS/FAIL report.

```bash
bash loadtest/run.sh smoke                  # 1-3 min: the database must equal the accepted events exactly
bash loadtest/run-suite.sh [--soak]         # every scenario in a row (about 1 h; --soak adds 34 min)
```

Reports land in `loadtest/results/<RUN_ID>-<scenario>/` (start with `report.md`) and in `temp/`, both gitignored. Exit code: 0 PASS, 1 FAIL, 2 harness error. Scenarios, options and how to read the report: [docs/load-testing.md](docs/load-testing.md); design: [ADR 0016](docs/adrs/0016-load-and-crash-testing.md).

Measured results (throughput up to 3000 req/s, bottleneck per component, crash recovery, effectively once, soak): [docs/load-test-results.md](docs/load-test-results.md); scaling decision: [ADR 0017](docs/adrs/0017-scaling-and-capacity.md).

## Assumptions

The challenge leaves several points open. The full list, with the reasoning, is in [ADR 0000](docs/adrs/0000-scope-interpretation-and-assumptions.md).

- **UTC day boundaries.** `activity_date` is the UTC calendar date of the event's `timestamp`, not the date it was received. An offset such as `+02:00` is converted to UTC first.
- **Users per day.** A user is counted once per day: one row per `(activity_date, user_id)`. Users over a date range are `COUNT(DISTINCT user_id)`, never a sum of daily counts ([ADR 0009](docs/adrs/0009-aggregation-data-model.md)).
- **Unknown activity types** are accepted and counted in `activity_count`; only `page_view` also increments `page_view_count`.
- **Metadata is not stored.** It is validated for size and passed through Kafka unchanged, but not aggregated or kept in PostgreSQL, and never logged in full.
- **Client retries are counted twice.** If a client repeats `POST /v1/activities` after a timeout, both requests become records. The payload has no idempotency key, so the system cannot tell them apart. Redelivery inside the consumer is handled and not counted twice.
- **A second table, `consumer_offsets`,** holds the consumer's Kafka position per partition (at most 6 rows, one per partition that has received events). This deviates from the challenge's "only one table is needed/recommended" on purpose: the position is stored in the same transaction as the aggregates, so a crash cannot count a batch twice. Justification: [ADR 0008a](docs/adrs/0008a-separate-offset-table-justification.md).

## Delivery guarantees

**202 means durable** (`acks=all`). The consumer is **effectively once per partition**: aggregates and the Kafka position commit in one PostgreSQL transaction. **Not covered**: a client retry of `POST /v1/activities` after a timeout counts twice. Details, DLQ and retry rules: [docs/architecture.md](docs/architecture.md#delivery-guarantees).

## Repository layout

| Path | Contents |
|---|---|
| `services/producer/`, `services/consumer/` | Go modules, hexagonal: `cmd/`, `internal/{domain,ports,application,adapters,platform}`; consumer also `migrations/` (goose) |
| `contracts/`, `deploy/` | event JSON Schema and golden examples; topic creation, DB role init, secret templates |
| `loadtest/` | k6 scenarios, `run.sh`, Compose overlays, report generator (own Go module) |
| `docs/` | the documents above; ADRs in `docs/adrs/` |
| `compose.yaml`, `.env.example`, `Makefile`, `scripts/` | local stack, settings, repo tooling |
