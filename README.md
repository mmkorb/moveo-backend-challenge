# moveo_ai: real-time user activity analytics

An HTTP service accepts user activity events and publishes them to Kafka. A second service consumes them and keeps per-user, per-day counters in PostgreSQL. Two Go services, one repository, two Docker images.

```
client ──HTTP──▶ producer ──▶ Kafka: incoming.user_activity ──▶ consumer ──▶ PostgreSQL
                 (validate,     (6 partitions, key = user_id)     (batch,      activity_aggregates
                  202 after ack)                │                  upsert)      consumer_offsets
                                                └── invalid records ──▶ incoming.user_activity.dlq
```

**At a glance**

- `202 Accepted` is returned only after Kafka acknowledged the record (`acks=all`).
- Counts are **effectively once** per partition: the aggregates and the Kafka position are saved in the same Postgres transaction.
- Consumers scale horizontally, up to the partition count (6).
- Stack: Go 1.27, Apache Kafka 4.3.1 (KRaft), PostgreSQL 18.6, Docker Compose.

## Quick start

Requires Docker with Compose v2 (give it at least 4 GiB of memory) and free local ports `8080` and `29092`. The commands are bash; on Windows use Git Bash or WSL.

```bash
# 1. Local secrets (example values are fine)
cp deploy/secrets/postgres_owner_password.txt.example deploy/secrets/postgres_owner_password.txt
cp deploy/secrets/analytics_app_password.txt.example  deploy/secrets/analytics_app_password.txt

# 2. Start everything (first build takes a few minutes)
docker compose up -d --build --wait

# 3. Send an event -> 202 Accepted
curl -i -X POST http://127.0.0.1:8080/v1/activities \
  -H "Content-Type: application/json" \
  -d '{"user_id":"12345","activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z","metadata":{"page_url":"https://example.com/home","referrer":"https://google.com"}}'

# 4. See the result (the consumer flushes at least once per second)
docker compose exec postgres psql -U analytics_owner -d analytics \
  -c "SELECT * FROM activity_aggregates ORDER BY activity_date, user_id;"

# 5. Stop and delete the data
docker compose down -v
```

### Query the analytics

Users and `page_view` activities per day:

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

Distinct users over a date range. Don't add up the daily user counts: someone active on two days would be counted twice.

```bash
docker compose exec postgres psql -U analytics_owner -d analytics -c \
  "SELECT COUNT(DISTINCT user_id) AS unique_users,
          COALESCE(SUM(page_view_count), 0) AS page_views
   FROM activity_aggregates
   WHERE activity_date BETWEEN '2024-07-01' AND '2024-07-31';"
```

### More operations

| Task | Command |
|---|---|
| Health | `docker compose ps -a` · `curl -i http://127.0.0.1:8080/readyz` |
| Logs | `docker compose logs --tail 20 producer consumer` |
| Scale consumers | `docker compose up -d --scale consumer=3` |
| Consumer lag | `MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --describe --group analytics-consumer` |
| Stop, keep data | `docker compose down` |

The API contract is [`services/producer/api/openapi.yaml`](services/producer/api/openapi.yaml). It defines `202`, `400` (invalid event), `413` (over 64 KiB), `415` and `503` (Kafka unavailable, with `Retry-After`). All settings are environment variables, listed with their defaults in [`.env.example`](.env.example).

## Key decisions

| Topic | Decision | Why it matters | ADR |
|---|---|---|---|
| Architecture | Two Go modules, hexagonal layout, manual DI | Clear boundaries; the services deploy independently | [0003](docs/adrs/0003-system-decomposition-and-layout.md) |
| Ingestion | `net/http`; `202` only after the Kafka ack | An accepted event is never lost | [0005](docs/adrs/0005-ingestion-http-api.md) |
| Kafka | franz-go; 6 partitions keyed by `user_id`; DLQ topic | Per-user ordering and parallel consumption | [0007](docs/adrs/0007-kafka-client-and-topic-topology.md) |
| Delivery | Kafka position stored in Postgres, in the same transaction as the counters | A crash or rebalance never counts a batch twice | [0008](docs/adrs/0008-consumer-concurrency-batching-and-delivery-semantics.md) |
| Data model | One row per (UTC date, user); atomic upsert | Cheap per-day queries; exact distinct users | [0009](docs/adrs/0009-aggregation-data-model.md) |
| Failures | Permanent errors go to the DLQ; transient errors are retried with backoff | Bad input never blocks a partition | [0011](docs/adrs/0011-failure-handling-retry-and-dlq.md) |
| Scaling | Stateless producers; consumers up to the partition count | Measured bottleneck order: Kafka, producer, consumer, Postgres | [0017](docs/adrs/0017-scaling-and-capacity.md) |

All 18 decisions are in the [ADR index](docs/adrs/adr-index.md).

## Assumptions

The challenge leaves some points open. These are the choices that change results. The full list is in [ADR 0000](docs/adrs/0000-scope-interpretation-and-assumptions.md).

- **Days are UTC.** `activity_date` is the UTC date of the event's `timestamp`, not the date it was received.
- **A user counts once per day.** Across a date range, use `COUNT(DISTINCT user_id)`.
- **Unknown activity types are counted** in `activity_count`. Only `page_view` also increments `page_view_count`.
- **Metadata is validated but not stored.** It is never logged in full.
- **Client retries count twice.** The payload has no idempotency key. Redelivery inside the pipeline is not counted twice.
- **Two tables, not one.** `consumer_offsets` holds the Kafka positions, so they commit atomically with the counters ([ADR 0008a](docs/adrs/0008a-separate-offset-table-justification.md)).

## Where the core logic lives

| File | What it does |
|---|---|
| `services/producer/internal/adapters/httpapi/handler.go` | Validates the event; returns `202` after the Kafka ack |
| `services/consumer/internal/application/process_activity.go` | Folds a batch into per-user, per-day deltas; sends rejected records to the DLQ |
| `services/consumer/internal/adapters/postgres/repository.go` | `ApplyDeltas`: the upsert and the position compare-and-set, in one transaction |
| `services/consumer/internal/adapters/kafka/member.go`, `worker.go` | Group membership, one worker per partition, micro-batching, rebalance handling, retry |

## Tests

Each service is its own Go module. Run the commands from the service directory:

```bash
cd services/consumer   # or services/producer
GOWORK=off go test -count=1 ./...                          # unit tests
GOWORK=off go test -count=1 -p 1 -tags=integration ./...   # integration tests (needs Docker, uses Testcontainers)
```

To check the import boundaries, run `bash scripts/check-boundaries.sh`. The `Makefile` wraps all of these checks. Strategy: [ADR 0014](docs/adrs/0014-testing-strategy-and-quality-tooling.md).

**Load and crash tests (optional extra).** `bash loadtest/run.sh smoke` runs the stack under k6, can kill or pause a component, and ends with a PASS/FAIL report. The results (up to 3000 req/s, crash recovery, zero overcount) are in [load-test-results.md](docs/load-test-results.md). How to run them: [load-testing.md](docs/load-testing.md).

## Documentation

| Document | Read it for |
|---|---|
| [Architecture](docs/architecture.md) | Packages, layer dependencies, delivery guarantees |
| [Domain](docs/domain.md) | The event, the tables, validation rules, counters |
| [Security](docs/security.md) | Input limits, logs and personal data, secrets, DB roles |
| [ADRs](docs/adrs/adr-index.md) | The reasoning behind each decision |
| [Challenge](docs/challenge_description.md) | The original task |

## Repository layout

| Path | Contents |
|---|---|
| `services/producer/`, `services/consumer/` | The two Go services (`cmd/`, `internal/{domain,ports,application,adapters,platform}`) |
| `contracts/` | Event JSON Schema and example events |
| `deploy/` | Topic creation, DB role setup, secret templates |
| `loadtest/` | k6 scenarios, run scripts, report generator |
| `docs/` | Documentation and ADRs |
