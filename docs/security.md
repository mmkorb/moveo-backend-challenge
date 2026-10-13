---
description: Data classification, trust boundaries, authn/authz, secrets, logging rules, data egress and known weak points.
last_verified: 2026-10-12
---

# Security

> **TL;DR** — Lists data classification, trust boundaries, auth, secrets, logging rules and egress as the code implements them today for a local deployment. Key takeaway: there is no authentication, `user_id` and `metadata` are never logged, and the DLQ is as sensitive as the main topic.

Scope: a local or evaluation deployment on one machine. The threat model, mitigations and the production gap are in [ADR 0002](adrs/0002-security-baseline-and-threat-model.md); this file lists what the code does today and the rules to keep.

## Data classification

| Data | Class | Where it lives | Rules |
|---|---|---|---|
| `user_id` | personal data (LGPD/GDPR, per ADR 0002) | `ActivityEvent.UserID`; Kafka record key and value; `activity_aggregates.user_id` | Never log it: no code path does today. Never put it in a metric label, an error response or a DLQ header |
| `metadata` (page URL, referrer, any string) | potentially sensitive | `ActivityEvent.Metadata`; Kafka record value only; not in Postgres | Never log it, even partly. Never persist it outside Kafka |
| `activity_type`, `timestamp`, counters | internal | Kafka value; `activity_aggregates` | No restriction |
| Original record bytes in the DLQ | as sensitive as the main topic | topic `incoming.user_activity.dlq` (14 days retention, `deploy/kafka/create-topics.sh`) | Treat the DLQ like the main topic. Headers the DLQ publisher adds carry the error class and original coordinates, never error text; original headers are copied unchanged |
| Database passwords | credential | files in `deploy/secrets/` mounted as Compose secrets | Never in env values, `ARG`, logs, errors or git |
| `event-id`, `request_id`, `trace_id` | internal identifiers | headers, logs | Safe to log. `loggableEventID` in `services/consumer/internal/adapters/kafka/message.go` sanitizes the header before it is logged |

## Trust boundaries

| Input | Validated in | Notes |
|---|---|---|
| Public HTTP body | `decodeActivity` in `services/producer/internal/adapters/httpapi/decode.go`, then `NewActivityEvent` in `services/producer/internal/domain/activity.go` | `Content-Type: application/json`, body up to 64 KiB, one JSON object, unknown fields rejected. Violations return fixed texts, never decoder output |
| HTTP headers `X-Request-ID`, `traceparent`, `tracestate` | `validRequestID` in `middleware.go`; `Start` in `services/producer/internal/platform/tracectx/tracectx.go` | Invalid values are replaced. `tracestate` is limited to 512 printable ASCII bytes and copied to Kafka headers |
| Kafka record | `decodeRecord` in `services/consumer/internal/adapters/kafka/message.go`, then `NewActivityEvent` in `services/consumer/internal/domain/activity.go` | Anything that can write to the topic is untrusted. Failures become dead letters, never a crash |
| Kafka record header `schema-version` | `decodeRecord` | A value other than `1` is dead-lettered as `decode` |
| Environment and secret files | `Load` in each `internal/platform/config/config.go` | Invalid configuration stops the process at start |
| SQL | `repository.go` | Static statements with `$n` parameters; no string-built SQL |

## Authentication and authorization

- Authentication: none. `POST /v1/activities` accepts any caller (`router.go`). The port is published on `127.0.0.1` only (`compose.yaml`).
- Authorization: none. The `user_id` in the body is stored as sent; nothing binds it to a caller.
- `GET /metrics` on the producer shares the API listener and needs no credentials. The consumer serves `/healthz`, `/readyz`, `/metrics` on 8081, not published.
- Outbound to Postgres: user and password from `DATABASE_USER` and `DATABASE_PASSWORD_FILE`, `sslmode` from `DATABASE_SSLMODE` (`disable` in Compose). Config in `services/consumer/internal/platform/config/config.go`.
- Outbound to Kafka: PLAINTEXT, no SASL, no ACLs (`compose.yaml`). The host listener `29092` is bound to `127.0.0.1`.
- Database roles: `analytics_owner` (Compose bootstrap user, superuser locally) runs `consumer migrate`; `analytics_app` has `SELECT`, `INSERT`, `UPDATE` on `activity_aggregates` and `consumer_offsets`, with no `DELETE`, `TRUNCATE` or DDL (`deploy/postgres/init.sql`, migrations). Never connect the running consumer as the owner.
- Never use `user_id` from the request body as proof of identity: it is data supplied by the caller.

## Secrets

| Secret (name) | Source | Used in |
|---|---|---|
| `postgres_owner_password` | Compose secret from `deploy/secrets/postgres_owner_password.txt` | `postgres` (`POSTGRES_PASSWORD_FILE`), `consumer-migrate` (`DATABASE_PASSWORD_FILE`) |
| `analytics_app_password` | Compose secret from `deploy/secrets/analytics_app_password.txt` | `consumer` (`DATABASE_PASSWORD_FILE`); `deploy/postgres/init.sql` creates the role with it |

- The `*.txt` files and `.env` are gitignored (`.gitignore`); only the `*.example` files are tracked. Do not commit real values.
- `Secret` in `services/consumer/internal/platform/config/config.go` redacts itself in `String`, `GoString`, `LogValue` and `MarshalText`. Keep passwords in that type. `postgres.Config` prints without its password.
- The DSN is built with `url.UserPassword` (`connString` in `services/consumer/internal/adapters/postgres/pool.go`), never by string concatenation.

## Logging and observability

- Never log: request bodies, Kafka record values, `metadata`, `user_id`, HTTP headers, passwords, or error text that may echo input.
- The access log (`Logging` in `services/producer/internal/adapters/httpapi/middleware.go`) records method, route pattern, status and duration only.
- The consumer logs topic, partition, offset, error class, `event_id` and `trace_id`, and logs Kafka errors by type (`error_type`), not by message.
- Logs are JSON through `log/slog` (`NewLogger` in each `internal/platform/telemetry/`). There is no automatic masking beyond `Secret`; the rules above are the control.
- Metric labels carry result, dependency, error class and partition, never user data.
- Error responses are RFC 9457 problem documents with fixed texts (`problem.go`): no Go errors, SQL or addresses.

## Data egress

| External destination | Data sent | Client in code |
|---|---|---|
| Kafka topic `incoming.user_activity` | full event: `user_id`, `activity_type`, `timestamp`, `metadata` | `Publisher` in `services/producer/internal/adapters/kafka/publisher.go` |
| Kafka topic `incoming.user_activity.dlq` | original record key, value and headers | `DeadLetterPublisher` in `services/consumer/internal/adapters/kafka/dlq.go` |
| PostgreSQL | `user_id`, UTC date, counters, stored positions | `AggregateRepository` in `services/consumer/internal/adapters/postgres/repository.go` |

No data leaves the Compose network other than through these three. There are no third-party calls.

## Points of attention

- Containers: application images are distroless and non-root; the app services run `read_only` with `cap_drop: [ALL]` and `no-new-privileges` (`x-app` in `compose.yaml`). Kafka and Postgres do not set `read_only` or `cap_drop`.
- Base images are pinned by tag, not by digest ([ADR 0002](adrs/0002-security-baseline-and-threat-model.md), "Deliberately not done").
- `activity_aggregates` has no retention or erasure path; `user_id` stays until someone deletes rows (ADR 0002, "Data protection and PII").
- Client-supplied `X-Request-ID`, `traceparent` and `tracestate` reach logs and Kafka headers after format checks only.
- Anything with access to the Kafka network can read every event, including `metadata`, and the DLQ.
