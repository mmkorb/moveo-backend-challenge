# Real-Time Analytics Microservice Challenge — Design Summary

## 1. Goal and scope

Implement a real-time analytics pipeline in Go that receives user activity events over HTTP, publishes them to Kafka, consumes them asynchronously, calculates aggregates, and stores those aggregates in PostgreSQL.

Kafka is an explicit requirement of this challenge. This implementation should demonstrate that the pipeline works reliably; it is not a claim that Kafka would be the best choice for every production application.

## 2. Proposed architecture

1. **Producer service (Go):** exposes a REST endpoint, validates incoming activity events, and publishes them to Kafka.
2. **Kafka:** transports events through the `incoming.user_activity` topic. Configure retention and broker settings explicitly.
3. **Consumer service (Go):** reads events, updates aggregate counters, and persists the results in PostgreSQL.
4. **PostgreSQL:** stores the aggregate results rather than every raw event.

Use separate Docker images for the producer and consumer, with one container per service in a `docker-compose.yml`. Compose should also start Kafka and PostgreSQL and configure connectivity and environment variables.

The challenge wording, “Create a Docker image for both the producer and consumer,” is grammatically ambiguous about the number of images. Two images are the clearest implementation choice because the producer and consumer are distinct microservices and the bonus section discusses scaling consumers.

## 3. Aggregation model

Use **one aggregate row per user per UTC calendar day**. This supports daily analytics while keeping the table smaller than a raw event table.

Proposed schema:

```sql
CREATE TABLE activity_aggregates (
    activity_date   DATE NOT NULL,
    user_id         TEXT NOT NULL,
    activity_count  BIGINT NOT NULL DEFAULT 0,
    page_view_count BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (activity_date, user_id),
    CHECK (activity_count >= 0),
    CHECK (page_view_count >= 0),
    CHECK (page_view_count <= activity_count)
);

CREATE INDEX idx_activity_aggregates_user
    ON activity_aggregates (user_id);
```

- `activity_date`: the UTC date derived from the event timestamp.
- `user_id`: the user associated with the activity.
- `activity_count`: number of events attributed to this user on that date.
- `page_view_count`: number of `page_view` events attributed to this user on that date.
- `updated_at`: time when the database row was most recently inserted or updated; it is not the event timestamp.

The composite primary key enforces one row per `(activity_date, user_id)` and creates an index beginning with `activity_date`. Therefore, do not add a separate `activity_date` index initially. The additional `user_id` index supports queries for a user across dates. Add indexes only when supported by actual query patterns and query plans.

## 4. UPSERT behavior

For each consumed event, derive its UTC date and set the increments:

- `activity_count`: increment by 1 for every accepted event.
- `page_view_count`: increment by 1 for `page_view`; otherwise increment by 0.
- `updated_at`: set to the current database time on both inserts and conflict updates.

Use `INSERT ... ON CONFLICT (activity_date, user_id) DO UPDATE` to update an existing aggregate row or create one if it does not exist. The `updated_at` field must be set explicitly in the conflict-update clause; its `DEFAULT` only applies when a row is inserted.

## 5. Metrics and semantics

At minimum, make it possible to derive:

- **Page views:** `SUM(page_view_count)` over the requested date range.
- **Unique users:** `COUNT(DISTINCT user_id)` over the requested date range.
- **Activity volume:** `SUM(activity_count)` over the requested date range.

Do not sum daily unique-user counts to calculate unique users across multiple days: a user may appear on more than one day. Query `COUNT(DISTINCT user_id)` over the full requested range instead.

The challenge does not specify time-zone semantics, so use UTC consistently and document the choice.

## 6. Raw events, logs, and duplicate delivery

- Do not store every raw event in PostgreSQL for the initial solution; the stated deliverable is to persist aggregate results in one table.
- Use structured application logs for operational events such as publish/consume failures, retries, database errors, processing latency, and shutdown. Avoid logging full event payloads unnecessarily, since metadata may contain sensitive information.
- Kafka retention provides a configurable opportunity to consume or replay retained events; it should not be treated as permanent archival storage unless explicitly configured and designed for that purpose.
- An aggregate UPSERT alone is **not idempotent**. If the same event is processed twice, counters can be incremented twice. The sample challenge payload does not include an event ID. Document this limitation and the delivery semantics; if deduplication is required, introduce a stable event identifier and design a deduplication strategy. Strict deduplication may require state or storage beyond the simple aggregate table.

## 7. Indexing and partitioning

Start with a regular PostgreSQL table and the indexes described above. Indexes are a sensible first step; partitioning is a separate physical data-management decision and does not automatically improve every query.

Do **not** introduce daily or multi-level partitioning pre-emptively. Reconsider partitioning only if measured data volume, query performance, or retention/deletion requirements justify its operational cost. The aggregation grain (daily per user) does not dictate the physical partitioning strategy.

## 8. Reliability and testing

Implement and test the core end-to-end path first:

1. Submit a valid activity through the producer REST endpoint.
2. Verify that the producer publishes to `incoming.user_activity`.
3. Verify that the consumer reads the event and updates the PostgreSQL aggregate.
4. Check that repeated users, different activity types, and date boundaries produce the expected counters.
5. Test invalid requests, Kafka/PostgreSQL connection failures, retry behavior, and service shutdown.

For the bonus requirements, support multiple consumer instances where practical, define a failure/retry strategy, and shut down gracefully on `SIGTERM` and `SIGINT`. Ensure offset-commit behavior is aligned with successful processing so failures do not silently lose work.

Observability tooling such as Prometheus and Grafana is not specified as mandatory. Structured logs and basic metrics are a useful starting point; add a full monitoring stack only if it contributes clear value to the challenge demonstration.

## 9. Delivery recommendation

Provide a repository with separate producer and consumer services, individual Dockerfiles, a `docker-compose.yml`, database schema/migrations, automated tests, an `.env.example`, and a README explaining how to start the environment, submit a test event, and inspect aggregate results.

Prioritize a correct, reproducible end-to-end pipeline and clearly documented assumptions over adding extra infrastructure that the challenge does not require.
