# ADR 0000: Scope, interpretation and assumptions

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** both

> **TL;DR** — Builds the producer, consumer, two images and Compose first, then the bonus items, and lists what is out of scope. Fixes 12 assumptions: UTC dates, one row per user per day, unknown activity types accepted, `202` means durable in Kafka. Main trade-off: a second 6-row table, `consumer_offsets`, deviates from the challenge's "one table".

## Context

The challenge ([`../challenge_description.md`](../challenge_description.md)) is short and leaves several points open. It invites us to fill the gaps ourselves:

> "If you encounter any doubts or questions, we encourage you to make assumptions, use your best judgment, and proceed."

> "Remember, more isn't always better focus on quality over quantity. Feel free to bend or even break the rules if you believe it leads to a better solution, but make sure you can justify your decision and explain your thought process."

Our interpretation is written down in [`../real_time_analytics_challenge_summary.md`](../real_time_analytics_challenge_summary.md). The summary records our reading of the points the challenge leaves ambiguous (scope and data model among them); where the challenge text is explicit, the challenge text wins. This ADR lists what we build, what we leave out, and every assumption a reviewer would otherwise have to guess.

What the challenge text actually requires:

| Requirement (quoted) | Our reading |
|---|---|
| "Implement a Kafka producer that exposes a REST endpoint and publishes messages to the `incoming.user_activity` topic." | Producer service: `POST /v1/activities` → Kafka ([ADR 0005](0005-ingestion-http-api.md)). |
| "Implement a Kafka consumer that reads messages from the `incoming.user_activity` topic." | Consumer service in a consumer group ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). |
| "counting the number of users and the number of `page_view` activities" | Per-user daily counters; unique users and page views come from SQL ([ADR 0009](0009-aggregation-data-model.md)). |
| "Write aggregated results to a PostgreSQL database. Only one table is needed/recommended." | One analytics table, `activity_aggregates`. A second, 6-row control table, `consumer_offsets`, stores the Kafka positions so a crash cannot count a batch twice: a deliberate deviation from "one table", justified in [ADR 0008a](0008a-separate-offset-table-justification.md). |
| "Create a Docker image for both the producer and consumer." | Two images, one per service ([ADR 0015](0015-containerization-and-local-deployment.md)). |
| "Detailed instructions for deploying the service, including how to run the Docker containers." | README with one-command start, a sample `curl`, and SQL to inspect the results. |

## Decision

We split the work into three tiers and build them in this order. The first tier has to work end to end before anything else gets attention.

**1. Required (built first, must work end to end)**
- Producer: REST ingestion with validation, publishing to `incoming.user_activity`.
- Consumer: reads the topic and upserts the aggregates into one PostgreSQL table.
- Two Dockerfiles, plus `compose.yaml` that starts Kafka, PostgreSQL, the topic init job, the migration job and both services.
- Migrations, automated tests, `.env.example` and a README covering start, submitting an event and inspecting results.

**2. Bonus (built, because the summary or the challenge asks for them)**
- Horizontal scaling: N consumer-group members as goroutines in one process, and/or `docker compose up --scale consumer=N` ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).
- A defined failure and retry strategy: permanent errors go to the DLQ, transient errors get backoff ([ADR 0011](0011-failure-handling-retry-and-dlq.md)).
- Graceful shutdown on `SIGTERM`/`SIGINT` in both services ([ADR 0012](0012-lifecycle-and-graceful-shutdown.md)).
- Load and crash testing (our own addition): a k6 harness in `loadtest/` that checks the three items above under load and ends every run with a report ([ADR 0016](0016-load-and-crash-testing.md)).

**3. Out of scope (documented, not built)**
- Authentication, authorization, TLS and rate limiting on the API, and SASL/TLS on Kafka ([ADR 0002](0002-security-baseline-and-threat-model.md)).
- A query/read API for the aggregates. Results are inspected with SQL.
- Storing raw events in PostgreSQL.
- Deduplication of client retries: if a client resends an HTTP request whose first attempt was already accepted, the event is counted twice. It would need a client-supplied idempotency key. Consumer redelivery is handled (effectively once per Kafka record, assumption 6).
- A Prometheus/Grafana stack. The services expose `/metrics` only ([ADR 0013](0013-observability.md)); load and crash runs are evaluated through their end-of-run report ([ADR 0016](0016-load-and-crash-testing.md)).
- Table partitioning, Schema Registry and Kubernetes manifests.

### Assumptions (ready to paste into the README)

1. **Time zone.** All dates are **UTC**. `activity_date` is the UTC calendar date of the event's `timestamp`, not the date the event was received.
2. **Aggregation grain.** One row per **user per UTC day** (`activity_aggregates`). Page views, unique users and activity volume over any date range are derived with SQL. Unique users are always `COUNT(DISTINCT user_id)` over the whole range, never a sum of daily counts.
3. **Activity types.** The challenge names only `page_view`. **Unknown activity types are accepted** and counted in `activity_count`. Only `page_view` also increments `page_view_count`.
4. **Metadata.** `metadata` is a free-form object of string values. It is validated for size, passed through to Kafka unchanged, and **not aggregated**. It is never logged in full, because it may hold URLs and referrers.
5. **URLs in the example.** The `<https://...>` angle brackets in the challenge's sample payload are a formatting artifact of the source document. Clients are expected to send plain strings, and the server stores whatever string it receives.
6. **Delivery semantics.** Effectively once per Kafka record: the consumer stores each partition's position in PostgreSQL in the same transaction as the aggregates and resumes from it, so a crash cannot count a batch twice (amended 2026-10-12, [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), option A; it replaced at-least-once with a one-batch overcount window). A client that retries an HTTP request still creates a second record. The payload has no event ID, so deduplicating client retries would need a client-supplied ID and extra state; it is a documented future option.
7. **Acknowledgement.** `202 Accepted` means the event is durably in Kafka (`acks=all`). It does not mean the aggregate is already updated.
8. **Security.** No auth or TLS on the API; it is meant for a local or evaluation deployment ([ADR 0002](0002-security-baseline-and-threat-model.md)).
9. **Results.** There is no query API. Inspect results with `docker compose exec postgres psql …` and the queries in [ADR 0009](0009-aggregation-data-model.md).
10. **Compose file name.** We use `compose.yaml`, the canonical name in the Compose Specification. The summary says `docker-compose.yml`, and Compose accepts both.
11. **Images.** "A Docker image for both" is read as **two images**, one per service, since they are separate microservices and the consumer scales on its own.
12. **Kafka is a given.** It is a challenge requirement, not a claim that Kafka is the best fit for every workload of this kind.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| Build the bonus items before the core path works | Rejected | The summary ranks a correct, reproducible pipeline above extras. |
| Reject unknown `activity_type` values (closed enum) | Rejected | It would drop data at the edge whenever a client adds a type, and the challenge lists only one type ([ADR 0004](0004-domain-model-and-ubiquitous-language.md)). |
| Local-time or per-user time-zone dates | Rejected | The challenge gives no time-zone rule; UTC is unambiguous and reproducible. |
| Store raw events next to the aggregates | Rejected | The challenge recommends one table, and Kafka retention already allows replay. |
| Effectively once via a dedupe table keyed by `event_id`, or Kafka transactions | Rejected | A dedupe table grows by one row per event and Kafka transactions cannot cover the PostgreSQL write. Chosen instead: the Kafka position stored in a 6-row `consumer_offsets` table in the same transaction as the upsert ([ADR 0008a](0008a-separate-offset-table-justification.md)). See [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md). |
| A single image with two entrypoints | Rejected | It couples the release and scaling of two independent services. |
| Add a read API for metrics | Deferred | Not asked for; SQL inspection is enough to show the results. |

## Consequences

**Positive**
- A reviewer can check every interpretation in one place.
- Effort goes to correctness and reproducibility (`docker compose up -d --build --wait` → `curl` → `psql`).
- The scope stays small enough to build and test properly within the deadline.

**Negative / trade-offs**
- Counts can be inflated by client retries (a retried request is a new record). Crashes no longer inflate them ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). Mitigation: documented here and in the README; client-side dedupe is described as a future option.
- Without auth, the API is unsafe to expose publicly. Mitigation: only `127.0.0.1` is published, and the production path is described in [ADR 0002](0002-security-baseline-and-threat-model.md).
- Without a read API, reviewers have to use SQL. Mitigation: copy-paste queries in the README.

## Implementation rules

- The README MUST contain the assumption list above, kept in sync with this ADR.
- Code MUST derive dates in UTC only (`t.UTC()`); `time.Local` MUST NOT be used.
- The producer MUST NOT reject an event because its `activity_type` is unknown.
- Any new feature outside tier 1 MUST NOT be merged while the end-to-end path (HTTP → Kafka → Postgres) is broken or untested.
- A change to any assumption MUST come with a new ADR that supersedes this one.

## Related

- [ADR 0003](0003-system-decomposition-and-layout.md) — how the two services and the repo are organized.
- [ADR 0004](0004-domain-model-and-ubiquitous-language.md) — the domain rules behind assumptions 1–3.
- [ADR 0009](0009-aggregation-data-model.md) — table and SQL queries for the metrics.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — effectively-once delivery with positions stored in PostgreSQL.
- [ADR 0002](0002-security-baseline-and-threat-model.md) — what "no auth" means and the production path.
- [ADR 0016](0016-load-and-crash-testing.md) — load and crash tests that verify the bonus items.

## References

- [1] Michael Nygard, "Documenting Architecture Decisions", Cognitect blog, 15 November 2011. https://www.cognitect.com/blog/2011/11/15/documenting-architecture-decisions. The Context / Decision / Consequences record form this ADR set follows, and the reason assumptions are written down as decisions.
- [2] Mark Richards and Neal Ford, *Fundamentals of Software Architecture*, O'Reilly Media, 2020, ISBN 978-1-4920-4345-4, ch. 19, "Architecture Decisions". Justifying and documenting decisions, which the challenge text asks for ("make sure you can justify your decision").
- [3] Apache Kafka 4.3 documentation, "Design", section "Message Delivery Semantics". https://kafka.apache.org/43/design/design/. Assumption 6 and the rejected alternative: Kafka transactions cover Kafka-to-Kafka only, so the consumer stores its position in the output store.
- [4] Martin Kleppmann, *Designing Data-Intensive Applications*, 1st ed., O'Reilly Media, 2017, ISBN 978-1-4493-7332-0, ch. 12, "The Future of Data Systems", section "The End-to-End Argument for Databases" (duplicate suppression). Why a client retry creates a second record unless the client supplies an end-to-end operation ID (out of scope).
- [5] R. Fielding, M. Nottingham, J. Reschke (eds.), *RFC 9110: HTTP Semantics*, IETF, June 2022, section 15.3.3 "202 Accepted". https://www.rfc-editor.org/rfc/rfc9110. Assumption 7: 202 acknowledges receipt, not completion of the processing.
- [6] G. Klyne and C. Newman, *RFC 3339: Date and Time on the Internet: Timestamps*, IETF, July 2002. https://www.rfc-editor.org/rfc/rfc3339. The `timestamp` format, and the UTC (`Z`) basis of assumption 1.
- [7] Docker Docs, "How Compose works: the Compose file". https://docs.docker.com/compose/intro/compose-application-model/. Assumption 10: `compose.yaml` is the preferred file name and `docker-compose.yml` is still read.
