# ADR 0009: Aggregation data model (daily per-user table)

- **Status:** Accepted
- **Date:** 2026-10-11
- **Amended:** 2026-10-12 — the consumer's Kafka positions live in a second, infrastructure-only table, `consumer_offsets` ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), [addendum](0008a-separate-offset-table-justification.md)). The aggregate model is unchanged.
- **Scope:** consumer

> **TL;DR** — One table, `activity_aggregates`, with one row per user per UTC day and CHECK constraints. Writes are a batched `unnest` upsert with additive deltas, and unique users come from `COUNT(DISTINCT user_id)` over the range. Trade-off: no sub-day resolution, and the upsert is not idempotent on its own, so it runs only with the position compare-and-set.

## Context

The challenge asks the consumer to "perform data transformations, such as counting the number of users and the number of `page_view` activities" and to "write aggregated results to a PostgreSQL database. Only one table is needed/recommended."

Our summary ([`../real_time_analytics_challenge_summary.md`](../real_time_analytics_challenge_summary.md) §3–§7) fixes the data model:

- one aggregate row per user per **UTC** calendar day;
- no raw events in PostgreSQL (Kafka retention covers replay);
- three metrics over any date range: page views, unique users, activity volume;
- no preemptive partitioning; indexes only where query patterns justify them.

The domain rules come from [ADR 0004](0004-domain-model-and-ubiquitous-language.md): every accepted `ActivityEvent` adds 1 to `activity_count`; an event whose `ActivityType` is `page_view` also adds 1 to `page_view_count`; the date is the UTC date of `timestamp`. A `DailyUserAggregate` is the in-memory delta for one `(activity_date, user_id)`.

## Decision

We use exactly one table, `activity_aggregates`, created by the first migration ([ADR 0010](0010-postgresql-access-and-migrations.md)). The schema is the one in the summary, verbatim:

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

| Column | Meaning |
|---|---|
| `activity_date` | UTC calendar date of the event's `timestamp` (not the date it was received or processed). |
| `user_id` | The user the activity belongs to, as sent by the client. Opaque text. |
| `activity_count` | Number of accepted events for this user on this date, of any `ActivityType`. |
| `page_view_count` | Number of `page_view` events for this user on this date. Always `<= activity_count`. |
| `updated_at` | Database time of the last insert or update of the row. Not an event time. |

All aggregated data lives in this one table. Since the 2026-10-12 amendment, a second table, `consumer_offsets` (migration `00002`), stores the Kafka position of each partition next to the aggregates it produced. It holds no business data, one row per `(consumer_group, topic, partition)` (6 rows today), and is described in [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) and [ADR 0010](0010-postgresql-access-and-migrations.md). Why it is a separate table and not columns of `activity_aggregates`: [ADR 0008 addendum](0008a-separate-offset-table-justification.md).

### Indexes

- The primary key `(activity_date, user_id)` enforces one row per user per day and gives a B-tree that leads with `activity_date`. Every date-range metric below uses it, so we do **not** add a separate `activity_date` index.
- `idx_activity_aggregates_user` serves per-user queries across dates (a user's history, data-subject erasure).
- Further indexes are added only after `EXPLAIN (ANALYZE, BUFFERS)` on a real query shows the need.

### Writes: atomic delta upsert

The consumer never reads a row to modify it. It sends deltas, and PostgreSQL adds them atomically, so concurrent writers cannot lose each other's increments. `updated_at` is set explicitly in the conflict clause, because its `DEFAULT` applies only on insert.

Single-row form (reference semantics):

```sql
INSERT INTO activity_aggregates AS a
    (activity_date, user_id, activity_count, page_view_count)
VALUES ($1, $2, $3, $4)
ON CONFLICT (activity_date, user_id) DO UPDATE
SET activity_count  = a.activity_count  + EXCLUDED.activity_count,
    page_view_count = a.page_view_count + EXCLUDED.page_view_count,
    updated_at      = now();
```

Batched form, which the consumer actually runs once per flush ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). One round trip, one statement, any batch size:

```sql
INSERT INTO activity_aggregates AS a
    (activity_date, user_id, activity_count, page_view_count)
SELECT d, u, ac, pv
FROM unnest($1::date[], $2::text[], $3::bigint[], $4::bigint[]) AS t(d, u, ac, pv)
ON CONFLICT (activity_date, user_id) DO UPDATE
SET activity_count  = a.activity_count  + EXCLUDED.activity_count,
    page_view_count = a.page_view_count + EXCLUDED.page_view_count,
    updated_at      = now();
```

The arrays MUST hold each `(activity_date, user_id)` at most once. PostgreSQL rejects an `ON CONFLICT DO UPDATE` that touches the same row twice in one statement (SQLSTATE `21000`), so the consumer folds events into one `DailyUserAggregate` per key before the flush. It also sorts the keys, so row locks are always taken in the same order.

**This upsert is not idempotent.** Running it twice for the same events adds the deltas twice. It therefore runs only inside the transaction that also moves the partition's stored position in `consumer_offsets` with a compare-and-set. A redelivered batch finds the position already moved and writes nothing, so each Kafka record is counted once per partition (effectively once). Details and the remaining caveat (a client that retries an HTTP request sends a second event): [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md).

### Reads: the three metrics over a date range

Dates are inclusive bounds, in UTC (`$1` = first day, `$2` = last day).

```sql
-- Page views and activity volume
SELECT COALESCE(SUM(page_view_count), 0) AS page_views,
       COALESCE(SUM(activity_count), 0)  AS activity_volume
FROM activity_aggregates
WHERE activity_date BETWEEN $1 AND $2;

-- Unique users over the WHOLE range (never sum daily unique counts)
SELECT COUNT(DISTINCT user_id) AS unique_users,
       COUNT(DISTINCT user_id) FILTER (WHERE page_view_count > 0) AS unique_page_viewers
FROM activity_aggregates
WHERE activity_date BETWEEN $1 AND $2;

-- Daily breakdown (per-day unique users are valid only per day)
SELECT activity_date,
       COUNT(*)             AS unique_users,
       SUM(page_view_count) AS page_views,
       SUM(activity_count)  AS activity_volume
FROM activity_aggregates
WHERE activity_date BETWEEN $1 AND $2
GROUP BY activity_date
ORDER BY activity_date;
```

A user active on Monday and Tuesday is one unique user for the two-day range, but appears in both daily rows. Summing the daily `unique_users` column would count that user twice. That is why the range query uses `COUNT(DISTINCT user_id)` and never adds up daily figures.

### Partitioning: not now

We start with a regular table. Partitioning is a physical data-management choice, and the daily grain does not imply daily partitions. At challenge scale it adds operational cost (creating partitions ahead of time, routing, more planner work) without making any of the queries above faster. We revisit it only when measurements show one of these triggers:

- a retention policy that deletes by date (then `DETACH PARTITION … CONCURRENTLY` + `DROP` beats a bulk `DELETE`);
- the table or its indexes no longer fit comfortably in memory, and range queries degrade;
- autovacuum can no longer keep up with the update rate.

### Growth estimate

One row per active user per day. A row with a short `user_id` takes about 70–80 bytes in the heap, plus roughly 40 bytes in the PK and 30 bytes in the `user_id` index: about 150 bytes in total. At 1 million daily active users that is about 150 MB/day, or about 55 GB/year, before bloat. At challenge scale (thousands of users) it is negligible. Each event updates a row in place, so dead tuples come from updates, not from row count. We tune autovacuum and `fillfactor` (for HOT updates) only if `pg_stat_user_tables` shows a problem.

### Personal data

`user_id` is an identifier of a natural person, so the table is personal data under LGPD/GDPR. Erasure for one user is `DELETE FROM activity_aggregates WHERE user_id = $1`, which `idx_activity_aggregates_user` serves. It runs as an operator role, not as the runtime role, which has no `DELETE` ([ADR 0010](0010-postgresql-access-and-migrations.md)). Raw events stay in Kafka until topic retention expires them ([ADR 0007](0007-kafka-client-and-topic-topology.md)). The retention and erasure policy is in [ADR 0002](0002-security-baseline-and-threat-model.md).

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| One row per user per UTC day (this ADR) | Chosen | Answers all three metrics for any day range; the table stays far smaller than the event stream; one row per key makes the upsert simple. |
| Store raw events, aggregate at query time | Rejected | The summary excludes raw events from PostgreSQL. It is the largest table possible, and Kafka retention already allows replay. |
| Per-minute or per-hour buckets | Rejected | No requirement needs sub-day resolution. Up to 1,440× more rows per user per day, and more upsert conflicts to resolve. |
| Global counters (one row per day, or one row in total) | Rejected | Unique users cannot be derived over a range: you cannot de-duplicate users across days without per-user rows. Every consumer would also contend for one hot row. |
| HyperLogLog sketches per day | Rejected | Approximate counts; needs an extension that the official image does not ship; harder to review and test. Worth reconsidering only if exact distinct counts become too expensive. |
| Separate `activity_date` index | Rejected | The PK already leads with `activity_date`. |
| Daily or multi-level partitioning now | Rejected | No measured need; see the triggers above. |

## Consequences

**Positive**
- One table for the aggregated data, as the challenge recommends. Every metric is one SQL query using an index. The only other table, `consumer_offsets`, is infrastructure state of 6 rows.
- Atomic increments: concurrent members and partitions cannot overwrite each other.
- `CHECK` constraints catch a buggy delta (negative counts, more page views than activities) at the database.

**Negative / trade-offs**
- No sub-day analytics. Mitigation: a finer grain is a new table plus a migration; Kafka retention allows backfill within the retention window.
- The upsert is not idempotent, so on its own a redelivery would overcount. Mitigation: it only runs in the transaction that compare-and-sets the partition's stored position, so a redelivered batch is never applied twice ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).
- `COUNT(DISTINCT user_id)` over long ranges scans many rows. Mitigation: fine at this scale; HLL or rollups if measurements demand it.
- Events with skewed or far-future timestamps create rows on those dates. Mitigation: the data stays correct per its own date; a plausibility bound on `timestamp` belongs to validation ([ADR 0005](0005-ingestion-http-api.md)) if it is ever needed.

## Implementation rules

- The migration MUST create the table and index exactly as above. Column names, types and constraint expressions MUST NOT diverge from the summary.
- `activity_date` MUST be computed in Go as the UTC date of the event `timestamp` (`t.UTC()`), never from `time.Local` or the processing time.
- Writes MUST use `INSERT … ON CONFLICT (activity_date, user_id) DO UPDATE` with `x = a.x + EXCLUDED.x` increments. Read-modify-write (`SELECT` then `UPDATE`) MUST NOT be used.
- The conflict clause MUST set `updated_at = now()`.
- The upsert MUST run only in the transaction that compare-and-sets the partition's row in `consumer_offsets` ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).
- Offsets and other transport metadata MUST NOT be added as columns of `activity_aggregates` ([addendum](0008a-separate-offset-table-justification.md)).
- A batch MUST contain each `(activity_date, user_id)` key at most once, sorted by key.
- All SQL MUST be parameterized; values MUST NOT be interpolated into SQL strings ([ADR 0002](0002-security-baseline-and-threat-model.md)).
- Unique users across a range MUST be computed with `COUNT(DISTINCT user_id)` over the range, never by summing daily values. The README queries MUST follow this ADR.
- New indexes or partitioning MUST NOT be added without a query plan or measurement in the PR that justifies them.

## Related

- [ADR 0004](0004-domain-model-and-ubiquitous-language.md) — `DailyUserAggregate` and the delta rules.
- [ADR 0005](0005-ingestion-http-api.md) — validation of `user_id` and `timestamp`.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — when the batched upsert runs, and the stored positions that make it effectively once.
- [ADR 0010](0010-postgresql-access-and-migrations.md) — the Go side of the upsert, migrations and roles.
- [ADR 0002](0002-security-baseline-and-threat-model.md) — personal data, retention and erasure.

## References

- [1] PostgreSQL 18 documentation, "INSERT", clause `ON CONFLICT`. https://www.postgresql.org/docs/18/sql-insert.html. `DO UPDATE` is atomic under concurrency; it is a deterministic statement that raises a cardinality violation if it would affect one row twice (SQLSTATE `21000`); `SELECT` privilege is needed on columns it reads.
- [2] PostgreSQL 18 documentation, "Constraints" (https://www.postgresql.org/docs/18/ddl-constraints.html) and "Date/Time Types" (https://www.postgresql.org/docs/18/datatype-datetime.html). Primary key and `CHECK` constraints, and the `DATE` and `TIMESTAMPTZ` types of the schema.
- [3] PostgreSQL 18 documentation, "Aggregate Expressions" (https://www.postgresql.org/docs/18/sql-expressions.html) and "Array Functions and Operators" (https://www.postgresql.org/docs/18/functions-array.html). `COUNT(DISTINCT ...) FILTER (WHERE ...)` for the range metrics, and `unnest` over parallel arrays for the batched upsert.
- [4] PostgreSQL 18 documentation, "Table Partitioning" (https://www.postgresql.org/docs/18/ddl-partitioning.html) and "Routine Vacuuming" (https://www.postgresql.org/docs/18/routine-vacuuming.html). The costs and benefits weighed in "Partitioning: not now" and the autovacuum remark in the growth estimate.
- [5] Martin Kleppmann, *Designing Data-Intensive Applications*, 1st ed., O'Reilly Media, 2017, ISBN 978-1-4493-7332-0, ch. 7, "Transactions", section "Preventing Lost Updates". Atomic increments instead of read-modify-write.
- [6] Lei no 13.709 of 14 August 2018 (LGPD), art. 18, https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm, and Regulation (EU) 2016/679 (GDPR), art. 17, https://eur-lex.europa.eu/eli/reg/2016/679/oj. The erasure requirement behind the per-user index and the erasure statement.
