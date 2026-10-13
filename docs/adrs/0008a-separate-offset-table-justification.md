# ADR 0008a: Justification for a separate offset tracking table

- **Status:** Accepted
- **Date:** 2026-10-12
- **Scope:** consumer
- **Addendum to:** [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)

> **TL;DR** — Keeps Kafka positions in a separate 6-row `consumer_offsets` table instead of adding offset columns to `activity_aggregates`. Recovery is a primary-key lookup, the business table stays free of transport metadata, and partition changes stay cheap. Trade-off: two tables, against the challenge's "one table" recommendation.

## Context

- The challenge says "Only one table is needed/recommended" for the aggregated results ([ADR 0000](0000-scope-interpretation-and-assumptions.md)).
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) stores the Kafka position of each partition in PostgreSQL, in the same transaction as the aggregate upsert. That is what makes delivery effectively once per partition. The position has to live somewhere in the database, and there are two ways to hold it:
  - **Option A:** a second table, `consumer_offsets`, with one row per `(consumer_group, topic, partition)`.
  - **Option B:** extra columns in `activity_aggregates` (for example `kafka_partition` and `last_offset`), so that one table suffices.
- Option B is workable: state-control columns embedded in the business rows can give the same guarantee, so a single table does meet the "needed" part of the sentence. The question is whether it is the better design, which is what "recommended" asks.

## Decision

We keep `activity_aggregates` as the analytics table and add a second, small control table, `consumer_offsets`, for the stored Kafka positions (Option A). This is a deliberate deviation from "one table", recorded as such in [ADR 0000](0000-scope-interpretation-and-assumptions.md).

The reasons are below.

### Recovery cost

After a crash or a rebalance the consumer reads its stored positions. With Option A this is a lookup by primary key `(consumer_group, topic, partition)` in a table of 6 rows: a constant-time lookup, independent of the number of users.

With Option B, recovery has two forms, and each is costly:

- scan the aggregate rows to find the highest offset per partition, which grows linearly with the number of rows (Option B1); or
- keep a secondary index on the offset columns, which adds index maintenance to every write and increases write amplification.

### Storage

With Option B, a database with 10 million user rows would carry 10 million offset markers, of which only 6 are the current positions; the rest are obsolete transport metadata attached to business data. Updated rows also leave dead tuples that vacuum has to reclaim. With Option A the state occupies 6 rows and does not touch the aggregate table or compete with it for the buffer pool.

### Domain isolation

Kafka transport metadata is not part of the user-activity data model. Keeping it in its own table leaves `activity_aggregates` free of it. This keeps the option open to move the analytical table to a column-oriented store later (ClickHouse, Redshift, Snowflake) while the offsets and their compare-and-set stay in a relational store, without changing the messaging contract.

### Topology changes

- **Adding partitions** (for example 6 to 24) needs no action with Option A. Existing partitions keep their rows, and each new partition gets its row on its first flush. Keys map to other partitions from that point on, which is harmless because the counters are additive.
- **Re-creating the topic** (offsets restart at 0) needs a controlled cut-over on the 6-row table: stop the consumers, commit the stored positions to Kafka, then delete the rows (procedure and commands in ADR 0008, "Operations"). A bare `TRUNCATE` is not enough on its own: without rows the consumer falls back to the Kafka committed offsets, which may lag behind the database, and every record in that gap would be counted twice.
- With Option B, adding partitions breaks the invariant that the recovery query relies on (a row is written from one partition only), and re-creating the topic invalidates the partition and offset values stored across all historical rows, which is hard to migrate.

## Consequences

**Positive**
- Position recovery is a primary-key lookup on 6 rows.
- The analytics table holds only analytics data.
- Partition and topic changes are small operations on a small table.

**Negative / trade-offs**
- The solution has two tables, not the one the challenge recommends. Mitigation: the deviation is documented in [ADR 0000](0000-scope-interpretation-and-assumptions.md), the second table has 6 rows, and it exists for a stated correctness reason (no double count after a crash).
- The runtime role needs rights on one more table. Mitigation: it gets `SELECT, INSERT, UPDATE` only; resetting positions is an operator task run as the owner ([ADR 0010](0010-postgresql-access-and-migrations.md)).

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| A: `consumer_offsets` table | Chosen | See the Decision. |
| B: offset columns in `activity_aggregates`, recovery by scan (B1) | Rejected | Linear-time recovery, and transport metadata inside the business table. |
| B: offset columns plus a secondary index | Rejected | Index maintenance on every write; the same metadata coupling. |
| Positions only in Kafka (commit after the DB write) | Rejected | A crash between the write and the commit counts a batch twice ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). |

## Related

- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md): the delivery semantics this table supports.
- [ADR 0009](0009-aggregation-data-model.md): both tables and their columns.
- [ADR 0010](0010-postgresql-access-and-migrations.md): migration `00002` and the runtime grants.

## References

- [1] Apache Kafka 4.3 documentation, "Design", section "Message Delivery Semantics". https://kafka.apache.org/43/design/design/. Letting the consumer store its offset in the same place as its output, because many output systems cannot join a two-phase commit.
- [2] Martin Kleppmann, *Designing Data-Intensive Applications*, 1st ed., O'Reilly Media, 2017, ISBN 978-1-4493-7332-0, ch. 3, "Storage and Retrieval": "B-Trees" and "Comparing B-Trees and LSM-Trees" (write amplification), and "Transaction Processing or Analytics?" with "Column-Oriented Storage". The logarithmic cost of index maintenance on every write, and the OLTP/OLAP split behind the domain-isolation argument.
- [3] PostgreSQL 18 documentation, "Routine Vacuuming". https://www.postgresql.org/docs/18/routine-vacuuming.html. Updated rows leave dead tuples that vacuum must reclaim, the storage cost of carrying offset columns in a large business table.
