# ADR 0017: Scaling and capacity

- **Status:** Accepted
- **Date:** 2026-10-12
- **Scope:** both

## Context

- The challenge gives bonus points if the service "can be scaled horizontally by adding more consumers" ([ADR 0000](0000-scope-interpretation-and-assumptions.md)). [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) and [ADR 0012](0012-lifecycle-and-graceful-shutdown.md) define how consumers join the group. This ADR covers the whole pipeline: producer, consumer, Kafka and PostgreSQL.
- The delivery shape is Docker Compose on one host ([ADR 0015](0015-containerization-and-local-deployment.md)): one Kafka broker with replication factor 1, one PostgreSQL instance, and the producer published on `127.0.0.1:8080`.
- The load and crash tests ([ADR 0016](0016-load-and-crash-testing.md)) measured each component under equal limits and with one component starved. The results are in [load-test-results.md](../load-test-results.md):
  - With 1.5 CPU per component, the pipeline kept up with every stress step up to 3000 req/s, the harness maximum.
  - Kafka was the first component at its CPU limit, from the 1757 req/s step. At 3000 req/s the producer used 78 % of its limit, the consumer 47 % and PostgreSQL 4 %.
  - Scaling out to a second consumer replica under load lost and double-counted nothing at 200 and at 1000 req/s.

## Decision

We keep Compose as the only deployment in this repository and record here how each component scales beyond it, which signals would drive autoscaling, and what the measurements say about where capacity runs out.

### Producer: stateless replicas behind a load balancer

- The producer holds no state between requests. Every replica has its own Kafka client with `acks=all` and idempotence ([ADR 0007](0007-kafka-client-and-topic-topology.md)). We scale it by running N replicas behind a load balancer (a Kubernetes Service or a cloud load balancer).
- Partitioning is by `user_id`, whatever the replica, so a user's events still land on one partition. Two requests for the same user that hit different replicas may be appended in either order. The counters are additive, so the totals do not depend on that order.
- Locally, `compose.yaml` publishes a fixed host port (`127.0.0.1:8080:8080`), and one host port binds to one container only. `docker compose up --scale producer=2` therefore fails on the port. Running several producers locally would need an extra reverse-proxy service or unpublished ports with a port range. We do not ship either: a single producer was not the first bottleneck.
- Measured: one producer at 1.5 CPU served 2991 req/s at 117 % CPU. At 0.5 CPU it was pinned from 929 req/s and still served 2849 req/s, with p95 rising to 105 ms and 22,484 dropped iterations. CPU saturation shows up first as latency, before throughput drops.

### Consumer: members up to the partition count

- Parallelism is `CONSUMER_WORKERS` members per process × replicas, all in one group ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). At most 6 members own partitions (6 partitions); the others are idle standbys.
- Members use static membership with the instance ID `<CONSUMER_INSTANCE_ID>-<i>`, which defaults to the hostname ([ADR 0012](0012-lifecycle-and-graceful-shutdown.md)). A restarted replica takes its partitions back without a rebalance. The instance ID must be unique and stable per replica: a StatefulSet pod name works, a Deployment pod name does not.
- **Scaling down.** A clean stop (SIGTERM) leaves the group explicitly, so its partitions move at once. A replica that crashes and never returns holds its partitions until `CONSUMER_SESSION_TIMEOUT` (30 s) expires. Lowering `CONSUMER_WORKERS` leaves the removed instance IDs registered until their session expires.
- Correctness does not depend on the member count. Positions live in `consumer_offsets` and are moved by compare-and-set, in the same transaction as the aggregates ([ADR 0008a](0008a-separate-offset-table-justification.md)).
- Measured: `crash-scale-out` (1 to 2 replicas under load) was ready in 1.6 s at 200 req/s and 2.1 s at 1000 req/s, with zero overcount. A single consumer at 0.25 CPU kept up with 2990 req/s (lag at most 2,669, drain 3.7 s). With the default pool of 5,000 users, consumer CPU is not what limits throughput.

### Kafka: brokers, replication and partitions

- **Production cluster.** At least 3 brokers, replication factor 3 on both topics and on the internal topics, and `min.insync.replicas=2` ([ADR 0007](0007-kafka-client-and-topic-topology.md)). With `acks=all` this tolerates the loss of one broker without losing an acknowledged event.
- **More brokers.** A new broker gets no existing partitions on its own. Replicas are moved with `kafka-reassign-partitions.sh` (generate, execute, verify); replication traffic can be throttled during the move.
- **More partitions.** More partitions raise the consumer cap above 6. With option A no consumer action is needed: each new partition gets its row in `consumer_offsets` on its first flush ([ADR 0008a](0008a-separate-offset-table-justification.md)). Keys then map to other partitions, which is harmless for additive counters. Re-creating the topic is different: it needs the controlled cut-over in [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), "Operations".
- Measured: Kafka was the first component at its CPU limit, at 1.5 CPU from the 1757 req/s step. At 0.5 CPU it was pinned at every step and still carried 2941 req/s, with producer p95 near 100 ms and the longest drain (14.7 s). Broker CPU sets producer latency before it caps throughput. The broker had replication factor 1, so these numbers are an upper bound for a replicated cluster.

### PostgreSQL: one writer, bounded connections

- All writes go to one primary. The upsert batches per `(activity_date, user_id)` and sorts the keys ([ADR 0009](0009-aggregation-data-model.md)). The record key is `user_id`, so the rows of a given user are written by one partition worker at a time, and members do not contend for row locks.
- **Connections.** Each consumer process opens up to `DATABASE_MAX_CONNS` (default 8) connections, so the total is about replicas × `DATABASE_MAX_CONNS`. That total must stay below `max_connections` (default 100), with room for the migration job and operators ([ADR 0010](0010-postgresql-access-and-migrations.md)). Members beyond 6 add connections without adding throughput.
- **PgBouncer.** If many replicas are needed, a PgBouncer in transaction pooling mode keeps the server connection count fixed. Each flush is one transaction, which fits that mode. Prepared-statement handling must be checked against the pooling mode before it is enabled.
- **Hot rows.** A very active user means many updates of one row: dead tuples and vacuum work, not lock contention. HOT updates avoid index churn when the page has free space. `fillfactor` and autovacuum are tuned only if `pg_stat_user_tables` shows a problem ([ADR 0009](0009-aggregation-data-model.md)).
- Measured: under equal limits PostgreSQL used at most 7.9 % CPU per step. At 0.25 CPU with 200,000 users (235,150 rows written), flush p95 rose to 480 ms and p99 to 1.6 s, lag peaked at 5,412 and drained in 4.0 s. PostgreSQL's mean CPU stayed below its limit.

### Autoscaling signals

| Component | Signal | Mechanism | Limits of the signal |
|---|---|---|---|
| consumer | `consumer_lag_records` (sum) or the group lag read from Kafka | KEDA Kafka scaler (`lagThreshold`) or an HPA on an external metric | Replicas are capped at the partition count. The gauge refreshes every 15 s and is empty while the consumer is down. Lag rose most when the database was slow (db-bound), where more consumers add load on the bottleneck. Every scaling step is a rebalance, so a stabilisation window is needed. |
| producer | CPU utilisation, request rate, p95 latency | Kubernetes HPA | CPU tracks producer saturation. When the broker is slow, producer CPU falls (63.6 % against 85.4 %) while latency rises, and more producers would not help. Latency is the better alarm; CPU is the scaling input. |
| Kafka, PostgreSQL | broker CPU and request latency; flush duration, connections | operator or managed-service tooling, not autoscaled | Brokers and the primary are stateful: adding capacity means reassigning partitions or resizing the primary. These are planned operations. |

### What the measurements say

Bottleneck order with equal resources: **Kafka, then the producer, then the consumer, then PostgreSQL**. Capacity observed per component, as a lower bound because the generator capped the load at 3000 req/s:

| Component | CPU tested | Highest step served | Cost of the limit |
|---|---|---|---|
| Kafka (RF 1) | 1.5 / 0.5 | 2991 / 2941 req/s | pinned from 1757 req/s at 1.5; p95 near 100 ms at 0.5 |
| producer | 1.5 / 0.5 | 2991 / 2849 req/s | p95 105 ms and 22,484 dropped at 0.5 |
| consumer | 1.5 / 0.25 | 2991 / 2990 req/s | flush p95 95 ms at 0.25 |
| PostgreSQL | 1.5 / 0.25 (200k users) | 2991 / 2998 req/s | flush p95 480 ms, p99 1.6 s at 0.25 |

The first capacity step for this workload is the broker (more brokers, more CPU per broker), then producer replicas. Consumer replicas pay off once the database or the per-partition flush becomes the limit, and only up to 6 members unless partitions are added.

### Why Compose stays local

Compose on one host is one failure domain. It schedules on one machine only, cannot publish one port for several replicas, has no autoscaler, and its single broker and single database have no replicas. The path to production is an orchestrator such as Kubernetes:

- the producer as a Deployment with an HPA behind a Service;
- the consumer as a StatefulSet, for stable instance IDs, scaled by KEDA on lag up to the partition count;
- Kafka and PostgreSQL as managed services or operator-run clusters, with RF 3 / min ISR 2, and PgBouncer in front of PostgreSQL if needed.

The images, the env-only configuration ([ADR 0003](0003-system-decomposition-and-layout.md)) and the health endpoints ([ADR 0013](0013-observability.md)) already fit that path. None of it is built in this repository.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| Ship Kubernetes manifests, KEDA and HPA objects now | Rejected | Out of the challenge's scope and untested here; a manifest that is never run is a liability. |
| A load balancer service in Compose to scale the producer | Rejected | One more component for a bottleneck that was not first; the measurements put Kafka ahead of the producer. |
| Autoscale consumers on CPU | Rejected | Consumer CPU stayed low while lag grew (db-bound); lag measures the backlog directly. |
| More partitions now (for example 12) | Rejected | The consumer at 0.25 CPU kept up with 2990 req/s, so 6 partitions are not limiting; adding partitions later needs no consumer action (option A). |

## Consequences

**Positive**
- Each scaling step is tied to a measured bottleneck and a named signal.
- Consumer scaling is safe at any member count, because positions are stored with the aggregates.

**Negative / trade-offs**
- The capacity figures come from one host and one broker with RF 1. Mitigation: they are stated as lower bounds for throughput and upper bounds for broker headroom, and should be re-measured on the target platform.
- The true ceiling was not found (k6 maximum of 3000 req/s, 300 VUs). Mitigation: [load-test-results.md](../load-test-results.md) lists the limits; a distributed generator is needed to go further.

## Implementation rules

- Consumer replicas × `CONSUMER_WORKERS` SHOULD NOT exceed the partition count, except for deliberate standbys.
- Replicas × `DATABASE_MAX_CONNS` MUST stay below PostgreSQL's `max_connections`, minus the connections reserved for migrations and operators.
- Every consumer replica MUST have a unique, stable `CONSUMER_INSTANCE_ID` ([ADR 0012](0012-lifecycle-and-graceful-shutdown.md)).
- A production Kafka MUST use RF 3 and `min.insync.replicas=2` for both topics ([ADR 0007](0007-kafka-client-and-topic-topology.md)).
- Capacity claims MUST cite a run in [load-test-results.md](../load-test-results.md) or a newer run of the harness.

## Related

- [ADR 0007](0007-kafka-client-and-topic-topology.md): partitions, keys, replication settings.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), [ADR 0008a](0008a-separate-offset-table-justification.md): group members, stored positions, partition changes.
- [ADR 0010](0010-postgresql-access-and-migrations.md): the pool and `DATABASE_MAX_CONNS`.
- [ADR 0012](0012-lifecycle-and-graceful-shutdown.md): static membership and scale-down behaviour.
- [ADR 0016](0016-load-and-crash-testing.md): the harness behind the measurements.

## References

- [1] Apache Kafka 4.3 documentation, "Basic Kafka Operations": sections "Expanding your cluster", "Increasing replication factor" and "Modifying topics". https://kafka.apache.org/43/operations/basic-kafka-operations/. Partition reassignment with `kafka-reassign-partitions.sh` and adding partitions.
- [2] Apache Kafka 4.3 documentation, "Topic Configs", `min.insync.replicas`. https://kafka.apache.org/43/configuration/topic-configs/. The minimum number of in-sync replicas for an `acks=all` write.
- [3] KEDA v2.21 documentation, "Apache Kafka" scaler. https://keda.sh/docs/2.21/scalers/apache-kafka/. `lagThreshold`, and replicas capped at the partition count unless `allowIdleConsumers` is set.
- [4] Kubernetes documentation, "Horizontal Pod Autoscaling". https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/. Scaling on CPU utilisation and on custom or external metrics.
- [5] PgBouncer, "Features". https://www.pgbouncer.org/features.html. Session, transaction and statement pooling modes.
- [6] PostgreSQL 18 documentation, "Connections and Authentication", `max_connections`. https://www.postgresql.org/docs/18/runtime-config-connection.html. The server-wide connection limit (default typically 100).
- [7] PostgreSQL 18 documentation, "Heap-Only Tuples (HOT)". https://www.postgresql.org/docs/18/storage-hot.html. Updates without new index entries when the page has free space.
- [8] Docker documentation, "docker compose up", option `--scale`. https://docs.docker.com/reference/cli/docker/compose/up/. Running several instances of one service.
- [9] Docker documentation, Compose file reference, "Services", `ports`. https://docs.docker.com/reference/compose-file/services/. Fixed published ports and runtime-assigned host ports.
- [10] Docker documentation, "Resource constraints", `--cpus`. https://docs.docker.com/engine/containers/resource_constraints/. CPU limits as a CFS quota, the mechanism behind the profiles.
- [11] Grafana k6 documentation, "Dropped iterations". https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/dropped-iterations/. Arrival-rate executors drop iterations when no VU is free, the reason the 300-VU cap bounds the measurements.
