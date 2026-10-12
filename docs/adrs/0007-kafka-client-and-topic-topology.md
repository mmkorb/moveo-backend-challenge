# ADR 0007: Kafka client and topic topology

- **Status:** Accepted
- **Date:** 2026-10-11
- **Amended:** 2026-10-12 (review) - the consumer settings table follows the code (static membership, session timeout, stored-position resume, explicit static leave), and the DLQ client allows idempotent produce cancellation.
- **Scope:** both

## Context

- Kafka is a hard requirement. The topic name `incoming.user_activity` is fixed by the challenge.
- Images are static `CGO_ENABLED=0` binaries on distroless ([ADR 0015](0015-containerization-and-local-deployment.md)), so the client must be pure Go.
- The consumer needs per-partition control: rebalance callbacks, committing only what was written to Postgres, and cooperative rebalancing so scaling doesn't stop the whole group ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).
- Several consumers may write to the same `(activity_date, user_id)` row. Atomic upserts make that safe ([ADR 0004](0004-domain-model-and-ubiquitous-language.md)), but routing all of a user's events to one partition also gives per-user ordering and avoids row-lock contention across members.
- The summary asks for **explicit** retention and broker settings. Topics that appear implicitly (broker auto-create) have default partitions and retention, and they hide typos in topic names.

## Decision

### Client: franz-go

We use `github.com/twmb/franz-go` in both services (exact versions in [ADR 0001](0001-versions-and-upgrade-policy.md)):
- `pkg/kgo` for producing and consuming;
- `pkg/kadm` in the consumer for admin calls (the startup check that both topics exist, and the group-lag gauge);
- `pkg/kmsg` in the producer for its startup topic check, a raw metadata request (the producer needs no other admin call);
- `plugin/kprom` for client metrics registered on the injected Prometheus registry ([ADR 0013](0013-observability.md)).

### Topics

| Topic | Partitions | `retention.ms` | `cleanup.policy` | Writer | Reader |
|---|---|---|---|---|---|
| `incoming.user_activity` | 6 | 604800000 (7 days) | `delete` | producer | consumer group |
| `incoming.user_activity.dlq` | 3 | 1209600000 (14 days) | `delete` | consumer | humans / replay tool |

- Broker auto-create is **off** (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`).
- Topics are created by `deploy/kafka/create-topics.sh`, run once by the `kafka-init` compose service. It calls `kafka-topics.sh --create --if-not-exists` with the partitions and `retention.ms` from the table above, so it is safe to re-run. The script itself, verified against a running broker, lives in [ADR 0015](0015-containerization-and-local-deployment.md#topic-init-script-deploykafkacreate-topicssh). This ADR owns the values; 0015 owns the mechanics.

- Both services check at startup that their topics exist, retrying with backoff until a deadline, then failing with a clear error. The consumer lists its input and DLQ topics with `kadm`; the producer sends a `kmsg` metadata request for its topic (`Publisher.EnsureTopic`, with auto-creation disabled). They never create topics.
- Replication factor 1 and `min.insync.replicas=1` are **local-only** (single broker). Production would use RF 3 and min ISR 2.
- 7 days of retention allows replaying the topic after a consumer bug fix. It is not archival storage (summary §6). The DLQ keeps records longer, so a person has time to inspect them.

### Partitioning and key

- **Record key = `user_id`.** franz-go's default partitioner hashes keyed records with murmur2, the same as the Java client, so a given user always lands on the same partition.
- Consequences: events of one user are processed in order by one worker. A given `(activity_date, user_id)` row is only written from one partition at a time, so batches from different partitions never contend on the same row.
- **6 partitions = at most 6 active consumers** in the group. Extra members sit idle as hot standbys. Raising the count later is possible (`kafka-topics.sh --alter`), but it remaps keys, so it is a deliberate operation.
- Hot users (skew) are accepted at this scale; the mitigation would be a composite key, which loses per-user ordering.

### Producer settings (producer service; the DLQ publisher differs, see the table below)

- `acks=all` and the **idempotent producer**. Both are franz-go defaults (`RequiredAcks` defaults to all ISR acks; idempotency is on unless `DisableIdempotentWrite` is set). We keep them and assert them in a test.
- `DefaultProduceTopic` set from config; a short linger (`ProducerLinger`, a few ms) for small batches; `RecordDeliveryTimeout` = `PUBLISH_TIMEOUT` (default 5 s, minimum 1 s), the same bound the handler uses for the publish context.
- `kgo.AllowIdempotentProduceCancellation()` on the producer. Without it, `ProduceSync` ignored the context and `RecordDeliveryTimeout` when the broker died with a record in flight, and blocked for more than 7 minutes, so the 503 path never fired. The cost: a cancelled record that is retried may be written twice. That is at-least-once delivery, already accepted in [ADR 0000](0000-scope-interpretation-and-assumptions.md) and [ADR 0011](0011-failure-handling-retry-and-dlq.md).
- `PUBLISH_TIMEOUT` must be at least 1s (the franz-go minimum); the config rejects lower values.
- The HTTP handler produces with `ProduceSync(ctx, record)` and returns 202 only after the ack ([ADR 0005](0005-ingestion-http-api.md)).
- The idempotent producer removes broker-side duplicates from **internal** retries. It does not deduplicate client retries or consumer redelivery ([ADR 0006](0006-event-contract-and-json-serialization.md)).

### Consumer settings (summary; details in [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md))

- `ConsumerGroup("analytics-consumer")` (configurable), `ConsumeTopics("incoming.user_activity")`.
- `Balancers(CooperativeStickyBalancer())` (also the franz-go default, set explicitly for clarity), so a rebalance only moves the partitions that change owner.
- `AutoCommitMarks()`: only offsets marked with `MarkCommitRecords` after a successful Postgres commit are committed. A final `CommitMarkedOffsets` runs on revoke and on shutdown.
- `OnPartitionsAssigned` / `OnPartitionsRevoked` / `OnPartitionsLost` start and stop one worker per partition.
- Shutdown uses `CloseAllowingRebalance()` ([ADR 0012](0012-lifecycle-and-graceful-shutdown.md)).

### Settings that differ from the defaults

"Default" is the franz-go v1.22.1 default (read from `pkg/kgo/config.go`) or the Kafka 4.3 broker default. Broker defaults were verified against a running apache/kafka:4.3.1 container (`kafka-configs.sh --describe --all`, `kafka-server-start.sh`). "Kept" means the value equals the default and we set it only to make it explicit or to assert it in a test.

**Producer client** (HTTP producer; the consumer's DLQ publisher uses the same defaults except where noted)

| Setting | Default | Ours | Why | Where |
|---|---|---|---|---|
| `RequiredAcks` | all ISR acks | all ISR (kept, asserted in a test) | 202 means the event is replicated ([ADR 0005](0005-ingestion-http-api.md)) | `services/producer/internal/adapters/kafka/publisher.go` |
| Idempotent write | on | on (`DisableIdempotentWrite` never used) | No broker-side duplicates from internal retries | same |
| `ProducerLinger` | 10 ms | 5 ms (`Config.Linger`; DLQ fixed 5 ms) | `ProduceSync` sends one record per call, so a long linger is pure latency; 5 ms still packs concurrent requests | `publisher.go`, `services/consumer/internal/adapters/kafka/dlq.go` |
| `RecordDeliveryTimeout` | none (unbounded) | `PUBLISH_TIMEOUT` (5s in `.env.example`, minimum 1s); DLQ fixed 10s | Bounds the wait for an ack so the handler can answer 503. For the DLQ it makes the worker's retry loop, not the client, decide what happens next | same |
| `AllowIdempotentProduceCancellation` | off | on (producer and DLQ client) | A cancelled in-flight record otherwise blocks `ProduceSync` for minutes. On the DLQ client it keeps a flush attempt within `FLUSH_TIMEOUT` and shutdown within its grace period when the broker dies mid-publish. The cost is a possible duplicate (at-least-once; duplicate dead letters are accepted, [ADR 0011](0011-failure-handling-retry-and-dlq.md)) | `publisher.go`, `dlq.go` |
| `DefaultProduceTopic` | none | topic from config | Safety net; each record also names its topic | `publisher.go` |
| `ClientID` | `kgo` | `moveo-producer`; `<group>-dlq`; `<group>-ping` | Broker logs and kprom series show which client is which | `publisher.go`, `dlq.go`, `ping.go` |
| Batch compression | Snappy, falling back to none | not set (default) | Events are small JSON; the choice waits for a measurement (see the production profile) | - |
| Partitioner | `UniformBytesPartitioner`; keyed records use murmur2 | not set (default) | Keyed records hash `user_id` like the Java client | - |
| kprom hooks | none | `kprom.NewMetrics("kafka")` | [ADR 0013](0013-observability.md) | `publisher.go` |

**Consumer client** (one client per group member; the `-ping` client at startup uses defaults only)

| Setting | Default | Ours | Why | Where |
|---|---|---|---|---|
| `ConsumerGroup` | none | `Config.Group` (`analytics-consumer`) | One group for all members of all processes | `services/consumer/internal/adapters/kafka/member.go` |
| `ConsumeResetOffset` | `RewindOffset(1m)`: no committed offset means start one minute back | `NewOffset().AtStart()` | A new group, or an expired offset, reads from the beginning and loses nothing; re-reading is safe because positions are stored with the aggregates and moved by compare-and-set ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)) | `member.go` |
| `AdjustFetchOffsetsFn` | none | `m.adjustOffsets` | Runs after assignment and the group's `OffsetFetch`, before fetching: resumes each partition from the position stored in PostgreSQL, the source of truth ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)) | `member.go` |
| `SessionTimeout` | 45s | `CONSUMER_SESSION_TIMEOUT` (30s, within the broker's group min/max) | A killed member's partitions move after this; with the instance ID a restart inside it takes them back ([ADR 0012](0012-lifecycle-and-graceful-shutdown.md)) | `member.go`, `config.go` |
| `InstanceID` | none (dynamic membership) | `<CONSUMER_INSTANCE_ID>-<i>` (hostname by default) | Static membership: a restarted process rejoins as the same member without a full rebalance ([ADR 0012](0012-lifecycle-and-graceful-shutdown.md)) | `member.go` |
| `Balancers` | `CooperativeStickyBalancer` | the same (kept, explicit) | Scaling only moves the partitions that change owner | `member.go` |
| Autocommit | every 5s, on polled records | `AutoCommitMarks()` | Only records marked after the Postgres commit are committed | `member.go` |
| `BlockRebalanceOnPoll` | off | on | Rebalance callbacks never run while a poll result is dispatched, so a worker always exists for dispatched records; `AllowRebalance` runs after each poll | `member.go` |
| `OnPartitionsAssigned/Revoked/Lost` | none | start, flush or drop one worker per partition | [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) | `member.go` |
| `PollRecords` size | - | `FLUSH_MAX_RECORDS` | Caps the batch handed to a worker | `member.go` |
| `ClientID` | `kgo` | `<group>-member-<n>` | Per-member series | `member.go` |
| Shutdown | `Close` | `LeaveGroupContext` + explicit static `LeaveGroup` (`leaveStatic`) + `CloseAllowingRebalance` | franz-go sends no `LeaveGroup` for a static member, so we send it ourselves; the partitions then move without waiting for the session timeout ([ADR 0012](0012-lifecycle-and-graceful-shutdown.md)) | `member.go` |
| `RevokeFlushTimeout`, `DrainTimeout` (15s), commit timeout (5s), lag interval (15s) | - | app settings, not Kafka options | Bound the work inside rebalance callbacks and shutdown | `config.go` |
| Heartbeat / rebalance timeouts | 3s / 60s | not set | `RevokeFlushTimeout` must stay well below the rebalance timeout | - |
| Fetch sizes, `FetchMaxWait`, isolation | 50 MiB (1 MiB per partition), 5s, read-uncommitted | not set | Nothing is transactional; see the knobs below | - |

**Broker and topics**

| Setting | Default | Ours | Why | Where |
|---|---|---|---|---|
| Image and mode | - | `apache/kafka:4.3.1`, one node, KRaft `broker,controller` | Smallest runnable setup; fixed `CLUSTER_ID` so the volume survives restarts | `compose.yaml` |
| `auto.create.topics.enable` | `true` | `false` | A typo must fail, not create a topic | `compose.yaml` |
| Partitions | `num.partitions=1` | 6 events, 3 DLQ, set per topic | At most 6 active consumers; the DLQ is low volume | `deploy/kafka/create-topics.sh` |
| `retention.ms` | 7 days (`log.retention.hours=168`) | 7 days events, 14 days DLQ, both explicit | A broker default change cannot alter it; the DLQ outlives the events so a person can inspect it | `create-topics.sh` |
| Topic replication factor | 1 (broker `default.replication.factor=1`) | 1 | Single broker; **local-only** | `create-topics.sh` |
| `offsets.topic.replication.factor` | 3 | 1 | One broker cannot hold 3 replicas; **local-only** | `compose.yaml` |
| `transaction.state.log.replication.factor` / `.min.isr` | 3 / 2 | 1 / 1 | Same; the broker does not start otherwise; **local-only** | `compose.yaml` |
| `min.insync.replicas` | 1 | 1 (not set) | With RF 1, `acks=all` is only as strong as one disk; **local-only** | - |
| `group.initial.rebalance.delay.ms` | 3000 (4.3.1 `DEFAULT_CONFIG`) | 0 | A local group need not wait for more members; faster `up` and tests | `compose.yaml` |
| Listeners | one `PLAINTEXT` | `INTERNAL :9092`, `EXTERNAL :29092` (127.0.0.1 only), `CONTROLLER :9093` | Containers use `kafka:9092`, host tools use `localhost:29092`; **local-only** | `compose.yaml` |
| JVM heap | image default `-Xmx1G -Xms1G` (`kafka-server-start.sh`) | `-Xms512m -Xmx512m`; load test `-Xms768m -Xmx768m` | Heap below the container limit (1 GiB; load test 1.25 GiB, 1.5 CPU) leaves room for page cache and metaspace | `compose.yaml`, `loadtest/compose.loadtest.yaml` |
| `cleanup.policy`, `compression.type`, `max.message.bytes` | `delete`, `producer`, about 1 MB | not set | Defaults fit the small events | - |

### Production profile (not applied locally)

- Replication factor 3 on both topics and on the internal topics.
- `min.insync.replicas=2`, so `acks=all` survives one broker loss without losing acknowledged events.
- `unclean.leader.election.enable=false` (the Kafka default; assert it, do not rely on it).
- TLS plus SASL for clients and between brokers, with per-service ACLs ([ADR 0002](0002-security-baseline-and-threat-model.md)).
- Rack awareness (`broker.rack`, client rack) so replicas span zones.
- Compression: choose a codec by measuring bytes on the wire against CPU in the load test `load` scenario ([ADR 0016](0016-load-and-crash-testing.md)). Until then the franz-go default stays.

### Tuning knobs left at defaults on purpose

| Knob | Change it when |
|---|---|
| Session, heartbeat and rebalance timeouts | Rebalances appear during GC or Postgres stalls, or a revoke flush legitimately exceeds the rebalance timeout |
| Fetch max bytes, max partition bytes, max wait, min bytes | A stress run shows fetch round trips limiting throughput, or fetch buffers dominate consumer memory |
| `MaxBufferedRecords` (50000) | Producer memory grows during a broker outage; lower it to fail fast |
| Record retries, request timeout overhead | Delivery timeout alone does not bound failures in a chaos run |
| Producer batch size | Compression is enabled and batches stay small at the knee of the `stress` scenario |
| Partition count (6) | Lag stays high with all 6 members busy; raising it remaps keys ([above](#partitioning-and-key)) |
| `max.message.bytes`, `cleanup.policy` | Events grow beyond about 100 KB, or the topic needs compaction |

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| `confluentinc/confluent-kafka-go` | Rejected | Needs cgo (`librdkafka`): no static binary, glibc base image, harder cross-compilation, C heap invisible to `pprof`. |
| `segmentio/kafka-go` | Rejected | Pure Go, but less control over partition lifecycle and commits, and slower release cadence. |
| `IBM/sarama` | Rejected | Verbose, channel-heavy API; easy to leak goroutines or deadlock during rebalances. |
| `lovoo/goka` | Rejected | Stream-processing framework on top of sarama; hides the commit and retry control we need. |
| `hibiken/asynq`, `goworker` | Rejected | Redis-backed task queues, not Kafka. |
| Broker auto-create | Rejected | Implicit partitions and retention; typos create topics silently. |
| Topics created by the apps via `kadm` | Rejected | Mixes deploy concerns into runtime and needs admin rights for the app identities ([ADR 0002](0002-security-baseline-and-threat-model.md)). |
| No key (round-robin / sticky) | Rejected | No per-user ordering; the same row is written concurrently from many partitions. |
| 1 partition | Rejected | Defeats the horizontal-scaling bonus. |
| `acks=1` / non-idempotent | Rejected | A leader failure can lose acknowledged events; we promise 202 = durable. |

## Consequences

**Positive**
- Static pure-Go binaries; one client library covers producing, consuming and admin.
- Topic configuration is reviewable code, identical on every `docker compose up`.
- Per-user ordering and no cross-partition row contention, without application locks.
- `202` means the event is replicated per `acks=all`.

**Negative / trade-offs**
- franz-go option names differ from `librdkafka` keys. Mitigation: config mapping documented in `.env.example`.
- Scaling is capped at 6 active consumers. Mitigation: documented; partitions can be raised deliberately.
- A hot user concentrates on one partition. Mitigation: accepted at this scale; lag is visible in metrics.
- RF 1 locally loses data if the broker volume is lost. Mitigation: local-only, documented in the README.

## Implementation rules

- Only `adapters/kafka` (and `cmd/` wiring) MAY import franz-go packages.
- Records to `incoming.user_activity` MUST use `user_id` as the key.
- The producer MUST NOT disable idempotency or lower acks below all ISR.
- Applications MUST NOT create or alter topics; `deploy/kafka/create-topics.sh` is the single source of topic config.
- Topic retention MUST be set explicitly in that script.
- The consumer MUST use `AutoCommitMarks()` and mark records only after the database commit; plain autocommit MUST NOT be used.
- Topic names and the group ID MUST come from config with the defaults above.

## Related

- [ADR 0006](0006-event-contract-and-json-serialization.md) — record value and headers.
- [ADR 0005](0005-ingestion-http-api.md) — synchronous produce from the HTTP handler.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — group members, workers, commits.
- [ADR 0011](0011-failure-handling-retry-and-dlq.md) — what goes to the DLQ topic.
- [ADR 0015](0015-containerization-and-local-deployment.md) — `kafka` and `kafka-init` services.
- [ADR 0001](0001-versions-and-upgrade-policy.md) — franz-go and Kafka versions.

## References

- [1] Apache Kafka 4.3 documentation, "Design". https://kafka.apache.org/43/design/design/. Sections "The Producer" (partition choice by key), "The Consumer" (consumer position, static membership) and "Replication" (ISR, availability and durability guarantees) behind the key, partition count and `acks=all` decisions.
- [2] Apache Kafka 4.3 documentation, configuration reference: producer (https://kafka.apache.org/43/configuration/producer-configs/), consumer (https://kafka.apache.org/43/configuration/consumer-configs/), broker (https://kafka.apache.org/43/configuration/broker-configs/) and topic (https://kafka.apache.org/43/configuration/topic-configs/) configs. The defaults quoted in the "Settings that differ from the defaults" tables: `auto.create.topics.enable`, `group.initial.rebalance.delay.ms`, `offsets.topic.replication.factor`, `retention.ms`, `min.insync.replicas`, `unclean.leader.election.enable`, `session.timeout.ms` and `group.instance.id`.
- [3] franz-go v1.22.1, package `kgo`. https://pkg.go.dev/github.com/twmb/franz-go@v1.22.1/pkg/kgo (release: https://github.com/twmb/franz-go/releases/tag/v1.22.1). The options in the tables and their defaults at this version: `ConsumeResetOffset` (default `RewindOffset(time.Minute)`), `SessionTimeout` (default 45 s), `InstanceID`, `Balancers`, `AutoCommitMarks`, `BlockRebalanceOnPoll`, `AdjustFetchOffsetsFn`, `RecordDeliveryTimeout`, `ProducerLinger` and `AllowIdempotentProduceCancellation` (added in v1.21.0). Companions: `kadm` v1.19.0 (https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm@v1.19.0) and `kprom` v1.5.0 (https://pkg.go.dev/github.com/twmb/franz-go/plugin/kprom@v1.5.0).
- [4] Apache Kafka, KIP-429: Kafka Consumer Incremental Rebalance Protocol. https://cwiki.apache.org/confluence/display/KAFKA/KIP-429%3A+Kafka+Consumer+Incremental+Rebalance+Protocol. The cooperative-sticky balancer, so scaling moves only the partitions that change owner.
- [5] Martin Kleppmann, *Designing Data-Intensive Applications*, 1st ed., O'Reilly Media, 2017, ISBN 978-1-4493-7332-0, ch. 6, "Partitioning", sections "Partitioning of Key-Value Data" (skewed workloads and hot spots) and "Rebalancing Partitions". Hash partitioning by key, hot keys, and why changing the partition count remaps keys.
- [6] Gwen Shapira, Todd Palino, Rajini Sivaram and Krit Petty, *Kafka: The Definitive Guide*, 2nd ed., O'Reilly Media, 2021, ISBN 978-1-4920-4308-9, ch. 3 "Kafka Producers", ch. 4 "Kafka Consumers" and ch. 7 "Reliable Data Delivery". Producer acknowledgements and idempotence, consumer groups and offset commits, and replication settings for durability.
