# ADR 0011: Failure handling: classification, retry, DLQ

- **Status:** Accepted
- **Date:** 2026-10-11
- **Amended:** 2026-10-12 — `FLUSH_TIMEOUT`: a per-attempt deadline, so a dependency that does not answer at all is retried like any other outage.
- **Amended:** 2026-10-12 (review) — control characters in `user_id` / `activity_type` are a validation failure on the consumer too, and SQLSTATE class 22 (data exception) gets its own retry class `data_exception`.
- **Scope:** both

## Context

- The challenge asks us to "handle task retries and failures with a well-defined strategy". The summary (§8) asks that failures "do not silently lose work" and that Kafka/PostgreSQL connection failures and retries be tested.
- The upsert is not idempotent ([ADR 0009](0009-aggregation-data-model.md)). Since 2026-10-12 it runs in the transaction that compare-and-sets the partition's stored position ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)), so retrying a flush whose `COMMIT` actually succeeded is recognized and applies nothing twice. The DLQ publish happens before that transaction and stays at-least-once, so retry boundaries still matter.
- Four mistakes are common here, and this design avoids each:
  - "retry 3× then DLQ" for every error sends valid events to the DLQ during a short database outage;
  - `time.Sleep` backoff ignores shutdown;
  - retries in both the adapters and the consumer loop multiply (3 × 3 = 9 attempts);
  - DLQ headers that carry the raw error text can echo payload data.
- The payload has no stable event ID. The producer-generated `event-id` header exists for tracing and DLQ correlation only ([ADR 0006](0006-event-contract-and-json-serialization.md)).

## Decision

### Classify first, then act

| Class | Examples | Action | Commit? |
|---|---|---|---|
| **Permanent** (per record) | JSON decode error; schema/validation failure (`domain.ErrInvalidEvent`): empty `user_id`, bad timestamp, oversized field, control character (including NUL) in `user_id` or `activity_type` | No retry. Publish the original record to `incoming.user_activity.dlq`; count a metric; log class + coordinates. | Yes, as part of the batch, once the DLQ publish succeeds |
| **Transient** | PostgreSQL SQLSTATE classes `08` (connection), `40` (serialization/deadlock), `53` (insufficient resources), `57` (operator intervention, e.g. `57P01` admin shutdown); `*pgconn.ConnectError`, `pgconn.Timeout`, `net.Error`; retriable Kafka errors (`kerr.IsRetriable`), DLQ publish failure | Retry the whole flush with exponential backoff and full jitter, **without an attempt limit**, while the context is alive. **Never** sent to the DLQ. | No, until it succeeds |
| **Unknown** | Any other error: SQLSTATE `23514` (CHECK violation from a bug), `42501` (missing grant), `42P01` (migrations not applied) | Treated as transient (the partition blocks), but logged at `ERROR` with `class=unknown`, so it raises an alert. A bug or misconfiguration is fixed and redeployed; data is never dropped to get past it. | No |
| **Shutdown** | `ctx.Err() != nil` | Stop retrying. No DLQ. The worker returns; the uncommitted batch is redelivered after restart. | No |
| **Data exception** | PostgreSQL SQLSTATE class `22`: `22021` (NUL or invalid UTF-8 in `text`), `22003` (numeric overflow) | Same as unknown, but wrapped with `ports.ErrDataException` and labelled `error_class=data_exception`. Logged at `ERROR`, retried, **never** dead-lettered. | No |

Permanent errors are always decided **per record at decode time**, before any write. A flush (one DLQ publish plus one upsert transaction) can therefore only fail transiently or unknownly. A database outage never sends valid events to the DLQ.

**Why a data exception is not dead-lettered.** The first line of defense against data PostgreSQL cannot store is domain validation, which both services apply identically (`user_id` and `activity_type`: 1..N bytes of UTF-8, no control characters; the JSON schema carries the same `pattern`). A class 22 error from the upsert therefore means the validation and the schema have drifted apart, which is a bug. The adapter cannot send the offending record to the DLQ: the flush writes one aggregated delta per `(activity_date, user_id)`, so the error names a statement, not a record, and dead-lettering the batch would park good records with the bad one. Per-record isolation (a savepoint per row, then rejecting only the failing keys) would need the raw records of each delta and is out of proportion for a defense in depth. A bounded retry has the same flaw, since giving up means either dropping the batch or skipping its offsets. So the behavior is the safe one: the error is wrapped with `ports.ErrDataException`, retried with the normal backoff, counted as `retries_total{error_class="data_exception"}` and logged at `ERROR`, so it alerts apart from `unknown`. The partition blocks and no data is lost; the fix is to correct the validation rule and redeploy, after which the blocked batch fails validation and is dead-lettered.

### One retry layer: the partition worker

- The Postgres and Kafka adapters **do not retry**. They translate driver errors into classes: transient errors are wrapped with `ports.ErrUnavailable` (`fmt.Errorf("%w: %w", ports.ErrUnavailable, err)`), and everything else is returned wrapped but unclassified. The only exception is franz-go's own internal request retries, which happen inside the client and are bounded by its timeouts.
- The partition worker ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)) retries the **whole flush**: DLQ publish first, then the transaction. A transaction is all or nothing, so retrying it is safe, except for the rare lost-`COMMIT`-reply case that ADR 0008 documents.
- Policy: base `RETRY_BASE_DELAY` = 100 ms, cap `RETRY_MAX_DELAY` = 10 s, full jitter (`rand.N(min(base·2ⁿ, cap) + 1)`).
- While a worker retries, its queue fills, and the poll loop pauses that partition with `PauseFetchPartitions`. Other partitions keep flowing. franz-go heartbeats from a background goroutine, so a paused partition does not lose the group session. Because the poll loop never blocks and always calls `AllowRebalance`, a long outage does not hold up rebalances. On revoke, a retrying worker gets `REVOKE_FLUSH_TIMEOUT`, then gives up its batch.
- Every retry increments a retry counter labelled with the class and logs at `WARN` (rate-limited: the first attempt, then every 10th). The metric names are in [ADR 0013](0013-observability.md).
- The offset commit is **not** part of the retried flush. If `CommitMarkedOffsets` fails after the transaction committed, we log it and count it. The `AutoCommitMarks` background commit retries it, and the database write is never repeated for a commit failure.

### Attempt deadline: `FLUSH_TIMEOUT`

*Amended 2026-10-12.* `crash-postgres-pause` ([ADR 0016](0016-load-and-crash-testing.md)) froze PostgreSQL for 30 s and the consumer reported **0** retries: every flush simply hung. A frozen server (`docker pause`, a VM stall, a hung storage layer) never answers, so it enforces no `statement_timeout`. Its kernel still accepts connections and acknowledges TCP keepalives, so keepalives never declare the connection dead. Without a client-side deadline, an attempt waits as long as the worker lives.

- Every flush attempt (DLQ publish + transaction) runs under its own deadline, `FLUSH_TIMEOUT` (default **10 s**). The deadline is a context deadline (`context.WithTimeoutCause`), so it covers every step: acquiring a pool connection, dialling a new one, each statement and the `COMMIT`. pgx enforces it by setting a deadline on the socket (its default `DeadlineContextWatcherHandler`) and closes the connection when it fires.
- A deadline hit is a **transient** error: the worker wraps it as "attempt exceeded FLUSH_TIMEOUT", retries with the normal backoff, counts it in `retries_total{dependency="postgres"|"kafka",error_class="transient"}` and logs it at `WARN`. Only the cancellation of the worker's own context (shutdown, revoke deadline) stops the retries, as before.
- The position load of a new assignment and of a fence resync ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)) uses the same per-attempt deadline inside its `REVOKE_FLUSH_TIMEOUT` budget.
- `FLUSH_TIMEOUT` MUST be at least `DATABASE_STATEMENT_TIMEOUT` (validated at startup). A slow but alive server then fails a statement with a clean SQLSTATE `57014` first; the client deadline only ends attempts against a server that does not answer at all.
- **Correctness.** The deadline can fire after the `COMMIT` reached the server (the reply is lost, or the frozen server executes the buffered `COMMIT` when it thaws). That is the lost-reply case of ADR 0008: the retry's compare-and-set either waits for the old transaction's row lock and then finds the stored position at `Checkpoint.To`, or finds it there at once, and returns success without writing. If the old transaction never commits, the server rolls it back when it sees the closed connection. Either way the batch is applied once. The DLQ publish of an abandoned attempt may be repeated, which the DLQ already tolerates (at-least-once).
- No transaction-local `SET LOCAL statement_timeout` / `lock_timeout`: they would add a round trip per flush and, like any server-side timeout, cannot fire on a frozen server. Lock waits on `consumer_offsets` only happen between two owners of one partition and are bounded by `DATABASE_STATEMENT_TIMEOUT`.

The backoff helper (one copy per module, in `internal/platform/retry`; no shared Go code between services):

```go
package retry

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

type Policy struct{ Base, Max time.Duration }

// Do runs op until it succeeds, op's error is not retryable, or ctx is done.
func Do(ctx context.Context, p Policy, retryable func(error) bool, onRetry func(attempt int, wait time.Duration, err error), op func(context.Context) error) error {
	for attempt := 0; ; attempt++ {
		err := op(ctx)
		if err == nil || !retryable(err) {
			return err
		}
		ceil := p.Max
		if attempt < 30 {
			ceil = min(p.Base<<attempt, p.Max)
		}
		wait := rand.N(ceil + 1) // full jitter: uniform in [0, ceil]
		onRetry(attempt+1, wait, err)
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("retry aborted after %d attempts: %w (last error: %w)", attempt+1, ctx.Err(), err)
		case <-t.C:
		}
	}
}
```

The worker's `retryable` returns `true` for every error except `context.Canceled`/`context.DeadlineExceeded` from its own context and `ports.ErrFenced` ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). A `FLUSH_TIMEOUT` deadline belongs to the attempt, not to the worker, so it is retried. Classification only picks the log level and metric label (`transient` for `ports.ErrUnavailable` and attempt timeouts, `data_exception` for `ports.ErrDataException`, `unknown` otherwise).

PostgreSQL classification in `adapters/postgres`:

```go
package postgres

import (
	"errors"
	"net"

	"github.com/jackc/pgx/v5/pgconn"
)

func transient(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code[:2] {
		case "08", "40", "53", "57":
			return true
		}
		return false // a server-reported error outside these classes is "unknown"
	}
	var connErr *pgconn.ConnectError
	var netErr net.Error
	return errors.As(err, &connErr) || errors.As(err, &netErr) || pgconn.Timeout(err)
}
```

### Dead-letter queue

- Topic `incoming.user_activity.dlq` ([ADR 0007](0007-kafka-client-and-topic-topology.md)). The record **key and value are the original bytes**, so the event can be replayed unchanged. The original headers (`event-id`, `schema-version`, and `traceparent` / `tracestate` when present) are copied unchanged, so a dead-lettered record still joins the trace of the request that produced it ([ADR 0013](0013-observability.md)).
- Added headers, with no PII and no free-text error message:

| Header | Value |
|---|---|
| `x-error-class` | `decode` or `validation` |
| `x-original-topic` | `incoming.user_activity` |
| `x-original-partition` | partition number, decimal |
| `x-original-offset` | offset, decimal |
| `x-failed-at` | RFC 3339 UTC time of the DLQ publish |

- Decode errors from `encoding/json` can quote input bytes, so `err.Error()` is never put in a header and never logged as-is. Logs carry the class, the coordinates, `event-id` and, when the record has a valid `traceparent`, its `trace_id` only ([ADR 0013](0013-observability.md), [ADR 0002](0002-security-baseline-and-threat-model.md)); `tracestate` is never logged.
- **DLQ duplicates are acceptable.** A retried flush or a crash before the offset commit republishes the same invalid record. `(x-original-topic, x-original-partition, x-original-offset)` identifies it uniquely, and `event-id` correlates it with producer logs.
- The DLQ holds raw payloads (with `metadata`), so it gets the same protection as the main topic. Its retention is explicit (14 days, [ADR 0007](0007-kafka-client-and-topic-topology.md)).
- **Replay is out of scope.** It would be a manual tool: read the DLQ, fix or filter, and republish to the main topic. It is documented, not built.

### Producer side

- `RegisterActivity` calls `ActivityPublisher.Publish` once, under a context bounded by `PUBLISH_TIMEOUT` (default 5 s). The adapter uses `ProduceSync`. franz-go retries internally (idempotent producer, `acks=all`), so those retries do not duplicate records in Kafka.
- On failure or timeout, the handler returns **503** with `Retry-After: 1` and never retries itself ([ADR 0005](0005-ingestion-http-api.md)). Retrying is the client's decision. A client retry after an ack that was lost in transit can produce a duplicate event: at-least-once again, documented in [ADR 0000](0000-scope-interpretation-and-assumptions.md).

### Startup dependencies

Both services retry their dependencies at startup with the same `retry.Do`: Postgres `Ping` (consumer) and Kafka `Ping` (both). The context is bounded by `STARTUP_TIMEOUT` (default 60 s). When it expires, `run` returns an error, the process exits 1 and the Compose restart policy takes over ([ADR 0015](0015-containerization-and-local-deployment.md)). Readiness stays false until the dependencies answer.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| Classified errors, unlimited transient retry, DLQ only for permanent (this ADR) | Chosen | Valid data is never parked, and only bad data is. |
| Retry N times, then DLQ for any error | Rejected | A 30 s DB outage dead-letters valid events and needs a manual replay to fix counts. |
| Retry topics with delays (`.retry-5s`, `.retry-1m`) | Rejected | Reorders a user's events, adds topics and consumers, and solves a problem that pausing one partition already solves here. |
| Retries in adapters and in the worker | Rejected | Retries multiply and break the backoff budget; double writes become more likely. |
| `time.Sleep` backoff | Rejected | Ignores cancellation, so shutdown waits for the full sleep. |
| `cenkalti/backoff` or similar | Rejected | About 20 lines of stdlib cover our single policy. |
| Circuit breaker around Postgres | Deferred | One dependency, one retry site, and pausing already sheds load. Revisit if more callers share the pool. |
| Skip (commit without DLQ) on permanent errors | Rejected | Silently loses data, which the summary forbids. |

## Consequences

**Positive**
- No valid event is lost or dead-lettered because a dependency is down. The consumer catches up automatically when it recovers.
- Shutdown is prompt even mid-backoff.
- A failure affects one partition, not the whole consumer.
- The DLQ is safe to share: no PII in headers, and it is replayable byte for byte.

**Negative / trade-offs**
- An outage longer than the Kafka retention (7 days) would lose events. Mitigation: lag and retry metrics plus `ERROR` logs make an outage visible long before that.
- An `unknown` error (for example, a missing grant) blocks a partition until someone fixes it. Mitigation: intentional, since data is kept; it alerts via `ERROR` logs and metrics.
- The DLQ can contain duplicates. Mitigation: unique coordinates in the headers.

## Implementation rules

- Adapters MUST NOT retry. The only retry loop for message processing MUST be the partition worker's `retry.Do`.
- Backoff waits MUST `select` on `ctx.Done()` and a timer. `time.Sleep` MUST NOT be used for retries.
- Transient and unknown errors MUST NOT be sent to the DLQ. Permanent errors MUST NOT be retried.
- Offsets MUST NOT be committed for a record until its batch is in PostgreSQL or the record is in the DLQ.
- DLQ headers MUST be exactly the set above plus copied original headers. Error strings, payload fragments and `metadata` MUST NOT appear in headers or logs.
- A DLQ publish failure MUST be treated as transient.
- The consumer's domain validation MUST match the producer's for every field PostgreSQL stores (today `user_id`), and `contracts/user_activity.v1.schema.json` MUST carry the same rule. A PostgreSQL data exception (SQLSTATE class 22) MUST NOT dead-letter a batch.
- Every flush attempt MUST run under the `FLUSH_TIMEOUT` deadline, and a deadline hit MUST be retried as a transient error. `FLUSH_TIMEOUT` MUST be validated `>= DATABASE_STATEMENT_TIMEOUT`.
- The producer handler MUST NOT loop on publish; it MUST map publish failure or timeout to 503.
- Tests MUST cover: a decode error goes to the DLQ and its offset is committed; a DB outage retries with no DLQ and no commit, then recovers; shutdown during backoff returns promptly; DLQ publish failure causes no commit; startup retry gives up after `STARTUP_TIMEOUT`; an attempt against a frozen server ends at the deadline with a transient error and the retry succeeds once it thaws.

## Related

- [ADR 0006](0006-event-contract-and-json-serialization.md) — `event-id` header; decode rules.
- [ADR 0007](0007-kafka-client-and-topic-topology.md) — DLQ topic and retention.
- [ADR 0005](0005-ingestion-http-api.md) — 503 mapping on the producer.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — the worker, pausing, stored positions and fencing (`ports.ErrFenced` is never retried).
- [ADR 0010](0010-postgresql-access-and-migrations.md) — where pgx errors are translated.
- [ADR 0013](0013-observability.md) — retry/DLQ metrics and log fields.
- [ADR 0002](0002-security-baseline-and-threat-model.md) — what may appear in logs and headers.

## References

- [1] Gregor Hohpe and Bobby Woolf, *Enterprise Integration Patterns*, Addison-Wesley, 2003, ISBN 978-0-321-20068-6, patterns "Dead Letter Channel" (https://www.enterpriseintegrationpatterns.com/patterns/messaging/DeadLetterChannel.html), "Invalid Message Channel" (https://www.enterpriseintegrationpatterns.com/patterns/messaging/InvalidMessageChannel.html), "Guaranteed Delivery" (https://www.enterpriseintegrationpatterns.com/patterns/messaging/GuaranteedMessaging.html) and "Idempotent Receiver" (https://www.enterpriseintegrationpatterns.com/patterns/messaging/IdempotentReceiver.html). Parking messages that can never be processed, and tolerating redelivery.
- [2] Marc Brooker, "Exponential Backoff And Jitter", AWS Architecture Blog, 4 March 2015. https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/. The "Full Jitter" algorithm used by `retry.Do`.
- [3] Betsy Beyer, Chris Jones, Jennifer Petoff and Niall Richard Murphy (eds.), *Site Reliability Engineering*, O'Reilly Media, 2016, ISBN 978-1-4919-2912-4. Ch. 21, "Handling Overload" (https://sre.google/sre-book/handling-overload/): requests should be retried only at the layer immediately above the one rejecting them. Ch. 22, "Addressing Cascading Failures" (https://sre.google/sre-book/addressing-cascading-failures/): naive retries amplify load. The single retry layer.
- [4] Michael T. Nygard, *Release It!*, 2nd ed., Pragmatic Bookshelf, 2018, ISBN 978-1-68050-239-8, Part "Create Stability", chapter "Stability Patterns" (timeouts, circuit breaker, fail fast). https://pragprog.com/titles/mnee2/release-it-second-edition/. The per-attempt deadline (`FLUSH_TIMEOUT`) and the deferred circuit breaker.
- [5] PostgreSQL 18 documentation, "PostgreSQL Error Codes". https://www.postgresql.org/docs/18/errcodes-appendix.html. SQLSTATE classes `08`, `22` (`22003`, `22021`), `40`, `53` and `57`, and `23514`, `42501`, `42P01` as used in the classification table.
- [6] pgx v5.11.0, package `pgconn`. https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0/pgconn. `PgError`, `ConnectError` and `Timeout` in the classifier, and the context-deadline handling that closes a connection when `FLUSH_TIMEOUT` fires.
- [7] franz-go v1.22.1, package `kerr`. https://pkg.go.dev/github.com/twmb/franz-go@v1.22.1/pkg/kerr. `IsRetriable` for Kafka errors.
- [8] The Go Project, Go 1.21 release notes (https://go.dev/doc/go1.21, `context.WithTimeoutCause`) and Go 1.22 release notes (https://go.dev/doc/go1.22, `math/rand/v2`). The deadline-with-cause and the jitter source in the backoff helper.
- [9] R. Fielding, M. Nottingham, J. Reschke (eds.), *RFC 9110: HTTP Semantics*, IETF, June 2022, sections 15.6.4 and 10.2.3. https://www.rfc-editor.org/rfc/rfc9110. The 503 and `Retry-After` response of the producer side.
