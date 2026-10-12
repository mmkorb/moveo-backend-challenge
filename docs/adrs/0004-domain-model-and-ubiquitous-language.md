# ADR 0004: Domain model and ubiquitous language (lightweight tactical DDD)

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** both

## Context

- [ADR 0003](0003-system-decomposition-and-layout.md) gives each service a hexagonal layout. Hexagonal architecture says **where** code goes; it doesn't say how to model the problem. DDD covers that, and the two are complementary.
- The problem is small. Events come in, and per-user daily counters go out. A full DDD treatment (bounded contexts, aggregates, domain events) would be ceremony.
- The vocabulary is easy to get wrong: "aggregate" means a SQL roll-up here, not a DDD Aggregate. Generic names (`Data`, `Processor`, `Manager`) hide intent.
- The domain event type carries no `json` tags. The Kafka message format is an **integration contract** ([ADR 0006](0006-event-contract-and-json-serialization.md)), and it changes for different reasons than the domain model does.
- Several consumers may update the same `(activity_date, user_id)` row. Concurrency safety has to come from the model.

## Decision

We adopt **lightweight tactical DDD**: a ubiquitous language, explicit domain types with validating constructors, use cases in `application`, and dependency inversion through ports.

### Glossary (same names in code, tests, docs and logs)

| Term | Kind | Meaning |
|---|---|---|
| `ActivityEvent` | domain type (both services) | One user action: `UserID`, `ActivityType`, `Timestamp` (UTC), `Metadata`. Valid by construction (`NewActivityEvent`). |
| `ActivityType` | domain type | A string type. One known constant: `ActivityPageView = "page_view"`. Any other non-empty value is valid. |
| `Metadata` | field | Free-form `map[string]string` that is passed through unchanged and never aggregated. |
| activity date | derived value | The UTC calendar date of `Timestamp`. |
| `DailyUserAggregate` | domain value (consumer) | Metrics for one user on one UTC date: `ActivityCount`, `PageViewCount`. In the code it is a **delta** to be added, not a stored total. |
| `RegisterActivity` | use case (producer) | Accepts a valid `ActivityEvent` and publishes it via `ActivityPublisher`. |
| `ProcessActivity` | use case (consumer) | Folds a batch of `ActivityEvent`s into `DailyUserAggregate` deltas and persists them via `AggregateRepository`. Invalid events go to `DeadLetterPublisher`. |
| `ActivityPublisher` | port (producer) | Publishes an `ActivityEvent` to Kafka. |
| `AggregateRepository` | port (consumer) | Atomically adds deltas to `activity_aggregates`. |
| `DeadLetterPublisher` | port (consumer) | Parks a record that can never be processed ([ADR 0011](0011-failure-handling-retry-and-dlq.md)). |
| `DeadLetter` | port type (consumer) | A rejected record: original key/value bytes, partition/offset coordinates and error class. The Kafka adapter builds it when decoding or validation fails, so the application never sees JSON ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). |

Banned as type or package names: `Data`, `Info`, `Processor`, `Manager`, `Helper`, `Utils`, `Common`.

### Domain rules

1. Every **accepted** event adds 1 to `activity_count`.
2. An event with `ActivityType == "page_view"` also adds 1 to `page_view_count`. All other types add 0.
3. The activity date is `Timestamp.UTC()` truncated to the calendar day.
4. **Unknown activity types are accepted** and counted in `activity_count` only. The domain has no closed list of types.
5. `user_id` and `activity_type` are non-empty and length-bounded. `Timestamp` is non-zero. `Metadata` has a bounded size. The exact limits are in [ADR 0005](0005-ingestion-http-api.md).
6. Consequently `0 ≤ page_view_count ≤ activity_count`, matching the table's CHECK constraints ([ADR 0009](0009-aggregation-data-model.md)).

### No read-modify-write

The domain produces **deltas**, never new totals. The repository applies them with one atomic statement:

```sql
INSERT … ON CONFLICT (activity_date, user_id) DO UPDATE
SET activity_count = activity_aggregates.activity_count + EXCLUDED.activity_count, …
```

Two consumers adding to the same row therefore can't overwrite each other's increments. The full statement is in [ADR 0009](0009-aggregation-data-model.md).

### Domain types (consumer module; the producer has the same event types but no `DailyUserAggregate`)

```go
package domain

import (
	"errors"
	"fmt"
	"maps"
	"time"
	"unicode/utf8"
)

// ActivityType is open-ended: only page_view has special meaning.
type ActivityType string

const ActivityPageView ActivityType = "page_view"

const (
	MaxUserIDBytes       = 128
	MaxActivityTypeBytes = 64
	MaxMetadataEntries   = 32
	MaxMetadataKeyBytes  = 64
	MaxMetadataValBytes  = 2048
)

var ErrInvalidEvent = errors.New("invalid activity event")

type ActivityEvent struct {
	UserID       string
	ActivityType ActivityType
	Timestamp    time.Time // always UTC
	Metadata     map[string]string
}

func NewActivityEvent(userID string, typ ActivityType, ts time.Time, md map[string]string) (ActivityEvent, error) {
	if !boundedText(userID, MaxUserIDBytes) {
		return ActivityEvent{}, fmt.Errorf("%w: user_id must be 1..%d bytes of UTF-8", ErrInvalidEvent, MaxUserIDBytes)
	}
	if !boundedText(string(typ), MaxActivityTypeBytes) {
		return ActivityEvent{}, fmt.Errorf("%w: activity_type must be 1..%d bytes of UTF-8", ErrInvalidEvent, MaxActivityTypeBytes)
	}
	if ts.IsZero() {
		return ActivityEvent{}, fmt.Errorf("%w: timestamp is required", ErrInvalidEvent)
	}
	if len(md) > MaxMetadataEntries {
		return ActivityEvent{}, fmt.Errorf("%w: at most %d metadata entries", ErrInvalidEvent, MaxMetadataEntries)
	}
	for k, v := range md {
		if !boundedText(k, MaxMetadataKeyBytes) || len(v) > MaxMetadataValBytes || !utf8.ValidString(v) {
			return ActivityEvent{}, fmt.Errorf("%w: metadata entry out of bounds", ErrInvalidEvent)
		}
	}
	return ActivityEvent{UserID: userID, ActivityType: typ, Timestamp: ts.UTC(), Metadata: maps.Clone(md)}, nil
}

// ActivityDate is the UTC calendar day of the event, at 00:00 UTC.
func (e ActivityEvent) ActivityDate() time.Time {
	y, m, d := e.Timestamp.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func boundedText(s string, maxBytes int) bool {
	return s != "" && len(s) <= maxBytes && utf8.ValidString(s)
}

// DailyUserAggregate is a delta for one (ActivityDate, UserID). It is a value,
// not a DDD Aggregate: Postgres, not this type, guards consistency.
type DailyUserAggregate struct {
	ActivityDate  time.Time
	UserID        string
	ActivityCount int64
	PageViewCount int64
}

var ErrKeyMismatch = errors.New("event does not belong to this daily aggregate")

// Apply adds one accepted event to the delta (domain rules 1 and 2).
func (a *DailyUserAggregate) Apply(e ActivityEvent) error {
	if !a.ActivityDate.Equal(e.ActivityDate()) || a.UserID != e.UserID {
		return ErrKeyMismatch
	}
	a.ActivityCount++
	if e.ActivityType == ActivityPageView {
		a.PageViewCount++
	}
	return nil
}
```

`ProcessActivity` keeps a `map` keyed by `(ActivityDate, UserID)`, calls `Apply` for each event, and hands the resulting deltas to `AggregateRepository` in one call ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).

### What we do NOT adopt

- **Strategic DDD** (bounded contexts, context maps, anti-corruption layers). This is **one analytics context**. Two deployables do not make two bounded contexts; they are two processes inside the same model, and they share the contract, not a model translation.
- **DDD Aggregates and repositories of entities.** There is no entity with identity and lifecycle to protect. `DailyUserAggregate` is named after the data-aggregation sense, as the summary uses it, and the naming caveat is documented here.
- **Internal domain events and event sourcing.** The Kafka message is an integration event, defined by the contract.
- **Struct tags in `domain`.** The JSON DTOs live in `adapters/kafka` (both services) and `adapters/httpapi` (producer), and they map DTO ⇄ domain.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| Anemic structs with `json` tags used everywhere | Rejected | It couples the wire contract to the core; any contract change ripples through the use cases. |
| Closed enum of activity types (reject unknown) | Rejected | It drops valid data whenever a client adds a type; only `page_view` matters for the metrics. |
| Full tactical + strategic DDD | Rejected | Ceremony without a consistency boundary to protect. |
| Read current totals, add in Go, write back | Rejected | Lost updates under concurrent consumers, and an extra round-trip. |
| Rename `DailyUserAggregate` to avoid the DDD clash | Rejected | Our summary and the SQL use "aggregate"; one clear caveat is cheaper than two vocabularies. |

## Consequences

**Positive**
- A reviewer can map a word in the README straight to a type, a test and a column.
- Domain rules are pure functions, so they're unit-tested without fakes or I/O.
- Atomic deltas make concurrent consumers safe without locks.

**Negative / trade-offs**
- `ActivityEvent` is defined in both modules. Mitigation: contract tests against `contracts/` ([ADR 0006](0006-event-contract-and-json-serialization.md)).
- DTO ⇄ domain mapping is boilerplate. Mitigation: one small function per adapter, covered by tests.
- Accepting any `activity_type` lets typos create new "types". Mitigation: length bound, and types are visible in DLQ/metrics; a closed list can be added later.

## Implementation rules

- Code, tests and docs MUST use the glossary names; banned names MUST NOT be introduced.
- `ActivityEvent` MUST be created only through `NewActivityEvent`; adapters MUST NOT build it with a struct literal.
- `domain` types MUST NOT have struct tags; DTOs MUST live in adapters.
- The producer MUST NOT reject unknown activity types; only `page_view` MAY be special-cased.
- Dates MUST be derived with `ActivityDate()` (UTC); `time.Local` MUST NOT be used.
- Persistence MUST apply deltas with an atomic `ON CONFLICT … DO UPDATE SET x = x + EXCLUDED.x`; read-modify-write MUST NOT be used.
- Domain rules MUST have unit tests (UTC day boundaries, page_view vs other types, invalid input); adapters MUST have integration tests.

## Related

- [ADR 0003](0003-system-decomposition-and-layout.md) — layers and import direction.
- [ADR 0006](0006-event-contract-and-json-serialization.md) — the wire DTO that maps to `ActivityEvent`.
- [ADR 0005](0005-ingestion-http-api.md) — validation limits enforced at the edge.
- [ADR 0009](0009-aggregation-data-model.md) — the table and upsert that apply deltas.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — how batches are folded into deltas.

## References

- [1] Eric Evans, *Domain-Driven Design Reference: Definitions and Pattern Summaries*, Domain Language, Inc., March 2015 (CC BY 4.0). https://www.domainlanguage.com/ddd/reference/. The definitions of ubiquitous language, value object, aggregate, repository and bounded context used to decide what is and is not adopted.
- [2] Martin Fowler, "Bounded Context" (https://martinfowler.com/bliki/BoundedContext.html) and "Anemic Domain Model" (https://martinfowler.com/bliki/AnemicDomainModel.html). Why two deployables are not two bounded contexts, and the cost of tag-carrying anemic structs.
- [3] Alistair Cockburn, "Hexagonal architecture". https://alistair.cockburn.us/hexagonal-architecture/. Hexagonal architecture says where code goes; the domain model is a separate question.
- [4] PostgreSQL 18 documentation, "INSERT", clause `ON CONFLICT`. https://www.postgresql.org/docs/18/sql-insert.html. `ON CONFLICT DO UPDATE` guarantees an atomic insert-or-update outcome under concurrency, which is why deltas need no read-modify-write.
- [5] Martin Kleppmann, *Designing Data-Intensive Applications*, 1st ed., O'Reilly Media, 2017, ISBN 978-1-4493-7332-0, ch. 7, "Transactions", section "Preventing Lost Updates" (atomic write operations). The lost-update problem of read-then-write.
