# ADR 0006: Event contract and JSON serialization

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** both

## Context

- The challenge defines the message on `incoming.user_activity` by example, as JSON:

  ```json
  {
    "user_id": "12345",
    "activity_type": "page_view",
    "timestamp": "2024-07-01T12:34:56Z",
    "metadata": {
      "page_url": "https://example.com/home",
      "referrer": "https://google.com"
    }
  }
  ```

  (The `<…>` around the URLs in the original document is a formatting artifact; see [ADR 0000](0000-scope-interpretation-and-assumptions.md).)
- The producer and the consumer are independent Go modules with no shared code ([ADR 0003](0003-system-decomposition-and-layout.md)). They still have to agree on the format, and drift must be caught by tests, not in production.
- The payload has no event identifier. No component deduplicates by an event ID, so the contract does not claim UUID-based deduplication.
- Payloads are about 200 bytes. Throughput is bound by Kafka and Postgres I/O, not by JSON CPU time.
- Since Go 1.27, the stdlib `encoding/json` is implemented on top of the `encoding/json/v2` engine, keeping v1 semantics. The usual objection that "json/v2 is experimental" no longer applies.

## Decision

### Payload

- The Kafka record **value** is the challenge's JSON object, UTF-8 encoded, with exactly the fields `user_id`, `activity_type`, `timestamp`, `metadata`.
- `timestamp` is RFC 3339. The producer normalizes it to UTC before publishing (`…Z`).
- `metadata` is a JSON object with **string values only** (`map[string]string`), optional, passed through and not aggregated. The size limits are in [ADR 0005](0005-ingestion-http-api.md).
- The record **key** is `user_id` ([ADR 0007](0007-kafka-client-and-topic-topology.md)).

### Contract artifact

`contracts/user_activity.v1.schema.json` (JSON Schema 2020-12) is the **only shared artifact** between the modules:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "user_activity.v1.schema.json",
  "type": "object",
  "required": ["user_id", "activity_type", "timestamp"],
  "additionalProperties": false,
  "properties": {
    "user_id":       { "type": "string", "minLength": 1, "maxLength": 128, "pattern": "^[^\\u0000-\\u001F\\u007F-\\u009F]+$" },
    "activity_type": { "type": "string", "minLength": 1, "maxLength": 64, "pattern": "^[^\\u0000-\\u001F\\u007F-\\u009F]+$" },
    "timestamp":     { "type": "string", "format": "date-time" },
    "metadata": {
      "type": "object",
      "maxProperties": 32,
      "propertyNames": { "minLength": 1, "maxLength": 64 },
      "additionalProperties": { "type": "string", "maxLength": 2048 }
    }
  }
}
```

(`maxLength` in JSON Schema counts characters, while the Go limits count bytes, so Go is the stricter of the two for non-ASCII input. The `pattern` rejects Unicode control characters (category Cc) in `user_id` and `activity_type`. Both services' domains enforce the same rule with `unicode.IsControl`, so a record containing a NUL byte is dead-lettered as `validation` instead of failing the PostgreSQL upsert forever ([ADR 0011](0011-failure-handling-retry-and-dlq.md)). Metadata keys and values are not restricted this way, because PostgreSQL never stores them.)

Each service keeps its own wire DTO in `internal/adapters/kafka` and maps it to and from `domain.ActivityEvent`:

```go
package kafka

import "time"

// activityMessage mirrors contracts/user_activity.v1.schema.json.
type activityMessage struct {
	UserID       string            `json:"user_id"`
	ActivityType string            `json:"activity_type"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}
```

A **contract test** in each module reads `../../contracts/user_activity.v1.schema.json` and the golden example files next to it. It asserts that:
- the producer's encoded DTO validates against the schema;
- the consumer decodes every valid example and rejects every invalid one.

The schema validator is a test-only dependency (see [ADR 0014](0014-testing-strategy-and-quality-tooling.md)).

### Kafka headers

The producer adds these headers to every record:

| Header | Value | Purpose |
|---|---|---|
| `event-id` | UUIDv7 string from the stdlib `uuid` package (`uuid.NewV7()`, Go 1.27) | Tracing: the same ID is returned to the client as `event_id` (202 body), logged by both services, and copied into DLQ records. |
| `schema-version` | `1` | Lets the consumer choose a decoder; enables additive evolution. |
| `traceparent` | W3C Trace Context level 1: `00-<trace-id>-<span-id>-<flags>` (32 / 16 lowercase hex, flags `00` or `01`). The trace ID is the caller's, or a new one; the span ID is the producer's own for this hop. | Joins the logs of every service that touched the event under one `trace_id` ([ADR 0013](0013-observability.md)). |
| `tracestate` | The caller's opaque value, unchanged. Present only when the request carried a valid `traceparent` and a `tracestate` of at most 512 bytes. | Vendor data of upstream tracing systems; we never read it. |

The two trace headers follow [W3C Trace Context](https://www.w3.org/TR/trace-context/) and are transport metadata only: they are not part of the JSON payload, `contracts/user_activity.v1.schema.json` is unchanged, and `schema-version` stays `1`. The producer builds them in `internal/platform/tracectx` ([ADR 0005](0005-ingestion-http-api.md) describes how the request supplies them); a record without `traceparent` is valid.

The consumer parses `traceparent` (with its own copy of `internal/platform/tracectx`) only for records it rejects, and puts the `trace_id` into the log lines about that record: `record rejected` (decode or validation failure, `debug`) and `record dead-lettered` (DLQ publish, `warn`). If several `traceparent` headers are present, the first one is used. It does not log a trace ID on hot-path success lines, and batch-level lines (flush) carry none, because a batch mixes traces. A missing or malformed `traceparent` is **never** an error: the consumer logs without a `trace_id` and processes the record normally. DLQ records carry the original `traceparent` and `tracestate` unchanged ([ADR 0011](0011-failure-handling-retry-and-dlq.md)).

**`event-id` is for tracing and DLQ correlation only. It is NOT used for deduplication.** The producer generates it, so a client retry gets a new ID, and the consumer stores no IDs. The consumer applies each Kafka record once (positions stored with the aggregates, [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)), but a client retry is a new record and is counted again. Real dedupe would need a client-supplied idempotency key plus state; that is a documented future option.

The consumer treats a missing `schema-version` as `1`. An unknown version, or a value that fails to decode or validate, is a **permanent** error and goes to the DLQ ([ADR 0011](0011-failure-handling-retry-and-dlq.md)).

### Codec

We use stdlib **`encoding/json`** (v1 API, v2 engine since Go 1.27) in both services:
- The HTTP edge decodes with `json.Decoder` + `DisallowUnknownFields()` and checks there is no trailing data ([ADR 0005](0005-ingestion-http-api.md)).
- The consumer decodes with `json.Unmarshal` into the DTO and **ignores unknown fields** (tolerant reader), so the producer can add optional fields first.
- We don't use the `encoding/json/v2` API directly yet. Its stricter defaults (case-sensitive names, duplicate-key rejection) are attractive, so moving to it is a cheap later change confined to adapters.

### Evolution rules

1. **Additive only within v1:** a new optional field or a new metadata key. Consumers ignore unknown fields.
2. Renaming or removing a field, changing a type, or changing a field's meaning is **breaking**. It requires `user_activity.v2.schema.json`, `schema-version: 2`, and a consumer that understands both versions before any producer emits v2.
3. A change to the **key** or to partitioning semantics requires a new topic (e.g. `incoming.user_activity.v2`), because ordering guarantees change.
4. Every contract change updates the schema, the golden examples and both DTOs in the same pull request.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| Protobuf | Rejected | Needs `protoc`/`buf` codegen in both builds for one 4-field message, and the input is already JSON from HTTP. |
| Avro + Confluent Schema Registry | Rejected | Adds a registry container and a client for a single static schema. |
| Shared Go package with the DTO | Rejected | Couples the two modules' releases; the schema plus contract tests gives the same safety ([ADR 0003](0003-system-decomposition-and-layout.md)). |
| `bytedance/sonic` | Rejected | JIT + `unsafe`, limited to amd64/arm64; a CPU optimization for a workload that is I/O-bound. |
| `segmentio/encoding` | Rejected | Low maintenance activity; no measurable gain for ~200 B payloads. |
| `goccy/go-json` | Rejected | YAGNI until a profile shows JSON in the hot path; extra supply-chain surface. |
| `encoding/json/v2` API now | Deferred | Stable in Go 1.27, but the v1 API is familiar to reviewers, and switching later only touches adapters. |
| Typed metadata struct (`page_url`, `referrer`) | Rejected | The challenge only shows examples; a free-form map keeps it as passthrough data. |
| `event-id` used for dedupe | Rejected | Producer-generated IDs don't survive client retries, and storing IDs breaks the one-table rule. |
| `google/uuid` | Rejected | The stdlib `uuid` package (Go 1.27) covers v7. |

## Consequences

**Positive**
- The wire format is exactly the challenge's example, readable with any Kafka tool.
- No codegen, no registry, no third-party codec; the JSON path is fully stdlib.
- The contract is explicit and versioned, and drift fails the tests in either module.
- `event-id` lets a reviewer follow one request from the HTTP response to the logs to the DLQ.

**Negative / trade-offs**
- JSON is bigger and slower than binary formats. Mitigation: irrelevant at this scale; the triggers to reconsider are a CPU profile with JSON in the top frames or sustained >10k msg/s per instance.
- Schema evolution is enforced by convention and tests, not by a registry. Mitigation: the rules above, plus contract tests in both services.
- Non-string metadata values (numbers, objects) are rejected with 400. Mitigation: documented in the OpenAPI spec and README.

## Implementation rules

- Only the DTO in `adapters/kafka` (and `adapters/httpapi` for HTTP) MAY carry `json` tags; `domain` MUST NOT.
- The producer MUST set the `event-id` (UUIDv7, stdlib `uuid`) and `schema-version` headers on every record.
- Neither service MAY use `event-id` for deduplication or any business decision.
- The producer MUST set `traceparent` (version `00`, valid IDs, new span ID) on every record published from an HTTP request, and `tracestate` only when it is acceptable; both headers are optional for the consumer, which MUST NOT fail, retry or dead-letter a record because of them.
- The trace headers MUST NOT influence partitioning, the key, the payload or any business decision.
- The consumer MUST ignore unknown JSON fields and MUST route undecodable or unknown-version records to the DLQ.
- Both modules MUST have a contract test against `contracts/user_activity.v1.schema.json`.
- A contract change MUST follow the evolution rules; breaking changes MUST introduce v2.
- Third-party JSON codecs MUST NOT be added without a profile proving the need.
- Payloads (especially `metadata`) MUST NOT be logged in full ([ADR 0002](0002-security-baseline-and-threat-model.md)).

## Related

- [ADR 0004](0004-domain-model-and-ubiquitous-language.md) — `ActivityEvent`, which the DTO maps to.
- [ADR 0007](0007-kafka-client-and-topic-topology.md) — topic, key and headers on the client side.
- [ADR 0005](0005-ingestion-http-api.md) — HTTP decoding and validation limits.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — effectively once per Kafka record; why client retries are still counted twice.
- [ADR 0011](0011-failure-handling-retry-and-dlq.md) — what happens to undecodable records.
- [ADR 0001](0001-versions-and-upgrade-policy.md) — Go version that provides `uuid` and the v2 JSON engine.

## References

- [1] A. Wright, H. Andrews, B. Hutton and G. Dennis, *JSON Schema: A Media Type for Describing JSON Documents* (core, draft-bhutton-json-schema-01), and A. Wright, H. Andrews and B. Hutton, *JSON Schema Validation: A Vocabulary for Structural Validation of JSON* (draft-bhutton-json-schema-validation-01), Internet-Drafts of 16 June 2022 for JSON Schema draft 2020-12. https://json-schema.org/draft/2020-12/json-schema-core and https://json-schema.org/draft/2020-12/json-schema-validation. The `$schema` dialect of the contract; validation section 6.3.1 (`maxLength` counts characters) and section 7 (`format`) back the note about byte versus character limits.
- [2] T. Bray (ed.), *RFC 8259: The JavaScript Object Notation (JSON) Data Interchange Format*, IETF, December 2017. https://www.rfc-editor.org/rfc/rfc8259. JSON exchanged between systems is UTF-8; the payload is an object with string members.
- [3] G. Klyne and C. Newman, *RFC 3339: Date and Time on the Internet: Timestamps*, IETF, July 2002. https://www.rfc-editor.org/rfc/rfc3339. The `timestamp` field format and its normalization to UTC.
- [4] K. Davis, B. Peabody, P. Leach, *RFC 9562: Universally Unique IDentifiers (UUIDs)*, IETF, May 2024. https://www.rfc-editor.org/rfc/rfc9562. UUIDv7 for the `event-id` header.
- [5] W3C, *Trace Context*, W3C Recommendation, 23 November 2021 (Level 1). https://www.w3.org/TR/2021/REC-trace-context-1-20211123/. Section 3.2 and 3.3: the `traceparent` and `tracestate` header formats carried as Kafka record headers.
- [6] The Go Project, Go 1.27 release notes (https://go.dev/doc/go1.27) and package `encoding/json` at Go 1.27.2 (https://pkg.go.dev/encoding/json@go1.27.2). The stdlib `uuid` package, `encoding/json` on the v2 engine, `Decoder.DisallowUnknownFields`, and the stable `encoding/json/v2` package.
- [7] Martin Fowler, "Tolerant Reader". https://martinfowler.com/bliki/TolerantReader.html. The consumer ignores unknown fields so the producer can add optional ones first.
- [8] Martin Kleppmann, *Designing Data-Intensive Applications*, 1st ed., O'Reilly Media, 2017, ISBN 978-1-4493-7332-0, ch. 4, "Encoding and Evolution". Forward and backward compatibility, schema evolution rules, and the cost of binary formats with generated code.
- [9] Neal Ford, Mark Richards, Pramod Sadalage and Zhamak Dehghani, *Software Architecture: The Hard Parts*, O'Reilly Media, 2021, ISBN 978-1-4920-8689-5, ch. 13, "Contracts". A schema plus contract tests as the integration contract between independently deployed services.
