# ADR 0005: Ingestion HTTP API on `net/http`

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** producer

## Context

- The challenge asks for "a Kafka producer that exposes a REST endpoint". The API surface is **one write endpoint** plus operational endpoints (health, readiness, metrics).
- `http.ServeMux` is enough for this API: `http.StripPrefix` has always existed, and since Go 1.22 `ServeMux` matches methods and path wildcards (`"POST /v1/activities"`, `r.PathValue`). A router library would add a dependency without need (minimal dependencies, [ADR 0001](0001-versions-and-upgrade-policy.md)).
- The response must tell the client something true. The summary asks for a reliable pipeline, so an accepted event has to be durable in Kafka, not just buffered in memory.
- The API is unauthenticated and receives untrusted input ([ADR 0000](0000-scope-interpretation-and-assumptions.md), [ADR 0002](0002-security-baseline-and-threat-model.md)).

## Decision

We use stdlib **`net/http`** with `http.ServeMux` and a tiny middleware chain. We use no router or framework.

### Routes

| Method & path | Purpose | Success |
|---|---|---|
| `POST /v1/activities` | Validate one `ActivityEvent` and publish it to `incoming.user_activity`. | `202 {"event_id":"<uuidv7>"}` |
| `GET /healthz` | Liveness: the process is serving. | `200 {"status":"ok"}` |
| `GET /readyz` | Readiness: Kafka is reachable (`kgo.Client.Ping` with a 1 s timeout), and the server is not shutting down. | `200` or `503` |
| `GET /metrics` | Prometheus exposition from the injected registry ([ADR 0013](0013-observability.md)). | `200` |

`ServeMux` already answers wrong methods with `405` (and an `Allow` header) and unknown paths with `404`.

### Semantics of `202`

The handler calls `RegisterActivity`, which publishes through `ActivityPublisher`. The Kafka adapter uses `ProduceSync` with the **request context plus a publish timeout** (default 5 s, below the server's `WriteTimeout`). We return `202 Accepted` **only after the broker ack** (`acks=all`, [ADR 0007](0007-kafka-client-and-topic-topology.md)). 202 rather than 201 because the aggregate is updated asynchronously. The body carries the `event_id` that also goes into the `event-id` header ([ADR 0006](0006-event-contract-and-json-serialization.md)); it is for tracing only.

If the client disconnects or times out, the record may or may not have been produced. A client retry can then duplicate the event, which is consistent with at-least-once ([ADR 0000](0000-scope-interpretation-and-assumptions.md)).

### Correlation headers

Every response, on every route and status, carries two correlation headers:

| Header | Request | Response |
|---|---|---|
| `X-Request-ID` | Optional. A well-formed value (1–64 characters of `[A-Za-z0-9_-]`) is kept. | The request ID, kept or generated. |
| `traceparent` | Optional, [W3C Trace Context](https://www.w3.org/TR/trace-context/) level 1. | The **producer's own span**: the caller's `trace-id` and a new `parent-id` generated for this hop. |
| `tracestate` | Optional, opaque. | Not returned. |

Trace handling is a middleware and never rejects a request:

- A valid `traceparent` continues the caller's trace: same `trace-id`, new `parent-id` (16 hex, `crypto/rand`), and the sampled flag is kept. Version `00` must be exactly 55 characters of lowercase hex with non-zero IDs. A higher version is read by its first four fields, as the specification allows. Version `ff` is invalid.
- A missing, malformed or repeated `traceparent` starts a new trace with a random `trace-id`, and any `tracestate` is dropped. The request is processed normally and no 4xx is returned.
- `tracestate` is passed through unchanged when `traceparent` is valid, multiple headers are joined with `,`, and values longer than 512 bytes or with non-printable bytes are dropped.
- The trace context travels in the request context to the Kafka adapter, which writes it as record headers ([ADR 0006](0006-event-contract-and-json-serialization.md)), and every log line of the request carries `trace_id` and `span_id` ([ADR 0013](0013-observability.md)). The response `traceparent` has the same span, so a caller can look up the producer's log lines and the Kafka record by trace ID.

Trace IDs are correlation data, not credentials: they MUST NOT be trusted for authorization or sampling decisions beyond the passed-through flag.

### Validation

| Field | Rule | Failure |
|---|---|---|
| body | `Content-Type: application/json` (parameters such as `charset=utf-8` allowed) | 415 |
| body | ≤ 64 KiB (`http.MaxBytesReader`) | 413 |
| body | exactly one JSON object; unknown fields rejected (`DisallowUnknownFields`); no trailing data | 400 |
| `user_id` | string, 1–128 bytes, valid UTF-8 | 400 |
| `activity_type` | string, 1–64 bytes, valid UTF-8; **any value accepted** ([ADR 0004](0004-domain-model-and-ubiquitous-language.md)) | 400 |
| `timestamp` | RFC 3339 (`time.RFC3339`, fractional seconds allowed), normalized to UTC | 400 |
| `metadata` | optional object of string values; ≤ 32 entries; key 1–64 bytes; value ≤ 2048 bytes | 400 |

Shape checks (JSON, content type, size) happen in `adapters/httpapi`. Field rules live in `domain.NewActivityEvent`, so the consumer enforces the same ones.

### Errors

Errors use `application/problem+json` (RFC 9457) and never include internals (stack traces, broker addresses, raw driver errors):

| Status | When | Notes |
|---|---|---|
| 400 | malformed JSON, unknown field, rule violation | `detail` names the field and rule; `errors[]` lists every violated field. |
| 413 | body > 64 KiB | |
| 415 | wrong or missing content type | |
| 503 | Kafka unavailable or publish timeout (adapter returns `ports.ErrPublishUnavailable`) | `Retry-After: 1`; logged at warn. |
| 500 | anything else, including recovered panics | Generic message; logged once at error with the request ID. |

Example:

```text
POST /v1/activities
Content-Type: application/json
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01

{"user_id":"12345","activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z",
 "metadata":{"page_url":"https://example.com/home","referrer":"https://google.com"}}

HTTP/1.1 202 Accepted
Content-Type: application/json
X-Request-ID: 6f1c2a9e0b7d4c3a8e5f1a2b3c4d5e6f
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-b7ad6b7169203331-01

{"event_id":"0199d3a4-7c2e-7b1a-9f3e-2a4b6c8d0e1f"}
```

```text
HTTP/1.1 400 Bad Request
Content-Type: application/problem+json

{"type":"about:blank","title":"Bad Request","status":400,
 "detail":"invalid activity event","errors":[{"field":"timestamp","rule":"must be RFC 3339"}]}
```

### Handler sketch

```go
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
	"uuid"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/ports"
)

// activityRegistrar is satisfied by *application.RegisterActivity.
type activityRegistrar interface {
	Register(ctx context.Context, ev domain.ActivityEvent) (uuid.UUID, error)
}

type activityHandler struct {
	reg            activityRegistrar
	log            *slog.Logger
	publishTimeout time.Duration
}

func NewRouter(reg activityRegistrar, ready func(context.Context) error, metrics http.Handler, logger *slog.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST /v1/activities", &activityHandler{reg: reg, log: logger, publishTimeout: 5 * time.Second})
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", readyz(ready))
	mux.Handle("GET /metrics", metrics)
	return Chain(mux, RequestID, Trace, Logging(logger), Recover(logger))
}

func (h *activityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isJSON(r.Header.Get("Content-Type")) {
		h.problem(w, r, http.StatusUnsupportedMediaType, nil)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	ev, err := decodeActivity(r.Body) // DisallowUnknownFields, trailing-data check, NewActivityEvent
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		h.problem(w, r, http.StatusRequestEntityTooLarge, nil)
		return
	case err != nil:
		h.problem(w, r, http.StatusBadRequest, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.publishTimeout)
	defer cancel()
	id, err := h.reg.Register(ctx, ev)
	switch {
	case errors.Is(err, ports.ErrPublishUnavailable):
		w.Header().Set("Retry-After", "1")
		h.problem(w, r, http.StatusServiceUnavailable, nil)
	case err != nil:
		h.log.ErrorContext(r.Context(), "register activity", "error", err, "request_id", RequestIDFrom(r.Context()))
		h.problem(w, r, http.StatusInternalServerError, nil)
	default:
		writeJSON(w, http.StatusAccepted, acceptedResponse{EventID: id.String()})
	}
}
```

The middleware (`Chain`, `RequestID`, `Trace`, `Logging`, `Recover`) is about 100 lines in `adapters/httpapi`; parsing and formatting of the trace headers live in `internal/platform/tracectx`. The order is request-id → trace → access log → recover → mux: `Trace` runs before `Logging` so the access log can read the trace context from the request context. The access log records `request_id`, `trace_id`, `span_id`, method, `r.Pattern`, status and duration, and never bodies or `metadata` ([ADR 0002](0002-security-baseline-and-threat-model.md)).

### Server

```go
srv := &http.Server{
	Addr:              cfg.HTTPAddr, // ":8080"
	Handler:           router,
	ReadHeaderTimeout: 5 * time.Second,
	ReadTimeout:       10 * time.Second,
	WriteTimeout:      15 * time.Second,
	IdleTimeout:       60 * time.Second,
	MaxHeaderBytes:    16 << 10,
	ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
}
```

Shutdown (`srv.Shutdown`, then `Flush`/`Close` of the Kafka client) is in [ADR 0012](0012-lifecycle-and-graceful-shutdown.md).

### OpenAPI

`services/producer/api/openapi.yaml` (OpenAPI 3.1) is **hand-written, spec-first**. It is the reviewer-facing contract, and the request schema matches `contracts/user_activity.v1.schema.json`. A test loads the spec and checks that every documented route and status code is served by the router.

### Deferred

A batch endpoint (`POST /v1/activities:batch`) is not built. One event per request is enough for the challenge, and partial-failure semantics would add complexity. It is the first extension if client round-trips become the bottleneck.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| `go-chi/chi` | Rejected | A good, `net/http`-compatible library, but with one endpoint its route groups and middleware scoping solve problems we don't have (YAGNI). `http.StripPrefix` already covers prefix stripping. |
| `gin`, `echo` | Rejected | Their own context types (`*gin.Context`, `echo.Context`) leak into handlers and tests. |
| `fiber` | Rejected | Built on `fasthttp`, not `net/http`: incompatible with `http.Handler`, `httptest` and standard middleware. |
| `beego`, `revel` | Rejected | Full MVC frameworks that impose their own layout. |
| `go-kit` | Rejected | Endpoint/transport layering duplicates our hexagonal ports for one route. |
| `swaggo` (OpenAPI from annotations) | Rejected | Code-first comments drift; spec-first is easier to review. |
| `go-playground/validator` | Rejected | Reflection and struct tags for five fields; explicit checks give clearer messages. |
| `202` before the Kafka ack (fire-and-forget) | Rejected | Events lost on crash would still be reported as accepted. |
| `201 Created` | Rejected | Nothing addressable is created synchronously. |

## Consequences

**Positive**
- No third-party HTTP dependency; handlers are plain `http.Handler`, tested with `httptest`.
- `202` is an honest durability signal.
- Strict input limits bound memory per request and reject malformed data at the edge.

**Negative / trade-offs**
- Request latency includes the Kafka round-trip (low ms locally). Mitigation: short linger; a batch endpoint if needed.
- During a Kafka outage the API returns 503 instead of buffering. Mitigation: explicit `Retry-After`; clients retry.
- Hand-rolled middleware is our code to maintain. Mitigation: about 60 lines, covered by tests, built as standard `func(http.Handler) http.Handler` decorators.

## Implementation rules

- The producer MUST use `http.NewServeMux()`; `http.DefaultServeMux` and router frameworks MUST NOT be used.
- `POST /v1/activities` MUST return 202 only after `ProduceSync` succeeds, under a bounded context derived from the request.
- The body MUST be wrapped in `http.MaxBytesReader` (64 KiB), and unknown fields MUST be rejected.
- The handler MUST NOT reject unknown `activity_type` values.
- Error responses MUST NOT contain internal error text, stack traces or infrastructure addresses.
- `http.Server` MUST set `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout` and `MaxHeaderBytes`.
- Logs MUST NOT contain request bodies or `metadata`; `user_id` only at debug level ([ADR 0002](0002-security-baseline-and-threat-model.md)).
- A request MUST NOT be rejected because of `traceparent` or `tracestate`; an invalid `traceparent` starts a new trace. Every response MUST carry the producer's `traceparent`.
- `api/openapi.yaml` MUST be updated in the same change as any route or status-code change.

## Related

- [ADR 0004](0004-domain-model-and-ubiquitous-language.md) — `NewActivityEvent` and the field limits.
- [ADR 0006](0006-event-contract-and-json-serialization.md) — payload, `event-id`, JSON codec.
- [ADR 0007](0007-kafka-client-and-topic-topology.md) — `acks=all`, idempotent producer, key.
- [ADR 0012](0012-lifecycle-and-graceful-shutdown.md) — server shutdown order.
- [ADR 0013](0013-observability.md) — access log fields, metrics, health.
- [ADR 0002](0002-security-baseline-and-threat-model.md) — threat model behind the limits.

## References

- [1] R. Fielding, M. Nottingham, J. Reschke (eds.), *RFC 9110: HTTP Semantics*, IETF, June 2022. https://www.rfc-editor.org/rfc/rfc9110. Section 15.3.3 (202 Accepted), 15.5.14 (413 Content Too Large), 15.5.16 (415 Unsupported Media Type), 15.5.6 (405 and `Allow`), 15.6.4 (503), 10.2.3 (`Retry-After`) and 8.3 (`Content-Type`): the status codes and headers of the endpoint.
- [2] M. Nottingham, E. Wilde, S. Dalal, *RFC 9457: Problem Details for HTTP APIs*, IETF, July 2023, section 3. https://www.rfc-editor.org/rfc/rfc9457. The `application/problem+json` error body, with `errors[]` as an extension member.
- [3] W3C, *Trace Context*, W3C Recommendation, 23 November 2021 (Level 1). https://www.w3.org/TR/2021/REC-trace-context-1-20211123/. Section 3.2 (`traceparent`), 3.3 (`tracestate`), 3.4 (mutating the `traceparent` field) and 4.2 and 4.3 (processing model when no or a valid `traceparent` is received) behind the correlation-header rules; section 7 (security considerations: information exposure and denial of service) for why the headers are validated and never trusted for decisions.
- [4] The Go Project, "Routing Enhancements for Go 1.22" (https://go.dev/blog/routing-enhancements) and the Go 1.22 release notes (https://go.dev/doc/go1.22). `ServeMux` method and wildcard patterns, which make a router dependency unnecessary.
- [5] The Go Project, package `net/http` at Go 1.27.2. https://pkg.go.dev/net/http@go1.27.2. `Server` timeouts and `MaxHeaderBytes`, `MaxBytesReader`, `ServeMux` 404/405 behaviour and `Server.Shutdown`.
- [6] OWASP, *API Security Top 10*, 2023: API4 "Unrestricted Resource Consumption" (https://owasp.org/API-Security/editions/2023/en/0xa4-unrestricted-resource-consumption/) and API8 "Security Misconfiguration" (https://owasp.org/API-Security/editions/2023/en/0xa8-security-misconfiguration/). Body-size limits and server timeouts.
- [7] OpenAPI Initiative, *OpenAPI Specification v3.1.1*. https://spec.openapis.org/oas/v3.1.1.html. The format of the hand-written `api/openapi.yaml`.
- [8] K. Davis, B. Peabody, P. Leach, *RFC 9562: Universally Unique IDentifiers (UUIDs)*, IETF, May 2024. https://www.rfc-editor.org/rfc/rfc9562. UUIDv7, the format of `event_id`.
