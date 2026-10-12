# ADR 0013: Observability: slog, minimal Prometheus, health

- **Status:** Accepted
- **Date:** 2026-10-11
- **Amended:** 2026-10-12 — `offset_fence_rejections_total` added; `offset_commit_failures_total` is now informational ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) option A).
- **Amended:** 2026-10-12 — `offset_fence_resyncs_total` (fence recovery), `instance_id` (static membership), consumer `trace_id` lines named.
- **Scope:** both

## Context

- The challenge asks for nothing beyond a working pipeline. Our summary of the challenge (§6, §8) asks for structured logs for operational events (publish/consume failures, retries, DB errors, latency, shutdown) and says *basic metrics are a useful starting point*; Prometheus/Grafana are **not** mandatory.
- `metadata` may hold URLs and referrers, so full payloads must not be logged (§6). Security rules for data in logs live in [0002](0002-security-baseline-and-threat-model.md).
- Both services run as distroless containers with no shell ([0015](0015-containerization-and-local-deployment.md)). Compose healthchecks and `docker compose up --wait` need an HTTP probe that the binary itself can call.
- The bonus requirements (scaling consumers, retry strategy, graceful shutdown) are much easier to demonstrate with a few counters than with log grepping.
- `slog.SetDefault` and `promauto` package globals are hidden global state that tests cannot isolate and that violate the "no package globals" rule in [0003](0003-system-decomposition-and-layout.md).

## Decision

### Logging: `log/slog`, JSON, injected

- `cmd/<svc>/main.go` builds one `*slog.Logger` with `slog.NewJSONHandler(os.Stdout, …)`. The level comes from `LOG_LEVEL` (default `info`). `platform/telemetry` owns the constructor.
- The logger is passed to constructors (`httpapi.NewHandler(logger, …)`, `kafka.NewConsumer(logger, …)`). Packages never call `slog.Default()`, `slog.SetDefault` or the package-level `slog.Info`.
- Logging happens **at the edges only**: adapters, middleware and `main`. `domain` and `application` return errors and never log.
- Standard fields:

| Field | Where | Note |
|---|---|---|
| `service` | every record (set once with `logger.With`) | `producer` / `consumer` |
| `request_id` | producer HTTP path | from the request-id middleware ([0005](0005-ingestion-http-api.md)) |
| `trace_id`, `span_id` | producer: every HTTP request log line (access log, errors, panics). Consumer: `trace_id` on per-record lines only, when the record has a valid `traceparent`: `record rejected` (`debug`, decode or validation failure, logged as the record is batched) and `record dead-lettered` (`warn`, after the DLQ publish). | from `traceparent` ([0006](0006-event-contract-and-json-serialization.md), [0005](0005-ingestion-http-api.md)); `span_id` is the producer's span for this request. Batch-level lines (flush) and hot-path success lines carry no trace ID, because a batch mixes traces. |
| `event_id` | producer publish, consumer DLQ | from the `event-id` header ([0006](0006-event-contract-and-json-serialization.md)) |
| `topic`, `partition`, `offset` | consumer records, DLQ, commits | enough to find the message in Kafka |
| `error_class` | failures | `permanent` / `transient` ([0011](0011-failure-handling-retry-and-dlq.md)) |
| `error` | failures of a dependency or of the process itself (Kafka, PostgreSQL, HTTP server, startup retries) | The full error text, under this one key in both services so cross-service log queries work. Allowed only for errors that cannot carry payload bytes. |
| `error_type` | any failure derived from a record or its payload (decode, validation, flush, DLQ publish) | The Go type of the innermost error, never its text, because `encoding/json` and driver errors can echo input ([0011](0011-failure-handling-retry-and-dlq.md), [0002](0002-security-baseline-and-threat-model.md)). |
| `attempt`, `backoff` | retries | per partition worker |
| `batch_size`, `duration` | flushes | consumer |
| `instance_id` | consumer `starting` line | the static group membership identity of the replica ([0012](0012-lifecycle-and-graceful-shutdown.md)); member `i` joins as `<instance_id>-<i>` |

- **Never logged:** request bodies, record values, `metadata`, headers, passwords, DSNs. `user_id` appears **only at `debug`**. The only header values that are logged are the IDs extracted from `traceparent` (`trace_id`, `span_id`) after strict validation, so they are 32 and 16 lowercase hex characters and cannot inject log content; `tracestate` is never logged.

### Trace context: W3C level 1 without OpenTelemetry

Logs of every service that touched an event can be joined by one `trace_id`, including upstream services that call the producer and downstream ones reading Kafka. The mechanism is the [W3C Trace Context](https://www.w3.org/TR/trace-context/) level 1 header format, implemented with the standard library only:

1. The producer's `Trace` middleware reads `traceparent` / `tracestate` from the request, or starts a new trace, and creates a new span ID for this hop ([0005](0005-ingestion-http-api.md)).
2. The Kafka publisher writes `traceparent` (with that span) and `tracestate` (unchanged, when valid) as record headers ([0006](0006-event-contract-and-json-serialization.md)).
3. The consumer reads `traceparent` per record and logs `trace_id` on per-record failure paths; DLQ records keep the original `traceparent` / `tracestate` ([0011](0011-failure-handling-retry-and-dlq.md)).

Rules shared by both services: version `00` is exactly 55 characters of lowercase hex; all-zero IDs and version `ff` are invalid; a higher version is read by its first four fields; an invalid or missing `traceparent` means a new trace and no `tracestate`; `tracestate` over 512 bytes is dropped; new IDs come from `crypto/rand`. Tracing never fails a request or a record.

The parser lives in `internal/platform/tracectx` in each module. Two modules cannot share code ([0003](0003-system-decomposition-and-layout.md)), so the small package is duplicated, and trace types appear in no domain or port. Both copies are covered by table-driven tests built from the specification's examples and a fuzz test.

### Metrics: Prometheus `client_golang`, injected registry

- `main` creates `reg := prometheus.NewRegistry()`, registers `collectors.NewGoCollector()` and `collectors.NewProcessCollector(…)`, and passes `reg` (as `prometheus.Registerer`) to `telemetry.NewMetrics(reg)`. That returns a struct of typed collectors, which is injected into the adapters that record them. No `promauto`, no `prometheus.DefaultRegisterer`, no package-level collectors.
- `/metrics` is served by `promhttp.HandlerFor(reg, promhttp.HandlerOpts{})`.
- Custom metrics (low-cardinality labels only; **never** `user_id`, `event_id` or offsets as labels):

| Metric | Type | Labels | Service | Recorded by |
|---|---|---|---|---|
| `activities_published_total` | counter | `result` = `ok` \| `invalid` \| `kafka_error` | producer | `httpapi` handler |
| `records_consumed_total` | counter | `result` = `ok` \| `dlq` | consumer | partition worker |
| `dlq_records_total` | counter | `error_class` = `decode` \| `validation` | consumer | DLQ publisher |
| `aggregate_flush_duration_seconds` | histogram | — | consumer | partition worker, around one flush attempt (DLQ publish + the transaction that moves the stored position and upserts) |
| `retries_total` | counter | `dependency` = `postgres` \| `kafka` (both services); consumer also `error_class` = `transient` \| `data_exception` \| `unknown` | both | retry loop ([0011](0011-failure-handling-retry-and-dlq.md) asks for the class on the consumer; `transient` includes attempts that hit `FLUSH_TIMEOUT`; `data_exception` is a PostgreSQL SQLSTATE class 22 error, retried and logged at ERROR; `unknown` is an unclassified error that is retried anyway) |
| `consumer_lag_records` | gauge | `partition` | consumer | lag poller (below) |
| `offset_commit_failures_total` | counter | — | consumer | partition worker, when `CommitMarkedOffsets` fails after a DB commit. Informational since option A: the position stored in PostgreSQL is the source of truth, so a failed Kafka commit no longer causes an overcount; it only delays the lag gauge ([0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)) |
| `offset_fence_rejections_total` | counter | — | consumer | partition worker, when a flush is rolled back because another owner had moved the partition's stored position (`ports.ErrFenced`). The batch is dropped, that partition's worker stops and the member resumes the partition from the reloaded stored position (see `offset_fence_resyncs_total`). **Alert on any increase**: it means two members wrote the same partition (a stale member) ([0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)) |
| `offset_fence_resyncs_total` | counter | — | consumer | member poll loop, each time a fenced partition is resumed in place from the reloaded stored position, without a rebalance ([0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) fence recovery). Normally equal to `offset_fence_rejections_total`; a lower value with a growing `consumer_lag_records{partition}` means the stored position could not be reloaded |

- Kafka client metrics (connections, bytes, buffered records, errors) come from franz-go's `plugin/kprom`, created with `kprom.NewMetrics("kafka", kprom.Registerer(reg))` (the consumer adds `kprom.WithClientLabel()`, so its series carry a `client_id` label: one consumer process runs several clients, the group members and the DLQ publisher) and passed to the client through `kgo.WithHooks`.
- `kprom` does **not** expose consumer lag. The consumer computes it every 15 s with `kadm.Client.Lag(ctx, group)` and sets `consumer_lag_records`. This is the metric that shows whether `--scale consumer=N` / `CONSUMER_WORKERS` keeps up. It reads the offsets committed to **Kafka**, which the consumer keeps committing after every flush although PostgreSQL holds the authoritative positions; a new owner whose stored position is ahead of Kafka (crash between the two commits) commits it to Kafka before its first batch, so the lag returns to 0 even when nothing else arrives.

### Health endpoints on both services

| Endpoint | Meaning | Producer | Consumer |
|---|---|---|---|
| `GET /healthz` | liveness: process is up and serving | always 200 | always 200 |
| `GET /readyz` | readiness: can do useful work | Kafka reachable (`kgo.Client.Ping`, short timeout) | DB `pool.Ping` succeeds **and** the member has joined the group once |
| `GET /metrics` | Prometheus exposition | yes | yes |

- The producer serves them on its public port (`HTTP_ADDR`, default `:8080`) next to `/v1/activities`.
- The consumer runs a small internal `http.Server` (`HTTP_ADDR`, default `:8081`) with only these three routes. The port is **not** published by Compose. It shuts down after the Kafka client in the teardown order ([0012](0012-lifecycle-and-graceful-shutdown.md)).
- Both binaries have a `healthcheck` subcommand. It calls `GET http://127.0.0.1:<port>/readyz` with a 2 s timeout and exits 0 or 1. The image `HEALTHCHECK` and Compose use it ([0015](0015-containerization-and-local-deployment.md)). Readiness (not liveness) is used, so `docker compose up --wait` returns only once the pipeline can actually move data.
- Readiness checks are cached for 1–2 s so frequent probes don't hammer Kafka or Postgres.

### Not built

- **No Prometheus/Grafana containers.** Metrics are read with `curl 127.0.0.1:8080/metrics` (producer) or `docker compose exec`/a scrape in a real deployment. A monitoring stack adds images, config and memory without proving anything the challenge asks for (summary §8: "only if it contributes clear value"). Load and crash runs are evaluated through the end-of-run report of the load-test harness instead of a monitoring stack ([0016](0016-load-and-crash-testing.md)).
- **No OpenTelemetry SDK or tracing backend yet.** There is no collector or tracing UI in scope, and spans would have nowhere to go. We propagate the W3C trace context (level 1) and log `trace_id`, which already joins logs across the hops (HTTP → Kafka → DB) and across upstream callers, next to `event_id` and `topic/partition/offset`. **Trigger to add OTel:** a third service, a synchronous downstream call, or a production deployment with a tracing backend. **Upgrade path:** replace `internal/platform/tracectx` with the OTel propagator (`propagation.TraceContext`) and franz-go's `plugin/kotel`; the `traceparent` / `tracestate` header format stays identical, so mixed old and new services keep joining the same traces.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| `zap` / `zerolog` | Rejected | Faster, but an extra dependency for an I/O-bound service. Reconsider only if profiling shows logging above ~10 % CPU. |
| `slog.SetDefault` + package-level calls | Rejected | Hidden global; tests can't capture or silence logs per case. |
| `promauto` / default registry | Rejected | Package globals, duplicate-registration panics in tests, and an implicit coupling to `/metrics` of the default registry. |
| Full OpenTelemetry tracing SDK now | Rejected | Needs a collector or backend to be useful, plus SDK, exporter and `kotel` dependencies in both modules. Level 1 header propagation gives the log correlation we need and keeps the wire format OTel-compatible. |
| Propagating only `X-Request-ID` / `event-id` through Kafka | Rejected | Not understood by upstream callers or other services; `traceparent` is the standard header they already forward. |
| OpenTelemetry metrics SDK | Rejected | MeterProvider + exporter + collector for six metrics. Prometheus text format is scrapeable by OTel collectors anyway. |
| Prometheus + Grafana in Compose | Rejected | Not required; more images and RAM; distracts from the end-to-end path. |
| Logs only, no metrics | Rejected | No rates or lag, so scaling and retry behaviour can't be shown. |
| `/healthz` doing deep checks | Rejected | A DB outage would mark the process "dead", and an orchestrator would restart a healthy process that is correctly waiting and retrying ([0011](0011-failure-handling-retry-and-dlq.md)). |

## Consequences

**Positive**
- Zero logging dependencies; one small, well-known metrics dependency (versions in [0001](0001-versions-and-upgrade-policy.md)).
- Every test builds its own logger (e.g. writing to a `bytes.Buffer`) and its own registry, so tests run in parallel and assert on metric values with `prometheus/testutil`.
- Health semantics are explicit: liveness never depends on a dependency, so outages cause retries, not restart loops.
- Lag, DLQ and retry counters make the bonus behaviour visible in a demo.

**Negative / trade-offs**
- Constructors take an extra `*slog.Logger` and metrics parameter. Mitigation: `telemetry` bundles them; `main` stays the only wiring point.
- The consumer runs an HTTP server only for probes and metrics. Mitigation: it is small, internal, has timeouts, and is shut down in order.
- The lag poller adds an admin request every 15 s. Negligible locally; the interval is configurable.
- No dashboards. Mitigation: metric names are stable, so a scrape config and dashboard can be added later without code changes.

## Implementation rules

- Packages MUST receive `*slog.Logger` via constructors. They MUST NOT call `slog.SetDefault`, `slog.Default()`, package-level `slog.Info/Error/…`, `log.Print*` or `fmt.Print*`.
- `domain` and `application` MUST NOT log; they return wrapped errors.
- Logs MUST NOT contain request bodies, record values, `metadata`, headers or credentials. `user_id` MAY appear only at `debug` level.
- Consumer log records about a Kafka record MUST include `topic`, `partition` and `offset`.
- Producer request log lines MUST include `trace_id` and `span_id`. Consumer per-record failure and DLQ lines MUST include `trace_id` when the record has a valid `traceparent`; hot-path success lines and batch-level lines MUST NOT.
- Trace propagation MUST follow W3C Trace Context level 1 and use the standard library only; it MUST NOT fail a request or a record, and `tracestate` MUST NOT be logged.
- Metrics MUST be registered on the registry created in `main`. `promauto` and `prometheus.DefaultRegisterer` MUST NOT be used.
- Metric labels MUST come from a fixed, small set of values. `user_id`, `event_id`, offsets and raw error strings MUST NOT be labels.
- `/healthz` MUST NOT check dependencies. `/readyz` MUST check them with a short timeout.
- The consumer's HTTP port MUST NOT be published in Compose.
- The `healthcheck` subcommand MUST exit non-zero on any non-200 response, timeout or connection error.
- Both HTTP servers MUST set `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout` and `IdleTimeout` ([0002](0002-security-baseline-and-threat-model.md)).

## Related

- [0003](0003-system-decomposition-and-layout.md) — `platform/telemetry` location; no package globals.
- [0006](0006-event-contract-and-json-serialization.md) — `event-id`, `traceparent` and `tracestate` headers used for log correlation.
- [0005](0005-ingestion-http-api.md) — request-id, trace and access-log middleware; request and response `traceparent`.
- [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — stored positions; what `offset_fence_rejections_total` and `offset_commit_failures_total` mean.
- [0011](0011-failure-handling-retry-and-dlq.md) — what `retries_total` and `dlq_records_total` count.
- [0012](0012-lifecycle-and-graceful-shutdown.md) — shutdown order of the health server.
- [0015](0015-containerization-and-local-deployment.md) — `HEALTHCHECK` and Compose healthchecks use the `healthcheck` subcommand.
- [0001](0001-versions-and-upgrade-policy.md) — `client_golang`, `kprom` and `kadm` versions.
- [0002](0002-security-baseline-and-threat-model.md) — PII rules for logs and DLQ headers.
- [0016](0016-load-and-crash-testing.md) — load and crash runs read these metrics and end with a report instead of dashboards.

## References

- [1] The Go Project, package `log/slog` at Go 1.27.2 (https://pkg.go.dev/log/slog@go1.27.2) and Jonathan Amsterdam, "Structured Logging with slog", Go Blog, 22 August 2023 (https://go.dev/blog/slog). JSON handler, levels, `With` attributes and `NewLogLogger`.
- [2] Prometheus documentation: "Metric and label naming" (https://prometheus.io/docs/practices/naming/) and "Instrumentation" (https://prometheus.io/docs/practices/instrumentation/). Do not use labels with high cardinality such as user IDs; naming and base units.
- [3] Prometheus `client_golang` v1.24.1, packages `prometheus` and `promhttp`. https://pkg.go.dev/github.com/prometheus/client_golang@v1.24.1/prometheus and https://pkg.go.dev/github.com/prometheus/client_golang@v1.24.1/prometheus/promhttp. The injected `Registry`, collectors and `HandlerFor`.
- [4] franz-go `kprom` v1.5.0 (https://pkg.go.dev/github.com/twmb/franz-go/plugin/kprom@v1.5.0) and `kadm` v1.19.0 (https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm@v1.19.0, `Lag`). Client metrics and the consumer-lag gauge.
- [5] W3C, *Trace Context*, W3C Recommendation, 23 November 2021 (Level 1). https://www.w3.org/TR/2021/REC-trace-context-1-20211123/. Header formats and the mutation and processing rules (sections 3 and 4); section 6, privacy considerations, for what a trace ID reveals.
- [6] OpenTelemetry, "Context propagation" (https://opentelemetry.io/docs/concepts/context-propagation/) and the franz-go `kotel` plugin (https://pkg.go.dev/github.com/twmb/franz-go/plugin/kotel). The upgrade path: the W3C header format stays identical when the SDK is adopted later.
- [7] Betsy Beyer, Chris Jones, Jennifer Petoff and Niall Richard Murphy (eds.), *Site Reliability Engineering*, O'Reilly Media, 2016, ISBN 978-1-4919-2912-4, ch. 6, "Monitoring Distributed Systems". https://sre.google/sre-book/monitoring-distributed-systems/. What to measure (latency, traffic, errors, saturation) and why a small set of well-chosen metrics beats many.
- [8] The Twelve-Factor App, XI. "Logs". https://12factor.net/logs. Logs as an event stream written to standard output.
- [9] OWASP Cheat Sheet Series, "Logging". https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html. Data that must not be logged, and log-injection concerns behind the strict validation of logged trace IDs.
