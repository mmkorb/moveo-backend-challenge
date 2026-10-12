# ADR 0016: Load and crash testing with k6 and an end-of-run report

- **Status:** Accepted
- **Date:** 2026-10-11
- **Amended:** 2026-10-12 — the overcount bound is 0 in every scenario, and every run asserts `SUM(activity_count) = input topic records − DLQ records` ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) option A).
- **Amended:** 2026-10-12 — the crash table names the recovery times and retries each crash run reports (static membership, `FLUSH_TIMEOUT`, [ADR 0011](0011-failure-handling-retry-and-dlq.md), [ADR 0012](0012-lifecycle-and-graceful-shutdown.md)).
- **Scope:** both

## Context

- The bonus requirements (horizontal scaling, retry strategy, graceful shutdown) and the delivery semantics in [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) are claims. Unit and integration tests ([0014](0014-testing-strategy-and-quality-tooling.md)) check them piece by piece; nothing yet checks them on the whole stack under load and under failure.
- We want answers to four questions:
  1. **Where is the knee?** The request rate at which p95 latency rises, k6 drops iterations or consumer lag keeps growing.
  2. **Does effectively-once hold under crashes?** No accepted event is lost and none is counted twice: the database total equals what Kafka holds ([0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).
  3. **Is shutdown graceful?** `SIGTERM` gives exit code 0 and zero overcount ([0012](0012-lifecycle-and-graceful-shutdown.md)).
  4. **How do resources behave?** Peak CPU and memory against the limits, no OOM kills, no memory growth over time.
- Decisions: **no monitoring stack** (no Prometheus, Grafana or Loki containers; [0013](0013-observability.md) stays as it is). Instead, **every run ends with a report** that answers the questions above. Runs happen on the local Docker Desktop (12 CPUs / 15.5 GiB on the reference machine), with **hard resource limits on every container**, so a stress run cannot take the machine down. The load tool is **k6**. The harness lives in `loadtest/`, apart from the services.
- [0014](0014-testing-strategy-and-quality-tooling.md) planned a separate `scripts/load.sh` end-to-end smoke. Two harnesses for the same stack would drift apart.

## Decision

We build a load and crash test harness in `loadtest/`. It runs k6 inside an isolated Compose project with explicit limits, drives chaos actions with `docker compose`, and ends every run with a Markdown + HTML report and a PASS/FAIL exit code. `scripts/load.sh` is replaced by `loadtest/run.sh smoke`.

### Layout

```text
loadtest/
├── README.md                     pointer to docs/load-testing.md (how to run, outputs, how to read the report)
├── compose.loadtest.yaml         overlay: k6 service (profile "loadtest") + explicit limits for every service
├── run.sh                        orchestrator (bash, Git Bash on Windows; needs only docker)
├── run-suite.sh                  runs scenarios in a row through run.sh; soak only with --soak
├── profiles/                     resource profiles (*.env, LT_* only) that make one component the bottleneck
├── k6/
│   ├── activities.js             scenarios, thresholds, custom counters, handleSummary
│   └── lib/payload.js            user pool, activity mix, timestamps (incl. UTC midnight)
├── report/                       Go module, stdlib only → report.md + report.html
│   ├── go.mod
│   ├── main.go
│   ├── report.tmpl.md, report.tmpl.html
│   └── testdata/                 a fixture run for `go test`
└── results/                      gitignored; one directory per run: <UTC-timestamp>-<scenario>/
```

- `loadtest/report` is a third Go module with **no third-party dependencies**. It is not a service, has no image and shares no code with `services/` ([0003](0003-system-decomposition-and-layout.md)). `run.sh` runs it in the `golang` image ([0001](0001-versions-and-upgrade-policy.md)) with `loadtest/` mounted, so the host needs no Go toolchain.

### Isolation

- `run.sh` always calls `docker compose -p moveo-loadtest -f compose.yaml -f loadtest/compose.loadtest.yaml`. The separate project name gives separate containers, network and volumes, so the developer's `moveo-analytics` stack and its data are never touched.
- The base `compose.yaml` is reused as is ([0015](0015-containerization-and-local-deployment.md)); the overlay only adds the k6 service and tightens limits. The stack under test is the stack a reviewer runs.
- Every run ends with `down -v` (volumes included), unless `--keep` is given for debugging.

### Resource budget

The overlay sets `deploy.resources.limits` (`cpus`, `memory`, `pids`) on **every** service, the `local` log driver with rotation (`max-size: 10m`, `max-file: 3`), and `GOMEMLIMIT` at about 90 % of the memory limit for the Go services:

| Service | CPUs | Memory | Notes |
|---|---|---|---|
| kafka | 1.5 | 1.25G | JVM heap fixed below the limit (`KAFKA_HEAP_OPTS=-Xms768m -Xmx768m`) |
| postgres | 1.0 | 768M | `shm_size: 128m` |
| producer | 0.75 | 256M | |
| consumer | 1.0 | 384M | `CONSUMER_WORKERS=3` |
| k6 | 1.0 | 768M | `maxVUs` ≤ 300 |
| kafka-init / consumer-migrate | 0.5 | 256M | one-shot |
| **Total cap** | **≈ 5.75** | **≈ 3.7 GiB** | half the CPUs, under ¼ of the RAM |

Every value can be overridden through an env variable with a default in the overlay (`LT_KAFKA_CPUS`, `LT_KAFKA_MEMORY`, …), so a smaller machine can shrink the budget without editing files.

### Safety rails

- **Preflight** aborts when Docker has less than 8 GiB of memory or a `moveo-loadtest` project is already running.
- **Capped load:** every k6 scenario has a fixed maximum rate and `maxVUs`.
- **Abort on runaway:** thresholds with `abortOnFail` stop a run when the error rate stays above 10 % or p95 stays above 2 s. A stress test finds the knee; it does not keep pushing past it.
- **Always clean up:** a `trap` on `EXIT` and `INT` stops the background samplers and runs `down -v`, even when the run fails or is interrupted.

### k6

- The image is `grafana/k6`, pinned in the overlay (version in [0001](0001-versions-and-upgrade-policy.md)). k6 runs **inside** the Compose network: it targets `http://producer:8080` and reads `http://consumer:8081/metrics` directly. No extra ports are published, and the consumer port stays unpublished ([0013](0013-observability.md)).
- `run.sh` takes its `/metrics` snapshots and `/readyz` polls through a small pinned `curlimages/curl` service (`probe`) in the same network, because the Go images have no shell or curl and the consumer port is not published ([0001](0001-versions-and-upgrade-policy.md)).
- The k6 service sits in `profiles: [loadtest]`, so `up --wait` starts only the stack under test; `run.sh` starts k6 with `run --rm k6`.

### Payload design (`k6/lib/payload.js`)

- **Traceable users:** `user_id = lt-<RUN_ID>-u<n>`. The report counts only rows whose `user_id` starts with `lt-<RUN_ID>-`, so a run can be verified exactly even if other data exists.
- **Skew:** a pool of users (default 5 000) with a skewed pick, so a few hot users receive many events. This exercises the per-key fold in [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) and the row contention on `activity_aggregates`.
- **Dates:** timestamps are "now", with about 5 % on the previous **UTC** day, so every run writes rows for two `activity_date` values and exercises the UTC rules of [0000](0000-scope-interpretation-and-assumptions.md).
- **Activity mix:** about 60 % `page_view`, the rest `click`, `scroll` and unknown types (accepted, [0004](0004-domain-model-and-ubiquitous-language.md)). Small `metadata` with synthetic values only.
- **Invalid requests:** a configurable ratio (default 0) of bad payloads to exercise the 400 path.
- **Custom counters:** `events_accepted` and `page_views_accepted` (on 202), and `events_unknown` (timeouts and 5xx: the event may or may not have reached Kafka). Checks: status 202 and an `event_id` in the body.

### Scenarios

Selected with `run.sh <scenario>` (`SCENARIO=` for k6); rates and durations are tunable through env.

| Scenario | Executor | Default shape | Purpose |
|---|---|---|---|
| `smoke` | shared-iterations | 1 VU, 200 events | correctness: DB totals equal accepted events, exactly; the end-to-end gate |
| `load` | constant-arrival-rate | 300 req/s, 5 min | steady-state latency, lag and resources |
| `stress` | ramping-arrival-rate | 100 → 1 500 req/s in steps over 10 min | find the knee (dropped iterations, p95 rise, lag growth) |
| `spike` | ramping-arrival-rate | 50 → 1 500 req/s for 30 s → 50 | burst absorption and drain time |
| `soak` | constant-arrival-rate | 150 req/s, 30 min | memory growth and leaks |
| `crash-*` | constant-arrival-rate | 200 req/s, 3 min + a chaos action from `run.sh` | resilience (below) |
| `crash-commit-gap` | constant-arrival-rate | the `crash` load + Kafka stop, consumer SIGKILL, restarts (below) | the stored position, not the Kafka offset, prevents double counting |
| `poison-records` | constant-arrival-rate | the `crash` load + `LT_POISON_COUNT` malformed records written to the topic at 60 s | the DLQ path end to end: classes, count, no stalled partition |

Default thresholds: `http_req_failed` < 1 %, `http_req_duration` p(95) < 300 ms, `checks` > 99 %. They are relaxed for `stress`, and for the outage window of the crash scenarios.

### Crash scenarios

`run.sh` runs the chaos action in the background, timed against the k6 run, and records each action in `timeline.jsonl`. The expected outcome follows from an existing ADR and is asserted in the report.

| Scenario | Action | Expected outcome (asserted) | Source |
|---|---|---|---|
| `crash-consumer-kill` | `kill -s SIGKILL consumer` at 60 s, `start` at 75 s | no loss; **zero** overcount: the restarted consumer resumes from the positions stored in PostgreSQL. Reported: ready again within a few seconds of `start`, because the static members take their partitions back without waiting for the session timeout (2026-10-12: 0.3 s; 29.9 s before static membership) | [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), [0012](0012-lifecycle-and-graceful-shutdown.md) |
| `crash-consumer-stop` | `stop consumer` (`SIGTERM`) at 60 s, `start` at 75 s | exit code 0 (not 137); **zero** overcount; no loss. Reported: the stop takes about 1 s (explicit leave of the static members) and the consumer is ready again about 1 s after `start` (2026-10-12: stop 1.1 s, ready 0.3 s) | [0012](0012-lifecycle-and-graceful-shutdown.md) |
| `crash-postgres-pause` | `pause postgres` for 30 s, then `unpause` | lag grows, then drains; no DLQ records; no loss; zero overcount. Reported: `postgres` retries > 0, because every flush attempt against the frozen server ends at `FLUSH_TIMEOUT` (2026-10-12: 12 retries; 0 before, the flushes hung) | [0011](0011-failure-handling-retry-and-dlq.md) |
| `crash-kafka-stop` | `stop kafka` for 20 s, then `start` | producer 503s only inside the outage window; recovery time measured; no loss of accepted events; zero overcount beyond `events_unknown` | [0005](0005-ingestion-http-api.md), [0011](0011-failure-handling-retry-and-dlq.md) |
| `crash-scale-out` | `up -d --scale consumer=2` at 60 s | rebalance without loss and without overcount | [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) |
| `crash-commit-gap` | `stop kafka` at 60 s, `kill -s SIGKILL consumer` at 70 s (Kafka still down), `start` kafka at 85 s, `start` consumer at 95 s | no loss; **zero** overcount; `DB = topic − DLQ`. Asserted: at least one partition resumed with `stored_offset > committed_offset` (the database committed, the Kafka commit did not), otherwise the run did not test the case and fails. Reported: records protected by the stored position, fence counters | [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), [0008a](0008a-separate-offset-table-justification.md) |
| `poison-records` | `LT_POISON_COUNT` (50) malformed records written to `incoming.user_activity` with `kafka-console-producer.sh` at 60 s | DLQ holds exactly the injected records, with the expected `error_class` mix (`decode`: JSON the decoder rejects; `validation`: valid JSON the domain rejects); `DB = topic − DLQ`; lag drains | [0011](0011-failure-handling-retry-and-dlq.md) |

**Correctness rules for every scenario:**

1. `accepted` is the number of 202s. `overcount = SUM(activity_count) − accepted` for the run's users. Loss (`overcount < 0`) always fails. The upper bound is **0 in every scenario**, crash runs included, because the consumer stores its Kafka positions in the same transaction as the aggregates ([0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). Events in `events_unknown` (timeouts and 5xx) may or may not have reached Kafka; they are reported separately and widen the upper bound by their count, never the lower one. That allowance covers client-side uncertainty, not consumer double counting. The same rule applies to `page_view_count` against `page_views_accepted`.
2. **Effectively once:** `SUM(activity_count) = records in the input topic − records in the DLQ topic`. The topic's end offsets are the ground truth for "produced": they include the `events_unknown` requests that did reach Kafka, so this check needs no allowance and any difference is consumer loss or double counting. It relies on the run starting from empty volumes (as `run.sh` does) and on the producer being the only writer of the topic.

### `run.sh` flow

1. Preflight (docker present, memory, project not running) → `RUN_ID`, results directory.
2. `up -d --build --wait` (k6 is excluded by its profile).
3. Baseline: `/metrics` of both services, `docker compose images` (the exact tags under test).
4. Background sampler: `docker stats --no-stream --format '{{json .}}'` every 2 s → `stats.jsonl`.
5. `run --rm k6`, plus the chaos timeline for `crash-*`; start, end and actions go to `timeline.jsonl`.
6. Drain: poll `kafka-consumer-groups.sh --describe --group analytics-consumer` inside the kafka container until total lag is 0 or a timeout → **drain time**.
7. Collect: final `/metrics`; `psql` totals for `lt-<RUN_ID>-%` (`SUM(activity_count)`, `SUM(page_view_count)`, `COUNT(DISTINCT user_id)`, rows per date); DLQ end offsets; `docker inspect` (OOMKilled, RestartCount, ExitCode); ERROR/WARN counts per service from `compose logs`.
8. Render the report with `go run ./report <results-dir>` in a `golang` container.
9. `down -v` (unless `--keep`); print the report path and the verdict; exit with the verdict's code.

### The report

`report.md` plus a self-contained `report.html` (inline CSS and SVG, no external assets) in the run's results directory, with nine sections:

1. **Verdict** — PASS/FAIL per threshold and per scenario expectation, with the reason for each FAIL.
2. **Run** — scenario, UTC start, duration, host CPUs/RAM, image tags, configuration (partitions, workers, flush settings, limits).
3. **Ingestion (producer)** — requests, achieved vs target rate, dropped iterations, status breakdown (202/400/503/timeout), latency p50/p90/p95/p99/max.
4. **Pipeline (consumer)** — events consumed per second, max lag, **drain time** (last 202 → lag 0), flush duration p95, retries, DLQ records, offset-commit failures (`offset_commit_failures_total`, [0013](0013-observability.md)).
5. **Correctness** — accepted vs `SUM(activity_count)` and page views, overcount against its bound (0 + `events_unknown`), the database total against the input topic minus the DLQ, distinct users and rows per date.
6. **Resources** — per container: peak and average CPU %, peak memory vs limit, network I/O, OOMKilled, restarts; time series as inline SVG in the HTML.
7. **Errors** — ERROR/WARN counts per service and the top 5 messages.
8. **Chaos timeline** (crash runs only) — actions with timestamps and the measured recovery times.
9. **Artifacts** — links to `k6-dashboard.html` and the raw JSON/JSONL files.

- **Exit codes:** `0` = PASS; `1` = FAIL (a threshold or an expectation was not met); `2` = the harness itself failed (preflight, startup, collection), so there is no verdict. Automation uses the exit code; humans read the report.
- **k6's own artifacts:** `handleSummary` writes `k6-summary.json`, the report's main input. The built-in web dashboard export (`K6_WEB_DASHBOARD=true`, `K6_WEB_DASHBOARD_EXPORT=k6-dashboard.html`) adds a self-contained HTML file with time-series charts of the k6 metrics. It needs no server and stays after the containers are gone.

### No monitoring stack

The report replaces dashboards for evaluation. It is produced from data collected during the run (k6 summary, `/metrics` snapshots, `docker stats`, SQL, `docker inspect`, logs) and needs nothing running afterwards. [0013](0013-observability.md) keeps "No Prometheus/Grafana containers".

### End-to-end gate

`bash loadtest/run.sh smoke` (`make smoke`) is the end-to-end gate of [0014](0014-testing-strategy-and-quality-tooling.md), run locally; it fails on a non-zero exit code. `report.md` and `k6-dashboard.html` are written to the run directory. The heavier scenarios are manual: they need minutes to tens of minutes and more resources than a shared runner guarantees.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| k6 in an isolated Compose project + end-of-run report (this ADR) | Chosen | Scenarios, executors with real arrival rates and thresholds out of the box; one container; the report answers the four questions without a running stack. |
| Prometheus + Grafana (+ Loki) stack for the test | Rejected | Adds 3 images and roughly 1 GiB of RAM to a machine that should spend its budget on the system under test, and dashboards still need a human to read them. The report gives a verdict and a durable record. |
| Custom Go load generator | Rejected | Open-model rate control, coordinated-omission-free latency, percentiles and thresholds would all be rebuilt by hand. |
| vegeta | Rejected | Good for a constant rate, but has no multi-stage scenarios, no thresholds and no scripted payloads. |
| Locust / JMeter / Gatling | Rejected | Python or JVM runtimes, heavier images and more memory per generated request; nothing we need that k6 lacks. |
| k6 installed on the host | Rejected | An extra install on every machine, and it cannot reach the unpublished consumer `/metrics` without opening a port. |
| Chaos tooling (Pumba, toxiproxy) | Deferred | `docker compose kill/stop/pause/start` covers the failures we assert. Revisit for latency or packet-loss injection. |
| Running against the dev project (`moveo-analytics`) | Rejected | Pollutes the developer's data, and a crash test could leave their stack broken. |
| Keep `scripts/load.sh` next to the harness | Rejected | Two end-to-end paths that drift apart. `run.sh smoke` does the same check with the same code as every other scenario. |

## Consequences

**Positive**
- The claims of [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), [0011](0011-failure-handling-retry-and-dlq.md) and [0012](0012-lifecycle-and-graceful-shutdown.md) become checked results, with exact counts per run.
- Every run leaves a self-contained, shareable report and a PASS/FAIL exit code; automation and humans use the same harness.
- A run cannot exhaust the host: the total cap is about half the CPUs and under a quarter of the RAM, and runaway runs abort.
- Dev data is never touched, and every run starts from empty volumes.

**Negative / trade-offs**
- Load generator, brokers and services share one machine, so absolute numbers are local and pessimistic. Mitigation: the report records host and limits; the value is in relative changes and in correctness, not in absolute throughput.
- No live view during a run. Mitigation: `--keep` leaves the stack up for inspection, `docker stats` works as usual, and `k6-dashboard.html` gives the time series afterwards.
- `run.sh` is a non-trivial shell script. Mitigation: `bash -n` and shellcheck, a `trap` for cleanup, and every piece of analysis lives in the tested Go report module, not in bash.
- The `SIGKILL` scenario cannot choose where in a flush the process dies. Mitigation: the assertion does not depend on it: whatever the timing, the database must equal the input topic minus the DLQ ([0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)); `offset_commit_failures_total` and `offset_fence_rejections_total` are reported next to it.
- The full end-to-end run needs both service images. Mitigation: until they exist, the harness is validated statically and against Kafka + Postgres only, as stated in [docs/load-testing.md](../load-testing.md).

## Implementation rules

- Every service in `compose.loadtest.yaml` MUST have `cpus`, `memory` and `pids` limits and `local` logging with rotation. Go services MUST set `GOMEMLIMIT` below their memory limit; Kafka MUST set a fixed heap below its limit.
- `run.sh` MUST use the project name `moveo-loadtest` and both compose files on every compose call. It MUST NOT operate on the default project.
- `run.sh` MUST always clean up: a `trap` on `EXIT` and `INT` MUST stop the samplers and run `down -v` unless `--keep` was given.
- Preflight MUST abort when Docker memory is below 8 GiB or the `moveo-loadtest` project already runs.
- Every k6 scenario MUST cap its rate and `maxVUs`, and MUST define thresholds with `abortOnFail` for a runaway error rate or latency.
- Every scenario MUST assert "no loss", zero overcount beyond `events_unknown`, and `SUM(activity_count) = input topic records − DLQ records`.
- Load-test users MUST use the `lt-<RUN_ID>-` prefix, and verification queries MUST filter on it.
- The k6 image MUST be pinned to the tag in [0001](0001-versions-and-upgrade-policy.md); the report module MUST use the stdlib only.
- Reports and results MUST NOT contain `metadata` values, request bodies or any data beyond the synthetic user ids ([0002](0002-security-baseline-and-threat-model.md)). Payloads MUST use synthetic values only.
- `loadtest/results/` MUST stay gitignored.
- `run.sh` MUST exit 0 only on PASS, 1 on FAIL and 2 on a harness error.
- The end-to-end gate MUST be `loadtest/run.sh smoke`, run locally.

## Related

- [0003](0003-system-decomposition-and-layout.md) — repository layout with `loadtest/`.
- [0005](0005-ingestion-http-api.md) — the endpoint under load and its 503 behaviour.
- [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — stored positions (option A) and the effectively-once guarantee asserted after `SIGKILL` and the other crashes.
- [0011](0011-failure-handling-retry-and-dlq.md) — transient errors retry and never reach the DLQ (Postgres pause).
- [0012](0012-lifecycle-and-graceful-shutdown.md) — graceful shutdown asserted by `crash-consumer-stop`.
- [0013](0013-observability.md) — metrics read by the harness; no monitoring stack.
- [0014](0014-testing-strategy-and-quality-tooling.md) — the end-to-end layer is `run.sh smoke`.
- [0015](0015-containerization-and-local-deployment.md) — the base `compose.yaml` the overlay extends.
- [0001](0001-versions-and-upgrade-policy.md) — `grafana/k6` and `golang` image tags.
- [0002](0002-security-baseline-and-threat-model.md) — no PII in reports; synthetic data only.

## References

- [1] Grafana Labs, k6 documentation for the pinned line v2.3.x (https://grafana.com/docs/k6/v2.3.x/; release https://github.com/grafana/k6/releases/tag/v2.3.0): "Scenarios" (https://grafana.com/docs/k6/v2.3.x/using-k6/scenarios/) and "Thresholds" (https://grafana.com/docs/k6/v2.3.x/using-k6/thresholds/). The executors used (`shared-iterations`, `constant-arrival-rate`, `ramping-arrival-rate`) and `abortOnFail`.
- [2] Grafana Labs, k6 documentation: "Open and closed models" (https://grafana.com/docs/k6/latest/using-k6/scenarios/concepts/open-vs-closed/), "Load test types" (https://grafana.com/docs/k6/latest/testing-guides/test-types/), "Custom summary" (https://grafana.com/docs/k6/latest/results-output/end-of-test/custom-summary/) and "Web dashboard" (https://grafana.com/docs/k6/latest/results-output/web-dashboard/). Arrival-rate executors use the open model; the smoke, average-load, stress, spike and soak test types behind the scenario shapes; `handleSummary` and the exported dashboard.
- [3] Brendan Gregg, "The USE Method". https://www.brendangregg.com/usemethod.html. Utilization, saturation and errors per resource, the frame for the report's resources section.
- [4] Betsy Beyer, Chris Jones, Jennifer Petoff and Niall Richard Murphy (eds.), *Site Reliability Engineering*, O'Reilly Media, 2016, ISBN 978-1-4919-2912-4: ch. 6 "Monitoring Distributed Systems" (https://sre.google/sre-book/monitoring-distributed-systems/), ch. 17 "Testing for Reliability" (https://sre.google/sre-book/testing-reliability/) and ch. 21 "Handling Overload" (https://sre.google/sre-book/handling-overload/). Latency, saturation and error signals; stress and failure tests; behaviour under overload.
- [5] Michael T. Nygard, *Release It!*, 2nd ed., Pragmatic Bookshelf, 2018, ISBN 978-1-68050-239-8, "Create Stability" and "Chaos Engineering". https://pragprog.com/titles/mnee2/release-it-second-edition/. Deliberate failure injection and the stability patterns the crash scenarios check.
- [6] Martin Kleppmann, *Designing Data-Intensive Applications*, 1st ed., O'Reilly Media, 2017, ISBN 978-1-4493-7332-0, ch. 1, "Reliable, Scalable, and Maintainable Applications", section "Describing Performance". Percentiles rather than averages for response times, and tail latency.
- [7] Docker Docs: `docker compose kill` (https://docs.docker.com/reference/cli/docker/compose/kill/), `docker container pause` (https://docs.docker.com/reference/cli/docker/container/pause/), `docker container stats` (https://docs.docker.com/reference/cli/docker/container/stats/), "Resource constraints" (https://docs.docker.com/engine/containers/resource_constraints/) and Compose "Project name" (https://docs.docker.com/compose/how-tos/project-name/). The chaos actions, the sampler, the hard limits and the separate `moveo-loadtest` project.
- [8] Apache Kafka 4.3 documentation, "Monitoring" (https://kafka.apache.org/43/operations/monitoring/) and `kafka-consumer-groups.sh` in "Basic Kafka Operations" (https://kafka.apache.org/43/operations/basic-kafka-operations/). Consumer lag and the tool that reports it for the drain-time measurement.
