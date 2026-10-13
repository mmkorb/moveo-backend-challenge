# Load and crash testing

> **TL;DR** — Explains how to run the k6 load and crash harness (`loadtest/run.sh`), its scenarios, resource profiles, output files and PASS criteria. Key takeaway: every run is isolated, ends with a PASS/FAIL report, and checks no loss, zero overcount and `DB = topic - DLQ`.

How to run the load and crash-test harness, where its output goes, and how to read it. Design rationale and rules: [ADR 0016](adrs/0016-load-and-crash-testing.md). Measured results of the 2026-10-12 runs: [load-test-results.md](load-test-results.md). To start the system itself, see the [README](../README.md#run-the-system).

## What the harness is

`loadtest/run.sh <scenario>` brings up the whole pipeline (producer, consumer, Kafka, Postgres) in its own Compose project, `moveo-loadtest`, with hard CPU and memory limits. It drives the producer with [k6](https://grafana.com/docs/k6/), optionally injects one failure (kill, stop, pause or scale a container), waits for the consumer to drain, collects data, and renders a PASS/FAIL report. There is no monitoring stack: the report is built from k6's summary, `docker stats` samples, `/metrics` snapshots, Postgres totals, Kafka offsets and container logs.

- The project is separate from the dev stack (`moveo-analytics`): own network and volumes, no published host ports. Both can run at the same time.
- Every run starts from empty volumes and ends with `down -v` (unless `--keep`).
- Users are `lt-<RUN_ID>-u<n>`, so all verification queries filter on that prefix.

## Prerequisites

- Docker Engine with Compose v2+ (tested: Docker Desktop 29.8, Compose 5.5). The Docker VM needs **at least 8 GiB** of memory (Docker Desktop, Settings, Resources). Preflight fails below that; override the floor with `LT_MIN_DOCKER_MEM_GIB`.
- Bash. On Windows use Git Bash. The host needs nothing else: k6, curl, psql, the Kafka tools and Go (for the report) all run in containers.
- Secrets: `deploy/secrets/*.txt` are created from the `*.example` files if missing.
- Images pulled on first use: `grafana/k6:2.3.0`, `curlimages/curl:8.22.0`, `golang:1.27.2`, `apache/kafka:4.3.1`, `postgres:18.6`. The two service images are built by the run.
- Run one harness at a time: preflight aborts if the `moveo-loadtest` project already has containers.

### Resource limits

Every container has explicit limits from `loadtest/compose.loadtest.yaml`. Override any of them with `LT_<SERVICE>_{CPUS,MEMORY,PIDS}`, where `<SERVICE>` is `KAFKA`, `POSTGRES`, `PRODUCER`, `CONSUMER`, `K6`, `ONESHOT` or `PROBE`.

| Service | CPUs | Memory | PIDs | Notes |
|---|---|---|---|---|
| kafka | 1.5 | 1.25G | 1024 | `LT_KAFKA_HEAP_OPTS` default `-Xms768m -Xmx768m` |
| postgres | 1.0 | 768M | 256 | `shm_size: 128mb` |
| producer | 0.75 | 256M | 200 | `LT_PRODUCER_GOMEMLIMIT` default `230MiB` |
| consumer | 1.0 | 384M | 200 | `LT_CONSUMER_GOMEMLIMIT` default `345MiB`; `LT_CONSUMER_WORKERS` default 3 |
| k6 | 1.0 | 768M | 512 | `maxVUs` at most 300 |
| kafka-init, consumer-migrate | 0.5 | 256M | 256 | one-shot (`ONESHOT`) |
| probe (curl) | 0.25 | 64M | 64 | helper used by `run.sh` |
| **Total** | **about 6** | **about 3.7 GiB** | | |

### Profiles

A profile is a file `loadtest/profiles/<name>.env` with `LT_*` limits, one `KEY=value` per line (comments allowed, no quotes, no other keys). Select it with `--profile <name>` on `run.sh` or `run-suite.sh`; the name must match a file, and `--help` lists them. A variable already set in the environment wins over the profile, so `LT_USERS=50000 bash loadtest/run.sh --profile db-bound stress` still works. Without a profile the defaults in the table above apply.

Every profile starts from `equal` (producer, consumer, Kafka and Postgres at 1.5 CPU and 1.25G; `GOMEMLIMIT` 1150MiB for the two Go services; k6 at 2 CPUs) and then starves one component. Run the same scenario with `equal` and with a `*-bound` profile and compare the reports. The total is about 8 CPUs and 6 GiB, within the 12 CPU / 15.5 GiB reference machine.

| Profile | Change from `equal` | What it stresses | Expected symptom |
|---|---|---|---|
| `equal` | none | baseline | no component is the limit; compare the others against it |
| `producer-bound` | producer 0.5 CPU | HTTP handling and Kafka publish in the producer | producer CPU pinned near 50 %, p95 up, dropped iterations; consumer lag stays low |
| `consumer-bound` | consumer 0.25 CPU | polling, batching and flushing in the consumer | consumer CPU pinned, lag grows during the load, long drain time; producer latency unaffected because Kafka buffers |
| `db-bound` | postgres 0.25 CPU, `LT_USERS=200000` | the upserts: with 200 000 users almost every record is a distinct (date, user) row, so each batch is heavy | flush duration up, lag grows, long drain; producer unaffected |
| `kafka-bound` | kafka 0.5 CPU | the broker, on both the produce and the fetch path | producer publish latency up (p95, `kafka_error` results), possibly 503s |

`LT_USERS=200000` reaches k6 (it is a k6 knob; the script accepts up to 10 000 000) and costs no k6 memory, because user ids are generated per request and not kept in a pool.

```bash
bash loadtest/run.sh stress --profile equal              # baseline for the comparison
bash loadtest/run.sh stress --profile producer-bound
bash loadtest/run.sh stress --profile consumer-bound
bash loadtest/run.sh stress --profile db-bound
bash loadtest/run.sh stress --profile kafka-bound
bash loadtest/run-suite.sh --profile consumer-bound load spike   # several scenarios, one profile
```

`stress` (about 13 min) shows the knee best; `load` or `spike` are cheaper. The profile name is recorded in `run.json`, shown in the report title and the Run section, and appended to the run directory and `temp/` file names ([Outputs](#outputs)).

#### Crash under heavy load

The `crash-*` scenarios default to 200 req/s, which the default limits absorb easily. To kill or pause a component while it is busy, raise the rate with `LT_RATE` (it applies to `crash-*`, `load` and `soak`; capped at 3 000 req/s) and give the stack more room with a profile, because the default producer limit (0.75 CPU) cannot serve 1 000 req/s:

```bash
LT_RATE=1000 bash loadtest/run-suite.sh --profile equal crash-consumer-kill crash-consumer-stop crash-postgres-pause crash-kafka-stop crash-scale-out crash-commit-gap poison-records
LT_RATE=1000 bash loadtest/run.sh crash-consumer-kill --profile consumer-bound    # one crash on an already saturated consumer
```

The correctness checks do not change: no loss and no double counting, whatever the rate. What changes is how much has to be absorbed: after the consumer is killed, the lag has grown by roughly `rate x down time` records and the drain takes longer. The strict `{phase:steady}` thresholds (p95 below 300 ms) can fail at high rates even without the crash, if the producer is the limit; read the Ingestion section before blaming the failure.

### Safety rails

- k6 caps the rate at 3 000 req/s, `maxVUs` at 300 and the duration at 2 h, whatever the env says.
- k6 aborts a run (`abortOnFail`, evaluated after 30 s) when `http_req_failed` exceeds 10 % or p95 exceeds 2 s. Crash runs abort at 60 % failures or p50 above 2 s. The report still runs afterwards (k6 exit code 108).
- Container logs use the `local` driver, 10 MB x 3 per container.
- A `trap` on EXIT, INT and TERM stops the samplers and runs `down -v` (unless `--keep`).

## Run a scenario

```bash
bash loadtest/run.sh smoke                  # correctness check
bash loadtest/run.sh load                   # steady load
bash loadtest/run.sh crash-consumer-kill    # SIGKILL the consumer mid-load
bash loadtest/run.sh --help                 # usage
```

On success the script prints the results path and the verdict, for example `verdict: PASS (k6 exit 0, drain 5.0s)`.

### Run the full suite

`loadtest/run-suite.sh` runs several scenarios one after another through `run.sh`. Soak is optional because it takes about 34 min.

```bash
bash loadtest/run-suite.sh                  # smoke, load, stress, spike, the five crash-*, crash-commit-gap and poison-records (about 1 h 15 min)
bash loadtest/run-suite.sh --soak           # the same, then soak (about 1 h 50 min)
bash loadtest/run-suite.sh load spike       # only the named scenarios
bash loadtest/run-suite.sh --profile equal load spike   # every run with one resource profile
bash loadtest/run-suite.sh --name baseline  # name the experiment "baseline" (default: suite)
```

| Option | Effect |
|---|---|
| `--soak` | Also run `soak`, after the other scenarios. |
| `--profile <name>` | Pass `--profile <name>` to every run ([Profiles](#profiles)). Fails before starting if the profile does not exist. |
| `--name <slug>` | Name of the experiment (see below). Letters, digits, `.`, `_`, `-` and `@`; anything else becomes `-`. Default: `suite`, or `suite@<profile>` with `--profile`. |
| `--no-build` | Do not build images, not even for the first run. Without it, only the first run builds; the others reuse the images. |
| `--help`, `-h` | Print usage. |

- `LT_*` overrides apply to every run in the suite, for example equal limits for all services: `LT_PRODUCER_CPUS=1.5 LT_CONSUMER_CPUS=1.5 LT_KAFKA_CPUS=1.5 LT_POSTGRES_CPUS=1.5 bash loadtest/run-suite.sh`.
- A failing scenario does not stop the suite.
- **Experiment.** One `run-suite.sh` call is one experiment: its id is the suite's start time (`SUITE_ID`, the same UTC timestamp as the suite directory) and its name comes from `--name`. Both are passed to every `run.sh` (`LT_EXPERIMENT_ID`, `LT_EXPERIMENT_NAME`), written to `run.json` (`experiment.id`, `experiment.name`), shown in the Run section of each report and used to group the report copies in `temp/` ([Report copies](#report-copies-temp)). A standalone `run.sh` is an experiment of its own: id = `RUN_ID`, name = `<scenario>[@<profile>]`. Use names to tell comparable sets apart, for example `--name baseline` before a change and `--name after-batching` after it.
- Each run writes its usual run directory and `temp/` copies. The suite adds `loadtest/results/<SUITE_ID>-suite/` with `summary.txt` (one line per scenario: verdict, start and end time) and `<scenario>.log` (the console output of that run).
- Exit code: 0 if every scenario passed, 1 if any failed, 2 if any harness error.

### Scenarios

All scenarios send `POST /v1/activities` from inside the network to `http://producer:8080`. About 60 % of events are `page_view`; the rest are `click`, `scroll`, `purchase`, `login` and unknown types. About 5 % of timestamps fall on the previous UTC day. Users come from a skewed pool (default 5 000), so a few users are hot.

| Scenario | Load shape | k6 phase | Wall clock | Purpose |
|---|---|---|---|---|
| `smoke` | 1 VU, 200 events | seconds | 1-3 min (about 1 min with images cached) | correctness: the database equals what was accepted, exactly |
| `load` | 300 req/s constant | 5 min | about 8 min | steady-state latency, lag, resources |
| `stress` | 100 to 1 500 req/s in 7 steps | 10 min | about 13 min | find the knee |
| `spike` | 50 req/s, then 1 500 req/s for 30 s, then 50 | about 4 min | about 7 min | burst absorption and drain time |
| `soak` | 150 req/s constant | 30 min | about 34 min | memory growth and leaks |
| `crash-*` | 200 req/s constant | 3 min | about 6 min | resilience under one failure |
| `crash-commit-gap` | 200 req/s constant | 3 min | about 6 min | the case the `consumer_offsets` table exists for: records in Postgres that Kafka never saw committed |
| `poison-records` | 200 req/s constant | 3 min | about 6 min | the DLQ path end to end: malformed records written straight to the topic |

`crash-commit-gap` and `poison-records` use the `crash` k6 load (`LT_RATE` applies) and its timeline, like the `crash-*` scenarios. Wall clock adds stack start (about 1 min, longer on the first build), the drain wait and the report. Times are indicative.

### Crash scenarios

The chaos action runs in the background, timed from the k6 start, and is logged in `timeline.jsonl` and `chaos.log`.

| Scenario | Action | Extra check (besides the common ones) |
|---|---|---|
| `crash-consumer-kill` | `kill -s SIGKILL consumer` at 60 s, restart at 75 s | none; the restarted consumer resumes from the positions stored in Postgres. `recovered` is reported in the timeline |
| `crash-consumer-stop` | `stop -t 30 consumer` (SIGTERM) at 60 s, restart at 75 s | consumer exit code is 0 |
| `crash-postgres-pause` | `pause postgres` at 60 s, `unpause` at 90 s | DLQ holds 0 records; `postgres` retries are reported |
| `crash-kafka-stop` | `stop kafka` at 60 s, restart at 80 s | none; producer 5xx only in the outage window, recovery time to `/readyz` is reported |
| `crash-scale-out` | `up -d --scale consumer=2` at 60 s | none; rebalance without loss or overcount |
| `crash-commit-gap` | `stop kafka` at 60 s, SIGKILL the consumer at 70 s (Kafka still down), `start kafka` at 85 s, start the consumer at 95 s | **Commit gap exercised**: stored position greater than the committed offset on at least one partition on resume, otherwise FAIL. Producer 5xx in the outage window is expected, as in `crash-kafka-stop` |
| `poison-records` | at 60 s, write `LT_POISON_COUNT` (50) malformed records to `incoming.user_activity` from inside the Kafka container | DLQ holds exactly the injected records, in the injected error classes; lag drains (no stalled partition) |

#### `crash-commit-gap`: the case the offsets table exists for

The consumer stores its position per partition in `consumer_offsets`, in the same transaction as the aggregates, and commits to Kafka right after (for lag visibility). If the process dies, or Kafka is unreachable, between the two, the database is ahead of Kafka: `stored_offset > committed_offset` on resume. Resuming from the Kafka offset would apply those records a second time; resuming from the stored position does not. The other crash runs rarely hit that window. This scenario builds it:

1. Kafka stops at 60 s. The consumer keeps flushing what it already fetched (up to one `FLUSH_INTERVAL` of records per partition, about 1 s) to Postgres, and every Kafka offset commit fails.
2. The consumer is SIGKILLed at 70 s, while Kafka is still down, so no later commit can close the gap.
3. Kafka starts at 85 s, the consumer at 95 s. It logs `resuming from the stored position` with `stored_offset` and `committed_offset` for each partition, and commits the stored position to Kafka before its first batch.

Why it is reliable: the producer feeds every partition continuously (about 33 records per second each at the default 200 req/s), and a worker flushes its batch only when the 1 s timer fires, so when the broker goes away every partition has a batch that was fetched but not yet flushed. That batch reaches Postgres after Kafka is gone, and its commit fails. Nothing else needs to line up. The report proves it instead of assuming it: the **Commit gap exercised** check fails if no partition resumed with a positive gap. No code in the services is involved; the failure is injected from outside, with the same Docker actions as the other crash scenarios.

Timings are configurable, all in seconds after the k6 start: `LT_CHAOS_AT` (Kafka stop, 60), `LT_GAP_KILL_AT` (70), `LT_GAP_KAFKA_START_AT` (85), `LT_GAP_CONSUMER_START_AT` (95). They must be in increasing order. The k6 outage window is `[LT_CHAOS_AT - 10, LT_GAP_KAFKA_START_AT + LT_CHAOS_RECOVERY]`.

#### `poison-records`: records the producer would refuse

The producer answers 400 to invalid input, so no other run sends a record to the DLQ. This scenario writes the records straight to the input topic with `kafka-console-producer.sh` (via `docker exec -i` in the Kafka container, with a small heap so the broker's memory limit is safe), using keys `poison-<RUN_ID>-<n>`. The `poison_injected` event in `timeline.jsonl` records the count, the kinds and the error class each record must get. Kinds are cycled in this order:

| Kind | Example | Consumer path | DLQ `error_class` |
|---|---|---|---|
| `invalid_json` | `{not json ...` | `json.Unmarshal` fails | `decode` |
| `truncated_json` | valid object without the closing brace | `json.Unmarshal` fails | `decode` |
| `missing_fields` | `{"note":"..."}` | valid JSON, `user_id` empty | `validation` |
| `missing_timestamp` | no `timestamp` | valid JSON, zero timestamp | `validation` |
| `wrong_type_user_id` | `"user_id":12345` | number into a string field: `json.Unmarshal` fails | `decode` |
| `control_char_user_id` | `"user_id":"...\u0001x"` | valid JSON, control character in `user_id` | `validation` |
| `zero_timestamp` | `"timestamp":"0001-01-01T00:00:00Z"` | valid JSON, `Time.IsZero` | `validation` |
| `invalid_timestamp` | `"timestamp":"yesterday"` | not an RFC 3339 time: `json.Unmarshal` fails | `decode` |

The split follows `decodeRecord` in `services/consumer/internal/adapters/kafka/message.go`: anything `encoding/json` rejects is `decode`, anything the domain rules reject after a clean decode is `validation`. With 50 records: 26 `decode` and 24 `validation`. "Records in input topic" now includes the poison records, so it is larger than the k6 accepted count by `LT_POISON_COUNT`; the effectively-once check (`db = topic - DLQ`) accounts for it and the accepted-versus-database checks are unchanged.

### Options

| Option | Effect |
|---|---|
| `--profile <name>` | Load `loadtest/profiles/<name>.env` (resource limits that make one component the bottleneck). Variables already set in the environment win. See [Profiles](#profiles). |
| `--keep` | Leave the stack and its volumes up afterwards for inspection. Remove it with `docker compose -p moveo-loadtest -f compose.yaml -f loadtest/compose.loadtest.yaml --profile loadtest down -v`. |
| `--no-build` | Do not build; use existing `moveo-analytics/producer:dev` and `moveo-analytics/consumer:dev`. |
| `--help`, `-h` | Print usage. |

### Environment overrides

Set them in front of the command, for example `LT_RATE=500 LT_DURATION=2m bash loadtest/run.sh load`.

| Group | Variables (default) |
|---|---|
| k6 load shape | `LT_RATE` (`load`, `soak`, `crash-*`), `LT_START_RATE`, `LT_PEAK_RATE`, `LT_DURATION`, `LT_ITERATIONS`, `LT_PRE_ALLOCATED_VUS`, `LT_MAX_VUS` (at most 300) |
| k6 payload | `LT_USERS` (5000), `LT_INVALID_RATIO` (0, share of deliberately invalid payloads, expected 400), `LT_PREV_DAY_RATIO` (0.05), `LT_REQUEST_TIMEOUT` (10s) |
| Chaos (`crash-*`) | `LT_CHAOS_AT` (60 s), `LT_CHAOS_DOWN_FOR` (per scenario: 15, 15, 30, 20 s, 0 for scale-out, 25 s for commit-gap), `LT_CHAOS_RECOVERY` (30 s, extra seconds tagged `phase=outage`; 0 for `poison-records`) |
| `crash-commit-gap` | `LT_GAP_KILL_AT` (70), `LT_GAP_KAFKA_START_AT` (85), `LT_GAP_CONSUMER_START_AT` (95), seconds after the k6 start; the Kafka stop is `LT_CHAOS_AT` |
| `poison-records` | `LT_POISON_COUNT` (50, 1 to 100 000) injected at `LT_CHAOS_AT` |
| Experiment | `LT_EXPERIMENT_ID`, `LT_EXPERIMENT_NAME`: set by `run-suite.sh`; leave them unset for a standalone run |
| Harness | `LT_UP_TIMEOUT` (300 s), `LT_DRAIN_TIMEOUT` (300 s), `LT_DRAIN_POLL` (5 s), `LT_RECOVERY_TIMEOUT` (120 s), `LT_MIN_DOCKER_MEM_GIB` (8) |
| Consumer under test | `LT_CONSUMER_WORKERS` (3), `LT_FLUSH_MAX_RECORDS` (500) |
| Limits | `LT_<SERVICE>_{CPUS,MEMORY,PIDS}`, `LT_PRODUCER_GOMEMLIMIT`, `LT_CONSUMER_GOMEMLIMIT`, `LT_KAFKA_HEAP_OPTS` |
| Dashboard | `LT_K6_DASHBOARD_PERIOD` (5s) |
| Compose overlay | `LT_COMPOSE_OVERLAY` (extra Compose file, see [E2E coverage](#e2e-coverage)) |

### Exit codes

| Code | Meaning |
|---|---|
| 0 | PASS |
| 1 | FAIL: a threshold or an expectation was not met |
| 2 | Harness error (preflight, stack start, report); no verdict |

### What `run.sh` does

1. Preflight: Docker reachable, memory at least 8 GiB, no `moveo-loadtest` containers, secrets present, Dockerfiles (or images with `--no-build`) present.
2. If `--profile` is given, loads the profile (variables already set win). `RUN_ID` is the UTC time (`20261012t030000z`); the results directory is `loadtest/results/<RUN_ID>-<scenario>/`, or `<RUN_ID>-<scenario>@<profile>/` with a profile.
3. `docker compose -p moveo-loadtest -f compose.yaml -f loadtest/compose.loadtest.yaml up -d --build --wait`. k6 and the probe sit behind the `loadtest` profile and are not started here.
4. Starts the curl probe (consumer lag every 5 s, `/metrics`, `/readyz`) and a `docker stats` sampler, and takes the baseline `/metrics` snapshots.
5. Runs k6, and for `crash-*` and `poison-records` the scenario timeline (chaos actions or the poison injection) in the background.
6. Drain: polls `kafka-consumer-groups.sh --describe --group analytics-consumer` until the total lag is 0 or `LT_DRAIN_TIMEOUT`.
7. Collects final `/metrics`, Postgres totals for the run's users, topic and DLQ end offsets, `docker inspect` and logs.
8. Writes `run.json`, then runs `loadtest/report` in a `golang:1.27.2` container to produce `report.md` and `report.html`.
9. `down -v` (unless `--keep`), copies the report to `temp/`, prints the verdict and exits with the report's code.

## Outputs

A run produces two kinds of output: the full **run directory** and a **copy of the report** in `temp/`. Both are gitignored.
`run-suite.sh` also writes a **suite directory**, `loadtest/results/<SUITE_ID>-suite/`, with `summary.txt` and one console log per scenario ([Run the full suite](#run-the-full-suite)).

### Run directory: `loadtest/results/<RUN_ID>-<scenario>[@<profile>]/`

Examples: `loadtest/results/20261012t030000z-smoke/` (no profile), `loadtest/results/20261012t030000z-stress@producer-bound/`. Start with `report.md` or `report.html`. Everything else is raw data kept so the report can be regenerated or audited. A file may be missing or empty when its collection step failed; the failure is then a `collect_error` event in `timeline.jsonl`, and the report shows "Not available" for that part.

| File | Written by | What it is |
|---|---|---|
| `report.md` | report | The verdict and all report sections as text. Start here. |
| `report.html` | report | The same report, self-contained (inline CSS and SVG), with CPU, memory and lag charts. Open it in a browser. |
| `k6-dashboard.html` | k6 | k6's own time-series dashboard (request rate, latency, VUs). Open in a browser. Absent for very short runs: `smoke` does not produce it. |
| `k6-summary.json` | k6 `handleSummary` | End-of-test metrics: latency percentiles, status counts, thresholds, and the custom counters `events_accepted`, `page_views_accepted`, `events_accepted_prev_day`, `events_unknown`, `events_rejected`, `invalid_sent`. Main input of the report. |
| `k6-output.log` | run.sh | k6 console output, ending with its text summary. |
| `run.json` | run.sh | Run metadata: scenario, `profile` (name or `null`), `experiment` (`id`, `name`), `RUN_ID`, start/end times, k6 exit code, drain seconds, Docker host (CPUs, memory, versions), image tags, limits, configuration (topic, partitions, workers, k6 knobs, chaos timing) and expectations (`overcount_max` 0, `consumer_stop_exit_code`). Always written. |
| `stats.jsonl` | run.sh | One `docker stats` sample per container about every 2 s: CPU %, memory, network, PIDs. Feeds the Resources section and charts. |
| `lag.jsonl` | probe | Consumer lag every 5 s: `{"ts", "up", "lag"}`. The lag is the sum of `consumer_lag_records` from the consumer's `/metrics` (the consumer refreshes it every 15 s). Feeds the lag chart and "Max lag". |
| `timeline.jsonl` | run.sh | Ordered run events with UTC timestamps: `run_start`, `stack_up`, `k6_start`, `chaos`, `recovered`, `poison_injected` (count, kinds and expected DLQ classes), `k6_end`, `drain_start`, `drain_end`, `collect_start`, `collect_end`, `collect_error`, `run_end`. Feeds the Chaos timeline. |
| `drain.json` | run.sh | Drain result: `seconds` (k6 end to first poll with total lag 0), `timed_out`, `final_lag`, number of polls and every sample. |
| `metrics-before-producer.txt`, `metrics-before-consumer.txt` | probe | Prometheus `/metrics` text taken right after the stack is up (baseline). |
| `metrics-after-producer.txt`, `metrics-after-consumer.txt` | probe | The same after the drain. The report subtracts before from after. |
| `db-totals.csv` | psql | One row for the run's users: `activity_count`, `page_view_count`, `distinct_users`, `rows`. Header only if the query failed. |
| `db-by-date.csv` | psql | One row per `activity_date` for the run's users: rows, distinct users, counts. Shows the previous-UTC-day rows. |
| `topic-offsets.txt` | Kafka tools | End offsets of `incoming.user_activity`, one `topic:partition:offset` per line. The sum is the number of records produced. |
| `dlq-offsets.txt` | Kafka tools | The same for `incoming.user_activity.dlq`. The sum is the number of dead letters. |
| `inspect.jsonl` | docker | One line per container at the end: status, `oom_killed`, `restart_count`, `exit_code`, limits, image. |
| `logs/<service>.log` | docker compose | Full log of each Compose service: `kafka`, `kafka-init`, `postgres`, `consumer-migrate`, `producer`, `consumer`, plus `k6` and `probe` (usually empty: k6 output goes to `k6-output.log`). Go services write one JSON object per line. |
| `logs-summary.txt` | run.sh | Tab-separated lines, ERROR and WARN counts per service. |
| `chaos.log` | run.sh | Output of the chaos commands. Written by `crash-*` and `poison-records` runs only (a `smoke` run has no such file). |

The exact formats are documented in the comment block at the top of [`loadtest/run.sh`](../loadtest/run.sh).

### Report copies: `temp/`

After each run, `loadtest/publish-report.sh` copies `report.md` and `report.html` into `temp/` at the repository root, in the folder of the run's **experiment**, and rebuilds the two indexes. An experiment is the set of runs of one `run-suite.sh` call (or one standalone `run.sh`); `run.sh` records it in `run.json`. The script also works on its own: `bash loadtest/publish-report.sh loadtest/results/<RUN_ID>-<scenario>[@<profile>]` for one run, `bash loadtest/publish-report.sh --all` to regenerate `temp/` from `loadtest/results/`.

```text
temp/
├── README.md                                   index of experiments, newest first
├── 2026-10-12_030000Z_suite/                   one experiment: <YYYY-MM-DD_HHMMSSZ>_<name>
│   ├── README.md                               its runs in run order: time, scenario, profile, what it tests, result, links
│   ├── 030012Z_smoke_PASS.md                   report as text:   <HHMMSSZ>_<scenario>[@<profile>]_<PASS|FAIL>
│   ├── 030012Z_smoke_PASS.html                 same report with charts
│   ├── 030541Z_crash-commit-gap_PASS.md
│   └── ...
└── 2026-10-12_091500Z_stress@producer-bound/   a standalone run: its own experiment
    ├── README.md
    └── 091500Z_stress@producer-bound_FAIL.md
```

- `temp/README.md` has one row per experiment: date and time (UTC), name, number of runs, PASS and FAIL counts (anything not PASS counts as FAIL), and a link to the folder.
- The experiment folder name is the experiment id as a timestamp, then the name (a suite: `suite`, `suite@<profile>` or the `--name` slug); names contain no `:` so they are valid on Windows. The time in a file name is the run's start time, from its `RUN_ID`. The result suffix is the first line of `report.md` (`ERROR` if it has no verdict).
- Only the two report files are copied; raw data, logs and the k6 dashboard stay in the run directory.
- `--all` deletes everything in `temp/` first (it is only copies) and rebuilds it. Runs written before experiments existed have no `experiment` in `run.json`: such a run joins the experiment `suite` (plus `@<profile>` when its directory name has a profile) of the newest `loadtest/results/<SUITE_ID>-suite/*.log` that names its directory; any other run becomes an experiment of its own.

## How to read the report

`report.md` and `report.html` have the same sections. The title line reads `Load test report - <scenario>: PASS` (or `FAIL`; with a profile, `<scenario> (<profile>)`, for example `stress (producer-bound)`), followed by the run ID and `N checks, M failed`. Runs with a scenario timeline (`crash-*`, `poison-records`) have a section 8 (Chaos timeline). Runs in which the consumer started more than once have a Recovery section after it. Artifacts is always last: 8, 9 or 10.

1. **Verdict.** One row per check with the expected value, the actual value and PASS/FAIL. The run passes only if every row passes. A check whose input is missing fails: a report that cannot prove correctness never says PASS. See [PASS criteria](#pass-criteria).
2. **Run.** Scenario, profile (or "none"), experiment (name and id), run ID, start and end times, load duration, k6 exit code, Docker host. Then three tables: **Images** (tags under test), **Resource limits** and **Configuration** (topic, partitions, workers, flush size, k6 knobs, chaos timing). Use it to compare two runs like for like.
3. **Ingestion (producer).** What k6 sent: requests, achieved rate against the target, iterations and **dropped** iterations, accepted (202), rejected (4xx), unknown (timeout or 5xx), `http_req_failed` and `checks`. Then the status breakdown, latency (avg, p50, p90, p95, p99, max) and the producer's own `activities_published_total` by result (`ok`, `invalid`, `kafka_error`).
   - Dropped iterations above 0 mean k6 could not start requests on time (producer too slow or `maxVUs` reached): the achieved rate is below the target.
4. **Pipeline (consumer).** Events consumed, throughput (events/s), records in the input topic, **max lag**, final lag, **drain time** (last request to lag 0, or "timed out"), flush count and duration p50/p95/p99, retries by dependency (`kafka`, `postgres`), DLQ records and their error classes, offset commit failures (informational) and fenced batches (`offset_fence_rejections_total`). A note is added when the consumer restarted during the run, because its counters then cover the last process only. In `poison-records`, two more rows show the injected records (count and kinds) and the DLQ error classes they should produce. A large drain time means the consumer could not keep up; look at consumer CPU in Resources and at the lag chart.
5. **Correctness.** The core of the report. Compares what k6 saw accepted with what Postgres holds for the run's users:
   - **Overcount** is `db - accepted`. The allowed range is `0 ... 0 + events_unknown`.
   - A **negative** overcount is **loss** and always fails.
   - A positive overcount above `events_unknown` is double counting and fails. `events_unknown` are requests that timed out or got a 5xx: they may or may not have reached Kafka, so each can legitimately add one. That is client-side uncertainty, not consumer double counting.
   - The same comparison is made for page views.
   - **Effectively once**: `SUM(activity_count) = records in the input topic - records in the DLQ`. The topic's end offsets are the ground truth for "produced" (they include the 5xx and timeout requests that did reach Kafka), so this check has no allowance: any difference is a consumer that lost or double-counted records.
   - Below the tables: distinct users and rows per `activity_date` (two dates, because about 5 % of events are on the previous UTC day).
6. **Resources.** Per container: CPU average and peak (docker's figure, 100 % is one core), memory peak against the limit, peak/limit, network rx/tx, OOMKilled, restarts, exit code, status. CPU and memory figures use only the `docker stats` samples between the `k6_start` and `k6_end` timeline events, so container start-up and the drain wait do not inflate the peaks; if those events are missing the table uses all samples and a note below it says so. In `report.html`: charts for CPU, memory and consumer lag over the whole run, with chaos actions as dashed lines and the k6 window marked on the CPU and memory charts. A peak close to the limit or any OOMKilled means the limit was hit.
7. **Errors.** Lines, ERROR and WARN counts per service, and the top messages. Some WARN/ERROR lines are expected in crash runs (for example connection errors while Kafka is stopped).
8. **Chaos timeline** (`crash-*` and `poison-records` runs only). Time, offset relative to the k6 start, event and details: the action (`kill`, `stop`, `pause`, `scale`), its exit codes, `recovered` with the seconds until `/readyz` answered, and `poison_injected` with the injected counts.
9. **Recovery** (every run in which the consumer started more than once: a kill, a stop, a second replica; always for `crash-commit-gap`). Built from the resume lines in `logs/consumer.log`. Each time a consumer starts, it logs for every partition it receives either `resuming from the stored position` (with `stored_offset` and `committed_offset`) or `no stored position; resuming from the Kafka committed offset` (with `committed_offset`, or `has_committed_offset=false` when nothing is committed yet). The section lists the events after the first start:
   - A table with one row per event: time, member, partition, stored offset, committed offset and **gap** (`stored - committed`; a partition with nothing committed counts from 0; "none" means no stored row).
   - **Records protected from double counting by the stored position**: the sum of the positive gaps. These are records that Postgres already held when the consumer came back but that Kafka's committed offset did not cover. Resuming from the Kafka offset would have applied them twice; the stored position skipped them. The overcount check proves that none was counted twice.
   - When every gap is 0 the section says so: "Stored == committed on every partition: the commit gap was not exercised in this run." That is normal for a plain kill or stop, where the last Kafka commit usually keeps up with the database, and it is why `crash-commit-gap` exists.
   - The fence counters `offset_fence_rejections_total` and `offset_fence_resyncs_total` (after minus before, last process only if the consumer restarted), when the consumer exports them. Non-zero means another owner moved a stored position and a batch was dropped and re-read.
10. **Artifacts.** Links to the raw files in the run directory that exist for this run, plus **Collection notes** about any file that could not be parsed.

### PASS criteria

The rules live in [`loadtest/report/verdict.go`](../loadtest/report/verdict.go); k6 thresholds are in [`loadtest/k6/activities.js`](../loadtest/k6/activities.js).

Common to every scenario:

| Check | Pass when |
|---|---|
| Each k6 threshold | not crossed (table below) |
| k6 run completed | k6 exit code 0 (99 means thresholds crossed, 108 means aborted by a runaway guard) |
| Events accepted | greater than 0 |
| No lost events | `db >= accepted` |
| Overcount within bound | `db - accepted <= 0 + events_unknown` |
| Page views | no loss, overcount within the same bound |
| Effectively once | `db = input topic records - DLQ records`, exactly |
| No container OOMKilled | none |
| Consumer drained | lag reached 0 before `LT_DRAIN_TIMEOUT` |

Added per scenario:

| Scenario | k6 thresholds | Extra check |
|---|---|---|
| `smoke`, `load`, `spike`, `soak` | `http_req_failed` rate < 1 %, `http_req_duration` p(95) < 300 ms, `checks` rate > 99 % | none |
| `stress` | `http_req_failed` < 5 %, p(95) < 1 s, `checks` > 95 % | none |
| `crash-*` | the strict thresholds apply only to requests outside the outage window (`{phase:steady}`) | see below |
| `crash-postgres-pause` | as `crash-*` | No DLQ records (0) |
| `crash-consumer-stop` | as `crash-*` | Consumer exit code on SIGTERM is 0 |
| `crash-commit-gap` | as `crash-*` | **Commit gap exercised**: at least one partition resumed with `stored_offset > committed_offset`. FAIL if every gap is 0 or the resume lines are missing: the run then repeated a plain kill and did not test what it claims. The common checks (no loss, zero overcount, effectively once, drained) must hold as well |
| `poison-records` | as `crash-*` (the outage window is only the injection second) | **DLQ records = injected poison records** (topic end offsets against `poison_injected`); **DLQ by error class matches the injected mix** (`dlq_records_total{error_class}` against the injected `decode`/`validation` counts, no other class); **No partition stalled** (final lag 0 before the timeout). Effectively once holds with the poison records on the topic: `db = topic - DLQ` |

The outage window is `[LT_CHAOS_AT - 10 s, LT_CHAOS_AT + LT_CHAOS_DOWN_FOR + LT_CHAOS_RECOVERY]` from the k6 start. The overcount bound is **0 in every scenario**, crashes included, because the consumer stores its Kafka positions in the same transaction as the aggregates ([ADR 0008](adrs/0008-consumer-concurrency-batching-and-delivery-semantics.md)). The tables above are the contract; the stress run is the one that is expected to find a limit, so read its latency and dropped iterations even when it passes.

## E2E coverage

To see which production code a run exercises, build the services with `go build -cover` and collect the coverage data they write on exit ([ADR 0014](adrs/0014-testing-strategy-and-quality-tooling.md)). It is opt-in; the normal images are unchanged.

```bash
mkdir -p -m 777 loadtest/results/coverage-data      # must be writable by uid 65532
LT_COMPOSE_OVERLAY=loadtest/compose.coverage.yaml bash loadtest/run.sh smoke
go tool covdata percent -i=loadtest/results/coverage-data
```

- [`loadtest/compose.coverage.yaml`](../loadtest/compose.coverage.yaml) builds with `COVER=1`, tags `moveo-analytics/<service>:cover` (so `:dev` is never overwritten), sets `GOCOVERDIR=/coverdata` and mounts the host directory. Change the directory with `LT_COVERAGE_DIR`. Do not pass `--no-build`.
- Data is written when the process exits. The final `down -v` stops containers with SIGTERM, which is enough. A killed instance (`crash-consumer-kill`) writes nothing, so use `smoke` or `load`.
- Other `covdata` commands work on the same directory, for example `go tool covdata textfmt -i=<dir> -o=e2e.out`, then `go tool cover -html=e2e.out` from inside the service module.

## Windows notes

- Run from Git Bash. The script exports `MSYS_NO_PATHCONV=1` so container paths such as `/opt/kafka/...` are not rewritten.
- `.gitattributes` keeps `*.sh` with LF endings; a CRLF `run.sh` fails with `$'\r': command not found`.
- If Docker Desktop is short on memory, raise it in Settings, Resources.

## Manual use

The overlay works without `run.sh`, for example to keep a stack up and send ad-hoc load:

```bash
export LT_RESULTS_DIR=./loadtest/results/adhoc
dc="docker compose -p moveo-loadtest -f compose.yaml -f loadtest/compose.loadtest.yaml"
$dc up -d --build --wait
SCENARIO=smoke RUN_ID=adhoc $dc run --rm --no-deps k6
$dc --profile loadtest down -v
```

The report step caches Go builds in the Docker volume `moveo-loadtest-gocache`; remove it with `docker volume rm moveo-loadtest-gocache`.

## Report module (developers)

[`loadtest/report`](../loadtest/report) is a stdlib-only Go module that turns a run directory into `report.md` and `report.html`. Run it by hand on an existing run directory, for example:

```bash
cd loadtest/report
GOWORK=off go run . ../results/<RUN_ID>-<scenario>      # exit 0 PASS, 1 FAIL, 2 error
GOWORK=off go test ./...
```

Layout: `parse_*.go` read each input file, `build.go` assembles the report model, `verdict.go` holds the pass/fail rules, `report.tmpl.md` and `report.tmpl.html` are the templates, `recovery.go` parses the consumer log for the Recovery section, and `testdata/` holds fixture runs (a SIGKILL, a commit gap, poison records) with golden `report.md` files; refresh them with `go test -update`.
