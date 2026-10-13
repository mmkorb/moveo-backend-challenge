# Load test results

> **TL;DR** — All 24 runs on 2026-10-12 passed: with equal limits the pipeline kept up with every stress step up to 3000 req/s (the harness maximum) at p95 64 ms, and Kafka saturated first (from 1757 req/s), then producer, consumer, Postgres. All crash runs showed zero loss and zero overcount, including a commit-gap run that skipped 462 already-applied records.

Results of the load, stress, crash and soak runs made on 2026-10-12 with the harness described in [load-testing.md](load-testing.md) ([ADR 0016](adrs/0016-load-and-crash-testing.md)). Every number comes from a run directory (`report.md`, `k6-summary.json`, `k6-dashboard.html`, `stats.jsonl`, `lag.jsonl`, `timeline.jsonl`, `logs/consumer.log`). Values marked **(derived)** are computed from those raw files, not printed in a report. CPU % is docker's figure: 100 % is one core, so a 1.5 CPU limit is 150 %. The scaling decision built on these results is [ADR 0017](adrs/0017-scaling-and-capacity.md).

## 1. Setup and method

**Host.** Docker Desktop 29.8.2, Compose 5.5.1, 12 CPUs and 15.5 GiB for the Docker VM (`run.json`). k6 runs on the same host, inside the Compose project.

**Limits.** Topic `incoming.user_activity` with 6 partitions, one consumer replica with `CONSUMER_WORKERS=3`, `FLUSH_MAX_RECORDS=500`, 5 000 users unless stated.

| Limits | producer | consumer | kafka | postgres | k6 | Used by |
|---|---|---|---|---|---|---|
| equal (env vars or `--profile equal`) | 1.5 CPU / 1.25G | 1.5 / 1.25G | 1.5 / 1.25G | 1.5 / 1.25G | 2 / 768M | every run except `043214Z` |
| harness defaults | 0.75 / 256M | 1.0 / 384M | 1.5 / 1.25G | 1.0 / 768M | 1.0 / 768M | `043214Z_stress` |

Each `*-bound` profile starts from equal and starves one component: `producer-bound` producer 0.5 CPU, `consumer-bound` consumer 0.25 CPU, `db-bound` Postgres 0.25 CPU with `LT_USERS=200000`, `kafka-bound` Kafka 0.5 CPU.

**Stress shape.** 7 steps from 100 req/s to the peak, each a 10 s ramp plus a 75 s hold (`loadtest/k6/activities.js`). With `LT_PEAK_RATE=3000` the steps are 514, 929, 1343, 1757, 2171, 2586 and 3000 req/s; with the default peak they are 300 to 1500 in steps of 200. k6 uses at most 300 VUs.

**Per-step numbers (derived).** Achieved rate, dropped iterations and p95 come from the 5 s snapshots embedded in `k6-dashboard.html`, restricted to the 75 s hold of each step; p95 is the median of the 5 s p95 values. CPU is the mean of the `stats.jsonl` samples (about every 2 s) in the same hold window. Max lag is the largest `lag.jsonl` sample in it.

**Reports from before the k6-window fix** (every run before `154924Z`) give CPU over all samples. This document recomputes CPU and memory for the k6 window only. For the load, spike, stress and soak runs used here, the CPU figures differ from the reported ones by at most 0.8 percentage points, because the sampler starts close to `k6_start`.

**Reproduce** (from the repository root, in Git Bash):

```bash
bash loadtest/run-suite.sh --profile equal --soak smoke load spike crash-consumer-stop crash-consumer-kill \
  crash-postgres-pause crash-kafka-stop crash-scale-out             # 134708Z (it set the same limits via LT_* env vars)
bash loadtest/run.sh stress --profile equal                          # 133558Z (peak 1500)
LT_PEAK_RATE=3000 bash loadtest/run.sh stress --profile equal        # 145346Z, 154924Z
bash loadtest/run.sh stress                                          # 043214Z (harness defaults)
for p in producer-bound consumer-bound db-bound kafka-bound; do
  LT_PEAK_RATE=3000 bash loadtest/run-suite.sh --profile "$p" stress  # 160024Z ... 163356Z
done
LT_RATE=1000 bash loadtest/run-suite.sh --profile equal crash-consumer-stop crash-consumer-kill \
  crash-postgres-pause crash-kafka-stop crash-scale-out             # 164604Z
LT_RATE=1000 bash loadtest/run-suite.sh --profile equal --name recovery-dlq crash-commit-gap poison-records  # 170735Z
```

All 24 runs listed in section 8 passed (`PASS`, 0 failed checks).

## 2. Headline

**With equal limits the pipeline kept up with every stress step up to 3000 req/s, the harness's maximum rate.** In run `154927Z` (stress@equal) the 3000 req/s step achieved 2991 req/s. The whole run accepted 1,036,295 events with 0 failed requests, p95 64.3 ms and p99 96.0 ms, max consumer lag 1,854 and a drain of 4.3 s, and `db = topic − DLQ` held exactly. The ceiling of the system was not reached. What stopped the test was the load generator: k6 hit its 300 VU cap ("Insufficient VUs" at 15:57:54, during the 2586 req/s step).

**Kafka saturates first.** Per step, `154927Z` (derived):

| Target req/s | 514 | 929 | 1343 | 1757 | 2171 | 2586 | 3000 |
|---|---|---|---|---|---|---|---|
| Achieved req/s | 508 | 929 | 1343 | 1749 | 2149 | 2540 | 2991 |
| Dropped iterations | 0 | 16 | 31 | 531 | 1,611 | 3,138 | 594 |
| p95 ms (median of 5 s windows) | 1.6 | 12.6 | 2.1 | 56.1 | 69.4 | 64.2 | 56.1 |
| Max VUs | 33 | 49 | 65 | 121 | 250 | 294 | 197 |
| Kafka CPU % (limit 150) | 58.6 | 66.8 | 90.4 | **155.8** | **158.1** | **156.4** | **158.2** |
| Producer CPU % (limit 150) | 30.3 | 49.3 | 70.9 | 112.0 | 120.8 | 123.5 | 117.4 |
| Consumer CPU % (limit 150) | 19.0 | 31.6 | 44.0 | 67.0 | 69.5 | 70.4 | 70.9 |
| Postgres CPU % (limit 150) | 1.0 | 1.8 | 2.9 | 5.4 | 6.8 | 7.9 | 6.0 |
| k6 CPU % (limit 200) | 33.7 | 51.8 | 68.0 | 125.2 | 150.6 | 156.4 | 149.1 |
| Max lag | 362 | 693 | 745 | 1,265 | 1,500 | 1,537 | 1,854 |

- Kafka reaches its CPU limit at the 1757 req/s step and stays there. In that same step p95 rises from 2.1 ms to 56.1 ms and dropped iterations from 31 to 531. Kafka's samples read slightly above 150 % because of docker's sampling.
- At 3000 req/s the share of each component's limit used (derived) is Kafka 105 %, producer 78 %, consumer 47 % and Postgres 4 %. **Bottleneck order with equal resources: Kafka, then the producer, then the consumer, then Postgres.**
- Run `145346Z` (same limits, an earlier build) shows the same order: Kafka 135.2 % at 1757 and 156.9 % at 2171, producer at most 124.1 %, consumer at most 74.4 %, Postgres at most 6.7 % (derived). It reached 2992 req/s in the last step, with 1,418 dropped iterations in total against 9,704 in `154927Z`.
- `133558Z` (equal, peak 1500) reached 1501 req/s in the last step with 137 dropped in total and p95 40.6 ms; Kafka peaked at 117.9 % per step, below its limit (derived).
- `043214Z` (harness defaults) did not keep up: 1289 req/s at the 1500 step, with 15,657 dropped in that step and 32,456 over the run. The producer was pinned at its 0.75 CPU (70.3 to 74.9 % from the 700 step), and so was k6 at its 1.0 CPU (102.8 to 103.1 % from the 1100 step) (derived). Two components were at their limit at once, so this run cannot separate them; see section 7.
- Steady scenarios at equal limits (`134708Z`): `load` (300 req/s, 5 min) p95 11.4 ms, max lag 220, drain 3.1 s. `spike` (50, then 1500 req/s for 30 s) p95 56.0 ms, max lag 912, drain 3.2 s, 186 dropped.

## 3. Bottleneck matrix (stress to 3000 req/s, one run per profile)

| Profile | Achieved req/s (run / 3000 step) | Dropped | p95 / p99 | Max lag / drain | Flush p95 | 5xx / unknown | Starved CPU % (k6 window avg / peak; limit) | Symptom |
|---|---|---|---|---|---|---|---|---|
| equal `154927Z` | 1713.0 / 2991 | 9,704 | 64.3 / 96.0 ms | 1,854 / 4.3 s | 24.6 ms | 0 | Kafka 117.0 / 169.4; 150 | Kafka pinned from 1757 req/s, p95 up |
| producer-bound `160027Z` | 1691.9 / 2849 | 22,484 | 94.7 / 175.9 ms | 2,195 / 3.0 s | 22.2 ms | 0 | producer 48.4 / 55.9; 50 | rate below target, most drops, p95 rises step by step |
| consumer-bound `161132Z` | 1725.0 / 2990 | 2,449 | 51.0 / 71.7 ms | 2,669 / 3.7 s | 95.0 ms | 0 | consumer 25.1 / 28.0; 25 | flush p95 ×3.9, lag up, producer unaffected |
| db-bound `162241Z` | 1713.1 / 2998 | 9,679 | 70.9 / 102.7 ms | 5,412 / 4.0 s | 480.3 ms (p99 1.6 s) | 0 | Postgres 10.7 / 28.8; 25 | flush p95 ×19.5, highest lag |
| kafka-bound `163400Z` | 1704.5 / 2941 | 14,851 | 103.2 / 173.3 ms | 1,939 / 14.7 s | 27.4 ms | 0 | Kafka 51.6 / 56.1; 50 | p95 near 100 ms at every rate, longest drain |

"Achieved, run" is the report's average over the whole k6 run, ramps included; "3000 step" is derived. The flush ratios are against equal's 24.6 ms. Every run passed with no loss, zero overcount and `db = topic − DLQ`. Per step (derived), CPU of the starved component / achieved req/s / p95 ms:

| Profile | 514 | 929 | 1343 | 1757 | 2171 | 2586 | 3000 |
|---|---|---|---|---|---|---|---|
| producer-bound | 35.2 / 507 / 36.7 | 51.4 / 928 / 46.5 | 51.9 / 1341 / 60.5 | 51.4 / 1753 / 70.0 | 50.7 / 2073 / 74.9 | 51.2 / 2567 / 84.4 | 50.8 / 2849 / 104.6 |
| consumer-bound | 20.7 / 508 / 18.1 | 25.8 / 928 / 35.2 | 25.9 / 1342 / 21.7 | 26.4 / 1752 / 36.5 | 26.3 / 2170 / 7.0 | 26.2 / 2574 / 53.7 | 26.0 / 2990 / 53.7 |
| db-bound | 2.6 / 514 / 20.1 | 5.5 / 926 / 42.0 | 9.9 / 1335 / 49.5 | 13.3 / 1726 / 61.8 | 16.4 / 2118 / 67.2 | 16.7 / 2561 / 72.8 | 13.4 / 2998 / 60.2 |
| kafka-bound | 50.6 / 506 / 146.8 | 51.9 / 927 / 95.6 | 51.5 / 1340 / 93.8 | 52.1 / 1740 / 101.4 | 51.7 / 2160 / 98.8 | 52.0 / 2500 / 102.3 | 51.9 / 2941 / 92.6 |

- **Producer-bound.** The producer is at its 0.5 CPU from the 929 req/s step. It keeps accepting, but each request waits longer: p95 climbs from 36.7 to 104.6 ms, k6 needs more VUs (300 reached), and the achieved rate falls below target at 2171 (2073) and 3000 (2849). Downstream sees less load: consumer CPU drops to 24.4 % and Kafka to 66.4 % at the last step, and lag (2,195) and drain (3.0 s) stay normal.
- **Consumer-bound.** At 0.25 CPU the consumer is pinned from the 929 step and still keeps pace with 2990 req/s. Its batches get larger and slower (flush p95 95.0 ms against 24.6 ms), and Kafka holds the difference: lag peaks at 2,669 and drains in 3.7 s. The producer is not affected (p95 51.0 ms, lower than equal), so Kafka does buffer a slow consumer while the producer stays fast. The workload of 5,000 users folds into few rows per batch, so 0.25 CPU was enough at this rate.
- **DB-bound.** With 200,000 users the run wrote 235,150 rows for 194,217 distinct users, so each batch upserts many distinct rows. Flush p95 rises to 480.3 ms and p99 to 1.6 s, and lag reaches 5,412, the highest of all profiles. Postgres's mean CPU stays below its 25 % limit (at most 16.7 % per step), with samples up to 28.8 %. The quota throttles bursts rather than pinning the mean. The producer is unaffected and the backlog drains in 4.0 s.
- **Kafka-bound.** At 0.5 CPU the broker is pinned at every step, including 514 req/s. Producer p95 sits near 100 ms from the start, because each `202` waits for the broker's acknowledgement. Throughput still reaches 2941 req/s, and the producer's own CPU drops (63.6 % mean against 85.4 % in equal). The consumer fetches from the same throttled broker, so drain takes 14.7 s, the longest of all profiles. No `kafka_error` and no 5xx were recorded.

## 4. Crash results (equal limits)

`Ready` is the `recovered` event in `timeline.jsonl`: the seconds until `/readyz` answered after the restart (or unpause, or scale) action. It is measured on the consumer, except for `crash-kafka-stop`, where it is the producer. Each cell gives the 200 req/s run (`134708Z`) / the 1000 req/s run (`164604Z`).

| Scenario | Ready | Max lag | Drain | Unknown (5xx/timeout) | Achieved req/s | Accepted = DB (overcount) |
|---|---|---|---|---|---|---|
| `crash-consumer-stop` (SIGTERM 60 s, start 75 s) | 0.3 / 0.7 s | 172 / 853 | 2.4 / 2.9 s | 0 / 0 | 199.9 / 997.6 | 35,977 / 179,589 (0 / 0) |
| `crash-consumer-kill` (SIGKILL 60 s, start 75 s) | 0.3 / 0.6 s | 138 / 1,105 | 2.4 / 2.7 s | 0 / 0 | 199.9 / 998.2 | 35,987 / 179,706 (0 / 0) |
| `crash-postgres-pause` (pause 60-90 s) | 0.3 / 0.8 s | 5,184 / 25,876 | 2.3 / 2.8 s | 0 / 0 | 199.9 / 998.1 | 35,982 / 179,651 (0 / 0) |
| `crash-kafka-stop` (stop 60 s, start 80 s) | 8.3 / 11.3 s | 156 / 895 | 2.3 / 3.4 s | 1,236 / 1,500 | 175.6 / 815.1 | 30,374 / 145,217 (0 / 0) |
| `crash-scale-out` (`--scale consumer=2` at 60 s) | 1.6 / 2.1 s | 181 / 840 | 2.3 / 3.1 s | 0 / 0 | 199.9 / 997.5 | 35,984 / 179,583 (0 / 0) |

- **Zero loss and zero overcount in all 10 runs.** `db = topic − DLQ` held exactly, the DLQ stayed empty, and no batch was fenced. The SIGTERM exit code was 0 in both stop runs. `crash-postgres-pause` logged 12 `postgres` retries at each rate and sent nothing to the DLQ.
- **Kafka outage.** Every failed request fell in the outage window. At 1000 req/s, 0 of 119,502 steady-phase requests failed and 1,500 of 27,215 outage-phase requests did; at 200 req/s the counts were 0 of 23,980 and 1,236 of 7,630. The producer counted the same 1,500 as `activities_published_total{result="kafka_error"}` and answers 503 for them ([ADR 0005](adrs/0005-ingestion-http-api.md)). Requests that waited past k6's 10 s timeout also count as unknown (outage p99 10.0 s). The topic holds exactly the accepted count, so none of the unknown requests was written. Blocked VUs explain the lower achieved rate (33,284 dropped at 1000 req/s).
- **Lag while the consumer is down is not observed.** The probe reads `consumer_lag_records` from the consumer itself, which is refreshed every 15 s, so its samples are empty while the process is down and for about 15 s after the restart. The first sample after the restart was 568 (kill, 1000 req/s) and 853 (stop), both normal steady-state values (derived from `lag.jsonl`).
- **Postgres pause, 1000 req/s.** Lag was 25,876 in the last sample before the unpause and 637 in the next one, 15 s later. The backlog therefore cleared within one refresh interval (derived).
- **Commit gaps after the Kafka restart (derived from `logs/consumer.log`).** When the broker came back, the consumer resumed with stored offset greater than committed offset on 6 partitions (107 records, 200 req/s) and on 5 partitions (520 records, 1000 req/s). The plain stop, kill and scale-out runs resumed with stored offset equal to committed offset on every partition.

## 5. Effectively once and DLQ (`170735Z`, 1000 req/s)

**`crash-commit-gap`** (`170737Z`): Kafka stopped at +60.2 s, the consumer was SIGKILLed at +70.1 s, Kafka started at +85.2 s and the consumer at +95.1 s. The consumer was ready after 3.7 s. Recovery table from the report:

| Time (UTC) | Member | Partition | Stored offset | Committed offset | Gap |
|---|---|---|---|---|---|
| 17:09:46.911 | 0 | 2 | 9,197 | 9,110 | +87 |
| 17:09:46.911 | 0 | 5 | 9,083 | 9,036 | +47 |
| 17:09:46.911 | 1 | 3 | 11,080 | 10,957 | +123 |
| 17:09:46.911 | 1 | 4 | 9,752 | 9,675 | +77 |
| 17:09:47.419 | 2 | 0 | 10,145 | 10,081 | +64 |
| 17:09:47.419 | 2 | 1 | 10,384 | 10,320 | +64 |

**462 records on 6 partitions were already in Postgres but not committed in Kafka.** Resuming from Kafka's committed offset would have counted them twice. The consumer resumed from the stored position instead, and the result was DB 139,394 = accepted 139,394 = topic 139,394 − DLQ 0: overcount 0, with 1,500 unknown requests in the outage window. Fence counters were 0. They cover only the restarted process.

**`poison-records`** (`171150Z`): 50 malformed records were written straight to the input topic at +60.2 s.

| Kind | Injected | Class | | Kind | Injected | Class |
|---|---|---|---|---|---|---|
| `invalid_json` | 7 | decode | | `missing_fields` | 6 | validation |
| `truncated_json` | 7 | decode | | `missing_timestamp` | 6 | validation |
| `wrong_type_user_id` | 6 | decode | | `control_char_user_id` | 6 | validation |
| `invalid_timestamp` | 6 | decode | | `zero_timestamp` | 6 | validation |

The DLQ held exactly 50 records (end offsets 12, 19 and 19 on its 3 partitions), and `dlq_records_total` was decode 26/26 and validation 24/24 (DLQ/injected). The run accepted 179,396 events: DB 179,396 = topic 179,446 − DLQ 50. Final lag was 0, so no partition stalled behind a poison record. Steady-phase p95 was 82.4 ms with 0 failures.

## 6. Soak (`141711Z`, 150 req/s for 30 min, equal limits)

The run accepted 270,001 events, with DB = topic = 270,001 and DLQ 0. p95 was 2.6 ms, p99 24.7 ms, max lag 118 and drain 2.3 s, with no OOM kill and no restart. Memory (MiB) is the mean of the `stats.jsonl` samples in each 5-minute slice of the k6 window (derived):

| Minutes | 0-5 | 5-10 | 10-15 | 15-20 | 20-25 | 25-30 | Peak (limit 1.25 GiB) |
|---|---|---|---|---|---|---|---|
| producer | 11.2 | 11.6 | 11.7 | 12.1 | 12.2 | 12.3 | 16.0 |
| consumer | 9.5 | 10.0 | 12.0 | 12.3 | 12.3 | 12.4 | 17.9 |
| kafka | 532.3 | 553.0 | 572.0 | 589.3 | 612.4 | 635.4 | 748.9 (58.5 %) |
| postgres | 52.8 | 51.1 | 50.6 | 50.7 | 50.8 | 50.0 | 56.9 |

The two Go services stay flat within about 3 MiB and use under 1.5 % of their limit. Kafka grows by about 100 MiB over the 30 minutes. It runs with a fixed heap (`-Xms768m -Xmx768m` by default) and keeps appending to its log, so a JVM touching its committed heap is a likely cause. This run cannot tell that apart from a slow leak; see section 7.

## 7. Limits of these tests

- **One host.** All containers, k6 included, share one Docker Desktop VM. A starved component frees CPU for the others, and k6 competes with the system it measures (up to 156.4 % CPU in `154927Z`).
- **One broker, replication factor 1.** Production would replicate every write to 2 followers (RF 3, `min.insync.replicas=2`), which adds broker CPU and network. The Kafka figures here are an upper bound.
- **The generator was the ceiling.** k6 caps the rate at 3000 req/s and VUs at 300. The equal, producer-bound and kafka-bound runs reached 294 to 300 VUs, so dropped iterations above about 2,500 req/s mix k6 limits with system limits. The maximum throughput of the system was not found. In `043214Z`, k6 was at its own 1.0 CPU limit.
- **Short and single runs.** Each stress step holds for 75 s and each profile ran once. Two stress@equal runs at 3000 req/s dropped 1,418 and 9,704 iterations, so differences of that size are within run-to-run variance. Soak lasted 30 min, which is too short to rule out a slow leak in Kafka.
- **Sampling.** `docker stats` samples about every 2 s, and short bursts can read above the quota (Kafka peak 169.4 % with a 150 % limit). The lag gauge updates every 15 s and is empty while the consumer is down.
- **Builds.** Images were rebuilt for each experiment. `043214Z` used an earlier build and the harness defaults, so it is not directly comparable with the afternoon runs.

## 8. Where the raw data lives

- Run directories: `loadtest/results/<RUN_ID>-<scenario>[@<profile>]/` (start with `report.md`). Suite directories: `loadtest/results/<SUITE_ID>-suite/`. Report copies by experiment: `temp/<date>_<time>Z_<name>/`.
- None of them is committed (`.gitignore`). The commands in section 1 regenerate them. `bash loadtest/publish-report.sh --all` rebuilds `temp/`, and `cd loadtest/report && GOWORK=off go run . ../results/<run>` re-renders one report ([load-testing.md](load-testing.md#outputs)).
- Run IDs used here: `20261012t043214z-stress`, `…t133558z-stress`, `…t134710z-smoke`, `…t134801z-load`, `…t135349z-spike`, `…t135826z`/`140211z`/`140555z`/`140941z`/`141327z` (crashes at 200 req/s), `…t141711z-soak`, `…t145346z-stress`, `…t154927z-stress@equal`, `…t160027z-stress@producer-bound`, `…t161132z-stress@consumer-bound`, `…t162241z-stress@db-bound`, `…t163400z-stress@kafka-bound`, `…t164607z`/`165017z`/`165416z`/`165819z`/`170223z` (crashes at 1000 req/s, `@equal`), `…t170737z-crash-commit-gap@equal`, `…t171150z-poison-records@equal`.
