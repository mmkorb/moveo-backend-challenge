#!/usr/bin/env bash
# loadtest/run.sh — load and crash test orchestrator (ADR 0016).
#
# Usage: loadtest/run.sh <scenario> [--profile <name>] [--keep] [--no-build] [--help]
# Needs only docker (Engine + Compose v2+) and the coreutils/awk/sed that ship with Git Bash.
#
# Flow: preflight -> RUN_ID + results dir -> `up -d --build --wait` (project moveo-loadtest, base stack +
# overlay; k6/probe excluded by profile) -> baseline /metrics -> background samplers -> k6 (+ chaos
# timeline for crash-*) -> drain wait (consumer-group lag = 0) -> collection -> run.json -> report
# (loadtest/report, Go, in a golang container) -> `down -v` (unless --keep) -> exit with the report's code
# (0 PASS, 1 FAIL, 2 error).
#
# ---------------------------------------------------------------------------------------------------
# RESULTS CONTRACT — files written to loadtest/results/<RUN_ID>-<scenario>[@<profile>]/ (read by loadtest/report)
# ---------------------------------------------------------------------------------------------------
# RUN_ID = `date -u +%Y%m%dt%H%M%Sz` (only [0-9a-z]); every user_id of the run is lt-<RUN_ID>-u<n>.
# Timestamps: UTC RFC 3339 with milliseconds ("2026-10-12T03:00:00.123Z"). JSON/JSONL are UTF-8, LF.
# Any file except run.json may be missing or empty when its step failed; the failure is then a
# `collect_error` event in timeline.jsonl. Missing file = "no data" for the report.
#
#   run.json            JSON object, written last before the report:
#                       schema_version(1), scenario, k6_scenario, profile (name|null), experiment{id, name}
#                       (LT_EXPERIMENT_ID/LT_EXPERIMENT_NAME from run-suite.sh; a standalone run is its
#                       own experiment: id = run_id, name = <scenario>[@<profile>]), run_id, user_prefix, project,
#                       results_dir, keep, build, started_at, stack_ready_at, k6_started_at,
#                       k6_finished_at, k6_exit_code (0 ok, 99 thresholds failed, 108 aborted by a
#                       threshold, other = error), drain_seconds (number|null), finished_at,
#                       host{docker_cpus, docker_mem_bytes, docker_server_version, docker_os,
#                       compose_version}, images (raw `docker compose images --format json` array),
#                       limits{<kafka|postgres|producer|consumer|k6|oneshot|probe>: {cpus, memory, pids}},
#                       config{topic, partitions, dlq_topic, dlq_partitions, consumer_group,
#                       consumer_workers, flush_max_records, consumer_replicas, drain_timeout_s,
#                       k6{LT_*: string, "" = script default}, chaos{at_s, down_for_s,
#                       recovery_grace_s}|null}, expect{overcount_max, consumer_stop_exit_code|null}
#   k6-summary.json     k6 handleSummary data object (legacy shape: options, state, metrics,
#                       root_group, setup_data). Custom counters: events_accepted,
#                       page_views_accepted, events_accepted_prev_day, events_unknown,
#                       events_rejected, invalid_sent. Crash runs add sub-metrics {phase:steady|outage}.
#                       Built-in metrics with no samples (e.g. dropped_iterations = 0) are absent.
#   k6-dashboard.html   k6 web-dashboard export (absent for runs shorter than a few seconds)
#   k6-output.log       k6 stdout+stderr, ends with the script's text summary
#   stats.jsonl         one line per container per sample (~every 2 s):
#                       {"ts":..., <docker stats --format '{{json .}}' fields: BlockIO, CPUPerc,
#                       Container, ID, MemPerc, MemUsage, Name, NetIO, PIDs>}
#                       service = Name minus "moveo-loadtest-" and the "-<n>" suffix.
#   lag.jsonl           consumer lag from the consumer's /metrics every 5 s during the run:
#                       {"ts":...,"source":"consumer_metrics","up":true|false,"lag":<int>|null}
#                       lag = SUM(consumer_lag_records) (refreshed by the consumer every 15 s).
#   timeline.jsonl      {"ts":...,"event":<name>, ...}. Events: run_start{scenario,run_id},
#                       stack_up{rc}, k6_start{k6_scenario}, chaos{action,service,rc,ended_at,
#                       exit_codes?{<container>:<int>},replicas?}, recovered{service,after_action,
#                       seconds,ok}, poison_injected{rc,count,classes{decode,validation},
#                       kinds{<kind>:<n>}} (poison-records: what was written to the input topic and
#                       the DLQ error class each record should get), k6_end{exit_code}, drain_start, drain_end{seconds,timed_out,
#                       group_found,final_lag}, collect_start, collect_end,
#                       collect_error{step,detail}, run_end.
#   drain.json          {"started_at","finished_at","seconds":<num>|null,"timed_out","group_found",
#                       "final_lag":<int>|null,"polls","timeout_seconds",
#                       "samples":[{"ts","group_found","lag"}]}; seconds = k6 end -> first poll
#                       with total lag 0. Lag = SUM(LAG) over incoming.user_activity partitions for
#                       group analytics-consumer; a partition without a committed offset counts
#                       as its LOG-END-OFFSET.
#   metrics-before-producer.txt, metrics-before-consumer.txt   Prometheus text, after `up`
#   metrics-after-producer.txt,  metrics-after-consumer.txt    Prometheus text, after the drain
#   db-totals.csv       header "activity_count,page_view_count,distinct_users,rows" + 1 row,
#                       for user_id LIKE 'lt-<RUN_ID>-%' (header only if the query failed)
#   db-by-date.csv      header "activity_date,rows,distinct_users,activity_count,page_view_count",
#                       one row per activity_date (YYYY-MM-DD), ascending
#   dlq-offsets.txt     kafka-get-offsets.sh --time -1 for incoming.user_activity.dlq:
#                       "topic:partition:offset" per line; sum = records in the DLQ
#   topic-offsets.txt   same for incoming.user_activity; sum = records produced
#   inspect.jsonl       one line per project container at collection time: {"name","service",
#                       "oneoff","status","oom_killed","restart_count","exit_code","started_at",
#                       "finished_at","memory_limit_bytes","nano_cpus","pids_limit","image"}
#   logs-summary.txt    TSV with header "service<TAB>lines<TAB>error<TAB>warn"
#                       error = '"level":"ERROR"' or the words ERROR|FATAL|PANIC;
#                       warn = '"level":"WARN"' or the words WARN|WARNING
#   logs/<service>.log  `docker compose logs --no-color --no-log-prefix <service>`
#                       (Go services: one slog JSON object per line)
#   chaos.log           output of the chaos commands (crash-* only)
#   report.md, report.html   written by loadtest/report
# ---------------------------------------------------------------------------------------------------

set -euo pipefail

# Git Bash: never rewrite container paths such as /opt/kafka/... or /work.
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

readonly PROJECT=moveo-loadtest
readonly GOLANG_IMAGE=golang:1.27.2           # ADR 0001
readonly TOPIC=incoming.user_activity
readonly DLQ_TOPIC=incoming.user_activity.dlq
readonly GROUP=analytics-consumer
readonly PARTITIONS=6
readonly DLQ_PARTITIONS=3
readonly LAGPROBE="${PROJECT}-lagprobe"
readonly SCENARIOS="smoke load stress spike soak crash-consumer-kill crash-consumer-stop crash-postgres-pause crash-kafka-stop crash-scale-out crash-commit-gap poison-records"

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

# Compose: base stack + overlay, own project. COMPOSE_LT also enables the loadtest profile (k6, probe).
# LT_COMPOSE_OVERLAY adds one more Compose file, e.g. loadtest/compose.coverage.yaml (docs/load-testing.md, "E2E coverage").
COMPOSE=(docker compose -p "$PROJECT" -f compose.yaml -f loadtest/compose.loadtest.yaml ${LT_COMPOSE_OVERLAY:+-f "$LT_COMPOSE_OVERLAY"})
COMPOSE_LT=("${COMPOSE[@]}" --profile loadtest)

readonly PROFILES_DIR="$REPO_ROOT/loadtest/profiles"
K6_KNOBS=(LT_RATE LT_START_RATE LT_PEAK_RATE LT_DURATION LT_ITERATIONS LT_PRE_ALLOCATED_VUS LT_MAX_VUS
  LT_USERS LT_INVALID_RATIO LT_PREV_DAY_RATIO LT_REQUEST_TIMEOUT)

# list_profiles prints the names of loadtest/profiles/*.env, space separated.
list_profiles() {
  local f out=""
  for f in "$PROFILES_DIR"/*.env; do
    [[ -f $f ]] || continue
    f=${f##*/}
    out+="${out:+ }${f%.env}"
  done
  printf '%s' "$out"
}

usage() {
  cat <<EOF
Usage: loadtest/run.sh <scenario> [--profile <name>] [--keep] [--no-build] [--help]

Runs the stack (project ${PROJECT}, compose.yaml + loadtest/compose.loadtest.yaml) with hard
resource limits, drives it with k6, optionally injects a failure, waits for the consumer to drain,
collects metrics/DB totals/logs and renders loadtest/results/<RUN_ID>-<scenario>[@<profile>]/report.{md,html}.

Scenarios:
  smoke                 1 VU, 200 events; DB must equal accepted exactly
  load                  300 req/s for 5 min
  stress                100 -> 1500 req/s in 7 steps over 10 min
  spike                 50 -> 1500 req/s for 30 s -> 50
  soak                  150 req/s for 30 min
  crash-consumer-kill   200 req/s, 3 min; SIGKILL consumer at 60 s, start at 75 s
  crash-consumer-stop   200 req/s, 3 min; stop consumer (SIGTERM) at 60 s, start at 75 s
  crash-postgres-pause  200 req/s, 3 min; pause postgres at 60 s for 30 s
  crash-kafka-stop      200 req/s, 3 min; stop kafka at 60 s for 20 s
  crash-scale-out       200 req/s, 3 min; scale consumer to 2 at 60 s
  crash-commit-gap      200 req/s, 3 min; stop kafka at 60 s, SIGKILL consumer at 70 s (kafka still down),
                        start kafka at 85 s, consumer at 95 s: the DB holds records Kafka never saw
                        committed, so the stored position (not the Kafka offset) must prevent double counting
  poison-records        200 req/s, 3 min; at 60 s write LT_POISON_COUNT (50) malformed records straight to the
                        input topic: they must all reach the DLQ, in the right error class, without a stall

Options:
  --profile <name>
               load loadtest/profiles/<name>.env (resource limits that make one component the
               bottleneck); variables already set in the environment win over the profile.
               Profiles: $(list_profiles)
  --keep       leave the stack and its volumes running afterwards (remove later with
               docker compose -p ${PROJECT} -f compose.yaml -f loadtest/compose.loadtest.yaml --profile loadtest down -v)
  --no-build   do not build images; use existing moveo-analytics/{producer,consumer}:dev
  --help       show this help

Environment (all optional): LT_RATE, LT_START_RATE, LT_PEAK_RATE, LT_DURATION, LT_ITERATIONS,
LT_PRE_ALLOCATED_VUS, LT_MAX_VUS (<= 300), LT_USERS, LT_INVALID_RATIO, LT_PREV_DAY_RATIO,
LT_REQUEST_TIMEOUT (k6); LT_CHAOS_AT, LT_CHAOS_DOWN_FOR, LT_CHAOS_RECOVERY (crash-*);
LT_GAP_KILL_AT, LT_GAP_KAFKA_START_AT, LT_GAP_CONSUMER_START_AT (crash-commit-gap, seconds after the k6
start; defaults 70, 85, 95; the kafka stop is LT_CHAOS_AT); LT_POISON_COUNT (poison-records, default 50);
LT_EXPERIMENT_ID, LT_EXPERIMENT_NAME (set by run-suite.sh);
LT_DRAIN_TIMEOUT, LT_DRAIN_POLL, LT_UP_TIMEOUT, LT_RECOVERY_TIMEOUT, LT_MIN_DOCKER_MEM_GIB;
LT_<SERVICE>_{CPUS,MEMORY,PIDS} for SERVICE in KAFKA POSTGRES PRODUCER CONSUMER K6 ONESHOT PROBE;
LT_PRODUCER_GOMEMLIMIT, LT_CONSUMER_GOMEMLIMIT, LT_CONSUMER_WORKERS, LT_FLUSH_MAX_RECORDS. See docs/load-testing.md.

Exit code: the report's (0 PASS, 1 FAIL, 2 error); 2 for harness errors.
EOF
}

# ---------- helpers ----------

log() { printf '[run.sh %s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "ERROR: $*"; exit 2; }
ts() { date -u +%Y-%m-%dT%H:%M:%S.%3NZ; }
now_ms() { date -u +%s%3N; }
ms_to_s() { awk -v ms="$1" 'BEGIN { printf "%.1f", ms / 1000 }'; }

# json_str prints its argument as a JSON string literal.
json_str() {
  local s=$1
  s=${s//\\/\\\\}
  s=${s//\"/\\\"}
  s=${s//$'\n'/\\n}
  s=${s//$'\r'/}
  s=${s//$'\t'/\\t}
  printf '"%s"' "$s"
}

# timeline_at <ts> <event> [raw JSON members] appends one event to timeline.jsonl.
timeline_at() {
  local at=$1 event=$2 extra=${3:-}
  printf '{"ts":"%s","event":"%s"%s}\n' "$at" "$event" "${extra:+,$extra}" >>"$RESULTS/timeline.jsonl"
}
timeline() { timeline_at "$(ts)" "$@"; }

collect_error() {
  log "warning: $1 failed: $2"
  timeline collect_error "\"step\":$(json_str "$1"),\"detail\":$(json_str "$2")"
}

host_path() {
  if (cd "$1" && pwd -W) >/dev/null 2>&1; then (cd "$1" && pwd -W); else (cd "$1" && pwd); fi
}

project_containers() { docker ps -aq --filter "label=com.docker.compose.project=$PROJECT"; }

# probe_get <url> <file>: GET through the probe container inside the project network.
probe_get() {
  local url=$1 out=$2 err
  if ! err=$(docker exec "$LAGPROBE" curl -fsS -m 5 "$url" 2>&1 >"$out"); then
    collect_error "GET $url" "$err"
  fi
}

# ---------- arguments ----------

SCENARIO=""
PROFILE=""
KEEP=0
BUILD=1
while (($#)); do
  arg=$1
  case $arg in
    --help | -h) usage; exit 0 ;;
    --keep) KEEP=1 ;;
    --no-build) BUILD=0 ;;
    --profile)
      (($# >= 2)) || die "--profile needs a name (one of: $(list_profiles))"
      PROFILE=$2
      shift
      ;;
    --profile=*) PROFILE=${arg#--profile=} ;;
    -*) usage >&2; die "unknown option: $arg" ;;
    *)
      [[ -z $SCENARIO ]] || die "only one scenario allowed (got '$SCENARIO' and '$arg')"
      SCENARIO=$arg
      ;;
  esac
  shift
done
[[ -n $SCENARIO ]] || { usage >&2; die "missing <scenario>"; }
[[ " $SCENARIOS " == *" $SCENARIO "* ]] || die "unknown scenario '$SCENARIO' (one of: $SCENARIOS)"

# load_profile <name> exports the LT_* variables of loadtest/profiles/<name>.env that are not
# already set in the environment (an explicit variable always wins). The file is parsed, never
# sourced: only blank lines, # comments and LT_<NAME>=<value> lines are accepted.
load_profile() {
  local name=$1 file line key val applied="" kept=""
  [[ $name =~ ^[a-z0-9][a-z0-9-]*$ ]] || die "invalid profile name '$name' (one of: $(list_profiles))"
  file="$PROFILES_DIR/$name.env"
  [[ -f $file ]] || die "unknown profile '$name' (one of: $(list_profiles))"
  while IFS= read -r line || [[ -n $line ]]; do
    line=${line%$'\r'}
    line=${line#"${line%%[![:space:]]*}"}
    line=${line%"${line##*[![:space:]]}"}
    [[ -z $line || $line == \#* ]] && continue
    [[ $line =~ ^(LT_[A-Z0-9_]+)=([^[:space:]]*)$ ]] || die "profile $name: unsupported line '$line' (expected LT_<NAME>=<value>)"
    key=${BASH_REMATCH[1]}
    val=${BASH_REMATCH[2]}
    if [[ -v $key ]]; then
      kept+="${kept:+ }$key"
      continue
    fi
    export "$key=$val"
    applied+="${applied:+ }$key=$val"
  done <"$file"
  log "profile $name: ${applied:-nothing applied}${kept:+ (kept from the environment: $kept)}"
}
[[ -z $PROFILE ]] || load_profile "$PROFILE"

# Resource budget (ADR 0016). Same defaults as loadtest/compose.loadtest.yaml — keep in sync.
# Set after the profile is loaded, so profile values and explicit variables win over these.
: "${LT_KAFKA_CPUS:=1.5}"      "${LT_KAFKA_MEMORY:=1.25G}"   "${LT_KAFKA_PIDS:=1024}"
: "${LT_POSTGRES_CPUS:=1.0}"   "${LT_POSTGRES_MEMORY:=768M}" "${LT_POSTGRES_PIDS:=256}"
: "${LT_PRODUCER_CPUS:=0.75}"  "${LT_PRODUCER_MEMORY:=256M}" "${LT_PRODUCER_PIDS:=200}"
: "${LT_CONSUMER_CPUS:=1.0}"   "${LT_CONSUMER_MEMORY:=384M}" "${LT_CONSUMER_PIDS:=200}"
: "${LT_K6_CPUS:=1.0}"         "${LT_K6_MEMORY:=768M}"       "${LT_K6_PIDS:=512}"
: "${LT_ONESHOT_CPUS:=0.5}"    "${LT_ONESHOT_MEMORY:=256M}"  "${LT_ONESHOT_PIDS:=256}"
: "${LT_PROBE_CPUS:=0.25}"     "${LT_PROBE_MEMORY:=64M}"     "${LT_PROBE_PIDS:=64}"
: "${LT_CONSUMER_WORKERS:=3}"  "${LT_FLUSH_MAX_RECORDS:=500}"
export LT_KAFKA_CPUS LT_KAFKA_MEMORY LT_KAFKA_PIDS LT_POSTGRES_CPUS LT_POSTGRES_MEMORY LT_POSTGRES_PIDS \
  LT_PRODUCER_CPUS LT_PRODUCER_MEMORY LT_PRODUCER_PIDS LT_CONSUMER_CPUS LT_CONSUMER_MEMORY LT_CONSUMER_PIDS \
  LT_K6_CPUS LT_K6_MEMORY LT_K6_PIDS LT_ONESHOT_CPUS LT_ONESHOT_MEMORY LT_ONESHOT_PIDS \
  LT_PROBE_CPUS LT_PROBE_MEMORY LT_PROBE_PIDS LT_CONSUMER_WORKERS LT_FLUSH_MAX_RECORDS

# Harness knobs.
: "${LT_MIN_DOCKER_MEM_GIB:=8}"     # preflight: Docker VM memory floor
: "${LT_UP_TIMEOUT:=300}"           # seconds for `up --wait`
: "${LT_DRAIN_TIMEOUT:=300}"        # seconds to wait for consumer lag 0 after k6
: "${LT_DRAIN_POLL:=5}"             # seconds between lag polls
: "${LT_RECOVERY_TIMEOUT:=120}"     # seconds to wait for /readyz after a restoring chaos action
: "${LT_CHAOS_AT:=60}"              # seconds after k6 start for the chaos action
LT_CHAOS_RECOVERY_DEFAULT=30        # poison-records breaks nothing: only the injection second is tagged
[[ $SCENARIO == poison-records ]] && LT_CHAOS_RECOVERY_DEFAULT=0
: "${LT_CHAOS_RECOVERY:=$LT_CHAOS_RECOVERY_DEFAULT}" # extra seconds tagged phase=outage in k6 after the action window

K6_SCENARIO=$SCENARIO
CHAOS_DOWN_DEFAULT=0
OVERCOUNT_MAX=0 # every scenario: positions are stored with the aggregates (ADR 0008, option A)
STOP_EXIT_EXPECT=null
case $SCENARIO in
  crash-consumer-kill) CHAOS_DOWN_DEFAULT=15 ;;
  crash-consumer-stop) CHAOS_DOWN_DEFAULT=15; STOP_EXIT_EXPECT=0 ;;
  crash-postgres-pause) CHAOS_DOWN_DEFAULT=30 ;;
  crash-kafka-stop) CHAOS_DOWN_DEFAULT=20 ;;
  crash-scale-out) CHAOS_DOWN_DEFAULT=0 ;;
  crash-commit-gap)
    # Seconds after the k6 start (LT_CHAOS_AT is the kafka stop): kill the consumer while kafka is
    # still down, restart kafka, then the consumer. The k6 outage window is the kafka downtime.
    : "${LT_GAP_KILL_AT:=70}" "${LT_GAP_KAFKA_START_AT:=85}" "${LT_GAP_CONSUMER_START_AT:=95}"
    for knob in LT_CHAOS_AT LT_GAP_KILL_AT LT_GAP_KAFKA_START_AT LT_GAP_CONSUMER_START_AT; do
      [[ ${!knob} =~ ^[0-9]+$ ]] || die "$knob=${!knob} is not a whole number of seconds"
    done
    ((LT_CHAOS_AT < LT_GAP_KILL_AT && LT_GAP_KILL_AT < LT_GAP_KAFKA_START_AT && LT_GAP_KAFKA_START_AT < LT_GAP_CONSUMER_START_AT)) ||
      die "crash-commit-gap needs LT_CHAOS_AT < LT_GAP_KILL_AT < LT_GAP_KAFKA_START_AT < LT_GAP_CONSUMER_START_AT (got ${LT_CHAOS_AT}, ${LT_GAP_KILL_AT}, ${LT_GAP_KAFKA_START_AT}, ${LT_GAP_CONSUMER_START_AT})"
    CHAOS_DOWN_DEFAULT=$((LT_GAP_KAFKA_START_AT - LT_CHAOS_AT))
    export LT_GAP_KILL_AT LT_GAP_KAFKA_START_AT LT_GAP_CONSUMER_START_AT
    ;;
  poison-records)
    : "${LT_POISON_COUNT:=50}"
    [[ $LT_POISON_COUNT =~ ^[0-9]+$ ]] || die "LT_POISON_COUNT=$LT_POISON_COUNT is not a whole number"
    ((LT_POISON_COUNT >= 1 && LT_POISON_COUNT <= 100000)) || die "LT_POISON_COUNT=$LT_POISON_COUNT must be between 1 and 100000"
    CHAOS_DOWN_DEFAULT=0
    ;;
esac
CHAOS_RUN=0 # 1: chaos_timeline runs (crash-* and poison-records)
if [[ $SCENARIO == crash-* || $SCENARIO == poison-records ]]; then
  K6_SCENARIO=crash
  CHAOS_RUN=1
  : "${LT_CHAOS_DOWN_FOR:=$CHAOS_DOWN_DEFAULT}"
  export LT_CHAOS_AT LT_CHAOS_DOWN_FOR LT_CHAOS_RECOVERY
fi

# ---------- 1. preflight ----------

STARTED_AT=$(ts)
command -v docker >/dev/null 2>&1 || die "docker not found in PATH"
docker info >/dev/null 2>&1 || die "Docker daemon not reachable (is Docker Desktop running?)"
docker compose version >/dev/null 2>&1 || die "docker compose plugin not available"

DOCKER_CPUS=$(docker info --format '{{.NCPU}}')
DOCKER_MEM=$(docker info --format '{{.MemTotal}}')
DOCKER_VERSION=$(docker info --format '{{.ServerVersion}}')
DOCKER_OS=$(docker info --format '{{.OperatingSystem}}')
COMPOSE_VERSION=$(docker compose version --short)
min_mem=$((LT_MIN_DOCKER_MEM_GIB * 1024 * 1024 * 1024))
((DOCKER_MEM >= min_mem)) ||
  die "Docker has $((DOCKER_MEM / 1024 / 1024)) MiB of memory; need >= ${LT_MIN_DOCKER_MEM_GIB} GiB (Docker Desktop > Settings > Resources)"

if [[ -n $(project_containers) ]]; then
  die "project '$PROJECT' already has containers; remove them first: ${COMPOSE_LT[*]} down -v"
fi

for example in deploy/secrets/*.txt.example; do
  secret=${example%.example}
  if [[ ! -f $secret ]]; then
    cp "$example" "$secret"
    log "created $secret from $(basename "$example") (local throwaway secret; gitignored)"
  fi
done

if ((BUILD)); then
  for svc in producer consumer; do
    [[ -f services/$svc/Dockerfile ]] ||
      die "services/$svc/Dockerfile not found: build context is missing (or use --no-build with prebuilt moveo-analytics/$svc:dev)"
  done
else
  for svc in producer consumer; do
    docker image inspect "moveo-analytics/$svc:dev" >/dev/null 2>&1 ||
      die "--no-build: image moveo-analytics/$svc:dev not found; build it or drop --no-build"
  done
fi
[[ -f loadtest/report/go.mod ]] || log "warning: loadtest/report/go.mod not found; the report step will fail"

# RUN_ID and results dir.
RUN_ID=$(date -u +%Y%m%dt%H%M%Sz)
RESULTS_NAME="${RUN_ID}-${SCENARIO}${PROFILE:+@$PROFILE}"
RESULTS="loadtest/results/${RESULTS_NAME}"
[[ ! -e $RESULTS ]] || die "$RESULTS already exists"

# Experiment (docs/load-testing.md, "Experiments"): run-suite.sh passes one id/name for all its runs;
# a standalone run is its own experiment. The name ends up in a directory name under temp/.
EXPERIMENT_ID=${LT_EXPERIMENT_ID:-$RUN_ID}
EXPERIMENT_NAME=${LT_EXPERIMENT_NAME:-${SCENARIO}${PROFILE:+@$PROFILE}}
[[ $EXPERIMENT_ID =~ ^[0-9a-z]+$ ]] || die "LT_EXPERIMENT_ID=$EXPERIMENT_ID must match [0-9a-z]+"
EXPERIMENT_NAME=$(printf '%s' "$EXPERIMENT_NAME" | tr -c 'A-Za-z0-9@._-' '-')
[[ -n $EXPERIMENT_NAME ]] || die "LT_EXPERIMENT_NAME is empty after sanitizing"
mkdir -p "$RESULTS/logs"
export SCENARIO_NAME=$SCENARIO RUN_ID LT_RESULTS_DIR="./${RESULTS}"
export SCENARIO=$K6_SCENARIO                # read by the k6 service (overlay)
log "scenario=$SCENARIO_NAME profile=${PROFILE:-none} run_id=$RUN_ID results=$RESULTS"
timeline_at "$STARTED_AT" run_start "\"scenario\":\"$SCENARIO_NAME\",\"run_id\":\"$RUN_ID\""

# ---------- cleanup ----------

STACK_STARTED=0
STATS_PID=""
CHAOS_PID=""
# shellcheck disable=SC2329  # invoked by the EXIT trap
cleanup() {
  local rc=$?
  trap - EXIT INT TERM
  [[ -n $STATS_PID ]] && kill "$STATS_PID" 2>/dev/null
  [[ -n $CHAOS_PID ]] && kill "$CHAOS_PID" 2>/dev/null
  docker rm -f "$LAGPROBE" >/dev/null 2>&1 || true
  if ((STACK_STARTED)); then
    if ((KEEP)); then
      log "--keep: stack left running; remove with: ${COMPOSE_LT[*]} down -v"
    else
      log "tearing down project $PROJECT (down -v)"
      "${COMPOSE_LT[@]}" unpause >/dev/null 2>&1 || true
      "${COMPOSE_LT[@]}" down -v --remove-orphans >/dev/null 2>&1 || log "warning: down -v failed"
    fi
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'log "interrupted"; exit 130' INT
trap 'log "terminated"; exit 143' TERM

# collect_logs saves per-service logs and the ERROR/WARN summary.
collect_logs() {
  local svc file lines errors warns services
  services=$("${COMPOSE_LT[@]}" config --services 2>/dev/null) || services=""
  printf 'service\tlines\terror\twarn\n' >"$RESULTS/logs-summary.txt"
  for svc in $services; do
    file="$RESULTS/logs/$svc.log"
    "${COMPOSE_LT[@]}" logs --no-color --no-log-prefix "$svc" >"$file" 2>/dev/null || true
    lines=$(wc -l <"$file" | tr -d ' ')
    errors=$(grep -c -E '"level":"ERROR"|\b(ERROR|FATAL|PANIC)\b' "$file" || true)
    warns=$(grep -c -E '"level":"WARN"|\b(WARN|WARNING)\b' "$file" || true)
    printf '%s\t%s\t%s\t%s\n' "$svc" "$lines" "${errors:-0}" "${warns:-0}" >>"$RESULTS/logs-summary.txt"
  done
}

# ---------- 2. stack up ----------

up_flags=(-d --wait --wait-timeout "$LT_UP_TIMEOUT")
if ((BUILD)); then up_flags+=(--build); else up_flags+=(--no-build); fi
log "starting stack: ${COMPOSE[*]} up ${up_flags[*]}"
STACK_STARTED=1
up_rc=0
"${COMPOSE[@]}" up "${up_flags[@]}" || up_rc=$?
timeline stack_up "\"rc\":$up_rc"
if ((up_rc != 0)); then
  collect_logs
  die "stack did not become healthy (rc=$up_rc); logs saved in $RESULTS/logs"
fi
STACK_READY_AT=$(ts)

# ---------- 3./4. probe, baseline, samplers ----------

# Long-running probe: samples consumer lag from /metrics every 5 s into lag.jsonl and serves as the
# exec target for /metrics snapshots and /readyz polling (the Go images have no shell or curl).
# shellcheck disable=SC2016  # expanded by the probe's sh, not here
LAG_SCRIPT='
while :; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
  if body=$(curl -fsS -m 3 http://consumer:8081/metrics 2>/dev/null); then
    lag=$(printf "%s\n" "$body" | awk '"'"'/^consumer_lag_records[{ ]/ { s += $NF; n++ } END { if (n) printf "%d", s; else printf "null" }'"'"')
    printf "{\"ts\":\"%s\",\"source\":\"consumer_metrics\",\"up\":true,\"lag\":%s}\n" "$ts" "$lag"
  else
    printf "{\"ts\":\"%s\",\"source\":\"consumer_metrics\",\"up\":false,\"lag\":null}\n" "$ts"
  fi >>/results/lag.jsonl
  sleep 5
done'
"${COMPOSE_LT[@]}" run -d --no-deps --name "$LAGPROBE" probe sh -c "$LAG_SCRIPT" >/dev/null ||
  die "could not start the probe container"

log "baseline /metrics snapshots"
probe_get http://producer:8080/metrics "$RESULTS/metrics-before-producer.txt"
probe_get http://consumer:8081/metrics "$RESULTS/metrics-before-consumer.txt"

stats_loop() {
  local ids sample_ts
  while :; do
    mapfile -t ids < <(docker ps -q --filter "label=com.docker.compose.project=$PROJECT" 2>/dev/null)
    if ((${#ids[@]})); then
      sample_ts=$(ts)
      docker stats --no-stream --format '{{json .}}' "${ids[@]}" 2>/dev/null |
        sed -e 's/\r$//' -e "s/^{/{\"ts\":\"$sample_ts\",/" >>"$RESULTS/stats.jsonl" || true
    fi
    sleep 1
  done
}
stats_loop &
STATS_PID=$!

# ---------- 5. k6 + chaos ----------

# exit_codes <service> prints {"<container>":<exit code>,...} for the service's containers.
exit_codes() {
  local name out="" code
  while read -r name; do
    [[ -n $name ]] || continue
    code=$(docker inspect --format '{{.State.ExitCode}}' "$name" 2>/dev/null || echo null)
    out+="${out:+,}$(json_str "$name"):${code}"
  done < <(docker ps -a --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.service=$1" --filter "label=com.docker.compose.oneoff=False" \
    --format '{{.Names}}')
  printf '{%s}' "$out"
}

replica_count() {
  docker ps -q --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.service=$1" --filter "label=com.docker.compose.oneoff=False" | wc -l | tr -d ' '
}

# chaos <action> <service> <compose args...>: runs one action and logs it to the timeline.
# The pseudo-command `docker-start` starts the service's existing containers with plain
# `docker start`: `compose start <svc>` would first re-run the one-shot dependencies
# (kafka-init, consumer-migrate), which delays the restore by ~15 s and blurs the timing.
chaos() {
  local action=$1 service=$2 started rc=0 extra=""
  shift 2
  started=$(ts)
  log "chaos: $action $service"
  {
    if [[ $1 == docker-start ]]; then
      local ids
      mapfile -t ids < <(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" \
        --filter "label=com.docker.compose.service=$service" --filter "label=com.docker.compose.oneoff=False")
      printf '%s docker start %s\n' "$(ts)" "${ids[*]}"
      docker start "${ids[@]}" 2>&1 || rc=$?
    else
      printf '%s %s\n' "$(ts)" "${COMPOSE[*]} $*"
      "${COMPOSE[@]}" "$@" 2>&1 || rc=$?
    fi
  } >>"$RESULTS/chaos.log"
  case $action in
    kill | stop) extra=",\"exit_codes\":$(exit_codes "$service")" ;;
    scale) extra=",\"replicas\":$(replica_count "$service")" ;;
  esac
  timeline_at "$started" chaos "\"action\":\"$action\",\"service\":\"$service\",\"rc\":$rc,\"ended_at\":\"$(ts)\"$extra"
}

# wait_ready <service> <url> <after_action>: polls /readyz after a restoring action.
wait_ready() {
  local service=$1 url=$2 after=$3 t0 ok=false
  t0=$(now_ms)
  while (($(now_ms) - t0 < LT_RECOVERY_TIMEOUT * 1000)); do
    if docker exec "$LAGPROBE" curl -fsS -m 2 -o /dev/null "$url" >/dev/null 2>&1; then
      ok=true
      break
    fi
    sleep 1
  done
  timeline recovered "\"service\":\"$service\",\"after_action\":\"$after\",\"seconds\":$(ms_to_s $(($(now_ms) - t0))),\"ok\":$ok"
}

# chaos_timeline runs the scenario's actions at fixed offsets from the k6 start.
chaos_timeline() {
  local t0
  t0=$(now_ms)
  sleep_until() { local wait_ms=$(($1 * 1000 - ($(now_ms) - t0))); ((wait_ms <= 0)) || sleep "$(ms_to_s "$wait_ms")"; }
  local restore_at=$((LT_CHAOS_AT + LT_CHAOS_DOWN_FOR))
  sleep_until "$LT_CHAOS_AT"
  case $SCENARIO_NAME in
    crash-consumer-kill)
      chaos kill consumer kill -s SIGKILL consumer
      sleep_until "$restore_at"
      chaos start consumer docker-start
      wait_ready consumer http://consumer:8081/readyz start
      ;;
    crash-consumer-stop)
      chaos stop consumer stop -t 30 consumer
      sleep_until "$restore_at"
      chaos start consumer docker-start
      wait_ready consumer http://consumer:8081/readyz start
      ;;
    crash-postgres-pause)
      chaos pause postgres pause postgres
      sleep_until "$restore_at"
      chaos unpause postgres unpause postgres
      wait_ready consumer http://consumer:8081/readyz unpause
      ;;
    crash-kafka-stop)
      chaos stop kafka stop kafka
      sleep_until "$restore_at"
      chaos start kafka docker-start
      wait_ready producer http://producer:8080/readyz start
      ;;
    crash-scale-out)
      chaos scale consumer up -d --no-deps --no-build --no-recreate --scale consumer=2 consumer
      wait_ready consumer http://consumer:8081/readyz scale
      ;;
    crash-commit-gap)
      # Kafka goes away first; the consumer then keeps flushing what it already fetched to Postgres
      # (the DB commit succeeds) while every Kafka offset commit fails. SIGKILL while kafka is still
      # down, so no later commit can close the gap: the stored position is ahead of the committed one.
      chaos stop kafka stop kafka
      sleep_until "$LT_GAP_KILL_AT"
      chaos kill consumer kill -s SIGKILL consumer
      sleep_until "$LT_GAP_KAFKA_START_AT"
      chaos start kafka docker-start
      sleep_until "$LT_GAP_CONSUMER_START_AT"
      chaos start consumer docker-start
      wait_ready consumer http://consumer:8081/readyz start
      wait_ready producer http://producer:8080/readyz start
      ;;
    poison-records)
      inject_poison
      ;;
  esac
}

# Poison kinds, in the order they are cycled, with the DLQ error class each must get. The class follows
# the consumer's decodeRecord (services/consumer/internal/adapters/kafka/message.go): a record that
# json.Unmarshal rejects (bad JSON, a wrong type, an unparsable timestamp) is `decode`; valid JSON that
# domain.NewActivityEvent rejects (missing user_id, a control character, a zero timestamp) is `validation`.
readonly POISON_KINDS=(invalid_json truncated_json missing_fields missing_timestamp wrong_type_user_id control_char_user_id zero_timestamp invalid_timestamp)
readonly POISON_CLASSES=(decode decode validation validation decode validation validation decode)

# poison_payload <kind> <id> prints one record value (no newline, no '|').
poison_payload() {
  local kind=$1 id=$2 ts='2026-10-12T10:00:00Z'
  case $kind in
    invalid_json) printf '{not json %s' "$id" ;;
    truncated_json) printf '{"user_id":"%s","activity_type":"page_view","timestamp":"%s"' "$id" "$ts" ;;
    missing_fields) printf '{"note":"%s"}' "$id" ;;
    missing_timestamp) printf '{"user_id":"%s","activity_type":"page_view"}' "$id" ;;
    wrong_type_user_id) printf '{"user_id":12345,"activity_type":"page_view","timestamp":"%s"}' "$ts" ;;
    control_char_user_id) printf '{"user_id":"%s\\u0001x","activity_type":"page_view","timestamp":"%s"}' "$id" "$ts" ;;
    zero_timestamp) printf '{"user_id":"%s","activity_type":"page_view","timestamp":"0001-01-01T00:00:00Z"}' "$id" ;;
    invalid_timestamp) printf '{"user_id":"%s","activity_type":"page_view","timestamp":"yesterday"}' "$id" ;;
  esac
}

# inject_poison writes LT_POISON_COUNT malformed records straight to the input topic from inside the
# kafka container (the producer would answer 400), with keys "poison-<RUN_ID>-<n>", and records the
# injected counts per kind and per expected error class in timeline.jsonl.
inject_poison() {
  local n=$LT_POISON_COUNT i k kafka_ctr input="" started rc=0
  local -A kind_count=() class_count=()
  started=$(ts)
  for ((i = 0; i < n; i++)); do
    k=$((i % ${#POISON_KINDS[@]}))
    input+="poison-${RUN_ID}-${i}|$(poison_payload "${POISON_KINDS[k]}" "poison-${RUN_ID}-${i}")"$'\n'
    kind_count[${POISON_KINDS[k]}]=$((${kind_count[${POISON_KINDS[k]}]:-0} + 1))
    class_count[${POISON_CLASSES[k]}]=$((${class_count[${POISON_CLASSES[k]}]:-0} + 1))
  done
  kafka_ctr=$(docker ps -q --filter "label=com.docker.compose.project=$PROJECT" \
    --filter "label=com.docker.compose.service=kafka" --filter "label=com.docker.compose.oneoff=False" | head -n 1)
  log "poison: writing $n malformed records to $TOPIC"
  {
    printf '%s poison: %s records via kafka-console-producer.sh in %s\n' "$(ts)" "$n" "${kafka_ctr:-?}"
    if [[ -z $kafka_ctr ]]; then
      echo "kafka container not found"
      rc=1
    else
      # The tool runs in the broker's container: a small heap, so it cannot push the broker over its
      # memory limit. `--property` is the older spelling of `--reader-property`; argument errors exit
      # before any record is read, so the second attempt cannot duplicate records.
      local prop
      for prop in --property --reader-property; do
        rc=0
        printf '%s' "$input" | docker exec -i -e KAFKA_HEAP_OPTS="-Xms32m -Xmx64m" "$kafka_ctr" \
          /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic "$TOPIC" \
          "$prop" parse.key=true "$prop" 'key.separator=|' 2>&1 || rc=$?
        ((rc == 0)) && break
      done
    fi
  } >>"$RESULTS/chaos.log"
  local kinds="" classes="" name
  for name in "${POISON_KINDS[@]}"; do
    [[ -n ${kind_count[$name]:-} ]] && kinds+="${kinds:+,}\"$name\":${kind_count[$name]}"
  done
  for name in decode validation; do
    classes+="${classes:+,}\"$name\":${class_count[$name]:-0}"
  done
  timeline_at "$started" poison_injected "\"rc\":$rc,\"count\":$n,\"classes\":{$classes},\"kinds\":{$kinds}"
}

K6_STARTED_AT=$(ts)
timeline_at "$K6_STARTED_AT" k6_start "\"k6_scenario\":\"$K6_SCENARIO\""
if ((CHAOS_RUN)); then
  chaos_timeline &
  CHAOS_PID=$!
fi
log "running k6 (scenario=$K6_SCENARIO)"
set +e
"${COMPOSE_LT[@]}" run --rm --no-deps -T k6 2>&1 | tee "$RESULTS/k6-output.log"
k6_rc=${PIPESTATUS[0]}
set -e
K6_FINISHED_AT=$(ts)
k6_end_ms=$(now_ms)
timeline_at "$K6_FINISHED_AT" k6_end "\"exit_code\":$k6_rc"
log "k6 finished (exit code $k6_rc)"
if [[ -n $CHAOS_PID ]]; then
  log "waiting for the chaos timeline to finish"
  wait "$CHAOS_PID" || true
  CHAOS_PID=""
fi

# ---------- 6. drain ----------

# kafka_lag prints the group's total lag on incoming.user_activity, "nogroup" or "error".
kafka_lag() {
  local out rc=0
  out=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka:9092 --describe --group "$GROUP" 2>&1) || rc=$?
  printf '%s\n' "$out" | awk -v topic="$TOPIC" -v rc="$rc" '
    /does not exist|GroupIdNotFoundException|not found/ { nogroup = 1 }
    $1 == "GROUP" && $2 == "TOPIC" { for (i = 1; i <= NF; i++) col[$i] = i; hdr = 1; next }
    hdr && NF >= col["LAG"] && $(col["TOPIC"]) == topic {
      lag = $(col["LAG"]); end = $(col["LOG-END-OFFSET"])
      if (lag ~ /^[0-9]+$/) s += lag; else if (end ~ /^[0-9]+$/) s += end
      n++
    }
    END {
      if (nogroup || (rc == 0 && n == 0)) print "nogroup"
      else if (n == 0) print "error"
      else printf "%d\n", s
    }'
}

log "drain: waiting up to ${LT_DRAIN_TIMEOUT}s for consumer-group lag 0"
DRAIN_STARTED_AT=$(ts)
timeline_at "$DRAIN_STARTED_AT" drain_start
polls=0
samples=""
drain_seconds=null
timed_out=true
group_found=false
final_lag=null
while :; do
  lag=$(kafka_lag)
  poll_ms=$(now_ms)
  polls=$((polls + 1))
  case $lag in
    nogroup) sample_found=false; sample_lag=null ;;
    error) sample_found=false; sample_lag=null ;;
    *) sample_found=true; sample_lag=$lag; group_found=true; final_lag=$lag ;;
  esac
  samples+="${samples:+,}{\"ts\":\"$(ts)\",\"group_found\":$sample_found,\"lag\":$sample_lag}"
  if [[ $sample_lag == 0 ]]; then
    drain_seconds=$(ms_to_s $((poll_ms - k6_end_ms)))
    timed_out=false
    break
  fi
  if ((poll_ms - k6_end_ms >= LT_DRAIN_TIMEOUT * 1000)); then
    break
  fi
  sleep "$LT_DRAIN_POLL"
done
DRAIN_FINISHED_AT=$(ts)
printf '{"started_at":"%s","finished_at":"%s","seconds":%s,"timed_out":%s,"group_found":%s,"final_lag":%s,"polls":%d,"timeout_seconds":%d,"samples":[%s]}\n' \
  "$DRAIN_STARTED_AT" "$DRAIN_FINISHED_AT" "$drain_seconds" "$timed_out" "$group_found" "$final_lag" \
  "$polls" "$LT_DRAIN_TIMEOUT" "$samples" >"$RESULTS/drain.json"
timeline_at "$DRAIN_FINISHED_AT" drain_end \
  "\"seconds\":$drain_seconds,\"timed_out\":$timed_out,\"group_found\":$group_found,\"final_lag\":$final_lag"
if [[ $timed_out == true ]]; then
  log "drain: not drained after ${LT_DRAIN_TIMEOUT}s (group_found=$group_found, last lag=$final_lag)"
else
  log "drain: lag 0 after ${drain_seconds}s"
fi

# ---------- 7. collect ----------

timeline collect_start
log "collecting metrics, DB totals, offsets, container state and logs"
kill "$STATS_PID" 2>/dev/null || true
STATS_PID=""
probe_get http://producer:8080/metrics "$RESULTS/metrics-after-producer.txt"
probe_get http://consumer:8081/metrics "$RESULTS/metrics-after-consumer.txt"
docker rm -f "$LAGPROBE" >/dev/null 2>&1 || true

psql_csv() { # psql_csv <header> <out file> <step> <<SQL
  local header=$1 out=$2 step=$3 err
  printf '%s\n' "$header" >"$out"
  if ! err=$("${COMPOSE[@]}" exec -T postgres psql -U analytics_owner -d analytics -X -q \
    -v ON_ERROR_STOP=1 -v pattern="lt-${RUN_ID}-%" -At -F, 2>&1 >>"$out"); then
    printf '%s\n' "$header" >"$out"
    collect_error "$step" "$err"
  fi
}
psql_csv "activity_count,page_view_count,distinct_users,rows" "$RESULTS/db-totals.csv" "db totals" <<'SQL'
SELECT COALESCE(SUM(activity_count), 0), COALESCE(SUM(page_view_count), 0),
       COUNT(DISTINCT user_id), COUNT(*)
FROM activity_aggregates
WHERE user_id LIKE :'pattern';
SQL
psql_csv "activity_date,rows,distinct_users,activity_count,page_view_count" "$RESULTS/db-by-date.csv" "db by date" <<'SQL'
SELECT activity_date, COUNT(*), COUNT(DISTINCT user_id), SUM(activity_count), SUM(page_view_count)
FROM activity_aggregates
WHERE user_id LIKE :'pattern'
GROUP BY activity_date
ORDER BY activity_date;
SQL

offsets() { # offsets <topic> <out file>
  local err
  if ! err=$("${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server kafka:9092 --topic "$1" --time -1 2>&1 >"$2"); then
    collect_error "offsets $1" "$err"
  fi
  sed -i 's/\r$//' "$2" 2>/dev/null || true
}
offsets "$DLQ_TOPIC" "$RESULTS/dlq-offsets.txt"
offsets "$TOPIC" "$RESULTS/topic-offsets.txt"

INSPECT_FMT='{"name":{{json (slice .Name 1)}},"service":{{json (index .Config.Labels "com.docker.compose.service")}},"oneoff":{{json (index .Config.Labels "com.docker.compose.oneoff")}},"status":{{json .State.Status}},"oom_killed":{{.State.OOMKilled}},"restart_count":{{.RestartCount}},"exit_code":{{.State.ExitCode}},"started_at":{{json .State.StartedAt}},"finished_at":{{json .State.FinishedAt}},"memory_limit_bytes":{{.HostConfig.Memory}},"nano_cpus":{{.HostConfig.NanoCpus}},"pids_limit":{{json .HostConfig.PidsLimit}},"image":{{json .Config.Image}}}'
mapfile -t containers < <(project_containers)
if ((${#containers[@]})); then
  docker inspect --format "$INSPECT_FMT" "${containers[@]}" 2>/dev/null | sed 's/\r$//' >"$RESULTS/inspect.jsonl" ||
    collect_error "docker inspect" "docker inspect failed"
fi

collect_logs

images=$("${COMPOSE_LT[@]}" images --format json 2>/dev/null | tr -d '\r') || images=""
[[ $images == \[* ]] || images="[]"
consumer_replicas=$(replica_count consumer)

timeline collect_end
FINISHED_AT=$(ts)

# ---------- run.json ----------

limit_json() { printf '{"cpus":%s,"memory":%s,"pids":%s}' "$(json_str "$1")" "$(json_str "$2")" "$3"; }
k6_json=""
for knob in "${K6_KNOBS[@]}"; do
  k6_json+="${k6_json:+,}\"$knob\":$(json_str "${!knob:-}")"
done
if ((CHAOS_RUN)); then
  chaos_extra=""
  case $SCENARIO_NAME in
    crash-commit-gap) chaos_extra=",\"kill_at_s\":$LT_GAP_KILL_AT,\"kafka_start_at_s\":$LT_GAP_KAFKA_START_AT,\"consumer_start_at_s\":$LT_GAP_CONSUMER_START_AT" ;;
    poison-records) chaos_extra=",\"poison_count\":$LT_POISON_COUNT" ;;
  esac
  chaos_json="{\"at_s\":$LT_CHAOS_AT,\"down_for_s\":$LT_CHAOS_DOWN_FOR,\"recovery_grace_s\":$LT_CHAOS_RECOVERY$chaos_extra}"
else
  chaos_json=null
fi
cat >"$RESULTS/run.json" <<EOF
{
  "schema_version": 1,
  "scenario": "$SCENARIO_NAME",
  "k6_scenario": "$K6_SCENARIO",
  "profile": $( [[ -n $PROFILE ]] && json_str "$PROFILE" || echo null),
  "experiment": {"id": $(json_str "$EXPERIMENT_ID"), "name": $(json_str "$EXPERIMENT_NAME")},
  "run_id": "$RUN_ID",
  "user_prefix": "lt-${RUN_ID}-",
  "project": "$PROJECT",
  "results_dir": "$RESULTS_NAME",
  "keep": $( ((KEEP)) && echo true || echo false),
  "build": $( ((BUILD)) && echo true || echo false),
  "started_at": "$STARTED_AT",
  "stack_ready_at": "$STACK_READY_AT",
  "k6_started_at": "$K6_STARTED_AT",
  "k6_finished_at": "$K6_FINISHED_AT",
  "k6_exit_code": $k6_rc,
  "drain_seconds": $drain_seconds,
  "finished_at": "$FINISHED_AT",
  "host": {
    "docker_cpus": $DOCKER_CPUS,
    "docker_mem_bytes": $DOCKER_MEM,
    "docker_server_version": $(json_str "$DOCKER_VERSION"),
    "docker_os": $(json_str "$DOCKER_OS"),
    "compose_version": $(json_str "$COMPOSE_VERSION")
  },
  "images": $images,
  "limits": {
    "kafka": $(limit_json "$LT_KAFKA_CPUS" "$LT_KAFKA_MEMORY" "$LT_KAFKA_PIDS"),
    "postgres": $(limit_json "$LT_POSTGRES_CPUS" "$LT_POSTGRES_MEMORY" "$LT_POSTGRES_PIDS"),
    "producer": $(limit_json "$LT_PRODUCER_CPUS" "$LT_PRODUCER_MEMORY" "$LT_PRODUCER_PIDS"),
    "consumer": $(limit_json "$LT_CONSUMER_CPUS" "$LT_CONSUMER_MEMORY" "$LT_CONSUMER_PIDS"),
    "k6": $(limit_json "$LT_K6_CPUS" "$LT_K6_MEMORY" "$LT_K6_PIDS"),
    "oneshot": $(limit_json "$LT_ONESHOT_CPUS" "$LT_ONESHOT_MEMORY" "$LT_ONESHOT_PIDS"),
    "probe": $(limit_json "$LT_PROBE_CPUS" "$LT_PROBE_MEMORY" "$LT_PROBE_PIDS")
  },
  "config": {
    "topic": "$TOPIC",
    "partitions": $PARTITIONS,
    "dlq_topic": "$DLQ_TOPIC",
    "dlq_partitions": $DLQ_PARTITIONS,
    "consumer_group": "$GROUP",
    "consumer_workers": $LT_CONSUMER_WORKERS,
    "flush_max_records": $LT_FLUSH_MAX_RECORDS,
    "consumer_replicas": ${consumer_replicas:-0},
    "drain_timeout_s": $LT_DRAIN_TIMEOUT,
    "k6": {$k6_json},
    "chaos": $chaos_json
  },
  "expect": {
    "overcount_max": $OVERCOUNT_MAX,
    "consumer_stop_exit_code": $STOP_EXIT_EXPECT
  }
}
EOF

# ---------- 8. report ----------

report_rc=2
if [[ -f loadtest/report/go.mod ]]; then
  log "rendering report ($GOLANG_IMAGE)"
  set +e
  docker run --rm --cpus 2 --memory 1g \
    -e GOTOOLCHAIN=local -e GOPROXY=off -e GOFLAGS=-buildvcs=false \
    -v "$(host_path loadtest):/work" \
    -v moveo-loadtest-gocache:/root/.cache/go-build \
    -w /work/report "$GOLANG_IMAGE" go run . "/work/results/$RESULTS_NAME"
  report_rc=$?
  set -e
else
  log "ERROR: loadtest/report/go.mod not found; no report rendered"
fi
timeline run_end

# ---------- 9. teardown + verdict ----------

if ((!KEEP)); then
  log "tearing down project $PROJECT (down -v)"
  "${COMPOSE_LT[@]}" down -v --remove-orphans >/dev/null 2>&1 || log "warning: down -v failed"
  STACK_STARTED=0
else
  STACK_STARTED=1
fi

case $report_rc in
  0) verdict=PASS ;;
  1) verdict=FAIL ;;
  *) verdict=ERROR ;;
esac
log "results: $RESULTS"
[[ -f $RESULTS/report.md ]] && log "report:  $RESULTS/report.md and $RESULTS/report.html"
[[ -f $RESULTS/report.md ]] && bash loadtest/publish-report.sh "$RESULTS" | sed 's/^/copy:    /' >&2 || true
log "verdict: $verdict (k6 exit $k6_rc, drain ${drain_seconds}s)"
exit "$report_rc"
