#!/usr/bin/env bash
# Copies run reports into temp/, grouped by experiment, and rebuilds the READMEs (the indexes).
#
#   loadtest/publish-report.sh loadtest/results/<RUN_ID>-<scenario>[@<profile>]   # publish one run
#   loadtest/publish-report.sh --all                                 # rebuild temp/ from loadtest/results/
#
# An experiment is the set of runs of one `run-suite.sh` call (or one standalone `run.sh`). run.sh records
# it in run.json as experiment{id, name}.
#
# Layout (temp/ is gitignored; everything in it is a copy and can be regenerated with --all):
#   temp/README.md                                          index of experiments, newest first
#   temp/<YYYY-MM-DD_HHMMSSZ>_<name>/README.md             the runs of that experiment, in run order
#   temp/<YYYY-MM-DD_HHMMSSZ>_<name>/<HHMMSSZ>_<scenario>[@<profile>]_<PASS|FAIL>.md    report (text)
#   temp/<YYYY-MM-DD_HHMMSSZ>_<name>/<HHMMSSZ>_<scenario>[@<profile>]_<PASS|FAIL>.html  same, with charts
#
# --all deletes the contents of temp/ first. Runs written before experiments existed (no experiment in
# run.json) join the experiment "suite" (+ "@<profile>") of the newest loadtest/results/<SUITE_ID>-suite/*.log
# that mentions the run directory; any other becomes an experiment of its own.
#
# Environment (tests): LT_TEMP_DIR (default <repo>/temp), LT_RESULTS_DIR (default <repo>/loadtest/results).
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
TEMP_DIR="${LT_TEMP_DIR:-$REPO_ROOT/temp}"
RESULTS_DIR="${LT_RESULTS_DIR:-$REPO_ROOT/loadtest/results}"

describe() {
  case $1 in
    smoke)                echo "200 events; database totals must equal accepted events exactly" ;;
    load)                 echo "steady 300 req/s for 5 min: latency, lag and resources" ;;
    stress)               echo "ramp 100 to 1500 req/s over ~10 min: find the throughput limit" ;;
    spike)                echo "burst 50 to 1500 req/s for 30 s and back: absorb and drain" ;;
    soak)                 echo "150 req/s for 30 min: memory growth and leaks" ;;
    crash-consumer-stop)  echo "consumer stopped gracefully (SIGTERM) for 15 s: zero double counts" ;;
    crash-consumer-kill)  echo "consumer killed (SIGKILL) for 15 s: no loss, zero double counts" ;;
    crash-postgres-pause) echo "Postgres frozen for 30 s: no loss, nothing dead-lettered" ;;
    crash-kafka-stop)     echo "Kafka stopped for 20 s: producer returns 503, then recovers" ;;
    crash-scale-out)      echo "second consumer added mid-run: rebalance with no loss" ;;
    crash-commit-gap)     echo "Kafka down, consumer killed before it can commit: the stored position prevents double counts" ;;
    poison-records)       echo "malformed records written to the topic: all dead-lettered in the right class, no stall" ;;
    *)                    echo "-" ;;
  esac
}

# safe_name makes a string usable in a directory name on every host (no ':' and the like).
safe_name() {
  local s
  s=$(printf '%s' "$1" | tr -c 'A-Za-z0-9@._-' '-')
  printf '%s' "${s:-unnamed}"
}

# stamp_of <id> prints YYYY-MM-DD_HHMMSSZ for an id like 20261012t030000z.
stamp_of() { printf '%s-%s-%s_%sZ' "${1:0:4}" "${1:4:2}" "${1:6:2}" "${1:9:6}"; }

# run_experiment <run.json> prints "<id> <name>" of its experiment, or nothing.
run_experiment() {
  [[ -f $1 ]] || return 0
  sed -n 's/.*"experiment": *{ *"id": *"\([0-9a-z]*\)", *"name": *"\([^"]*\)".*/\1 \2/p' "$1" | head -n 1
}

# suite_of <run dir name> prints the id of the newest suite whose logs mention the run, or nothing.
suite_of() {
  local suite_dir id
  while IFS= read -r suite_dir; do
    [[ -n $suite_dir ]] || continue
    if grep -qsF -- "$1" "$suite_dir"/*.log; then
      id=$(basename "$suite_dir")
      printf '%s' "${id%-suite}"
      return 0
    fi
  done < <(find "$RESULTS_DIR" -mindepth 1 -maxdepth 1 -type d -name '*-suite' 2>/dev/null | sort -r)
}

# publish_one <run dir> [<experiment id> <experiment name>] copies the report into the experiment folder.
# Without an experiment, run.json's is used; a run without one is an experiment of its own.
publish_one() {
  local dir=${1%/} exp_id=${2:-} exp_name=${3:-} name run_id scenario profile verdict base exp_dir hms
  [[ -f $dir/report.md ]] || { echo "skip (no report.md): $dir" >&2; return 0; }
  name=$(basename "$dir")
  run_id=${name%%-*}
  scenario=${name#*-}       # <scenario> or <scenario>@<profile>
  profile=""
  if [[ $scenario == *@* ]]; then
    profile=${scenario#*@}
    scenario=${scenario%%@*}
  fi
  if [[ -z $exp_id ]]; then
    read -r exp_id exp_name < <(run_experiment "$dir/run.json") || true
  fi
  if [[ -z $exp_id ]]; then
    exp_id=$run_id
    exp_name="${scenario}${profile:+@$profile}"
  fi
  exp_dir="$TEMP_DIR/$(stamp_of "$exp_id")_$(safe_name "$exp_name")"
  verdict=$(head -n 1 "$dir/report.md" | grep -oE '(PASS|FAIL)$' || echo ERROR)
  hms="${run_id:9:6}Z"
  base="${hms}_${scenario}${profile:+@$profile}_${verdict}"
  mkdir -p "$exp_dir"
  # A rerun of the same run with another verdict replaces its earlier copy.
  rm -f "$exp_dir/${hms}_${scenario}${profile:+@$profile}"_*.md "$exp_dir/${hms}_${scenario}${profile:+@$profile}"_*.html
  cp "$dir/report.md" "$exp_dir/$base.md"
  [[ -f $dir/report.html ]] && cp "$dir/report.html" "$exp_dir/$base.html"
  echo "temp/$(basename "$exp_dir")/$base.md"
  PUBLISHED_EXPERIMENTS+=("$exp_dir")
}

# write_experiment_index <experiment dir> writes its README.md: the runs in run order.
write_experiment_index() {
  local exp_dir=$1 f file rest verdict scenario profile checks html
  {
    echo "# Experiment ${exp_dir##*/}"
    echo
    echo "The runs of one experiment (one \`run-suite.sh\` call or one standalone \`run.sh\`), in run order."
    echo "Times are UTC. **.md** is the report as text, **.html** the same report with charts."
    echo
    echo "| Time (UTC) | Scenario | Profile | What it tests | Result | Report |"
    echo "|---|---|---|---|---|---|"
    while IFS= read -r f; do
      file=$(basename "$f" .md)
      rest=${file#*Z_}              # <scenario>[@<profile>]_<VERDICT>
      verdict=${rest##*_}
      scenario=${rest%_*}
      profile="-"
      if [[ $scenario == *@* ]]; then
        profile=${scenario#*@}
        scenario=${scenario%%@*}
      fi
      checks=$(head -n 5 "$f" | grep -m1 -oE '[0-9]+ checks, [0-9]+ failed' || true)
      html=""
      [[ -f ${f%.md}.html ]] && html=" · [html]($file.html)"
      echo "| ${file:0:2}:${file:2:2}:${file:4:2} | \`$scenario\` | $profile | $(describe "$scenario") | **$verdict**${checks:+ ($checks)} | [md]($file.md)$html |"
    done < <(find "$exp_dir" -maxdepth 1 -name '*.md' ! -name README.md | sort)
  } > "$exp_dir/README.md"
}

# write_root_index writes temp/README.md: one row per experiment, newest first.
write_root_index() {
  local d dir_name stamp date time name runs pass fail
  {
    echo "# Load-test reports"
    echo
    echo "Copies of the reports written by \`loadtest/run.sh\`, grouped by experiment, newest first. An"
    echo "experiment is one \`run-suite.sh\` call (or one standalone \`run.sh\`). The full run (raw data,"
    echo "logs, k6 dashboard) stays in \`loadtest/results/<run-id>-<scenario>/\`."
    echo
    echo "- Open an experiment to see its runs, with a link to each report (.md) and its charts (.html)."
    echo "- Times are UTC. This file is regenerated after every run."
    echo
    echo "| Date (UTC) | Experiment | Runs | PASS | FAIL | Folder |"
    echo "|---|---|---|---|---|---|"
    while IFS= read -r d; do
      dir_name=$(basename "$d")
      stamp=${dir_name%%Z_*}
      date=${stamp%%_*}
      time=${stamp#*_}
      name=${dir_name#*Z_}
      runs=$(find "$d" -maxdepth 1 -name '*.md' ! -name README.md | wc -l | tr -d ' ')
      pass=$(find "$d" -maxdepth 1 -name '*_PASS.md' | wc -l | tr -d ' ')
      fail=$((runs - pass))
      echo "| $date ${time:0:2}:${time:2:2}:${time:4:2} | \`$name\` | $runs | $pass | $fail | [$dir_name]($dir_name/README.md) |"
    done < <(find "$TEMP_DIR" -mindepth 1 -maxdepth 1 -type d | awk -F/ '{print $NF "\t" $0}' | sort -r | cut -f2-)
  } > "$TEMP_DIR/README.md"
}

# rebuild_all regenerates temp/ from the results directory.
rebuild_all() {
  local d name run_id scenario profile exp_id exp_name suite_id
  [[ -n $TEMP_DIR && $TEMP_DIR != / ]] || { echo "refusing to clean '$TEMP_DIR'" >&2; exit 2; }
  mkdir -p "$TEMP_DIR"
  find "$TEMP_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
  for d in "$RESULTS_DIR"/*/; do
    [[ -d $d ]] || continue
    name=$(basename "$d")
    [[ $name == *-suite ]] && continue
    exp_id="" exp_name=""
    read -r exp_id exp_name < <(run_experiment "$d/run.json") || true
    if [[ -z $exp_id ]]; then
      # Legacy run: a suite whose console log names it, else an experiment of its own.
      suite_id=$(suite_of "$name")
      if [[ -n $suite_id ]]; then
        run_id=${name%%-*}
        scenario=${name#*-}
        profile=""
        [[ $scenario == *@* ]] && profile=${scenario#*@}
        exp_id=$suite_id
        exp_name="suite${profile:+@$profile}"
      fi
    fi
    publish_one "$d" "$exp_id" "$exp_name" >/dev/null
  done
}

PUBLISHED_EXPERIMENTS=()
if [[ ${1:-} == --all ]]; then
  rebuild_all
  while IFS= read -r d; do write_experiment_index "$d"; done < <(find "$TEMP_DIR" -mindepth 1 -maxdepth 1 -type d)
elif [[ -n ${1:-} ]]; then
  mkdir -p "$TEMP_DIR"
  publish_one "$1"
  for d in "${PUBLISHED_EXPERIMENTS[@]}"; do write_experiment_index "$d"; done
else
  echo "usage: $0 <results-dir> | --all" >&2
  exit 2
fi
mkdir -p "$TEMP_DIR"
write_root_index
