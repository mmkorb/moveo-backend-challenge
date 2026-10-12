#!/usr/bin/env bash
# Runs the load and crash scenarios one after another with loadtest/run.sh and prints
# one summary line per scenario. Soak (about 34 min) runs only with --soak, and last.
#
#   bash loadtest/run-suite.sh                  # smoke, load, stress, spike, crash-*, poison-records (about 1 h 15 min)
#   bash loadtest/run-suite.sh --soak           # the same plus soak (about 1 h 50 min)
#   bash loadtest/run-suite.sh load spike       # only the named scenarios
#   bash loadtest/run-suite.sh --profile producer-bound stress   # every run uses that resource profile
#   bash loadtest/run-suite.sh --name baseline  # name the experiment (default: suite, or suite@<profile>)
#
# Images are built once by the first run; later runs use --no-build. LT_* variables
# (see docs/load-testing.md) apply to every run. A failing scenario does not stop the suite.
#
# Experiment: one call is one experiment. Its id is the SUITE_ID (UTC timestamp) and its name comes from
# --name; both are passed to every run.sh (LT_EXPERIMENT_ID, LT_EXPERIMENT_NAME), recorded in run.json and
# used to group the report copies under temp/<YYYY-MM-DD_HHMMSSZ>_<name>/.
#
# Output: each run writes loadtest/results/<RUN_ID>-<scenario>[@<profile>]/ and its temp/ copies as usual.
# The console output of each run and the summary go to loadtest/results/<SUITE_ID>-suite/.
#
# Exit code: 0 if every scenario passed, 1 if any failed, 2 if any harness error.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
readonly DEFAULT_SCENARIOS="smoke load stress spike crash-consumer-stop crash-consumer-kill crash-postgres-pause crash-kafka-stop crash-scale-out crash-commit-gap poison-records"

list_profiles() {
  local f out=""
  for f in "$SCRIPT_DIR"/profiles/*.env; do
    [[ -f $f ]] || continue
    f=${f##*/}
    out+="${out:+ }${f%.env}"
  done
  printf '%s' "$out"
}

usage() {
  cat <<EOF
Usage: bash loadtest/run-suite.sh [--soak] [--no-build] [--profile <name>] [--name <slug>] [scenario ...]

Runs the scenarios one after another (default: smoke load stress spike, the five crash-* plus
crash-commit-gap, and poison-records). The runs form one experiment (see temp/README.md).
  --soak            also run soak (150 req/s for 30 min), after the others
  --no-build        do not build images, not even for the first run
  --profile <name>  run every scenario with loadtest/profiles/<name>.env
                    (profiles: $(list_profiles))
  --name <slug>     name of the experiment (letters, digits, '.', '_', '-'; default:
                    suite, or suite@<profile> with --profile)
  --help            show this help

Exit code: 0 all PASS, 1 any FAIL, 2 any harness error. See docs/load-testing.md.
EOF
}

SOAK=0
BUILD=1
PROFILE=""
NAME=""
SCENARIOS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --soak) SOAK=1 ;;
    --no-build) BUILD=0 ;;
    --profile)
      [[ $# -ge 2 ]] || { echo "--profile needs a name (one of: $(list_profiles))" >&2; exit 2; }
      PROFILE=$2
      shift
      ;;
    --profile=*) PROFILE=${1#--profile=} ;;
    --name)
      [[ $# -ge 2 ]] || { echo "--name needs a slug" >&2; exit 2; }
      NAME=$2
      shift
      ;;
    --name=*) NAME=${1#--name=} ;;
    -h | --help) usage; exit 0 ;;
    -*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
    *) SCENARIOS+=("$1") ;;
  esac
  shift
done
if [[ -n $PROFILE && ! ( $PROFILE =~ ^[a-z0-9][a-z0-9-]*$ && -f $SCRIPT_DIR/profiles/$PROFILE.env ) ]]; then
  echo "unknown profile '$PROFILE' (one of: $(list_profiles))" >&2
  exit 2
fi
[[ ${#SCENARIOS[@]} -gt 0 ]] || read -r -a SCENARIOS <<<"$DEFAULT_SCENARIOS"
if [[ $SOAK -eq 1 && " ${SCENARIOS[*]} " != *" soak "* ]]; then SCENARIOS+=(soak); fi

# The experiment name ends up in a directory name (also on Windows): keep it to a safe alphabet.
NAME=${NAME:-suite${PROFILE:+@$PROFILE}}
NAME=$(printf '%s' "$NAME" | tr -c 'A-Za-z0-9@._-' '-')
SUITE_ID=$(date -u +%Y%m%dt%H%M%Sz)
export LT_EXPERIMENT_ID=$SUITE_ID LT_EXPERIMENT_NAME=$NAME
SUITE_DIR="$REPO_ROOT/loadtest/results/$SUITE_ID-suite"
mkdir -p "$SUITE_DIR"
SUMMARY="$SUITE_DIR/summary.txt"

cd "$REPO_ROOT"
echo "[run-suite] experiment $NAME ($SUITE_ID)" >&2
worst=0
for s in "${SCENARIOS[@]}"; do
  args=("$s")
  [[ -z $PROFILE ]] || args+=(--profile "$PROFILE")
  [[ $BUILD -eq 1 ]] || args+=(--no-build)
  start=$(date -u +%H:%M:%S)
  echo "[run-suite $start] $s" >&2
  rc=0
  bash loadtest/run.sh "${args[@]}" >"$SUITE_DIR/$s.log" 2>&1 || rc=$?
  BUILD=0
  case $rc in
    0) verdict=PASS ;;
    1) verdict=FAIL; if [[ $worst -lt 1 ]]; then worst=1; fi ;;
    *) verdict="ERROR (exit $rc)"; worst=2 ;;
  esac
  line="$s${PROFILE:+@$PROFILE}: $verdict,$start-$(date -u +%H:%M:%S) UTC, console log $SUITE_DIR/$s.log"
  echo "$line" | tee -a "$SUMMARY" >&2
done
echo "summary: $SUMMARY; reports: temp/README.md" >&2
exit "$worst"
