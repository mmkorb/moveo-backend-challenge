#!/usr/bin/env bash
# Coverage report per service module (ADR 0014).
#
#   scripts/coverage.sh [--integration] [module...]
#
# For each module (default: producer consumer) it runs the unit tests with
# -coverpkg=./... so every package of the module is measured, whichever package's
# tests exercise it. With --integration it also runs the tests tagged `integration`
# (needs Docker; one container at a time, -p 1) and merges both runs.
#
# Profiles are collected as Go coverage data directories (-test.gocoverdir) and
# merged with `go tool covdata`, which adds counters of the same code block across
# runs. Concatenating text profiles would double count blocks, so it is not used.
#
# Output, per module, under <repo>/coverage/:
#   <module>.out    merged text profile (go tool cover format)
#   <module>.html   annotated source report
# and on stdout: coverage per package and the total.
#
# Docker Desktop on Windows can time out waiting for testcontainers' reaper when
# several test binaries start containers one after another; if that happens, run
# with TESTCONTAINERS_RYUK_DISABLED=true (the tests still terminate their containers).
#
# This is a report, not a gate: the 80 % gate on domain + application stays in
# `make test`.
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/coverage.sh [--integration] [module...]

  --integration   also run the tests tagged "integration" (needs Docker) and merge
  module          producer and/or consumer (default: both)
EOF
}

integration=0
modules=()
for arg in "$@"; do
  case "$arg" in
    --integration) integration=1 ;;
    -h | --help)
      usage
      exit 0
      ;;
    -*)
      echo "unknown flag: $arg" >&2
      usage >&2
      exit 2
      ;;
    *) modules+=("$arg") ;;
  esac
done
if [ "${#modules[@]}" -eq 0 ]; then
  modules=(producer consumer)
fi

# Native absolute path: `pwd -W` on Git Bash (Go on Windows needs it), plain pwd elsewhere.
abs_path() {
  (
    cd "$1" && { pwd -W 2>/dev/null || pwd; }
  )
}

repo_root=$(abs_path "$(dirname "${BASH_SOURCE[0]}")/..")
out_dir="$repo_root/coverage"
mkdir -p "$out_dir"

export GOWORK=off
export GOMAXPROCS="${GOMAXPROCS:-2}"
export GOFLAGS="${GOFLAGS:--p=2}"

# percent <covdata dir>: per-package coverage, module path trimmed.
percent() {
  go tool covdata percent -i="$1" | grep "coverage:" | sed -e 's#github.com/mmkorb/moveo-backend-challenge/services/##' -e 's/^[[:space:]]*/  /'
}

# profile_total <module dir> <text profile>: the total statement coverage. cover
# resolves import paths with `go list`, so it runs inside the module.
profile_total() {
  (
    cd "$1" && go tool cover -func="$2" | awk '/^total:/ {print $3}'
  )
}

report_module() {
  local module=$1
  local dir="$repo_root/services/$module"
  if [ ! -f "$dir/go.mod" ]; then
    echo "no such module: services/$module" >&2
    return 2
  fi
  local work="$out_dir/.work-$module"
  rm -rf "$work"
  mkdir -p "$work/unit" "$work/merged"
  # Leave no raw coverage data behind, whatever happens.
  trap 'rm -rf "$work"' RETURN

  echo "== $module: unit tests"
  (
    cd "$dir"
    go test -count=1 -covermode=atomic -coverpkg=./... ./... -args -test.gocoverdir="$work/unit"
  )
  echo "$module unit coverage by package:"
  percent "$work/unit"
  go tool covdata textfmt -i="$work/unit" -o="$work/unit.out"
  echo "$module total (unit): $(profile_total "$dir" "$work/unit.out")"

  local merge_inputs="$work/unit"
  if [ "$integration" -eq 1 ]; then
    mkdir -p "$work/integration"
    echo "== $module: integration tests (Docker)"
    (
      cd "$dir"
      go test -count=1 -p 1 -tags=integration -covermode=atomic -coverpkg=./... ./... -args -test.gocoverdir="$work/integration"
    )
    merge_inputs="$work/unit,$work/integration"
  fi

  go tool covdata merge -i="$merge_inputs" -o="$work/merged"
  go tool covdata textfmt -i="$work/merged" -o="$out_dir/$module.out"
  # cover resolves the profile's import paths with `go list`, so it runs inside the module.
  (cd "$dir" && go tool cover -html="$out_dir/$module.out" -o="$out_dir/$module.html")

  local label="unit"
  if [ "$integration" -eq 1 ]; then
    label="unit + integration"
  fi
  echo "$module coverage by package ($label):"
  percent "$work/merged"
  echo "$module total ($label): $(profile_total "$dir" "$out_dir/$module.out")"
  echo "report: coverage/$module.html  profile: coverage/$module.out"
}

for module in "${modules[@]}"; do
  report_module "$module"
done
