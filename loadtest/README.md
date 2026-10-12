# loadtest

k6 load and crash-test harness. Run it with `bash loadtest/run.sh <scenario>`.

Everything about running it, its outputs and how to read the report is in [docs/load-testing.md](../docs/load-testing.md). Design: [ADR 0016](../docs/adrs/0016-load-and-crash-testing.md).

| Path | What it is |
|---|---|
| `run.sh` | orchestrator; the comment block at its top specifies every output file |
| `run-suite.sh` | runs several scenarios in a row as one experiment (`--name`); soak only with `--soak` |
| `publish-report.sh` | copies a run's report into `temp/<date>_<experiment>/` and rebuilds the indexes; `--all` regenerates `temp/` |
| `profiles/` | resource profiles (`--profile <name>`): `equal`, `producer-bound`, `consumer-bound`, `db-bound`, `kafka-bound` |
| `compose.loadtest.yaml`, `compose.coverage.yaml` | Compose overlays: limits plus k6 and probe; opt-in coverage build |
| `k6/` | scenarios, thresholds and payload generator |
| `report/` | stdlib-only Go module that renders `report.md` and `report.html` ([notes](../docs/load-testing.md#report-module-developers)) |
| `results/` | one directory per run (gitignored) |
