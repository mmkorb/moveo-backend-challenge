# ADR 0014: Testing strategy and quality tooling

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** both

## Context

- Our summary of the challenge (§8) lists what must be tested: the end-to-end path (HTTP → `incoming.user_activity` → aggregate row), repeated users, different activity types, date boundaries, invalid requests, Kafka/Postgres connection failures, retry behaviour and shutdown.
- The code is hexagonal ([0003](0003-system-decomposition-and-layout.md)): `domain` and `application` depend only on ports, so they can be tested with in-memory fakes. Adapters talk to real Kafka and Postgres and need real instances to be tested honestly.
- The consumer is concurrent and time-driven (flush every `FLUSH_INTERVAL`, backoff with jitter; [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), [0011](0011-failure-handling-retry-and-dlq.md)). Tests with real sleeps are slow and flaky.
- The two services are separate Go modules that share no Go code; the only shared artifact is `contracts/user_activity.v1.schema.json` ([0006](0006-event-contract-and-json-serialization.md)). Drift between the producer's and the consumer's DTO is the main integration risk.
- golangci-lint is not tracked with the `go.mod` `tool` directive, which its maintainers advise against (see Decision).

## Decision

### Libraries

- stdlib `testing` and `net/http/httptest`; `github.com/google/go-cmp/cmp` for diffs of structs and slices.
- **Hand-written fakes** for the ports (`fakeActivityPublisher`, `fakeAggregateRepository`, `fakeDeadLetterPublisher`). They record calls and return injected errors. No mock generators, no `testify`.
- `testcontainers-go` for integration tests: its `postgres` module for Postgres, and the generic `testcontainers.Run` with `apache/kafka:4.3.1` for Kafka. The `modules/kafka` module is Confluent-only in v0.44, so we do not use it.
- `go.uber.org/goleak` for goroutine-leak checks.
- `testing/synctest` for time-driven code.

### Test layers

| Layer | What | How | Build tag |
|---|---|---|---|
| `domain` | `ActivityEvent` validation; `DailyUserAggregate` delta rules | table-driven unit tests | — |
| `application` | `RegisterActivity`, `ProcessActivity` orchestration and error wrapping | fakes for ports | — |
| `adapters/httpapi` | status codes 202/400/413/415/503, error bodies, headers | `httptest.NewRecorder` + fake `ActivityPublisher` | — |
| `adapters/kafka` (DTO) | DTO ⇄ domain mapping; contract fixtures | golden JSON files | — |
| partition worker | flush on size/interval, retry/backoff, DLQ routing, commit only after flush | fakes + `synctest` | — |
| `adapters/postgres` | upsert SQL, CHECK constraints, concurrency, migrations, role grants | real Postgres via testcontainers | `integration` |
| `adapters/kafka` (client) | publish/consume round-trip, headers, DLQ, rebalance, commit | real Kafka via testcontainers | `integration` |
| end-to-end | whole stack | `compose.yaml` + `loadtest/run.sh smoke` ([0016](0016-load-and-crash-testing.md)) | local gate |

### Required domain and use-case cases

Table-driven, named `should <behavior> when <condition>`:

- **Repeated users:** three events for `u1` on the same day → one `DailyUserAggregate` with `activity_count = 3`.
- **Different activity types:** `page_view`, `click`, an unknown `foo` → `activity_count = 3`, `page_view_count = 1`. Unknown types are accepted ([0004](0004-domain-model-and-ubiquitous-language.md)).
- **`page_view` counting:** only the exact value `page_view` counts.
- **UTC date boundaries:** `23:59:59.999Z` and `00:00:00Z` land on different dates; `2024-07-01T23:30:00-03:00` lands on **2024-07-02** (UTC); `2024-07-02T01:00:00+05:00` lands on **2024-07-01**. Timestamps are compared after normalization to UTC.
- **Folding:** a batch with many users and dates folds into the right set of deltas, independent of input order.
- **Invalid input:** empty `user_id` or `activity_type`, non-RFC 3339 timestamp, oversized fields → validation error (producer 400; consumer DLQ).
- **Failures:** publisher error → 503; repository transient error → retried, never dead-lettered, nothing committed; decode error → DLQ then commit; DLQ publish failure → treated as transient.
- **Shutdown:** cancelling the context flushes the pending batch, commits, and returns without leaked goroutines.

### Contract test (no heavy dependencies)

- `contracts/examples/` holds golden JSON files: `valid/*.json` (the challenge example, no metadata, non-UTC offset, unknown activity type) and `invalid/*.json` (missing fields, wrong types, bad timestamp, empty activity type, a NUL in `user_id`, a newline in `activity_type`, non-string metadata value, not JSON). There is deliberately no extra-field example: the consumer ignores unknown fields ([ADR 0006](0006-event-contract-and-json-serialization.md)), while the producer rejects them, which its handler tests cover.
  - `valid/`: `challenge_example.json`, `no_metadata.json`, `non_utc_offset.json`, `unknown_activity_type.json`.
  - `invalid/`: `activity_type_newline.json`, `user_id_nul.json`, `empty_activity_type.json`, `metadata_non_string_value.json`, `missing_timestamp.json`, `missing_user_id.json`, `not_json.json`, `timestamp_not_rfc3339.json`, `user_id_wrong_type.json`.
- Each service has a test in `adapters/kafka` that reads the repo-root `contracts/examples` (`../../../../../contracts/examples` from the package directory). The Docker `test` stage only sees the service directory as its build context, so the test skips there when the fixtures are missing. When `CI=true` it fails instead of skipping, so a host run with `CI=true` enforces the contract:
  - every `valid` file decodes into the service's DTO with `DisallowUnknownFields` and maps to a domain `ActivityEvent`;
  - every `invalid` file is rejected by decode or validation;
  - the producer test also checks that encoding a DTO and decoding it again gives the same value.
- A second test parses the schema file with `encoding/json` and checks that the DTO's JSON field names equal the schema's `properties`, and that `required` fields are non-`omitempty`.
- A full JSON Schema validator (for example `santhosh-tekuri/jsonschema`) is **not** added. It is the escalation path if the schema gains formats or conditional rules that the fixtures can't express.
- A contract change MUST be tested in **both** services: run both test suites after touching `contracts/**`.

### Integration tests

- Files start with `//go:build integration`; run with `go test -tags=integration ./...`. They need Docker; `make test-integration` runs them.
- Containers use the same Kafka and Postgres images as `compose.yaml` ([0015](0015-containerization-and-local-deployment.md), tags in [0001](0001-versions-and-upgrade-policy.md)).
- Postgres tests run the real goose migrations first, then connect as `analytics_app`. They check:
  - the upsert adds deltas;
  - `updated_at` changes on conflict;
  - CHECK constraints reject bad rows;
  - two concurrent flushes to the same row sum correctly;
  - `analytics_app` cannot run DDL.
- Kafka tests cover:
  - produce/consume with key = `user_id` and the `event-id` / `schema-version` headers;
  - DLQ headers;
  - positions stored with the DB write: a crash between the DB commit and the Kafka commit resumes from the stored position, with no record counted twice ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md));
  - partition revoke on a second group member.
- Failure tests stop/start the Postgres container while the consumer runs. The partition blocks and retries, nothing goes to the DLQ, and processing resumes after the restart.

### Goroutines and time

- Every package that starts goroutines (`adapters/kafka`, the partition worker, `cmd/*` wiring tests) has `func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }`. Integration packages add `goleak.IgnoreTopFunction` only for named third-party background goroutines, with a comment.
- Flush-interval and backoff tests run inside `synctest.Test`. The bubble's fake clock makes "wait 1 s" or "back off 30 s" instant and deterministic. These tests use fakes only, because real network I/O can't run inside a bubble.
- `go test -race` needs cgo, which is unavailable on the development machine (Windows 11, Docker Desktop), so race runs happen in Linux containers.

### End-to-end smoke

`loadtest/run.sh smoke` ([0016](0016-load-and-crash-testing.md)) posts 200 events for a pool of synthetic users, some of them on the previous UTC day, to `POST /v1/activities`. It waits for the consumer lag to reach 0, then queries `activity_aggregates` through `psql` in the Postgres container and checks that the totals equal the accepted events exactly. It runs in its own Compose project (`moveo-loadtest`), starts with `up -d --build --wait`, always ends with `down -v`, and exits non-zero on FAIL. The report is written to the run directory. The heavier load and crash scenarios of the same harness are run manually.

### Coverage

- `domain` and `application`: **≥ 80 %** statement coverage, enforced by `make test`.
- `adapters/httpapi` and the partition worker: ≥ 60 %.
- Other adapters are covered by integration tests; no gate.
- `scripts/coverage.sh [--integration] [module...]` (or `make coverage`) writes a report, not a gate. It runs each module with `-coverpkg=./...` so a package counts even when only another package's tests reach it. It collects Go coverage data directories (`-test.gocoverdir`) and merges the unit and integration runs with `go tool covdata`. Concatenating text profiles would double count shared blocks. It prints coverage per package and the total, and writes `coverage/<module>.html` and `coverage/<module>.out` (git-ignored).
- End-to-end coverage is opt-in. Both Dockerfiles take `ARG COVER=0`; `COVER=1` builds with `go build -cover`. `loadtest/compose.coverage.yaml` builds `moveo-analytics/<service>:cover` images and mounts a host directory as `GOCOVERDIR`, because the root filesystem is read-only. After `loadtest/run.sh smoke`, `go tool covdata percent -i=<dir>` shows what the real stack ran ([docs/load-testing.md](../load-testing.md), "E2E coverage"). The default images are unchanged.

### Fuzzing, properties and benchmarks

- **Fuzz tests** (Go native, `FuzzXxx`) cover the two untrusted-input decoders: the producer's HTTP body decoder and the consumer's Kafka record decoder. Both are seeded from `contracts/examples/**`. They check that the decoder never panics, that an accepted event satisfies the domain rules (bounded non-empty `user_id` and `activity_type`, non-zero UTC timestamp), and that a rejected record becomes a dead letter of class `decode` or `validation` with the original bytes, headers and coordinates unchanged. Plain `go test` runs only the seeds; run `go test -run=NONE -fuzz=FuzzDecodeRecord -fuzztime=60s` to fuzz. A crash is saved under `testdata/fuzz/` and MUST be committed with the fix.
- **Property test** (consumer `application`): for seeded random event sets (`math/rand/v2`, no new dependency), any permutation and any split into batches gives the same totals per (date, user). The sum of `activity_count` equals the number of events, `page_view_count` equals the number of `page_view` events, and the keys in one batch are unique and sorted.
- **Benchmarks** (`b.ReportAllocs`): the consumer fold of a 500-event batch, the producer decode and validate of one request, and the consumer `decodeRecord`. The Postgres upsert benchmark (500 deltas per `ApplyDeltas`) carries the `integration` tag and reuses the package's single container. They are for comparing a change against a baseline on the same machine, not gates.

### Quality tooling

- **golangci-lint v2, installed as a binary** (version in [0001](0001-versions-and-upgrade-policy.md)). `make lint` installs the same release binary into `./bin` with the project's official install script, then runs it.
  - It is **not** tracked through the `go.mod` `tool` directive or `go install`. Its dependency graph (hundreds of linter modules) would join each service's module graph and `go.sum`. Minimal version selection could then silently raise versions of shared modules (`golang.org/x/tools`, `x/sync`, …) in production builds, and dependency updates would be noisy. The maintainers recommend binary installs, because the binary they release is the one they test.
- One shared `.golangci.yml` at the repo root. golangci-lint finds it from each module directory. It sets `run.build-tags: [integration]`, so `make lint` lints the Docker-backed test files too, and covers `loadtest/report` as well as both services. Test files are excluded from `errcheck`, `gosec` and `noctx` (unchecked `Close` in defers, fixture paths and fixed seeds, context-free `httptest` helpers). Production code is held to all linters; the few `//nolint` comments name the linter and give a reason.

```yaml
version: "2"
run:
  build-tags:
    - integration
linters:
  default: standard
  enable:
    - bodyclose
    - errorlint
    - forbidigo
    - gosec
    - noctx
    - rowserrcheck
    - sqlclosecheck
  settings:
    forbidigo:
      forbid:
        - pattern: ^fmt\.Print.*$
        - pattern: ^slog\.SetDefault$
        - pattern: ^promauto\.
  exclusions:
    rules:
      - path: _test\.go
        linters:
          - errcheck
          - gosec
          - noctx
formatters:
  enable:
    - gofmt
    - goimports
```

- **govulncheck** runs as `go run golang.org/x/vuln/cmd/govulncheck@<version from 0001> ./...` in each module. A `tool` directive would also be acceptable here, because its graph is small and maintained by the Go team. `go run …@version` keeps the service `go.mod` files free of tooling.
- **`scripts/check-boundaries.sh`** runs per module using `go list` and fails on:
  1. `internal/domain` importing anything but the stdlib;
  2. `internal/ports` importing anything but the stdlib and `domain`;
  3. `internal/application` importing `adapters` or `platform`;
  4. struct tags (`json:`, `db:`) in `internal/domain`;
  5. the producer module importing the consumer module, or the other way round (including `require`/`replace` lines in `go.mod`).

### Quality gate

The gate runs locally. With `GOWORK=off`:

1. Format and vet: `make lint` runs `gofmt -l .` (must print nothing), `go vet ./...`, `go vet -tags=integration ./...` and golangci-lint for both services and `loadtest/report`.
2. Unit tests and the coverage gate: `make test`.
3. Architecture boundaries: `make boundaries` (`scripts/check-boundaries.sh`).
4. Integration tests: `make test-integration` (Docker). Add `-race` in a Linux container, since the development machine has no cgo.
5. Vulnerabilities: `make vuln` (govulncheck).
6. Images: `docker build --check .`, then `docker build --target test .`, then the final image build, from each service directory.
7. Image scan: Docker Scout locally; Trivy failing on fixable critical/high findings is recommended in a real pipeline ([0002](0002-security-baseline-and-threat-model.md)).
8. End-to-end smoke with Compose: `bash loadtest/run.sh smoke` (`make smoke`, [0016](0016-load-and-crash-testing.md)).

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| `testify` (assert/require/mock) | Rejected | `go-cmp` gives better diffs; the stdlib idiom `got, want` is enough. |
| `gomock` / `mockery` | Rejected | Generated mocks pin call order and drift from real semantics. The ports are small; fakes are a few lines. |
| Docker Compose services for integration tests | Rejected | Shared state between tests, manual lifecycle. Testcontainers gives each test package a clean, disposable instance. Compose is still used for the end-to-end smoke. |
| Full JSON Schema validator in both modules | Deferred | One extra dependency per module for a three-field schema. Golden fixtures catch real drift. |
| golangci-lint via `go.mod` `tool` directive / `go install` | Rejected | Pollutes the service dependency graph; not the binary the maintainers test. |
| `depguard` instead of the boundary script | Deferred | Works, but the rules are verbose in YAML. The script also checks cross-module `go.mod` lines and struct tags. |
| `time.Sleep` / injected clock interfaces for timers | Rejected | Flaky, or more interfaces in production code. `synctest` tests real `time` calls. |

## Consequences

**Positive**
- Every case listed in the summary maps to a named test.
- Domain and use-case tests run in milliseconds, with no Docker.
- Producer/consumer drift is caught by shared fixtures without sharing Go code.
- Time-based behaviour is tested deterministically.
- Tooling doesn't touch production `go.mod` files.

**Negative / trade-offs**
- Integration tests need Docker and take tens of seconds. Mitigation: the build tag keeps them out of the inner loop; run `make test-integration` before finishing a change.
- Hand-written fakes must honour port contracts (same sentinel errors, respect `ctx`). Mitigation: Postgres integration tests run the same scenarios against the real adapter.
- `-race` can't run on the development machine (Windows 11). Mitigation: run it in a Linux container.
- The fixture-based contract test checks field names and types, not every schema keyword. Mitigation: the escalation path to a full validator is documented above.

## Implementation rules

- Tests MUST use `testing`, `httptest` and `go-cmp`. `testify` and mock generators MUST NOT be added.
- Every port MUST have a hand-written fake in the consuming package's tests.
- Domain rule tests MUST be table-driven and cover repeated users, mixed and unknown activity types, and UTC boundaries with non-UTC offsets.
- Tests that need Docker MUST carry `//go:build integration`.
- Packages that start goroutines MUST call `goleak.VerifyTestMain`.
- Timer and backoff tests MUST run inside `synctest.Test` and MUST NOT call real `time.Sleep` outside a bubble.
- Both services MUST run the contract test against `contracts/examples`. It MAY skip only when the fixtures are absent and `CI` is not set (`CI=true` makes a missing fixture a failure). A schema change MUST come with updated fixtures.
- golangci-lint MUST be installed as a release binary pinned to the version in 0001. It MUST NOT appear in any `go.mod`.
- The local gate MUST fail on: lint findings, coverage below 80 % for `domain` + `application`, boundary violations, govulncheck findings, `docker build --check` findings, and fixable critical/high image CVEs.

## Related

- [0003](0003-system-decomposition-and-layout.md) — layering enforced by `check-boundaries.sh`.
- [0004](0004-domain-model-and-ubiquitous-language.md) — the domain rules under test.
- [0006](0006-event-contract-and-json-serialization.md) — schema and fixtures for the contract test.
- [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md), [0011](0011-failure-handling-retry-and-dlq.md) — batching, retry and DLQ behaviour under test.
- [0012](0012-lifecycle-and-graceful-shutdown.md) — shutdown tests.
- [0015](0015-containerization-and-local-deployment.md) — `test` stage and Compose used in the smoke test.
- [0001](0001-versions-and-upgrade-policy.md) — tool and library versions.
- [0002](0002-security-baseline-and-threat-model.md) — security gates.
- [0016](0016-load-and-crash-testing.md) — the load-test harness whose `smoke` scenario is the end-to-end test.

## References

- [1] The Go Project: `testing` (https://pkg.go.dev/testing), `net/http/httptest` (https://pkg.go.dev/net/http/httptest), "Fuzzing" (https://go.dev/doc/security/fuzz/), `testing/synctest` at Go 1.27.2 (https://pkg.go.dev/testing/synctest@go1.27.2) and the Go 1.25 release notes (https://go.dev/doc/go1.25), where `synctest.Test` became generally available. The test layers, fuzz targets and fake-clock tests.
- [2] The Go Project, "The Go race detector" (https://go.dev/doc/articles/race_detector) and "Code coverage for Go integration tests" (https://go.dev/blog/integration-test-coverage). `-race`, and the `-test.gocoverdir` merge of unit and integration coverage with `go tool covdata`.
- [3] testcontainers-go v0.44.0: https://pkg.go.dev/github.com/testcontainers/testcontainers-go@v0.44.0 (release: https://github.com/testcontainers/testcontainers-go/releases/tag/v0.44.0). Disposable Postgres and Kafka containers for the integration tests. With `go-cmp` v0.7.0 (https://pkg.go.dev/github.com/google/go-cmp@v0.7.0/cmp) and `goleak` v1.3.0 (https://pkg.go.dev/go.uber.org/goleak@v1.3.0).
- [4] golangci-lint v2.14.0: release https://github.com/golangci/golangci-lint/releases/tag/v2.14.0 and "Local Installation" https://golangci-lint.run/docs/welcome/install/local/. The documentation recommends binary installation and warns that `go install`, `go get`, the tools pattern and `tool` directives are not guaranteed to work, which is the reason for not tracking it in `go.mod`.
- [5] The Go Project, "Go Vulnerability Management" (https://go.dev/doc/security/vuln/) and govulncheck v1.8.0 (https://pkg.go.dev/golang.org/x/vuln@v1.8.0/cmd/govulncheck, release https://github.com/golang/vuln/releases/tag/v1.8.0). The vulnerability gate run with `go run ...@version`.
- [6] Ham Vocke, "The Practical Test Pyramid", martinfowler.com, 26 February 2018 (https://martinfowler.com/articles/practical-test-pyramid.html), and Martin Fowler, "Contract Test" (https://martinfowler.com/bliki/ContractTest.html). The layer mix, and fixture-based contract tests between independently built services.
- [7] Docker Docs, "Build checks" (https://docs.docker.com/build/checks/). `docker build --check`.
- [8] Betsy Beyer, Chris Jones, Jennifer Petoff and Niall Richard Murphy (eds.), *Site Reliability Engineering*, O'Reilly Media, 2016, ISBN 978-1-4919-2912-4, ch. 17, "Testing for Reliability". https://sre.google/sre-book/testing-reliability/. Configuration and stress tests as part of a testing strategy.
