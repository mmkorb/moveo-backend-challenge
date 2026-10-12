# ADR 0001: Versions and upgrade policy

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** both

## Context

- Go, Apache Kafka and PostgreSQL have no "LTS" label. Each publishes a support window instead:
  - **Go** supports the two latest major releases; a major is supported until two newer ones ship.
  - **Apache Kafka** supports the last three feature releases (`4.3`, `4.2` and `4.1` today).
  - **PostgreSQL** supports each major for five years.
- Libraries ship often, and a fresh release is more likely to be yanked or quickly patched than one that has been in use for a couple of weeks.
- The repo has two independent modules and two images ([0003](0003-system-decomposition-and-layout.md)). Reproducible builds need every version pinned: toolchain, modules and images.
- We fixed PostgreSQL at **18**.
- The rest of the ADR set names components without versions and links here. **This is the only ADR with exact versions.**

## Decision

### Policy

1. **Language, runtime and infrastructure:** use the newest release line still under upstream support, at its latest patch. Patch releases of these (for example a Go point release or a Postgres minor) are taken immediately, because they carry security fixes.
2. **Libraries and tools:** use the latest stable release, but **skip any release younger than 14 days**; take the newest one that is at least 14 days old. No pre-releases.
3. **Pin exactly:**
   - `go.mod` has `go 1.27.0` and `toolchain go1.27.2`; `go.sum` is committed; builds use `-mod=readonly`.
   - Image tags carry minor/patch (`postgres:18.6`, not `postgres:18` or `latest`).
   - Base images carry exact tags; digest pinning waits for the first published release image ([0002](0002-security-baseline-and-threat-model.md), "Deliberately not done").
4. **Upgrade cadence:** Dependencies are reviewed monthly, with updates grouped. Security fixes are taken as soon as govulncheck or the image scan flags them. Each upgrade goes through the full local quality gate ([0014](0014-testing-strategy-and-quality-tooling.md)).
5. **Re-verify before writing:** every version below is checked again (pkg.go.dev, GitHub releases, Docker Hub) when it is first written into `go.mod`, a Dockerfile or `compose.yaml`.
6. **Minimal dependencies:** prefer the stdlib. A new direct dependency needs a reason in an ADR.

### Versions (checked 2026-10-11)

| Component | Pinned version | Released | Why this one |
|---|---|---|---|
| Go | `go 1.27.0`, `toolchain go1.27.2`; image `golang:1.27.2` | 1.27.0: 2026-08-19; 1.27.2: 2026-10-08 | Latest major; 1.26 is the only other supported line. Patch taken immediately (rule 1). |
| PostgreSQL | `postgres:18.6` | 18.0: 2025-09-25 | Chosen major; newest supported major (EOL 2030-11-14). PG 19 is still in beta. |
| Apache Kafka | `apache/kafka:4.3.1` (KRaft) | 2026-06-25 | Newest supported feature line (4.3 / 4.2 / 4.1 supported). |
| Distroless runtime | `gcr.io/distroless/static-debian13:nonroot` (tag only; a digest pin is deferred) | rolling | Current Debian base; it has no version tags, so the digest would be the only exact pin. |
| Dockerfile frontend | `# syntax=docker/dockerfile:1` | — | Latest stable 1.x; `check=error=true` is still deterministic enough for automated builds. |
| `github.com/twmb/franz-go` (`kgo`) | v1.22.1 | 2026-09-27 | Latest; 14 days old. |
| `github.com/twmb/franz-go/pkg/kadm` | v1.19.0 | 2026-09-18 | Latest. |
| `github.com/twmb/franz-go/plugin/kprom` | v1.5.0 | 2026-07-01 | Latest. |
| `github.com/twmb/franz-go/pkg/kmsg` | v1.14.0 | 2026-09-15 | Direct requirement of both modules, the version `franz-go` v1.22.1 resolves to. The consumer sends the explicit static `LeaveGroup` with it (`leaveStatic`); the producer uses it in the publisher. |
| `github.com/prometheus/client_model` | v0.6.2 | 2025-04-11 | Direct requirement of the consumer (reading metric values in tests); indirect in the producer. |
| `github.com/moby/moby/api` | v1.55.0 | 2026-06-18 | Direct requirement of both modules, used only by the integration tests to set container resource limits. |
| `github.com/jackc/pgx/v5` | v5.11.0 | 2026-09-07 | Latest. |
| `github.com/pressly/goose/v3` | v3.28.0 | 2026-09-02 | v3.29.0 came out 2026-10-11, so it is skipped (rule 2). |
| `github.com/prometheus/client_golang` | v1.24.1 | 2026-07-23 | v1.25.0 came out 2026-10-07, so it is skipped (rule 2). |
| `golang.org/x/sync` (`errgroup`) | v0.24.0 | 2026-09-23 | Latest; 18 days old. |
| `github.com/testcontainers/testcontainers-go` (+ `modules/postgres`) | v0.44.0 | 2026-08-07 | Latest. Kafka uses the generic `testcontainers.Run` with `apache/kafka:4.3.1`: `modules/kafka` is Confluent-only in v0.44. |
| `github.com/google/go-cmp` | v0.7.0 | 2025-01-14 | Latest. |
| `go.uber.org/goleak` | v1.3.0 | 2023-10-24 | Latest (stable, rarely released). |
| golangci-lint (binary) | v2.14.0 | 2026-09-24 | Latest; 17 days old. Not in any `go.mod` ([0014](0014-testing-strategy-and-quality-tooling.md)). |
| govulncheck (`golang.org/x/vuln`) | v1.8.0 | 2026-09-08 | v1.9.0 came out 2026-10-11, so it is skipped (rule 2). |
| k6 (load generator) | `grafana/k6:2.3.0` | 2026-09-21 | Latest; 20 days old. Used only by the load-test overlay ([0016](0016-load-and-crash-testing.md)). |
| curl (load-test probe) | `curlimages/curl:8.22.0` | 2026-09-02 | Latest. Used by `loadtest/run.sh` to read `/metrics` and `/readyz` inside the Compose network ([0016](0016-load-and-crash-testing.md)). |
| Docker Engine / Compose (local tooling) | ≥ 29 / ≥ v5 | — | Compose Specification (no `version:`), `HEALTHCHECK --start-interval`, `depends_on` conditions. |

**Dependencies we deliberately do not use:**
- `github.com/google/uuid`: Go 1.27's stdlib `uuid` package generates v7 IDs for the `event-id` header ([0006](0006-event-contract-and-json-serialization.md)).
- `encoding/json/v2` as a separate experiment: since Go 1.27, `encoding/json` runs on the v2 engine and `encoding/json/v2` is stable, so the stdlib codec is used directly.
- `go-chi/chi`, `promauto` and `testify` are not used ([0005](0005-ingestion-http-api.md), [0013](0013-observability.md), [0014](0014-testing-strategy-and-quality-tooling.md)).

### Who uses what

| Dependency | producer | consumer | test only | Tooling |
|---|---|---|---|---|
| `franz-go` (`kgo`) | ✓ | ✓ | | |
| `franz-go/pkg/kadm` | | ✓ (group lag; integration-test topic setup) | | |
| `franz-go/plugin/kprom` | ✓ | ✓ | | |
| `pgx/v5` | | ✓ | | |
| `goose/v3` | | ✓ (`migrate` subcommand) | | |
| `prometheus/client_golang` | ✓ | ✓ | | |
| `golang.org/x/sync` | ✓ | ✓ | | |
| `testcontainers-go` (+ modules) | | | ✓ both (integration tag) | |
| `go-cmp`, `goleak` | | | ✓ both | |
| golangci-lint, govulncheck | | | | ✓ (binary / `go run …@version`) |

Direct runtime dependencies: **producer 4** (`kgo`, `kprom`, `client_golang`, `x/sync`) and **consumer 7** (adds `kadm`, `pgx`, `goose`). Everything else is the stdlib: `net/http`, `log/slog`, `encoding/json`, `uuid`, `os/signal`, `testing/synctest`.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| Previous supported lines (Go 1.26, Kafka 4.2, Postgres 17) | Rejected | Shorter remaining support, for no compatibility gain in a greenfield repo. |
| Floating tags (`golang:1.27`, `postgres:18`, `latest`) | Rejected | Builds change silently. Tags carry the patch; digests are deferred. |
| Always the newest library release | Rejected | Day-zero releases are more often yanked or quickly patched. The 14-day buffer costs little. |
| Range-based or unpinned tool versions (`@latest`) | Rejected | Non-reproducible builds; a new linter release can break the build without a code change. |
| Vendoring (`go mod vendor`) | Rejected | `go.sum` + the module proxy checksum database already give integrity; vendoring bloats diffs. |

## Consequences

**Positive**
- Reproducible builds and images; upgrades are explicit, reviewable PRs.
- A long support runway for every runtime component.
- A small dependency surface: fewer CVEs and less upgrade work ([0002](0002-security-baseline-and-threat-model.md)).

**Negative / trade-offs**
- The 14-day rule can delay a wanted feature or fix. Mitigation: a security fix overrides the rule, with the reason in the PR.
- Monthly upgrades need attention. Mitigation: grouped updates, and the quality gate (tests, govulncheck, image scan) catches most regressions.
- The table goes stale. Mitigation: rule 5 (re-verify when writing), and this ADR is superseded, not edited, when the policy changes. Routine version bumps update the pins in code, not this ADR.

## Implementation rules

- Exact versions MUST appear only in this ADR and in code/config (`go.mod`, Dockerfiles, `compose.yaml`, the Makefile). Other ADRs MUST link here.
- `go.mod` MUST declare `go 1.27.0` and `toolchain go1.27.2` (or the current pinned patch); `go.sum` MUST be committed.
- Image references MUST carry a minor/patch tag. Digest pinning is deferred (see above).
- A library release younger than 14 days MUST NOT be adopted, except for a security fix noted in the PR.
- Every direct dependency MUST appear in the "Who uses what" table. A new one needs an ADR (or an update to the relevant ADR) explaining why the stdlib is not enough.
- Pre-release versions (`-rc`, `-beta`, Postgres betas) MUST NOT be used.

## Related

- [0003](0003-system-decomposition-and-layout.md) — two modules, each with its own `go.mod`.
- [0006](0006-event-contract-and-json-serialization.md) — stdlib `uuid` and `encoding/json` (v2 engine).
- [0007](0007-kafka-client-and-topic-topology.md), [0010](0010-postgresql-access-and-migrations.md), [0013](0013-observability.md) — the libraries pinned here.
- [0014](0014-testing-strategy-and-quality-tooling.md) — tooling installation (binary golangci-lint, `go run` govulncheck).
- [0015](0015-containerization-and-local-deployment.md) — image tags used in Dockerfiles and Compose.
- [0002](0002-security-baseline-and-threat-model.md) — supply-chain controls built on this policy.
- [0016](0016-load-and-crash-testing.md) — load-test images (`grafana/k6`, `curlimages/curl`, `golang` for the report).

## References

Versions are quoted as pinned in `go.mod` (both services), `compose.yaml`, `loadtest/compose.loadtest.yaml`, the Dockerfiles, and the Makefile. Each entry links the release or tag page of that exact version, or the versioned documentation page.

**Policy**

- [1] The Go Project, "Release History: Release Policy". https://go.dev/doc/devel/release. Each major release is supported until two newer majors exist; the basis of the Go support window.
- [2] The Go Project, "Go Toolchains" (https://go.dev/doc/toolchain) and "Go Modules Reference" (https://go.dev/ref/mod). The `go` and `toolchain` lines of `go.mod`, `go.sum`, `-mod=readonly` and the checksum database behind the pinning rules.
- [3] Apache Kafka, "Downloads: supported releases". https://kafka.apache.org/community/downloads/. The three supported feature lines (4.3, 4.2, 4.1) and the `apache/kafka:4.3.1` image released 25 June 2026.
- [4] The PostgreSQL Global Development Group, "Versioning Policy". https://www.postgresql.org/support/versioning/. A major version is supported for five years.

**Language and runtime**

- [5] The Go Project, "Go 1.27 Release Notes". https://go.dev/doc/go1.27. The new stdlib `uuid` package (UUIDv7 for `event-id`), `encoding/json/v2`, and `encoding/json` running on the v2 engine (opt-out `GOEXPERIMENT=nojsonv2`), as used in the "Dependencies we deliberately do not use" list.
- [6] The Go Project, release notes of Go 1.21 (https://go.dev/doc/go1.21), Go 1.22 (https://go.dev/doc/go1.22) and Go 1.25 (https://go.dev/doc/go1.25). The language and library features the code relies on: `log/slog`, `context.AfterFunc`, `WithoutCancel` and `WithTimeoutCause` (1.21); per-iteration loop variables, `math/rand/v2` and `ServeMux` method and wildcard patterns (1.22); `testing/synctest` (1.25, stable).
- [7] The Go Project, standard library at the pinned patch: `log/slog` (https://pkg.go.dev/log/slog@go1.27.2) and `net/http` (https://pkg.go.dev/net/http@go1.27.2). Versioned reference for the stdlib-only decisions.

**Kafka and PostgreSQL (versions 4.3.1 and 18.6)**

- [8] Apache Kafka 4.3 documentation, "Upgrading" (https://kafka.apache.org/43/getting-started/upgrade/) and the 4.3.1 release notes (https://downloads.apache.org/kafka/4.3.1/RELEASE_NOTES.html). 4.3 is KRaft only (ZooKeeper mode removed); 4.3.1 notable changes.
- [9] Docker Hub, `apache/kafka` official image. https://hub.docker.com/r/apache/kafka. Tag `4.3.1` of the image used in `compose.yaml`.
- [10] PostgreSQL 18 documentation, "Release 18" (https://www.postgresql.org/docs/18/release-18.html) and "Release 18.6" (https://www.postgresql.org/docs/release/18.6/). The major line and the minor release pinned as `postgres:18.6`.
- [11] Docker Hub, `postgres` official image. https://hub.docker.com/_/postgres. The `18` tag line and its version-specific `PGDATA`.

**Pinned libraries (`go.mod`)**

- [12] franz-go `kgo` v1.22.1: https://pkg.go.dev/github.com/twmb/franz-go@v1.22.1/pkg/kgo and https://github.com/twmb/franz-go/releases/tag/v1.22.1. With `kadm` v1.19.0 (https://pkg.go.dev/github.com/twmb/franz-go/pkg/kadm@v1.19.0), `kprom` v1.5.0 (https://pkg.go.dev/github.com/twmb/franz-go/plugin/kprom@v1.5.0) and `kmsg` v1.14.0 (https://pkg.go.dev/github.com/twmb/franz-go/pkg/kmsg@v1.14.0).
- [13] pgx v5.11.0 with `pgxpool`: https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0, https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0/pgxpool and https://github.com/jackc/pgx/releases/tag/v5.11.0.
- [14] goose v3.28.0: https://pkg.go.dev/github.com/pressly/goose/v3@v3.28.0 and https://github.com/pressly/goose/releases/tag/v3.28.0.
- [15] Prometheus `client_golang` v1.24.1: https://pkg.go.dev/github.com/prometheus/client_golang@v1.24.1/prometheus and https://github.com/prometheus/client_golang/releases/tag/v1.24.1.
- [16] `golang.org/x/sync` v0.24.0, package `errgroup`. https://pkg.go.dev/golang.org/x/sync@v0.24.0/errgroup.
- [17] testcontainers-go v0.44.0: https://pkg.go.dev/github.com/testcontainers/testcontainers-go@v0.44.0 and https://github.com/testcontainers/testcontainers-go/releases/tag/v0.44.0. `go-cmp` v0.7.0 (https://pkg.go.dev/github.com/google/go-cmp@v0.7.0/cmp) and `goleak` v1.3.0 (https://pkg.go.dev/go.uber.org/goleak@v1.3.0).

**Tools and images**

- [18] golangci-lint v2.14.0: https://github.com/golangci/golangci-lint/releases/tag/v2.14.0, installed as a binary per https://golangci-lint.run/docs/welcome/install/local/. govulncheck v1.8.0: https://pkg.go.dev/golang.org/x/vuln@v1.8.0/cmd/govulncheck and https://github.com/golang/vuln/releases/tag/v1.8.0.
- [19] Grafana k6 v2.3.0: https://github.com/grafana/k6/releases/tag/v2.3.0 and the documentation for that line, https://grafana.com/docs/k6/v2.3.x/. curl 8.22.0: https://curl.se/ch/8.22.0.html (image `curlimages/curl:8.22.0`, https://hub.docker.com/r/curlimages/curl).
- [20] Docker Docs: Docker Engine 29 release notes (https://docs.docker.com/engine/release-notes/29/), Compose release notes (https://docs.docker.com/compose/releases/release-notes/), Dockerfile frontend release notes (https://docs.docker.com/build/buildkit/dockerfile-release-notes/) and the Dockerfile reference (https://docs.docker.com/reference/dockerfile/). The local tooling floor (Engine and Compose) and `# syntax=docker/dockerfile:1`.
- [21] Docker Hub, `golang` official image (https://hub.docker.com/_/golang, tag `1.27.2`) and the distroless project (https://github.com/GoogleContainerTools/distroless, image `gcr.io/distroless/static-debian13:nonroot`).
- [22] Compose Specification. https://compose-spec.io/. Why `compose.yaml` carries no `version:` key.
