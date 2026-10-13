# ADR 0015: Containerization: Dockerfiles, Compose, Kafka and Postgres containers

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** both

> **TL;DR** — One distroless, non-root, static-binary image per service and a single `compose.yaml` that starts Kafka (KRaft), Postgres, a topic-init job, a migrate job and both services in dependency order. Hardening and file secrets apply to the app containers. Trade-off: no shell in the app images, and Kafka runs with replication factor 1 (local only).

## Context

- The challenge asks for "a Docker image for both the producer and consumer" and deployment instructions. Our summary of the challenge (§2, §9) settles on **two images, one container per service**, with Compose also starting Kafka and PostgreSQL.
- The repo holds two independent Go modules (`services/producer`, `services/consumer`; [0003](0003-system-decomposition-and-layout.md)). A dev-only `go.work` sits at the root.
- The consumer needs its schema before it starts ([0010](0010-postgresql-access-and-migrations.md)). Topics must exist before either service starts, because broker auto-create is off ([0007](0007-kafka-client-and-topic-topology.md)).
- Graceful shutdown needs up to 20 s ([0012](0012-lifecycle-and-graceful-shutdown.md)). Docker's default stop timeout is 10 s, then `SIGKILL`.
- The runtime image has no shell or curl, so healthchecks must be run by the binary itself ([0013](0013-observability.md)).
- The development machine is Windows with Git Bash and Docker Desktop. CRLF line endings and MSYS path conversion break scripts and container paths.
- This ADR follows the Docker build and runtime best practices (see References). Image tags below must match [0001](0001-versions-and-upgrade-policy.md). Hardening rationale lives in [0002](0002-security-baseline-and-threat-model.md).

## Decision

### Dockerfile per service (`services/<svc>/Dockerfile`)

The build context is the service directory. The consumer is shown; the producer is identical except for `./cmd/producer`, `EXPOSE 8080` and no `CMD`.

```dockerfile
# syntax=docker/dockerfile:1
# check=error=true
ARG GO_VERSION=1.27.2
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build
WORKDIR /src
ENV GOWORK=off GOFLAGS=-mod=readonly
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/consumer

FROM build AS test
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    go test ./...

FROM gcr.io/distroless/static-debian13:nonroot AS final
COPY --from=build /out/app /app
USER nonroot:nonroot
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --start-interval=2s --retries=3 \
    CMD ["/app", "healthcheck"]
ENTRYPOINT ["/app"]
CMD ["run"]
```

- **Parser directives first:** `docker build --check .` must report nothing; `check=error=true` turns findings into failures.
- **Cache order:** `go.mod`/`go.sum` are bind-mounted for `go mod download`, then the source is bind-mounted for the build. Module and build caches are cache mounts, so they never end up in a layer.
- **`GOWORK=off`:** images build each module alone, never through `go.work`. **`-mod=readonly`:** the build fails if `go.mod` would change.
- **Static binary:** `CGO_ENABLED=0`, `-trimpath` (no host paths), `-ldflags="-s -w"`. Cross-compiles from `$BUILDPLATFORM`, so `linux/amd64,linux/arm64` builds cost no emulation.
- **`test` stage:** `docker build --target test .` runs the unit tests in the same toolchain. Integration tests (`make test-integration`) and `-race` (Linux container) run outside this stage ([0014](0014-testing-strategy-and-quality-tooling.md)).
- **Runtime:** `distroless/static-debian13:nonroot` gives CA certificates, tzdata, `/etc/passwd` and uid/gid 65532, with no shell and no package manager. The image is about 20 MB.
- **Exec-form `ENTRYPOINT`:** the Go binary is PID 1 and receives `SIGTERM` directly. It spawns no children, so `init: true` is not needed.
- **Subcommands:** consumer `run` (default `CMD`) | `migrate` | `healthcheck`; producer serves by default and also has `healthcheck`. `HEALTHCHECK` uses the binary because distroless has no curl.
- **`.dockerignore`** in each service directory:

```gitignore
.git
.env
*.env
secrets/
Dockerfile*
.dockerignore
bin/
tmp/
coverage.out
```

### Topic init script (`deploy/kafka/create-topics.sh`)

```bash
#!/usr/bin/env bash
set -euo pipefail
bs="${BOOTSTRAP_SERVER:-kafka:9092}"
topics=/opt/kafka/bin/kafka-topics.sh
"$topics" --bootstrap-server "$bs" --create --if-not-exists \
  --topic incoming.user_activity --partitions 6 --replication-factor 1 \
  --config retention.ms=604800000
"$topics" --bootstrap-server "$bs" --create --if-not-exists \
  --topic incoming.user_activity.dlq --partitions 3 --replication-factor 1 \
  --config retention.ms=1209600000
```

The retentions are 7 days for the main topic and 14 days for the DLQ ([0007](0007-kafka-client-and-topic-topology.md)). `--if-not-exists` makes the script safe to re-run.

### `compose.yaml` (repo root)

Condensed excerpt; `compose.yaml` is the source of truth. Compose substitutes only `LOG_LEVEL` and `CONSUMER_WORKERS` from `.env`; every other non-secret value is set in the service's `environment:` block (`.env.example` lists them all with their defaults).

```yaml
name: moveo-analytics

x-logging: &logging
  driver: local
  options:
    max-size: "10m"
    max-file: "3"

x-app: &app
  read_only: true
  cap_drop: [ALL]
  security_opt: [no-new-privileges:true]
  stop_grace_period: 30s
  restart: unless-stopped
  logging: *logging
  environment: &app-env
    LOG_LEVEL: ${LOG_LEVEL:-info}
    KAFKA_BROKERS: kafka:9092
    GOMEMLIMIT: 230MiB
  deploy:
    resources:
      limits: { cpus: "1", memory: 256M, pids: 200 }

services:
  kafka:
    image: apache/kafka:4.3.1
    environment:
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_LISTENERS: INTERNAL://:9092,EXTERNAL://:29092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:9092,EXTERNAL://localhost:29092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1        # local single broker only
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_LOG_DIRS: /var/lib/kafka/data
      KAFKA_HEAP_OPTS: -Xms512m -Xmx512m
    security_opt: [no-new-privileges:true]
    ports:
      - "127.0.0.1:29092:29092"                        # host tools only
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      start_period: 30s
      start_interval: 2s
      retries: 6
    deploy:
      resources:
        limits: { cpus: "2", memory: 1G, pids: 1024 }
    logging: *logging
    restart: unless-stopped

  kafka-init:
    image: apache/kafka:4.3.1
    entrypoint: ["/bin/bash", "/create-topics.sh"]
    environment:
      BOOTSTRAP_SERVER: kafka:9092
    volumes:
      - ./deploy/kafka/create-topics.sh:/create-topics.sh:ro
    security_opt: [no-new-privileges:true]
    depends_on:
      kafka:
        condition: service_healthy
    restart: "no"

  postgres:
    image: postgres:18.6
    environment:
      POSTGRES_DB: analytics
      POSTGRES_USER: analytics_owner
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres_owner_password
    secrets: [postgres_owner_password, analytics_app_password]
    volumes:
      - pg-data:/var/lib/postgresql                    # PG18: PGDATA=/var/lib/postgresql/18/docker
      - ./deploy/postgres/init.sql:/docker-entrypoint-initdb.d/10-init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}"]
      interval: 10s
      timeout: 5s
      start_period: 30s
      start_interval: 1s
      retries: 5
    shm_size: 128mb
    security_opt: [no-new-privileges:true]
    deploy:
      resources:
        limits: { cpus: "1", memory: 512M, pids: 256 }
    logging: *logging
    restart: unless-stopped

  consumer-migrate:
    build: { context: ./services/consumer, target: final }
    image: moveo-analytics/consumer:dev
    command: ["migrate"]
    environment:
      DATABASE_HOST: postgres
      DATABASE_NAME: analytics
      DATABASE_USER: analytics_owner
      DATABASE_PASSWORD_FILE: /run/secrets/postgres_owner_password
      DATABASE_SSLMODE: disable
    secrets: [postgres_owner_password]
    healthcheck:
      disable: true                                    # one-shot job; image HEALTHCHECK does not apply
    depends_on:
      postgres:
        condition: service_healthy
    read_only: true
    cap_drop: [ALL]
    security_opt: [no-new-privileges:true]
    deploy:
      resources:
        limits: { cpus: "0.5", memory: 128M, pids: 100 }
    logging: *logging
    restart: "no"

  producer:
    <<: *app
    build: { context: ./services/producer, target: final }
    image: moveo-analytics/producer:dev
    environment:
      <<: *app-env
      HTTP_ADDR: ":8080"
    ports:
      - "127.0.0.1:8080:8080"
    depends_on:
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully

  consumer:                                            # no container_name, no ports: --scale works
    <<: *app
    build: { context: ./services/consumer, target: final }
    image: moveo-analytics/consumer:dev
    environment:
      <<: *app-env
      HTTP_ADDR: ":8081"                               # internal health/metrics only
      CONSUMER_WORKERS: ${CONSUMER_WORKERS:-3}
      DATABASE_HOST: postgres
      DATABASE_NAME: analytics
      DATABASE_USER: analytics_app
      DATABASE_PASSWORD_FILE: /run/secrets/analytics_app_password
      DATABASE_SSLMODE: disable
    secrets: [analytics_app_password]
    depends_on:
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
      postgres:
        condition: service_healthy
      consumer-migrate:
        condition: service_completed_successfully

volumes:
  kafka-data:
  pg-data:

secrets:
  postgres_owner_password:
    file: ./deploy/secrets/postgres_owner_password.txt
  analytics_app_password:
    file: ./deploy/secrets/analytics_app_password.txt
```

Checked on 2026-10-11:
- `docker compose config --quiet` accepts this excerpt;
- `docker build --check` passes on the Dockerfile above;
- the `kafka`, `kafka-init` and `postgres` services start healthy, `kafka-init` creates both topics with the stated partitions and `retention.ms`, and `init.sql` creates `analytics_app`.

Notes on the services:

- **File name.** `compose.yaml` is the canonical Compose Specification name, with no `version:` key. Our summary says `docker-compose.yml`; Compose reads both, and the README says so.
- **kafka.** `apache/kafka` (official Apache image), a single node in combined KRaft mode with a fixed `CLUSTER_ID`. Services use `INTERNAL` `kafka:9092`. Host tools use `EXTERNAL` `localhost:29092`, published on loopback only. Replication factor 1 and min ISR 1 are **local-only** settings. The heap is capped below the container memory limit.
- **kafka-init.** The same image runs `create-topics.sh` once, overriding the entrypoint so no broker starts. Its successful exit gates both services.
- **postgres.** Postgres 18 with the named volume at `/var/lib/postgresql` (PG18 moved `PGDATA` under it; mounting `…/data` is wrong). `init.sql` runs only on an empty volume. It creates the DML-only `analytics_app` role, reading its password from `/run/secrets/analytics_app_password` with a psql `\set` backtick command, so no password lives in the SQL file. `$$` stops Compose from interpolating `POSTGRES_USER` in the healthcheck. The port is **not published**; inspect with `docker compose exec postgres psql -U analytics_owner -d analytics`. Publishing `127.0.0.1:5432:5432` in a local override file is acceptable.
- **consumer-migrate.** The consumer image with `command: ["migrate"]`, owner credentials, `restart: "no"`. Its zero exit gates the consumer.
- **producer / consumer.** Both get `stop_grace_period: 30s`, which covers the 20 s shutdown budget. Their healthcheck is the image `HEALTHCHECK` (`/app healthcheck` → `/readyz`). Only the producer publishes a port, on `127.0.0.1`. `consumer-migrate` and `consumer` share one build and image tag; the second build is a cache hit.
- **Hardening** (`read_only`, `cap_drop: [ALL]`, `no-new-privileges`, CPU/memory/pids limits) applies to our containers. Kafka and Postgres get limits, logging and loopback-only ports, but no `cap_drop`/`read_only`, because their entrypoints need to `chown` and switch users. `GOMEMLIMIT` is about 90 % of the memory limit.
- **Secrets.** Compose `secrets:` mounted at `/run/secrets/<name>` and read through `*_FILE` variables. The real files under `deploy/secrets/` are gitignored; `*.example` files are committed ([0002](0002-security-baseline-and-threat-model.md)).
- **Logging.** The `local` driver with rotation (10 MB × 3) on every service.
- **Network.** One default project network; services resolve each other by service name.
- **Load-test overlay.** Load and crash runs reuse this file unchanged and add `loadtest/compose.loadtest.yaml` on top, in a separate Compose project (`moveo-loadtest`) with its own volumes. The overlay adds the k6 service and sets tighter, explicit CPU/memory/pids limits on every service, including Kafka and Postgres ([0016](0016-load-and-crash-testing.md)).

### Commands (README)

```bash
cp .env.example .env
for f in deploy/secrets/*.example; do cp "$f" "${f%.example}"; done
docker compose up -d --build --wait          # one-command start; blocks until healthy
docker compose logs -f consumer
docker compose up -d --scale consumer=3      # 3 containers × CONSUMER_WORKERS members, one group
docker compose down                          # stop, keep data
docker compose down -v                       # WARNING: deletes kafka-data and pg-data
```

With 6 partitions, at most 6 group members (containers × `CONSUMER_WORKERS`) receive work. Extra members stay idle ([0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).

### Windows notes

- Git Bash rewrites absolute container paths. Use `MSYS_NO_PATHCONV=1 docker compose exec consumer /app healthcheck` (or run it from PowerShell).
- `.gitattributes` has `*.sh text eol=lf` and `Dockerfile text eol=lf`. A CRLF shebang (`/bin/bash\r`) makes `kafka-init` fail with a misleading "not found".
- On Linux hosts, file secrets keep their host permissions. They must be readable by uid 65532 (apps) and by the Postgres entrypoint.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| Bitnami Kafka image | Rejected | Bitnami moved its free catalog to a legacy/limited model; `apache/kafka` is the upstream official image. |
| Kafka with ZooKeeper | Rejected | ZooKeeper mode was removed in Kafka 4; KRaft is the only mode. |
| `apache/kafka-native` (GraalVM) | Rejected | Starts faster but ships no CLI scripts, which `kafka-init` and the healthcheck need. |
| `scratch` runtime | Rejected | No CA certificates, tzdata or `nonroot` user; saves only about 2 MiB (the size of `distroless/static-debian13`). |
| Alpine runtime | Rejected | A shell and package manager the app doesn't need, and more CVE surface. |
| Migrations at app startup | Rejected | Every replica races to migrate and needs DDL rights. A one-shot job keeps the runtime role DML-only ([0010](0010-postgresql-access-and-migrations.md)). |
| One shared image with both binaries | Rejected | Decision: two services, two images. Each image holds only its own code and dependencies. |
| Testcontainers-only local runs (no Compose) | Rejected | The deliverable is a Compose stack a reviewer can start with one command. Testcontainers stays for integration tests ([0014](0014-testing-strategy-and-quality-tooling.md)). |
| Shell-based healthchecks (`curl`, `wget`) | Rejected | Impossible on distroless. The binary probes itself. |
| `docker-compose.yml` file name | Accepted as alias | Compose reads it, but `compose.yaml` is the canonical name. |

## Consequences

**Positive**
- `docker compose up -d --build --wait` starts the whole pipeline in dependency order and returns only when it is ready.
- Small, non-root, shell-less images with reproducible, cache-friendly builds and free multi-arch.
- Topic creation, schema migration and runtime are separate steps with separate privileges.
- Consumers scale with `--scale` without port or name clashes.

**Negative / trade-offs**
- No shell in the app containers for debugging. Mitigation: logs, `/metrics`, `docker debug`, or temporarily building the `build` stage.
- Single-broker Kafka with RF 1 has no durability guarantees. Mitigation: documented as local-only; production settings are in [0007](0007-kafka-client-and-topic-topology.md) and [0002](0002-security-baseline-and-threat-model.md).
- `init.sql` runs only on a fresh volume. Mitigation: `docker compose down -v` to reset; role changes after that go through a migration.
- The contract fixtures are outside the service build context, so the image `test` stage skips the contract test. Mitigation: `make test` runs it on the host ([0014](0014-testing-strategy-and-quality-tooling.md)).

## Implementation rules

- Every Dockerfile MUST start with `# syntax=docker/dockerfile:1` and `# check=error=true`, and `docker build --check` MUST pass.
- The final stage MUST be `distroless/static-debian13:nonroot` with `USER nonroot:nonroot` and exec-form `ENTRYPOINT ["/app"]`.
- Builds MUST use `CGO_ENABLED=0`, `-trimpath`, `GOWORK=off` and cache mounts. Secrets MUST NOT appear in `ARG`, `ENV`, `COPY` or labels.
- Each service directory MUST have a `.dockerignore` excluding `.git`, `.env` and secrets.
- `compose.yaml` MUST NOT have a `version:` key. It MUST gate startup with `service_healthy` / `service_completed_successfully`.
- Only the producer port (and Kafka's `EXTERNAL` listener) MAY be published, and only on `127.0.0.1`. The consumer MUST NOT have `ports` or `container_name`.
- App services MUST set `read_only`, `cap_drop: [ALL]`, `no-new-privileges`, resource limits, `stop_grace_period: 30s` and `local` logging.
- Passwords MUST reach containers only through Compose `secrets` and `*_FILE` variables.
- Image tags MUST match [0001](0001-versions-and-upgrade-policy.md); base images are pinned by tag only; digest pinning is deliberately not done yet ([0002](0002-security-baseline-and-threat-model.md), "Deliberately not done").
- `*.sh` and `Dockerfile` MUST be committed with LF line endings.

## Related

- [0003](0003-system-decomposition-and-layout.md) — repo layout and per-service build contexts.
- [0007](0007-kafka-client-and-topic-topology.md) — topics, partitions and retention created by `kafka-init`.
- [0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — scaling limits.
- [0010](0010-postgresql-access-and-migrations.md) — `migrate` subcommand and DB roles.
- [0012](0012-lifecycle-and-graceful-shutdown.md) — the 20 s budget behind `stop_grace_period: 30s`.
- [0013](0013-observability.md) — `/readyz` and the `healthcheck` subcommand.
- [0014](0014-testing-strategy-and-quality-tooling.md) — `test` stage, build checks, end-to-end smoke.
- [0001](0001-versions-and-upgrade-policy.md) — image tags and digest pinning.
- [0002](0002-security-baseline-and-threat-model.md) — hardening and secrets rationale.
- [0016](0016-load-and-crash-testing.md) — the load-test overlay and its resource budget.

## References

- [1] Docker Docs, Dockerfile reference (https://docs.docker.com/reference/dockerfile/), "Building best practices" (https://docs.docker.com/build/building/best-practices/), "Multi-stage builds" (https://docs.docker.com/build/building/multi-stage/), "Optimize cache usage in builds" (https://docs.docker.com/build/cache/optimize/) and "Build checks" (https://docs.docker.com/build/checks/). The `# syntax` and `# check` directives, `HEALTHCHECK --start-interval`, exec-form `ENTRYPOINT`, bind and cache mounts, the `test` stage, and `docker build --check`. Frontend release notes: https://docs.docker.com/build/buildkit/dockerfile-release-notes/.
- [2] Docker Docs, Compose file reference (https://docs.docker.com/reference/compose-file/), "Version and name top-level elements" (https://docs.docker.com/reference/compose-file/version-and-name/), `services` (https://docs.docker.com/reference/compose-file/services/: `depends_on` conditions `service_healthy` and `service_completed_successfully`, `read_only`, `cap_drop`, `security_opt`, `stop_grace_period`), "Secrets" (https://docs.docker.com/reference/compose-file/secrets/) and "Control startup order" (https://docs.docker.com/compose/how-tos/startup-order/). With the Compose Specification (https://compose-spec.io/). Engine 29 and Compose release notes: https://docs.docker.com/engine/release-notes/29/ and https://docs.docker.com/compose/releases/release-notes/.
- [3] Docker Docs, "Resource constraints". https://docs.docker.com/engine/containers/resource_constraints/. The CPU and memory limits applied to every service.
- [4] The distroless project, README. https://github.com/GoogleContainerTools/distroless. `gcr.io/distroless/static-debian13` with the `nonroot` tag; no shell or package manager, and the smallest distroless image at about 2 MiB.
- [5] Apache Kafka 4.3 documentation: "Docker" (https://kafka.apache.org/43/getting-started/docker/: `apache/kafka:4.3.1`, and the GraalVM native image described as experimental and for local development and testing), "KRaft" (https://kafka.apache.org/43/operations/kraft/) and "Upgrading" (https://kafka.apache.org/43/getting-started/upgrade/: ZooKeeper mode removed). The Docker Hub page of `apache/kafka-native` (https://hub.docker.com/r/apache/kafka-native) states that the command line tools are not included in that image. Background: KIP-500, https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum.
- [6] Docker Hub, `postgres` official image. https://hub.docker.com/_/postgres. From PostgreSQL 18 the image's `PGDATA` is version specific, so the volume is mounted at `/var/lib/postgresql`; initialization scripts run only on an empty data directory.
- [7] The Twelve-Factor App, V. "Build, release, run" (https://12factor.net/build-release-run) and III. "Config" (https://12factor.net/config). Strict separation of build and run, and configuration from the environment.
- [8] NIST SP 800-190, *Application Container Security Guide*, September 2017 (https://csrc.nist.gov/pubs/sp/800/190/final), and the OWASP Docker Security Cheat Sheet (https://cheatsheetseries.owasp.org/cheatsheets/Docker_Security_Cheat_Sheet.html). The hardening rationale: minimal images, non-root, dropped capabilities and read-only root filesystems.
- [9] The Go Project, "A Guide to the Go Garbage Collector". https://go.dev/doc/gc-guide. `GOMEMLIMIT` as a soft memory limit set below the container limit.
