# ADR 0002: Security baseline and threat model

- **Status:** Accepted
- **Date:** 2026-10-11
- **Scope:** both

## Context

- The deliverable is a **local / evaluation deployment**: one Compose stack on a developer machine ([0015](0015-containerization-and-local-deployment.md)). It has no authentication requirement, no TLS and no internet exposure. The design must still be secure by default and make the production gap explicit.
- The event payload carries `user_id` and a free-form `metadata` map (URLs, referrers). Our summary of the challenge (§6) warns that `metadata` may be sensitive. Aggregates keep `user_id`, which is personal data under LGPD/GDPR.
- Untrusted input enters in two places: the HTTP body (producer) and the Kafka record (consumer). The consumer must not trust the topic either; anything that can write to it can poison it.
- Security rules live in this ADR, and the other ADRs link to it instead of repeating them.
- Sources: OWASP API Security Top 10 (2023) and CWE, the Docker Engine security documentation, and the PostgreSQL roles and privileges documentation (see References).

## Decision

### Threat model (STRIDE-lite)

Assets: aggregate integrity in `activity_aggregates`, the availability of the pipeline, `user_id`/`metadata` confidentiality, and DB/Kafka credentials.

| Threat | Vector | Mitigation (local) | Production addition |
|---|---|---|---|
| **S**poofing | Any client can post events for any `user_id` | Documented assumption: no API auth (out of scope) | Gateway with authn (OAuth2/mTLS); bind `user_id` to the caller |
| **T**ampering | Poisoned or malformed Kafka record | Consumer re-validates every record; permanent errors go to the DLQ, never crash the worker ([0011](0011-failure-handling-retry-and-dlq.md)) | Kafka ACLs: only the producer principal may write the topic |
| **T**ampering | SQL injection through `user_id`/`activity_type` | Parameterized SQL only (CWE-89) | — |
| **R**epudiation | Can't tell who sent what | `request_id`, `event-id` header, `topic/partition/offset` in logs ([0013](0013-observability.md)) | Gateway access logs with caller identity |
| **I**nformation disclosure | `metadata`/`user_id` in logs, DLQ headers, error responses | Never log payloads or `user_id`; DLQ headers add the error class and original coordinates only (no error text); generic error bodies | Log pipeline with retention limits and access control |
| **I**nformation disclosure | Credentials in images, env or git | Compose secrets + `*_FILE`; `.env` and `deploy/secrets/*` gitignored | Secret manager (Vault, cloud KMS), rotation |
| **D**enial of service | Huge bodies, slowloris, request floods | 64 KiB `MaxBytesReader`, server timeouts, container CPU/memory/pids limits | Rate limiting at the gateway; autoscaling |
| **D**enial of service | DB outage makes the consumer drop data | Transient errors retried without limit, never dead-lettered; partition blocks, offsets not committed ([0011](0011-failure-handling-retry-and-dlq.md)) | Alerts on `consumer_lag_records` and `retries_total` |
| **E**levation of privilege | Compromised app container | distroless, non-root, `read_only`, `cap_drop: [ALL]`, `no-new-privileges`; runtime DB role is DML-only | Network policies; rootless/userns runtime; image signing |

### Input validation and limits (OWASP API4/API8, CWE-20, CWE-400)

- **Producer edge** ([0005](0005-ingestion-http-api.md)):
  - `Content-Type: application/json` is required (otherwise 415).
  - `http.MaxBytesReader` at 64 KiB (otherwise 413).
  - `DisallowUnknownFields`, and exactly one JSON value per body.
  - `user_id` and `activity_type` are non-empty, length-capped, valid UTF-8 with no control characters. Unknown activity types stay accepted ([0004](0004-domain-model-and-ubiquitous-language.md)).
  - `timestamp` is RFC 3339, normalized to UTC.
  - `metadata` is string-to-string, with a cap on entry count and key/value length.
  - The numeric caps are stated once, in the JSON Schema (`maxLength`, `maxProperties`) and in [0005](0005-ingestion-http-api.md). Each service's `domain` validation applies them, and the contract fixtures test them ([0014](0014-testing-strategy-and-quality-tooling.md)).
- **Consumer edge:** the same validation runs again after decoding. Failures are permanent errors and go to the DLQ.
- **HTTP servers:** `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout` and `IdleTimeout` are set; `MaxHeaderBytes` is bounded; never `http.ListenAndServe` with a zero-value server (OWASP API8 misconfiguration).
- **Errors to clients:** a stable error code and message only. No Go error strings, stack traces, SQL or broker addresses.

### Data protection and PII (CWE-532)

- `metadata` is passed through to Kafka but **never logged in full** and never stored in Postgres (there are no raw events; [0009](0009-aggregation-data-model.md)).
- DLQ records keep the original value, so the DLQ topic counts as sensitive. Its retention is bounded to 14 days ([0007](0007-kafka-client-and-topic-topology.md)). The headers the DLQ publisher **adds** are only `x-error-class`, `x-original-topic/partition/offset` and `x-failed-at`: no error messages that might echo payload fields. The original `event-id`, `schema-version`, `traceparent` and `tracestate` headers are copied unchanged.
- `user_id` is never logged, at any level; no code path does so today.
- **LGPD/GDPR:** `activity_aggregates` holds `user_id` per day. A production rollout needs:
  - a retention policy (e.g. drop rows older than N months);
  - an erasure path (`DELETE … WHERE user_id = $1`, plus Kafka retention for the in-flight copy);
  - a pseudonymous `user_id` decided with the data owner.
  
  These are documented future work, not built.

### Database (CWE-89, least privilege)

- SQL is static text with `$n` placeholders or pgx batch/array parameters. String-built SQL (`fmt.Sprintf`, `+`) is forbidden; there are no dynamic identifiers.
- Roles ([0010](0010-postgresql-access-and-migrations.md)):
  - **owner** (`analytics_owner`): owns the schema, runs migrations through the one-shot job only. Locally it is the image's bootstrap superuser. In production it is a dedicated non-superuser owner.
  - **`analytics_app`**: `LOGIN`, DML only on `activity_aggregates` (the upsert needs `INSERT` and `UPDATE`, and queries need `SELECT`). Exact `GRANT`s are in [0010](0010-postgresql-access-and-migrations.md). No DDL, no ownership, no superuser.
  - `PUBLIC` has no `CREATE` on schema `public` (the default since PG15; re-assert it if a database is ever restored from an older major).
- Passwords are stored as SCRAM (the server default). `sslmode` comes from config: `disable` in local Compose, `verify-full` in production.
- The DSN is built with `url.UserPassword` / pgx config structs, never by string concatenation, so passwords with special characters can't break parsing or leak into errors.

### Kafka

- **Local:** PLAINTEXT listeners on the Compose network. The host listener is bound to `127.0.0.1` only.
- **Production (documented, not built):**
  - SASL/SCRAM-SHA-512 over TLS;
  - ACLs: the producer principal gets `WRITE` on `incoming.user_activity`; the consumer principal gets `READ` on it, `READ` on its group and `WRITE` on `incoming.user_activity.dlq`;
  - no auto-create;
  - replication factor ≥ 3 and `min.insync.replicas=2`.
  
  franz-go supports all of this through client options, so it is a configuration change, not a code change.

### API authentication (OWASP API2/API5)

Out of scope for the challenge; this is a documented assumption in [0000](0000-scope-interpretation-and-assumptions.md). `POST /v1/activities` is unauthenticated and published on `127.0.0.1` only. In production it sits behind an API gateway or ingress that does authentication (OAuth2 client credentials or mTLS), rate limiting per client (API4) and TLS termination. The service code stays unchanged.

### Secrets (CWE-798)

- Passwords reach containers only through Compose `secrets:` (`/run/secrets/<name>`) and `*_FILE` variables (`POSTGRES_PASSWORD_FILE`, `DATABASE_PASSWORD_FILE`). The config loader reads the file and trims a trailing newline ([0003](0003-system-decomposition-and-layout.md)).
- Never in `ARG`/`ENV`, image layers, labels, logs, error messages or `docker inspect` output.
- `.env` (non-secret config) and `deploy/secrets/*.txt` are gitignored. `.env.example` and `deploy/secrets/*.example` hold placeholders. `.dockerignore` excludes `.env` and `secrets/`.
- The Compose passwords are for local use only, and the README says so.

### Containers ([0015](0015-containerization-and-local-deployment.md))

- `distroless/static` with the `nonroot` user (uid 65532): no shell, no package manager.
- `read_only: true`, `cap_drop: [ALL]`, `security_opt: [no-new-privileges:true]`, memory/CPU/pids limits. Never `privileged`, never host namespaces, never a `docker.sock` mount.
- Only the producer's port (and Kafka's host listener) is published, on `127.0.0.1`. Postgres and the consumer's health port are not published.

### Supply chain (OWASP A06, CWE-1104)

- `go.sum` committed; builds use `-mod=readonly`; the dependency footprint is minimal ([0001](0001-versions-and-upgrade-policy.md)).
- `govulncheck` per module via `make vuln` ([0014](0014-testing-strategy-and-quality-tooling.md)).
- An image scan (Docker Scout locally; Trivy recommended in a real pipeline) fails on fixable critical/high CVEs.
- Base images are pinned by exact tag (digest pinning is deferred, see "Deliberately not done").
- Attested image builds use `docker buildx build --sbom=true --provenance=mode=max` (docker-container builder, OCI archive output); run it locally or in a real pipeline. `mode=max` records build args, which is safe here because no secret is ever passed as `ARG`.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| API key / JWT in the producer now | Rejected | Not asked for. Key distribution and rotation would be half-built; a gateway does it better. |
| TLS + SASL in local Compose | Rejected | Certificates and SCRAM users make the one-command start fragile, for no local benefit. Documented for production instead. |
| Passwords in `.env` / `environment:` | Rejected | Visible in `docker inspect`, process environments and accidental logs. File secrets cost almost nothing. |
| App connects as the owner/superuser | Rejected | A bug or injection could drop tables. DML-only blast radius is much smaller. |
| Log full payloads for debugging | Rejected | `metadata` may be sensitive. `topic/partition/offset` locates the record in Kafka when needed. |
| Encrypt or hash `user_id` now | Deferred | Needs a key-management and analytics decision with the data owner. Noted under LGPD/GDPR. |

## Consequences

**Positive**
- The local stack is safe by default: loopback-only ports, no secrets in git or images, least-privilege containers and DB role.
- The production gap is explicit and mostly configuration (gateway, Kafka SASL/TLS/ACLs, `sslmode=verify-full`, secret manager).
- One checklist for every code review.

**Negative / trade-offs**
- Anyone with local access can post events for any user. Accepted for an evaluation deployment; documented.
- The DLQ holds full original records, so it is as sensitive as the main topic. Mitigation: bounded retention; production ACLs restrict readers.
- The local owner role is a superuser. Mitigation: it is used only by the migrate job; production uses a non-superuser owner.
- Strict input limits may reject legitimate but unusual events. Mitigation: limits live in one place and the contract fixtures document them.

## Implementation rules

Review checklist; every item MUST hold before merge.

- [x] Producer: `Content-Type` enforced, `MaxBytesReader` (64 KiB), `DisallowUnknownFields`, field length limits, RFC 3339 timestamp normalized to UTC. (CWE-20, API4)
- [x] Consumer re-validates every record; invalid records go to the DLQ and never crash a worker.
- [x] `http.Server` has `ReadHeaderTimeout`, `ReadTimeout`, `WriteTimeout`, `IdleTimeout` and `MaxHeaderBytes`. (API8)
- [x] Error responses MUST NOT contain internal error strings, SQL, stack traces or addresses.
- [x] Logs MUST NOT contain request bodies, record values, `metadata`, headers or credentials; `user_id` is not logged at any level. (CWE-532)
- [x] The headers a DLQ record adds are only the error class and the original coordinates (no error text). Original `event-id`, `schema-version`, `traceparent` and `tracestate` headers are copied unchanged ([0011](0011-failure-handling-retry-and-dlq.md)).
- [x] SQL uses placeholders/parameters only; no `fmt.Sprintf` or string concatenation into SQL. (CWE-89)
- [x] The runtime consumer connects as `analytics_app`; DDL appears only in migrations run by the owner.
- [x] `sslmode` comes from config; production config uses `verify-full`. (Read from config; `verify-full` is a production setting, not exercised locally.)
- [x] No secret in code, `ARG`, `ENV`, image layers, `compose.yaml` literals or git; passwords come from `*_FILE`. (CWE-798)
- [x] `.env` and `deploy/secrets/*` (except `*.example`) are gitignored and dockerignored.
- [x] App containers: distroless non-root, `read_only`, `cap_drop: [ALL]`, `no-new-privileges`, resource limits; only loopback ports.
- [x] `go.sum` committed; `-mod=readonly`; govulncheck clean; image scan has no fixable critical/high findings. (A06)
- [ ] Base images pinned by digest. (Base images are tag-pinned only, see "Deliberately not done".)
- [x] Any new endpoint, topic or table gets a row in the threat-model table above (via a superseding ADR).

### Deliberately not done (minimum scope)

| Item | Trigger to build it |
|---|---|
| API key / auth on `POST /v1/activities` | Any exposure beyond loopback, or more than one client |
| Per-client rate limiting | Same as above (gateway or middleware) |
| Image digest pinning + signing (cosign) | First published release image |
| TLS/SASL and ACLs for Kafka | Any non-local broker |
| Postgres `verify-full` TLS | Any non-local database |
| Secret manager and rotation | Any non-local deployment |

## Related

- [0000](0000-scope-interpretation-and-assumptions.md) — API authentication out of scope (assumption).
- [0003](0003-system-decomposition-and-layout.md) — config loading and `*_FILE` support.
- [0007](0007-kafka-client-and-topic-topology.md) — topics, retention, and the production broker settings.
- [0005](0005-ingestion-http-api.md) — status codes and validation at the HTTP edge.
- [0011](0011-failure-handling-retry-and-dlq.md) — DLQ contents and headers.
- [0010](0010-postgresql-access-and-migrations.md) — roles and migrations.
- [0013](0013-observability.md) — logging fields and PII rules.
- [0014](0014-testing-strategy-and-quality-tooling.md) — security gates.
- [0015](0015-containerization-and-local-deployment.md) — how Compose applies hardening and secrets.
- [0001](0001-versions-and-upgrade-policy.md) — pinning and upgrade policy.

## References

- [1] OWASP, *API Security Top 10*, 2023 edition. https://owasp.org/API-Security/editions/2023/en/0x00-header/. The API2, API4, API5 and API8 categories cited in the threat model and the checklist (unrestricted resource consumption, misconfiguration).
- [2] OWASP, *Application Security Verification Standard (ASVS)*, version 5.0.0. https://owasp.org/www-project-application-security-verification-standard/. A control catalogue to extend the review checklist when the API leaves the loopback deployment.
- [3] OWASP Cheat Sheet Series: Threat Modeling (https://cheatsheetseries.owasp.org/cheatsheets/Threat_Modeling_Cheat_Sheet.html), Input Validation (https://cheatsheetseries.owasp.org/cheatsheets/Input_Validation_Cheat_Sheet.html), SQL Injection Prevention (https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html), Logging (https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html) and Secrets Management (https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html). The STRIDE table, allow-list validation, parameterized SQL, data kept out of logs and file-based secrets.
- [4] MITRE, Common Weakness Enumeration: CWE-20 (https://cwe.mitre.org/data/definitions/20.html), CWE-89 (https://cwe.mitre.org/data/definitions/89.html), CWE-400 (https://cwe.mitre.org/data/definitions/400.html), CWE-532 (https://cwe.mitre.org/data/definitions/532.html), CWE-798 (https://cwe.mitre.org/data/definitions/798.html) and CWE-1104 (https://cwe.mitre.org/data/definitions/1104.html). The weakness identifiers used in the section headings.
- [5] M. Souppaya, J. Morello and K. Scarfone, *NIST SP 800-190: Application Container Security Guide*, NIST, September 2017. https://csrc.nist.gov/pubs/sp/800/190/final. Container hardening and image-vulnerability controls (non-root, read-only filesystem, image scanning).
- [6] M. Souppaya, K. Scarfone and D. Dodson, *NIST SP 800-218: Secure Software Development Framework (SSDF) Version 1.1*, NIST, February 2022. https://csrc.nist.gov/pubs/sp/800/218/final. The supply-chain controls (pinned dependencies, vulnerability scanning, provenance).
- [7] PostgreSQL 18 documentation, "Privileges" (https://www.postgresql.org/docs/18/ddl-priv.html), "GRANT" (https://www.postgresql.org/docs/18/sql-grant.html) and "Password Authentication" (https://www.postgresql.org/docs/18/auth-password.html). The DML-only runtime role, `PUBLIC` privileges on schema `public`, and SCRAM password storage.
- [8] Apache Kafka 4.3 documentation, "Security" (https://kafka.apache.org/43/security/) and "Authorization and ACLs" (https://kafka.apache.org/43/security/authorization-and-acls/). The production profile: SASL over TLS and per-principal ACLs.
- [9] Docker Docs, "Docker security" (https://docs.docker.com/engine/security/) and the OWASP Docker Security Cheat Sheet (https://cheatsheetseries.owasp.org/cheatsheets/Docker_Security_Cheat_Sheet.html). `cap_drop`, `no-new-privileges`, read-only root filesystem and never mounting the Docker socket.
- [10] Lei no 13.709 of 14 August 2018 (Lei Geral de Protecao de Dados Pessoais), art. 18. https://www.planalto.gov.br/ccivil_03/_ato2015-2018/2018/lei/l13709.htm. And Regulation (EU) 2016/679 (GDPR), art. 17. https://eur-lex.europa.eu/eli/reg/2016/679/oj. The data-subject erasure path described under "Data protection and PII".
