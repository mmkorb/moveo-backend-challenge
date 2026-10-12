# ADR 0010: PostgreSQL access (pgx) and migrations (goose)

- **Status:** Accepted
- **Date:** 2026-10-11
- **Amended:** 2026-10-12 — a flush is now one explicit transaction that also compare-and-sets the partition's position in `consumer_offsets` (migration `00002`), per [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) option A.
- **Scope:** consumer

## Context

- The consumer writes the aggregates to one table, `activity_aggregates` ([ADR 0009](0009-aggregation-data-model.md)), with a batched `unnest` upsert, and the Kafka position each flush reaches to `consumer_offsets`, in the same transaction ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).
- The deliverables include "database schema/migrations" (summary §9). The schema must exist before the consumer starts, including when several consumer replicas start at once.
- Concurrency is bounded: there are 6 partitions, so a process never runs more than 6 flushes at once.
- PostgreSQL is fixed at major version 18. Exact image and module versions are in [ADR 0001](0001-versions-and-upgrade-policy.md).
- Three pitfalls shape this design: migrations that run inside the app process or from a third-party goose image, a superuser shared by migrations and the app, and an upsert that is wrongly described as idempotent (it is not; see [ADR 0009](0009-aggregation-data-model.md)).

## Decision

### Driver and data access

- We use **`github.com/jackc/pgx/v5`** with **`pgxpool`** and hand-written SQL constants in `internal/adapters/postgres`. There is no ORM and no query builder.
- The pool is built from typed config. The password comes from `DATABASE_PASSWORD_FILE` ([ADR 0002](0002-security-baseline-and-threat-model.md)), and `sslmode` from config.

| Setting | Config | Default | Why |
|---|---|---|---|
| `MaxConns` | `DATABASE_MAX_CONNS` | 8 | ≥ partitions this process can own (6), plus health check and headroom. Across replicas: `replicas × DATABASE_MAX_CONNS` must stay below the server's `max_connections`. |
| `MinConns` | `DATABASE_MIN_CONNS` | 1 | Avoids a cold connect on the first flush. |
| `MaxConnLifetime` / `MaxConnIdleTime` | — | 30 min / 5 min | Recycles connections after failovers. |
| `statement_timeout` (via `ConnConfig.RuntimeParams`) | `DATABASE_STATEMENT_TIMEOUT` | 5 s | A stuck statement becomes a transient `57014` error instead of hanging a partition. |
| `application_name` | — | `consumer` | Shows up in `pg_stat_activity`. |

- **One transaction per flush, nothing else.** `AggregateRepository.ApplyDeltas(ctx, checkpoint, deltas)` runs `BEGIN` (via `pool.Begin`), the compare-and-set of the partition's position, the batched upsert (skipped when the batch held only dead letters) and `COMMIT`: four round trips whatever the batch size. No transaction spans a Kafka call. `NextOffsets` (one primary-key lookup on assignment) runs outside a transaction.
- **Error translation** happens in the adapter only. Transient SQLSTATE classes (`08`, `40`, `53`, `57`), connect and network errors and timeouts are wrapped with `ports.ErrUnavailable`. A compare-and-set that matches no row is wrapped with `ports.ErrFenced`. Everything else is returned wrapped and unclassified ("unknown"). The adapter never retries ([ADR 0011](0011-failure-handling-retry-and-dlq.md)).

The statements of a flush (`internal/adapters/postgres/repository.go`):

```sql
-- 1a. Checkpoint.From is a stored position: compare-and-set.
UPDATE consumer_offsets
SET next_offset = $5, updated_at = now()
WHERE consumer_group = $1 AND topic = $2 AND partition = $3 AND next_offset = $4;

-- 1b. Checkpoint.From is NoPosition (first batch of the partition): create it.
INSERT INTO consumer_offsets (consumer_group, topic, partition, next_offset)
VALUES ($1, $2, $3, $4)
ON CONFLICT (consumer_group, topic, partition) DO NOTHING;

-- 2. Only if step 1 changed one row: the batched delta upsert of ADR 0009.
INSERT INTO activity_aggregates AS a
    (activity_date, user_id, activity_count, page_view_count)
SELECT d, u, ac, pv
FROM unnest($1::date[], $2::text[], $3::bigint[], $4::bigint[]) AS t(d, u, ac, pv)
ON CONFLICT (activity_date, user_id) DO UPDATE
SET activity_count  = a.activity_count  + EXCLUDED.activity_count,
    page_view_count = a.page_view_count + EXCLUDED.page_view_count,
    updated_at      = now();
```

If step 1 changes no row, the adapter reads the stored position in the same transaction and rolls back. When the stored position already equals `Checkpoint.To`, an earlier attempt of this very batch committed (for example, the reply to `COMMIT` was lost and the worker retried), and `ApplyDeltas` returns `nil`. Otherwise it returns an error wrapping `ports.ErrFenced`. Under `READ COMMITTED`, a second writer of the same row waits for the first one's row lock and then re-checks the `WHERE` clause against the committed row, so two owners can never both move a position. Keys are sorted before the upsert, so row locks are always taken in the same order.

`Date` is the UTC midnight of `activity_date`. pgx encodes `time.Time` into `date` using its calendar date.

### Migrations

- We use **`github.com/pressly/goose/v3` as a library**, behind the `consumer migrate` subcommand of the consumer binary. SQL files live in `services/consumer/migrations/` and are compiled in with `embed.FS`. The image needs no extra tool and no mounted files.
- The provider API is `goose.NewProvider(goose.DialectPostgres, db, fsys, opts...)` with a `*sql.DB` (via `github.com/jackc/pgx/v5/stdlib`). `lock.NewPostgresSessionLocker()` takes an advisory lock, so two concurrent `migrate` runs cannot interleave.
- Compose runs it as the one-shot service **`consumer-migrate`**: the consumer image, `command: ["migrate"]`, **owner** credentials, `depends_on: postgres: service_healthy`, `restart: "no"`. `consumer` depends on it with `service_completed_successfully` ([ADR 0015](0015-containerization-and-local-deployment.md)).
- `consumer run` **never** runs DDL. It connects as `analytics_app`.

```go
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
```

```go
// cmd/consumer/migrate.go
package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/migrations"
)

func migrate(ctx context.Context, cc *pgx.ConnConfig) error {
	db := stdlib.OpenDB(*cc) // *sql.DB with owner credentials
	defer db.Close()
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return fmt.Errorf("migration locker: %w", err)
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, migrations.FS, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	if _, err := p.Up(ctx); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}
```

`migrations/00001_create_activity_aggregates.sql` contains the schema from [ADR 0009](0009-aggregation-data-model.md) verbatim between `-- +goose Up` and `-- +goose Down`, followed by the runtime grant:

```sql
GRANT SELECT, INSERT, UPDATE ON activity_aggregates TO analytics_app;
```

`SELECT` is required because `ON CONFLICT DO UPDATE` reads the existing row (`a.activity_count`). There is no `DELETE`, `TRUNCATE` or DDL.

`migrations/00002_create_consumer_offsets.sql` adds the stored positions of [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md):

```sql
CREATE TABLE consumer_offsets (
    consumer_group TEXT NOT NULL,
    topic          TEXT NOT NULL,
    partition      INT NOT NULL,
    next_offset    BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (consumer_group, topic, partition),
    CHECK (partition >= 0),
    CHECK (next_offset >= 0)
);

GRANT SELECT, INSERT, UPDATE ON consumer_offsets TO analytics_app;
```

`next_offset` is the next offset to read (last applied record + 1), as in Kafka's own commits. The primary key is the only access path; the table has one row per partition. The runtime role may read, create and move positions but never delete them: resetting positions is an operator task, run as the owner ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md#operations-resetting-stored-positions)). The migration only adds a table, so it takes no lock on `activity_aggregates`; an older consumer keeps working while it runs. goose's own bookkeeping table (`goose_db_version`) belongs to the owner. It is migration infrastructure, not application data, and the app has no access to it.

### Roles

There are two roles:

| Role | Used by | Rights |
|---|---|---|
| `analytics_owner` | `consumer migrate` (one-shot job) and humans using `psql` | Owns the database and the schema; runs DDL. **Locally** it is the image's bootstrap user (`POSTGRES_USER`), so it is a superuser. **In production** it MUST be a dedicated non-superuser owner ([ADR 0002](0002-security-baseline-and-threat-model.md)). |
| `analytics_app` | `consumer run` | `CONNECT` plus `SELECT, INSERT, UPDATE` on `activity_aggregates` and `consumer_offsets`. Never a superuser, never DDL, never `DELETE`/`TRUNCATE`. |

`deploy/postgres/init.sql` runs once, as `analytics_owner`, when the data volume is first initialized. It creates only the runtime role. It reads that role's password from the Compose secret file named in [ADR 0015](0015-containerization-and-local-deployment.md), so no password lives in the SQL:

```sql
\getenv dbname POSTGRES_DB
\set app_pw `cat /run/secrets/analytics_app_password`

CREATE ROLE analytics_app LOGIN PASSWORD :'app_pw';
REVOKE ALL ON DATABASE :"dbname" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"dbname" TO analytics_app;
```

### Future schema changes

Changes are made with **expand/contract**: add the new structure, deploy code that handles both, backfill in batches, switch, then drop. Each migration sets `SET lock_timeout = '5s'`. Indexes are built with `CREATE INDEX CONCURRENTLY` in a migration marked `-- +goose NO TRANSACTION`. Kafka buffers events while the consumer is stopped for a migration, so a short stop of the consumer is acceptable today.

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| pgx/v5 + pgxpool, hand-written SQL (this ADR) | Chosen | Native protocol, array binding for `unnest`, typed `PgError` with SQLSTATE, a mature pool. |
| GORM | Rejected | Reflection ORM. It hides the exact upsert and the transaction behaviour we must reason about. |
| `database/sql` + `lib/pq` | Rejected | Mostly text format and a smaller type system; pgx adds the binary protocol, native `pgtype` types, batching and `pgxpool`. |
| sqlc on pgx | Deferred | One statement doesn't justify a code generator. Adopt it when there are about 10 queries or a read API, since compile-time checking then pays off. |
| pgx `Batch` / `CopyFrom` | Rejected | A batch sends N statements. `COPY` cannot `ON CONFLICT`, so it would need a staging table, which breaks "one table". One `unnest` statement does both. |
| Compare-and-set and upsert in one statement (data-modifying CTE) | Rejected | Saves two round trips, but "already applied" vs "fenced" then needs a third query anyway, and the explicit transaction is far easier to review. |
| goose CLI image or separate migrate image | Rejected | A third-party image and mounted SQL files; the version can drift from the code. |
| Migrate on `consumer run` startup | Rejected | Replicas race; the app would need DDL rights; slow DDL fails health checks. |
| golang-migrate, Atlas | Rejected | Both are fine, but goose's embedded provider with a session locker covers our needs with plain SQL files. |
| One superuser for everything | Rejected | A compromised consumer could drop the table or read other databases. |

## Consequences

**Positive**
- Four round trips per flush (`BEGIN`, compare-and-set, upsert, `COMMIT`), whatever the batch size.
- The aggregates and the position they reach commit or roll back together, so a batch is applied at most once per position.
- Least privilege: the runtime role can only read, add and update rows in two tables.
- The schema version ships inside the same image as the code that needs it.

**Negative / trade-offs**
- SQL is checked only by integration tests. Mitigation: testcontainers tests run every query against real PostgreSQL ([ADR 0014](0014-testing-strategy-and-quality-tooling.md)).
- Two roles and two secrets to manage. Mitigation: `init.sql` and Compose secrets do it once ([ADR 0015](0015-containerization-and-local-deployment.md)).
- `init.sql` runs only on an empty data volume. Mitigation: the README says to run `docker compose down -v` after changing it.

## Implementation rules

- All SQL MUST be constants with `$n` parameters; values MUST NOT be concatenated into SQL ([ADR 0002](0002-security-baseline-and-threat-model.md)).
- The runtime consumer MUST connect as `analytics_app`. Migrations MUST run as `analytics_owner`. `analytics_app` MUST NOT be a superuser; in production neither may be.
- `consumer run` MUST NOT execute DDL or call goose.
- Migrations MUST be embedded with `embed.FS` and applied with the goose provider and the Postgres session locker.
- A migration file, once merged, MUST NOT be edited; a fix is a new migration.
- `pool.Close()` MUST be called once, last, during shutdown ([ADR 0012](0012-lifecycle-and-graceful-shutdown.md)).
- `DATABASE_MAX_CONNS` SHOULD be at least the number of partitions one process can own (6 here), so concurrent partition flushes do not queue on the pool. Configuration validation does not enforce this: it only requires a positive value that is not below `DATABASE_MIN_CONNS`. The default of 8 satisfies the recommendation.
- The delta upsert MUST run in the same transaction as the compare-and-set of `consumer_offsets`, after it, and only if it changed one row.
- The adapter MUST translate errors to `ports.ErrUnavailable` or leave them unclassified, and MUST NOT retry.

## Related

- [ADR 0009](0009-aggregation-data-model.md) — table, upsert and queries.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — when `ApplyDeltas` and `NextOffsets` are called; the stored positions.
- [ADR 0011](0011-failure-handling-retry-and-dlq.md) — error classes and the retry layer.
- [ADR 0015](0015-containerization-and-local-deployment.md) — `postgres` and `consumer-migrate` services, secrets.
- [ADR 0001](0001-versions-and-upgrade-policy.md) — pgx, goose and PostgreSQL versions.
- [ADR 0002](0002-security-baseline-and-threat-model.md) — roles, TLS and secret handling.

## References

- [1] pgx v5.11.0: https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0 (release: https://github.com/jackc/pgx/releases/tag/v5.11.0), with `pgxpool` (https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0/pgxpool), `pgconn` (https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0/pgconn, `PgError`, `ConnectError`) and `stdlib` (https://pkg.go.dev/github.com/jackc/pgx/v5@v5.11.0/stdlib). The pool settings in the table, SQLSTATE access for error translation, and the `*sql.DB` bridge for goose.
- [2] goose v3.28.0: https://pkg.go.dev/github.com/pressly/goose/v3@v3.28.0 (release: https://github.com/pressly/goose/releases/tag/v3.28.0) and its `lock` package (https://pkg.go.dev/github.com/pressly/goose/v3@v3.28.0/lock). `NewProvider`, embedded migrations and the Postgres session locker used by `consumer migrate`.
- [3] PostgreSQL 18 documentation, "Transaction Isolation" (https://www.postgresql.org/docs/18/transaction-iso.html) and "INSERT" (https://www.postgresql.org/docs/18/sql-insert.html). Read Committed row-lock behaviour behind the compare-and-set, and the privileges `ON CONFLICT DO UPDATE` requires (`SELECT` and `UPDATE`).
- [4] PostgreSQL 18 documentation, "PostgreSQL Error Codes" (https://www.postgresql.org/docs/18/errcodes-appendix.html) and "Client Connection Defaults" (https://www.postgresql.org/docs/18/runtime-config-client.html, `statement_timeout`). The SQLSTATE classes (`08`, `40`, `53`, `57`) and the `57014` error from a statement timeout.
- [5] PostgreSQL 18 documentation, "Privileges" (https://www.postgresql.org/docs/18/ddl-priv.html), "GRANT" (https://www.postgresql.org/docs/18/sql-grant.html), "CREATE INDEX" (https://www.postgresql.org/docs/18/sql-createindex.html, `CONCURRENTLY`) and "SSL Support" (https://www.postgresql.org/docs/18/libpq-ssl.html, `verify-full`). The role split, the DML-only grants, online index builds and the production TLS mode.
- [6] Pramod Sadalage and Martin Fowler, "Evolutionary Database Design" (https://martinfowler.com/articles/evodb.html) and Danilo Sato, "Parallel Change" (https://martinfowler.com/bliki/ParallelChange.html). The expand/contract approach to future schema changes.
- [7] Docker Hub, `postgres` official image. https://hub.docker.com/_/postgres. Initialization scripts run only on an empty data directory, which is why `init.sql` needs `docker compose down -v` to re-run.
- [8] OWASP Cheat Sheet Series, "SQL Injection Prevention". https://cheatsheetseries.owasp.org/cheatsheets/SQL_Injection_Prevention_Cheat_Sheet.html. Parameterized statements only.
