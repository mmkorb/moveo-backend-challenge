# ADR 0012: Lifecycle and graceful shutdown

- **Status:** Accepted
- **Date:** 2026-10-11
- **Amended:** 2026-10-12 — static group membership for the consumer, with an explicit leave on a clean stop and an explicit session timeout.
- **Scope:** both

## Context

- The challenge awards bonus points if "both of your microservices shutdown gracefully when receiving SIGTERM or SIGINT". The summary (§8) also asks that shutdown be tested.
- Losing work at shutdown has a concrete cost here:
  - **producer:** a request already accepted must not be dropped before its Kafka ack;
  - **consumer:** a folded but unflushed batch must reach PostgreSQL and have its offsets committed. Otherwise the next owner re-reads it, which is safe: the position stored with the aggregates tells it where to resume, so a DB write without a Kafka commit is not applied twice ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).
- `docker compose stop` sends `SIGTERM` and, by default, `SIGKILL` **10 s** later. A shutdown budget of 10 s or more would let the process be killed mid-commit, so the budget is set explicitly and the Compose grace period is raised above it.
- Real API signatures matter:
  - `http.Server.Shutdown(ctx)` and `kgo.Client.Flush(ctx)` take a context;
  - `kgo.Client.Close()`, `kgo.Client.CloseAllowingRebalance()` and `pgxpool.Pool.Close()` do **not**.

  `pgxpool.Pool.Close` takes no context, so none can be passed to it. Calls without a context must be made cheap by a bounded, context-aware step that runs before them.

## Decision

### Signals and ownership

- `signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)` is called **only in `main`**. Every component gets the resulting `ctx`; no package below `cmd/` touches signals.
- `context.AfterFunc(ctx, stop)` restores default signal handling after the first signal. A **second** `SIGINT`/`SIGTERM` therefore kills the process immediately, which is the operator's escape hatch.
- Long-running components run under `errgroup.WithContext`. If any component fails, the group context is cancelled and every other component shuts down through the same path as on a signal.
- Each teardown stage gets its own deadline from `context.WithTimeout(context.WithoutCancel(ctx), stage)`. The parent is already cancelled at that point, so `WithoutCancel` keeps its values but drops the cancellation.
- Images use an exec-form `ENTRYPOINT ["/app"]`, so the Go binary is PID 1 and receives `SIGTERM` directly ([ADR 0015](0015-containerization-and-local-deployment.md)).
- Compose sets `stop_grace_period: 30s` on `producer` and `consumer`. Our total budget is **20 s**, which leaves 10 s of margin before `SIGKILL`.

### Producer order

| # | Stage | Call | Deadline |
|---|---|---|---|
| 1 | Readiness flips to not-ready | `/readyz` returns 503 | immediate |
| 2 | Stop accepting, finish in-flight requests (each waits for its Kafka ack) | `srv.Shutdown(ctx)` | 12 s (> `PUBLISH_TIMEOUT` 5 s) |
| 3 | Deliver anything still buffered | `client.Flush(ctx)` | 5 s |
| 4 | Release connections | `client.Close()` (no ctx; fast after `Flush`) | — |

### Consumer order

| # | Stage | Call | Deadline |
|---|---|---|---|
| 1 | Readiness flips to not-ready | `/readyz` returns 503; `/healthz` still 200 | immediate |
| 2 | Stop polling; each partition worker folds its queue and **flushes its current batch** (one transaction) | worker stop + `ProcessActivity` | inside `DRAIN_TIMEOUT` (15 s), each flush capped by `REVOKE_FLUSH_TIMEOUT` (10 s) |
| 3 | **Commit** marked offsets | `cl.CommitMarkedOffsets(ctx)` | inside `DRAIN_TIMEOUT` |
| 4 | Leave the group, then close | `cl.LeaveGroupContext(ctx)` → explicit `LeaveGroup` of the static member (below) → `cl.CloseAllowingRebalance()` | `LeaveGroupContext` inside `DRAIN_TIMEOUT`; the explicit leave has its own 2 s; `Close` is immediate after the leave |
| 5 | Stop the health/metrics HTTP server | `health.Shutdown(ctx)` | `HTTP_SHUTDOWN_TIMEOUT` (5 s) |
| 6 | Close the DLQ producer, then the DB pool | `dlq.Close()`, `pool.Close()` (no ctx; every connection is idle by now) | — |

Notes:
- Stages 2–4 run per group member, in parallel across members. Stages 5–6 run once, after **all** members have finished.
- `CloseAllowingRebalance` is the right close because members use `BlockRebalanceOnPoll` ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). Leaving the group fires a final `onRevoked`. By then no workers are left, so it commits nothing new. `LeaveGroupContext` bounds the leave with our deadline, because `Close()` alone waits for the leave with no deadline.
- If a flush misses its deadline (for example, the DB is down), the worker drops the batch; its stored position and its Kafka offset do not move. The events are re-read after restart: nothing is lost, nothing is overcounted.
- Revoke and lost callbacks use the same stop-worker code path ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)). Shutdown is "revoke everything, then leave".

### Consumer restarts: static group membership

*Amended 2026-10-12.* `crash-consumer-kill` ([ADR 0016](0016-load-and-crash-testing.md)) became ready only **29.9 s** after the killed consumer was started again. A `SIGKILL` sends no `LeaveGroup`, so the broker kept the dead members, and their partitions, until their session expired. The restarted process joined with new member IDs, and the classic rebalance waited for the dead members until they were evicted.

**Decision: static membership.** Each group member sets `kgo.InstanceID` (KIP-345) to `<CONSUMER_INSTANCE_ID>-<i>`, where `i` is the member's index in the process (`CONSUMER_WORKERS` members) and `CONSUMER_INSTANCE_ID` defaults to the **hostname**. In Compose every container has its own hostname (also each replica of `--scale`), and a restarted container (`docker start`, the restart policy) keeps it. A member that joins with an instance ID the broker already knows *replaces* the old member and gets its previous assignment back **without a rebalance**; the old member ID is fenced. A restarted replica therefore owns its partitions again as soon as it has joined. Measured in the integration test: 0.1 s after the restart, against a 30 s session timeout.

Verified in the franz-go v1.22.1 source:

- The members use the **classic** group protocol: no `kgo.ServerSideBalancer()`, so `should848()` (`consumer_group_848.go`) is false and `kgo.SessionTimeout` applies.
- `kgo.InstanceID` (`config.go`): "the client does NOT send a leave group request when closing". `groupConsumer.leave` (`consumer_group.go`) returns before building the `LeaveGroup` request when an instance ID is set, and `Client.LeaveGroupContext` documents "If you have configured the group with an InstanceID, this does not leave the group."
- A restarted static *leader* receives its old assignment; franz-go handles the KIP-345/KIP-814 leader case in `joinGroup` (`leaderNoPlan`).
- Under KIP-848 (not used here) the leave of a static member is a heartbeat with epoch `-2`, which also keeps the partitions reserved until the session timeout. Moving to KIP-848 needs this decision revisited.

**A clean stop still hands over at once.** Because franz-go sends no `LeaveGroup` for a static member, stage 4 would have made every `SIGTERM` wait for the session timeout (verified: the clean-stop integration test times out at 30 s without the fix). After `LeaveGroupContext` (which still runs the final revoke and stops heartbeating), the member therefore sends its own `LeaveGroup` (v3+) with its **member ID and instance ID**, bounded by 2 s. The broker removes the member and rebalances at once (integration test: 0.5 s). Sending the member ID, not only the instance ID as an admin removal would, makes the broker answer `FENCED_INSTANCE_ID` instead of removing a newer process that has already taken over the same instance ID. A failed leave is logged, and the partitions then move after the session timeout.

**Session timeout: `CONSUMER_SESSION_TIMEOUT`, default 30 s** (franz-go default 45 s; the broker accepts 6 s to 30 min). Heartbeats run every 3 s in a background goroutine, so a slow flush or a paused partition never expires a session.

| Session timeout | Restart after a crash | Replica that never comes back (crash, `kill -9` during scale-in) | Spurious expiry (GC pause, network blip) |
|---|---|---|---|
| short (6–10 s) | a restart slower than the timeout loses the static benefit and rebalances | partitions stall up to the timeout | more likely |
| **30 s (chosen)** | covers a container restart (process start, Postgres and Kafka pings, join: a few seconds) with a wide margin | partitions stall up to 30 s, 15 s less than before | unlikely |
| long (minutes) | covers slow restarts and rolling deploys | partitions stall for minutes | very unlikely |

A clean stop is not affected by the timeout (explicit leave), and neither is correctness: a stale member is fenced by the stored positions ([ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md)).

**Operational rules.** Instance IDs MUST be unique per running replica: two processes with the same ID fence each other in a loop (`FENCED_INSTANCE_ID`). Set `CONSUMER_INSTANCE_ID` explicitly where the hostname is not unique (for example `network_mode: host`) and to a stable name where the hostname changes on every restart (a Kubernetes Deployment; a StatefulSet pod name is stable). Lowering `CONSUMER_WORKERS` after a crash leaves the instance IDs of the removed members registered until their session expires; their partitions stall for at most the session timeout.

### Consumer `main.go` skeleton

Constructor names are illustrative, as in [ADR 0003](0003-system-decomposition-and-layout.md). The `migrate` and `healthcheck` subcommands are dispatched before `run`.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/kafka"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/application"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/config"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/telemetry"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	context.AfterFunc(ctx, stop) // a second signal now terminates immediately
	err := run(ctx)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "consumer:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	logger := telemetry.NewLogger(os.Stdout, cfg.LogLevel)
	reg := prometheus.NewRegistry()

	pool, err := postgres.NewPool(ctx, cfg.Postgres) // startup retry, ADR 0011
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close() // stage 6b

	dlq, err := kafka.NewDeadLetterPublisher(ctx, cfg.Kafka, logger)
	if err != nil {
		return fmt.Errorf("create dlq publisher: %w", err)
	}
	defer dlq.Close() // stage 6a: bounded Flush, then Close

	process := application.NewProcessActivity(postgres.NewAggregateRepository(pool), dlq)
	members, err := kafka.NewGroupMembers(cfg.Kafka, cfg.ConsumerWorkers, process, logger, reg)
	if err != nil {
		return fmt.Errorf("create group members: %w", err)
	}
	health := telemetry.NewServer(cfg.HTTPAddr, reg, logger)

	g, gctx := errgroup.WithContext(ctx)
	context.AfterFunc(gctx, health.MarkNotReady) // stage 1

	consumers, cctx := errgroup.WithContext(gctx)
	for _, m := range members {
		consumers.Go(func() error { return m.Run(cctx) }) // stages 2–4 inside Run
	}
	g.Go(func() error {
		err := consumers.Wait() // every member has flushed, committed and left
		shCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.HTTPShutdownTimeout)
		defer cancel()
		return errors.Join(err, health.Shutdown(shCtx)) // stage 5
	})
	g.Go(func() error {
		if err := health.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("health server: %w", err)
		}
		return nil
	})
	return g.Wait()
}
```

Inside `kafka.GroupMember.Run`, after the poll loop returns:

```go
drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.cfg.DrainTimeout)
defer cancel()
flushCtx, flushCancel := context.WithTimeout(drainCtx, m.cfg.RevokeFlushTimeout)
m.stopWorkers(flushCtx, nil, stopGraceful) // stage 2: flush current batches, each bounded by REVOKE_FLUSH_TIMEOUT
flushCancel()
commitErr := cl.CommitMarkedOffsets(drainCtx) // stage 3
memberID, _ := cl.GroupMetadata()
leaveErr := cl.LeaveGroupContext(drainCtx)    // stage 4: bounded leave (sends nothing for a static member)
if leaveErr == nil && m.instanceID() != "" {  //          so leave explicitly, member + instance ID
	leaveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaveTimeout)
	leaveErr = leaveStatic(leaveCtx, cl, m.cfg.Group, memberID, m.instanceID())
	cancel()
}
cl.CloseAllowingRebalance()                   //          immediate after the leave
m.wg.Wait()
return errors.Join(pollErr, commitErr, leaveErr)
```

(`cl` is the member's local client. `Member.Run` in `adapters/kafka/member.go` is the source of truth.)

### Verification

- `docker compose stop consumer producer` while load is running; `loadtest/run.sh crash-consumer-stop` automates this for the consumer and asserts the outcome ([ADR 0016](0016-load-and-crash-testing.md)). The logs show each stage with its duration (`msg="shutdown stage" stage=flush took=…`). `docker compose ps -a` shows exit code **0**, not 137 (SIGKILL).
- After a restart, the aggregate totals equal the number of accepted events: no loss, and no overcount on a clean stop.
- `crash-consumer-stop` and `crash-consumer-kill` report how long the restarted consumer takes to become ready again; with static membership both are about a second ([ADR 0016](0016-load-and-crash-testing.md)). Integration tests: a static member that disappears without leaving gets its partitions back at once when it restarts, and a cleanly stopped static member hands its partitions to the remaining member at once (both far below the 30 s session timeout).
- Tests:
  - `run`/`Run` driven with a cancelled context and fakes, asserting that `Apply` happens before the commit and that nothing runs after `Close`;
  - an integration test that sends `SIGTERM` to the built binary;
  - `goleak.VerifyTestMain` in every package with goroutines ([ADR 0014](0014-testing-strategy-and-quality-tooling.md)).

## Alternatives considered

| Option | Verdict | Reason |
|---|---|---|
| Staged shutdown with per-stage deadlines, 20 s total, 30 s grace (this ADR) | Chosen | Fits inside Docker's kill window, with margin. |
| Docker's default 10 s grace | Rejected | Too tight for a flush plus commit during a slow DB; risks SIGKILL mid-commit. |
| One shared timeout for every stage | Rejected | An early slow stage starves later ones (for example, the commit after a slow flush). |
| `signal.Notify` + a hand-written channel loop | Rejected | `NotifyContext` gives the same result with less code and plugs straight into contexts. |
| Close the pool with a context / `db.Close(ctx)` | Rejected | That API does not exist. We bound the earlier stages instead. |
| Shell-form `ENTRYPOINT` or an init wrapper | Rejected | A shell as PID 1 does not forward `SIGTERM`; distroless has no shell anyway. |
| Rely on `Close()` alone to commit | Rejected | With a custom `onRevoked`, franz-go requires a manual commit, and `Close` waits for the leave without a deadline. |
| Static membership + explicit `LeaveGroup` on a clean stop (this ADR, amended) | Chosen | A crashed replica that restarts takes its partitions back without waiting for its old session; a clean stop still hands over at once. |
| Dynamic membership with a short session timeout (for example 6 s) | Rejected | Bounds the stall after a crash, but every restart still waits for the dead member's eviction, and short sessions expire spuriously. |
| Static membership without an explicit leave | Rejected | Every clean stop or rolling deploy would stall the stopped replica's partitions for the session timeout. |
| Instance ID only, admin-style `LeaveGroup` (`kadm.LeaveGroup`, no member ID) | Rejected | Could remove a newer process that already took over the same instance ID. |

## Consequences

**Positive**
- A clean stop loses nothing and overcounts nothing.
- Shutdown is bounded and observable stage by stage.
- One code path serves revoke, lost (minus the commit) and shutdown.

**Negative / trade-offs**
- Stopping can take up to 20 s. Mitigation: the second-signal escape hatch, and stages finish early when idle (typically under 1 s).
- If a dependency is down at shutdown, the last batch is dropped and re-read later. Mitigation: the stored position did not move, so it is re-read from there; nothing is lost or counted twice.
- Platforms with a shorter kill timeout need the budgets lowered. Mitigation: all stage deadlines are config values validated at startup.
- A replica that crashes and never comes back holds its partitions for `CONSUMER_SESSION_TIMEOUT` (30 s). Mitigation: shorter than the previous 45 s; the trade-off table above; lag alerts.
- Instance IDs must be unique per replica. Mitigation: the hostname default is unique in Compose; `CONSUMER_INSTANCE_ID` overrides it; a duplicate shows as `FENCED_INSTANCE_ID` errors and is still safe (fencing, ADR 0008).
- The explicit leave adds at most 2 s to stage 4: the worst case becomes 22 s, still inside the 30 s `stop_grace_period`.

## Implementation rules

- `signal.NotifyContext` MUST appear only in `cmd/<svc>/main.go`. `os.Exit` and `log.Fatal` MUST NOT appear outside `main`.
- Every goroutine MUST belong to an `errgroup` (or a member's `WaitGroup` for partition workers) and stop when its context is cancelled.
- Teardown MUST follow the stage order above. `pool.Close()` MUST run last, and a commit MUST NOT run before its batch's flush.
- Every context-aware teardown call MUST get its own deadline derived with `context.WithoutCancel`. Calls without a context MUST be preceded by a bounded draining step.
- `DRAIN_TIMEOUT` + `HTTP_SHUTDOWN_TIMEOUT` (consumer) and the producer's stage deadlines MUST sum to ≤ 20 s. Compose `stop_grace_period` MUST be ≥ 30 s.
- Readiness MUST report not-ready from the first signal until exit; liveness MUST stay healthy during the drain.
- Dockerfiles MUST use exec-form `ENTRYPOINT`.
- Consumer group members MUST use static membership with a per-replica instance ID (`CONSUMER_INSTANCE_ID`, default the hostname) and a per-member suffix, and the session timeout MUST be set explicitly (`CONSUMER_SESSION_TIMEOUT`).
- A static member that stops cleanly MUST leave the group explicitly (member ID + instance ID) after `LeaveGroupContext`, so its partitions move without waiting for the session timeout.

## Related

- [ADR 0003](0003-system-decomposition-and-layout.md) — the composition root this ADR extends.
- [ADR 0005](0005-ingestion-http-api.md) — producer server timeouts and `PUBLISH_TIMEOUT`.
- [ADR 0008](0008-consumer-concurrency-batching-and-delivery-semantics.md) — worker flush and revoke path.
- [ADR 0011](0011-failure-handling-retry-and-dlq.md) — backoff that stops on cancellation.
- [ADR 0013](0013-observability.md) — readiness and liveness endpoints.
- [ADR 0015](0015-containerization-and-local-deployment.md) — `stop_grace_period`, exec-form entrypoint.
- [ADR 0016](0016-load-and-crash-testing.md) — `crash-consumer-stop` asserts exit code 0 and zero overcount under load.

## References

- [1] The Go Project, package `os/signal` (`NotifyContext`), `net/http` (`Server.Shutdown`) and `context`, at Go 1.27.2: https://pkg.go.dev/os/signal@go1.27.2, https://pkg.go.dev/net/http@go1.27.2 and https://pkg.go.dev/context@go1.27.2. The Go 1.21 release notes (https://go.dev/doc/go1.21) introduce `context.AfterFunc` and `context.WithoutCancel`, which the staged teardown uses.
- [2] `golang.org/x/sync` v0.24.0, package `errgroup`. https://pkg.go.dev/golang.org/x/sync@v0.24.0/errgroup. Component ownership and group cancellation.
- [3] Docker Docs, `docker container stop` (https://docs.docker.com/reference/cli/docker/container/stop/), the Dockerfile reference on exec-form `ENTRYPOINT` (https://docs.docker.com/reference/dockerfile/) and the Compose services reference, `stop_grace_period` (https://docs.docker.com/reference/compose-file/services/). `SIGTERM` followed by `SIGKILL` after a grace period; a shell-form entrypoint does not receive signals.
- [4] Apache Kafka, KIP-345: Introduce static membership protocol to reduce consumer rebalances. https://cwiki.apache.org/confluence/display/KAFKA/KIP-345%3A+Introduce+static+membership+protocol+to+reduce+consumer+rebalances. With KIP-814 (https://cwiki.apache.org/confluence/display/KAFKA/KIP-814%3A+Static+membership+protocol+should+let+the+leader+skip+assignment) for a restarted static leader.
- [5] Apache Kafka, KIP-848: The Next Generation of the Consumer Rebalance Protocol. https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol. Static membership under the new protocol, noted as a decision to revisit if the consumer moves to it.
- [6] Apache Kafka 4.3 documentation: "Design", section "Static Membership" (https://kafka.apache.org/43/design/design/), consumer configs `group.instance.id` and `session.timeout.ms` (https://kafka.apache.org/43/configuration/consumer-configs/) and the broker session-timeout bounds in the broker configs (https://kafka.apache.org/43/configuration/broker-configs/).
- [7] franz-go v1.22.1, package `kgo`. https://pkg.go.dev/github.com/twmb/franz-go@v1.22.1/pkg/kgo. `InstanceID`, `SessionTimeout` (default 45 s), `BlockRebalanceOnPoll`, `Client.LeaveGroupContext` and `CloseAllowingRebalance`; with `kmsg` v1.14.0 (https://pkg.go.dev/github.com/twmb/franz-go/pkg/kmsg@v1.14.0) for the explicit `LeaveGroup` request.
- [8] The Twelve-Factor App, IX. "Disposability". https://12factor.net/disposability. Fast startup and graceful shutdown on `SIGTERM`.
