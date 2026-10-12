package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/retry"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// Member is one consumer-group member: its own franz-go client, poll loop and
// one worker goroutine per assigned partition (ADR 0008). All members of all
// consumer processes join the same group.
type Member struct {
	id        int
	cfg       Config
	proc      BatchProcessor
	positions PositionLoader
	logger    *slog.Logger
	metrics   *consumerMetrics
	reg       prometheus.Registerer
	pollLag   bool // only one member per process refreshes consumer_lag_records

	started atomic.Bool
	joined  atomic.Bool

	// afterProcess is a test seam; see worker.afterProcess.
	afterProcess func(partition int32) bool

	// lifeCtx is the parent of the worker flush contexts. Run sets it before
	// the client exists, so every rebalance callback sees it.
	lifeCtx context.Context

	mu      sync.Mutex
	workers map[int32]*worker
	wg      sync.WaitGroup // worker goroutines

	// Fence recovery (ADR 0008): fenced workers ask the poll loop to reload
	// their partition's stored position and re-seek between two polls.
	resyncMu sync.Mutex
	resync   map[int32]time.Time // fenced partitions waiting for a resync -> when it is due (zero: now; else a failed load is retried then)
	wake     context.CancelFunc  // cancels the poll in progress, if any
}

// NewGroupMembers validates cfg and returns n members of cfg.Group sharing proc
// and positions. It registers the pipeline metrics on reg; no client is created
// and no goroutine is started until Run.
func NewGroupMembers(cfg Config, n int, proc BatchProcessor, positions PositionLoader, logger *slog.Logger, reg prometheus.Registerer) ([]*Member, error) {
	if err := cfg.validateConsumer(); err != nil {
		return nil, fmt.Errorf("kafka consumer config: %w", err)
	}
	if n <= 0 {
		return nil, fmt.Errorf("kafka consumer config: member count %d must be > 0", n)
	}
	if proc == nil || positions == nil || logger == nil || reg == nil {
		return nil, errors.New("kafka consumer: nil processor, position loader, logger or registerer")
	}
	cfg = cfg.withDefaults()
	metrics, err := newConsumerMetrics(reg)
	if err != nil {
		return nil, err
	}
	members := make([]*Member, n)
	for i := range members {
		members[i] = newMember(cfg, i, proc, positions, logger, metrics, reg)
	}
	members[0].pollLag = true
	return members, nil
}

func newMember(cfg Config, id int, proc BatchProcessor, positions PositionLoader, logger *slog.Logger, metrics *consumerMetrics, reg prometheus.Registerer) *Member {
	return &Member{
		id:        id,
		cfg:       cfg,
		proc:      proc,
		positions: positions,
		logger:    logger.With("component", "kafka-consumer", "member", id),
		metrics:   metrics,
		reg:       reg,
		workers:   make(map[int32]*worker),
		resync:    make(map[int32]time.Time),
	}
}

// instanceID is the static group instance ID of this member, "" for dynamic
// membership. Each member of a process needs its own (ADR 0012).
func (m *Member) instanceID() string {
	if m.cfg.InstanceID == "" {
		return ""
	}
	return m.cfg.InstanceID + "-" + strconv.Itoa(m.id)
}

// Joined reports whether the member has received an assignment at least once
// (readiness, ADR 0013).
func (m *Member) Joined() bool { return m.joined.Load() }

// Run consumes until ctx is cancelled, then shuts down in the order of ADR
// 0012: stop polling; flush every partition batch (each within
// RevokeFlushTimeout, all within DrainTimeout); commit marked offsets; leave the
// group; close the client. It returns only after every goroutine it started has
// exited. Run may be called once.
func (m *Member) Run(ctx context.Context) error {
	if !m.started.CompareAndSwap(false, true) {
		return errors.New("kafka member: Run called twice")
	}
	lifeCtx, lifeCancel := context.WithCancel(context.WithoutCancel(ctx))
	defer lifeCancel()
	m.lifeCtx = lifeCtx

	cl, err := kgo.NewClient(m.clientOpts()...)
	if err != nil {
		return fmt.Errorf("create kafka member %d: %w", m.id, err)
	}
	var lagWG sync.WaitGroup
	lagCtx, lagCancel := context.WithCancel(ctx)
	if m.pollLag {
		lagWG.Go(func() { m.lagLoop(lagCtx, kadm.NewClient(cl)) })
	}

	pollErr := m.poll(ctx, cl)
	lagCancel()
	lagWG.Wait()

	drainCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), m.cfg.DrainTimeout)
	defer cancel()
	start := time.Now()
	flushCtx, flushCancel := context.WithTimeout(drainCtx, m.cfg.RevokeFlushTimeout)
	m.stopWorkers(flushCtx, nil, stopGraceful) // stage 2
	flushCancel()
	m.logger.Info("shutdown stage", "stage", "flush", "took", time.Since(start).String())

	start = time.Now()
	commitErr := cl.CommitMarkedOffsets(drainCtx) // stage 3
	if commitErr != nil {
		m.metrics.commitFailures.Inc()
		m.logger.Warn("offset commit failed after db commit", "stage", "shutdown", "error_type", errorType(commitErr))
		commitErr = fmt.Errorf("commit offsets on shutdown: %w", commitErr)
	}
	m.logger.Info("shutdown stage", "stage", "commit", "took", time.Since(start).String())

	start = time.Now()
	memberID, _ := cl.GroupMetadata()
	// Stage 4. With an instance ID, franz-go stops heartbeating here but
	// sends no LeaveGroup (consumer_group.go, groupConsumer.leave): the broker
	// would keep the partitions for the session timeout, waiting for this
	// instance to come back. A clean stop leaves explicitly instead.
	leaveErr := cl.LeaveGroupContext(drainCtx)
	if leaveErr == nil && m.instanceID() != "" {
		leaveCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), leaveTimeout)
		leaveErr = leaveStatic(leaveCtx, cl, m.cfg.Group, memberID, m.instanceID())
		cancel()
	}
	if leaveErr != nil {
		m.logger.Warn("leaving the group failed; the partitions move after the session timeout",
			"session_timeout", m.cfg.SessionTimeout.String(), "error_type", errorType(leaveErr))
		leaveErr = fmt.Errorf("leave group: %w", leaveErr)
	}
	cl.CloseAllowingRebalance()
	m.logger.Info("shutdown stage", "stage", "leave", "took", time.Since(start).String())

	m.wg.Wait()
	return errors.Join(pollErr, commitErr, leaveErr)
}

func (m *Member) clientOpts() []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(m.cfg.Brokers...),
		kgo.ClientID(clientIDPrefix(m.cfg) + "-member-" + strconv.Itoa(m.id)),
		kgo.ConsumerGroup(m.cfg.Group),
		// Classic group protocol (no kgo.ServerSideBalancer). With an instance
		// ID, a member that dies without leaving keeps its partitions for the
		// session timeout; a restart with the same ID within it takes them
		// back without a rebalance (ADR 0012).
		kgo.SessionTimeout(m.cfg.SessionTimeout),
		kgo.ConsumeTopics(m.cfg.Topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // no stored position, no commit: start at the beginning, lose nothing
		kgo.Balancers(kgo.CooperativeStickyBalancer()),
		kgo.AutoCommitMarks(),      // only marked (= persisted) records are committed
		kgo.BlockRebalanceOnPoll(), // callbacks never run while a poll is being dispatched
		// Runs after OnPartitionsAssigned and the group's OffsetFetch, before
		// the new partitions are fetched: resume from the positions stored in
		// PostgreSQL, the source of truth (ADR 0008).
		kgo.AdjustFetchOffsetsFn(m.adjustOffsets),
		kgo.OnPartitionsAssigned(func(_ context.Context, cl *kgo.Client, assigned map[string][]int32) {
			m.onAssigned(cl, assigned)
		}),
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
			m.onRevoked(ctx, cl, revoked)
		}),
		kgo.OnPartitionsLost(func(ctx context.Context, _ *kgo.Client, lost map[string][]int32) {
			m.onLost(ctx, lost)
		}),
		kgo.WithHooks(newClientMetrics(m.reg)),
	}
	if id := m.instanceID(); id != "" {
		opts = append(opts, kgo.InstanceID(id))
	}
	return opts
}

// leaveStatic removes the static member instanceID (with its current
// memberID) from group, which franz-go does not do on close (kgo.InstanceID).
// The broker then rebalances at once, so a clean stop hands its partitions
// over without waiting for the session timeout. Sending the member ID makes
// the broker reject the leave (FENCED_INSTANCE_ID) if another process has
// taken this instance ID over in the meantime. A member that never joined has
// nothing to leave. *kgo.Client routes the request to the group coordinator.
func leaveStatic(ctx context.Context, rq kmsg.Requestor, group, memberID, instanceID string) error {
	if memberID == "" {
		return nil
	}
	req := kmsg.NewPtrLeaveGroupRequest()
	req.Group = group
	member := kmsg.NewLeaveGroupRequestMember()
	member.MemberID = memberID
	member.InstanceID = kmsg.StringPtr(instanceID)
	member.Reason = kmsg.StringPtr("static member shutting down")
	req.Members = append(req.Members, member)
	resp, err := req.RequestWith(ctx, rq)
	if err != nil {
		return err
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return err
	}
	for _, mr := range resp.Members {
		if err := kerr.ErrorForCode(mr.ErrorCode); err != nil {
			return err
		}
	}
	return nil
}

// poll is the poll loop. It never blocks on a worker and always allows
// rebalancing after a poll (ADR 0008). It returns nil once ctx is cancelled.
func (m *Member) poll(ctx context.Context, cl *kgo.Client) error {
	for {
		pollCtx, cancel := m.pollContext(ctx)
		fetches := cl.PollRecords(pollCtx, m.cfg.FlushMaxRecords)
		m.disarmWake()
		cancel()
		if fetches.IsClientClosed() {
			cl.AllowRebalance()
			return fmt.Errorf("kafka member %d: %w", m.id, kgo.ErrClientClosed)
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			m.logger.Warn("fetch error", "topic", topic, "partition", partition, "error_type", errorType(err))
		})
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			if len(p.Records) > 0 {
				m.dispatch(p.Partition, p.Records)
			}
		})
		// Rebalances are still blocked here (BlockRebalanceOnPoll): no
		// partition can be revoked while a fenced one is re-sought.
		m.resyncFenced(ctx, cl)
		cl.AllowRebalance()
		if ctx.Err() != nil {
			return nil
		}
	}
}

// dispatch hands records to the worker of their partition.
func (m *Member) dispatch(partition int32, recs []*kgo.Record) {
	m.mu.Lock()
	w := m.workers[partition]
	m.mu.Unlock()
	if w == nil {
		// Not expected with BlockRebalanceOnPoll; the records stay uncommitted.
		m.logger.Warn("records for a partition without a worker", "topic", m.cfg.Topic,
			"partition", partition, "offset", recs[0].Offset)
		return
	}
	w.enqueue(recs)
}

// requestResync is called by a fenced worker after it sealed itself and paused
// its partition: the next turn of the poll loop reloads the stored position and
// resumes the partition from it (ADR 0008). It wakes a poll that is waiting
// for records, because the fenced partition no longer fetches any.
func (m *Member) requestResync(partition int32) {
	m.resyncMu.Lock()
	defer m.resyncMu.Unlock()
	// A new fence is due at once. Each partition keeps its own due time, so
	// it never cancels the back-off of another whose position load failed.
	m.resync[partition] = time.Time{}
	if m.wake != nil {
		m.wake()
	}
}

// pollContext returns the context of the next poll: cancelled at once when a
// resync is due, ending at the retry time of a failed one, otherwise ctx.
func (m *Member) pollContext(ctx context.Context) (context.Context, context.CancelFunc) {
	m.resyncMu.Lock()
	defer m.resyncMu.Unlock()
	var (
		pollCtx context.Context
		cancel  context.CancelFunc
	)
	next, pending, due := m.nextResyncLocked(time.Now())
	if pending && !due {
		pollCtx, cancel = context.WithDeadline(ctx, next)
	} else {
		pollCtx, cancel = context.WithCancel(ctx)
	}
	if due {
		cancel()
	}
	m.wake = cancel
	return pollCtx, cancel
}

// nextResyncLocked reports whether a resync is pending, whether one is due at
// now, and otherwise the earliest due time. The caller holds resyncMu.
func (m *Member) nextResyncLocked(now time.Time) (next time.Time, pending, due bool) {
	for _, at := range m.resync {
		pending = true
		if at.IsZero() || !now.Before(at) {
			return time.Time{}, true, true
		}
		if next.IsZero() || at.Before(next) {
			next = at
		}
	}
	return next, pending, false
}

func (m *Member) disarmWake() {
	m.resyncMu.Lock()
	m.wake = nil
	m.resyncMu.Unlock()
}

// dueResyncs takes the partitions whose resync is due.
func (m *Member) dueResyncs() []int32 {
	m.resyncMu.Lock()
	defer m.resyncMu.Unlock()
	now := time.Now()
	var parts []int32
	for p, at := range m.resync {
		if at.IsZero() || !now.Before(at) {
			parts = append(parts, p)
		}
	}
	for _, p := range parts {
		delete(m.resync, p)
	}
	slices.Sort(parts)
	return parts
}

// retryResyncs puts parts back, due after the maximum retry delay.
func (m *Member) retryResyncs(parts []int32) {
	m.resyncMu.Lock()
	defer m.resyncMu.Unlock()
	at := time.Now().Add(m.cfg.Retry.Max)
	for _, p := range parts {
		m.resync[p] = at
	}
}

// resyncFenced recovers fenced partitions that this member still owns. A
// fence means the stored position moved under the worker, so its fetch
// position and its From are stale; if the member is the rightful owner, the
// partition would otherwise stall until the next rebalance. For each one it
// reloads the stored position, moves the fetch position there with
// SetOffsets, starts a fresh worker that presents that position as
// Checkpoint.From, and resumes fetching.
//
// It runs in the poll loop between PollRecords and AllowRebalance, so no
// rebalance callback can run concurrently, as franz-go requires for
// SetOffsets in a group (consumer.go, Client.SetOffsets). SetOffsets stops
// the fetch session, which drops every buffered fetch, so no record fetched
// from the old position can reach the new worker. The fenced worker is sealed
// and the new one marks nothing before its first flush, so no commit of the
// partition races the seek. Fencing stays the guard: if this member is in
// fact stale, the new worker's first flush is fenced again, and the member
// learns that it lost the partition at its next heartbeat.
func (m *Member) resyncFenced(ctx context.Context, gc groupClient) {
	parts := m.dueResyncs()
	if len(parts) == 0 {
		return
	}
	stored, err := m.loadPositions(ctx, parts)
	if err != nil {
		if ctx.Err() == nil {
			m.logger.Error("cannot reload stored positions after a fence; retrying",
				"topic", m.cfg.Topic, "partitions", parts, "retry_in", m.cfg.Retry.Max.String(), "error_type", errorType(err))
			m.retryResyncs(parts)
		}
		return
	}
	for _, p := range parts {
		m.resyncPartition(ctx, gc, p, stored)
	}
}

func (m *Member) resyncPartition(ctx context.Context, gc groupClient, p int32, stored map[int32]int64) {
	m.mu.Lock()
	old := m.workers[p]
	m.mu.Unlock()
	if old == nil || !old.fenced.Load() {
		return // revoked or lost in the meantime
	}
	// The fenced worker returns right after it asked for the resync.
	waitCtx, cancel := context.WithTimeout(ctx, stopGrace)
	exited := old.wait(waitCtx)
	cancel()
	if !exited {
		m.retryResyncs([]int32{p})
		return
	}
	next, ok := stored[p]
	if !ok {
		// Only an operator deleting the row can cause this (ADR 0008,
		// Operations); without a position there is nothing safe to seek to.
		m.logger.Error("no stored position after a fence; the partition stays stopped until the next rebalance",
			"topic", m.cfg.Topic, "partition", p)
		return
	}
	gc.SetOffsets(map[string]map[int32]kgo.EpochOffset{m.cfg.Topic: {p: {Epoch: -1, Offset: next}}})
	w := newWorker(m.lifeCtx, gc, m.cfg.Topic, p, m)
	w.setPosition(next, next) // also align the Kafka committed offset first
	m.mu.Lock()
	m.workers[p] = w
	m.mu.Unlock()
	m.wg.Go(w.run)
	gc.ResumeFetchPartitions(w.tp())
	m.metrics.resyncs.Inc()
	m.logger.Warn("partition resumed from the stored position after a fence",
		"topic", m.cfg.Topic, "partition", p, "stored_offset", next)
}

// onAssigned starts one worker per newly assigned partition. It never blocks.
func (m *Member) onAssigned(gc groupClient, assigned map[string][]int32) {
	parts := assigned[m.cfg.Topic]
	m.joined.Store(true)
	if len(parts) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range parts {
		if _, ok := m.workers[p]; ok {
			continue
		}
		w := newWorker(m.lifeCtx, gc, m.cfg.Topic, p, m)
		m.workers[p] = w
		m.wg.Go(w.run)
	}
	m.logger.Info("partitions assigned", "topic", m.cfg.Topic, "partitions", sorted(parts))
}

// adjustOffsets replaces the Kafka committed offset of every newly assigned
// partition that has a stored position with that position, and hands each
// worker the position it must present as Checkpoint.From (ADR 0008).
// Partitions without a stored position keep the committed offset or, without
// one, the reset policy; their first flush creates the position. When the
// stored position is ahead of Kafka (a crash between the database commit and
// the offset commit), the worker also commits it to Kafka before its first
// batch, so consumer lag reads 0 once the partition is drained.
//
// franz-go calls it after OnPartitionsAssigned, once per group session, and
// assigns the returned offsets. An error ends the session: the partitions are
// reported lost, and the rejoin assigns and loads them again.
func (m *Member) adjustOffsets(ctx context.Context, offsets map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
	topicOffsets := offsets[m.cfg.Topic]
	if len(topicOffsets) == 0 {
		return offsets, nil
	}
	parts := slices.Sorted(maps.Keys(topicOffsets))
	stored, err := m.loadPositions(ctx, parts)
	if err != nil {
		m.logger.Error("cannot load stored positions; giving the partitions up",
			"topic", m.cfg.Topic, "partitions", parts, "error_type", errorType(err))
		return offsets, fmt.Errorf("load stored positions: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for _, p := range parts {
		committed := topicOffsets[p].EpochOffset().Offset // < 0: nothing committed
		next, ok := stored[p]
		w := m.workers[p]
		if !ok {
			if w != nil {
				w.setPosition(ports.NoPosition, 0)
			}
			m.logger.Info("no stored position; resuming from the Kafka committed offset",
				"topic", m.cfg.Topic, "partition", p, "has_committed_offset", committed >= 0, committedOffset(committed))
			continue
		}
		topicOffsets[p] = kgo.NewOffset().At(next) // epoch -1: no data-loss detection on a seek
		var syncTo int64
		switch {
		case committed < next:
			syncTo = next
		case committed > next:
			m.logger.Warn("Kafka committed offset is ahead of the stored position; resuming from the stored position",
				"topic", m.cfg.Topic, "partition", p, "has_committed_offset", committed >= 0, committedOffset(committed), "stored_offset", next)
		}
		if w != nil {
			w.setPosition(next, syncTo)
		}
		m.logger.Info("resuming from the stored position",
			"topic", m.cfg.Topic, "partition", p, "stored_offset", next, "has_committed_offset", committed >= 0, committedOffset(committed))
	}
	return offsets, nil
}

// committedOffset is the committed_offset log attribute. franz-go reports a
// negative sentinel (-1 at end, -2 at start) when the group has no commit; the
// attribute is then left out (an empty slog.Attr is ignored) so the field is
// always an integer, and has_committed_offset says whether it is present.
func committedOffset(committed int64) slog.Attr {
	if committed < 0 {
		return slog.Attr{}
	}
	return slog.Int64("committed_offset", committed)
}

// loadPositions reads the stored positions with the flush retry policy for at
// most RevokeFlushTimeout, each attempt within FlushTimeout: the call delays
// the start of the group session (or, for a resync, the next poll), so it
// must stay well below the rebalance timeout.
func (m *Member) loadPositions(ctx context.Context, parts []int32) (map[int32]int64, error) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.RevokeFlushTimeout)
	defer cancel()
	var stored map[int32]int64
	retryable := func(error) bool { return ctx.Err() == nil }
	onRetry := func(attempt int, wait time.Duration, err error) {
		class := retryClass(err)
		m.metrics.retries.WithLabelValues(dependencyPostgres, class).Inc()
		if attempt == 1 || attempt%retryLogEvery == 0 {
			m.logger.Warn("loading stored positions failed; retrying", "attempt", attempt,
				"backoff", wait.String(), "error_class", class, "error_type", errorType(err))
		}
	}
	err := retry.Do(ctx, m.cfg.Retry, retryable, onRetry, func(ctx context.Context) error {
		return withAttemptTimeout(ctx, m.cfg.FlushTimeout, func(ctx context.Context) error {
			var err error
			stored, err = m.positions.NextOffsets(ctx, m.cfg.Group, m.cfg.Topic, parts)
			return err
		})
	})
	return stored, err
}

// onRevoked flushes and marks the revoked partitions within RevokeFlushTimeout,
// then commits (ADR 0008). It also runs at the end of every group session with
// no partitions, which just commits.
func (m *Member) onRevoked(ctx context.Context, gc groupClient, revoked map[string][]int32) {
	parts := revoked[m.cfg.Topic]
	if len(parts) > 0 {
		start := time.Now()
		stopCtx, cancel := context.WithTimeout(ctx, m.cfg.RevokeFlushTimeout)
		m.stopWorkers(stopCtx, parts, stopGraceful)
		cancel()
		m.logger.Info("partitions revoked", "topic", m.cfg.Topic, "partitions", sorted(parts), "took", time.Since(start).String())
	}
	commitCtx, cancel := context.WithTimeout(ctx, commitTimeout)
	defer cancel()
	if err := gc.CommitMarkedOffsets(commitCtx); err != nil {
		m.metrics.commitFailures.Inc()
		m.logger.Warn("offset commit failed after db commit", "stage", "revoke", "error_type", errorType(err))
	}
}

// onLost drops the lost partitions' batches without committing: they already
// belong to another member (ADR 0008).
func (m *Member) onLost(ctx context.Context, lost map[string][]int32) {
	parts := lost[m.cfg.Topic]
	if len(parts) == 0 {
		return
	}
	stopCtx, cancel := context.WithTimeout(ctx, m.cfg.RevokeFlushTimeout)
	defer cancel()
	m.stopWorkers(stopCtx, parts, stopAbort)
	m.logger.Warn("partitions lost; uncommitted batches dropped", "topic", m.cfg.Topic, "partitions", sorted(parts))
}

// stopWorkers stops the workers of parts (all workers if parts is nil) in
// parallel and waits for them until ctx is done (plus a short grace). Every
// stopped worker is sealed before stopWorkers returns, so none of them can mark
// an offset afterwards, even if it has not exited yet. Concurrent calls for the
// same worker are safe; each waits for it.
func (m *Member) stopWorkers(ctx context.Context, parts []int32, mode stopMode) {
	m.mu.Lock()
	var ws []*worker
	if parts == nil {
		for _, w := range m.workers {
			ws = append(ws, w)
		}
	} else {
		for _, p := range parts {
			if w, ok := m.workers[p]; ok {
				ws = append(ws, w)
			}
		}
	}
	m.mu.Unlock()

	for _, w := range ws {
		w.stop(ctx, mode)
	}
	for _, w := range ws {
		if !w.wait(ctx) {
			m.logger.Error("partition worker did not stop in time; its offsets will not be marked",
				"topic", w.topic, "partition", w.partition)
		}
		w.seal()
	}

	m.mu.Lock()
	for _, w := range ws {
		if m.workers[w.partition] == w {
			delete(m.workers, w.partition)
		}
	}
	m.mu.Unlock()
}

// lagLoop refreshes consumer_lag_records every LagInterval until ctx is done.
func (m *Member) lagLoop(ctx context.Context, adm *kadm.Client) {
	t := time.NewTicker(m.cfg.LagInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.refreshLag(ctx, adm)
		}
	}
}

func (m *Member) refreshLag(ctx context.Context, adm *kadm.Client) {
	ctx, cancel := context.WithTimeout(ctx, m.cfg.LagInterval)
	defer cancel()
	lags, err := adm.Lag(ctx, m.cfg.Group)
	if err == nil {
		err = lags.Error()
	}
	if err != nil {
		if ctx.Err() == nil {
			m.logger.Debug("consumer lag unavailable", "error_type", errorType(err))
		}
		return
	}
	group, ok := lags[m.cfg.Group]
	if !ok {
		return
	}
	for p, l := range group.Lag[m.cfg.Topic] {
		if l.Err == nil && l.Lag >= 0 {
			m.metrics.lag.WithLabelValues(strconv.Itoa(int(p))).Set(float64(l.Lag))
		}
	}
}

func sorted(parts []int32) []int32 {
	s := slices.Clone(parts)
	slices.Sort(s)
	return s
}
