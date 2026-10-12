package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/retry"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// groupClient is the part of *kgo.Client that partition workers, rebalance
// callbacks and the fence resync use. It is a seam for unit tests;
// *kgo.Client satisfies it.
type groupClient interface {
	MarkCommitRecords(rs ...*kgo.Record)
	CommitMarkedOffsets(ctx context.Context) error
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
	PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32
	ResumeFetchPartitions(topicPartitions map[string][]int32)
	SetOffsets(setOffsets map[string]map[int32]kgo.EpochOffset)
}

var _ groupClient = (*kgo.Client)(nil)

// stopMode says how a worker gives up its partition.
type stopMode int

const (
	// stopGraceful (revoke, shutdown): flush what is queued, mark it, exit.
	stopGraceful stopMode = iota
	// stopAbort (lost): drop everything, never mark again, exit.
	stopAbort
)

// retryLogEvery rate-limits retry logs: the first attempt, then every 10th
// (ADR 0011).
const retryLogEvery = 10

var (
	errSealed         = errors.New("partition sealed while flushing")
	errSimulatedCrash = errors.New("simulated crash before offset mark")
)

// errAttemptTimeout is the cancellation cause of a flush attempt (or a
// position load) that ran into FLUSH_TIMEOUT.
var errAttemptTimeout = errors.New("attempt exceeded FLUSH_TIMEOUT")

// withAttemptTimeout runs op with a deadline of d. A dependency that does not
// answer at all (a frozen PostgreSQL server enforces no statement_timeout, and
// TCP keepalives are still acknowledged by its kernel) would otherwise hang
// the attempt for as long as the worker lives. When the deadline fires, the
// error wraps errAttemptTimeout, which retryClass treats as transient, so the
// attempt is retried and counted like any other outage (ADR 0011). The
// compare-and-set keeps a retry safe even if the abandoned attempt committed:
// its retry finds the stored position at Checkpoint.To and succeeds without
// writing (ADR 0008).
func withAttemptTimeout(ctx context.Context, d time.Duration, op func(context.Context) error) error {
	actx, cancel := context.WithTimeoutCause(ctx, d, errAttemptTimeout)
	defer cancel()
	err := op(actx)
	if err != nil && ctx.Err() == nil && errors.Is(context.Cause(actx), errAttemptTimeout) {
		return fmt.Errorf("%w after %v: %w", errAttemptTimeout, d, err)
	}
	return err
}

// retryClass labels a retried error for retries_total and the logs (ADR 0013).
func retryClass(err error) string {
	if errors.Is(err, ports.ErrUnavailable) || errors.Is(err, errAttemptTimeout) {
		return errorClassTransient
	}
	if errors.Is(err, ports.ErrDataException) {
		return errorClassDataException
	}
	return errorClassUnknown
}

// batch is the micro-batch of one partition (ADR 0008): accepted events,
// dead letters and the last record, whose offset is marked after the flush.
type batch struct {
	events   []domain.ActivityEvent
	rejected []ports.DeadLetter
	last     *kgo.Record
}

func (b *batch) size() int { return len(b.events) + len(b.rejected) }

// worker owns one assigned partition: it decodes records in order, folds them
// into a batch, flushes on size or time with retries (each flush also moves
// the partition's stored position, see Checkpoint), and marks + commits the
// Kafka offsets after each successful flush. Exactly one worker exists per
// assigned partition; it is stopped on revoke, lost and shutdown, and stops
// itself when fenced (ADR 0008).
//
// Queue bound: the poll loop pauses the partition once the queue holds
// highWater (= FLUSH_MAX_RECORDS) records, and one poll returns at most
// FLUSH_MAX_RECORDS records, so the queue never exceeds 2×FLUSH_MAX_RECORDS
// even though enqueue never blocks.
type worker struct {
	topic     string
	partition int32
	cl        groupClient
	proc      BatchProcessor
	cfg       Config
	logger    *slog.Logger
	metrics   *consumerMetrics
	// afterProcess is a test seam: when it returns true the worker behaves as
	// if the process died between the database commit and the offset mark.
	afterProcess func(partition int32) bool
	// onFenced asks the member to resume the partition from the reloaded
	// stored position (Member.requestResync).
	onFenced func(partition int32)
	// fenced is set once the worker stopped because of ports.ErrFenced.
	fenced atomic.Bool

	ctx    context.Context // flush context; cancelled on abort or stop deadline
	cancel context.CancelFunc
	notify chan struct{} // capacity 1: "queue is not empty"
	stopCh chan struct{} // closed on graceful stop
	done   chan struct{} // closed when run returns

	stopOnce sync.Once

	// next is the partition's stored position as this worker knows it: the
	// one loaded on assignment, then the To of every committed batch. It is
	// ports.NoPosition until a position exists. The next flush presents it as
	// Checkpoint.From (the fencing token), and records below it are dropped.
	next atomic.Int64

	mu     sync.Mutex
	queue  []*kgo.Record
	paused bool
	sealed bool  // once set, the worker never marks offsets again
	syncTo int64 // > 0: stored position to commit to Kafka before the first batch

	batch batch // owned by the run goroutine
}

func newWorker(parent context.Context, gc groupClient, topic string, partition int32, m *Member) *worker {
	ctx, cancel := context.WithCancel(parent)
	w := &worker{
		topic:        topic,
		partition:    partition,
		cl:           gc,
		proc:         m.proc,
		cfg:          m.cfg,
		logger:       m.logger.With("topic", topic, "partition", partition),
		metrics:      m.metrics,
		afterProcess: m.afterProcess,
		onFenced:     m.requestResync,
		ctx:          ctx,
		cancel:       cancel,
		notify:       make(chan struct{}, 1),
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}
	w.next.Store(ports.NoPosition)
	return w
}

// setPosition records the partition's stored position (ports.NoPosition if
// none) and, if syncTo > 0, a position to commit to Kafka before the first
// batch. Called by adjustOffsets before the partition is fetched.
func (w *worker) setPosition(next, syncTo int64) {
	w.next.Store(next)
	w.mu.Lock()
	w.syncTo = syncTo
	w.mu.Unlock()
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// syncCommitted commits a pending stored position to Kafka (see
// Member.adjustOffsets). The worker does it itself, before its first batch,
// so the commit can never land after, and rewind, a commit of a later batch.
// A failure is only logged: the stored position stays the source of truth and
// the next batch's commit moves Kafka past it anyway.
func (w *worker) syncCommitted() {
	w.mu.Lock()
	to := w.syncTo
	w.syncTo = 0
	sealed := w.sealed
	w.mu.Unlock()
	if to <= 0 || sealed {
		return
	}
	ctx, cancel := context.WithTimeout(w.ctx, commitTimeout)
	defer cancel()
	// CommitRecords commits Offset+1 as given, unlike a mark, which the client
	// skips when it equals the offset it believes committed (the stored one).
	rec := &kgo.Record{Topic: w.topic, Partition: w.partition, Offset: to - 1, LeaderEpoch: -1}
	if err := w.cl.CommitRecords(ctx, rec); err != nil {
		w.logger.Warn("could not commit the stored position to Kafka; consumer lag stays stale until the next batch",
			"stored_offset", to, "error_type", errorType(err))
		return
	}
	w.logger.Info("committed the stored position to Kafka", "stored_offset", to)
}

func (w *worker) tp() map[string][]int32 { return map[string][]int32{w.topic: {w.partition}} }

func (w *worker) highWater() int { return w.cfg.FlushMaxRecords }

// enqueue hands polled records to the worker. It never blocks; it pauses
// fetching for the partition at the high-water mark. Called by the poll loop.
func (w *worker) enqueue(recs []*kgo.Record) {
	w.mu.Lock()
	if w.sealed {
		w.mu.Unlock()
		return // the partition is being given up; the records stay uncommitted
	}
	w.queue = append(w.queue, recs...)
	if !w.paused && len(w.queue) >= w.highWater() {
		w.paused = true
		w.cl.PauseFetchPartitions(w.tp())
	}
	w.mu.Unlock()
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// take removes up to limit records from the queue and resumes fetching once
// the queue is back at or below half the high-water mark.
func (w *worker) take(limit int) []*kgo.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := min(limit, len(w.queue))
	if n <= 0 {
		return nil
	}
	recs := w.queue[:n:n]
	w.queue = w.queue[n:]
	if len(w.queue) == 0 {
		w.queue = nil
	}
	if w.paused && len(w.queue) <= w.highWater()/2 {
		w.paused = false
		w.cl.ResumeFetchPartitions(w.tp())
	}
	return recs
}

// seal stops any future mark and clears a pause, so a later re-assignment of
// the partition starts fetching again.
func (w *worker) seal() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sealed = true
	w.queue = nil
	if w.paused {
		w.paused = false
		w.cl.ResumeFetchPartitions(w.tp())
	}
}

// mark marks rec for commit unless the worker is sealed. The check and the mark
// happen under the same lock as seal, so no mark can follow a seal.
func (w *worker) mark(rec *kgo.Record) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return false
	}
	w.cl.MarkCommitRecords(rec)
	return true
}

// stop asks the worker to give up its partition. A graceful stop flushes what
// is queued; its flush context is cancelled when ctx is done. An abort seals
// the worker and cancels its flush at once. stop does not wait; see wait.
func (w *worker) stop(ctx context.Context, mode stopMode) {
	if mode == stopAbort {
		w.seal()
		w.cancel()
		return
	}
	w.stopOnce.Do(func() {
		close(w.stopCh)
		context.AfterFunc(ctx, w.cancel)
	})
}

// wait waits for the worker to exit: until ctx is done, plus stopGrace for a
// worker whose flush was just cancelled. It reports whether the worker exited.
func (w *worker) wait(ctx context.Context) bool {
	select {
	case <-w.done:
		return true
	case <-ctx.Done():
	}
	t := time.NewTimer(stopGrace)
	defer t.Stop()
	select {
	case <-w.done:
		return true
	case <-t.C:
		return false
	}
}

// run is the worker goroutine.
func (w *worker) run() {
	defer close(w.done)
	defer w.cancel()

	var (
		timer    *time.Timer
		timerC   <-chan time.Time
		stopping bool
	)
	stopTimer := func() {
		if timer != nil {
			timer.Stop()
			timer, timerC = nil, nil
		}
	}
	defer stopTimer()

	for {
		w.syncCommitted()
		// Move queued records into the batch, flushing every full batch.
		for {
			recs := w.take(w.cfg.FlushMaxRecords - w.batch.size())
			if len(recs) == 0 {
				break
			}
			w.addAll(recs)
			if w.batch.size() >= w.cfg.FlushMaxRecords {
				stopTimer()
				if !w.flush(stopping) {
					return
				}
			}
		}
		if stopping {
			stopTimer()
			w.flush(true)
			return
		}
		if w.batch.size() > 0 && timer == nil {
			timer = time.NewTimer(w.cfg.FlushInterval)
			timerC = timer.C
		}
		select {
		case <-w.notify:
		case <-timerC:
			timer, timerC = nil, nil
			if !w.flush(false) {
				return
			}
		case <-w.stopCh:
			stopping = true
		case <-w.ctx.Done():
			w.dropBatch("partition worker cancelled")
			return
		}
	}
}

// addAll adds records to the batch, dropping (defensively) every record below
// the stored position: such a record is already counted. With fetching started
// at the stored position this does not happen; the check makes "never applied
// twice" independent of the seek.
func (w *worker) addAll(recs []*kgo.Record) {
	next := w.next.Load()
	skipped := 0
	for _, r := range recs {
		if next != ports.NoPosition && r.Offset < next {
			skipped++
			continue
		}
		w.add(r)
	}
	if skipped > 0 {
		w.logger.Warn("dropped records below the stored position; they are already applied",
			"stored_offset", next, "dropped", skipped, "first_offset", recs[0].Offset)
	}
}

// add decodes one record into the batch (ADR 0008: decode per record).
func (w *worker) add(r *kgo.Record) {
	ev, dl := decodeRecord(r)
	if dl != nil {
		w.batch.rejected = append(w.batch.rejected, *dl)
		if w.logger.Enabled(w.ctx, slog.LevelDebug) {
			attrs := append([]slog.Attr{
				slog.Int64("offset", dl.Offset),
				slog.String("error_class", string(dl.Class)),
				slog.String("event_id", loggableEventID(dl.Headers)),
			}, traceAttrs(dl.Headers)...)
			w.logger.LogAttrs(w.ctx, slog.LevelDebug, "record rejected; it is dead-lettered with its batch", attrs...)
		}
	} else {
		w.batch.events = append(w.batch.events, ev)
	}
	w.batch.last = r
}

// flush persists the batch together with the move of the stored position
// (ADR 0008) with unlimited retries while the flush context is alive
// (ADR 0011), then marks its last record and, unless the worker is stopping
// (the stopper commits once for all workers), commits the Kafka offset. It
// returns false when the worker must exit; the unflushed batch is then dropped
// and the stored position does not move, so the next owner re-reads it.
func (w *worker) flush(stopping bool) bool {
	if w.batch.size() == 0 {
		return true
	}
	b := w.batch
	w.batch = batch{}
	n := b.size()
	cp := ports.Checkpoint{
		Group:     w.cfg.Group,
		Topic:     w.topic,
		Partition: w.partition,
		From:      w.next.Load(),
		To:        b.last.Offset + 1,
	}

	err := retry.Do(w.ctx, w.cfg.Retry, w.retryable, w.onRetry(n, b.last), func(ctx context.Context) error {
		start := time.Now()
		err := withAttemptTimeout(ctx, w.cfg.FlushTimeout, func(ctx context.Context) error {
			return w.proc.Process(ctx, cp, b.events, b.rejected)
		})
		w.metrics.flushDuration.Observe(time.Since(start).Seconds())
		return err
	})
	if errors.Is(err, ports.ErrFenced) {
		w.fence(cp, n, err)
		return false
	}
	if err != nil {
		w.logger.Warn("dropping unflushed batch; it will be redelivered",
			"offset", b.last.Offset, "batch_size", n, "error_type", errorType(err))
		return false
	}
	w.next.Store(cp.To)
	w.metrics.consumed.WithLabelValues(resultOK).Add(float64(len(b.events)))
	w.metrics.consumed.WithLabelValues(resultDLQ).Add(float64(len(b.rejected)))

	if w.afterProcess != nil && w.afterProcess(w.partition) {
		w.logger.Warn("simulated crash after flush", "offset", b.last.Offset, "error_type", errorType(errSimulatedCrash))
		return false
	}
	if !w.mark(b.last) {
		w.logger.Warn("partition sealed during flush; offset not marked",
			"offset", b.last.Offset, "batch_size", n, "error_type", errorType(errSealed))
		return false
	}
	w.logger.Debug("batch flushed", "offset", b.last.Offset, "batch_size", n)
	if stopping {
		return true
	}
	ctx, cancel := context.WithTimeout(w.ctx, commitTimeout)
	defer cancel()
	if err := w.cl.CommitMarkedOffsets(ctx); err != nil {
		w.metrics.commitFailures.Inc()
		w.logger.Warn("offset commit failed after db commit",
			"offset", b.last.Offset, "batch_size", n, "error_type", errorType(err))
	}
	return true
}

// fence handles ports.ErrFenced: another owner moved the stored position, so
// this worker's position is stale (it is a stale member that has not noticed
// its session ended, or the rightful owner that a stale member overtook). The
// batch was rolled back; it is dropped without a retry and without the DLQ,
// the worker seals itself (no more marks), pauses fetching for the partition,
// asks the member for a resync and exits. The member then resumes the
// partition from the reloaded stored position (Member.resyncFenced); a
// rebalance that takes the partition away first simply stops the dead worker.
func (w *worker) fence(cp ports.Checkpoint, batchSize int, err error) {
	w.metrics.fenced.Inc()
	w.logger.Error("stored position moved by another owner; batch dropped and partition worker stopped",
		"from_offset", cp.From, "to_offset", cp.To, "batch_size", batchSize, "error_type", errorType(err))
	w.mu.Lock()
	w.sealed = true
	w.queue = nil
	if !w.paused {
		w.paused = true
		w.cl.PauseFetchPartitions(w.tp())
	}
	w.mu.Unlock()
	w.fenced.Store(true)
	if w.onFenced != nil {
		w.onFenced(w.partition)
	}
}

func (w *worker) dropBatch(reason string) {
	if w.batch.size() == 0 {
		return
	}
	w.logger.Warn(reason+"; dropping unflushed batch, it will be redelivered",
		"offset", w.batch.last.Offset, "batch_size", w.batch.size())
	w.batch = batch{}
}

// retryable retries every error except the cancellation of the worker's own
// context (the shutdown class of ADR 0011) and ports.ErrFenced, which no retry
// can fix (ADR 0008).
func (w *worker) retryable(err error) bool {
	if errors.Is(err, ports.ErrFenced) {
		return false
	}
	if w.ctx.Err() != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return false
	}
	return true
}

// onRetry counts every retry by dependency and class and logs the first
// attempt and every 10th: transient at WARN, unknown at ERROR (ADR 0011).
func (w *worker) onRetry(batchSize int, last *kgo.Record) func(int, time.Duration, error) {
	return func(attempt int, wait time.Duration, err error) {
		dep := dependencyPostgres
		if errors.Is(err, errDeadLetterPublish) {
			dep = dependencyKafka
		}
		class, level := retryClass(err), slog.LevelWarn
		if class != errorClassTransient {
			level = slog.LevelError
		}
		w.metrics.retries.WithLabelValues(dep, class).Inc()
		if attempt != 1 && attempt%retryLogEvery != 0 {
			return
		}
		w.logger.LogAttrs(w.ctx, level, "flush failed; retrying",
			slog.Int64("offset", last.Offset),
			slog.Int("batch_size", batchSize),
			slog.Int("attempt", attempt),
			slog.String("backoff", wait.String()),
			slog.String("dependency", dep),
			slog.String("error_class", class),
			slog.String("error_type", errorType(err)),
		)
	}
}
