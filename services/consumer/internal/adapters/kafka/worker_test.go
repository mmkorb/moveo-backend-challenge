package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// stopAll ends a test the way Run does: stop every worker gracefully.
func stopAll(t *testing.T, m *Member) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), m.cfg.RevokeFlushTimeout)
	defer cancel()
	m.stopWorkers(ctx, nil, stopGraceful)
}

func TestPartitionWorkerFlush(t *testing.T) {
	t.Run("should flush, mark and commit when the batch reaches FLUSH_MAX_RECORDS", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 3
			cfg.FlushInterval = time.Hour
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 10, 3))
			synctest.Wait()

			calls := proc.snapshot()
			if len(calls) != 1 || len(calls[0].events) != 3 || len(calls[0].rejected) != 0 {
				t.Fatalf("Process calls = %+v, want one call with 3 events", calls)
			}
			if off, ok := gc.mark(0); !ok || off != 12 {
				t.Fatalf("marked offset = %d (%v), want 12", off, ok)
			}
			if last, n := gc.lastCommit(); n != 1 || last[0] != 12 {
				t.Fatalf("commits = %d, last = %v, want 1 commit including offset 12", n, last)
			}
			if got := metricValue(t, m.metrics.consumed.WithLabelValues(resultOK)); got != 3 {
				t.Errorf("records_consumed_total{result=ok} = %v, want 3", got)
			}
			stopAll(t, m)
		})
	})

	t.Run("should flush when FLUSH_INTERVAL elapses after the first record", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushInterval = time.Second
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 2))
			time.Sleep(999 * time.Millisecond)
			synctest.Wait()
			if n := len(proc.snapshot()); n != 0 {
				t.Fatalf("Process called %d times before FLUSH_INTERVAL, want 0", n)
			}
			time.Sleep(time.Millisecond)
			synctest.Wait()
			if calls := proc.snapshot(); len(calls) != 1 || len(calls[0].events) != 2 {
				t.Fatalf("Process calls = %+v, want one call with 2 events", calls)
			}
			if off, _ := gc.mark(0); off != 1 {
				t.Fatalf("marked offset = %d, want 1", off)
			}
			stopAll(t, m)
		})
	})

	t.Run("should hand invalid records to Process as dead letters and commit past them", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 4
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 2)

			m.dispatch(2, []*kgo.Record{
				record(2, 0, validValue("u1")),
				record(2, 1, []byte(`{"user_id":`)),
				record(2, 2, []byte(`{"user_id":"","activity_type":"x","timestamp":"2024-07-01T00:00:00Z"}`)),
				record(2, 3, validValue("u2"), kgo.RecordHeader{Key: headerSchemaVersion, Value: []byte("2")}),
			})
			synctest.Wait()

			calls := proc.snapshot()
			if len(calls) != 1 || len(calls[0].events) != 1 || len(calls[0].rejected) != 3 {
				t.Fatalf("Process calls = %+v, want 1 event and 3 dead letters", calls)
			}
			wantClasses := []ports.ErrorClass{ports.ErrorClassDecode, ports.ErrorClassValidation, ports.ErrorClassDecode}
			for i, dl := range calls[0].rejected {
				if dl.Class != wantClasses[i] || dl.Offset != int64(i+1) || dl.Partition != 2 {
					t.Errorf("rejected[%d] = class %s offset %d partition %d, want %s %d 2", i, dl.Class, dl.Offset, dl.Partition, wantClasses[i], i+1)
				}
			}
			if off, _ := gc.mark(2); off != 3 {
				t.Fatalf("marked offset = %d, want 3", off)
			}
			if got := metricValue(t, m.metrics.consumed.WithLabelValues(resultDLQ)); got != 3 {
				t.Errorf("records_consumed_total{result=dlq} = %v, want 3", got)
			}
			stopAll(t, m)
		})
	})
}

func TestPartitionWorkerRebalance(t *testing.T) {
	t.Run("should flush, mark and then commit a partial batch when the partition is revoked", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushInterval = time.Hour
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 0, 1)

			m.dispatch(0, validRecords(0, 5, 2))
			synctest.Wait()
			if n := len(proc.snapshot()); n != 0 {
				t.Fatalf("Process called %d times before the revoke, want 0", n)
			}

			m.onRevoked(t.Context(), gc, map[string][]int32{testTopic: {0}})

			if calls := proc.snapshot(); len(calls) != 1 || len(calls[0].events) != 2 {
				t.Fatalf("Process calls = %+v, want the partial batch of 2", calls)
			}
			last, n := gc.lastCommit()
			if n != 1 || last[0] != 6 {
				t.Fatalf("commits = %d, last = %v, want one commit after the mark of offset 6", n, last)
			}
			m.mu.Lock()
			_, still0 := m.workers[0]
			_, still1 := m.workers[1]
			m.mu.Unlock()
			if still0 || !still1 {
				t.Fatalf("workers after revoke: p0=%v p1=%v, want only p1", still0, still1)
			}
			stopAll(t, m)
		})
	})

	t.Run("should drop the batch without marking or committing when the partition is lost", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushInterval = time.Hour
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 2))
			synctest.Wait()
			m.onLost(t.Context(), map[string][]int32{testTopic: {0}})

			if n := len(proc.snapshot()); n != 0 {
				t.Errorf("Process called %d times, want 0", n)
			}
			if _, ok := gc.mark(0); ok {
				t.Error("an offset was marked for a lost partition")
			}
			if _, n := gc.lastCommit(); n != 0 {
				t.Errorf("%d commits after a lost partition, want 0", n)
			}
		})
	})

	t.Run("should not mark a batch whose flush succeeds after the partition was lost", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			gate := make(chan struct{})
			proc := &fakeProcessor{gate: gate} // ignores ctx: the write completes anyway
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 2))
			synctest.Wait() // flush in progress, blocked in Process

			lostDone := make(chan struct{})
			go func() {
				m.onLost(t.Context(), map[string][]int32{testTopic: {0}})
				close(lostDone)
			}()
			<-lostDone // returns after RevokeFlushTimeout + grace: the worker is still stuck
			close(gate)
			synctest.Wait()

			if _, ok := gc.mark(0); ok {
				t.Error("an offset was marked after the partition was lost")
			}
			if _, n := gc.lastCommit(); n != 0 {
				t.Errorf("%d commits, want 0", n)
			}
			m.wg.Wait()
		})
	})

	t.Run("should not mark when the process dies between the database commit and the mark", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			m.afterProcess = func(int32) bool { return true }
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 2))
			synctest.Wait()
			if n := len(proc.snapshot()); n != 1 {
				t.Fatalf("Process called %d times, want 1", n)
			}
			if _, ok := gc.mark(0); ok {
				t.Error("offset marked although the worker crashed before the mark")
			}
			stopAll(t, m)
		})
	})
}

func TestPartitionWorkerFailures(t *testing.T) {
	t.Run("should retry a transient failure without committing, then commit once it recovers", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			proc := &fakeProcessor{fail: func(n int) error {
				if n < 3 {
					return fmt.Errorf("apply deltas: %w: connection refused", ports.ErrUnavailable)
				}
				return nil
			}}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 2))
			synctest.Wait()
			if _, ok := gc.mark(0); ok {
				t.Fatal("offset marked while the flush was still failing")
			}
			time.Sleep(time.Minute)
			synctest.Wait()

			if n := len(proc.snapshot()); n != 4 {
				t.Fatalf("Process called %d times, want 4 (3 failures + success)", n)
			}
			if off, ok := gc.mark(0); !ok || off != 1 {
				t.Fatalf("marked offset = %d (%v), want 1", off, ok)
			}
			if got := metricValue(t, m.metrics.retries.WithLabelValues(dependencyPostgres, errorClassTransient)); got != 3 {
				t.Errorf("retries_total{postgres,transient} = %v, want 3", got)
			}
			if strings.Contains(logs.String(), "connection refused") {
				t.Error("logs contain the error text")
			}
			stopAll(t, m)
		})
	})

	t.Run("should never mark or commit while the DLQ publish fails", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			proc := &fakeProcessor{fail: func(int) error {
				return fmt.Errorf("publish 1 dead letters: %w: %w: %w", ports.ErrUnavailable, errDeadLetterPublish,
					errors.New(`broker said {"user_id":"secret"}`))
			}}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, []*kgo.Record{record(0, 0, validValue("u1")), record(0, 1, []byte("not json"))})
			time.Sleep(time.Minute)
			synctest.Wait()

			if n := len(proc.snapshot()); n < 2 {
				t.Fatalf("Process called %d times, want retries", n)
			}
			if got := metricValue(t, m.metrics.retries.WithLabelValues(dependencyKafka, errorClassTransient)); got < 1 {
				t.Errorf("retries_total{kafka,transient} = %v, want > 0", got)
			}

			start := time.Now()
			m.onRevoked(t.Context(), gc, map[string][]int32{testTopic: {0}})
			if took := time.Since(start); took > cfg.RevokeFlushTimeout+stopGrace {
				t.Errorf("revoke took %v, want at most REVOKE_FLUSH_TIMEOUT", took)
			}
			if _, ok := gc.mark(0); ok {
				t.Error("offset marked although the DLQ publish never succeeded")
			}
			if last, _ := gc.lastCommit(); len(last) != 0 {
				t.Errorf("committed %v, want nothing for partition 0", last)
			}
			if strings.Contains(logs.String(), "secret") {
				t.Error("logs contain payload data from the error text")
			}
		})
	})

	t.Run("should label a data exception apart, log it at ERROR, never dead-letter the batch and keep retrying", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 1
			proc := &fakeProcessor{fail: func(n int) error {
				if n == 0 {
					return fmt.Errorf("%w: upsert aggregates: %w", ports.ErrDataException, errors.New("invalid byte sequence"))
				}
				return nil
			}}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 1))
			time.Sleep(time.Minute)
			synctest.Wait()

			if got := metricValue(t, m.metrics.retries.WithLabelValues(dependencyPostgres, errorClassDataException)); got != 1 {
				t.Errorf("retries_total{postgres,data_exception} = %v, want 1", got)
			}
			if !strings.Contains(logs.String(), `"level":"ERROR"`) || !strings.Contains(logs.String(), `"error_class":"data_exception"`) {
				t.Errorf("logs = %s, want an ERROR record with error_class=data_exception", logs)
			}
			if off, ok := gc.mark(0); !ok || off != 0 {
				t.Errorf("marked offset = %d (%v), want 0 after recovery", off, ok)
			}
			stopAll(t, m)
		})
	})

	t.Run("should log an unknown error class at ERROR and keep retrying", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 1
			proc := &fakeProcessor{fail: func(n int) error {
				if n == 0 {
					return errors.New("permission denied for table activity_aggregates")
				}
				return nil
			}}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 1))
			time.Sleep(time.Minute)
			synctest.Wait()

			if got := metricValue(t, m.metrics.retries.WithLabelValues(dependencyPostgres, errorClassUnknown)); got != 1 {
				t.Errorf("retries_total{postgres,unknown} = %v, want 1", got)
			}
			if !strings.Contains(logs.String(), `"level":"ERROR"`) || !strings.Contains(logs.String(), `"error_class":"unknown"`) {
				t.Errorf("logs = %s, want an ERROR record with error_class=unknown", logs)
			}
			if off, ok := gc.mark(0); !ok || off != 0 {
				t.Errorf("marked offset = %d (%v), want 0 after recovery", off, ok)
			}
			stopAll(t, m)
		})
	})

	t.Run("should stop at the drain deadline instead of finishing a long backoff", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 1
			cfg.Retry = retryPolicy(time.Hour)
			proc := &fakeProcessor{fail: func(int) error { return ports.ErrUnavailable }}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 0)
			m.dispatch(0, validRecords(0, 0, 1))
			synctest.Wait() // first attempt failed; the worker is in a backoff of up to 1h

			start := time.Now()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			m.stopWorkers(ctx, nil, stopGraceful)
			if took := time.Since(start); took != 3*time.Second {
				t.Errorf("stop took %v, want the 3s deadline", took)
			}
			if _, ok := gc.mark(0); ok {
				t.Error("offset marked for a batch that never flushed")
			}
		})
	})

	t.Run("should count an offset commit failure and keep the mark", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 1
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			gc.commitErr = errors.New("rebalance in progress")
			m, logs := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 1))
			synctest.Wait()

			if got := metricValue(t, m.metrics.commitFailures); got != 1 {
				t.Errorf("offset_commit_failures_total = %v, want 1", got)
			}
			if off, ok := gc.mark(0); !ok || off != 0 {
				t.Errorf("marked offset = %d (%v), want 0 (background commit retries it)", off, ok)
			}
			if !strings.Contains(logs.String(), "offset commit failed after db commit") {
				t.Error("missing the commit-failure log record")
			}
			stopAll(t, m)
		})
	})
}

func TestPartitionWorkerBackpressure(t *testing.T) {
	t.Run("should pause the partition at the high-water mark and resume after draining", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			gate := make(chan struct{})
			proc := &fakeProcessor{gate: gate}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			m.dispatch(0, validRecords(0, 0, 2)) // full batch: flush blocks in Process
			synctest.Wait()
			if gc.isPaused(0) {
				t.Fatal("paused before the queue reached the high-water mark")
			}
			m.dispatch(0, validRecords(0, 2, 2)) // queue = 2 = high-water
			if !gc.isPaused(0) {
				t.Fatal("not paused at the high-water mark")
			}

			close(gate)
			synctest.Wait()
			if gc.isPaused(0) {
				t.Fatal("still paused after the worker drained its queue")
			}
			if off, _ := gc.mark(0); off != 3 {
				t.Fatalf("marked offset = %d, want 3", off)
			}
			stopAll(t, m)
		})
	})

	t.Run("should resume a paused partition when it is revoked", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 1
			gate := make(chan struct{})
			proc := &fakeProcessor{gate: gate, honorCtx: true}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 0)
			m.dispatch(0, validRecords(0, 0, 1))
			synctest.Wait()
			m.dispatch(0, validRecords(0, 1, 1))
			if !gc.isPaused(0) {
				t.Fatal("not paused")
			}
			m.onRevoked(t.Context(), gc, map[string][]int32{testTopic: {0}})
			if gc.isPaused(0) {
				t.Fatal("partition still paused after revoke; a re-assignment would never fetch")
			}
		})
	})
}

func TestNewGroupMembers(t *testing.T) {
	proc := &fakeProcessor{}
	logger := discardLogger()

	t.Run("should reject an invalid configuration", func(t *testing.T) {
		cfg := testConfig()
		cfg.FlushMaxRecords = 0
		cfg.Group = ""
		if _, err := NewGroupMembers(cfg, 1, proc, &fakePositions{}, logger, newRegistry()); err == nil {
			t.Fatal("NewGroupMembers() error = nil, want a config error")
		}
	})

	t.Run("should reject a nil position loader", func(t *testing.T) {
		if _, err := NewGroupMembers(testConfig(), 1, proc, nil, logger, newRegistry()); err == nil {
			t.Fatal("NewGroupMembers() error = nil, want an error")
		}
	})

	t.Run("should reject a non-positive member count", func(t *testing.T) {
		if _, err := NewGroupMembers(testConfig(), 0, proc, &fakePositions{}, logger, newRegistry()); err == nil {
			t.Fatal("NewGroupMembers() error = nil, want an error")
		}
	})

	t.Run("should return n members with lag polling on the first only", func(t *testing.T) {
		ms, err := NewGroupMembers(testConfig(), 3, proc, &fakePositions{}, logger, newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		if len(ms) != 3 || !ms[0].pollLag || ms[1].pollLag || ms[2].pollLag {
			t.Fatalf("members = %d, lag pollers = %v %v %v", len(ms), ms[0].pollLag, ms[1].pollLag, ms[2].pollLag)
		}
	})

	t.Run("should fail instead of panicking when metrics are registered twice", func(t *testing.T) {
		reg := newRegistry()
		if _, err := NewGroupMembers(testConfig(), 1, proc, &fakePositions{}, logger, reg); err != nil {
			t.Fatal(err)
		}
		if _, err := NewGroupMembers(testConfig(), 1, proc, &fakePositions{}, logger, reg); err == nil {
			t.Fatal("second NewGroupMembers() on the same registry error = nil, want an error")
		}
	})
}
