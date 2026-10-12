package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// adjust runs the AdjustFetchOffsetsFn callback for partitions with the given
// Kafka committed offsets (a negative value means "nothing committed").
func adjust(t *testing.T, m *Member, committed map[int32]int64) (map[int32]kgo.Offset, error) {
	t.Helper()
	in := map[int32]kgo.Offset{}
	for p, o := range committed {
		if o < 0 {
			in[p] = kgo.NewOffset().AtStart()
		} else {
			in[p] = kgo.NewOffset().At(o)
		}
	}
	out, err := m.adjustOffsets(t.Context(), map[string]map[int32]kgo.Offset{testTopic: in})
	return out[testTopic], err
}

func TestPartitionWorkerCheckpoints(t *testing.T) {
	t.Run("should create the position on the first batch and advance it from there", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 3
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			assign(m, gc, 4)
			if _, err := adjust(t, m, map[int32]int64{4: -1}); err != nil {
				t.Fatal(err)
			}

			m.dispatch(4, validRecords(4, 10, 3))
			synctest.Wait()
			m.dispatch(4, validRecords(4, 13, 3))
			synctest.Wait()

			calls := proc.snapshot()
			want := []ports.Checkpoint{
				{Group: cfg.Group, Topic: testTopic, Partition: 4, From: ports.NoPosition, To: 13},
				{Group: cfg.Group, Topic: testTopic, Partition: 4, From: 13, To: 16},
			}
			if len(calls) != len(want) {
				t.Fatalf("Process called %d times, want %d", len(calls), len(want))
			}
			for i, c := range calls {
				if c.cp != want[i] {
					t.Errorf("checkpoint %d = %+v, want %+v", i, c.cp, want[i])
				}
			}
			if _, ok := gc.recordCommit(4); ok {
				t.Error("committed a stored position although none existed")
			}
			stopAll(t, m)
		})
	})

	t.Run("should advance the position past dead letters", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			m.positions = &fakePositions{stored: map[int32]int64{0: 7}}
			assign(m, gc, 0)
			if _, err := adjust(t, m, map[int32]int64{0: 7}); err != nil {
				t.Fatal(err)
			}

			m.dispatch(0, []*kgo.Record{record(0, 7, []byte("not json")), record(0, 8, []byte("{}"))})
			synctest.Wait()

			calls := proc.snapshot()
			if len(calls) != 1 || len(calls[0].events) != 0 || len(calls[0].rejected) != 2 {
				t.Fatalf("Process calls = %+v, want one call with 2 dead letters", calls)
			}
			if cp := calls[0].cp; cp.From != 7 || cp.To != 9 {
				t.Errorf("checkpoint = %+v, want 7 -> 9", cp)
			}
			if off, _ := gc.mark(0); off != 8 {
				t.Errorf("marked offset = %d, want 8", off)
			}
			stopAll(t, m)
		})
	})
}

func TestAdjustOffsets(t *testing.T) {
	t.Run("should resume from the stored position and commit it to Kafka when Kafka is behind", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 3
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			m.positions = &fakePositions{stored: map[int32]int64{0: 50}}
			assign(m, gc, 0, 1)

			got, err := adjust(t, m, map[int32]int64{0: 40, 1: 12})
			if err != nil {
				t.Fatalf("adjustOffsets() error = %v", err)
			}
			if o := got[0].EpochOffset(); o.Offset != 50 || o.Epoch != -1 {
				t.Errorf("partition 0 offset = %+v, want 50 with epoch -1", o)
			}
			if o := got[1].EpochOffset(); o.Offset != 12 {
				t.Errorf("partition 1 offset = %+v, want the committed 12 (no stored position)", o)
			}
			synctest.Wait()
			if off, ok := gc.recordCommit(0); !ok || off != 50 {
				t.Errorf("Kafka commit for partition 0 = %d (%v), want the stored 50", off, ok)
			}
			if _, ok := gc.recordCommit(1); ok {
				t.Error("committed partition 1, which has no stored position")
			}

			// Records below the stored position are dropped, the rest form the batch.
			m.dispatch(0, validRecords(0, 48, 5))
			synctest.Wait()
			calls := proc.snapshot()
			if len(calls) != 1 || len(calls[0].events) != 3 {
				t.Fatalf("Process calls = %+v, want one call with offsets 50..52", calls)
			}
			if cp := calls[0].cp; cp.From != 50 || cp.To != 53 {
				t.Errorf("checkpoint = %+v, want 50 -> 53", cp)
			}
			if !strings.Contains(logs.String(), "dropped records below the stored position") {
				t.Error("missing the log record for dropped records")
			}
			stopAll(t, m)
		})
	})

	t.Run("should resume from the stored position without a commit when Kafka is ahead", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, testConfig(), &fakeProcessor{})
			m.positions = &fakePositions{stored: map[int32]int64{2: 30}}
			assign(m, gc, 2)

			got, err := adjust(t, m, map[int32]int64{2: 35})
			if err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if o := got[2].EpochOffset().Offset; o != 30 {
				t.Errorf("partition 2 offset = %d, want the stored 30", o)
			}
			if _, ok := gc.recordCommit(2); ok {
				t.Error("committed although Kafka was ahead")
			}
			if !strings.Contains(logs.String(), "Kafka committed offset is ahead of the stored position") {
				t.Error("missing the warning")
			}
			stopAll(t, m)
		})
	})

	t.Run("should commit the stored position when nothing is committed in Kafka", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, testConfig(), &fakeProcessor{})
			m.positions = &fakePositions{stored: map[int32]int64{3: 9}}
			assign(m, gc, 3)

			if _, err := adjust(t, m, map[int32]int64{3: -1}); err != nil {
				t.Fatal(err)
			}
			synctest.Wait()
			if off, ok := gc.recordCommit(3); !ok || off != 9 {
				t.Errorf("Kafka commit = %d (%v), want 9", off, ok)
			}
			stopAll(t, m)
		})
	})

	t.Run("should keep consuming when the Kafka commit of the stored position fails", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 1
			proc := &fakeProcessor{}
			gc := newFakeGroupClient()
			gc.recordCommitErr = errors.New("coordinator not available")
			m, logs := newTestMember(t, cfg, proc)
			m.positions = &fakePositions{stored: map[int32]int64{0: 5}}
			assign(m, gc, 0)

			if _, err := adjust(t, m, map[int32]int64{0: 1}); err != nil {
				t.Fatal(err)
			}
			m.dispatch(0, validRecords(0, 5, 1))
			synctest.Wait()

			if !strings.Contains(logs.String(), "could not commit the stored position to Kafka") {
				t.Error("missing the warning")
			}
			if off, ok := gc.mark(0); !ok || off != 5 {
				t.Errorf("marked offset = %d (%v), want 5", off, ok)
			}
			stopAll(t, m)
		})
	})

	t.Run("should not load anything for an assignment without partitions of the topic", func(t *testing.T) {
		m, _ := newTestMember(t, testConfig(), &fakeProcessor{})
		pos := &fakePositions{}
		m.positions = pos
		in := map[string]map[int32]kgo.Offset{"other": {0: kgo.NewOffset().At(1)}}

		out, err := m.adjustOffsets(t.Context(), in)

		if err != nil || pos.callCount() != 0 || out["other"][0].EpochOffset().Offset != 1 {
			t.Errorf("adjustOffsets() = %v, %v with %d loads, want the input unchanged and no load", out, err, pos.callCount())
		}
	})

	t.Run("should retry a failed load and give the partitions up after REVOKE_FLUSH_TIMEOUT", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.RevokeFlushTimeout = 5 * time.Second
			cfg.Retry = retryPolicy(time.Second)
			m, logs := newTestMember(t, cfg, &fakeProcessor{})
			pos := &fakePositions{err: fmt.Errorf("%w: connection refused", ports.ErrUnavailable)}
			m.positions = pos
			assign(m, newFakeGroupClient(), 0)

			start := time.Now()
			_, err := adjust(t, m, map[int32]int64{0: 1})

			if !errors.Is(err, ports.ErrUnavailable) {
				t.Fatalf("adjustOffsets() error = %v, want ErrUnavailable", err)
			}
			if took := time.Since(start); took > cfg.RevokeFlushTimeout {
				t.Errorf("load took %v, want at most %v", took, cfg.RevokeFlushTimeout)
			}
			if pos.callCount() < 2 {
				t.Errorf("loads = %d, want retries", pos.callCount())
			}
			if got := metricValue(t, m.metrics.retries.WithLabelValues(dependencyPostgres, errorClassTransient)); got < 1 {
				t.Errorf("retries_total{postgres,transient} = %v, want > 0", got)
			}
			if !strings.Contains(logs.String(), "cannot load stored positions") || strings.Contains(logs.String(), "connection refused") {
				t.Errorf("logs = %s, want the give-up record without the error text", logs)
			}
			stopAll(t, m)
		})
	})
}

func TestPartitionWorkerFence(t *testing.T) {
	t.Run("should drop the batch, stop the worker and pause the partition when fenced", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			proc := &fakeProcessor{fail: func(int) error {
				return fmt.Errorf("apply deltas: %w: stored next offset 9, expected 4", ports.ErrFenced)
			}}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			m.positions = &fakePositions{stored: map[int32]int64{0: 4}}
			assign(m, gc, 0)
			if _, err := adjust(t, m, map[int32]int64{0: 4}); err != nil {
				t.Fatal(err)
			}

			m.dispatch(0, validRecords(0, 4, 2))
			time.Sleep(time.Minute) // a retry would happen within this time
			synctest.Wait()

			if n := len(proc.snapshot()); n != 1 {
				t.Errorf("Process called %d times, want 1 (no retry when fenced)", n)
			}
			if _, ok := gc.mark(0); ok {
				t.Error("marked a fenced batch")
			}
			if _, n := gc.lastCommit(); n != 0 {
				t.Errorf("%d commits after a fence, want 0", n)
			}
			if got := metricValue(t, m.metrics.fenced); got != 1 {
				t.Errorf("offset_fence_rejections_total = %v, want 1", got)
			}
			if got := metricValue(t, m.metrics.retries.WithLabelValues(dependencyPostgres, errorClassUnknown)); got != 0 {
				t.Errorf("retries_total{postgres,unknown} = %v, want 0", got)
			}
			if !gc.isPaused(0) {
				t.Error("partition not paused after the fence")
			}
			if !strings.Contains(logs.String(), "stored position moved by another owner") {
				t.Error("missing the fence log record")
			}

			m.mu.Lock()
			w := m.workers[0]
			m.mu.Unlock()
			select {
			case <-w.done:
			default:
				t.Fatal("worker still running after the fence")
			}
			m.dispatch(0, validRecords(0, 6, 1)) // dropped: the worker is sealed
			synctest.Wait()
			if n := len(proc.snapshot()); n != 1 {
				t.Errorf("Process called %d times after the fence, want 1", n)
			}

			// The rebalance that takes the partition away clears the pause.
			m.onRevoked(t.Context(), gc, map[string][]int32{testTopic: {0}})
			if gc.isPaused(0) {
				t.Error("partition still paused after the revoke")
			}
			if _, ok := gc.mark(0); ok {
				t.Error("revoke marked an offset of a fenced partition")
			}
		})
	})
}

func TestAdjustOffsetsLogsNoCommittedOffsetAsAbsent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gc := newFakeGroupClient()
		m, logs := newTestMember(t, testConfig(), &fakeProcessor{})
		m.positions = &fakePositions{stored: map[int32]int64{}}
		assign(m, gc, 0, 1)

		if _, err := adjust(t, m, map[int32]int64{0: -2, 1: 12}); err != nil {
			t.Fatalf("adjustOffsets() error = %v", err)
		}
		synctest.Wait()

		byPartition := map[float64]map[string]any{}
		for line := range strings.SplitSeq(strings.TrimSpace(logs.String()), "\n") {
			var rec map[string]any
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				t.Fatalf("log line %q is not JSON: %v", line, err)
			}
			if rec["msg"] == "no stored position; resuming from the Kafka committed offset" {
				byPartition[rec["partition"].(float64)] = rec
			}
		}
		if rec := byPartition[0]; rec == nil || rec["has_committed_offset"] != false || rec["committed_offset"] != nil {
			t.Errorf("partition 0 log = %v, want has_committed_offset=false and no committed_offset", rec)
		}
		if rec := byPartition[1]; rec == nil || rec["has_committed_offset"] != true || rec["committed_offset"] != float64(12) {
			t.Errorf("partition 1 log = %v, want has_committed_offset=true and committed_offset=12", rec)
		}
		stopAll(t, m)
	})
}
