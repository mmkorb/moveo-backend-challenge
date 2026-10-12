package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

func TestFlushTimeout(t *testing.T) {
	t.Run("should time out a hanging attempt, count a transient retry and then succeed", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			cfg.FlushTimeout = 3 * time.Second
			cfg.Retry = retryPolicy(100 * time.Millisecond)
			hang := make(chan struct{}) // never closed: a server that does not answer
			attempts := 0
			proc := &blockingProcessor{first: func(ctx context.Context) error {
				attempts++
				if attempts == 1 {
					select {
					case <-hang:
					case <-ctx.Done():
						return fmt.Errorf("begin flush transaction: %w", ctx.Err())
					}
				}
				return nil
			}}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			assign(m, gc, 0)

			start := time.Now()
			m.dispatch(0, validRecords(0, 0, 2))
			synctest.Wait()
			if _, ok := gc.mark(0); ok {
				t.Fatal("marked while the first attempt still hangs")
			}
			time.Sleep(cfg.FlushTimeout + time.Second)
			synctest.Wait()

			if attempts != 2 {
				t.Fatalf("attempts = %d, want 2 (timed out, then succeeded)", attempts)
			}
			if off, ok := gc.mark(0); !ok || off != 1 {
				t.Fatalf("marked offset = %d (%v), want 1", off, ok)
			}
			if got := metricValue(t, m.metrics.retries.WithLabelValues(dependencyPostgres, errorClassTransient)); got != 1 {
				t.Errorf("retries_total{postgres,transient} = %v, want 1", got)
			}
			if elapsed := proc.firstReturn.Sub(start); elapsed != cfg.FlushTimeout {
				t.Errorf("first attempt returned after %v, want FLUSH_TIMEOUT %v", elapsed, cfg.FlushTimeout)
			}
			if !strings.Contains(logs.String(), `"error_class":"transient"`) {
				t.Errorf("retry log without the transient class: %s", logs)
			}
			stopAll(t, m)
		})
	})

	t.Run("should not mark the worker's own cancellation as a timeout", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		err := withAttemptTimeout(ctx, time.Hour, func(context.Context) error {
			cancel()
			return context.Canceled
		})
		if errors.Is(err, errAttemptTimeout) || !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want plain context.Canceled", err)
		}
	})

	t.Run("should keep ErrFenced visible through the timeout wrapper", func(t *testing.T) {
		err := withAttemptTimeout(t.Context(), time.Nanosecond, func(ctx context.Context) error {
			<-ctx.Done()
			return fmt.Errorf("apply: %w", ports.ErrFenced)
		})
		if !errors.Is(err, ports.ErrFenced) || !errors.Is(err, errAttemptTimeout) {
			t.Errorf("error = %v, want ErrFenced and errAttemptTimeout in the chain", err)
		}
		if retryClass(err) != errorClassTransient {
			t.Errorf("retryClass = %q, want transient", retryClass(err))
		}
	})

	t.Run("should time out a hanging position load and retry it", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushTimeout = time.Second
			cfg.RevokeFlushTimeout = 10 * time.Second
			cfg.Retry = retryPolicy(10 * time.Millisecond)
			m, _ := newTestMember(t, cfg, &fakeProcessor{})
			calls := 0
			m.positions = positionsFunc(func(ctx context.Context) (map[int32]int64, error) {
				calls++
				if calls == 1 {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return map[int32]int64{0: 5}, nil
			})

			got, err := m.loadPositions(t.Context(), []int32{0})

			if err != nil || got[0] != 5 {
				t.Fatalf("loadPositions = %v, %v; want {0:5}", got, err)
			}
			if got := metricValue(t, m.metrics.retries.WithLabelValues(dependencyPostgres, errorClassTransient)); got != 1 {
				t.Errorf("retries_total{postgres,transient} = %v, want 1", got)
			}
		})
	})
}

func TestFenceResync(t *testing.T) {
	fencedOnce := func(n int) error {
		if n == 0 {
			return fmt.Errorf("apply deltas: %w: stored next offset 9, expected 4", ports.ErrFenced)
		}
		return nil
	}

	t.Run("should resume a fenced partition from the reloaded stored position", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			proc := &fakeProcessor{fail: fencedOnce}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			positions := &fakePositions{stored: map[int32]int64{0: 4}}
			m.positions = positions
			assign(m, gc, 0)
			if _, err := adjust(t, m, map[int32]int64{0: 4}); err != nil {
				t.Fatal(err)
			}

			m.dispatch(0, validRecords(0, 4, 2)) // fenced: a stale owner moved the position to 9
			synctest.Wait()
			if !gc.isPaused(0) {
				t.Fatal("partition not paused after the fence")
			}
			pollCtx, cancel := m.pollContext(t.Context())
			if pollCtx.Err() == nil {
				t.Error("poll context not cancelled although a resync is due")
			}
			cancel()
			m.disarmWake()

			positions.mu.Lock()
			positions.stored[0] = 9
			positions.mu.Unlock()
			m.resyncFenced(t.Context(), gc)
			synctest.Wait()

			if seeks := gc.seekCalls(); len(seeks) != 1 || seeks[0][0] != 9 {
				t.Fatalf("SetOffsets calls = %v, want one seek of partition 0 to 9", seeks)
			}
			if gc.isPaused(0) {
				t.Error("partition still paused after the resync")
			}
			if off, ok := gc.recordCommit(0); !ok || off != 9 {
				t.Errorf("Kafka commit of the stored position = %d (%v), want 9", off, ok)
			}
			if got := metricValue(t, m.metrics.resyncs); got != 1 {
				t.Errorf("offset_fence_resyncs_total = %v, want 1", got)
			}
			if !strings.Contains(logs.String(), "partition resumed from the stored position after a fence") {
				t.Error("missing the resync log record")
			}

			m.dispatch(0, validRecords(0, 9, 2))
			synctest.Wait()
			calls := proc.snapshot()
			if len(calls) != 2 {
				t.Fatalf("Process called %d times, want 2", len(calls))
			}
			want := ports.Checkpoint{Group: cfg.Group, Topic: testTopic, Partition: 0, From: 9, To: 11}
			if calls[1].cp != want {
				t.Errorf("checkpoint after the resync = %+v, want %+v", calls[1].cp, want)
			}
			if off, ok := gc.mark(0); !ok || off != 10 {
				t.Errorf("marked offset = %d (%v), want 10", off, ok)
			}
			if pending := m.dueResyncs(); len(pending) != 0 {
				t.Errorf("resyncs still pending: %v", pending)
			}
			stopAll(t, m)
		})
	})

	t.Run("should not resync a partition revoked in the meantime", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			proc := &fakeProcessor{fail: fencedOnce}
			gc := newFakeGroupClient()
			m, _ := newTestMember(t, cfg, proc)
			m.positions = &fakePositions{stored: map[int32]int64{0: 4}}
			assign(m, gc, 0)
			if _, err := adjust(t, m, map[int32]int64{0: 4}); err != nil {
				t.Fatal(err)
			}
			m.dispatch(0, validRecords(0, 4, 2))
			synctest.Wait()

			m.onRevoked(t.Context(), gc, map[string][]int32{testTopic: {0}})
			m.resyncFenced(t.Context(), gc)

			if seeks := gc.seekCalls(); len(seeks) != 0 {
				t.Errorf("SetOffsets calls = %v, want none for a revoked partition", seeks)
			}
			m.mu.Lock()
			n := len(m.workers)
			m.mu.Unlock()
			if n != 0 {
				t.Errorf("%d workers after the revoke, want 0", n)
			}
		})
	})

	t.Run("should retry the resync later when the stored position cannot be loaded", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			cfg.RevokeFlushTimeout = time.Second
			cfg.Retry = retryPolicy(100 * time.Millisecond)
			proc := &fakeProcessor{fail: fencedOnce}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			positions := &fakePositions{stored: map[int32]int64{0: 4}}
			m.positions = positions
			assign(m, gc, 0)
			if _, err := adjust(t, m, map[int32]int64{0: 4}); err != nil {
				t.Fatal(err)
			}
			m.dispatch(0, validRecords(0, 4, 2))
			synctest.Wait()

			positions.mu.Lock()
			positions.err = fmt.Errorf("%w: connection refused", ports.ErrUnavailable)
			positions.mu.Unlock()
			m.resyncFenced(t.Context(), gc)
			if len(gc.seekCalls()) != 0 {
				t.Fatal("seeked without a stored position")
			}
			if !strings.Contains(logs.String(), "cannot reload stored positions after a fence") {
				t.Error("missing the failed-resync log record")
			}

			pollCtx, cancel := m.pollContext(t.Context())
			if pollCtx.Err() != nil {
				t.Error("poll context cancelled at once although the retry is not due yet")
			}
			deadline, ok := pollCtx.Deadline()
			if !ok || time.Until(deadline) > cfg.Retry.Max {
				t.Errorf("poll deadline = %v (%v), want within RETRY_MAX_DELAY", deadline, ok)
			}
			cancel()
			m.disarmWake()
			m.resyncFenced(t.Context(), gc) // not due yet: no load
			if len(gc.seekCalls()) != 0 {
				t.Fatal("resync ran before it was due")
			}

			time.Sleep(cfg.Retry.Max)
			positions.mu.Lock()
			positions.err = nil
			positions.stored[0] = 9
			positions.mu.Unlock()
			m.resyncFenced(t.Context(), gc)
			if seeks := gc.seekCalls(); len(seeks) != 1 || seeks[0][0] != 9 {
				t.Fatalf("SetOffsets calls = %v, want one seek to 9", seeks)
			}
			stopAll(t, m)
		})
	})

	t.Run("should keep the back-off of a failed resync when another partition is fenced", func(t *testing.T) {
		cfg := testConfig()
		cfg.Retry = retryPolicy(time.Minute)
		m, _ := newTestMember(t, cfg, &fakeProcessor{})
		m.retryResyncs([]int32{0}) // partition 0: position load failed, due in RETRY_MAX_DELAY

		m.requestResync(1) // partition 1 is fenced now

		pollCtx, cancel := m.pollContext(t.Context())
		defer cancel()
		if pollCtx.Err() == nil {
			t.Error("poll context not cancelled although partition 1 is due")
		}
		if got := m.dueResyncs(); !slices.Equal(got, []int32{1}) {
			t.Errorf("due resyncs = %v, want only [1]", got)
		}
		m.resyncMu.Lock()
		at, pending := m.resync[0]
		m.resyncMu.Unlock()
		if !pending || at.IsZero() {
			t.Errorf("partition 0 pending = %v, due %v, want its back-off kept", pending, at)
		}
		if got := m.dueResyncs(); len(got) != 0 {
			t.Errorf("due resyncs = %v, want none before the back-off ends", got)
		}
	})

	t.Run("should leave the partition stopped when no stored position exists", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.FlushMaxRecords = 2
			proc := &fakeProcessor{fail: fencedOnce}
			gc := newFakeGroupClient()
			m, logs := newTestMember(t, cfg, proc)
			positions := &fakePositions{stored: map[int32]int64{0: 4}}
			m.positions = positions
			assign(m, gc, 0)
			if _, err := adjust(t, m, map[int32]int64{0: 4}); err != nil {
				t.Fatal(err)
			}
			m.dispatch(0, validRecords(0, 4, 2))
			synctest.Wait()

			positions.mu.Lock()
			delete(positions.stored, 0)
			positions.mu.Unlock()
			m.resyncFenced(t.Context(), gc)

			if len(gc.seekCalls()) != 0 {
				t.Error("seeked without a stored position")
			}
			if !gc.isPaused(0) {
				t.Error("partition resumed without a stored position")
			}
			if !strings.Contains(logs.String(), "no stored position after a fence") {
				t.Error("missing the log record")
			}
		})
	})

	t.Run("should wake a waiting poll when a worker is fenced", func(t *testing.T) {
		m, _ := newTestMember(t, testConfig(), &fakeProcessor{})
		pollCtx, cancel := m.pollContext(t.Context())
		defer cancel()
		if pollCtx.Err() != nil {
			t.Fatal("poll context cancelled without a resync")
		}
		m.requestResync(3)
		if pollCtx.Err() == nil {
			t.Error("requestResync did not cancel the waiting poll")
		}
	})
}

func TestStaticMembership(t *testing.T) {
	t.Run("should derive one instance ID per member and none without CONSUMER_INSTANCE_ID", func(t *testing.T) {
		cfg := testConfig()
		cfg.InstanceID = "host-a"
		members, err := NewGroupMembers(cfg, 3, &fakeProcessor{}, &fakePositions{}, discardLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		for i, m := range members {
			if got, want := m.instanceID(), fmt.Sprintf("host-a-%d", i); got != want {
				t.Errorf("member %d instance ID = %q, want %q", i, got, want)
			}
		}
		dynamic, _ := newTestMember(t, testConfig(), &fakeProcessor{})
		if id := dynamic.instanceID(); id != "" {
			t.Errorf("instance ID without CONSUMER_INSTANCE_ID = %q, want empty", id)
		}
		if got := members[0].cfg.SessionTimeout; got != defaultSessionTimeout {
			t.Errorf("session timeout = %v, want the default %v", got, defaultSessionTimeout)
		}
	})

	t.Run("should send a LeaveGroup with the member and instance IDs", func(t *testing.T) {
		rq := &fakeRequestor{resp: &kmsg.LeaveGroupResponse{Members: []kmsg.LeaveGroupResponseMember{{MemberID: "m-1"}}}}
		if err := leaveStatic(t.Context(), rq, "g", "m-1", "host-a-0"); err != nil {
			t.Fatalf("leaveStatic error = %v", err)
		}
		req, ok := rq.req.(*kmsg.LeaveGroupRequest)
		if !ok {
			t.Fatalf("request = %T, want *kmsg.LeaveGroupRequest", rq.req)
		}
		if req.Group != "g" || len(req.Members) != 1 || req.Members[0].MemberID != "m-1" ||
			req.Members[0].InstanceID == nil || *req.Members[0].InstanceID != "host-a-0" {
			t.Errorf("request = %+v, want group g, member m-1, instance host-a-0", req)
		}
	})

	t.Run("should report a member-level error", func(t *testing.T) {
		rq := &fakeRequestor{resp: &kmsg.LeaveGroupResponse{Members: []kmsg.LeaveGroupResponseMember{{ErrorCode: kerr.FencedInstanceID.Code}}}}
		if err := leaveStatic(t.Context(), rq, "g", "m-1", "host-a-0"); !errors.Is(err, kerr.FencedInstanceID) {
			t.Errorf("error = %v, want FENCED_INSTANCE_ID", err)
		}
	})

	t.Run("should report a group-level error and a transport error", func(t *testing.T) {
		rq := &fakeRequestor{resp: &kmsg.LeaveGroupResponse{ErrorCode: kerr.CoordinatorNotAvailable.Code}}
		if err := leaveStatic(t.Context(), rq, "g", "m-1", "host-a-0"); !errors.Is(err, kerr.CoordinatorNotAvailable) {
			t.Errorf("error = %v, want COORDINATOR_NOT_AVAILABLE", err)
		}
		boom := errors.New("connection reset")
		if err := leaveStatic(t.Context(), &fakeRequestor{err: boom}, "g", "m-1", "host-a-0"); !errors.Is(err, boom) {
			t.Errorf("error = %v, want the transport error", err)
		}
	})

	t.Run("should send nothing for a member that never joined", func(t *testing.T) {
		rq := &fakeRequestor{}
		if err := leaveStatic(t.Context(), rq, "g", "", "host-a-0"); err != nil || rq.req != nil {
			t.Errorf("leaveStatic = %v, request %v; want nil and no request", err, rq.req)
		}
	})
}

// blockingProcessor runs first for every Process call and records when the
// first call returned.
type blockingProcessor struct {
	first       func(ctx context.Context) error
	firstReturn time.Time
	calls       int
}

func (p *blockingProcessor) Process(ctx context.Context, _ ports.Checkpoint, _ []domain.ActivityEvent, _ []ports.DeadLetter) error {
	err := p.first(ctx)
	p.calls++
	if p.calls == 1 {
		p.firstReturn = time.Now()
	}
	return err
}

// positionsFunc adapts a function to PositionLoader.
type positionsFunc func(ctx context.Context) (map[int32]int64, error)

func (f positionsFunc) NextOffsets(ctx context.Context, _, _ string, _ []int32) (map[int32]int64, error) {
	return f(ctx)
}

// fakeRequestor answers every request with resp or err.
type fakeRequestor struct {
	req  kmsg.Request
	resp kmsg.Response
	err  error
}

func (f *fakeRequestor) Request(_ context.Context, req kmsg.Request) (kmsg.Response, error) {
	f.req = req
	return f.resp, f.err
}
