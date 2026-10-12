//go:build integration

package kafka

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ownedPartitions is the number of partitions m currently has a worker for.
func ownedPartitions(m *Member) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.workers)
}

// recoveryBound is far below the session timeout of these tests: a handover
// that waited for the session to expire would exceed it.
const (
	itSessionTimeout = 30 * time.Second
	recoveryBound    = 10 * time.Second
)

func TestIntegrationStaticMembership(t *testing.T) {
	t.Run("should take the partitions of a killed member back at once when it restarts with the same instance ID", func(t *testing.T) {
		cfg := itConfig(t, 2)
		cfg.InstanceID = "it-kill-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		cfg.SessionTimeout = itSessionTimeout
		const n = 40
		prod := newProducer(t)
		var recs []*kgo.Record
		for i := range n {
			recs = append(recs, activity(cfg.Topic, "user-"+strconv.Itoa(i%7), i))
		}
		produce(t, prod, recs...)

		// The "killed" process: a static member that joins, consumes and then
		// disappears without a LeaveGroup (franz-go sends none for an instance
		// ID), like a SIGKILL. It commits nothing.
		dead, err := kgo.NewClient(
			kgo.SeedBrokers(cfg.Brokers...),
			kgo.ConsumerGroup(cfg.Group),
			kgo.ConsumeTopics(cfg.Topic),
			kgo.InstanceID(cfg.InstanceID+"-0"),
			kgo.SessionTimeout(cfg.SessionTimeout),
			kgo.Balancers(kgo.CooperativeStickyBalancer()),
			kgo.DisableAutoCommit(),
		)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
		for seen := 0; seen < n && ctx.Err() == nil; {
			seen += len(dead.PollFetches(ctx).Records())
		}
		cancel()
		dead.Close()

		col := newCollector(nil)
		ms, err := NewGroupMembers(cfg, 1, col, col, itLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		stop := runMember(t, ms[0])
		waitFor(t, itSessionTimeout, "the restarted member to own both partitions", func() bool {
			return ms[0].Joined() && ownedPartitions(ms[0]) == 2
		})
		took := time.Since(start)
		t.Logf("restarted static member owned its partitions after %v (session timeout %v)", took, cfg.SessionTimeout)
		if took > recoveryBound {
			t.Errorf("restart took %v to get the partitions back, want < %v (no wait for the session timeout)", took, recoveryBound)
		}
		waitFor(t, 60*time.Second, "all records", func() bool { d, _, _, _ := col.stats(n); return d == n })
		if err := stop(); err != nil {
			t.Errorf("Run() = %v", err)
		}
		if _, total, dups, missing := col.stats(n); total != n || dups != 0 || len(missing) != 0 {
			t.Errorf("total %d, dups %d, missing %v; want each record once", total, dups, missing)
		}
	})

	t.Run("should hand the partitions of a cleanly stopped static member over at once", func(t *testing.T) {
		cfg := itConfig(t, 2)
		cfg.InstanceID = "it-stop-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		cfg.SessionTimeout = itSessionTimeout
		col := newCollector(nil)
		ms, err := NewGroupMembers(cfg, 2, col, col, itLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		stopA := runMember(t, ms[0])
		stopB := runMember(t, ms[1])
		waitFor(t, 60*time.Second, "one partition per member", func() bool {
			return ownedPartitions(ms[0]) == 1 && ownedPartitions(ms[1]) == 1
		})

		start := time.Now()
		if err := stopA(); err != nil {
			t.Fatalf("member A Run() = %v, want a clean stop including the explicit leave", err)
		}
		waitFor(t, itSessionTimeout, "member B to own both partitions", func() bool { return ownedPartitions(ms[1]) == 2 })
		took := time.Since(start)
		t.Logf("partitions handed over %v after the stop began (session timeout %v)", took, cfg.SessionTimeout)
		if took > recoveryBound {
			t.Errorf("handover took %v, want < %v (the stopped member must leave explicitly)", took, recoveryBound)
		}

		const n = 20
		prod := newProducer(t)
		var recs []*kgo.Record
		for i := range n {
			recs = append(recs, activity(cfg.Topic, "user-"+strconv.Itoa(i%5), i))
		}
		produce(t, prod, recs...)
		waitFor(t, 60*time.Second, "all records", func() bool { d, _, _, _ := col.stats(n); return d == n })
		if err := stopB(); err != nil {
			t.Errorf("member B Run() = %v", err)
		}
	})
}

func TestIntegrationFenceRecovery(t *testing.T) {
	t.Run("should resume a fenced partition from the stored position without a rebalance", func(t *testing.T) {
		cfg := itConfig(t, 1)
		prod := newProducer(t)
		col := newCollector(nil)
		ms, err := NewGroupMembers(cfg, 1, col, col, itLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		stop := runMember(t, ms[0])
		batch := func(from, to int) []*kgo.Record {
			var recs []*kgo.Record
			for i := from; i < to; i++ {
				recs = append(recs, activity(cfg.Topic, "user-"+strconv.Itoa(i%3), i))
			}
			return recs
		}
		produce(t, prod, batch(0, 100)...)
		waitFor(t, 60*time.Second, "the first 100 records", func() bool {
			next, _ := col.NextOffsets(t.Context(), cfg.Group, cfg.Topic, []int32{0})
			return next[0] == 100
		})

		// A stale owner applies offsets 100..116 and moves the stored position
		// to 117 behind the rightful owner's back.
		col.mu.Lock()
		col.positions[0] = 117
		for i := 100; i < 117; i++ {
			col.seen[strconv.Itoa(i)]++
			col.events++
		}
		col.mu.Unlock()

		const n = 200
		produce(t, prod, batch(100, n)...)
		waitFor(t, 60*time.Second, "the rest of the records", func() bool { d, _, _, _ := col.stats(n); return d == n })
		waitFor(t, 30*time.Second, "the stored position at the end", func() bool {
			next, _ := col.NextOffsets(t.Context(), cfg.Group, cfg.Topic, []int32{0})
			return next[0] == n
		})
		waitFor(t, 30*time.Second, "the Kafka commit of the end offset", func() bool { return committed(t, cfg)[0] == n })

		_, total, dups, missing := col.stats(n)
		if total != n || dups != 0 || len(missing) != 0 {
			t.Errorf("total %d, dups %d, missing %v; want each record once", total, dups, missing)
		}
		col.mu.Lock()
		fenced := col.fenced
		col.mu.Unlock()
		if fenced < 1 {
			t.Errorf("fenced batches = %d, want >= 1", fenced)
		}
		if got := metricValue(t, ms[0].metrics.resyncs); got < 1 {
			t.Errorf("offset_fence_resyncs_total = %v, want >= 1", got)
		}
		if ownedPartitions(ms[0]) != 1 {
			t.Error("the member no longer owns the partition")
		}
		if err := stop(); err != nil {
			t.Errorf("Run() = %v", err)
		}
	})
}
