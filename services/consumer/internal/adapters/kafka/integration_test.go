//go:build integration

package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

const (
	kafkaImage   = "apache/kafka:4.3.1"
	externalPort = "29092/tcp"
	envFile      = "/tmp/advertised.env"
)

// brokers is the seed list of the single Kafka container shared by the tests
// of this package (one container per package; run the packages with -p 1 to
// keep the host responsive).
var brokers []string

func TestMain(m *testing.M) {
	code := runWithKafka(m)
	if code == 0 {
		// No goroutine of ours may outlive the tests. The ignored functions
		// are process-lifetime goroutines of the Docker client used by
		// testcontainers (named pipe I/O on Windows, HTTP keep-alive, reaper).
		if err := goleak.Find(
			goleak.IgnoreAnyFunction("github.com/Microsoft/go-winio.ioCompletionProcessor"),
			goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
			goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
			goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		); err != nil {
			fmt.Fprintln(os.Stderr, "goleak:", err)
			code = 1
		}
	}
	os.Exit(code)
}

func runWithKafka(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	ctr, err := startKafka(ctx)
	if ctr != nil {
		defer func() {
			if err := ctr.Terminate(context.Background()); err != nil {
				fmt.Fprintln(os.Stderr, "terminate kafka:", err)
			}
		}()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "start kafka:", err)
		return 1
	}
	ep, err := ctr.PortEndpoint(ctx, externalPort, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, "kafka endpoint:", err)
		return 1
	}
	brokers = []string{ep}
	return m.Run()
}

// startKafka runs one single-node KRaft broker with the compose settings
// (ADR 0015) and fixed memory and CPU limits. The advertised
// EXTERNAL address is only known after start, so the container waits for an
// env file that the post-start hook writes.
func startKafka(ctx context.Context) (testcontainers.Container, error) {
	return testcontainers.Run(ctx, kafkaImage,
		testcontainers.WithExposedPorts(externalPort),
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_LISTENERS":                                "INTERNAL://:9092,EXTERNAL://:29092,CONTROLLER://:9093",
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "INTERNAL",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":             "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_HEAP_OPTS":                                "-Xms384m -Xmx512m",
		}),
		testcontainers.WithEntrypoint("bash"),
		testcontainers.WithCmd("-c", "while [ ! -f "+envFile+" ]; do sleep 0.1; done; . "+envFile+"; exec /etc/kafka/docker/run"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.Memory = 1 << 30
			hc.NanoCPUs = 1_500_000_000
		}),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, c testcontainers.Container) error {
					if err := wait.ForMappedPort(externalPort).WaitUntilReady(ctx, c); err != nil {
						return fmt.Errorf("wait for port: %w", err)
					}
					ep, err := c.PortEndpoint(ctx, externalPort, "")
					if err != nil {
						return fmt.Errorf("endpoint: %w", err)
					}
					env := "export KAFKA_ADVERTISED_LISTENERS=INTERNAL://localhost:9092,EXTERNAL://" + ep + "\n"
					if err := c.CopyToContainer(ctx, []byte(env), envFile, 0o644); err != nil {
						return fmt.Errorf("copy env file: %w", err)
					}
					return wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute).WaitUntilReady(ctx, c)
				},
			},
		}),
	)
}

// ---- helpers ---------------------------------------------------------------

func newAdmin(t *testing.T) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

// itConfig creates a fresh topic pair and group for one test.
func itConfig(t *testing.T, partitions int32) Config {
	t.Helper()
	name := strings.ToLower(strings.NewReplacer("/", "-", " ", "-").Replace(t.Name())) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	cfg := Config{
		Brokers:            brokers,
		Topic:              name,
		DLQTopic:           name + ".dlq",
		Group:              name + "-group",
		FlushInterval:      200 * time.Millisecond,
		FlushMaxRecords:    50,
		RevokeFlushTimeout: 5 * time.Second,
		Retry:              retryPolicy(50 * time.Millisecond),
		DrainTimeout:       10 * time.Second,
		LagInterval:        time.Second,
	}
	adm := newAdmin(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for topic, p := range map[string]int32{cfg.Topic: partitions, cfg.DLQTopic: 1} {
		if _, err := adm.CreateTopic(ctx, p, 1, nil, topic); err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	return cfg
}

func newProducer(t *testing.T) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ProducerLinger(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// activity builds a valid record for user whose metadata carries seq, so the
// fake processor can detect loss and duplicates.
func activity(topic, user string, seq int) *kgo.Record {
	v := fmt.Appendf(nil, `{"user_id":%q,"activity_type":"page_view","timestamp":"2024-07-01T12:00:00Z","metadata":{"seq":"%d"}}`, user, seq)
	return &kgo.Record{
		Topic: topic, Key: []byte(user), Value: v,
		Headers: []kgo.RecordHeader{
			{Key: headerEventID, Value: fmt.Appendf(nil, "evt-%d", seq)},
			{Key: headerSchemaVersion, Value: []byte("1")},
		},
	}
}

func produce(t *testing.T, cl *kgo.Client, recs ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// collector is a BatchProcessor and PositionLoader that behaves like
// application.ProcessActivity on top of the Postgres repository: dead letters
// are published first, then the events and the stored position are applied
// atomically, with the same compare-and-set as ApplyDeltas.
type collector struct {
	dlq ports.DeadLetterPublisher

	mu        sync.Mutex
	seen      map[string]int // seq -> times processed
	order     map[string][]int
	events    int
	positions map[int32]int64 // stored next offset per partition
	fenced    int
}

func newCollector(dlq ports.DeadLetterPublisher) *collector {
	return &collector{dlq: dlq, seen: map[string]int{}, order: map[string][]int{}, positions: map[int32]int64{}}
}

func (c *collector) NextOffsets(_ context.Context, _, _ string, partitions []int32) (map[int32]int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[int32]int64{}
	for _, p := range partitions {
		if next, ok := c.positions[p]; ok {
			out[p] = next
		}
	}
	return out, nil
}

func (c *collector) Process(ctx context.Context, cp ports.Checkpoint, events []domain.ActivityEvent, rejected []ports.DeadLetter) error {
	if len(rejected) > 0 && c.dlq != nil {
		if err := c.dlq.Publish(ctx, rejected); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	stored, ok := c.positions[cp.Partition]
	if (cp.From == ports.NoPosition && ok) || (cp.From != ports.NoPosition && (!ok || stored != cp.From)) {
		if ok && stored == cp.To {
			return nil // already applied
		}
		c.fenced++
		return fmt.Errorf("%w: partition %d stored %d (%v), expected %d", ports.ErrFenced, cp.Partition, stored, ok, cp.From)
	}
	c.positions[cp.Partition] = cp.To
	for _, e := range events {
		seq := e.Metadata["seq"]
		c.seen[seq]++
		n, _ := strconv.Atoi(seq)
		c.order[e.UserID] = append(c.order[e.UserID], n)
		c.events++
	}
	return nil
}

func (c *collector) stats(want int) (distinct, total, dups int, missing []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := range want {
		switch n := c.seen[strconv.Itoa(i)]; {
		case n == 0:
			missing = append(missing, i)
		case n > 1:
			dups += n - 1
		}
	}
	return len(c.seen), c.events, dups, missing
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// runMember starts m.Run and returns a stop function that cancels it and
// returns Run's error.
func runMember(t *testing.T, m *Member) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	errCh := make(chan error, 1)
	go func() { errCh <- m.Run(ctx) }()
	var once sync.Once
	var err error
	stop = func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-errCh:
			case <-time.After(30 * time.Second):
				err = errors.New("member did not stop within 30s")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

func itLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

// committed returns the group's committed offset per partition of topic.
func committed(t *testing.T, cfg Config) map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	offs, err := newAdmin(t).FetchOffsets(ctx, cfg.Group)
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	out := map[int32]int64{}
	for p, o := range offs[cfg.Topic] {
		if o.Err == nil {
			out[p] = o.At
		}
	}
	return out
}

func endOffsets(t *testing.T, cfg Config) map[int32]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ends, err := newAdmin(t).ListEndOffsets(ctx, cfg.Topic)
	if err != nil {
		t.Fatalf("list end offsets: %v", err)
	}
	out := map[int32]int64{}
	for p, o := range ends[cfg.Topic] {
		out[p] = o.Offset
	}
	return out
}

func assertAllCommitted(t *testing.T, cfg Config) {
	t.Helper()
	got, want := committed(t, cfg), endOffsets(t, cfg)
	for p, end := range want {
		if end > 0 && got[p] != end {
			t.Errorf("partition %d: committed %d, end offset %d", p, got[p], end)
		}
	}
}

// ---- tests -----------------------------------------------------------------

func TestIntegrationPing(t *testing.T) {
	cfg := itConfig(t, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	t.Run("should succeed when both topics exist", func(t *testing.T) {
		// Topic creation reaches the broker's metadata cache asynchronously, so the
		// first Ping right after itConfig can still see UNKNOWN_TOPIC_OR_PARTITION.
		var err error
		waitFor(t, 10*time.Second, "both topics to be visible", func() bool {
			err = Ping(ctx, cfg)
			return err == nil
		})
	})
	t.Run("should fail when the DLQ topic is missing", func(t *testing.T) {
		bad := cfg
		bad.DLQTopic = cfg.Topic + ".missing"
		if err := Ping(ctx, bad); err == nil {
			t.Fatal("Ping() = nil, want an error for a missing topic")
		}
	})
}

func TestIntegrationRoundTrip(t *testing.T) {
	t.Run("should consume every record in per-user order and commit the end offsets", func(t *testing.T) {
		cfg := itConfig(t, 6)
		const n = 600
		prod := newProducer(t)
		var recs []*kgo.Record
		for i := range n {
			recs = append(recs, activity(cfg.Topic, fmt.Sprintf("user-%d", i%40), i))
		}
		produce(t, prod, recs...)

		col := newCollector(nil)
		reg := newRegistry()
		ms, err := NewGroupMembers(cfg, 2, col, col, itLogger(), reg)
		if err != nil {
			t.Fatal(err)
		}
		stops := []func() error{runMember(t, ms[0]), runMember(t, ms[1])}
		waitFor(t, 60*time.Second, "all records", func() bool { d, _, _, _ := col.stats(n); return d == n })
		waitFor(t, 30*time.Second, "lag gauge", func() bool {
			mfs, _ := reg.Gather()
			for _, mf := range mfs {
				if mf.GetName() == "consumer_lag_records" && len(mf.GetMetric()) > 0 {
					return true
				}
			}
			return false
		})
		if !ms[0].Joined() || !ms[1].Joined() {
			t.Error("Joined() = false after consuming")
		}
		for _, stop := range stops {
			if err := stop(); err != nil {
				t.Fatalf("Run() = %v, want nil after a clean stop", err)
			}
		}

		_, total, dups, missing := col.stats(n)
		if len(missing) > 0 || dups > 0 || total != n {
			t.Fatalf("total %d, dups %d, missing %v; want exactly %d", total, dups, missing, n)
		}
		col.mu.Lock()
		for user, seqs := range col.order {
			for i := 1; i < len(seqs); i++ {
				if seqs[i] < seqs[i-1] {
					t.Errorf("user %s processed out of order: %v", user, seqs)
					break
				}
			}
		}
		col.mu.Unlock()
		assertAllCommitted(t, cfg)
	})
}

func TestIntegrationDeadLetters(t *testing.T) {
	t.Run("should publish invalid records to the DLQ with the ADR 0011 headers and commit past them", func(t *testing.T) {
		cfg := itConfig(t, 1)
		reg := newRegistry()
		var dlqLogs bytes.Buffer
		dlqLogger := slog.New(slog.NewJSONHandler(&syncWriter{w: &dlqLogs}, &slog.HandlerOptions{Level: slog.LevelWarn}))
		dlq, err := NewDeadLetterPublisher(cfg, dlqLogger, reg)
		if err != nil {
			t.Fatal(err)
		}
		defer dlq.Close()

		const (
			traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
			tracestate  = "vendor=opaque"
		)
		prod := newProducer(t)
		bad := &kgo.Record{Topic: cfg.Topic, Key: []byte("user-x"), Value: []byte(`{"user_id":"user-x","metadata":`),
			Headers: []kgo.RecordHeader{
				{Key: headerEventID, Value: []byte("evt-bad")}, {Key: headerSchemaVersion, Value: []byte("1")},
				{Key: "traceparent", Value: []byte(traceparent)}, {Key: "tracestate", Value: []byte(tracestate)},
			}}
		invalid := &kgo.Record{Topic: cfg.Topic, Key: []byte("user-y"),
			Value: []byte(`{"user_id":"","activity_type":"click","timestamp":"2024-07-01T00:00:00Z"}`)}
		produce(t, prod, activity(cfg.Topic, "user-a", 0), bad, invalid, activity(cfg.Topic, "user-b", 1))

		col := newCollector(dlq)
		ms, err := NewGroupMembers(cfg, 1, col, col, itLogger(), reg)
		if err != nil {
			t.Fatal(err)
		}
		stop := runMember(t, ms[0])
		waitFor(t, 60*time.Second, "valid records", func() bool { d, _, _, _ := col.stats(2); return d == 2 })

		dlqRecs := readAll(t, cfg.DLQTopic, 2)
		if err := stop(); err != nil {
			t.Fatalf("Run() = %v", err)
		}
		assertAllCommitted(t, cfg)

		want := []struct {
			value, key, class, offset, eventID string
		}{
			{value: string(bad.Value), key: "user-x", class: "decode", offset: "1", eventID: "evt-bad"},
			{value: string(invalid.Value), key: "user-y", class: "validation", offset: "2"},
		}
		for i, r := range dlqRecs {
			h := map[string]string{}
			for _, hd := range r.Headers {
				h[hd.Key] = string(hd.Value)
			}
			w := want[i]
			if string(r.Value) != w.value || string(r.Key) != w.key {
				t.Errorf("dlq[%d] key/value = %q/%q, want original bytes %q/%q", i, r.Key, r.Value, w.key, w.value)
			}
			if h[headerErrorClass] != w.class || h[headerOriginalTopic] != cfg.Topic ||
				h[headerOriginalPartition] != "0" || h[headerOriginalOffset] != w.offset {
				t.Errorf("dlq[%d] headers = %v", i, h)
			}
			if _, err := time.Parse(time.RFC3339, h[headerFailedAt]); err != nil {
				t.Errorf("dlq[%d] %s = %q is not RFC 3339", i, headerFailedAt, h[headerFailedAt])
			}
			if w.eventID != "" && h[headerEventID] != w.eventID {
				t.Errorf("dlq[%d] event-id = %q, want the original %q", i, h[headerEventID], w.eventID)
			}
			if i == 0 && (h["traceparent"] != traceparent || h["tracestate"] != tracestate) {
				t.Errorf("dlq[0] trace headers = %q / %q, want the originals unchanged", h["traceparent"], h["tracestate"])
			}
			for k := range h {
				switch k {
				case headerErrorClass, headerOriginalTopic, headerOriginalPartition, headerOriginalOffset, headerFailedAt, headerEventID, headerSchemaVersion,
					"traceparent", "tracestate":
				default:
					t.Errorf("dlq[%d] has unexpected header %q", i, k)
				}
			}
		}
		var traced, untraced int
		for line := range strings.Lines(dlqLogs.String()) {
			switch {
			case !strings.Contains(line, "record dead-lettered"):
			case strings.Contains(line, `"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`):
				traced++
			case !strings.Contains(line, "trace_id"):
				untraced++
			}
		}
		if traced < 1 || untraced < 1 || strings.Contains(dlqLogs.String(), tracestate) {
			t.Errorf("dead-letter log lines with/without trace_id = %d/%d (tracestate must never be logged): %s", traced, untraced, dlqLogs.String())
		}
		mfs, _ := reg.Gather()
		for _, mf := range mfs {
			if mf.GetName() == "dlq_records_total" {
				var sum float64
				for _, m := range mf.GetMetric() {
					sum += m.GetCounter().GetValue()
				}
				if sum < 2 {
					t.Errorf("dlq_records_total = %v, want >= 2", sum)
				}
			}
		}
	})
}

// readAll reads n records from partition 0 of topic from the start.
func readAll(t *testing.T, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fs := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("read %s: got %d of %d records: %v", topic, len(out), n, err)
		}
		out = append(out, fs.Records()...)
	}
	return out
}

func TestIntegrationScaleWhileProducing(t *testing.T) {
	t.Run("should lose no record and count none twice while members join and leave", func(t *testing.T) {
		cfg := itConfig(t, 6)
		const n = 3000
		col := newCollector(nil)
		ms, err := NewGroupMembers(cfg, 4, col, col, itLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}

		prod := newProducer(t)
		var produced atomic.Int64
		prodDone := make(chan struct{})
		go func() {
			defer close(prodDone)
			for i := 0; i < n; i += 25 {
				batch := make([]*kgo.Record, 0, 25)
				for j := i; j < min(i+25, n); j++ {
					batch = append(batch, activity(cfg.Topic, fmt.Sprintf("user-%d", j%97), j))
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				err := prod.ProduceSync(ctx, batch...).FirstErr()
				cancel()
				if err != nil {
					t.Errorf("produce: %v", err)
					return
				}
				produced.Add(int64(len(batch)))
				time.Sleep(80 * time.Millisecond) // ~12 s of steady load
			}
		}()

		stopA := runMember(t, ms[0])
		time.Sleep(2 * time.Second)
		stopB := runMember(t, ms[1]) // scale up
		stopC := runMember(t, ms[2])
		time.Sleep(3 * time.Second)
		if err := stopB(); err != nil { // scale down mid-load
			t.Errorf("member B Run() = %v", err)
		}
		time.Sleep(2 * time.Second)
		stopD := runMember(t, ms[3]) // and up again
		time.Sleep(2 * time.Second)
		if err := stopA(); err != nil {
			t.Errorf("member A Run() = %v", err)
		}
		<-prodDone

		waitFor(t, 90*time.Second, "all records", func() bool { d, _, _, _ := col.stats(n); return d == n })
		for _, stop := range []func() error{stopC, stopD} {
			if err := stop(); err != nil {
				t.Errorf("Run() = %v", err)
			}
		}
		_, total, dups, missing := col.stats(n)
		t.Logf("produced %d, processed %d, duplicates %d", produced.Load(), total, dups)
		if len(missing) > 0 {
			t.Fatalf("lost %d records: %v", len(missing), missing)
		}
		if dups > 0 {
			t.Errorf("%d records counted twice across clean rebalances, want 0", dups)
		}
		assertAllCommitted(t, cfg)
	})
}

func TestIntegrationCrashBeforeCommit(t *testing.T) {
	t.Run("should resume from the stored position and count nothing twice after a crash between the database commit and the offset commit", func(t *testing.T) {
		cfg := itConfig(t, 1)
		cfg.FlushMaxRecords = 5
		cfg.FlushInterval = time.Minute // only full batches flush before the stop
		const n = 10
		prod := newProducer(t)
		var recs []*kgo.Record
		for i := range n {
			recs = append(recs, activity(cfg.Topic, "user-1", i))
		}
		produce(t, prod, recs...)

		col := newCollector(nil)
		first, err := NewGroupMembers(cfg, 1, col, col, itLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		var crashed atomic.Bool
		first[0].afterProcess = func(int32) bool { return crashed.CompareAndSwap(false, true) }
		stop := runMember(t, first[0])
		waitFor(t, 60*time.Second, "the first batch", func() bool { _, total, _, _ := col.stats(n); return total == 5 })
		if err := stop(); err != nil {
			t.Fatalf("Run() = %v", err)
		}
		if got, ok := committed(t, cfg)[0]; ok && got > 0 {
			t.Fatalf("Kafka committed offset after the crash = %d, want none (nothing marked)", got)
		}
		if next, _ := col.NextOffsets(t.Context(), cfg.Group, cfg.Topic, []int32{0}); next[0] != 5 {
			t.Fatalf("stored position after the crash = %v, want 5", next)
		}

		// From Kafka's point of view the first batch is still unconsumed (nothing
		// was committed), but the new owner resumes from the stored position.
		cfg.FlushInterval = 200 * time.Millisecond
		second, err := NewGroupMembers(cfg, 1, col, col, itLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		stop = runMember(t, second[0])
		waitFor(t, 60*time.Second, "the remaining records", func() bool { d, _, _, _ := col.stats(n); return d == n })
		waitFor(t, 30*time.Second, "the Kafka commit of the end offset", func() bool { return committed(t, cfg)[0] == n })
		if err := stop(); err != nil {
			t.Fatalf("Run() = %v", err)
		}
		distinct, total, dups, missing := col.stats(n)
		if distinct != n || len(missing) > 0 || dups != 0 || total != n {
			t.Fatalf("distinct %d, total %d, dups %d, missing %v; want each of the %d records exactly once", distinct, total, dups, missing, n)
		}
		col.mu.Lock()
		fenced := col.fenced
		col.mu.Unlock()
		if fenced != 0 {
			t.Errorf("fenced batches = %d, want 0", fenced)
		}
		assertAllCommitted(t, cfg)
	})

	t.Run("should commit the stored position to Kafka when nothing is left to consume", func(t *testing.T) {
		cfg := itConfig(t, 1)
		cfg.FlushMaxRecords = 5
		cfg.FlushInterval = time.Minute
		const n = 5
		prod := newProducer(t)
		var recs []*kgo.Record
		for i := range n {
			recs = append(recs, activity(cfg.Topic, "user-1", i))
		}
		produce(t, prod, recs...)

		col := newCollector(nil)
		first, err := NewGroupMembers(cfg, 1, col, col, itLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		first[0].afterProcess = func(int32) bool { return true } // every flush "crashes" before the mark
		stop := runMember(t, first[0])
		waitFor(t, 60*time.Second, "the only batch", func() bool { _, total, _, _ := col.stats(n); return total == n })
		if err := stop(); err != nil {
			t.Fatalf("Run() = %v", err)
		}

		// Without the alignment the group's lag would stay at 5: there is no
		// further record whose mark could move the Kafka commit.
		second, err := NewGroupMembers(cfg, 1, col, col, itLogger(), newRegistry())
		if err != nil {
			t.Fatal(err)
		}
		stop = runMember(t, second[0])
		waitFor(t, 30*time.Second, "the Kafka commit of the stored position", func() bool { return committed(t, cfg)[0] == n })
		if err := stop(); err != nil {
			t.Fatalf("Run() = %v", err)
		}
		if _, total, dups, _ := col.stats(n); total != n || dups != 0 {
			t.Fatalf("total %d, dups %d; want %d and 0", total, dups, n)
		}
	})
}
