package kafka

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/retry"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

const testTopic = "incoming.user_activity"

// fakeGroupClient records marks, commits and pause/resume calls.
type fakeGroupClient struct {
	mu        sync.Mutex
	marks     map[int32]int64   // partition -> offset of the last marked record
	commits   []map[int32]int64 // snapshot of marks at every commit
	commitErr error
	paused    map[int32]bool
	resumes   int

	recordCommits   map[int32]int64 // partition -> next offset of the last CommitRecords
	recordCommitErr error

	seeks []map[int32]int64 // every SetOffsets call: partition -> offset
}

func (f *fakeGroupClient) SetOffsets(set map[string]map[int32]kgo.EpochOffset) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := map[int32]int64{}
	for p, eo := range set[testTopic] {
		s[p] = eo.Offset
	}
	f.seeks = append(f.seeks, s)
}

func (f *fakeGroupClient) seekCalls() []map[int32]int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]map[int32]int64(nil), f.seeks...)
}

func newFakeGroupClient() *fakeGroupClient {
	return &fakeGroupClient{marks: map[int32]int64{}, paused: map[int32]bool{}}
}

func (f *fakeGroupClient) MarkCommitRecords(rs ...*kgo.Record) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range rs {
		if cur, ok := f.marks[r.Partition]; !ok || r.Offset > cur {
			f.marks[r.Partition] = r.Offset
		}
	}
}

func (f *fakeGroupClient) CommitMarkedOffsets(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, maps.Clone(f.marks))
	return f.commitErr
}

func (f *fakeGroupClient) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recordCommitErr != nil {
		return f.recordCommitErr
	}
	if f.recordCommits == nil {
		f.recordCommits = map[int32]int64{}
	}
	for _, r := range rs {
		f.recordCommits[r.Partition] = r.Offset + 1
	}
	return nil
}

func (f *fakeGroupClient) recordCommit(p int32) (int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.recordCommits[p]
	return o, ok
}

func (f *fakeGroupClient) PauseFetchPartitions(tps map[string][]int32) map[string][]int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range tps[testTopic] {
		f.paused[p] = true
	}
	return nil
}

func (f *fakeGroupClient) ResumeFetchPartitions(tps map[string][]int32) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range tps[testTopic] {
		if f.paused[p] {
			f.resumes++
		}
		delete(f.paused, p)
	}
}

func (f *fakeGroupClient) mark(p int32) (int64, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	o, ok := f.marks[p]
	return o, ok
}

func (f *fakeGroupClient) lastCommit() (map[int32]int64, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.commits) == 0 {
		return nil, 0
	}
	return f.commits[len(f.commits)-1], len(f.commits)
}

func (f *fakeGroupClient) isPaused(p int32) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.paused[p]
}

// processCall is one BatchProcessor.Process invocation.
type processCall struct {
	cp       ports.Checkpoint
	events   []domain.ActivityEvent
	rejected []ports.DeadLetter
}

// fakeProcessor records calls. fail(n) (0-based call index) injects errors;
// a non-nil gate makes calls wait for it (or for ctx when honorCtx is set).
type fakeProcessor struct {
	mu       sync.Mutex
	calls    []processCall
	fail     func(n int) error
	gate     chan struct{}
	honorCtx bool
}

func (p *fakeProcessor) Process(ctx context.Context, cp ports.Checkpoint, events []domain.ActivityEvent, rejected []ports.DeadLetter) error {
	p.mu.Lock()
	n := len(p.calls)
	p.calls = append(p.calls, processCall{cp: cp, events: events, rejected: rejected})
	gate, fail, honor := p.gate, p.fail, p.honorCtx
	p.mu.Unlock()
	if gate != nil {
		if honor {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			<-gate
		}
	}
	if fail != nil {
		return fail(n)
	}
	return nil
}

func (p *fakeProcessor) snapshot() []processCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]processCall(nil), p.calls...)
}

// fakePositions is a PositionLoader with fixed stored positions. err, when
// set, fails every call; calls counts them.
type fakePositions struct {
	mu     sync.Mutex
	stored map[int32]int64
	err    error
	calls  [][]int32
}

func (f *fakePositions) NextOffsets(_ context.Context, _, _ string, partitions []int32) (map[int32]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, partitions)
	if f.err != nil {
		return nil, f.err
	}
	out := map[int32]int64{}
	for _, p := range partitions {
		if v, ok := f.stored[p]; ok {
			out[p] = v
		}
	}
	return out, nil
}

func (f *fakePositions) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

// testConfig is a valid consumer config for unit tests.
func testConfig() Config {
	return Config{
		Brokers:            []string{"127.0.0.1:1"},
		Topic:              testTopic,
		DLQTopic:           testTopic + ".dlq",
		Group:              "test-group",
		FlushInterval:      time.Second,
		FlushMaxRecords:    100,
		RevokeFlushTimeout: 10 * time.Second,
		Retry:              retry.Policy{Base: 100 * time.Millisecond, Max: 10 * time.Second},
	}.withDefaults()
}

// newTestMember builds a member without a Kafka client; tests drive its
// callbacks directly with a fakeGroupClient. Logs go to the returned buffer.
func newTestMember(t *testing.T, cfg Config, proc BatchProcessor) (*Member, *bytes.Buffer) {
	t.Helper()
	metrics, err := newConsumerMetrics(prometheus.NewRegistry())
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&syncWriter{w: &logs}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m := newMember(cfg, 0, proc, &fakePositions{}, logger, metrics, prometheus.NewRegistry())
	m.lifeCtx = t.Context()
	return m, &logs
}

// syncWriter serializes writes from several worker goroutines.
type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(b)
}

func validValue(user string) []byte {
	return fmt.Appendf(nil, `{"user_id":%q,"activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z","metadata":{"page_url":"https://example.com/home"}}`, user)
}

func record(partition int32, offset int64, value []byte, headers ...kgo.RecordHeader) *kgo.Record {
	return &kgo.Record{
		Topic:     testTopic,
		Partition: partition,
		Offset:    offset,
		Key:       []byte("key"),
		Value:     value,
		Headers:   headers,
	}
}

// validRecords returns n valid records of partition p starting at offset from.
func validRecords(p int32, from int64, n int) []*kgo.Record {
	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = record(p, from+int64(i), validValue(fmt.Sprintf("u%d", i)))
	}
	return recs
}

func assign(m *Member, gc groupClient, parts ...int32) {
	m.onAssigned(gc, map[string][]int32{testTopic: parts})
}

func retryPolicy(d time.Duration) retry.Policy { return retry.Policy{Base: d, Max: d} }

func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

func newRegistry() *prometheus.Registry { return prometheus.NewRegistry() }

// metricValue reads a counter or gauge.
func metricValue(t *testing.T, m prometheus.Metric) float64 {
	t.Helper()
	var out dto.Metric
	if err := m.Write(&out); err != nil {
		t.Fatal(err)
	}
	switch {
	case out.Counter != nil:
		return out.Counter.GetValue()
	case out.Gauge != nil:
		return out.Gauge.GetValue()
	}
	t.Fatalf("metric %s is neither a counter nor a gauge", m.Desc())
	return 0
}
