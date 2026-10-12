package application_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/application"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// fakeAggregateRepository records every ApplyDeltas call and returns err.
type fakeAggregateRepository struct {
	calls       [][]domain.DailyUserAggregate
	checkpoints []ports.Checkpoint
	err         error
}

var _ ports.AggregateRepository = (*fakeAggregateRepository)(nil)

func (f *fakeAggregateRepository) ApplyDeltas(ctx context.Context, cp ports.Checkpoint, deltas []domain.DailyUserAggregate) error {
	f.calls = append(f.calls, slices.Clone(deltas))
	f.checkpoints = append(f.checkpoints, cp)
	return f.err
}

// testCheckpoint is a valid checkpoint for a batch of partition 2.
var testCheckpoint = ports.Checkpoint{Group: "g", Topic: "incoming.user_activity", Partition: 2, From: 100, To: 110}

// fakeDeadLetterPublisher records every Publish call and returns err.
type fakeDeadLetterPublisher struct {
	calls [][]ports.DeadLetter
	err   error
}

var _ ports.DeadLetterPublisher = (*fakeDeadLetterPublisher)(nil)

func (f *fakeDeadLetterPublisher) Publish(ctx context.Context, letters []ports.DeadLetter) error {
	f.calls = append(f.calls, slices.Clone(letters))
	return f.err
}

func newEvent(t *testing.T, user string, typ domain.ActivityType, ts string) domain.ActivityEvent {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, ts)
	if err != nil {
		t.Fatalf("parse %q: %v", ts, err)
	}
	e, err := domain.NewActivityEvent(user, typ, parsed, nil)
	if err != nil {
		t.Fatalf("NewActivityEvent: %v", err)
	}
	return e
}

func day(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

func delta(date time.Time, user string, count, views int64) domain.DailyUserAggregate {
	return domain.DailyUserAggregate{ActivityDate: date, UserID: user, ActivityCount: count, PageViewCount: views}
}

func newDeadLetter(offset int64) ports.DeadLetter {
	return ports.DeadLetter{
		Key: []byte("u1"), Value: []byte("{not json"), Topic: "incoming.user_activity",
		Partition: 2, Offset: offset, Class: ports.ErrorClassDecode,
	}
}

func TestProcessFoldsEventsIntoSortedUniqueDeltas(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		events func(t *testing.T) []domain.ActivityEvent
		want   []domain.DailyUserAggregate
	}{
		{
			name: "should produce one delta when one user repeats on the same day",
			events: func(t *testing.T) []domain.ActivityEvent {
				return []domain.ActivityEvent{
					newEvent(t, "u1", "click", "2024-07-01T08:00:00Z"),
					newEvent(t, "u1", "click", "2024-07-01T09:00:00Z"),
					newEvent(t, "u1", "click", "2024-07-01T10:00:00Z"),
				}
			},
			want: []domain.DailyUserAggregate{delta(day(2024, 7, 1), "u1", 3, 0)},
		},
		{
			name: "should count page views only for page_view when activity types are mixed",
			events: func(t *testing.T) []domain.ActivityEvent {
				return []domain.ActivityEvent{
					newEvent(t, "u1", domain.ActivityPageView, "2024-07-01T08:00:00Z"),
					newEvent(t, "u1", "click", "2024-07-01T09:00:00Z"),
					newEvent(t, "u1", "foo", "2024-07-01T10:00:00Z"),
				}
			},
			want: []domain.DailyUserAggregate{delta(day(2024, 7, 1), "u1", 3, 1)},
		},
		{
			name: "should split deltas by UTC date when offsets cross midnight",
			events: func(t *testing.T) []domain.ActivityEvent {
				return []domain.ActivityEvent{
					newEvent(t, "u1", domain.ActivityPageView, "2024-07-01T23:30:00-03:00"), // 2024-07-02 UTC
					newEvent(t, "u1", domain.ActivityPageView, "2024-07-02T01:00:00+05:00"), // 2024-07-01 UTC
					newEvent(t, "u1", "click", "2024-07-01T23:59:59.999Z"),
					newEvent(t, "u1", "click", "2024-07-02T00:00:00Z"),
				}
			},
			want: []domain.DailyUserAggregate{
				delta(day(2024, 7, 1), "u1", 2, 1),
				delta(day(2024, 7, 2), "u1", 2, 1),
			},
		},
		{
			name: "should sort deltas by date then user when the batch has many keys",
			events: func(t *testing.T) []domain.ActivityEvent {
				return []domain.ActivityEvent{
					newEvent(t, "u2", "click", "2024-07-02T10:00:00Z"),
					newEvent(t, "u1", domain.ActivityPageView, "2024-07-02T10:00:00Z"),
					newEvent(t, "u2", domain.ActivityPageView, "2024-07-01T10:00:00Z"),
					newEvent(t, "u10", "click", "2024-07-01T10:00:00Z"),
					newEvent(t, "u2", "click", "2024-07-01T11:00:00Z"),
				}
			},
			want: []domain.DailyUserAggregate{
				delta(day(2024, 7, 1), "u10", 1, 0),
				delta(day(2024, 7, 1), "u2", 2, 1),
				delta(day(2024, 7, 2), "u1", 1, 1),
				delta(day(2024, 7, 2), "u2", 1, 0),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			repo, dlq := &fakeAggregateRepository{}, &fakeDeadLetterPublisher{}
			uc := application.NewProcessActivity(repo, dlq)

			if err := uc.Process(t.Context(), testCheckpoint, tt.events(t), nil); err != nil {
				t.Fatalf("Process: %v", err)
			}
			if len(dlq.calls) != 0 {
				t.Errorf("DLQ called %d times, want 0", len(dlq.calls))
			}
			if len(repo.calls) != 1 {
				t.Fatalf("ApplyDeltas called %d times, want 1", len(repo.calls))
			}
			if diff := cmp.Diff(tt.want, repo.calls[0]); diff != "" {
				t.Errorf("deltas mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestProcessIsIndependentOfInputOrder(t *testing.T) {
	t.Parallel()
	users := []string{"a", "b", "c", "d"}
	types := []domain.ActivityType{domain.ActivityPageView, "click", "foo"}
	var events []domain.ActivityEvent
	for i := range 200 {
		ts := fmt.Sprintf("2024-07-%02dT%02d:00:00Z", 1+i%3, i%24)
		events = append(events, newEvent(t, users[i%len(users)], types[i%len(types)], ts))
	}

	run := func(batch []domain.ActivityEvent) []domain.DailyUserAggregate {
		repo := &fakeAggregateRepository{}
		if err := application.NewProcessActivity(repo, &fakeDeadLetterPublisher{}).Process(t.Context(), testCheckpoint, batch, nil); err != nil {
			t.Fatalf("Process: %v", err)
		}
		return repo.calls[0]
	}
	want := run(events)

	var total int64
	for _, d := range want {
		total += d.ActivityCount
	}
	if total != int64(len(events)) {
		t.Fatalf("sum of activity_count = %d, want %d", total, len(events))
	}

	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 5 {
		shuffled := slices.Clone(events)
		rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
		if diff := cmp.Diff(want, run(shuffled)); diff != "" {
			t.Fatalf("shuffle %d: deltas depend on input order (-want +got):\n%s", i, diff)
		}
	}
}

func TestProcessPublishesRejectedBeforeApplying(t *testing.T) {
	t.Parallel()
	repo, dlq := &fakeAggregateRepository{}, &fakeDeadLetterPublisher{}
	rejected := []ports.DeadLetter{newDeadLetter(7), newDeadLetter(9)}
	events := []domain.ActivityEvent{newEvent(t, "u1", "click", "2024-07-01T10:00:00Z")}

	if err := application.NewProcessActivity(repo, dlq).Process(t.Context(), testCheckpoint, events, rejected); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(dlq.calls) != 1 {
		t.Fatalf("DLQ called %d times, want 1", len(dlq.calls))
	}
	if diff := cmp.Diff(rejected, dlq.calls[0]); diff != "" {
		t.Errorf("dead letters mismatch (-want +got):\n%s", diff)
	}
	if len(repo.calls) != 1 {
		t.Errorf("ApplyDeltas called %d times, want 1", len(repo.calls))
	}
}

func TestProcessOnlyRejectedStillAdvancesThePosition(t *testing.T) {
	t.Parallel()
	repo, dlq := &fakeAggregateRepository{}, &fakeDeadLetterPublisher{}

	if err := application.NewProcessActivity(repo, dlq).Process(t.Context(), testCheckpoint, nil, []ports.DeadLetter{newDeadLetter(1)}); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(dlq.calls) != 1 {
		t.Errorf("DLQ called %d times, want 1", len(dlq.calls))
	}
	if len(repo.calls) != 1 {
		t.Fatalf("ApplyDeltas called %d times, want 1: dead letters advance the position too", len(repo.calls))
	}
	if len(repo.calls[0]) != 0 {
		t.Errorf("deltas = %v, want none", repo.calls[0])
	}
	if repo.checkpoints[0] != testCheckpoint {
		t.Errorf("checkpoint = %+v, want %+v", repo.checkpoints[0], testCheckpoint)
	}
}

func TestProcessPassesTheCheckpointThrough(t *testing.T) {
	t.Parallel()
	repo := &fakeAggregateRepository{}
	events := []domain.ActivityEvent{newEvent(t, "u1", "click", "2024-07-01T10:00:00Z")}
	cp := ports.Checkpoint{Group: "g", Topic: "t", Partition: 0, From: ports.NoPosition, To: 1}

	if err := application.NewProcessActivity(repo, &fakeDeadLetterPublisher{}).Process(t.Context(), cp, events, nil); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(repo.checkpoints) != 1 || repo.checkpoints[0] != cp {
		t.Errorf("checkpoints = %+v, want [%+v]", repo.checkpoints, cp)
	}
}

func TestProcessRejectsAnInvalidCheckpoint(t *testing.T) {
	t.Parallel()
	events := []domain.ActivityEvent{newEvent(t, "u1", "click", "2024-07-01T10:00:00Z")}
	tests := []struct {
		name string
		cp   ports.Checkpoint
	}{
		{"should reject an empty group", ports.Checkpoint{Topic: "t", From: 0, To: 1}},
		{"should reject an empty topic", ports.Checkpoint{Group: "g", From: 0, To: 1}},
		{"should reject a negative partition", ports.Checkpoint{Group: "g", Topic: "t", Partition: -1, From: 0, To: 1}},
		{"should reject a from below NoPosition", ports.Checkpoint{Group: "g", Topic: "t", From: -2, To: 1}},
		{"should reject a to that does not move forward", ports.Checkpoint{Group: "g", Topic: "t", From: 5, To: 5}},
		{"should reject a to of zero", ports.Checkpoint{Group: "g", Topic: "t", From: ports.NoPosition, To: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			repo, dlq := &fakeAggregateRepository{}, &fakeDeadLetterPublisher{}
			err := application.NewProcessActivity(repo, dlq).Process(t.Context(), tt.cp, events, []ports.DeadLetter{newDeadLetter(1)})
			if err == nil {
				t.Fatal("Process() error = nil, want an invalid checkpoint error")
			}
			if errors.Is(err, ports.ErrUnavailable) || errors.Is(err, ports.ErrFenced) {
				t.Errorf("err = %v, want unclassified", err)
			}
			if len(repo.calls)+len(dlq.calls) != 0 {
				t.Errorf("ports called with an invalid checkpoint: repo=%d dlq=%d", len(repo.calls), len(dlq.calls))
			}
		})
	}
}

func TestProcessEmptyBatchIsNoop(t *testing.T) {
	t.Parallel()
	repo, dlq := &fakeAggregateRepository{}, &fakeDeadLetterPublisher{}

	if err := application.NewProcessActivity(repo, dlq).Process(t.Context(), testCheckpoint, nil, nil); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(repo.calls)+len(dlq.calls) != 0 {
		t.Errorf("ports called on empty batch: repo=%d dlq=%d", len(repo.calls), len(dlq.calls))
	}
}

func TestProcessErrors(t *testing.T) {
	t.Parallel()
	unavailable := fmt.Errorf("%w: connection refused", ports.ErrUnavailable)
	unknown := errors.New("check constraint violated")

	tests := []struct {
		name          string
		dlqErr        error
		repoErr       error
		rejected      bool
		wantIs        error
		wantRepoCalls int
	}{
		{name: "should not apply deltas when the DLQ publish fails", dlqErr: unavailable, rejected: true, wantIs: ports.ErrUnavailable, wantRepoCalls: 0},
		{name: "should keep ErrUnavailable when the repository is unavailable", repoErr: unavailable, wantIs: ports.ErrUnavailable, wantRepoCalls: 1},
		{name: "should return an unclassified error when the repository fails otherwise", repoErr: unknown, wantIs: unknown, wantRepoCalls: 1},
		{name: "should keep ErrFenced when another owner moved the position", repoErr: fmt.Errorf("%w: expected 100", ports.ErrFenced), wantIs: ports.ErrFenced, wantRepoCalls: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			repo := &fakeAggregateRepository{err: tt.repoErr}
			dlq := &fakeDeadLetterPublisher{err: tt.dlqErr}
			var rejected []ports.DeadLetter
			if tt.rejected {
				rejected = []ports.DeadLetter{newDeadLetter(3)}
			}
			events := []domain.ActivityEvent{newEvent(t, "u1", "click", "2024-07-01T10:00:00Z")}

			err := application.NewProcessActivity(repo, dlq).Process(t.Context(), testCheckpoint, events, rejected)
			if !errors.Is(err, tt.wantIs) {
				t.Fatalf("err = %v, want errors.Is %v", err, tt.wantIs)
			}
			if !errors.Is(tt.wantIs, ports.ErrUnavailable) && errors.Is(err, ports.ErrUnavailable) {
				t.Errorf("err = %v is classified as unavailable, want unclassified", err)
			}
			if len(repo.calls) != tt.wantRepoCalls {
				t.Errorf("ApplyDeltas called %d times, want %d", len(repo.calls), tt.wantRepoCalls)
			}
		})
	}
}

// fakeOffsetStore returns next for every call and records the partitions asked for.
type fakeOffsetStore struct {
	next  map[int32]int64
	err   error
	calls [][]int32
}

var _ ports.OffsetStore = (*fakeOffsetStore)(nil)

func (f *fakeOffsetStore) NextOffsets(ctx context.Context, group, topic string, partitions []int32) (map[int32]int64, error) {
	f.calls = append(f.calls, slices.Clone(partitions))
	return f.next, f.err
}

func TestResumePositions(t *testing.T) {
	t.Parallel()
	t.Run("should return the stored positions", func(t *testing.T) {
		t.Parallel()
		store := &fakeOffsetStore{next: map[int32]int64{0: 42}}
		got, err := application.NewResumePositions(store).NextOffsets(t.Context(), "g", "t", []int32{0, 1})
		if err != nil {
			t.Fatalf("NextOffsets() error = %v", err)
		}
		if diff := cmp.Diff(map[int32]int64{0: 42}, got); diff != "" {
			t.Errorf("positions mismatch (-want +got):\n%s", diff)
		}
	})
	t.Run("should not call the store for no partitions", func(t *testing.T) {
		t.Parallel()
		store := &fakeOffsetStore{}
		got, err := application.NewResumePositions(store).NextOffsets(t.Context(), "g", "t", nil)
		if err != nil || len(got) != 0 || len(store.calls) != 0 {
			t.Errorf("NextOffsets(nil) = %v, %v with %d store calls, want empty, nil, 0", got, err, len(store.calls))
		}
	})
	t.Run("should keep ErrUnavailable", func(t *testing.T) {
		t.Parallel()
		store := &fakeOffsetStore{err: fmt.Errorf("%w: refused", ports.ErrUnavailable)}
		_, err := application.NewResumePositions(store).NextOffsets(t.Context(), "g", "t", []int32{3})
		if !errors.Is(err, ports.ErrUnavailable) {
			t.Errorf("err = %v, want ErrUnavailable", err)
		}
	})
}
