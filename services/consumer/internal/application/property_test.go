package application_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/application"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
)

// totals is the per-(date,user) sum of activity and page_view counts.
type totals struct{ activities, pageViews int64 }

type key struct {
	date time.Time
	user string
}

// randomEvents builds n valid events: few users and dates so keys repeat, mixed
// types, and time zones whose offsets move the UTC date.
func randomEvents(t testing.TB, rng *rand.Rand, n int) []domain.ActivityEvent {
	t.Helper()
	users := []string{"u1", "u2", "u3", "usuário-é", "a-much-longer-user-identifier-0123456789"}
	types := []domain.ActivityType{domain.ActivityPageView, domain.ActivityPageView, "click", "foo", "page_view_x"}
	zones := []*time.Location{
		time.UTC,
		time.FixedZone("-03", -3*3600),
		time.FixedZone("+05", 5*3600),
		time.FixedZone("+1345", 13*3600+45*60),
	}
	base := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	events := make([]domain.ActivityEvent, 0, n)
	for range n {
		ts := base.Add(time.Duration(rng.Int64N(int64(72 * time.Hour)))).In(zones[rng.IntN(len(zones))])
		e, err := domain.NewActivityEvent(users[rng.IntN(len(users))], types[rng.IntN(len(types))], ts, nil)
		if err != nil {
			t.Fatalf("NewActivityEvent: %v", err)
		}
		events = append(events, e)
	}
	return events
}

// reference computes the expected totals without the code under test.
func reference(events []domain.ActivityEvent) map[key]totals {
	want := map[key]totals{}
	for _, e := range events {
		y, m, d := e.Timestamp.UTC().Date()
		k := key{time.Date(y, m, d, 0, 0, 0, 0, time.UTC), e.UserID}
		v := want[k]
		v.activities++
		if e.ActivityType == "page_view" {
			v.pageViews++
		}
		want[k] = v
	}
	return want
}

// processBatch runs one Process call and returns its deltas, failing if they are
// not sorted by (date, user) with unique keys.
func processBatch(t *testing.T, batch []domain.ActivityEvent) []domain.DailyUserAggregate {
	t.Helper()
	repo := &fakeAggregateRepository{}
	if err := application.NewProcessActivity(repo, &fakeDeadLetterPublisher{}).Process(t.Context(), testCheckpoint, batch, nil); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(repo.calls) != 1 {
		t.Fatalf("ApplyDeltas called %d times, want 1", len(repo.calls))
	}
	deltas := repo.calls[0]
	for i := 1; i < len(deltas); i++ {
		prev, cur := deltas[i-1], deltas[i]
		if c := prev.ActivityDate.Compare(cur.ActivityDate); c > 0 || (c == 0 && prev.UserID >= cur.UserID) {
			t.Fatalf("deltas not sorted with unique keys at %d: %+v then %+v", i, prev, cur)
		}
	}
	return deltas
}

// merge adds deltas into acc, as Postgres does when several flushes hit one row.
func merge(acc map[key]totals, deltas []domain.DailyUserAggregate) {
	for _, d := range deltas {
		k := key{d.ActivityDate, d.UserID}
		v := acc[k]
		v.activities += d.ActivityCount
		v.pageViews += d.PageViewCount
		acc[k] = v
	}
}

// TestFoldProperties checks, for seeded random event sets, that any permutation
// and any split into batches give the same per-(date,user) totals, that the
// totals match an independent computation, and that counts add up (ADR 0014).
func TestFoldProperties(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 40; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(seed, seed*7919))
			events := randomEvents(t, rng, 1+rng.IntN(300))
			want := reference(events)

			var pageViews int64
			for _, e := range events {
				if e.ActivityType == domain.ActivityPageView {
					pageViews++
				}
			}

			whole := processBatch(t, events)
			var sumActivity, sumViews int64
			for _, d := range whole {
				sumActivity += d.ActivityCount
				sumViews += d.PageViewCount
			}
			if sumActivity != int64(len(events)) {
				t.Errorf("sum(activity_count) = %d, want %d", sumActivity, len(events))
			}
			if sumViews != pageViews {
				t.Errorf("sum(page_view_count) = %d, want %d", sumViews, pageViews)
			}
			got := map[key]totals{}
			merge(got, whole)
			if diff := cmp.Diff(want, got, cmp.AllowUnexported(key{}, totals{})); diff != "" {
				t.Errorf("whole batch differs from reference (-want +got):\n%s", diff)
			}

			shuffled := slices.Clone(events)
			rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
			if diff := cmp.Diff(whole, processBatch(t, shuffled)); diff != "" {
				t.Errorf("permutation changed the deltas (-whole +shuffled):\n%s", diff)
			}

			split := map[key]totals{}
			rest := shuffled
			for len(rest) > 0 {
				n := 1 + rng.IntN(len(rest))
				merge(split, processBatch(t, rest[:n]))
				rest = rest[n:]
			}
			if diff := cmp.Diff(want, split, cmp.AllowUnexported(key{}, totals{})); diff != "" {
				t.Errorf("split into batches changed the totals (-want +got):\n%s", diff)
			}
		})
	}
}
