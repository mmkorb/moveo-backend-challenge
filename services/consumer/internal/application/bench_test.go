package application_test

import (
	"context"
	"math/rand/v2"
	"testing"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/application"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// discardRepository accepts every call without recording it, so the benchmark
// measures the fold and not the growth of a fake's call log.
type discardRepository struct{}

var _ ports.AggregateRepository = discardRepository{}

func (discardRepository) ApplyDeltas(context.Context, ports.Checkpoint, []domain.DailyUserAggregate) error {
	return nil
}

type discardDLQ struct{}

var _ ports.DeadLetterPublisher = discardDLQ{}

func (discardDLQ) Publish(context.Context, []ports.DeadLetter) error { return nil }

// BenchmarkProcess500 folds and "persists" one full flush batch (FLUSH_MAX_RECORDS
// is 500 by default) of events over up to 20 (date, user) keys.
func BenchmarkProcess500(b *testing.B) {
	rng := rand.New(rand.NewPCG(1, 2))
	events := randomEvents(b, rng, 500)
	uc := application.NewProcessActivity(discardRepository{}, discardDLQ{})
	ctx := context.Background()

	b.ReportAllocs()
	for b.Loop() {
		if err := uc.Process(ctx, testCheckpoint, events, nil); err != nil {
			b.Fatal(err)
		}
	}
}
