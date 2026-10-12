//go:build integration

package postgres_test

import (
	"fmt"
	"testing"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/adapters/postgres"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
)

// BenchmarkApplyDeltas500 upserts 500 distinct (date, user) deltas per call against
// the package's single PostgreSQL container: the first call inserts, later calls
// hit ON CONFLICT DO UPDATE. Run with:
//
//	go test -tags=integration -run=NONE -bench=ApplyDeltas ./internal/adapters/postgres/
func BenchmarkApplyDeltas500(b *testing.B) {
	if _, err := ownerDB.Exec(b.Context(), "TRUNCATE activity_aggregates"); err != nil {
		b.Fatalf("truncate: %v", err)
	}
	pool, err := postgres.NewPool(b.Context(), appCfg)
	if err != nil {
		b.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()
	repo := postgres.NewAggregateRepository(pool)

	deltas := make([]domain.DailyUserAggregate, 0, 500)
	for i := range 500 {
		deltas = append(deltas, delta(day(2024, 7, 1+i%5), fmt.Sprintf("user-%03d", i), 3, 1))
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := apply(b.Context(), repo, deltas); err != nil {
			b.Fatal(err)
		}
	}
}
