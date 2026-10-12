//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"os/exec"
	"testing"
	"time"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// docker runs a docker CLI command against the test container.
func docker(t *testing.T, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput(); err != nil { //nolint:gosec // G204: fixed binary, arguments from the tests
		t.Fatalf("docker %v: %v: %s", args, err, out)
	}
}

// A frozen server (docker pause: the process stops, its kernel still accepts
// connections and acknowledges keepalives) enforces no statement_timeout, so
// only the caller's deadline ends a flush attempt. The deadline must turn into
// a transient error, and the retry after the thaw must apply the batch once.
func TestFrozenServerFailsTheAttemptAtTheDeadline(t *testing.T) {
	resetTable(t)
	repo := newRepo(t, appCfg)
	jul1 := day(2024, 7, 1)
	if err := repo.ApplyDeltas(t.Context(), checkpoint(3, ports.NoPosition, 10), []domain.DailyUserAggregate{delta(jul1, "u", 1, 1)}); err != nil {
		t.Fatalf("warm-up ApplyDeltas() error = %v", err)
	}

	docker(t, "pause", containerID)
	paused := true
	unpause := func() {
		if paused {
			paused = false
			docker(t, "unpause", containerID)
		}
	}
	t.Cleanup(unpause)

	cp := checkpoint(3, 10, 20)
	deltas := []domain.DailyUserAggregate{delta(jul1, "u", 2, 0)}
	const deadline = time.Second
	ctx, cancel := context.WithTimeout(t.Context(), deadline)
	start := time.Now()
	err := repo.ApplyDeltas(ctx, cp, deltas)
	took := time.Since(start)
	cancel()

	if !errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("ApplyDeltas() on a frozen server error = %v, want ports.ErrUnavailable", err)
	}
	if took > deadline+2*time.Second {
		t.Errorf("ApplyDeltas() returned after %v, want about the %v deadline", took, deadline)
	}

	// A second attempt on a fresh connection fails the same way.
	ctx, cancel = context.WithTimeout(t.Context(), deadline)
	err = repo.ApplyDeltas(ctx, cp, deltas)
	cancel()
	if !errors.Is(err, ports.ErrUnavailable) {
		t.Errorf("second ApplyDeltas() error = %v, want ports.ErrUnavailable", err)
	}

	unpause()
	var retryErr error
	for range 50 { // the thawed server may first answer with the closed connections' errors
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		retryErr = repo.ApplyDeltas(ctx, cp, deltas)
		cancel()
		if retryErr == nil || !errors.Is(retryErr, ports.ErrUnavailable) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if retryErr != nil {
		t.Fatalf("ApplyDeltas() after the thaw error = %v, want nil", retryErr)
	}
	if got := storedOffsets(t); got[3] != 20 {
		t.Errorf("stored position = %d, want 20", got[3])
	}
	if got := rows(t); len(got) != 1 || got[0].ActivityCount != 3 {
		t.Errorf("rows = %+v, want one row with activity_count 3 (batch applied once)", got)
	}
}
