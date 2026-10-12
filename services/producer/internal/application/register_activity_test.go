package application

import (
	"context"
	"errors"
	"testing"
	"time"
	"uuid"

	"github.com/google/go-cmp/cmp"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/ports"
)

type publishCall struct {
	ID    uuid.UUID
	Event domain.ActivityEvent
}

// fakeActivityPublisher records calls and returns an injected error.
type fakeActivityPublisher struct {
	calls []publishCall
	err   error
}

func (f *fakeActivityPublisher) Publish(_ context.Context, id uuid.UUID, ev domain.ActivityEvent) error {
	f.calls = append(f.calls, publishCall{ID: id, Event: ev})
	return f.err
}

var _ ports.ActivityPublisher = (*fakeActivityPublisher)(nil)

func newEventFixture(t *testing.T) domain.ActivityEvent {
	t.Helper()
	ev, err := domain.NewActivityEvent("u1", domain.ActivityPageView, time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC), nil)
	if err != nil {
		t.Fatalf("NewActivityEvent: %v", err)
	}
	return ev
}

func TestRegisterActivity(t *testing.T) {
	t.Parallel()
	fixedID := uuid.MustParse("0199d3a4-7c2e-7b1a-9f3e-2a4b6c8d0e1f")
	errBroker := errors.New("broker said no")
	tests := []struct {
		name       string
		publishErr error
		wantID     uuid.UUID
		wantErrIs  []error
	}{
		{name: "should publish once and return the event id when the publisher succeeds", wantID: fixedID},
		{name: "should wrap ErrPublishUnavailable when the broker is unavailable",
			publishErr: ports.ErrPublishUnavailable, wantErrIs: []error{ports.ErrPublishUnavailable}},
		{name: "should wrap any other publisher error", publishErr: errBroker, wantErrIs: []error{errBroker}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pub := &fakeActivityPublisher{err: tt.publishErr}
			uc, err := NewRegisterActivity(pub, func() uuid.UUID { return fixedID })
			if err != nil {
				t.Fatalf("NewRegisterActivity: %v", err)
			}
			ev := newEventFixture(t)
			id, err := uc.Register(context.Background(), ev)
			for _, target := range tt.wantErrIs {
				if !errors.Is(err, target) {
					t.Errorf("Register() error = %v, want errors.Is %v", err, target)
				}
			}
			if tt.wantErrIs == nil && err != nil {
				t.Fatalf("Register() error = %v, want nil", err)
			}
			if id != tt.wantID {
				t.Errorf("Register() id = %v, want %v", id, tt.wantID)
			}
			want := []publishCall{{ID: fixedID, Event: ev}}
			if diff := cmp.Diff(want, pub.calls); diff != "" {
				t.Errorf("publish calls mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewRegisterActivityRequiresDependencies(t *testing.T) {
	t.Parallel()
	if _, err := NewRegisterActivity(nil, uuid.NewV7); err == nil {
		t.Error("NewRegisterActivity(nil publisher) error = nil, want error")
	}
	if _, err := NewRegisterActivity(&fakeActivityPublisher{}, nil); err == nil {
		t.Error("NewRegisterActivity(nil id generator) error = nil, want error")
	}
}

func TestRegisterActivityUsesUUIDv7(t *testing.T) {
	t.Parallel()
	pub := &fakeActivityPublisher{}
	uc, err := NewRegisterActivity(pub, uuid.NewV7)
	if err != nil {
		t.Fatalf("NewRegisterActivity: %v", err)
	}
	id, err := uc.Register(context.Background(), newEventFixture(t))
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if v := id.String()[14]; v != '7' {
		t.Errorf("event id %s has version %c, want 7", id, v)
	}
}
