// Package application holds the producer's use cases.
package application

import (
	"context"
	"errors"
	"fmt"
	"uuid"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/ports"
)

// IDGenerator returns a new event ID. Production code passes uuid.NewV7.
type IDGenerator func() uuid.UUID

// RegisterActivity accepts a valid ActivityEvent, assigns it a tracing event
// ID and publishes it once through the ActivityPublisher.
type RegisterActivity struct {
	publisher ports.ActivityPublisher
	newID     IDGenerator
}

// NewRegisterActivity returns the use case. Both dependencies are required.
func NewRegisterActivity(publisher ports.ActivityPublisher, newID IDGenerator) (*RegisterActivity, error) {
	if publisher == nil || newID == nil {
		return nil, errors.New("register activity: publisher and id generator are required")
	}
	return &RegisterActivity{publisher: publisher, newID: newID}, nil
}

// Register publishes ev and returns the event ID carried in the record's
// event-id header. The ID is for tracing only; it is never used to
// deduplicate. Register does not retry: the caller decides.
func (u *RegisterActivity) Register(ctx context.Context, ev domain.ActivityEvent) (uuid.UUID, error) {
	id := u.newID()
	if err := u.publisher.Publish(ctx, id, ev); err != nil {
		return uuid.UUID{}, fmt.Errorf("register activity %s: %w", id, err)
	}
	return id, nil
}
