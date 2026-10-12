// Package ports declares the interfaces the producer's core needs from the
// outside world. It imports only the standard library and domain.
package ports

import (
	"context"
	"errors"
	"uuid"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
)

// ErrPublishUnavailable is wrapped by publishers when the broker cannot be
// reached or did not acknowledge the record in time. The HTTP edge maps it
// to 503 Service Unavailable.
var ErrPublishUnavailable = errors.New("activity publisher unavailable")

// ActivityPublisher durably publishes one ActivityEvent. Publish returns only
// after the broker acknowledged the record, or with an error.
type ActivityPublisher interface {
	Publish(ctx context.Context, eventID uuid.UUID, ev domain.ActivityEvent) error
}
