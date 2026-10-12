package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"uuid"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/tracectx"
)

// Record headers set on every record (ADR 0006).
const (
	// HeaderEventID carries the UUIDv7 returned to the client as event_id. It
	// is for tracing and DLQ correlation only, never for deduplication.
	HeaderEventID = "event-id"
	// HeaderSchemaVersion tells the consumer which decoder to use.
	HeaderSchemaVersion = "schema-version"
	// SchemaVersion is the version of contracts/user_activity.v1.schema.json.
	SchemaVersion = "1"
)

// recordHeaders returns the headers of one record: event-id and
// schema-version always. When ctx holds a trace context it adds traceparent
// (this hop's span) and, if the incoming request carried an acceptable one,
// tracestate unchanged.
func recordHeaders(ctx context.Context, eventID uuid.UUID) []kgo.RecordHeader {
	headers := []kgo.RecordHeader{
		{Key: HeaderEventID, Value: []byte(eventID.String())},
		{Key: HeaderSchemaVersion, Value: []byte(SchemaVersion)},
	}
	tc, ok := tracectx.FromContext(ctx)
	if !ok {
		return headers
	}
	headers = append(headers, kgo.RecordHeader{Key: tracectx.HeaderTraceparent, Value: []byte(tc.Traceparent())})
	if tc.State != "" {
		headers = append(headers, kgo.RecordHeader{Key: tracectx.HeaderTracestate, Value: []byte(tc.State)})
	}
	return headers
}

// activityMessage mirrors contracts/user_activity.v1.schema.json. It is the
// only type in the producer, besides the HTTP request DTO, that carries json
// tags.
type activityMessage struct {
	UserID       string            `json:"user_id"`
	ActivityType string            `json:"activity_type"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// newMessage maps a domain event to its wire DTO. The event's timestamp is
// already UTC, so it encodes with a "Z" suffix.
func newMessage(ev domain.ActivityEvent) activityMessage {
	return activityMessage{
		UserID:       ev.UserID,
		ActivityType: string(ev.ActivityType),
		Timestamp:    ev.Timestamp.UTC(),
		Metadata:     ev.Metadata,
	}
}

// encodeActivity returns the Kafka record value for ev: the JSON object of
// contracts/user_activity.v1.schema.json.
func encodeActivity(ev domain.ActivityEvent) ([]byte, error) {
	value, err := json.Marshal(newMessage(ev))
	if err != nil {
		return nil, fmt.Errorf("encode activity: %w", err)
	}
	return value, nil
}
