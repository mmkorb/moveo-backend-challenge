package kafka

import (
	"encoding/json"
	"log/slog"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/tracectx"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// Record headers (ADR 0006, 0011).
const (
	headerEventID           = "event-id"
	headerSchemaVersion     = "schema-version"
	headerErrorClass        = "x-error-class"
	headerOriginalTopic     = "x-original-topic"
	headerOriginalPartition = "x-original-partition"
	headerOriginalOffset    = "x-original-offset"
	headerFailedAt          = "x-failed-at"

	// supportedSchemaVersion is the only payload version this consumer decodes;
	// a missing schema-version header means this version.
	supportedSchemaVersion = "1"
	// maxLoggedEventIDBytes caps the event-id copied into logs; a UUID is 36.
	maxLoggedEventIDBytes = 64
)

// activityMessage mirrors contracts/user_activity.v1.schema.json.
type activityMessage struct {
	UserID       string            `json:"user_id"`
	ActivityType string            `json:"activity_type"`
	Timestamp    time.Time         `json:"timestamp"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// decodeRecord maps one Kafka record to a domain event. A record that cannot be
// decoded (unknown schema version, malformed JSON, wrong types) or that fails
// domain validation is returned as a DeadLetter instead; the error itself is
// dropped on purpose, because encoding/json errors can quote payload bytes
// (ADR 0011, 0002). Unknown JSON fields are ignored (tolerant reader, ADR 0006).
func decodeRecord(r *kgo.Record) (domain.ActivityEvent, *ports.DeadLetter) {
	if v, ok := headerValue(r, headerSchemaVersion); ok && v != supportedSchemaVersion {
		return domain.ActivityEvent{}, newDeadLetter(r, ports.ErrorClassDecode)
	}
	var msg activityMessage
	if err := json.Unmarshal(r.Value, &msg); err != nil {
		return domain.ActivityEvent{}, newDeadLetter(r, ports.ErrorClassDecode)
	}
	ev, err := domain.NewActivityEvent(msg.UserID, domain.ActivityType(msg.ActivityType), msg.Timestamp, msg.Metadata)
	if err != nil {
		return domain.ActivityEvent{}, newDeadLetter(r, ports.ErrorClassValidation)
	}
	return ev, nil
}

// newDeadLetter keeps the original key, value and headers byte for byte, so the
// record can be replayed unchanged, plus its coordinates and error class.
func newDeadLetter(r *kgo.Record, class ports.ErrorClass) *ports.DeadLetter {
	headers := make([]ports.Header, 0, len(r.Headers))
	for _, h := range r.Headers {
		headers = append(headers, ports.Header{Key: h.Key, Value: h.Value})
	}
	return &ports.DeadLetter{
		Key:       r.Key,
		Value:     r.Value,
		Headers:   headers,
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Class:     class,
	}
}

// headerValue returns the value of the first header named key.
func headerValue(r *kgo.Record, key string) (string, bool) {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value), true
		}
	}
	return "", false
}

// traceID returns the trace ID of the record's traceparent header (the first
// one) when it is valid W3C Trace Context, and "" otherwise (ADR 0006, 0013).
// A missing or malformed header is never an error. The parser only accepts
// 32 lowercase hex characters, so the result is safe to log.
func traceID(headers []ports.Header) string {
	for _, h := range headers {
		if h.Key != tracectx.HeaderTraceparent {
			continue
		}
		c, err := tracectx.Parse(string(h.Value))
		if err != nil {
			return ""
		}
		return c.TraceID
	}
	return ""
}

// traceAttrs returns the trace_id log attribute of a record, or none when it
// has no valid traceparent.
func traceAttrs(headers []ports.Header) []slog.Attr {
	if id := traceID(headers); id != "" {
		return []slog.Attr{slog.String("trace_id", id)}
	}
	return nil
}

// loggableEventID returns the event-id header if it is short printable text;
// anything else written to the topic by a hostile producer is not logged.
func loggableEventID(headers []ports.Header) string {
	for _, h := range headers {
		if h.Key != headerEventID {
			continue
		}
		if len(h.Value) == 0 || len(h.Value) > maxLoggedEventIDBytes || !utf8.Valid(h.Value) {
			return "invalid"
		}
		for _, c := range string(h.Value) {
			if !unicode.IsPrint(c) || unicode.IsSpace(c) {
				return "invalid"
			}
		}
		return string(h.Value)
	}
	return ""
}
