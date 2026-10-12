// Package domain holds the producer's core model: the ActivityEvent and the
// rules that make it valid. It imports only the standard library.
package domain

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// ActivityType is open-ended: any bounded, non-empty value is valid and only
// ActivityPageView has a special meaning downstream.
type ActivityType string

// ActivityPageView is the only activity type with special meaning (it is
// counted separately by the consumer).
const ActivityPageView ActivityType = "page_view"

// Field limits shared with contracts/user_activity.v1.schema.json. Lengths are
// counted in bytes, which is stricter than the schema's character counts for
// non-ASCII input.
const (
	MaxUserIDBytes       = 128
	MaxActivityTypeBytes = 64
	MaxMetadataEntries   = 32
	MaxMetadataKeyBytes  = 64
	MaxMetadataValBytes  = 2048
)

// Accepted range of the timestamp's UTC year: the range that RFC 3339 and JSON
// text can carry with four-digit years.
const (
	MinYear = 1
	MaxYear = 9999
)

// Field names used in validation violations. They are the ubiquitous-language
// names of the event's attributes.
const (
	FieldUserID       = "user_id"
	FieldActivityType = "activity_type"
	FieldTimestamp    = "timestamp"
	FieldMetadata     = "metadata"
)

// ErrInvalidEvent is matched (errors.Is) by every validation failure.
var ErrInvalidEvent = errors.New("invalid activity event")

// Violation describes one broken rule on one field.
type Violation struct {
	Field string
	Rule  string
}

// ValidationError lists every rule an event candidate violates. It matches
// ErrInvalidEvent with errors.Is.
type ValidationError struct {
	Violations []Violation
}

// Error implements error.
func (e *ValidationError) Error() string {
	parts := make([]string, 0, len(e.Violations))
	for _, v := range e.Violations {
		parts = append(parts, v.Field+" "+v.Rule)
	}
	return fmt.Sprintf("%s: %s", ErrInvalidEvent, strings.Join(parts, "; "))
}

// Unwrap makes errors.Is(err, ErrInvalidEvent) true.
func (e *ValidationError) Unwrap() error { return ErrInvalidEvent }

// ActivityEvent is one user action. It is valid by construction: build it
// only with NewActivityEvent.
type ActivityEvent struct {
	UserID       string
	ActivityType ActivityType
	Timestamp    time.Time // always UTC
	Metadata     map[string]string
}

// NewActivityEvent validates its input and returns an event whose Timestamp
// is normalized to UTC and whose Metadata is a private copy. On failure it
// returns a *ValidationError listing every violated rule.
func NewActivityEvent(userID string, typ ActivityType, ts time.Time, md map[string]string) (ActivityEvent, error) {
	var violations []Violation
	if !boundedName(userID, MaxUserIDBytes) {
		violations = append(violations, Violation{FieldUserID,
			fmt.Sprintf("must be 1..%d bytes of UTF-8 without control characters", MaxUserIDBytes)})
	}
	if !boundedName(string(typ), MaxActivityTypeBytes) {
		violations = append(violations, Violation{FieldActivityType,
			fmt.Sprintf("must be 1..%d bytes of UTF-8 without control characters", MaxActivityTypeBytes)})
	}
	switch {
	case ts.IsZero():
		violations = append(violations, Violation{FieldTimestamp, "is required"})
	case !yearInRange(ts):
		violations = append(violations, Violation{FieldTimestamp,
			fmt.Sprintf("must fall in UTC years %d..%d", MinYear, MaxYear)})
	}
	if v, ok := checkMetadata(md); !ok {
		violations = append(violations, v)
	}
	if len(violations) > 0 {
		return ActivityEvent{}, &ValidationError{Violations: violations}
	}
	return ActivityEvent{
		UserID:       userID,
		ActivityType: typ,
		Timestamp:    ts.UTC(),
		Metadata:     maps.Clone(md),
	}, nil
}

// ActivityDate is the UTC calendar day of the event, at 00:00 UTC.
func (e ActivityEvent) ActivityDate() time.Time {
	y, m, d := e.Timestamp.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// yearInRange reports whether ts, converted to UTC, has a year in
// MinYear..MaxYear. A valid RFC 3339 offset can push the UTC instant out of
// range (for example 9999-12-31T23:59:59-01:00).
func yearInRange(ts time.Time) bool {
	y := ts.UTC().Year()
	return y >= MinYear && y <= MaxYear
}

func checkMetadata(md map[string]string) (Violation, bool) {
	if len(md) > MaxMetadataEntries {
		return Violation{FieldMetadata, fmt.Sprintf("must have at most %d entries", MaxMetadataEntries)}, false
	}
	for k, v := range md {
		if k == "" || len(k) > MaxMetadataKeyBytes || !utf8.ValidString(k) {
			return Violation{FieldMetadata, fmt.Sprintf("keys must be 1..%d bytes of UTF-8", MaxMetadataKeyBytes)}, false
		}
		if len(v) > MaxMetadataValBytes || !utf8.ValidString(v) {
			return Violation{FieldMetadata, fmt.Sprintf("values must be at most %d bytes of UTF-8", MaxMetadataValBytes)}, false
		}
	}
	return Violation{}, true
}

// boundedName reports whether s is non-empty, at most maxBytes long, valid
// UTF-8 and free of control characters.
func boundedName(s string, maxBytes int) bool {
	if s == "" || len(s) > maxBytes || !utf8.ValidString(s) {
		return false
	}
	return !strings.ContainsFunc(s, unicode.IsControl)
}
