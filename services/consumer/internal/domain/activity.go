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

// ActivityType is open-ended: only page_view has special meaning.
type ActivityType string

const ActivityPageView ActivityType = "page_view"

const (
	MaxUserIDBytes       = 128
	MaxActivityTypeBytes = 64
	MaxMetadataEntries   = 32
	MaxMetadataKeyBytes  = 64
	MaxMetadataValBytes  = 2048
)

// Accepted range of the timestamp's UTC year; identical to the producer's rule.
const (
	MinYear = 1
	MaxYear = 9999
)

var ErrInvalidEvent = errors.New("invalid activity event")

type ActivityEvent struct {
	UserID       string
	ActivityType ActivityType
	Timestamp    time.Time // always UTC
	Metadata     map[string]string
}

func NewActivityEvent(userID string, typ ActivityType, ts time.Time, md map[string]string) (ActivityEvent, error) {
	if !boundedName(userID, MaxUserIDBytes) {
		return ActivityEvent{}, fmt.Errorf("%w: user_id must be 1..%d bytes of UTF-8 without control characters", ErrInvalidEvent, MaxUserIDBytes)
	}
	if !boundedName(string(typ), MaxActivityTypeBytes) {
		return ActivityEvent{}, fmt.Errorf("%w: activity_type must be 1..%d bytes of UTF-8 without control characters", ErrInvalidEvent, MaxActivityTypeBytes)
	}
	if ts.IsZero() {
		return ActivityEvent{}, fmt.Errorf("%w: timestamp is required", ErrInvalidEvent)
	}
	if y := ts.UTC().Year(); y < MinYear || y > MaxYear {
		return ActivityEvent{}, fmt.Errorf("%w: timestamp must fall in UTC years %d..%d", ErrInvalidEvent, MinYear, MaxYear)
	}
	if len(md) > MaxMetadataEntries {
		return ActivityEvent{}, fmt.Errorf("%w: at most %d metadata entries", ErrInvalidEvent, MaxMetadataEntries)
	}
	for k, v := range md {
		if !boundedText(k, MaxMetadataKeyBytes) || len(v) > MaxMetadataValBytes || !utf8.ValidString(v) {
			return ActivityEvent{}, fmt.Errorf("%w: metadata entry out of bounds", ErrInvalidEvent)
		}
	}
	return ActivityEvent{UserID: userID, ActivityType: typ, Timestamp: ts.UTC(), Metadata: maps.Clone(md)}, nil
}

// ActivityDate is the UTC calendar day of the event, at 00:00 UTC.
func (e ActivityEvent) ActivityDate() time.Time {
	y, m, d := e.Timestamp.UTC().Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// boundedText reports whether s is non-empty, at most maxBytes long and valid
// UTF-8. Metadata keys use this rule, like the producer's checkMetadata.
func boundedText(s string, maxBytes int) bool {
	return s != "" && len(s) <= maxBytes && utf8.ValidString(s)
}

// boundedName is boundedText plus "no control characters". The rule must match
// the producer's boundedName for user_id and activity_type: PostgreSQL text
// cannot hold NUL, so a record that bypassed the producer would otherwise fail
// the upsert with SQLSTATE 22021 and stall its partition (ADR 0011).
func boundedName(s string, maxBytes int) bool {
	return boundedText(s, maxBytes) && !strings.ContainsFunc(s, unicode.IsControl)
}

// DailyUserAggregate is a delta for one (ActivityDate, UserID). It is a value,
// not a DDD Aggregate: Postgres, not this type, guards consistency.
type DailyUserAggregate struct {
	ActivityDate  time.Time
	UserID        string
	ActivityCount int64
	PageViewCount int64
}

var ErrKeyMismatch = errors.New("event does not belong to this daily aggregate")

// Apply adds one accepted event to the delta (domain rules 1 and 2).
func (a *DailyUserAggregate) Apply(e ActivityEvent) error {
	if !a.ActivityDate.Equal(e.ActivityDate()) || a.UserID != e.UserID {
		return ErrKeyMismatch
	}
	a.ActivityCount++
	if e.ActivityType == ActivityPageView {
		a.PageViewCount++
	}
	return nil
}
