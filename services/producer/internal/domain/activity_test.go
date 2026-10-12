package domain

import (
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func validTS() time.Time { return time.Date(2024, 7, 1, 12, 34, 56, 0, time.UTC) }

// mustParse parses an RFC 3339 value that time.Parse accepts, including ones
// whose UTC instant lies outside years 1..9999.
func mustParse(s string) time.Time {
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return ts
}

func metadataOfSize(n int) map[string]string {
	md := make(map[string]string, n)
	for i := range n {
		md["k"+strconv.Itoa(i)] = "v"
	}
	return md
}

func TestNewActivityEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		userID    string
		typ       ActivityType
		ts        time.Time
		md        map[string]string
		wantRules []Violation // nil means success
	}{
		{name: "should accept the challenge example", userID: "12345", typ: ActivityPageView, ts: validTS(),
			md: map[string]string{"page_url": "https://example.com/home", "referrer": "https://google.com"}},
		{name: "should accept unknown activity types", userID: "u1", typ: "foo", ts: validTS()},
		{name: "should accept nil metadata", userID: "u1", typ: "click", ts: validTS()},
		{name: "should accept fields at their byte limits", userID: strings.Repeat("u", MaxUserIDBytes),
			typ: ActivityType(strings.Repeat("t", MaxActivityTypeBytes)), ts: validTS(),
			md: map[string]string{strings.Repeat("k", MaxMetadataKeyBytes): strings.Repeat("v", MaxMetadataValBytes)}},
		{name: "should accept metadata with the maximum entry count", userID: "u1", typ: "click", ts: validTS(),
			md: metadataOfSize(MaxMetadataEntries)},
		{name: "should accept an empty metadata value", userID: "u1", typ: "click", ts: validTS(),
			md: map[string]string{"k": ""}},
		{name: "should reject an empty user_id", userID: "", typ: ActivityPageView, ts: validTS(),
			wantRules: []Violation{{FieldUserID, "must be 1..128 bytes of UTF-8 without control characters"}}},
		{name: "should reject a user_id over 128 bytes", userID: strings.Repeat("u", MaxUserIDBytes+1), typ: "x", ts: validTS(),
			wantRules: []Violation{{FieldUserID, "must be 1..128 bytes of UTF-8 without control characters"}}},
		{name: "should reject a user_id with control characters", userID: "u\n1", typ: "x", ts: validTS(),
			wantRules: []Violation{{FieldUserID, "must be 1..128 bytes of UTF-8 without control characters"}}},
		{name: "should reject a user_id with invalid UTF-8", userID: "u\xff", typ: "x", ts: validTS(),
			wantRules: []Violation{{FieldUserID, "must be 1..128 bytes of UTF-8 without control characters"}}},
		{name: "should reject an activity_type over 64 bytes", userID: "u1", typ: ActivityType(strings.Repeat("t", 65)), ts: validTS(),
			wantRules: []Violation{{FieldActivityType, "must be 1..64 bytes of UTF-8 without control characters"}}},
		{name: "should reject a zero timestamp", userID: "u1", typ: "x",
			wantRules: []Violation{{FieldTimestamp, "is required"}}},
		{name: "should accept the last second of year 9999 UTC", userID: "u1", typ: "x",
			ts: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)},
		{name: "should accept the first second after the zero time", userID: "u1", typ: "x",
			ts: time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC)},
		{name: "should reject a timestamp whose UTC year is 10000", userID: "u1", typ: "x",
			ts:        mustParse("9999-12-31T23:59:59-01:00"),
			wantRules: []Violation{{FieldTimestamp, "must fall in UTC years 1..9999"}}},
		{name: "should reject a timestamp whose UTC year is 0", userID: "u1", typ: "x",
			ts:        mustParse("0000-01-01T00:00:00+01:00"),
			wantRules: []Violation{{FieldTimestamp, "must fall in UTC years 1..9999"}}},
		{name: "should reject too many metadata entries", userID: "u1", typ: "x", ts: validTS(), md: metadataOfSize(MaxMetadataEntries + 1),
			wantRules: []Violation{{FieldMetadata, "must have at most 32 entries"}}},
		{name: "should reject an empty metadata key", userID: "u1", typ: "x", ts: validTS(), md: map[string]string{"": "v"},
			wantRules: []Violation{{FieldMetadata, "keys must be 1..64 bytes of UTF-8"}}},
		{name: "should reject a metadata key over 64 bytes", userID: "u1", typ: "x", ts: validTS(),
			md:        map[string]string{strings.Repeat("k", 65): "v"},
			wantRules: []Violation{{FieldMetadata, "keys must be 1..64 bytes of UTF-8"}}},
		{name: "should reject a metadata value over 2048 bytes", userID: "u1", typ: "x", ts: validTS(),
			md:        map[string]string{"k": strings.Repeat("v", MaxMetadataValBytes+1)},
			wantRules: []Violation{{FieldMetadata, "values must be at most 2048 bytes of UTF-8"}}},
		{name: "should list every violated field when several are invalid", userID: "", typ: "",
			wantRules: []Violation{
				{FieldUserID, "must be 1..128 bytes of UTF-8 without control characters"},
				{FieldActivityType, "must be 1..64 bytes of UTF-8 without control characters"},
				{FieldTimestamp, "is required"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ev, err := NewActivityEvent(tt.userID, tt.typ, tt.ts, tt.md)
			if tt.wantRules == nil {
				if err != nil {
					t.Fatalf("NewActivityEvent() error = %v, want nil", err)
				}
				if ev.UserID != tt.userID || ev.ActivityType != tt.typ || !ev.Timestamp.Equal(tt.ts) {
					t.Errorf("NewActivityEvent() = %+v, fields not preserved", ev)
				}
				return
			}
			if !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("NewActivityEvent() error = %v, want ErrInvalidEvent", err)
			}
			verr, ok := errors.AsType[*ValidationError](err)
			if !ok {
				t.Fatalf("NewActivityEvent() error = %T, want *ValidationError", err)
			}
			if diff := cmp.Diff(tt.wantRules, verr.Violations); diff != "" {
				t.Errorf("violations mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestNewActivityEventNormalizesToUTC(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 7, 1, 23, 30, 0, 0, time.FixedZone("BRT", -3*3600))
	ev, err := NewActivityEvent("u1", ActivityPageView, ts, nil)
	if err != nil {
		t.Fatalf("NewActivityEvent() error = %v", err)
	}
	if ev.Timestamp.Location() != time.UTC {
		t.Errorf("Timestamp location = %v, want UTC", ev.Timestamp.Location())
	}
	if want := time.Date(2024, 7, 2, 2, 30, 0, 0, time.UTC); !ev.Timestamp.Equal(want) || ev.Timestamp.String() != want.String() {
		t.Errorf("Timestamp = %v, want %v", ev.Timestamp, want)
	}
}

func TestNewActivityEventCopiesMetadata(t *testing.T) {
	t.Parallel()
	md := map[string]string{"page_url": "https://example.com"}
	ev, err := NewActivityEvent("u1", ActivityPageView, validTS(), md)
	if err != nil {
		t.Fatalf("NewActivityEvent() error = %v", err)
	}
	md["page_url"] = "changed"
	if got := ev.Metadata["page_url"]; got != "https://example.com" {
		t.Errorf("Metadata aliased the caller's map: got %q", got)
	}
}

func TestActivityDate(t *testing.T) {
	t.Parallel()
	day := func(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }
	tests := []struct {
		name string
		ts   string
		want time.Time
	}{
		{"should keep the day when just before UTC midnight", "2024-07-01T23:59:59.999Z", day(2024, 7, 1)},
		{"should move to the next day at UTC midnight", "2024-07-02T00:00:00Z", day(2024, 7, 2)},
		{"should use the UTC day when the offset is negative", "2024-07-01T23:30:00-03:00", day(2024, 7, 2)},
		{"should use the UTC day when the offset is positive", "2024-07-02T01:00:00+05:00", day(2024, 7, 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ts, err := time.Parse(time.RFC3339, tt.ts)
			if err != nil {
				t.Fatalf("parse %q: %v", tt.ts, err)
			}
			ev, err := NewActivityEvent("u1", ActivityPageView, ts, nil)
			if err != nil {
				t.Fatalf("NewActivityEvent() error = %v", err)
			}
			if got := ev.ActivityDate(); !got.Equal(tt.want) {
				t.Errorf("ActivityDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidationErrorMessage(t *testing.T) {
	t.Parallel()
	err := &ValidationError{Violations: []Violation{{"user_id", "is bad"}, {"timestamp", "is required"}}}
	want := "invalid activity event: user_id is bad; timestamp is required"
	if got := err.Error(); got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}
