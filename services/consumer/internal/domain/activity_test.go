package domain_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
)

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return ts
}

func mustEvent(t *testing.T, user string, typ domain.ActivityType, ts string) domain.ActivityEvent {
	t.Helper()
	e, err := domain.NewActivityEvent(user, typ, mustTime(t, ts), nil)
	if err != nil {
		t.Fatalf("NewActivityEvent(%q, %q, %q): %v", user, typ, ts, err)
	}
	return e
}

func utcDate(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

func TestNewActivityEvent(t *testing.T) {
	t.Parallel()
	ts := time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC)
	tooManyMetadata := make(map[string]string, domain.MaxMetadataEntries+1)
	for i := range domain.MaxMetadataEntries + 1 {
		tooManyMetadata[strings.Repeat("k", i+1)] = "v"
	}
	fullMetadata := make(map[string]string, domain.MaxMetadataEntries)
	for i := range domain.MaxMetadataEntries {
		fullMetadata[strings.Repeat("k", i+1)] = "v"
	}

	tests := []struct {
		name    string
		user    string
		typ     domain.ActivityType
		ts      time.Time
		md      map[string]string
		wantErr bool
	}{
		{name: "should accept a page_view event", user: "u1", typ: domain.ActivityPageView, ts: ts},
		{name: "should accept an unknown activity type", user: "u1", typ: "foo", ts: ts},
		{name: "should accept metadata at the entry limit", user: "u1", typ: "click", ts: ts, md: fullMetadata},
		{name: "should accept user_id at the byte limit", user: strings.Repeat("u", domain.MaxUserIDBytes), typ: "click", ts: ts},
		{name: "should accept activity_type at the byte limit", user: "u1", typ: domain.ActivityType(strings.Repeat("t", domain.MaxActivityTypeBytes)), ts: ts},
		{name: "should accept a metadata value at the byte limit", user: "u1", typ: "click", ts: ts, md: map[string]string{"k": strings.Repeat("v", domain.MaxMetadataValBytes)}},
		{name: "should reject when user_id is empty", user: "", typ: "click", ts: ts, wantErr: true},
		{name: "should reject when user_id is too long", user: strings.Repeat("u", domain.MaxUserIDBytes+1), typ: "click", ts: ts, wantErr: true},
		{name: "should reject when user_id is not valid UTF-8", user: "u\xff", typ: "click", ts: ts, wantErr: true},
		{name: "should reject when user_id contains NUL", user: "a\x00b", typ: "click", ts: ts, wantErr: true},
		{name: "should reject when user_id contains a newline", user: "u\n1", typ: "click", ts: ts, wantErr: true},
		{name: "should reject when user_id contains a C1 control character", user: "u\u00851", typ: "click", ts: ts, wantErr: true},
		{name: "should reject when activity_type contains a control character", user: "u1", typ: "cl\tick", ts: ts, wantErr: true},
		{name: "should accept non-ASCII letters and spaces in user_id", user: "José da Silva", typ: "click", ts: ts},
		{name: "should accept control characters in metadata like the producer", user: "u1", typ: "click", ts: ts, md: map[string]string{"k": "line1\nline2"}},
		{name: "should reject when activity_type is empty", user: "u1", typ: "", ts: ts, wantErr: true},
		{name: "should reject when activity_type is too long", user: "u1", typ: domain.ActivityType(strings.Repeat("t", domain.MaxActivityTypeBytes+1)), ts: ts, wantErr: true},
		{name: "should reject when timestamp is zero", user: "u1", typ: "click", ts: time.Time{}, wantErr: true},
		{name: "should accept the last second of year 9999 UTC", user: "u1", typ: "click", ts: time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)},
		{name: "should accept the first second after the zero time", user: "u1", typ: "click", ts: time.Date(1, 1, 1, 0, 0, 1, 0, time.UTC)},
		{name: "should reject when the UTC year is 10000", user: "u1", typ: "click", ts: mustTime(t, "9999-12-31T23:59:59-01:00"), wantErr: true},
		{name: "should reject when the UTC year is 0", user: "u1", typ: "click", ts: mustTime(t, "0000-01-01T00:00:00+01:00"), wantErr: true},
		{name: "should reject when metadata has too many entries", user: "u1", typ: "click", ts: ts, md: tooManyMetadata, wantErr: true},
		{name: "should reject when a metadata key is empty", user: "u1", typ: "click", ts: ts, md: map[string]string{"": "v"}, wantErr: true},
		{name: "should reject when a metadata key is too long", user: "u1", typ: "click", ts: ts, md: map[string]string{strings.Repeat("k", domain.MaxMetadataKeyBytes+1): "v"}, wantErr: true},
		{name: "should reject when a metadata value is too long", user: "u1", typ: "click", ts: ts, md: map[string]string{"k": strings.Repeat("v", domain.MaxMetadataValBytes+1)}, wantErr: true},
		{name: "should reject when a metadata value is not valid UTF-8", user: "u1", typ: "click", ts: ts, md: map[string]string{"k": "\xff"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e, err := domain.NewActivityEvent(tt.user, tt.typ, tt.ts, tt.md)
			if tt.wantErr {
				if !errors.Is(err, domain.ErrInvalidEvent) {
					t.Fatalf("err = %v, want ErrInvalidEvent", err)
				}
				if diff := cmp.Diff(domain.ActivityEvent{}, e); diff != "" {
					t.Errorf("event on error is not zero (-want +got):\n%s", diff)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if e.UserID != tt.user || e.ActivityType != tt.typ || !e.Timestamp.Equal(tt.ts) {
				t.Errorf("event = %+v, want fields from input", e)
			}
		})
	}
}

func TestNewActivityEventNormalizesToUTCAndClonesMetadata(t *testing.T) {
	t.Parallel()
	md := map[string]string{"page": "/home"}
	ts := mustTime(t, "2024-07-01T23:30:00-03:00")

	e, err := domain.NewActivityEvent("u1", "click", ts, md)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e.Timestamp.Location() != time.UTC {
		t.Errorf("Timestamp location = %v, want UTC", e.Timestamp.Location())
	}
	md["page"] = "/changed"
	if got := e.Metadata["page"]; got != "/home" {
		t.Errorf("Metadata aliases the caller's map: got %q, want %q", got, "/home")
	}
}

func TestActivityDate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ts   string
		want time.Time
	}{
		{name: "should keep the day when the instant is the last millisecond of a UTC day", ts: "2024-07-01T23:59:59.999Z", want: utcDate(2024, 7, 1)},
		{name: "should move to the next day when the instant is UTC midnight", ts: "2024-07-02T00:00:00Z", want: utcDate(2024, 7, 2)},
		{name: "should use the UTC day when a negative offset crosses midnight", ts: "2024-07-01T23:30:00-03:00", want: utcDate(2024, 7, 2)},
		{name: "should use the UTC day when a positive offset crosses midnight", ts: "2024-07-02T01:00:00+05:00", want: utcDate(2024, 7, 1)},
		{name: "should handle a leap day", ts: "2024-02-29T12:00:00Z", want: utcDate(2024, 2, 29)},
		{name: "should handle the year boundary", ts: "2024-12-31T22:00:00-05:00", want: utcDate(2025, 1, 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := mustEvent(t, "u1", "click", tt.ts).ActivityDate()
			if !got.Equal(tt.want) || got.Location() != time.UTC {
				t.Errorf("ActivityDate() = %v, want %v (UTC)", got, tt.want)
			}
		})
	}
}

func TestDailyUserAggregateApply(t *testing.T) {
	t.Parallel()
	day := utcDate(2024, 7, 1)
	tests := []struct {
		name      string
		events    []domain.ActivityType
		wantCount int64
		wantViews int64
	}{
		{name: "should count three activities when the same user repeats on one day", events: []domain.ActivityType{"click", "click", "click"}, wantCount: 3},
		{name: "should count page views only for page_view when types are mixed", events: []domain.ActivityType{domain.ActivityPageView, "click", "foo"}, wantCount: 3, wantViews: 1},
		{name: "should count every page_view", events: []domain.ActivityType{domain.ActivityPageView, domain.ActivityPageView}, wantCount: 2, wantViews: 2},
		{name: "should not count page views when the type only resembles page_view", events: []domain.ActivityType{"Page_View", "page_view ", "pageview", "page-view"}, wantCount: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			agg := domain.DailyUserAggregate{ActivityDate: day, UserID: "u1"}
			for _, typ := range tt.events {
				if err := agg.Apply(mustEvent(t, "u1", typ, "2024-07-01T10:00:00Z")); err != nil {
					t.Fatalf("Apply(%q): %v", typ, err)
				}
			}
			want := domain.DailyUserAggregate{ActivityDate: day, UserID: "u1", ActivityCount: tt.wantCount, PageViewCount: tt.wantViews}
			if diff := cmp.Diff(want, agg); diff != "" {
				t.Errorf("aggregate mismatch (-want +got):\n%s", diff)
			}
			if agg.PageViewCount > agg.ActivityCount {
				t.Errorf("page_view_count %d > activity_count %d", agg.PageViewCount, agg.ActivityCount)
			}
		})
	}
}

func TestDailyUserAggregateApplyRejectsForeignEvents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		event domain.ActivityEvent
	}{
		{name: "should reject when the user differs", event: mustEvent(t, "u2", "click", "2024-07-01T10:00:00Z")},
		{name: "should reject when the UTC date differs", event: mustEvent(t, "u1", "click", "2024-07-01T23:30:00-03:00")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			agg := domain.DailyUserAggregate{ActivityDate: utcDate(2024, 7, 1), UserID: "u1"}
			if err := agg.Apply(tt.event); !errors.Is(err, domain.ErrKeyMismatch) {
				t.Fatalf("err = %v, want ErrKeyMismatch", err)
			}
			if agg.ActivityCount != 0 || agg.PageViewCount != 0 {
				t.Errorf("aggregate changed on mismatch: %+v", agg)
			}
		})
	}
}
