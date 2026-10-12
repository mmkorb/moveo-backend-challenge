package kafka

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

func TestDecodeRecord(t *testing.T) {
	ts := time.Date(2024, 7, 1, 12, 34, 56, 0, time.UTC)
	tests := []struct {
		name      string
		value     string
		headers   []kgo.RecordHeader
		wantEvent *domain.ActivityEvent
		wantClass ports.ErrorClass
	}{
		{
			name:  "should decode the challenge example",
			value: `{"user_id":"12345","activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z","metadata":{"page_url":"https://example.com/home","referrer":"https://google.com"}}`,
			wantEvent: &domain.ActivityEvent{UserID: "12345", ActivityType: domain.ActivityPageView, Timestamp: ts,
				Metadata: map[string]string{"page_url": "https://example.com/home", "referrer": "https://google.com"}},
		},
		{
			name:      "should treat a missing schema-version as 1 and ignore unknown fields",
			value:     `{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z","new_field":42}`,
			wantEvent: &domain.ActivityEvent{UserID: "u", ActivityType: "click", Timestamp: ts},
		},
		{
			name:      "should normalize a non-UTC timestamp to UTC",
			value:     `{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T09:34:56-03:00"}`,
			headers:   []kgo.RecordHeader{{Key: headerSchemaVersion, Value: []byte("1")}},
			wantEvent: &domain.ActivityEvent{UserID: "u", ActivityType: "click", Timestamp: ts},
		},
		{
			name:      "should dead-letter an unknown schema version as decode",
			value:     `{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z"}`,
			headers:   []kgo.RecordHeader{{Key: headerSchemaVersion, Value: []byte("2")}},
			wantClass: ports.ErrorClassDecode,
		},
		{name: "should dead-letter malformed JSON as decode", value: `{"user_id":"u"`, wantClass: ports.ErrorClassDecode},
		{name: "should dead-letter trailing data as decode", value: `{} {}`, wantClass: ports.ErrorClassDecode},
		{
			name:      "should dead-letter a non-string metadata value as decode",
			value:     `{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z","metadata":{"n":1}}`,
			wantClass: ports.ErrorClassDecode,
		},
		{
			name:      "should dead-letter a non-RFC 3339 timestamp as decode",
			value:     `{"user_id":"u","activity_type":"click","timestamp":"01/07/2024"}`,
			wantClass: ports.ErrorClassDecode,
		},
		{
			name:      "should dead-letter an empty user_id as validation",
			value:     `{"user_id":"","activity_type":"click","timestamp":"2024-07-01T12:34:56Z"}`,
			wantClass: ports.ErrorClassValidation,
		},
		{
			name:      "should dead-letter a missing timestamp as validation",
			value:     `{"user_id":"u","activity_type":"click"}`,
			wantClass: ports.ErrorClassValidation,
		},
		{name: "should dead-letter JSON null as validation", value: `null`, wantClass: ports.ErrorClassValidation},
		{
			// PostgreSQL text cannot hold NUL: such a record would fail the upsert with 22021 forever.
			name:      "should dead-letter a NUL in user_id as validation",
			value:     `{"user_id":"a\u0000b","activity_type":"click","timestamp":"2024-07-01T12:34:56Z"}`,
			wantClass: ports.ErrorClassValidation,
		},
		{
			name:      "should dead-letter a control character in activity_type as validation",
			value:     `{"user_id":"u","activity_type":"cl\nick","timestamp":"2024-07-01T12:34:56Z"}`,
			wantClass: ports.ErrorClassValidation,
		},
		{
			name:      "should dead-letter an oversized user_id as validation",
			value:     fmt.Sprintf(`{"user_id":%q,"activity_type":"click","timestamp":"2024-07-01T12:34:56Z"}`, strings.Repeat("a", 129)),
			wantClass: ports.ErrorClassValidation,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &kgo.Record{Topic: testTopic, Partition: 4, Offset: 77, Key: []byte("k"), Value: []byte(tt.value), Headers: tt.headers}
			ev, dl := decodeRecord(r)
			if tt.wantEvent != nil {
				if dl != nil {
					t.Fatalf("decodeRecord() dead-lettered with class %s, want an event", dl.Class)
				}
				if diff := cmp.Diff(*tt.wantEvent, ev); diff != "" {
					t.Fatalf("event mismatch (-want +got):\n%s", diff)
				}
				return
			}
			if dl == nil {
				t.Fatalf("decodeRecord() = %+v, want a dead letter", ev)
			}
			want := ports.DeadLetter{Key: r.Key, Value: r.Value, Headers: toPortHeaders(tt.headers), Topic: testTopic,
				Partition: 4, Offset: 77, Class: tt.wantClass}
			if diff := cmp.Diff(want, *dl); diff != "" {
				t.Fatalf("dead letter mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func toPortHeaders(hs []kgo.RecordHeader) []ports.Header {
	out := make([]ports.Header, 0, len(hs))
	for _, h := range hs {
		out = append(out, ports.Header{Key: h.Key, Value: h.Value})
	}
	return out
}

func TestDeadLetterRecord(t *testing.T) {
	t.Run("should keep the original bytes and headers and add exactly the x-* headers", func(t *testing.T) {
		l := ports.DeadLetter{
			Key:   []byte("user-1"),
			Value: []byte(`{"broken":`),
			Headers: []ports.Header{
				{Key: headerEventID, Value: []byte("0192a3b4-c5d6-7e8f-9012-3456789abcde")},
				{Key: headerSchemaVersion, Value: []byte("1")},
				{Key: headerErrorClass, Value: []byte("spoofed")}, // replaced, not duplicated
				{Key: "traceparent", Value: []byte(testTraceparent)},
				{Key: "tracestate", Value: []byte("vendor=opaque,other=1")},
			},
			Topic: testTopic, Partition: 5, Offset: 1234,
			Class: ports.ErrorClassDecode,
		}
		failedAt := time.Date(2026, 10, 12, 1, 2, 3, 456, time.FixedZone("x", -3*3600))
		r := deadLetterRecord("incoming.user_activity.dlq", &l, failedAt)

		if r.Topic != "incoming.user_activity.dlq" || string(r.Key) != "user-1" || string(r.Value) != `{"broken":` {
			t.Fatalf("record = topic %q key %q value %q", r.Topic, r.Key, r.Value)
		}
		got := map[string]string{}
		for _, h := range r.Headers {
			if _, dup := got[h.Key]; dup {
				t.Fatalf("duplicate header %q", h.Key)
			}
			got[h.Key] = string(h.Value)
		}
		want := map[string]string{
			headerEventID:           "0192a3b4-c5d6-7e8f-9012-3456789abcde",
			headerSchemaVersion:     "1",
			"traceparent":           testTraceparent,         // unchanged (ADR 0011)
			"tracestate":            "vendor=opaque,other=1", // unchanged
			headerErrorClass:        "decode",
			headerOriginalTopic:     testTopic,
			headerOriginalPartition: "5",
			headerOriginalOffset:    "1234",
			headerFailedAt:          "2026-10-12T04:02:03Z",
		}
		if diff := cmp.Diff(want, got); diff != "" {
			t.Fatalf("headers mismatch (-want +got):\n%s", diff)
		}
	})
}

func TestLoggableEventID(t *testing.T) {
	tests := []struct {
		name, value, want string
		present           bool
	}{
		{name: "should return a UUID unchanged", value: "0192a3b4-c5d6-7e8f-9012-3456789abcde", want: "0192a3b4-c5d6-7e8f-9012-3456789abcde", present: true},
		{name: "should hide control characters", value: "a\nb", want: "invalid", present: true},
		{name: "should hide an oversized value", value: string(make([]byte, 65)), want: "invalid", present: true},
		{name: "should return empty when the header is missing", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var hs []ports.Header
			if tt.present {
				hs = []ports.Header{{Key: headerEventID, Value: []byte(tt.value)}}
			}
			if got := loggableEventID(hs); got != tt.want {
				t.Fatalf("loggableEventID() = %q, want %q", got, tt.want)
			}
		})
	}
}

const (
	testTraceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
	testTraceparent = "00-" + testTraceID + "-00f067aa0ba902b7-01"
)

func TestTraceID(t *testing.T) {
	tests := []struct {
		name    string
		headers []ports.Header
		want    string
	}{
		{"should read the trace ID of a valid traceparent", []ports.Header{{Key: "traceparent", Value: []byte(testTraceparent)}}, testTraceID},
		{"should use the first traceparent header", []ports.Header{
			{Key: "traceparent", Value: []byte(testTraceparent)},
			{Key: "traceparent", Value: []byte("00-11111111111111111111111111111111-00f067aa0ba902b7-01")},
		}, testTraceID},
		{"should return empty without a traceparent", []ports.Header{{Key: headerEventID, Value: []byte("e")}}, ""},
		{"should return empty for an uppercase trace ID", []ports.Header{{Key: "traceparent", Value: []byte(strings.ToUpper(testTraceparent))}}, ""},
		{"should return empty for a value that could inject log content", []ports.Header{{Key: "traceparent", Value: []byte("00-" + testTraceID + "\n-00f067aa0ba902b7-01")}}, ""},
		{"should return empty for an all-zero trace ID", []ports.Header{{Key: "traceparent", Value: []byte("00-00000000000000000000000000000000-00f067aa0ba902b7-01")}}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := traceID(tt.headers); got != tt.want {
				t.Errorf("traceID() = %q, want %q", got, tt.want)
			}
			if attrs := traceAttrs(tt.headers); (len(attrs) == 1) != (tt.want != "") {
				t.Errorf("traceAttrs() = %v, want a trace_id attribute only for a valid traceparent", attrs)
			}
		})
	}
}

func TestRejectedRecordLog(t *testing.T) {
	t.Run("should log the trace ID of a rejected record and never its tracestate", func(t *testing.T) {
		cfg := testConfig()
		cfg.FlushMaxRecords = 10
		m, logs := newTestMember(t, cfg, &fakeProcessor{})
		gc := newFakeGroupClient()
		assign(m, gc, 0)

		m.dispatch(0, []*kgo.Record{
			record(0, 0, []byte("not json"),
				kgo.RecordHeader{Key: "traceparent", Value: []byte(testTraceparent)},
				kgo.RecordHeader{Key: "tracestate", Value: []byte("vendor=secret-state")}),
			record(0, 1, []byte("also not json")),
		})
		stopAll(t, m)

		var withTrace, withoutTrace int
		for line := range strings.Lines(logs.String()) {
			if !strings.Contains(line, "record rejected") {
				continue
			}
			if strings.Contains(line, `"trace_id":"`+testTraceID+`"`) {
				withTrace++
			} else if !strings.Contains(line, "trace_id") {
				withoutTrace++
			}
		}
		if withTrace != 1 || withoutTrace != 1 {
			t.Errorf("rejected-record lines with/without trace_id = %d/%d, want 1/1; logs: %s", withTrace, withoutTrace, logs)
		}
		if strings.Contains(logs.String(), "secret-state") {
			t.Error("tracestate was logged")
		}
	})
}

type pgLikeError struct{}

func (*pgLikeError) Error() string { return `violates check constraint, row ("secret-user")` }

func TestErrorType(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "should return empty for nil", err: nil, want: ""},
		{name: "should follow %w chains", err: fmt.Errorf("a: %w", fmt.Errorf("b: %w", &pgLikeError{})), want: "*kafka.pgLikeError"},
		{name: "should follow the last error of a multi-%w", err: fmt.Errorf("%w: %w", ports.ErrUnavailable, &pgLikeError{}), want: "*kafka.pgLikeError"},
		{name: "should name a plain error", err: errors.New("x"), want: "*errors.errorString"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorType(tt.err); got != tt.want {
				t.Fatalf("errorType() = %q, want %q", got, tt.want)
			}
		})
	}
}
