package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// BenchmarkDecodeRecord measures the per-record cost of the consumer's decode and
// validate path for the accepted case and for a record that becomes a dead letter.
func BenchmarkDecodeRecord(b *testing.B) {
	headers := []kgo.RecordHeader{{Key: headerEventID, Value: []byte("0199d3a4-7c2e-7b1a-9f3e-2a4b6c8d0e1f")}, {Key: headerSchemaVersion, Value: []byte("1")}}
	cases := []struct {
		name  string
		value string
	}{
		{"valid", `{"user_id":"12345","activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z","metadata":{"page_url":"https://example.com/home","referrer":"https://google.com"}}`},
		{"invalid", `{"user_id":"","activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z"}`},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			r := &kgo.Record{Topic: testTopic, Key: []byte("12345"), Value: []byte(c.value), Headers: headers}
			b.ReportAllocs()
			for b.Loop() {
				decodeRecord(r)
			}
		})
	}
}
