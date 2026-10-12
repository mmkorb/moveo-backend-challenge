package httpapi

import (
	"strings"
	"testing"
)

// BenchmarkDecodeActivity measures decoding and validating one request body: the
// producer's CPU cost per accepted event before it touches Kafka.
func BenchmarkDecodeActivity(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, err := decodeActivity(strings.NewReader(validBody)); err != nil {
			b.Fatal(err)
		}
	}
}
