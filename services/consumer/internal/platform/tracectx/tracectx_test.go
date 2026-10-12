package tracectx

import (
	"errors"
	"strings"
	"testing"
)

// The parser tests mirror the producer's copy of this package (ADR 0013).
const (
	specTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	specSpanID  = "00f067aa0ba902b7"
	specHeader  = "00-" + specTraceID + "-" + specSpanID + "-01"
)

func TestParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    Context
		invalid bool
	}{
		{"should parse the specification example", specHeader, Context{TraceID: specTraceID, SpanID: specSpanID, Flags: 1}, false},
		{"should parse a not sampled trace", "00-" + specTraceID + "-" + specSpanID + "-00", Context{TraceID: specTraceID, SpanID: specSpanID}, false},
		{"should keep only the sampled bit", "00-" + specTraceID + "-" + specSpanID + "-09", Context{TraceID: specTraceID, SpanID: specSpanID, Flags: 1}, false},
		{"should parse a future version with extra fields", "cc-" + specTraceID + "-" + specSpanID + "-01-what-the-future-will-be-like", Context{TraceID: specTraceID, SpanID: specSpanID, Flags: 1}, false},
		{"should parse a future version of exactly 55 characters", "01-" + specTraceID + "-" + specSpanID + "-01", Context{TraceID: specTraceID, SpanID: specSpanID, Flags: 1}, false},
		{"should reject the empty string", "", Context{}, true},
		{"should reject version ff", "ff-" + specTraceID + "-" + specSpanID + "-01", Context{}, true},
		{"should reject uppercase version", "0A-" + specTraceID + "-" + specSpanID + "-01", Context{}, true},
		{"should reject a non-hex version", "0g-" + specTraceID + "-" + specSpanID + "-01", Context{}, true},
		{"should reject an all-zero trace id", "00-00000000000000000000000000000000-" + specSpanID + "-01", Context{}, true},
		{"should reject an all-zero parent id", "00-" + specTraceID + "-0000000000000000-01", Context{}, true},
		{"should reject an uppercase trace id", "00-" + strings.ToUpper(specTraceID) + "-" + specSpanID + "-01", Context{}, true},
		{"should reject an uppercase parent id", "00-" + specTraceID + "-" + strings.ToUpper(specSpanID) + "-01", Context{}, true},
		{"should reject uppercase flags", "00-" + specTraceID + "-" + specSpanID + "-0A", Context{}, true},
		{"should reject non-hex flags", "00-" + specTraceID + "-" + specSpanID + "-zz", Context{}, true},
		{"should reject a short value", specHeader[:54], Context{}, true},
		{"should reject a short trace id", "00-" + specTraceID[:30] + "-" + specSpanID + "00-01", Context{}, true},
		{"should reject version 00 with trailing data", specHeader + "-extra", Context{}, true},
		{"should reject version 00 with a trailing space", specHeader + " ", Context{}, true},
		{"should reject a future version with a bad delimiter", "cc-" + specTraceID + "-" + specSpanID + "-01.extra", Context{}, true},
		{"should reject wrong delimiters", "00_" + specTraceID + "_" + specSpanID + "_01", Context{}, true},
		{"should reject a non-ASCII value", "00-" + specTraceID + "-" + specSpanID + "-0é", Context{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := Parse(tt.in)
			if tt.invalid {
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("Parse(%q) error = %v, want ErrInvalid", tt.in, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Parse(%q) error = %v", tt.in, err)
			}
			if got != tt.want {
				t.Errorf("Parse(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

func TestTraceparentRoundTrip(t *testing.T) {
	t.Parallel()
	c, err := Parse(specHeader)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Traceparent(); got != specHeader {
		t.Errorf("Traceparent() = %q, want %q", got, specHeader)
	}
}

func FuzzParse(f *testing.F) {
	for _, seed := range []string{
		specHeader,
		"cc-" + specTraceID + "-" + specSpanID + "-01-future",
		"ff-" + specTraceID + "-" + specSpanID + "-01",
		"00-00000000000000000000000000000000-" + specSpanID + "-01",
		"",
		"00",
		strings.Repeat("-", 55),
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, traceparent string) {
		c, err := Parse(traceparent)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Parse(%q) error %v does not wrap ErrInvalid", traceparent, err)
			}
			return
		}
		// A parsed value is safe to log and re-formats into a valid version
		// 00 header that parses back to the same IDs and flags.
		if !isLowerHex(c.TraceID, traceIDLen) {
			t.Fatalf("Parse(%q) trace id %q is not 32 lowercase hex characters", traceparent, c.TraceID)
		}
		again, err := Parse(c.Traceparent())
		if err != nil || again != c {
			t.Fatalf("round trip of %q: %+v, %v", traceparent, again, err)
		}
	})
}
