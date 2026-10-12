package tracectx

import (
	"context"
	"errors"
	"strings"
	"testing"
)

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

func TestStart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		traceparent string
		tracestate  []string
		sameTrace   bool
		wantState   string
		wantFlags   byte
	}{
		{"should continue a valid trace and pass tracestate through", specHeader, []string{"vendor=opaque,other=1"}, true, "vendor=opaque,other=1", 1},
		{"should join several tracestate headers", specHeader, []string{"a=1", "b=2"}, true, "a=1,b=2", 1},
		{"should ignore empty tracestate values", specHeader, []string{"", "a=1", ""}, true, "a=1", 1},
		{"should keep an unsampled flag", "00-" + specTraceID + "-" + specSpanID + "-00", nil, true, "", 0},
		{"should accept a tracestate of exactly 512 bytes", specHeader, []string{strings.Repeat("a", 512)}, true, strings.Repeat("a", 512), 1},
		{"should drop a tracestate over 512 bytes", specHeader, []string{strings.Repeat("a", 513)}, true, "", 1},
		{"should drop a tracestate with control characters", specHeader, []string{"a=1\r\nX: y"}, true, "", 1},
		{"should drop a tracestate with non-ASCII bytes", specHeader, []string{"a=é"}, true, "", 1},
		{"should start a new trace when traceparent is missing and drop tracestate", "", []string{"a=1"}, false, "", 1},
		{"should start a new trace when traceparent is invalid and drop tracestate", "garbage", []string{"a=1"}, false, "", 1},
		{"should start a new trace for an all-zero trace id", "00-00000000000000000000000000000000-" + specSpanID + "-01", []string{"a=1"}, false, "", 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Start(tt.traceparent, tt.tracestate...)
			if !validIDs(got) {
				t.Fatalf("Start returned malformed IDs: %+v", got)
			}
			if (got.TraceID == specTraceID) != tt.sameTrace {
				t.Errorf("trace id = %s, same trace = %v, want %v", got.TraceID, got.TraceID == specTraceID, tt.sameTrace)
			}
			if got.SpanID == specSpanID {
				t.Error("span id was not replaced by a new one")
			}
			if got.State != tt.wantState {
				t.Errorf("State = %q, want %q", got.State, tt.wantState)
			}
			if got.Flags != tt.wantFlags {
				t.Errorf("Flags = %d, want %d", got.Flags, tt.wantFlags)
			}
		})
	}
}

func TestNewTraceAndChildAreRandom(t *testing.T) {
	t.Parallel()
	a, b := NewTrace(), NewTrace()
	if !validIDs(a) || !validIDs(b) {
		t.Fatalf("malformed IDs: %+v %+v", a, b)
	}
	if a.TraceID == b.TraceID || a.SpanID == b.SpanID {
		t.Error("two new traces share an ID")
	}
	c := a.Child()
	if c.TraceID != a.TraceID || c.SpanID == a.SpanID || c.Flags != a.Flags || !validIDs(c) {
		t.Errorf("Child() = %+v, parent %+v", c, a)
	}
}

func TestContextCarriesTheSpan(t *testing.T) {
	t.Parallel()
	if _, ok := FromContext(context.Background()); ok {
		t.Error("FromContext on an empty context reported a span")
	}
	want := NewTrace()
	got, ok := FromContext(NewContext(context.Background(), want))
	if !ok || got != want {
		t.Errorf("FromContext = %+v, %v, want %+v", got, ok, want)
	}
}

// validIDs checks the invariants of a generated Context via the parser.
func validIDs(c Context) bool {
	p, err := Parse(c.Traceparent())
	return err == nil && p.TraceID == c.TraceID && p.SpanID == c.SpanID
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
		f.Add(seed, "a=1")
	}
	f.Fuzz(func(t *testing.T, traceparent, tracestate string) {
		c, err := Parse(traceparent)
		if err != nil {
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("Parse(%q) error %v does not wrap ErrInvalid", traceparent, err)
			}
			if got := Start(traceparent, tracestate); got.State != "" || !validIDs(got) {
				t.Fatalf("Start(%q) after a parse error = %+v, want a fresh trace without state", traceparent, got)
			}
			return
		}
		// A parsed value always re-formats into a valid version 00 header
		// that parses back to the same IDs and flags.
		again, err := Parse(c.Traceparent())
		if err != nil || again != c {
			t.Fatalf("round trip of %q: %+v, %v", traceparent, again, err)
		}
		got := Start(traceparent, tracestate)
		if got.TraceID != c.TraceID || got.SpanID == c.SpanID || len(got.State) > maxTracestateLen {
			t.Fatalf("Start(%q, %q) = %+v from %+v", traceparent, tracestate, got, c)
		}
	})
}
