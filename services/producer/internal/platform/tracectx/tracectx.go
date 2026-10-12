// Package tracectx implements W3C Trace Context level 1
// (https://www.w3.org/TR/trace-context/) with the standard library only: it
// parses and formats the traceparent header, passes tracestate through, and
// carries the current span in a context.Context.
//
// It is transport metadata, so it lives outside domain, ports and
// application. The consumer module keeps its own copy of this small package
// because service modules share no code (ADR 0003). Replacing it with the
// OpenTelemetry propagator keeps the header format identical (ADR 0013).
package tracectx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Header names defined by the specification.
const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

const (
	// maxTracestateLen is the longest tracestate we propagate. The
	// specification asks vendors to propagate at least 512 characters and
	// allows dropping a larger value.
	maxTracestateLen = 512

	version00   = "00"
	traceIDLen  = 32
	spanIDLen   = 16
	flagsLen    = 2
	flagSampled = 0x01

	// Offsets of the fields in "vv-<trace-id>-<parent-id>-ff".
	traceIDStart = 3
	spanIDStart  = traceIDStart + traceIDLen + 1
	flagsStart   = spanIDStart + spanIDLen + 1
	// headerLen is the length of a version 00 traceparent (55) and the
	// minimum length of any later version.
	headerLen = flagsStart + flagsLen

	zeroTraceID = "00000000000000000000000000000000"
	zeroSpanID  = "0000000000000000"
)

// ErrInvalid is wrapped by Parse for every malformed traceparent value.
var ErrInvalid = errors.New("invalid traceparent")

// Context is the trace identity of one span: the position of this service
// hop in a distributed trace.
type Context struct {
	// TraceID is 32 lowercase hex characters, never all zero.
	TraceID string
	// SpanID is 16 lowercase hex characters, never all zero.
	SpanID string
	// Flags is the trace-flags byte. Only the sampled bit (0x01) is kept,
	// because the specification reserves the others.
	Flags byte
	// State is the opaque tracestate value, "" when absent.
	State string
}

// Traceparent returns the version 00 header value for c.
func (c Context) Traceparent() string {
	return version00 + "-" + c.TraceID + "-" + c.SpanID + "-" + hex.EncodeToString([]byte{c.Flags})
}

// Child returns a new span of the same trace: same trace ID, flags and
// tracestate, fresh span ID.
func (c Context) Child() Context {
	c.SpanID = newSpanID()
	return c
}

// NewTrace starts a trace with random IDs. It is marked sampled so that
// downstream tracing systems keep it; this service samples nothing itself.
func NewTrace() Context {
	return Context{TraceID: newTraceID(), SpanID: newSpanID(), Flags: flagSampled}
}

// Parse decodes a traceparent header value. Version 00 must have exactly 55
// characters. A higher version is read by its first four fields and may carry
// more data after a "-" (the specification's forward-compatibility rule);
// version ff is invalid. IDs and flags must be lowercase hex and the IDs must
// not be all zero. The returned Context has no State.
func Parse(traceparent string) (Context, error) {
	if len(traceparent) < headerLen {
		return Context{}, fmt.Errorf("%w: length %d, want at least %d", ErrInvalid, len(traceparent), headerLen)
	}
	version := traceparent[:2]
	switch {
	case !isLowerHex(version, 2) || version == "ff":
		return Context{}, fmt.Errorf("%w: version", ErrInvalid)
	case version == version00 && len(traceparent) != headerLen:
		return Context{}, fmt.Errorf("%w: version 00 length %d, want %d", ErrInvalid, len(traceparent), headerLen)
	case len(traceparent) > headerLen && traceparent[headerLen] != '-':
		return Context{}, fmt.Errorf("%w: no delimiter after the flags", ErrInvalid)
	case traceparent[2] != '-' || traceparent[spanIDStart-1] != '-' || traceparent[flagsStart-1] != '-':
		return Context{}, fmt.Errorf("%w: delimiters", ErrInvalid)
	}

	traceID := traceparent[traceIDStart : traceIDStart+traceIDLen]
	spanID := traceparent[spanIDStart : spanIDStart+spanIDLen]
	flags := traceparent[flagsStart:headerLen]
	switch {
	case !isLowerHex(traceID, traceIDLen) || traceID == zeroTraceID:
		return Context{}, fmt.Errorf("%w: trace-id", ErrInvalid)
	case !isLowerHex(spanID, spanIDLen) || spanID == zeroSpanID:
		return Context{}, fmt.Errorf("%w: parent-id", ErrInvalid)
	case !isLowerHex(flags, flagsLen):
		return Context{}, fmt.Errorf("%w: trace-flags", ErrInvalid)
	}
	var f [1]byte
	_, _ = hex.Decode(f[:], []byte(flags)) // validated above
	return Context{TraceID: traceID, SpanID: spanID, Flags: f[0] & flagSampled}, nil
}

// Start returns the Context of this service's hop for an incoming request.
// With a valid traceparent it continues that trace under a new span ID and
// passes the tracestate through when it is acceptable. Without one, or with
// an invalid one, it starts a new trace and drops the tracestate. tracestate
// holds the values of every tracestate header, in order.
func Start(traceparent string, tracestate ...string) Context {
	in, err := Parse(traceparent)
	if err != nil {
		return NewTrace()
	}
	in.State = joinTracestate(tracestate)
	return in.Child()
}

// joinTracestate combines multiple tracestate header values into one and
// returns "" when the result must be dropped: longer than 512 bytes, or with
// characters outside printable ASCII (which could inject headers or logs).
func joinTracestate(values []string) string {
	nonEmpty := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			nonEmpty = append(nonEmpty, v)
		}
	}
	state := strings.Join(nonEmpty, ",")
	if len(state) > maxTracestateLen {
		return ""
	}
	for i := range len(state) {
		if c := state[i]; c < 0x20 || c > 0x7e {
			return ""
		}
	}
	return state
}

type ctxKey struct{}

// NewContext returns a copy of ctx that carries c.
func NewContext(ctx context.Context, c Context) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

// FromContext returns the Context stored by NewContext.
func FromContext(ctx context.Context) (Context, bool) {
	c, ok := ctx.Value(ctxKey{}).(Context)
	return c, ok
}

func newTraceID() string { return randomHex(traceIDLen/2, zeroTraceID) }

func newSpanID() string { return randomHex(spanIDLen/2, zeroSpanID) }

// randomHex returns n random bytes as lowercase hex, never equal to zero.
func randomHex(n int, zero string) string {
	var b [16]byte
	for {
		_, _ = rand.Read(b[:n]) // crypto/rand.Read never returns an error (Go 1.24+)
		if s := hex.EncodeToString(b[:n]); s != zero {
			return s
		}
	}
}

// isLowerHex reports whether s is exactly n lowercase hex characters.
func isLowerHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
