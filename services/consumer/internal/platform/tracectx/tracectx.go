// Package tracectx parses the W3C Trace Context level 1 traceparent header
// (https://www.w3.org/TR/trace-context/) with the standard library only.
//
// It is the consumer's copy of the producer's package of the same name,
// reduced to what a reader of Kafka records needs: the consumer only reads
// the trace ID of a record for its log lines and forwards the trace headers
// of dead letters byte for byte, so it never starts, continues or formats a
// trace of its own beyond the round trip below. Service modules share no
// code (ADR 0003), so the parser is duplicated; both copies follow the same
// rules and tests (ADR 0013). It is transport metadata, so it lives outside
// domain, ports and application. Replacing it with the OpenTelemetry
// propagator keeps the header format identical.
package tracectx

import (
	"encoding/hex"
	"errors"
	"fmt"
)

// Header names defined by the specification.
const (
	HeaderTraceparent = "traceparent"
	HeaderTracestate  = "tracestate"
)

const (
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

// Context is the trace identity carried by a traceparent header.
type Context struct {
	// TraceID is 32 lowercase hex characters, never all zero.
	TraceID string
	// SpanID is the parent-id field: 16 lowercase hex characters, never all
	// zero.
	SpanID string
	// Flags is the trace-flags byte. Only the sampled bit (0x01) is kept,
	// because the specification reserves the others.
	Flags byte
}

// Traceparent returns the version 00 header value for c.
func (c Context) Traceparent() string {
	return version00 + "-" + c.TraceID + "-" + c.SpanID + "-" + hex.EncodeToString([]byte{c.Flags})
}

// Parse decodes a traceparent header value. Version 00 must have exactly 55
// characters. A higher version is read by its first four fields and may carry
// more data after a "-" (the specification's forward-compatibility rule);
// version ff is invalid. IDs and flags must be lowercase hex and the IDs must
// not be all zero. Because of these rules a parsed TraceID is always safe to
// log.
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
