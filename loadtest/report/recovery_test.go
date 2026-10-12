package main

import (
	"strings"
	"testing"
)

const sampleLog = `{"time":"2026-10-12T03:00:17.123Z","level":"INFO","msg":"starting","workers":3}
not json at all
{"time":"2026-10-12T03:00:18.100Z","level":"INFO","msg":"no stored position; resuming from the Kafka committed offset","member":0,"partition":0,"committed_offset":-1}
{"time":"2026-10-12T03:02:07.630Z","level":"INFO","msg":"starting","workers":3}
{"time":"2026-10-12T03:02:09.500Z","level":"INFO","msg":"resuming from the stored position","member":1,"partition":3,"stored_offset":2310,"committed_offset":2279}
{"time":"2026-10-12T03:02:09.501Z","level":"WARN","msg":"Kafka committed offset is ahead of the stored position; resuming from the stored position","member":1,"partition":4,"committed_offset":900,"stored_offset":800}
{"time":"2026-10-12T03:02:09.502Z","level":"INFO","msg":"resuming from the stored position","member":1,"partition":5,"stored_offset":500,"committed_offset":-1}
`

func TestParseConsumerLog(t *testing.T) {
	t.Parallel()
	starts, events, err := parseConsumerLog(strings.NewReader(sampleLog))
	if err != nil {
		t.Fatal(err)
	}
	if len(starts) != 2 {
		t.Fatalf("starts = %d, want 2", len(starts))
	}
	if len(events) != 3 {
		t.Fatalf("events = %d, want 3 (the committed-ahead warning is not a resume line)", len(events))
	}
	if events[0].HasStored {
		t.Error("first event has a stored position, want none")
	}
	if _, ok := events[0].Gap(); ok {
		t.Error("an event without a stored position must have no gap")
	}
	if g, ok := events[1].Gap(); !ok || g != 31 {
		t.Errorf("gap = %v, %v; want 31, true", g, ok)
	}
	// Nothing committed (-1) counts from offset 0: all 500 records are protected.
	if g, ok := events[2].Gap(); !ok || g != 500 {
		t.Errorf("gap without a committed offset = %v, %v; want 500, true", g, ok)
	}
}

func TestParsePoison(t *testing.T) {
	t.Parallel()
	p := parsePoison(object{
		"count":   50.0,
		"classes": map[string]any{"decode": 26.0, "validation": 24.0},
		"kinds":   map[string]any{"b": 1.0, "a": 2.0},
	})
	if p.Count.V != 50 || p.Classes["decode"] != 26 || p.Classes["validation"] != 24 {
		t.Errorf("parsePoison = %+v", p)
	}
	if len(p.Kinds) != 2 || p.Kinds[0].K != "a" {
		t.Errorf("kinds = %v, want sorted a, b", p.Kinds)
	}
}
