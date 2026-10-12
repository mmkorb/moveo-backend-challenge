package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// statSample is one `docker stats --no-stream --format '{{json .}}'` line.
type statSample struct {
	Time     time.Time
	Name     string
	CPUPerc  float64
	MemUsed  float64 // bytes
	MemLimit float64 // bytes
	NetRx    float64 // bytes, cumulative
	NetTx    float64 // bytes, cumulative
}

// byteUnits covers both docker conventions: decimal units (kB, MB) used for
// network and block I/O, and binary units (KiB, MiB) used for memory.
var byteUnits = map[string]float64{
	"b":   1,
	"kb":  1e3,
	"mb":  1e6,
	"gb":  1e9,
	"tb":  1e12,
	"kib": 1 << 10,
	"mib": 1 << 20,
	"gib": 1 << 30,
	"tib": 1 << 40,
}

// parseBytes parses a docker-formatted size such as "123.4MiB", "1.2kB" or "0B".
func parseBytes(s string) (float64, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "--" {
		return 0, fmt.Errorf("parse size %q: empty", s)
	}
	i := strings.IndexFunc(s, func(r rune) bool {
		return (r < '0' || r > '9') && r != '.'
	})
	if i == 0 {
		return 0, fmt.Errorf("parse size %q: no number", s)
	}
	numPart, unitPart := s, "b"
	if i > 0 {
		numPart, unitPart = s[:i], strings.TrimSpace(s[i:])
	}
	n, err := strconv.ParseFloat(numPart, 64)
	if err != nil {
		return 0, fmt.Errorf("parse size %q: %w", s, err)
	}
	mult, ok := byteUnits[strings.ToLower(unitPart)]
	if !ok {
		return 0, fmt.Errorf("parse size %q: unknown unit %q", s, unitPart)
	}
	return n * mult, nil
}

// parsePair parses "a / b" sizes.
func parsePair(s string) (float64, float64, error) {
	a, b, ok := strings.Cut(s, "/")
	if !ok {
		return 0, 0, fmt.Errorf("parse pair %q: missing '/'", s)
	}
	x, err := parseBytes(a)
	if err != nil {
		return 0, 0, err
	}
	y, err := parseBytes(b)
	if err != nil {
		return 0, 0, err
	}
	return x, y, nil
}

// parsePercent parses "12.34%".
func parsePercent(s string) (float64, error) {
	s = strings.TrimSuffix(strings.TrimSpace(s), "%")
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("parse percent %q: %w", s, err)
	}
	return f, nil
}

// parseStatSample converts one decoded stats line. Containers that were
// stopped at sampling time report "--" and are rejected.
func parseStatSample(o object) (statSample, error) {
	var s statSample
	s.Name = o.str("Name", "Container", "name")
	if s.Name == "" {
		return s, fmt.Errorf("stats line without container name")
	}
	ts, ok := o.time("ts", "timestamp", "time", "utc", "at")
	if !ok {
		return s, fmt.Errorf("stats line for %s without timestamp", s.Name)
	}
	s.Time = ts
	var err error
	if s.CPUPerc, err = parsePercent(o.str("CPUPerc", "cpu")); err != nil {
		return s, fmt.Errorf("stats %s: %w", s.Name, err)
	}
	if s.MemUsed, s.MemLimit, err = parsePair(o.str("MemUsage", "mem")); err != nil {
		return s, fmt.Errorf("stats %s: %w", s.Name, err)
	}
	if net := o.str("NetIO", "net"); net != "" {
		if s.NetRx, s.NetTx, err = parsePair(net); err != nil {
			return s, fmt.Errorf("stats %s: %w", s.Name, err)
		}
	}
	return s, nil
}

// parseStats converts decoded stats lines, returning the samples and the
// number of lines skipped.
func parseStats(objs []object) (samples []statSample, skipped int) {
	for _, o := range objs {
		s, err := parseStatSample(o)
		if err != nil {
			skipped++
			continue
		}
		samples = append(samples, s)
	}
	return samples, skipped
}
