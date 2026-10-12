package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

// promSample is one sample of the Prometheus text exposition format.
type promSample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// promSet is a parsed /metrics scrape.
type promSet []promSample

// parseProm parses the Prometheus text exposition format (version 0.0.4).
// Comments, HELP and TYPE lines are ignored; malformed lines are an error.
func parseProm(r io.Reader) (promSet, error) {
	var set promSet
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	n := 0
	for sc.Scan() {
		n++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		s, err := parsePromLine(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", n, err)
		}
		set = append(set, s)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan metrics: %w", err)
	}
	return set, nil
}

func parsePromLine(line string) (promSample, error) {
	var s promSample
	i := strings.IndexAny(line, "{ \t")
	if i <= 0 {
		return s, fmt.Errorf("parse sample %q: missing name or value", line)
	}
	s.Name = line[:i]
	rest := line[i:]
	s.Labels = map[string]string{}
	if strings.HasPrefix(rest, "{") {
		end, err := parsePromLabels(rest, s.Labels)
		if err != nil {
			return s, fmt.Errorf("parse sample %q: %w", line, err)
		}
		rest = rest[end:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return s, fmt.Errorf("parse sample %q: missing value", line)
	}
	v, err := parsePromValue(fields[0])
	if err != nil {
		return s, fmt.Errorf("parse sample %q: %w", line, err)
	}
	s.Value = v
	return s, nil
}

// parsePromLabels parses `{a="b",c="d"}` at the start of s into dst and
// returns the index just past the closing brace.
func parsePromLabels(s string, dst map[string]string) (int, error) {
	i := 1
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i >= len(s) {
			return 0, fmt.Errorf("unterminated labels")
		}
		if s[i] == '}' {
			return i + 1, nil
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 {
			return 0, fmt.Errorf("label without '='")
		}
		name := strings.TrimSpace(s[i : i+eq])
		i += eq + 1
		if i >= len(s) || s[i] != '"' {
			return 0, fmt.Errorf("label %q: value not quoted", name)
		}
		i++
		var val strings.Builder
		for {
			if i >= len(s) {
				return 0, fmt.Errorf("label %q: unterminated value", name)
			}
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				switch s[i+1] {
				case 'n':
					val.WriteByte('\n')
				default:
					val.WriteByte(s[i+1])
				}
				i += 2
				continue
			}
			if c == '"' {
				i++
				break
			}
			val.WriteByte(c)
			i++
		}
		dst[name] = val.String()
	}
}

func parsePromValue(s string) (float64, error) {
	switch s {
	case "+Inf", "Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(s, 64)
}

// matches reports whether every label in want is present with that value.
func (s promSample) matches(want map[string]string) bool {
	for k, v := range want {
		if s.Labels[k] != v {
			return false
		}
	}
	return true
}

// sum adds every sample of name whose labels match want.
// found is false when no sample matched.
func (p promSet) sum(name string, want map[string]string) (total float64, found bool) {
	for _, s := range p {
		if s.Name == name && s.matches(want) {
			total += s.Value
			found = true
		}
	}
	return total, found
}

// byLabel sums the samples of name grouped by the value of label.
func (p promSet) byLabel(name, label string) map[string]float64 {
	out := map[string]float64{}
	for _, s := range p {
		if s.Name == name {
			out[s.Labels[label]] += s.Value
		}
	}
	return out
}

// bucket is one cumulative histogram bucket.
type bucket struct {
	LE    float64
	Count float64
}

// buckets returns the cumulative buckets of histogram name, summed over all
// other labels and sorted by upper bound.
func (p promSet) buckets(name string) []bucket {
	acc := map[float64]float64{}
	for _, s := range p {
		if s.Name != name+"_bucket" {
			continue
		}
		le, err := parsePromValue(s.Labels["le"])
		if err != nil {
			continue
		}
		acc[le] += s.Value
	}
	out := make([]bucket, 0, len(acc))
	for le, c := range acc {
		out = append(out, bucket{LE: le, Count: c})
	}
	slices.SortFunc(out, func(a, b bucket) int { return cmpFloat(a.LE, b.LE) })
	return out
}

func cmpFloat(a, b float64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// counterDelta returns after-before for a counter. A negative difference means
// the process restarted between the scrapes (counter reset); the after value
// is then the best available lower bound and reset is true.
func counterDelta(before, after float64) (delta float64, reset bool) {
	if after < before {
		return after, true
	}
	return after - before, false
}

// bucketDelta subtracts the before histogram from the after one, bucket by
// bucket. On a counter reset the after histogram is returned as is.
func bucketDelta(before, after []bucket) (out []bucket, reset bool) {
	prev := map[float64]float64{}
	for _, b := range before {
		prev[b.LE] = b.Count
	}
	out = make([]bucket, 0, len(after))
	for _, b := range after {
		if b.Count < prev[b.LE] {
			return slices.Clone(after), true
		}
		out = append(out, bucket{LE: b.LE, Count: b.Count - prev[b.LE]})
	}
	return out, false
}

// histogramQuantile estimates quantile q (0..1) from cumulative buckets with
// linear interpolation inside the target bucket, like PromQL's
// histogram_quantile. ok is false when there are no observations.
func histogramQuantile(q float64, bs []bucket) (v float64, ok bool) {
	if len(bs) == 0 {
		return 0, false
	}
	total := bs[len(bs)-1].Count
	if total <= 0 {
		return 0, false
	}
	rank := q * total
	lowerBound, lowerCount := 0.0, 0.0
	for _, b := range bs {
		if b.Count >= rank {
			if math.IsInf(b.LE, 1) {
				// Quantile falls in the overflow bucket: the best estimate is
				// the largest finite bound.
				return lowerBound, true
			}
			inBucket := b.Count - lowerCount
			if inBucket <= 0 {
				return b.LE, true
			}
			return lowerBound + (b.LE-lowerBound)*(rank-lowerCount)/inBucket, true
		}
		lowerBound, lowerCount = b.LE, b.Count
	}
	return lowerBound, true
}
