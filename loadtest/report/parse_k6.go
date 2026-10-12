package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"strings"
)

// k6Metric is one entry of the `metrics` map in the handleSummary data object.
type k6Metric struct {
	Type       string                 `json:"type"`
	Contains   string                 `json:"contains"`
	Values     map[string]float64     `json:"values"`
	Thresholds map[string]k6Threshold `json:"thresholds"`
}

// k6Threshold is the evaluation result of one threshold expression.
type k6Threshold struct {
	OK bool `json:"ok"`
}

// k6Summary is the subset of the k6 handleSummary data object the report uses.
type k6Summary struct {
	Metrics map[string]k6Metric `json:"metrics"`
	State   struct {
		TestRunDurationMs float64 `json:"testRunDurationMs"`
	} `json:"state"`
}

// thresholdResult is one evaluated threshold, flattened for the verdict.
type thresholdResult struct {
	Metric string
	Expr   string
	OK     bool
}

// parseK6Summary decodes k6-summary.json. A summary wrapped in {"data": …}
// is accepted too.
func parseK6Summary(b []byte) (*k6Summary, error) {
	var s k6Summary
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("decode k6 summary: %w", err)
	}
	if s.Metrics == nil {
		var wrapped struct {
			Data k6Summary `json:"data"`
		}
		if err := json.Unmarshal(b, &wrapped); err == nil && wrapped.Data.Metrics != nil {
			s = wrapped.Data
		}
	}
	if s.Metrics == nil {
		return nil, fmt.Errorf("decode k6 summary: no metrics object")
	}
	return &s, nil
}

func readK6Summary(path string) (*k6Summary, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is under the results directory given on the operator's command line
	if err != nil {
		return nil, err
	}
	return parseK6Summary(b)
}

// value returns metrics[name].values[key].
func (s *k6Summary) value(name, key string) (float64, bool) {
	if s == nil {
		return 0, false
	}
	m, ok := s.Metrics[name]
	if !ok {
		return 0, false
	}
	v, ok := m.Values[key]
	return v, ok
}

// count returns the count of a counter metric, or 0 when it never fired:
// k6 omits counters that were never incremented.
func (s *k6Summary) count(name string) float64 {
	v, _ := s.value(name, "count")
	return v
}

// thresholds returns every evaluated threshold, sorted by metric and expression.
func (s *k6Summary) thresholds() []thresholdResult {
	if s == nil {
		return nil
	}
	var out []thresholdResult
	for _, name := range slices.Sorted(maps.Keys(s.Metrics)) {
		m := s.Metrics[name]
		for _, expr := range slices.Sorted(maps.Keys(m.Thresholds)) {
			out = append(out, thresholdResult{Metric: name, Expr: expr, OK: m.Thresholds[expr].OK})
		}
	}
	return out
}

var (
	statusSubmetric = regexp.MustCompile(`^http_reqs\{.*status:\s*"?([0-9]{1,3})"?.*\}$`)
	statusCounter   = regexp.MustCompile(`^(?:status|responses|http)_([0-9]{3}|timeout|error)$`)
)

// statusBreakdown returns request counts per HTTP status (or "timeout"), from
// http_reqs{status:N} sub-metrics or status_N counters, whichever exist.
func (s *k6Summary) statusBreakdown() map[string]float64 {
	sub, counters := map[string]float64{}, map[string]float64{}
	if s == nil {
		return sub
	}
	for name, m := range s.Metrics {
		if g := statusSubmetric.FindStringSubmatch(name); g != nil {
			key := g[1]
			if key == "0" { // k6 reports status 0 for timeouts and network errors
				key = "timeout"
			}
			sub[key] += m.Values["count"]
			continue
		}
		if g := statusCounter.FindStringSubmatch(strings.ToLower(name)); g != nil {
			counters[g[1]] += m.Values["count"]
		}
	}
	if len(sub) > 0 {
		return sub
	}
	return counters
}
