package main

import (
	"maps"
	"testing"
)

const k6Fixture = `{
  "options": {"summaryTrendStats": ["avg","min","med","max","p(90)","p(95)","p(99)"]},
  "state": {"testRunDurationMs": 182700},
  "root_group": {"name": "", "checks": []},
  "setup_data": null,
  "metrics": {
    "http_reqs": {"type": "counter", "contains": "default", "values": {"count": 36000, "rate": 197.04}},
    "events_accepted": {"type": "counter", "contains": "default", "values": {"count": 35998, "rate": 197.03}},
    "http_req_duration": {"type": "trend", "contains": "time",
      "values": {"avg": 4.87, "min": 0.61, "med": 2.94, "max": 10021.4, "p(90)": 7.12, "p(95)": 11.86, "p(99)": 41.3}},
    "http_req_duration{phase:steady}": {"type": "trend", "contains": "time", "values": {"p(95)": 10.42},
      "thresholds": {"p(95)<300": {"ok": true}}},
    "http_req_failed{phase:steady}": {"type": "rate", "contains": "default", "values": {"rate": 0.02},
      "thresholds": {"rate<0.01": {"ok": false}}},
    "checks": {"type": "rate", "contains": "default", "values": {"rate": 0.9999, "passes": 71996, "fails": 4}}
  }
}`

func TestParseK6Summary(t *testing.T) {
	t.Parallel()
	s, err := parseK6Summary([]byte(k6Fixture))
	if err != nil {
		t.Fatalf("parseK6Summary: %v", err)
	}
	tests := []struct {
		name, metric, key string
		want              float64
		found             bool
	}{
		{"should read a counter count", "http_reqs", "count", 36000, true},
		{"should read a trend percentile", "http_req_duration", "p(99)", 41.3, true},
		{"should read the median", "http_req_duration", "med", 2.94, true},
		{"should read a rate", "checks", "rate", 0.9999, true},
		{"should report a missing metric", "dropped_iterations", "count", 0, false},
		{"should report a missing stat", "http_reqs", "p(95)", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, found := s.value(tt.metric, tt.key)
			if got != tt.want || found != tt.found {
				t.Errorf("value(%s, %s) = %v, %v; want %v, %v", tt.metric, tt.key, got, found, tt.want, tt.found)
			}
		})
	}
	if got := s.count("events_unknown"); got != 0 {
		t.Errorf("count of a never-incremented counter = %v, want 0", got)
	}
	if s.State.TestRunDurationMs != 182700 {
		t.Errorf("testRunDurationMs = %v", s.State.TestRunDurationMs)
	}

	th := s.thresholds()
	want := []thresholdResult{
		{Metric: "http_req_duration{phase:steady}", Expr: "p(95)<300", OK: true},
		{Metric: "http_req_failed{phase:steady}", Expr: "rate<0.01", OK: false},
	}
	if len(th) != len(want) {
		t.Fatalf("thresholds = %+v, want %+v", th, want)
	}
	for i := range want {
		if th[i] != want[i] {
			t.Errorf("threshold %d = %+v, want %+v", i, th[i], want[i])
		}
	}
}

func TestParseK6SummaryShapes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{"should accept a summary wrapped in data", `{"data": {"metrics": {"http_reqs": {"type": "counter", "values": {"count": 1}}}}}`, false},
		{"should reject JSON without metrics", `{"options": {}}`, true},
		{"should reject invalid JSON", `{"metrics":`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s, err := parseK6Summary([]byte(tt.in))
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && s.count("http_reqs") != 1 {
				t.Errorf("http_reqs count = %v, want 1", s.count("http_reqs"))
			}
		})
	}
}

func TestStatusBreakdown(t *testing.T) {
	t.Parallel()
	counter := func(n float64) k6Metric { return k6Metric{Type: "counter", Values: map[string]float64{"count": n}} }
	tests := []struct {
		name    string
		metrics map[string]k6Metric
		want    map[string]float64
	}{
		{
			name: "should prefer http_reqs status sub-metrics and map status 0 to timeout",
			metrics: map[string]k6Metric{
				"http_reqs{status:202}": counter(900), "http_reqs{status:503}": counter(40),
				"http_reqs{status:0}": counter(3), "status_202": counter(1),
			},
			want: map[string]float64{"202": 900, "503": 40, "timeout": 3},
		},
		{
			name:    "should fall back to status counters",
			metrics: map[string]k6Metric{"status_202": counter(10), "status_timeout": counter(2)},
			want:    map[string]float64{"202": 10, "timeout": 2},
		},
		{
			name:    "should be empty when the script does not tag statuses",
			metrics: map[string]k6Metric{"events_accepted": counter(10)},
			want:    map[string]float64{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := (&k6Summary{Metrics: tt.metrics}).statusBreakdown()
			if !maps.Equal(got, tt.want) {
				t.Errorf("statusBreakdown = %v, want %v", got, tt.want)
			}
		})
	}
}
