package main

import (
	"math"
	"strings"
	"testing"
)

const promFixture = `# HELP records_consumed_total Records consumed by result.
# TYPE records_consumed_total counter
records_consumed_total{result="dlq"} 3
records_consumed_total{result="ok"} 1.2345e+04
# a comment
retries_total{dependency="postgres",note="say \"hi\", a\\b"} 7 1728700000000
offset_commit_failures_total 2
consumer_lag_records{partition="0"} 10
consumer_lag_records{partition="1"} 5
# TYPE aggregate_flush_duration_seconds histogram
aggregate_flush_duration_seconds_bucket{le="0.01"} 10
aggregate_flush_duration_seconds_bucket{le="0.1"} 90
aggregate_flush_duration_seconds_bucket{le="1"} 100
aggregate_flush_duration_seconds_bucket{le="+Inf"} 100
aggregate_flush_duration_seconds_sum 4.2
aggregate_flush_duration_seconds_count 100
go_gc_duration_seconds{quantile="0.5"} NaN
`

func TestParseProm(t *testing.T) {
	t.Parallel()
	set, err := parseProm(strings.NewReader(promFixture))
	if err != nil {
		t.Fatalf("parseProm: %v", err)
	}
	tests := []struct {
		name   string
		metric string
		labels map[string]string
		want   float64
		found  bool
	}{
		{"should read a labelled counter", "records_consumed_total", map[string]string{"result": "ok"}, 12345, true},
		{"should sum across labels when none are given", "records_consumed_total", nil, 12348, true},
		{"should read an unlabelled counter", "offset_commit_failures_total", nil, 2, true},
		{"should ignore the optional timestamp and unescape labels", "retries_total",
			map[string]string{"dependency": "postgres", "note": `say "hi", a\b`}, 7, true},
		{"should sum a gauge over partitions", "consumer_lag_records", nil, 15, true},
		{"should report a missing metric", "dlq_records_total", nil, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, found := set.sum(tt.metric, tt.labels)
			if found != tt.found || got != tt.want {
				t.Errorf("sum(%s, %v) = %v, %v; want %v, %v", tt.metric, tt.labels, got, found, tt.want, tt.found)
			}
		})
	}

	if got := set.byLabel("consumer_lag_records", "partition"); len(got) != 2 || got["0"] != 10 || got["1"] != 5 {
		t.Errorf("byLabel = %v", got)
	}
	bs := set.buckets("aggregate_flush_duration_seconds")
	if len(bs) != 4 || bs[0].LE != 0.01 || !math.IsInf(bs[3].LE, 1) || bs[3].Count != 100 {
		t.Errorf("buckets = %+v", bs)
	}
}

func TestParsePromErrors(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, in string }{
		{"should reject a sample without value", "metric_without_value\n"},
		{"should reject an unterminated label set", `m{a="b" 1` + "\n"},
		{"should reject an unquoted label value", "m{a=b} 1\n"},
		{"should reject a non-numeric value", "m 12abc\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := parseProm(strings.NewReader(tt.in)); err == nil {
				t.Errorf("parseProm(%q) succeeded, want error", tt.in)
			}
		})
	}
}

func TestHistogramQuantile(t *testing.T) {
	t.Parallel()
	inf := math.Inf(1)
	buckets := []bucket{{0.01, 10}, {0.1, 90}, {1, 100}, {inf, 100}}
	tests := []struct {
		name    string
		q       float64
		bs      []bucket
		want    float64
		wantOK  bool
		epsilon float64
	}{
		{"should interpolate inside the first bucket from zero", 0.05, buckets, 0.005, true, 1e-12},
		{"should interpolate the median inside the second bucket", 0.5, buckets, 0.01 + 0.09*40.0/80.0, true, 1e-12},
		{"should interpolate p95 inside the third bucket", 0.95, buckets, 0.1 + 0.9*5.0/10.0, true, 1e-12},
		{"should return the largest finite bound when p falls in +Inf", 0.99, []bucket{{0.5, 90}, {inf, 100}}, 0.5, true, 0},
		{"should report no observations", 0.95, []bucket{{0.1, 0}, {inf, 0}}, 0, false, 0},
		{"should report no buckets", 0.95, nil, 0, false, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := histogramQuantile(tt.q, tt.bs)
			if ok != tt.wantOK || math.Abs(got-tt.want) > tt.epsilon {
				t.Errorf("histogramQuantile(%v) = %v, %v; want %v, %v", tt.q, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestBucketDelta(t *testing.T) {
	t.Parallel()
	inf := math.Inf(1)
	before := []bucket{{0.1, 5}, {1, 8}, {inf, 8}}
	after := []bucket{{0.1, 15}, {1, 28}, {inf, 30}}

	got, reset := bucketDelta(before, after)
	want := []bucket{{0.1, 10}, {1, 20}, {inf, 22}}
	if reset || len(got) != len(want) {
		t.Fatalf("bucketDelta = %+v, reset %v", got, reset)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("bucket %d = %+v, want %+v", i, got[i], want[i])
		}
	}

	got, reset = bucketDelta(after, before)
	if !reset || got[2].Count != 8 {
		t.Errorf("bucketDelta on reset = %+v, reset %v; want the after histogram and reset", got, reset)
	}
}

func TestCounterDelta(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		before, after float64
		want          float64
		wantReset     bool
	}{
		{"should subtract the baseline", 100, 350, 250, false},
		{"should keep after as a lower bound on reset", 500, 120, 120, true},
		{"should be zero when nothing happened", 7, 7, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, reset := counterDelta(tt.before, tt.after)
			if got != tt.want || reset != tt.wantReset {
				t.Errorf("counterDelta(%v, %v) = %v, %v; want %v, %v", tt.before, tt.after, got, reset, tt.want, tt.wantReset)
			}
		})
	}
}
