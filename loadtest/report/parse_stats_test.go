package main

import (
	"testing"
	"time"
)

func TestParseBytes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    float64
		wantErr bool
	}{
		{"should parse zero bytes", "0B", 0, false},
		{"should parse decimal kilobytes", "1.2kB", 1200, false},
		{"should parse decimal megabytes", "3.4MB", 3.4e6, false},
		{"should parse decimal gigabytes", "1.5GB", 1.5e9, false},
		{"should parse binary kibibytes", "512KiB", 512 * 1024, false},
		{"should parse binary mebibytes", "123.4MiB", 123.4 * 1024 * 1024, false},
		{"should parse binary gibibytes", "1.25GiB", 1.25 * 1024 * 1024 * 1024, false},
		{"should trim surrounding spaces", " 384MiB ", 384 * 1024 * 1024, false},
		{"should accept a bare number as bytes", "42", 42, false},
		{"should reject a stopped-container placeholder", "--", 0, true},
		{"should reject an unknown unit", "12XB", 0, true},
		{"should reject a missing number", "MiB", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseBytes(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseBytes(%q) error = %v, wantErr %v", tt.in, err, tt.wantErr)
			}
			if !tt.wantErr && !approx(got, tt.want) {
				t.Errorf("parseBytes(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestParsePair(t *testing.T) {
	t.Parallel()
	used, limit, err := parsePair("100.2MiB / 1.25GiB")
	if err != nil {
		t.Fatalf("parsePair: %v", err)
	}
	if !approx(used, 100.2*(1<<20)) || !approx(limit, 1.25*(1<<30)) {
		t.Errorf("parsePair = %v, %v", used, limit)
	}
	if _, _, err := parsePair("100MiB"); err == nil {
		t.Error("parsePair without '/' should fail")
	}
}

func TestParseStatSample(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 10, 12, 3, 0, 2, 0, time.UTC)
	tests := []struct {
		name    string
		in      object
		want    statSample
		wantErr bool
	}{
		{
			name: "should parse a docker stats line with the run.sh timestamp",
			in: object{"ts": "2026-10-12T03:00:02.000Z", "CPUPerc": "12.34%", "MemUsage": "100.2MiB / 1.25GiB",
				"Name": "moveo-loadtest-kafka-1", "NetIO": "1.2MB / 3.4kB"},
			want: statSample{Time: ts, Name: "moveo-loadtest-kafka-1", CPUPerc: 12.34,
				MemUsed: 100.2 * (1 << 20), MemLimit: 1.25 * (1 << 30), NetRx: 1.2e6, NetTx: 3400},
		},
		{
			name: "should reject a stopped container",
			in: object{"ts": "2026-10-12T03:00:02.000Z", "CPUPerc": "--", "MemUsage": "-- / --",
				"Name": "moveo-loadtest-consumer-1", "NetIO": "--"},
			wantErr: true,
		},
		{
			name:    "should reject a line without timestamp",
			in:      object{"CPUPerc": "1%", "MemUsage": "1MiB / 2MiB", "Name": "x"},
			wantErr: true,
		},
		{
			name:    "should reject a line without name",
			in:      object{"ts": "2026-10-12T03:00:02Z", "CPUPerc": "1%", "MemUsage": "1MiB / 2MiB"},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseStatSample(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !got.Time.Equal(tt.want.Time) || got.Name != tt.want.Name || !approx(got.CPUPerc, tt.want.CPUPerc) ||
				!approx(got.MemUsed, tt.want.MemUsed) || !approx(got.MemLimit, tt.want.MemLimit) ||
				!approx(got.NetRx, tt.want.NetRx) || !approx(got.NetTx, tt.want.NetTx) {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// approx compares floats with a relative tolerance.
func approx(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= 1e-9*max(1, abs(a), abs(b))
}

func abs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
