package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// statLine renders one stats.jsonl line for the kafka container.
func statLine(ts string, cpu float64, memMiB int) string {
	return fmt.Sprintf(`{"ts":%q,"CPUPerc":"%.2f%%","MemUsage":"%dMiB / 1.25GiB","Name":"moveo-loadtest-kafka-1","NetIO":"1kB / 1kB"}`+"\n",
		ts, cpu, memMiB)
}

// writeRun writes a minimal results directory: a start-up spike (150 % CPU, 900 MiB)
// before the k6 window, steady load inside it and a drain spike after it.
func writeRun(t *testing.T, timeline string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "20261012t050000z-smoke")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	stats := statLine("2026-10-12T05:00:01.000Z", 150, 900) +
		statLine("2026-10-12T05:00:09.000Z", 120, 800) +
		statLine("2026-10-12T05:00:10.000Z", 10, 500) + // k6_start, inclusive
		statLine("2026-10-12T05:00:20.000Z", 30, 520) +
		statLine("2026-10-12T05:00:30.000Z", 20, 510) + // k6_end, inclusive
		statLine("2026-10-12T05:00:40.000Z", 90, 700)
	files := map[string]string{
		fileStats:    stats,
		fileTimeline: timeline,
		fileRun:      `{"scenario":"smoke","profile":"producer-bound","run_id":"20261012t050000z"}`,
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func kafkaRow(t *testing.T, r *report) containerRow {
	t.Helper()
	for _, c := range r.Resources.Containers {
		if c.Name == "kafka-1" {
			return c
		}
	}
	t.Fatalf("no kafka-1 row in %+v", r.Resources.Containers)
	return containerRow{}
}

func TestResourcesUseOnlyTheK6Window(t *testing.T) {
	t.Parallel()
	dir := writeRun(t, `{"ts":"2026-10-12T05:00:10.000Z","event":"k6_start"}
{"ts":"2026-10-12T05:00:30.000Z","event":"k6_end","exit_code":0}
`)
	r, err := buildReport(dir)
	if err != nil {
		t.Fatal(err)
	}
	k := kafkaRow(t, r)
	if got := k.CPUPeak.V; got != 30 {
		t.Errorf("CPU peak = %v, want 30 (start-up and drain samples excluded)", got)
	}
	if got := k.CPUAvg.V; got != 20 {
		t.Errorf("CPU avg = %v, want 20", got)
	}
	if got, want := k.MemPeak.V, float64(520<<20); got != want {
		t.Errorf("mem peak = %v, want %v", got, want)
	}
	if k.Samples != 3 {
		t.Errorf("samples in window = %d, want 3", k.Samples)
	}
	if !strings.Contains(r.Resources.WindowNote, "k6 window only") {
		t.Errorf("window note = %q", r.Resources.WindowNote)
	}
	// The charts keep the whole timeline and mark the window.
	var labels []string
	for _, m := range r.Resources.CPUChart.Markers {
		labels = append(labels, m.Label)
	}
	if got := strings.Join(labels, ","); got != "k6 start,k6 end" {
		t.Errorf("CPU chart markers = %q, want the k6 window", got)
	}
	if r.Run.Title() != "smoke (producer-bound)" {
		t.Errorf("title = %q", r.Run.Title())
	}
}

func TestResourcesFallBackToAllSamples(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		timeline string
	}{
		{"should fall back without a timeline", ""},
		{"should fall back without k6_end", `{"ts":"2026-10-12T05:00:10.000Z","event":"k6_start"}` + "\n"},
		{"should fall back without k6_start", `{"ts":"2026-10-12T05:00:30.000Z","event":"k6_end"}` + "\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r, err := buildReport(writeRun(t, tt.timeline))
			if err != nil {
				t.Fatal(err)
			}
			k := kafkaRow(t, r)
			if k.CPUPeak.V != 150 || k.Samples != 6 {
				t.Errorf("CPU peak = %v over %d samples, want 150 over 6", k.CPUPeak.V, k.Samples)
			}
			if !strings.Contains(r.Resources.WindowNote, "all samples") {
				t.Errorf("window note = %q, want a fallback note", r.Resources.WindowNote)
			}
		})
	}
}
