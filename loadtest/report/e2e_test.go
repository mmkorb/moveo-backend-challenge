package main

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/*.golden files")

const fixtureRun = "testdata/20261012t030000z-crash-consumer-kill"

// copyFixture copies a fixture run into a temp dir so the test never writes
// into testdata.
func copyFixture(t *testing.T, src string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), filepath.Base(src))
	if err := os.CopyFS(dst, os.DirFS(src)); err != nil {
		t.Fatalf("copy fixture: %v", err)
	}
	return dst
}

func TestRunRendersCrashConsumerKill(t *testing.T) {
	t.Parallel()
	dir := copyFixture(t, fixtureRun)
	var stdout, stderr bytes.Buffer
	if code := run([]string{dir}, &stdout, &stderr); code != exitPass {
		t.Fatalf("exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitPass, stdout.String(), stderr.String())
	}
	if !strings.HasPrefix(stdout.String(), "PASS  crash-consumer-kill") {
		t.Errorf("stdout = %q, want a PASS verdict line", stdout.String())
	}

	md, err := os.ReadFile(filepath.Join(dir, "report.md"))
	if err != nil {
		t.Fatalf("read report.md: %v", err)
	}
	golden := filepath.Join("testdata", "crash-consumer-kill.report.md.golden")
	if *update {
		if err := os.WriteFile(golden, md, 0o644); err != nil {
			t.Fatalf("update golden: %v", err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("read golden (run go test -update): %v", err)
	}
	if !bytes.Equal(md, want) {
		t.Errorf("report.md differs from %s (run go test -update and review the diff)\n%s", golden, firstDiff(string(want), string(md)))
	}

	html, err := os.ReadFile(filepath.Join(dir, "report.html"))
	if err != nil {
		t.Fatalf("read report.html: %v", err)
	}
	h := string(html)
	for _, want := range []string{
		"<!doctype html>",
		"prefers-color-scheme: dark",
		`<svg class="chart"`,
		"CPU % per container",
		"Memory per container (MiB)",
		"Consumer lag (records, all partitions)",
		`class="marker"`, // chaos actions on the charts
		"8. Chaos timeline",
		`<a href="k6-dashboard.html">`,
		"36,000", // SUM(activity_count)
	} {
		if !strings.Contains(h, want) {
			t.Errorf("report.html does not contain %q", want)
		}
	}
	// Self-contained: no external stylesheets, scripts, images or fonts.
	if m := regexp.MustCompile(`(?i)(src|href)="(https?:)?//`).FindString(h); m != "" {
		t.Errorf("report.html references an external asset: %s", m)
	}
}

// runFixture renders a copy of a fixture and returns its exit code and report.md.
func runFixture(t *testing.T, fixture string, mutate func(dir string)) (int, string) {
	t.Helper()
	dir := copyFixture(t, fixture)
	if mutate != nil {
		mutate(dir)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{dir}, &stdout, &stderr)
	md, err := os.ReadFile(filepath.Join(dir, "report.md"))
	if err != nil {
		t.Fatalf("read report.md: %v\nstdout:\n%s\nstderr:\n%s", err, stdout.String(), stderr.String())
	}
	return code, string(md)
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestRunRendersNewScenariosAgainstGoldens(t *testing.T) {
	t.Parallel()
	tests := []struct {
		fixture, golden string
		mustContain     []string
	}{
		{
			"testdata/20261012t050000z-crash-commit-gap", "testdata/crash-commit-gap.report.md.golden",
			[]string{
				"| Commit gap exercised (stored > committed on resume) | gap > 0 on at least one partition | 156 records on 5 partitions | **PASS** |",
				"Records protected from double counting by the stored position: **156**",
				"`offset_fence_rejections_total` 0, `offset_fence_resyncs_total` 0",
			},
		},
		{
			"testdata/20261012t060000z-poison-records", "testdata/poison-records.report.md.golden",
			[]string{
				"| DLQ records = injected poison records | equal | dlq 50, injected 50 | **PASS** |",
				"| DLQ by error class matches the injected mix | per class, dlq_records_total = injected | decode 26/26, validation 24/24 (dlq/injected) | **PASS** |",
				"| Effectively once (db = input topic − DLQ) |",
				"| Records in input topic | 36,050 |",
				"## 8. Chaos timeline",
			},
		},
	}
	for _, tt := range tests {
		t.Run(filepath.Base(tt.fixture), func(t *testing.T) {
			t.Parallel()
			code, md := runFixture(t, tt.fixture, nil)
			if code != exitPass {
				t.Fatalf("exit code = %d, want %d\n%s", code, exitPass, md)
			}
			for _, want := range tt.mustContain {
				if !strings.Contains(md, want) {
					t.Errorf("report.md does not contain %q", want)
				}
			}
			if *update {
				if err := os.WriteFile(tt.golden, []byte(md), 0o644); err != nil {
					t.Fatalf("update golden: %v", err)
				}
			}
			want, err := os.ReadFile(tt.golden)
			if err != nil {
				t.Fatalf("read golden (run go test -update): %v", err)
			}
			if md != string(want) {
				t.Errorf("report.md differs from %s (run go test -update and review the diff)\n%s", tt.golden, firstDiff(string(want), md))
			}
		})
	}
}

func TestCommitGapFailsWhenStoredEqualsCommitted(t *testing.T) {
	t.Parallel()
	// The kill fixture resumed with stored == committed everywhere; relabelled as
	// crash-commit-gap it must fail: the scenario would not have tested what it claims.
	code, md := runFixture(t, fixtureRun, func(dir string) {
		b, err := os.ReadFile(filepath.Join(dir, "run.json"))
		if err != nil {
			t.Fatal(err)
		}
		writeFile(t, filepath.Join(dir, "run.json"), strings.Replace(string(b), `"crash-consumer-kill"`, `"crash-commit-gap"`, 1))
	})
	if code != exitFail {
		t.Fatalf("exit code = %d, want %d", code, exitFail)
	}
	for _, want := range []string{
		"| Commit gap exercised (stored > committed on resume) | gap > 0 on at least one partition | stored == committed on every partition: the scenario did not test what it claims | **FAIL** |",
		"Stored == committed on every partition: the commit gap was not exercised in this run.",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("report.md does not contain %q", want)
		}
	}
}

func TestCommitGapFailsWithoutConsumerLog(t *testing.T) {
	t.Parallel()
	code, md := runFixture(t, "testdata/20261012t050000z-crash-commit-gap", func(dir string) {
		if err := os.Remove(filepath.Join(dir, "logs", "consumer.log")); err != nil {
			t.Fatal(err)
		}
	})
	if code != exitFail {
		t.Fatalf("exit code = %d, want %d", code, exitFail)
	}
	if !strings.Contains(md, "the commit gap was not measured") {
		t.Errorf("report.md does not say the gap was not measured:\n%s", md)
	}
}

func TestPoisonFailsWhenTheDLQClassMixDiffers(t *testing.T) {
	t.Parallel()
	code, md := runFixture(t, "testdata/20261012t060000z-poison-records", func(dir string) {
		b, err := os.ReadFile(filepath.Join(dir, fileConsumerPost))
		if err != nil {
			t.Fatal(err)
		}
		s := strings.Replace(string(b), `{error_class="decode"} 26`, `{error_class="decode"} 25`, 1)
		s = strings.Replace(s, `{error_class="validation"} 24`, `{error_class="validation"} 25`, 1)
		writeFile(t, filepath.Join(dir, fileConsumerPost), s)
	})
	if code != exitFail {
		t.Fatalf("exit code = %d, want %d", code, exitFail)
	}
	if !strings.Contains(md, "decode 25/26, validation 25/24 (dlq/injected) | **FAIL** |") {
		t.Errorf("report.md does not show the class mismatch:\n%s", md)
	}
}

func TestRecoverySectionIsHiddenWithoutARestart(t *testing.T) {
	t.Parallel()
	_, md := runFixture(t, "testdata/20261012t060000z-poison-records", nil)
	if strings.Contains(md, "Recovery") {
		t.Errorf("a run with a single consumer start must not have a Recovery section:\n%s", md)
	}
}

func TestRunWithMissingFiles(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "20261012t040000z-load")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{dir}, &stdout, &stderr); code != exitFail {
		t.Fatalf("exit code = %d, want %d (FAIL: nothing can be verified)\nstderr: %s", code, exitFail, stderr.String())
	}
	md, err := os.ReadFile(filepath.Join(dir, "report.md"))
	if err != nil {
		t.Fatalf("read report.md: %v", err)
	}
	for _, want := range []string{
		"# Load test report — load: FAIL",
		"_Not available: run.json missing._",
		"_Not available: k6-summary.json missing or unreadable._",
		"_Not available: db-totals.csv missing or empty._",
		"_Not available: stats.jsonl and inspect.jsonl missing._",
		"## 8. Artifacts", // no chaos timeline for a non-crash run
	} {
		if !strings.Contains(string(md), want) {
			t.Errorf("report.md does not contain %q", want)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "report.html")); err != nil {
		t.Errorf("report.html not written: %v", err)
	}
}

func TestRunWithCorruptFiles(t *testing.T) {
	t.Parallel()
	dir := copyFixture(t, fixtureRun)
	for name, content := range map[string]string{
		fileK6Summary:    "{not json",
		fileConsumerPost: "metric{broken 1\n",
		fileStats:        "{\"ts\":\"2026-10-12T03:00:00Z\",\"Name\":\"x\"\n",
		fileDBTotals:     "activity_count,page_view_count,distinct_users,rows\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{dir}, &stdout, &stderr); code != exitFail {
		t.Fatalf("exit code = %d, want %d\nstdout:\n%s\nstderr:\n%s", code, exitFail, stdout.String(), stderr.String())
	}
}

func TestRunUsageErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		args []string
	}{
		{"should reject no arguments", nil},
		{"should reject a missing directory", []string{filepath.Join(t.TempDir(), "nope")}},
		{"should reject a file instead of a directory", []string{"e2e_test.go"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var stdout, stderr bytes.Buffer
			if code := run(tt.args, &stdout, &stderr); code != exitError {
				t.Errorf("exit code = %d, want %d", code, exitError)
			}
		})
	}
}

// firstDiff returns the first differing line of two texts.
func firstDiff(want, got string) string {
	w, g := strings.Split(want, "\n"), strings.Split(got, "\n")
	for i := range max(len(w), len(g)) {
		var wl, gl string
		if i < len(w) {
			wl = w[i]
		}
		if i < len(g) {
			gl = g[i]
		}
		if wl != gl {
			return "line " + strconv.Itoa(i+1) + ":\n  want: " + wl + "\n  got:  " + gl
		}
	}
	return ""
}
