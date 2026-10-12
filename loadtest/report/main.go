// Command report turns a load-test results directory written by
// loadtest/run.sh into report.md and a self-contained report.html, prints a
// short verdict and exits 0 on PASS, 1 on FAIL and 2 on error.
//
// Usage:
//
//	go run . <results-dir>
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Exit codes.
const (
	exitPass  = 0
	exitFail  = 1
	exitError = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) != 1 || args[0] == "-h" || args[0] == "--help" {
		_, _ = fmt.Fprintln(stderr, "usage: report <results-dir>")
		return exitError
	}
	dir := args[0]
	r, err := buildReport(dir)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "report: %v\n", err)
		return exitError
	}
	mdPath, htmlPath := filepath.Join(dir, "report.md"), filepath.Join(dir, "report.html")
	if err := writeReports(r, mdPath, htmlPath); err != nil {
		_, _ = fmt.Fprintf(stderr, "report: %v\n", err)
		return exitError
	}

	_, _ = fmt.Fprintf(stdout, "%s  %s  (%d/%d checks passed)\n", r.Verdict(), r.Run.Scenario, len(r.Checks)-r.Failed(), len(r.Checks))
	for _, c := range r.Checks {
		if !c.Pass {
			_, _ = fmt.Fprintf(stdout, "  FAIL %s: expected %s, got %s\n", c.Name, c.Expected, c.Actual)
		}
	}
	_, _ = fmt.Fprintf(stdout, "report: %s\n        %s\n", mdPath, htmlPath)
	if r.Pass {
		return exitPass
	}
	return exitFail
}
