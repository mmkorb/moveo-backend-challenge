package main

import (
	"bufio"
	"cmp"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// csvTable is a CSV file with a header row, addressed by normalised column name.
type csvTable struct {
	Header []string
	Rows   [][]string
}

// parseCSV reads a CSV with a header row. psql's unaligned output with a
// comma separator and a trailing "(N rows)" footer is tolerated.
func parseCSV(r io.Reader) (*csvTable, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1
	cr.TrimLeadingSpace = true
	recs, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read csv: %w", err)
	}
	var t csvTable
	for _, rec := range recs {
		if len(rec) == 1 && (strings.TrimSpace(rec[0]) == "" || strings.HasPrefix(strings.TrimSpace(rec[0]), "(")) {
			continue
		}
		if t.Header == nil {
			t.Header = rec
			continue
		}
		t.Rows = append(t.Rows, rec)
	}
	if t.Header == nil {
		return nil, fmt.Errorf("read csv: empty")
	}
	return &t, nil
}

// col returns the index of the first header matching one of names.
func (t *csvTable) col(names ...string) int {
	for _, n := range names {
		nn := normKey(n)
		for i, h := range t.Header {
			if normKey(h) == nn {
				return i
			}
		}
	}
	return -1
}

// num returns row[col] as a number.
func (t *csvTable) num(row []string, col int) (float64, bool) {
	if col < 0 || col >= len(row) {
		return 0, false
	}
	s := strings.TrimSpace(row[col])
	if s == "" {
		return 0, true // SUM over zero rows is NULL in SQL: treat as 0
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

// dbTotals are the aggregate totals for the run's users.
type dbTotals struct {
	Activity  float64
	PageViews float64
	Users     float64
	HasUsers  bool
}

func parseDBTotals(r io.Reader) (dbTotals, error) {
	var d dbTotals
	t, err := parseCSV(r)
	if err != nil {
		return d, err
	}
	if len(t.Rows) == 0 {
		return d, fmt.Errorf("db totals: no data row")
	}
	row := t.Rows[0]
	var ok bool
	if d.Activity, ok = t.num(row, t.col("activity_count", "sum_activity_count", "activity", "total_activity", "activities")); !ok {
		return d, fmt.Errorf("db totals: no activity_count column in %v", t.Header)
	}
	if d.PageViews, ok = t.num(row, t.col("page_view_count", "sum_page_view_count", "page_views", "pageviews")); !ok {
		return d, fmt.Errorf("db totals: no page_view_count column in %v", t.Header)
	}
	d.Users, d.HasUsers = t.num(row, t.col("distinct_users", "users", "unique_users", "count_distinct_user_id", "user_count"))
	return d, nil
}

// dbDateRow is one row of db-by-date.csv.
type dbDateRow struct {
	Date      string
	Rows      string
	Activity  string
	PageViews string
	Users     string
}

func parseDBByDate(r io.Reader) ([]dbDateRow, error) {
	t, err := parseCSV(r)
	if err != nil {
		return nil, err
	}
	get := func(row []string, names ...string) string {
		i := t.col(names...)
		if i < 0 || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}
	out := make([]dbDateRow, 0, len(t.Rows))
	for _, row := range t.Rows {
		out = append(out, dbDateRow{
			Date:      get(row, "activity_date", "date", "day"),
			Rows:      get(row, "rows", "row_count", "count"),
			Activity:  get(row, "activity_count", "sum_activity_count", "activity"),
			PageViews: get(row, "page_view_count", "sum_page_view_count", "page_views"),
			Users:     get(row, "distinct_users", "users", "unique_users"),
		})
	}
	return out, nil
}

var intToken = regexp.MustCompile(`-?[0-9]+`)

// parseOffsets sums the end offsets of the DLQ topic. Each non-blank line
// contributes its last integer, which covers kafka-get-offsets output
// ("topic:partition:offset"), "partition offset" pairs and a bare total.
// The DLQ topic is created fresh per run, so end offsets equal record counts.
func parseOffsets(r io.Reader) (float64, error) {
	sc := bufio.NewScanner(r)
	total, lines := 0.0, 0
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		toks := intToken.FindAllString(line, -1)
		if len(toks) == 0 {
			continue
		}
		v, err := strconv.ParseFloat(toks[len(toks)-1], 64)
		if err != nil {
			return 0, fmt.Errorf("dlq offsets %q: %w", line, err)
		}
		total += max(v, 0)
		lines++
	}
	if err := sc.Err(); err != nil {
		return 0, fmt.Errorf("scan dlq offsets: %w", err)
	}
	if lines == 0 {
		return 0, fmt.Errorf("dlq offsets: no offsets found")
	}
	return total, nil
}

// containerState is one docker inspect record.
type containerState struct {
	Name         string
	OOMKilled    bool
	RestartCount float64
	ExitCode     float64
	Status       string
}

func parseInspect(objs []object) []containerState {
	out := make([]containerState, 0, len(objs))
	for _, o := range objs {
		st := o.obj("State")
		c := containerState{Name: strings.TrimPrefix(o.str("name", "Name", "container", "service"), "/")}
		if c.Name == "" {
			continue
		}
		var ok bool
		if c.OOMKilled, ok = o.boolean("oom_killed", "OOMKilled", "oom"); !ok {
			c.OOMKilled, _ = st.boolean("OOMKilled")
		}
		if c.RestartCount, ok = o.num("restart_count", "RestartCount", "restarts"); !ok {
			c.RestartCount, _ = st.num("RestartCount")
		}
		if c.ExitCode, ok = o.num("exit_code", "ExitCode"); !ok {
			c.ExitCode, _ = st.num("ExitCode")
		}
		if c.Status = o.str("status", "Status", "state"); c.Status == "" || strings.HasPrefix(c.Status, "{") {
			c.Status = st.str("Status")
		}
		out = append(out, c)
	}
	return out
}

// logMessage is a distinct log message with its level and number of occurrences.
type logMessage struct {
	Service string
	Level   string
	Msg     string
	Count   int
}

// logStats are the ERROR/WARN counts of one service.
type logStats struct {
	Service string
	Errors  int
	Warns   int
	Lines   int
}

var plainLevel = regexp.MustCompile(`\b(ERROR|FATAL|PANIC|WARN|WARNING)\b`)

// classifyLogLine returns the level ("ERROR", "WARN" or "") and message of one
// log line. Compose prefixes ("consumer-1  | ") are skipped; slog JSON lines
// use their "level" and "msg" fields, anything else is matched textually.
func classifyLogLine(line string) (level, msg string) {
	if i := strings.IndexByte(line, '{'); i >= 0 {
		var rec map[string]any
		if json.Unmarshal([]byte(line[i:]), &rec) == nil {
			o := object(rec)
			lvl := strings.ToUpper(o.str("level", "lvl", "severity"))
			msg := o.str("msg", "message")
			switch {
			case strings.HasPrefix(lvl, "ERR"), lvl == "FATAL", lvl == "PANIC":
				return "ERROR", msg
			case strings.HasPrefix(lvl, "WARN"):
				return "WARN", msg
			}
			return "", msg
		}
	}
	m := plainLevel.FindString(line)
	if m == "" {
		return "", ""
	}
	msg = line
	if i := strings.Index(line, " | "); i >= 0 {
		msg = line[i+3:]
	}
	if !strings.HasPrefix(m, "WARN") {
		return "ERROR", strings.TrimSpace(msg)
	}
	return "WARN", strings.TrimSpace(msg)
}

// parseLogs scans logs/<service>.log files and returns per-service counts
// sorted by service and the most frequent ERROR/WARN messages.
func parseLogs(dir string, top int) ([]logStats, []logMessage, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, nil, fmt.Errorf("glob logs: %w", err)
	}
	if len(files) == 0 {
		return nil, nil, fmt.Errorf("no log files in %s", dir)
	}
	slices.Sort(files)
	var stats []logStats
	msgs := map[[3]string]int{}
	for _, f := range files {
		svc := strings.TrimSuffix(filepath.Base(f), ".log")
		ls, err := scanLogFile(f, svc, msgs)
		if err != nil {
			return nil, nil, err
		}
		stats = append(stats, ls)
	}
	var out []logMessage
	for _, k := range slices.SortedFunc(maps.Keys(msgs), func(a, b [3]string) int {
		return cmp.Or(msgs[b]-msgs[a], strings.Compare(a[0], b[0]), strings.Compare(a[1], b[1]), strings.Compare(a[2], b[2]))
	}) {
		if len(out) == top {
			break
		}
		out = append(out, logMessage{Service: k[0], Level: k[1], Msg: k[2], Count: msgs[k]})
	}
	return stats, out, nil
}

func scanLogFile(path, svc string, msgs map[[3]string]int) (logStats, error) {
	ls := logStats{Service: svc}
	f, err := os.Open(path) //nolint:gosec // G304: path is under the results directory given on the operator's command line
	if err != nil {
		return ls, err
	}
	defer func() { _ = f.Close() }() // opened read-only: a failed close loses nothing
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		ls.Lines++
		level, msg := classifyLogLine(sc.Text())
		switch level {
		case "ERROR":
			ls.Errors++
		case "WARN":
			ls.Warns++
		default:
			continue
		}
		msgs[[3]string{svc, level, truncate(msg, 160)}]++
	}
	if err := sc.Err(); err != nil {
		return ls, fmt.Errorf("scan %s: %w", path, err)
	}
	return ls, nil
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n-1]) + "…"
}

// parseLogsSummary parses logs-summary.txt: tab-separated, header
// "service lines error warn".
func parseLogsSummary(r io.Reader) ([]logStats, error) {
	cr := csv.NewReader(r)
	cr.Comma = '\t'
	cr.FieldsPerRecord = -1
	recs, err := cr.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read logs summary: %w", err)
	}
	if len(recs) == 0 {
		return nil, fmt.Errorf("read logs summary: empty")
	}
	t := csvTable{Header: recs[0], Rows: recs[1:]}
	svc, lines, errs, warns := t.col("service"), t.col("lines"), t.col("error", "errors"), t.col("warn", "warns", "warning")
	if svc < 0 || errs < 0 || warns < 0 {
		return nil, fmt.Errorf("read logs summary: unexpected header %v", t.Header)
	}
	var out []logStats
	for _, row := range t.Rows {
		if svc >= len(row) || strings.TrimSpace(row[svc]) == "" {
			continue
		}
		ls := logStats{Service: strings.TrimSpace(row[svc])}
		if v, ok := t.num(row, lines); ok {
			ls.Lines = int(v)
		}
		if v, ok := t.num(row, errs); ok {
			ls.Errors = int(v)
		}
		if v, ok := t.num(row, warns); ok {
			ls.Warns = int(v)
		}
		out = append(out, ls)
	}
	slices.SortFunc(out, func(a, b logStats) int { return strings.Compare(a.Service, b.Service) })
	return out, nil
}
