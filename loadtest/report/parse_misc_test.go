package main

import (
	"strings"
	"testing"
)

func TestParseDBTotals(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    dbTotals
		wantErr bool
	}{
		{
			name: "should parse the run.sh CSV",
			in:   "activity_count,page_view_count,distinct_users,rows\n36000,21600,4980,5100\n",
			want: dbTotals{Activity: 36000, PageViews: 21600, Users: 4980, HasUsers: true},
		},
		{
			name: "should treat empty SUMs as zero and tolerate a psql footer",
			in:   "activity_count,page_view_count\n,\n(1 row)\n",
			want: dbTotals{},
		},
		{name: "should fail on a header-only file (query failed)", in: "activity_count,page_view_count,distinct_users,rows\n", wantErr: true},
		{name: "should fail on an empty file", in: "", wantErr: true},
		{name: "should fail without an activity column", in: "a,b\n1,2\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseDBTotals(strings.NewReader(tt.in))
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseDBByDate(t *testing.T) {
	t.Parallel()
	in := "activity_date,rows,distinct_users,activity_count,page_view_count\n2026-10-11,1500,1500,1800,1080\n2026-10-12,3600,3600,34200,20520\n"
	got, err := parseDBByDate(strings.NewReader(in))
	if err != nil {
		t.Fatalf("parseDBByDate: %v", err)
	}
	want := []dbDateRow{
		{Date: "2026-10-11", Rows: "1500", Activity: "1800", PageViews: "1080", Users: "1500"},
		{Date: "2026-10-12", Rows: "3600", Activity: "34200", PageViews: "20520", Users: "3600"},
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestParseOffsets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    float64
		wantErr bool
	}{
		{"should sum topic:partition:offset lines", "incoming.user_activity.dlq:0:3\nincoming.user_activity.dlq:1:0\nincoming.user_activity.dlq:2:4\n", 7, false},
		{"should be zero for an empty DLQ", "incoming.user_activity.dlq:0:0\n", 0, false},
		{"should ignore blank and comment lines", "# end offsets\n\nt:0:5\n", 5, false},
		{"should fail without offsets", "\n", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseOffsets(strings.NewReader(tt.in))
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseLogsSummary(t *testing.T) {
	t.Parallel()
	got, err := parseLogsSummary(strings.NewReader("service\tlines\terror\twarn\nproducer\t3\t2\t0\nconsumer\t1234\t0\t17\n"))
	if err != nil {
		t.Fatalf("parseLogsSummary: %v", err)
	}
	want := []logStats{{Service: "consumer", Lines: 1234, Errors: 0, Warns: 17}, {Service: "producer", Lines: 3, Errors: 2}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %+v, want %+v", got, want)
	}
	if _, err := parseLogsSummary(strings.NewReader("a\tb\n")); err == nil {
		t.Error("unexpected header should fail")
	}
}

func TestClassifyLogLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, in, level, msg string
	}{
		{"should read slog JSON errors", `{"time":"t","level":"ERROR","msg":"db down","err":"x"}`, "ERROR", "db down"},
		{"should read slog JSON warnings", `{"time":"t","level":"WARN","msg":"retrying"}`, "WARN", "retrying"},
		{"should ignore slog JSON info", `{"time":"t","level":"INFO","msg":"flush"}`, "", "flush"},
		{"should strip a compose prefix before JSON", `consumer-1  | {"level":"ERROR","msg":"boom"}`, "ERROR", "boom"},
		{"should match log4j warnings", "[2026-10-12 03:01:47,010] WARN [GroupCoordinator 1]: rebalance", "WARN",
			"[2026-10-12 03:01:47,010] WARN [GroupCoordinator 1]: rebalance"},
		{"should treat FATAL as an error", "2026-10-12 03:00:02 UTC [1] FATAL:  role missing", "ERROR",
			"2026-10-12 03:00:02 UTC [1] FATAL:  role missing"},
		{"should not match words inside identifiers", "ERRORS_TOTAL is fine", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			level, msg := classifyLogLine(tt.in)
			if level != tt.level || msg != tt.msg {
				t.Errorf("classifyLogLine(%q) = %q, %q; want %q, %q", tt.in, level, msg, tt.level, tt.msg)
			}
		})
	}
}

func TestParseInspect(t *testing.T) {
	t.Parallel()
	objs := []object{
		{"name": "moveo-loadtest-consumer-1", "oom_killed": true, "restart_count": float64(1), "exit_code": float64(137), "status": "exited"},
		// Raw `docker inspect` shape.
		{"Name": "/moveo-loadtest-kafka-1", "State": map[string]any{"OOMKilled": false, "ExitCode": float64(0), "Status": "running"}, "RestartCount": float64(0)},
		{"status": "running"}, // no name: skipped
	}
	got := parseInspect(objs)
	want := []containerState{
		{Name: "moveo-loadtest-consumer-1", OOMKilled: true, RestartCount: 1, ExitCode: 137, Status: "exited"},
		{Name: "moveo-loadtest-kafka-1", Status: "running"},
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
