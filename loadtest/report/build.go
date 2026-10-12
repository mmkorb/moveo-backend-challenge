package main

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// Result-directory file names: the contract between loadtest/run.sh and this
// tool (see docs/load-testing.md, "Outputs").
const (
	fileRun          = "run.json"
	fileK6Summary    = "k6-summary.json"
	fileK6Dashboard  = "k6-dashboard.html"
	fileStats        = "stats.jsonl"
	fileTimeline     = "timeline.jsonl"
	fileDrain        = "drain.json"
	fileProducerPre  = "metrics-before-producer.txt"
	fileProducerPost = "metrics-after-producer.txt"
	fileConsumerPre  = "metrics-before-consumer.txt"
	fileConsumerPost = "metrics-after-consumer.txt"
	fileDBTotals     = "db-totals.csv"
	fileDBByDate     = "db-by-date.csv"
	fileDLQOffsets   = "dlq-offsets.txt"
	fileInspect      = "inspect.jsonl"
	fileLogsSummary  = "logs-summary.txt"
	fileLag          = "lag.jsonl"
	fileTopicOffsets = "topic-offsets.txt"
	fileK6Output     = "k6-output.log"
	fileChaosLog     = "chaos.log"
	dirLogs          = "logs"
	defaultProject   = "moveo-loadtest"
	topLogMessages   = 5
)

// kv is a key/value row.
type kv struct{ K, V string }

// report is the view model rendered by both templates.
type report struct {
	Pass        bool
	Checks      []check
	Run         runSection
	Ingestion   ingestionSection
	Pipeline    pipelineSection
	Correctness correctnessSection
	Recovery    recoverySection
	Resources   resourcesSection
	Errors      errorsSection
	Timeline    timelineSection
	Artifacts   []artifact
	Notes       []string // parse warnings, shown at the end
}

// Verdict renders the overall result.
func (r *report) Verdict() string {
	if r.Pass {
		return "PASS"
	}
	return "FAIL"
}

// Failed counts the failed checks.
func (r *report) Failed() int {
	n := 0
	for _, c := range r.Checks {
		if !c.Pass {
			n++
		}
	}
	return n
}

type runSection struct {
	Available bool
	Scenario  string
	Profile   string
	RunID     string
	Project   string
	// Experiment groups the runs of one run-suite.sh call (run.json "experiment").
	ExperimentID   string
	ExperimentName string
	Start          string
	End            string
	Duration       num // k6 start → k6 end
	K6Start        string
	K6End          string
	K6Exit         num
	K6Scen         string
	Docker         string
	HostCPUs       string
	HostMem        string
	Images         []kv
	Limits         []kv
	Config         []kv
	Crash          bool
	k6Start        time.Time
	expectMax      num
}

// Title is the scenario, followed by the resource profile when one was used,
// e.g. "stress (producer-bound)".
func (rs runSection) Title() string {
	if rs.Profile == "" {
		return rs.Scenario
	}
	return rs.Scenario + " (" + rs.Profile + ")"
}

type ingestionSection struct {
	Available     bool
	Requests      num
	Rate          num
	TargetRate    num
	TargetNote    string
	PrevDay       num
	InvalidSent   num
	Iterations    num
	Dropped       num
	Accepted      num
	PageViews     num
	Rejected      num
	Unknown       num
	FailedRate    num
	ChecksRate    num
	Status        []kv
	P50, P90, P95 num
	P99, Max, Avg num
	Published     []kv // producer activities_published_total{result} delta
}

type pipelineSection struct {
	Available      bool
	Consumed       num
	ConsumedDLQ    num
	Throughput     num
	ThroughputNote string
	MaxLag         num
	FinalLag       num
	DrainSeconds   num
	DrainTimedOut  bool
	DrainAvailable bool
	Flushes        num
	FlushP50       num
	FlushP95       num
	FlushP99       num
	Retries        []kv
	DLQTopic       num
	DLQByClass     []kv
	CommitFailures num
	Fenced         num
	Resyncs        num
	TopicRecords   num
	Poison         *poisonInfo // the injected poison records (poison-records)
	dlqClasses     map[string]float64
	LagChart       *chart
	Reset          bool
}

type correctnessSection struct {
	Available   bool
	Accepted    num
	DBActivity  num
	Overcount   num
	PVAccepted  num
	DBPageViews num
	PVOvercount num
	Users       num
	Unknown     num
	Bound       num
	BoundRule   string
	ByDate      []dbDateRow
}

type containerRow struct {
	Name      string
	Samples   int
	CPUAvg    num
	CPUPeak   num
	MemPeak   num
	MemAvg    num
	MemLimit  num
	MemPct    num // peak / limit, 0..1
	NetRx     num
	NetTx     num
	OOMKilled string
	Restarts  string
	ExitCode  string
	Status    string
}

type resourcesSection struct {
	Available  bool
	Containers []containerRow
	CPUChart   *chart
	MemChart   *chart
	Skipped    int
	// WindowNote says which samples the CPU and memory figures cover.
	WindowNote string
	inspect    []containerState
	inspectOK  bool
}

type errorsSection struct {
	Available bool
	Services  []logStats
	Top       []logMessage
}

type timelineEvent struct {
	Time    string
	Offset  string // relative to k6 start
	Event   string
	Action  string
	Details string
}

type timelineSection struct {
	Show         bool
	Available    bool
	Events       []timelineEvent
	stopExitCode num
	markers      []event
	// k6Start and k6End are the k6_start and k6_end events (zero when missing).
	k6Start, k6End time.Time
	// consumerRestarted is set by a kill or stop of the consumer.
	consumerRestarted bool
	// poison is the poison_injected event of poison-records (nil otherwise).
	poison *poisonInfo
}

// poisonInfo is what run.sh wrote to timeline.jsonl when it injected the
// malformed records: the total and the error class each is expected to get.
type poisonInfo struct {
	Count   num
	Classes map[string]float64
	Kinds   []kv // injected record kinds with their counts, informational
}

// ClassRows renders the expected DLQ classes for the report.
func (p *poisonInfo) ClassRows() []kv {
	var out []kv
	for _, class := range classUnion(p.Classes, nil) {
		out = append(out, kv{class, some(p.Classes[class]).Int()})
	}
	return out
}

type artifact struct {
	Name, Path, Desc string
}

// loader collects files from a results directory, tolerating missing ones.
type loader struct {
	dir   string
	notes []string
}

func (l *loader) path(name string) string { return filepath.Join(l.dir, name) }

func (l *loader) note(format string, args ...any) {
	l.notes = append(l.notes, fmt.Sprintf(format, args...))
}

// missing records why an optional file could not be used. A file that does
// not exist is expected for partial runs and only noted once.
func (l *loader) missing(name string, err error) {
	if errors.Is(err, fs.ErrNotExist) {
		l.note("%s: not found", name)
		return
	}
	l.note("%s: %v", name, err)
}

func (l *loader) prom(name string) (promSet, bool) {
	f, err := os.Open(l.path(name))
	if err != nil {
		l.missing(name, err)
		return nil, false
	}
	defer func() { _ = f.Close() }() // opened read-only: a failed close loses nothing
	set, err := parseProm(f)
	if err != nil {
		l.note("%s: %v", name, err)
		return nil, false
	}
	return set, true
}

// buildReport reads every file of a results directory and computes the report.
// Only an unreadable directory is an error; anything else degrades a section.
func buildReport(dir string) (*report, error) {
	st, err := os.Stat(dir) //nolint:gosec // G703: dir is the results directory named on the operator's command line
	if err != nil {
		return nil, fmt.Errorf("results dir: %w", err)
	}
	if !st.IsDir() {
		return nil, fmt.Errorf("results dir %s: not a directory", dir)
	}
	l := &loader{dir: dir}
	r := &report{}

	runObj := l.loadRun(&r.Run)
	cfg := runObj.obj("config", "Config")

	k6, err := readK6Summary(l.path(fileK6Summary))
	if err != nil {
		l.missing(fileK6Summary, err)
		k6 = nil
	}

	prodPre, okPP := l.prom(fileProducerPre)
	prodPost, okPA := l.prom(fileProducerPost)
	consPre, okCP := l.prom(fileConsumerPre)
	consPost, okCA := l.prom(fileConsumerPost)
	if !okPP {
		prodPre = nil // a missing baseline means deltas start at zero
	}
	if !okCP {
		consPre = nil
	}

	l.buildIngestion(&r.Ingestion, k6, r.Run.K6Scen, cfg, prodPre, prodPost, okPA)
	l.buildTimeline(&r.Timeline, r.Run)
	origin, markers := r.Run.k6Start, r.Timeline.markers
	drain := l.loadDrain()
	lag := append(l.loadLag(), drain.samples...)
	slices.SortFunc(lag, func(a, b lagSample) int { return a.Time.Compare(b.Time) })
	l.buildPipeline(&r.Pipeline, consPre, consPost, okCA, drain, lag)
	r.Pipeline.LagChart = lagChart(lag, origin, markers)
	l.buildCorrectness(&r.Correctness, r, cfg)
	if r.Timeline.consumerRestarted {
		// A baseline taken before the load is ~0, so the reset is invisible
		// in after−before; the timeline is the reliable signal.
		r.Pipeline.Reset = true
	}
	r.Pipeline.finishThroughput(r.Run.Duration, r.Correctness.DBActivity)
	r.Pipeline.Poison = r.Timeline.poison
	rec := l.buildRecovery(&r.Recovery, r.Run.Scenario, r.Timeline.consumerRestarted, r.Pipeline)
	l.buildResources(&r.Resources, r.Run.Project, origin, markers, r.Timeline.k6Start, r.Timeline.k6End)
	l.buildErrors(&r.Errors)

	in := verdictInput{
		Scenario:         r.Run.Scenario,
		K6Available:      k6 != nil,
		Thresholds:       k6.thresholds(),
		Accepted:         r.Correctness.Accepted,
		PVAccepted:       r.Correctness.PVAccepted,
		Unknown:          r.Correctness.Unknown,
		DBActivity:       r.Correctness.DBActivity,
		DBPageViews:      r.Correctness.DBPageViews,
		InspectAvailable: r.Resources.inspectOK,
		Containers:       r.Resources.inspect,
		DrainAvailable:   r.Pipeline.DrainAvailable,
		DrainTimedOut:    r.Pipeline.DrainTimedOut,
		DLQ:              r.Pipeline.dlqTotal(),
		TopicRecords:     r.Pipeline.TopicRecords,
		StopExitCode:     r.Timeline.stopExitCode,
		K6ExitCode:       r.Run.K6Exit,
		GapAvailable:     r.Recovery.Available,
		GapPartitions:    rec.gapPartitions,
		GapRecords:       r.Recovery.Protected,
		DLQByClass:       r.Pipeline.dlqClasses,
		FinalLag:         r.Pipeline.FinalLag,
	}
	if po := r.Timeline.poison; po != nil {
		in.PoisonInjected, in.PoisonClasses = po.Count, po.Classes
	}
	if r.Run.expectMax.OK && r.Run.expectMax.V != r.Correctness.Bound.V {
		l.note("run.json expect.overcount_max = %s differs from the report's bound %s for %s; the report's rule (verdict.go) is used",
			r.Run.expectMax.Int(), r.Correctness.Bound.Int(), r.Run.Scenario)
	}
	r.Checks = evaluate(in)
	r.Pass = passed(r.Checks)
	r.Artifacts = l.artifacts()
	r.Notes = l.notes
	return r, nil
}

func (l *loader) loadRun(rs *runSection) object {
	o, err := readObject(l.path(fileRun))
	if err != nil {
		l.missing(fileRun, err)
	}
	rs.Available = o != nil
	rs.Scenario = o.str("scenario", "SCENARIO")
	rs.Profile = o.str("profile")
	if rs.Scenario == "" {
		// <UTC-ts>-<scenario>[@<profile>] is the results-dir naming convention.
		base := filepath.Base(l.dir)
		if i := strings.Index(strings.ToLower(base), "z-"); i >= 0 {
			rs.Scenario = base[i+2:]
			if sc, prof, ok := strings.Cut(rs.Scenario, "@"); ok {
				rs.Scenario = sc
				if rs.Profile == "" {
					rs.Profile = prof
				}
			}
		} else {
			rs.Scenario = "unknown"
		}
	}
	rs.Crash = hasTimeline(rs.Scenario)
	rs.ExperimentID = o.obj("experiment").str("id")
	rs.ExperimentName = o.obj("experiment").str("name")
	rs.RunID = o.str("run_id", "runId", "RUN_ID", "id")
	rs.Project = o.str("project", "compose_project")
	if rs.Project == "" {
		rs.Project = defaultProject
	}
	if t, ok := o.time("started_at", "start_utc", "start"); ok {
		rs.Start = t.Format(time.RFC3339)
	}
	if t, ok := o.time("finished_at", "end_utc", "end"); ok {
		rs.End = t.Format(time.RFC3339)
	}
	k6s, okS := o.time("k6_started_at", "k6_start")
	k6e, okE := o.time("k6_finished_at", "k6_end")
	if okS {
		rs.K6Start = k6s.Format(time.RFC3339)
		rs.k6Start = k6s
	}
	if okE {
		rs.K6End = k6e.Format(time.RFC3339)
	}
	if okS && okE {
		rs.Duration = some(k6e.Sub(k6s).Seconds())
	}
	rs.K6Exit = maybe(o.num("k6_exit_code"))
	rs.K6Scen = o.str("k6_scenario")
	rs.expectMax = maybe(o.obj("expect").num("overcount_max"))
	host := o.obj("host")
	if v := host.str("docker_server_version"); v != "" {
		rs.Docker = strings.TrimSpace(v + " " + host.str("docker_os") + " (compose " + host.str("compose_version") + ")")
	}
	if v, ok := host.num("docker_cpus", "cpus", "ncpu"); ok {
		rs.HostCPUs = some(v).Int()
	} else if v, ok := o.num("host_cpus"); ok {
		rs.HostCPUs = some(v).Int()
	}
	if v, ok := host.num("docker_mem_bytes", "mem_bytes", "memory_bytes", "mem_total"); ok {
		rs.HostMem = formatBytes(v)
	} else if s := host.str("mem", "memory"); s != "" {
		rs.HostMem = s
	} else if v, ok := o.num("host_mem_bytes"); ok {
		rs.HostMem = formatBytes(v)
	}
	rs.Images = flattenImages(o)
	rs.Limits = flattenLimits(o.obj("limits", "resources"))
	rs.Config = flatten(o.obj("config", "Config"), "")
	return o
}

// flattenImages accepts {"svc": "image:tag"} or the array printed by
// `docker compose images --format json`.
func flattenImages(o object) []kv {
	v, ok := o.lookup("images", "image_tags", "versions")
	if !ok {
		return nil
	}
	switch t := v.(type) {
	case map[string]any:
		return flatten(object(t), "")
	case []any:
		var out []kv
		for _, e := range t {
			m, ok := e.(map[string]any)
			if !ok {
				continue
			}
			eo := object(m)
			name := shortName(eo.str("Service", "service", "ContainerName", "name"), defaultProject)
			img := eo.str("image", "Image")
			if img == "" {
				img = eo.str("Repository", "repository")
				if tag := eo.str("Tag", "tag"); tag != "" {
					img += ":" + tag
				}
			}
			out = append(out, kv{name, img})
		}
		slices.SortFunc(out, func(a, b kv) int { return strings.Compare(a.K, b.K) })
		return out
	}
	return nil
}

// flattenLimits renders {"svc": {"cpus": …, "memory": …}} as one row per service.
func flattenLimits(o object) []kv {
	var out []kv
	for _, svc := range slices.Sorted(maps.Keys(o)) {
		m, ok := o[svc].(map[string]any)
		if !ok {
			out = append(out, kv{svc, object{"v": o[svc]}.str("v")})
			continue
		}
		var parts []string
		for _, kvs := range flatten(object(m), "") {
			parts = append(parts, kvs.K+"="+kvs.V)
		}
		out = append(out, kv{svc, strings.Join(parts, " ")})
	}
	return out
}

// shortName strips the compose project prefix from a container name:
// "moveo-loadtest-consumer-1" → "consumer-1".
func shortName(name, project string) string {
	return strings.TrimPrefix(strings.TrimPrefix(name, "/"), project+"-")
}

// flatten renders a nested object as sorted dotted key/value rows.
func flatten(o object, prefix string) []kv {
	var out []kv
	for _, k := range slices.Sorted(maps.Keys(o)) {
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if m, ok := o[k].(map[string]any); ok {
			out = append(out, flatten(object(m), key)...)
			continue
		}
		v := object{"v": o[k]}.str("v")
		switch {
		case o[k] == nil:
			continue
		case v == "":
			v = "(default)"
		}
		out = append(out, kv{key, v})
	}
	return out
}

// findNum looks for a numeric key anywhere in a nested config object.
func findNum(o object, keys ...string) (float64, bool) {
	if v, ok := o.num(keys...); ok {
		return v, true
	}
	for _, k := range slices.Sorted(maps.Keys(o)) {
		if m, ok := o[k].(map[string]any); ok {
			if v, ok := findNum(object(m), keys...); ok {
				return v, true
			}
		}
	}
	return 0, false
}

// k6DefaultRate mirrors the constant-arrival-rate defaults of
// loadtest/k6/activities.js, used when run.json leaves LT_RATE empty.
// Ramping scenarios have no single target.
var k6DefaultRate = map[string]float64{"load": 300, "soak": 150, "crash": 200}

func (l *loader) buildIngestion(in *ingestionSection, k6 *k6Summary, k6Scenario string, cfg object, pre, post promSet, postOK bool) {
	if postOK {
		for _, res := range slices.Sorted(maps.Keys(post.byLabel("activities_published_total", "result"))) {
			before, _ := pre.sum("activities_published_total", map[string]string{"result": res})
			after, _ := post.sum("activities_published_total", map[string]string{"result": res})
			d, _ := counterDelta(before, after)
			in.Published = append(in.Published, kv{res, some(d).Int()})
		}
	}
	if v, ok := findNum(cfg, "LT_RATE", "rate", "target_rate"); ok {
		in.TargetRate = some(v)
	} else if v, ok := k6DefaultRate[k6Scenario]; ok {
		in.TargetRate = some(v)
		in.TargetNote = "script default"
	}
	if k6 == nil {
		return
	}
	in.Available = true
	in.Requests = maybe(k6.value("http_reqs", "count"))
	in.Rate = maybe(k6.value("http_reqs", "rate"))
	in.Iterations = maybe(k6.value("iterations", "count"))
	in.Dropped = some(k6.count("dropped_iterations"))
	in.Accepted = some(k6.count("events_accepted"))
	in.PageViews = some(k6.count("page_views_accepted"))
	in.Rejected = some(k6.count("events_rejected"))
	in.Unknown = some(k6.count("events_unknown"))
	in.FailedRate = maybe(k6.value("http_req_failed", "rate"))
	in.ChecksRate = maybe(k6.value("checks", "rate"))
	in.Avg = maybe(k6.value("http_req_duration", "avg"))
	in.P50 = maybe(k6.value("http_req_duration", "med"))
	in.P90 = maybe(k6.value("http_req_duration", "p(90)"))
	in.P95 = maybe(k6.value("http_req_duration", "p(95)"))
	in.P99 = maybe(k6.value("http_req_duration", "p(99)"))
	in.Max = maybe(k6.value("http_req_duration", "max"))
	in.PrevDay = some(k6.count("events_accepted_prev_day"))
	in.InvalidSent = some(k6.count("invalid_sent"))
	sb := k6.statusBreakdown()
	for _, k := range slices.Sorted(maps.Keys(sb)) {
		in.Status = append(in.Status, kv{k, some(sb[k]).Int()})
	}
	if len(in.Status) == 0 {
		// The script classifies responses with counters instead of tagging
		// http_reqs by status.
		in.Status = []kv{
			{"202 accepted", in.Accepted.Int()},
			{"4xx rejected", in.Rejected.Int()},
			{"5xx / timeout (unknown)", in.Unknown.Int()},
		}
	}
}

type drainInfo struct {
	ok       bool
	seconds  num
	finalLag num
	maxLag   num
	timedOut bool
	samples  []lagSample // drain polls (kafka-consumer-groups)
}

func (l *loader) loadDrain() drainInfo {
	o, err := readObject(l.path(fileDrain))
	if err != nil {
		l.missing(fileDrain, err)
		return drainInfo{}
	}
	d := drainInfo{ok: true}
	d.seconds = maybe(o.num("seconds", "drain_seconds"))
	d.finalLag = maybe(o.num("final_lag", "lag", "total_lag"))
	d.maxLag = maybe(o.num("max_lag", "peak_lag"))
	d.timedOut, _ = o.boolean("timed_out", "timeout")
	if v, ok := o.lookup("samples"); ok {
		arr, _ := v.([]any)
		for _, e := range arr {
			m, ok := e.(map[string]any)
			if !ok {
				continue
			}
			so := object(m)
			ts, okT := so.time("ts")
			lag, okL := so.num("lag")
			if okT && okL {
				d.samples = append(d.samples, lagSample{ts, lag})
			}
		}
	}
	return d
}

// lagSample is one total-lag observation from lag.jsonl.
type lagSample struct {
	Time time.Time
	Lag  float64
}

// loadLag reads lag.jsonl (consumer /metrics during the run); samples whose
// lag is null (consumer down) are skipped.
func (l *loader) loadLag() []lagSample {
	objs, bad, err := readJSONLines(l.path(fileLag))
	if err != nil {
		l.missing(fileLag, err)
		return nil
	}
	if bad > 0 {
		l.note("%s: %d malformed lines skipped", fileLag, bad)
	}
	var out []lagSample
	for _, o := range objs {
		ts, okT := o.time("ts")
		lag, okL := o.num("lag")
		if okT && okL {
			out = append(out, lagSample{ts, lag})
		}
	}
	return out
}

func (l *loader) buildPipeline(p *pipelineSection, pre, post promSet, postOK bool, d drainInfo, lag []lagSample) {
	p.DrainAvailable = d.ok
	p.DrainSeconds = d.seconds
	p.DrainTimedOut = d.timedOut
	p.FinalLag = d.finalLag
	p.MaxLag = d.maxLag
	for _, s := range lag {
		if !p.MaxLag.OK || s.Lag > p.MaxLag.V {
			p.MaxLag = some(s.Lag)
		}
	}

	if b, err := os.ReadFile(l.path(fileTopicOffsets)); err != nil {
		l.missing(fileTopicOffsets, err)
	} else if v, err := parseOffsets(strings.NewReader(string(b))); err != nil {
		l.note("%s: %v", fileTopicOffsets, err)
	} else {
		p.TopicRecords = some(v)
	}

	if b, err := os.ReadFile(l.path(fileDLQOffsets)); err != nil {
		l.missing(fileDLQOffsets, err)
	} else if v, err := parseOffsets(strings.NewReader(string(b))); err != nil {
		l.note("%s: %v", fileDLQOffsets, err)
	} else {
		p.DLQTopic = some(v)
	}

	if !postOK {
		return
	}
	p.Available = true
	delta := func(name string, labels map[string]string) num {
		after, found := post.sum(name, labels)
		if !found {
			return num{}
		}
		before, _ := pre.sum(name, labels)
		d, reset := counterDelta(before, after)
		p.Reset = p.Reset || reset
		return some(d)
	}
	p.Consumed = delta("records_consumed_total", map[string]string{"result": "ok"})
	p.ConsumedDLQ = delta("records_consumed_total", map[string]string{"result": "dlq"})
	p.CommitFailures = delta("offset_commit_failures_total", nil)
	p.Fenced = delta("offset_fence_rejections_total", nil)
	p.Resyncs = delta("offset_fence_resyncs_total", nil)
	p.Flushes = delta("aggregate_flush_duration_seconds_count", nil)
	for _, dep := range slices.Sorted(maps.Keys(post.byLabel("retries_total", "dependency"))) {
		p.Retries = append(p.Retries, kv{dep, delta("retries_total", map[string]string{"dependency": dep}).Int()})
	}
	p.dlqClasses = map[string]float64{}
	for _, class := range slices.Sorted(maps.Keys(post.byLabel("dlq_records_total", "error_class"))) {
		d := delta("dlq_records_total", map[string]string{"error_class": class})
		p.DLQByClass = append(p.DLQByClass, kv{class, d.Int()})
		p.dlqClasses[class] = d.V
	}
	bs, reset := bucketDelta(pre.buckets("aggregate_flush_duration_seconds"), post.buckets("aggregate_flush_duration_seconds"))
	p.Reset = p.Reset || reset
	p.FlushP50 = maybe(histogramQuantile(0.50, bs))
	p.FlushP95 = maybe(histogramQuantile(0.95, bs))
	p.FlushP99 = maybe(histogramQuantile(0.99, bs))
	if !p.FinalLag.OK {
		if v, ok := post.sum("consumer_lag_records", nil); ok {
			p.FinalLag = some(v)
		}
	}
}

// finishThroughput computes consumer throughput over load + drain time. When
// the consumer restarted, its counters cover only the last process, so the
// database total is used instead.
func (p *pipelineSection) finishThroughput(load, dbActivity num) {
	window := 0.0
	if load.OK {
		window = load.V
	}
	if p.DrainSeconds.OK {
		window += p.DrainSeconds.V
	}
	if window <= 0 {
		return
	}
	switch {
	case p.Reset && dbActivity.OK:
		p.Throughput = some(dbActivity.V / window)
		p.ThroughputNote = "the consumer restarted during the run, so its counters (consumed, flushes, retries, flush quantiles) cover the last process only; " +
			"throughput is SUM(activity_count) ÷ (load duration + drain time)"
	case p.Reset:
		p.ThroughputNote = "the consumer restarted during the run, so its counters cover the last process only"
	case p.Consumed.OK:
		p.Throughput = some(p.Consumed.V / window)
		p.ThroughputNote = "throughput is records_consumed_total{result=\"ok\"} ÷ (load duration + drain time)"
	}
}

// dlqTotal prefers the DLQ topic end offsets (they survive consumer restarts).
func (p pipelineSection) dlqTotal() num {
	if p.DLQTopic.OK {
		return p.DLQTopic
	}
	if p.ConsumedDLQ.OK && !p.Reset {
		return p.ConsumedDLQ
	}
	return num{}
}

func (l *loader) buildCorrectness(c *correctnessSection, r *report, cfg object) {
	in := r.Ingestion
	c.Accepted, c.PVAccepted, c.Unknown = in.Accepted, in.PageViews, in.Unknown
	if !in.Available {
		c.Accepted, c.PVAccepted, c.Unknown = num{}, num{}, num{}
	}
	c.Bound = some(overcountBound)
	c.BoundRule = "exactly zero in every scenario: the consumer stores its Kafka positions with the aggregates (ADR 0008, option A)"

	f, err := os.Open(l.path(fileDBTotals))
	if err != nil {
		l.missing(fileDBTotals, err)
	} else {
		defer func() { _ = f.Close() }() // opened read-only: a failed close loses nothing
		t, err := parseDBTotals(f)
		if err != nil {
			l.note("%s: %v", fileDBTotals, err)
		} else {
			c.Available = true
			c.DBActivity, c.DBPageViews = some(t.Activity), some(t.PageViews)
			c.Users = maybe(t.Users, t.HasUsers)
		}
	}
	if c.Accepted.OK && c.DBActivity.OK {
		c.Overcount = some(c.DBActivity.V - c.Accepted.V)
	}
	if c.PVAccepted.OK && c.DBPageViews.OK {
		c.PVOvercount = some(c.DBPageViews.V - c.PVAccepted.V)
	}

	g, err := os.Open(l.path(fileDBByDate))
	if err != nil {
		l.missing(fileDBByDate, err)
		return
	}
	defer func() { _ = g.Close() }() // opened read-only: a failed close loses nothing
	rows, err := parseDBByDate(g)
	if err != nil {
		l.note("%s: %v", fileDBByDate, err)
		return
	}
	c.ByDate = rows
}

func (l *loader) buildErrors(e *errorsSection) {
	// Counts: logs-summary.txt is authoritative (run.sh also matches Kafka and
	// Postgres formats); top messages come from the raw logs.
	if b, err := os.ReadFile(l.path(fileLogsSummary)); err != nil {
		l.missing(fileLogsSummary, err)
	} else if stats, err := parseLogsSummary(strings.NewReader(string(b))); err != nil {
		l.note("%s: %v", fileLogsSummary, err)
	} else {
		e.Available = true
		e.Services = stats
	}
	stats, top, err := parseLogs(l.path(dirLogs), topLogMessages)
	if err != nil {
		l.note("%s/: %v", dirLogs, err)
		return
	}
	e.Top = top
	if !e.Available {
		e.Available = true
		e.Services = stats
	}
}

func (l *loader) buildTimeline(t *timelineSection, run runSection) {
	t.Show = run.Crash
	objs, bad, err := readJSONLines(l.path(fileTimeline))
	if err != nil {
		l.missing(fileTimeline, err)
		return
	}
	if bad > 0 {
		l.note("%s: %d malformed lines skipped", fileTimeline, bad)
	}
	t.Available = len(objs) > 0
	ref := run.k6Start
	for _, o := range objs {
		ev := timelineEvent{Event: o.str("event", "type")}
		if ts, ok := o.time("ts", "time", "timestamp"); ok {
			ev.Time = ts.Format("15:04:05.000")
			if !ref.IsZero() {
				ev.Offset = fmt.Sprintf("%+.1f s", ts.Sub(ref).Seconds())
			}
		}
		ev.Action = strings.TrimSpace(o.str("action") + " " + o.str("service", "target"))
		var details []string
		for _, k := range slices.Sorted(maps.Keys(o)) {
			switch normKey(k) {
			case "ts", "event", "action", "service", "target":
				continue
			}
			if o[k] == nil {
				continue
			}
			details = append(details, k+"="+o.str(k))
		}
		ev.Details = strings.Join(details, " ")
		if ts, ok := o.time("ts"); ok && ev.Event == "chaos" {
			t.markers = append(t.markers, event{T: ts, Label: ev.Action})
		}
		if ts, ok := o.time("ts"); ok {
			switch ev.Event {
			case "k6_start":
				t.k6Start = ts
			case "k6_end":
				t.k6End = ts
			}
		}
		if ev.Event == "chaos" && strings.HasPrefix(o.str("service", "target"), "consumer") &&
			(o.str("action") == "kill" || o.str("action") == "stop") {
			t.consumerRestarted = true
		}
		if ev.Event == "chaos" && o.str("action") == "stop" {
			if code, ok := consumerExitCode(o.obj("exit_codes")); ok {
				t.stopExitCode = some(code)
			}
		}
		if ev.Event == "poison_injected" {
			t.poison = parsePoison(o)
		}
		if ev.Event == "collect_error" {
			l.note("collection step %q failed: %s", o.str("step"), o.str("detail"))
		}
		t.Events = append(t.Events, ev)
	}
}

// consumerExitCode returns the highest exit code among consumer containers
// in a chaos event's exit_codes map, so one bad replica fails the check.
func consumerExitCode(codes object) (float64, bool) {
	var worst float64
	found := false
	for _, name := range slices.Sorted(maps.Keys(codes)) {
		if !strings.Contains(name, "consumer") {
			continue
		}
		if v, ok := codes.num(name); ok && (!found || v > worst) {
			worst, found = v, true
		}
	}
	return worst, found
}

func (l *loader) artifacts() []artifact {
	cands := []artifact{
		{"k6 dashboard", fileK6Dashboard, "k6 web-dashboard export (time-series charts)"},
		{"k6 summary", fileK6Summary, "k6 handleSummary data"},
		{"k6 output", fileK6Output, "k6 stdout/stderr"},
		{"Run metadata", fileRun, "scenario, timestamps, host, images, limits, config"},
		{"Container stats", fileStats, "docker stats samples (JSON lines)"},
		{"Consumer lag", fileLag, "lag samples (JSON lines)"},
		{"Timeline", fileTimeline, "run events and chaos actions (JSON lines)"},
		{"Chaos log", fileChaosLog, "output of the chaos commands"},
		{"Drain", fileDrain, "drain time and final lag"},
		{"Producer metrics (before)", fileProducerPre, "Prometheus text"},
		{"Producer metrics (after)", fileProducerPost, "Prometheus text"},
		{"Consumer metrics (before)", fileConsumerPre, "Prometheus text"},
		{"Consumer metrics (after)", fileConsumerPost, "Prometheus text"},
		{"DB totals", fileDBTotals, "SUM(activity_count), SUM(page_view_count), users"},
		{"DB by date", fileDBByDate, "rows per activity_date"},
		{"DLQ offsets", fileDLQOffsets, "DLQ topic end offsets"},
		{"Topic offsets", fileTopicOffsets, "input topic end offsets"},
		{"Container inspect", fileInspect, "OOMKilled, restarts, exit codes"},
		{"Logs summary", fileLogsSummary, "ERROR/WARN counts per service"},
	}
	var out []artifact
	for _, a := range cands {
		if _, err := os.Stat(l.path(a.Path)); err == nil {
			out = append(out, a)
		}
	}
	logs, _ := filepath.Glob(filepath.Join(l.dir, dirLogs, "*.log"))
	slices.Sort(logs)
	for _, f := range logs {
		base := filepath.Base(f)
		out = append(out, artifact{"Log: " + strings.TrimSuffix(base, ".log"), dirLogs + "/" + base, "container log"})
	}
	return out
}
