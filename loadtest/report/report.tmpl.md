# Load test report — {{.Run.Title}}: {{.Verdict}}

{{if .Run.RunID}}Run `{{.Run.RunID}}`{{if .Run.Start}}, started {{.Run.Start}}{{end}}. {{end}}{{len .Checks}} checks, {{.Failed}} failed.

## 1. Verdict

| Check | Expected | Actual | Result |
|---|---|---|---|
{{- range .Checks}}
| {{cell .Name}} | {{cell .Expected}} | {{cell .Actual}} | **{{.Status}}** |
{{- end}}

## 2. Run

{{if .Run.Available -}}
| Item | Value |
|---|---|
| Scenario | `{{.Run.Scenario}}`{{if .Run.K6Scen}} (k6 scenario `{{.Run.K6Scen}}`){{end}} |
| Profile | {{if .Run.Profile}}`{{.Run.Profile}}`{{else}}none (default limits){{end}} |
| Experiment | {{if .Run.ExperimentName}}`{{.Run.ExperimentName}}` (`{{.Run.ExperimentID}}`){{else}}none{{end}} |
| Run ID | `{{.Run.RunID}}` |
| Compose project | `{{.Run.Project}}` |
| Run start / end (UTC) | {{or .Run.Start "n/a"}} → {{or .Run.End "n/a"}} |
| k6 start / end (UTC) | {{or .Run.K6Start "n/a"}} → {{or .Run.K6End "n/a"}} |
| Load duration | {{.Run.Duration.Sec}} |
| k6 exit code | {{.Run.K6Exit.Int}} |
| Docker host | {{or .Run.HostCPUs "n/a"}} CPUs, {{or .Run.HostMem "n/a"}}{{if .Run.Docker}} — {{.Run.Docker}}{{end}} |
{{- if .Run.Images}}

**Images**

| Container | Image |
|---|---|
{{- range .Run.Images}}
| {{cell .K}} | `{{.V}}` |
{{- end}}
{{- end}}
{{- if .Run.Limits}}

**Resource limits**

| Service | Limits |
|---|---|
{{- range .Run.Limits}}
| {{cell .K}} | {{cell .V}} |
{{- end}}
{{- end}}
{{- if .Run.Config}}

**Configuration**

| Key | Value |
|---|---|
{{- range .Run.Config}}
| `{{.K}}` | {{cell .V}} |
{{- end}}
{{- end}}
{{- else -}}
_Not available: run.json missing._
{{- end}}

## 3. Ingestion (producer)

{{with .Ingestion}}{{if .Available -}}
| Metric | Value |
|---|---|
| HTTP requests | {{.Requests.Int}} |
| Achieved rate | {{.Rate.F 1}} req/s |
| Target rate | {{.TargetRate.F 0}} req/s{{if .TargetNote}} ({{.TargetNote}}){{end}} |
| Iterations / dropped | {{.Iterations.Int}} / {{.Dropped.Int}} |
| Accepted (202) | {{.Accepted.Int}} (page views {{.PageViews.Int}}, previous UTC day {{.PrevDay.Int}}) |
| Rejected (4xx) | {{.Rejected.Int}} (deliberately invalid sent: {{.InvalidSent.Int}}) |
| Unknown (timeout / 5xx) | {{.Unknown.Int}} |
| http_req_failed | {{.FailedRate.Pct}} |
| checks | {{.ChecksRate.Pct}} |

**Status breakdown**

| Status | Requests |
|---|---|
{{- range .Status}}
| {{cell .K}} | {{.V}} |
{{- end}}

**Latency (http_req_duration)**

| avg | p50 | p90 | p95 | p99 | max |
|---|---|---|---|---|---|
| {{.Avg.Ms}} | {{.P50.Ms}} | {{.P90.Ms}} | {{.P95.Ms}} | {{.P99.Ms}} | {{.Max.Ms}} |
{{- else -}}
_Not available: k6-summary.json missing or unreadable._
{{- end}}
{{- if .Published}}

**Producer `activities_published_total` (after − before)**

| Result | Count |
|---|---|
{{- range .Published}}
| {{.K}} | {{.V}} |
{{- end}}
{{- end}}{{end}}

## 4. Pipeline (consumer)

{{with .Pipeline -}}
| Metric | Value |
|---|---|
| Events consumed (ok) | {{.Consumed.Int}} |
| Consumer throughput | {{.Throughput.F 1}} events/s |
| Records in input topic | {{.TopicRecords.Int}} |
| Max lag | {{.MaxLag.Int}} |
| Final lag | {{.FinalLag.Int}} |
| Drain time (last request → lag 0) | {{if not .DrainAvailable}}n/a{{else if .DrainTimedOut}}**timed out**{{else}}{{.DrainSeconds.Sec}}{{end}} |
| Flushes | {{.Flushes.Int}} |
| Flush duration p50 / p95 / p99 | {{.FlushP50.Sec}} / {{.FlushP95.Sec}} / {{.FlushP99.Sec}} |
| Retries | {{range $i, $r := .Retries}}{{if $i}}, {{end}}{{$r.K}} {{$r.V}}{{else}}n/a{{end}} |
| DLQ records (topic) | {{.DLQTopic.Int}} |
| DLQ by error class | {{range $i, $r := .DLQByClass}}{{if $i}}, {{end}}{{$r.K}} {{$r.V}}{{else}}n/a{{end}} |
| Offset commit failures (Kafka, informational) | {{.CommitFailures.Int}} |
| Fenced batches (offset_fence_rejections_total) | {{.Fenced.Int}} |
{{- with .Poison}}
| Poison records injected (written straight to the input topic) | {{.Count.Int}}: {{range $i, $r := .Kinds}}{{if $i}}, {{end}}{{$r.K}} {{$r.V}}{{end}} |
| Expected DLQ by error class (injected) | {{range $i, $r := .ClassRows}}{{if $i}}, {{end}}{{$r.K}} {{$r.V}}{{end}} |
{{- end}}
{{- if .ThroughputNote}}

_Note: {{.ThroughputNote}}._
{{- end}}
{{- if not .Available}}

_Consumer metrics not available: values from /metrics are n/a._
{{- end}}
{{- end}}

## 5. Correctness

{{with .Correctness}}{{if .Available -}}
| | Accepted (k6) | Database | Overcount (db − accepted) |
|---|---|---|---|
| Activities | {{.Accepted.Int}} | {{.DBActivity.Int}} | {{.Overcount.Signed}} |
| Page views | {{.PVAccepted.Int}} | {{.DBPageViews.Int}} | {{.PVOvercount.Signed}} |

- Allowed overcount: 0 … {{.Bound.Int}} + events_unknown ({{.Unknown.Int}}). Rule: {{.BoundRule}}.
- A negative overcount is **loss** and always fails.
- Distinct users in the database: {{.Users.Int}}.
{{- if .ByDate}}

| activity_date | rows | users | activity_count | page_view_count |
|---|---|---|---|---|
{{- range .ByDate}}
| {{.Date}} | {{grp .Rows}} | {{grp .Users}} | {{grp .Activity}} | {{grp .PageViews}} |
{{- end}}
{{- end}}
{{- else -}}
_Not available: db-totals.csv missing or empty._
{{- end}}{{end}}

## 6. Resources

{{with .Resources}}{{if .Available -}}
| Container | CPU avg | CPU peak | Mem peak | Mem limit | Peak / limit | Net rx / tx | OOMKilled | Restarts | Exit | Status |
|---|---|---|---|---|---|---|---|---|---|---|
{{- range .Containers}}
| {{.Name}} | {{.CPUAvg.Perc}} | {{.CPUPeak.Perc}} | {{.MemPeak.Bytes}} | {{.MemLimit.Bytes}} | {{.MemPct.Pct}} | {{.NetRx.Bytes}} / {{.NetTx.Bytes}} | {{.OOMKilled}} | {{.Restarts}} | {{.ExitCode}} | {{.Status}} |
{{- end}}

CPU % is docker's figure: 100 % = one core. Time-series charts are in report.html.
{{- if .WindowNote}}

{{.WindowNote}}
{{- end}}
{{- else -}}
_Not available: stats.jsonl and inspect.jsonl missing._
{{- end}}{{end}}

## 7. Errors

{{with .Errors}}{{if .Available -}}
| Service | Lines | ERROR | WARN |
|---|---|---|---|
{{- range .Services}}
| {{.Service}} | {{.Lines}} | {{.Errors}} | {{.Warns}} |
{{- end}}
{{- if .Top}}

**Top messages**

| Service | Level | Count | Message |
|---|---|---|---|
{{- range .Top}}
| {{.Service}} | {{.Level}} | {{.Count}} | {{cell .Msg}} |
{{- end}}
{{- end}}
{{- else -}}
_Not available: logs-summary.txt and logs/ missing._
{{- end}}{{end}}
{{- if .Timeline.Show}}

## 8. Chaos timeline

{{with .Timeline}}{{if .Available -}}
Offsets are relative to the k6 start.

| Time (UTC) | Offset | Event | Action | Details |
|---|---|---|---|---|
{{- range .Events}}
| {{.Time}} | {{.Offset}} | {{.Event}} | {{cell .Action}} | {{cell .Details}} |
{{- end}}
{{- else -}}
_Not available: timeline.jsonl missing._
{{- end}}{{end}}
{{- end}}

{{- if .Recovery.Show}}

## {{.RecoveryNum}}. Recovery

{{with .Recovery}}{{if .Available -}}
Resume events after the consumer's first start (`logs/consumer.log`, {{.Starts}} starts). Gap = stored − committed offset; a partition without a committed offset counts from 0.

| Time (UTC) | Member | Partition | Stored offset | Committed offset | Gap |
|---|---|---|---|---|---|
{{- range .Rows}}
| {{.Time}} | {{.Member}} | {{.Partition}} | {{.Stored}} | {{.Committed}} | {{.Gap}} |
{{- end}}

{{if .GapPartitions -}}
- Records protected from double counting by the stored position: **{{.Protected.Int}}** (partitions that resumed with a gap: {{.GapPartitions}}).
{{- else -}}
- Stored == committed on every partition: the commit gap was not exercised in this run.
{{- end}}
{{- else -}}
_Not available: `logs/consumer.log` is missing or has no resume lines after the first start; the commit gap was not measured._
{{- end}}
{{- if or .FenceRejections.OK .FenceResyncs.OK}}
- Fence counters: `offset_fence_rejections_total` {{.FenceRejections.Int}}, `offset_fence_resyncs_total` {{.FenceResyncs.Int}}{{if .FenceNote}} ({{.FenceNote}}){{end}}.
{{- end}}
{{- end}}
{{- end}}

## {{.ArtifactsNum}}. Artifacts

{{range .Artifacts -}}
- [{{.Name}}]({{.Path}}) — {{.Desc}}
{{end -}}
{{- if .Notes}}

### Collection notes

{{range .Notes -}}
- {{.}}
{{end -}}
{{- end}}
