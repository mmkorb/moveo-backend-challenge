// k6 load script for POST /v1/activities (ADR 0005, ADR 0016). Tested with grafana/k6:2.3.0.
//
// Select the scenario with SCENARIO=smoke|load|stress|spike|soak|crash. Every rate/duration is
// tunable with LT_* env vars (see CONFIG below) and capped: rate <= MAX_RATE req/s, maxVUs <= MAX_VUS.
//
// Outputs (written by handleSummary / the k6 web dashboard into /results, the run's results dir):
//   /results/k6-summary.json   full handleSummary data object (legacy shape)
//   /results/k6-dashboard.html web-dashboard export (K6_WEB_DASHBOARD=true, K6_WEB_DASHBOARD_EXPORT)
// and a compact text summary on stdout (k6 v2 has no built-in textSummary module and we avoid
// remote jslib imports, so the script renders its own).

import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';
import { nextEvent } from './lib/payload.js';

const MAX_RATE = 3000; // req/s, hard cap for any scenario
const MAX_VUS = 300; // hard cap (ADR 0016 resource budget: k6 has 1 CPU / 768M)
const MAX_DURATION_S = 2 * 60 * 60; // 2 h

const SCENARIOS = ['smoke', 'load', 'stress', 'spike', 'soak', 'crash'];

function env(name, fallback) {
  const v = __ENV[name];
  return v === undefined || v === '' ? fallback : v;
}

function num(name, fallback, min, max) {
  const raw = env(name, '');
  const v = raw === '' ? fallback : Number(raw);
  if (!Number.isFinite(v)) {
    throw new Error(`${name}=${raw} is not a number`);
  }
  return Math.min(max, Math.max(min, v));
}

function int(name, fallback, min, max) {
  return Math.floor(num(name, fallback, min, max));
}

// parseDuration accepts k6-style durations ("90s", "5m", "1h30m", "2m30s") or plain seconds.
function parseDuration(name, fallback) {
  const raw = String(env(name, fallback)).trim();
  let seconds;
  if (/^\d+(\.\d+)?$/.test(raw)) {
    seconds = Number(raw);
  } else {
    const re = /(\d+(?:\.\d+)?)(h|m|s)/g;
    let m;
    let consumed = '';
    seconds = 0;
    while ((m = re.exec(raw)) !== null) {
      seconds += Number(m[1]) * { h: 3600, m: 60, s: 1 }[m[2]];
      consumed += m[0];
    }
    if (consumed !== raw) {
      throw new Error(`${name}=${raw} is not a duration like 90s, 5m or 1h30m`);
    }
  }
  return Math.min(MAX_DURATION_S, Math.max(1, Math.round(seconds)));
}

const SCENARIO = env('SCENARIO', 'smoke');
if (!SCENARIOS.includes(SCENARIO)) {
  throw new Error(`SCENARIO=${SCENARIO} is not one of ${SCENARIOS.join(', ')}`);
}

const RUN_ID = env('RUN_ID', 'adhoc');
if (!/^[0-9a-z]+$/.test(RUN_ID)) {
  // Only [0-9a-z]: the id ends up in a SQL LIKE pattern, where _ and % are wildcards.
  throw new Error(`RUN_ID=${RUN_ID} must match [0-9a-z]+`);
}

// Per-scenario defaults (ADR 0016).
const DEFAULTS = {
  smoke: { rate: 0, duration: '2m', iterations: 200 },
  load: { rate: 300, duration: '5m' },
  stress: { startRate: 100, peakRate: 1500, duration: '10m' },
  spike: { startRate: 50, peakRate: 1500, duration: '30s' }, // duration = time at peak
  soak: { rate: 150, duration: '30m' },
  crash: { rate: 200, duration: '3m' },
}[SCENARIO];

const maxVUs = int('LT_MAX_VUS', MAX_VUS, 1, MAX_VUS);

const CONFIG = {
  scenario: SCENARIO,
  runId: RUN_ID,
  targetUrl: env('TARGET_URL', 'http://producer:8080'),
  rate: int('LT_RATE', DEFAULTS.rate || 0, 1, MAX_RATE),
  startRate: int('LT_START_RATE', DEFAULTS.startRate || 0, 1, MAX_RATE),
  peakRate: int('LT_PEAK_RATE', DEFAULTS.peakRate || 0, 1, MAX_RATE),
  durationS: parseDuration('LT_DURATION', DEFAULTS.duration),
  iterations: int('LT_ITERATIONS', DEFAULTS.iterations || 200, 1, 1000000),
  preAllocatedVUs: int('LT_PRE_ALLOCATED_VUS', 50, 1, maxVUs),
  maxVUs,
  users: int('LT_USERS', 5000, 1, 10000000),
  invalidRatio: num('LT_INVALID_RATIO', 0, 0, 1),
  prevDayRatio: num('LT_PREV_DAY_RATIO', 0.05, 0, 1),
  requestTimeout: env('LT_REQUEST_TIMEOUT', '10s'),
  // Chaos window (crash only), seconds from the start of the scenario. run.sh performs the action;
  // the script only tags requests inside [at - 10, at + downFor + recovery] as phase=outage, so the
  // strict thresholds apply to phase=steady and the outage is reported, not failed.
  chaosAt: int('LT_CHAOS_AT', 60, 0, MAX_DURATION_S),
  chaosDownFor: int('LT_CHAOS_DOWN_FOR', 15, 0, MAX_DURATION_S),
  chaosRecovery: int('LT_CHAOS_RECOVERY', 30, 0, MAX_DURATION_S),
};

function constantArrival(rate, durationS) {
  return {
    executor: 'constant-arrival-rate',
    rate,
    timeUnit: '1s',
    duration: `${durationS}s`,
    preAllocatedVUs: CONFIG.preAllocatedVUs,
    maxVUs: CONFIG.maxVUs,
    gracefulStop: '10s',
  };
}

// stressStages: 7 equal steps from startRate to peakRate within durationS; each step ramps for 10 s
// and then holds, so the report can see where p95, dropped iterations and lag start to rise.
function stressStages(start, peak, durationS) {
  const steps = 7;
  const stepS = Math.max(20, Math.floor(durationS / steps));
  const stages = [];
  for (let i = 1; i <= steps; i += 1) {
    const level = Math.round(start + ((peak - start) * i) / steps);
    stages.push({ target: level, duration: '10s' });
    stages.push({ target: level, duration: `${stepS - 10}s` });
  }
  stages.push({ target: 0, duration: '10s' });
  return stages;
}

function buildScenario() {
  switch (SCENARIO) {
    case 'smoke':
      return {
        executor: 'shared-iterations',
        vus: 1,
        iterations: CONFIG.iterations,
        maxDuration: `${CONFIG.durationS}s`,
      };
    case 'load':
    case 'soak':
    case 'crash':
      return constantArrival(CONFIG.rate, CONFIG.durationS);
    case 'stress':
      return {
        executor: 'ramping-arrival-rate',
        startRate: CONFIG.startRate,
        timeUnit: '1s',
        preAllocatedVUs: CONFIG.preAllocatedVUs,
        maxVUs: CONFIG.maxVUs,
        stages: stressStages(CONFIG.startRate, CONFIG.peakRate, CONFIG.durationS),
        gracefulStop: '10s',
      };
    case 'spike':
      return {
        executor: 'ramping-arrival-rate',
        startRate: CONFIG.startRate,
        timeUnit: '1s',
        preAllocatedVUs: CONFIG.preAllocatedVUs,
        maxVUs: CONFIG.maxVUs,
        stages: [
          { target: CONFIG.startRate, duration: '1m' },
          { target: CONFIG.peakRate, duration: '10s' },
          { target: CONFIG.peakRate, duration: `${CONFIG.durationS}s` },
          { target: CONFIG.startRate, duration: '10s' },
          { target: CONFIG.startRate, duration: '2m' },
        ],
        gracefulStop: '10s',
      };
    default:
      throw new Error(`unhandled scenario ${SCENARIO}`);
  }
}

// Safety rails: abort a runaway test (ADR 0016) once enough data is in.
const RAIL_FAILED = { threshold: 'rate<0.10', abortOnFail: true, delayAbortEval: '30s' };
const RAIL_P95 = { threshold: 'p(95)<2000', abortOnFail: true, delayAbortEval: '30s' };

function buildThresholds() {
  switch (SCENARIO) {
    case 'stress':
      return {
        http_req_failed: [RAIL_FAILED, 'rate<0.05'],
        http_req_duration: [RAIL_P95, 'p(95)<1000'],
        checks: ['rate>0.95'],
      };
    case 'crash':
      // The outage is expected to fail requests and slow them down (producer publish timeout 5 s),
      // so the rails are wider and the pass criteria apply to phase=steady only.
      return {
        http_req_failed: [{ threshold: 'rate<0.60', abortOnFail: true, delayAbortEval: '30s' }],
        http_req_duration: [{ threshold: 'p(50)<2000', abortOnFail: true, delayAbortEval: '30s' }],
        'http_req_failed{phase:steady}': ['rate<0.01'],
        'http_req_duration{phase:steady}': ['p(95)<300'],
        'checks{phase:steady}': ['rate>0.99'],
        'http_req_failed{phase:outage}': ['rate<1.01'], // always passes; makes the sub-metric visible
        'http_req_duration{phase:outage}': ['max>=0'],
      };
    default: // smoke, load, spike, soak
      return {
        http_req_failed: [RAIL_FAILED, 'rate<0.01'],
        http_req_duration: [RAIL_P95, 'p(95)<300'],
        checks: ['rate>0.99'],
      };
  }
}

export const options = {
  scenarios: { [SCENARIO]: buildScenario() },
  thresholds: buildThresholds(),
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  discardResponseBodies: false,
  userAgent: `moveo-loadtest/${RUN_ID}`,
};

const eventsAccepted = new Counter('events_accepted');
const pageViewsAccepted = new Counter('page_views_accepted');
const prevDayAccepted = new Counter('events_accepted_prev_day');
const eventsUnknown = new Counter('events_unknown');
const eventsRejected = new Counter('events_rejected');
const invalidSent = new Counter('invalid_sent');

const URL = `${CONFIG.targetUrl}/v1/activities`;
const ACCEPT_400 = http.expectedStatuses(400);

function phase() {
  if (SCENARIO !== 'crash') {
    return 'steady';
  }
  const elapsedS = (Date.now() - exec.scenario.startTime) / 1000;
  const from = CONFIG.chaosAt - 10;
  const to = CONFIG.chaosAt + CONFIG.chaosDownFor + CONFIG.chaosRecovery;
  return elapsedS >= from && elapsedS <= to ? 'outage' : 'steady';
}

function hasEventId(res) {
  try {
    const id = res.json('event_id');
    return typeof id === 'string' && id.length > 0;
  } catch (_) {
    return false;
  }
}

export function setup() {
  // Register every custom counter with 0 so it always appears in k6-summary.json.
  for (const c of [eventsAccepted, pageViewsAccepted, prevDayAccepted, eventsUnknown, eventsRejected, invalidSent]) {
    c.add(0);
  }
  console.log(`config ${JSON.stringify(CONFIG)}`);
  return { config: CONFIG };
}

export default function () {
  const ev = nextEvent(CONFIG);
  const tags = { name: 'POST /v1/activities', phase: phase() };
  const params = {
    headers: { 'Content-Type': 'application/json' },
    timeout: CONFIG.requestTimeout,
    tags,
  };
  if (!ev.valid) {
    params.responseCallback = ACCEPT_400;
    invalidSent.add(1, tags);
  }

  const res = http.post(URL, ev.body, params);

  if (res.status === 202 && ev.valid) {
    eventsAccepted.add(1, tags);
    if (ev.type === 'page_view') {
      pageViewsAccepted.add(1, tags);
    }
    if (ev.prevDay) {
      prevDayAccepted.add(1, tags);
    }
  } else if (res.status === 0 || res.status >= 500) {
    // Timeout, connection error or 5xx (503 = Kafka unavailable / publish timeout): the event may or
    // may not be in Kafka, so it is neither accepted nor rejected.
    eventsUnknown.add(1, tags);
  } else if (res.status >= 400 && res.status < 500) {
    eventsRejected.add(1, tags);
  }

  if (ev.valid) {
    check(res, {
      'status is 202': (r) => r.status === 202,
      'event_id present': (r) => hasEventId(r),
    }, tags);
  } else {
    check(res, { 'invalid payload rejected with 400': (r) => r.status === 400 }, tags);
  }
}

// ---------- end-of-test summary ----------

function fmtNum(v, digits = 2) {
  if (v === undefined || v === null || Number.isNaN(v)) {
    return '-';
  }
  return Number.isInteger(v) ? String(v) : v.toFixed(digits);
}

function fmtMs(v) {
  if (v === undefined || v === null) {
    return '-';
  }
  return v >= 1000 ? `${(v / 1000).toFixed(2)}s` : `${v.toFixed(1)}ms`;
}

function metricLine(data, name) {
  const m = data.metrics[name];
  if (!m) {
    return null;
  }
  const v = m.values;
  switch (m.type) {
    case 'counter':
      return `${name}: count=${fmtNum(v.count)} rate=${fmtNum(v.rate)}/s`;
    case 'rate':
      return `${name}: ${(v.rate * 100).toFixed(2)}% (${fmtNum(v.passes)} true / ${fmtNum(v.fails)} false)`;
    case 'gauge':
      return `${name}: value=${fmtNum(v.value)} min=${fmtNum(v.min)} max=${fmtNum(v.max)}`;
    case 'trend':
      return `${name}: avg=${fmtMs(v.avg)} med=${fmtMs(v.med)} p90=${fmtMs(v['p(90)'])} `
        + `p95=${fmtMs(v['p(95)'])} p99=${fmtMs(v['p(99)'])} max=${fmtMs(v.max)}`;
    default:
      return `${name}: ${JSON.stringify(v)}`;
  }
}

export function textSummary(data) {
  const lines = [];
  lines.push('');
  lines.push(`==== moveo load test: scenario=${SCENARIO} run_id=${RUN_ID} ====`);
  const durS = data.state && data.state.testRunDurationMs ? data.state.testRunDurationMs / 1000 : 0;
  lines.push(`duration: ${durS.toFixed(1)}s`);
  const names = [
    'http_reqs', 'http_req_failed', 'http_req_duration', 'iterations', 'dropped_iterations', 'vus_max',
    'checks', 'events_accepted', 'page_views_accepted', 'events_accepted_prev_day', 'events_unknown',
    'events_rejected', 'invalid_sent',
  ];
  for (const n of names) {
    const l = metricLine(data, n);
    if (l) {
      lines.push(`  ${l}`);
    }
  }
  lines.push('thresholds:');
  let failed = 0;
  for (const [name, m] of Object.entries(data.metrics).sort((a, b) => a[0].localeCompare(b[0]))) {
    if (!m.thresholds) {
      continue;
    }
    for (const [expr, res] of Object.entries(m.thresholds)) {
      if (!res.ok) {
        failed += 1;
      }
      lines.push(`  ${res.ok ? 'PASS' : 'FAIL'}  ${name}  ${expr}`);
    }
  }
  lines.push(`thresholds failed: ${failed}`);
  lines.push('');
  return lines.join('\n');
}

export function handleSummary(data) {
  const path = env('LT_SUMMARY_PATH', '/results/k6-summary.json');
  return {
    [path]: JSON.stringify(data, null, 2),
    stdout: textSummary(data),
  };
}
