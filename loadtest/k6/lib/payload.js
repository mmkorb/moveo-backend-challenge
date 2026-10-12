// Request payloads for POST /v1/activities (ADR 0005).
//
// - user_id: `lt-<RUN_ID>-u<n>` from a pool of `users` ids, skewed so low ids are "hot" users.
//   The prefix lets run.sh count exactly this run's rows in Postgres (user_id LIKE 'lt-<RUN_ID>-%').
// - activity_type: ~60 % page_view, the rest known and unknown types (the API accepts any type).
// - timestamp: now, or (prevDayRatio) a time on the previous UTC day, half of those in the last
//   minute before UTC midnight, to exercise the per-UTC-day aggregation (ADR 0009).
// - metadata: a few small string values (limits: <= 32 entries, value <= 2048 bytes).
// - invalidRatio: share of deliberately invalid payloads (expected 400), default 0.

const DAY_MS = 24 * 60 * 60 * 1000;

// Cumulative weights; must end at 1.
const ACTIVITY_MIX = [
  ['page_view', 0.60],
  ['click', 0.75],
  ['scroll', 0.85],
  ['purchase', 0.90],
  ['login', 0.94],
  ['video_play', 0.97], // types below are not "known" to the domain; they must still be accepted
  ['lt_custom_event', 0.99],
  ['Signup', 1.0],
];

const PAGES = ['/home', '/pricing', '/docs', '/blog/kafka', '/checkout', '/account'];
const REFERRERS = ['https://google.com', 'https://news.ycombinator.com', '', 'https://example.com/ad'];

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

// skewedUser returns an index in [0, users): Math.random()^skew pushes mass towards 0, so with the
// default skew 2 the hottest 10 % of users receive ~32 % of the events.
export function skewedUser(users, skew = 2) {
  return Math.min(users - 1, Math.floor(users * Math.pow(Math.random(), skew)));
}

export function userId(runId, n) {
  return `lt-${runId}-u${n}`;
}

export function activityType() {
  const r = Math.random();
  for (const [type, cumulative] of ACTIVITY_MIX) {
    if (r < cumulative) {
      return type;
    }
  }
  return 'page_view';
}

// timestamp returns [RFC 3339 string, isPrevDay].
export function timestamp(prevDayRatio) {
  const now = Date.now();
  if (Math.random() >= prevDayRatio) {
    return [new Date(now).toISOString(), false];
  }
  const todayStart = Math.floor(now / DAY_MS) * DAY_MS; // 00:00:00Z today
  const prevStart = todayStart - DAY_MS;
  const offset = Math.random() < 0.5
    ? DAY_MS - 1 - Math.floor(Math.random() * 60 * 1000) // last minute before midnight
    : Math.floor(Math.random() * DAY_MS);
  return [new Date(prevStart + offset).toISOString(), true];
}

function metadata(type) {
  const m = { page_url: `https://shop.example.com${pick(PAGES)}`, source: 'loadtest' };
  const ref = pick(REFERRERS);
  if (ref !== '') {
    m.referrer = ref;
  }
  if (type === 'purchase') {
    m.amount_cents = String(100 + Math.floor(Math.random() * 10000));
  }
  return m;
}

// invalidBody returns a JSON body the API must reject with 400.
function invalidBody(runId, user) {
  switch (Math.floor(Math.random() * 4)) {
    case 0: // missing user_id
      return JSON.stringify({ activity_type: 'page_view', timestamp: new Date().toISOString() });
    case 1: // not RFC 3339
      return JSON.stringify({ user_id: userId(runId, user), activity_type: 'click', timestamp: '12/10/2026 10:00' });
    case 2: // unknown field
      return JSON.stringify({
        user_id: userId(runId, user), activity_type: 'click', timestamp: new Date().toISOString(), extra: 1,
      });
    default: // empty activity_type
      return JSON.stringify({ user_id: userId(runId, user), activity_type: '', timestamp: new Date().toISOString() });
  }
}

// nextEvent builds one request body plus the facts the script needs to count it.
export function nextEvent(cfg) {
  const user = skewedUser(cfg.users);
  if (cfg.invalidRatio > 0 && Math.random() < cfg.invalidRatio) {
    return { body: invalidBody(cfg.runId, user), valid: false, type: '', prevDay: false };
  }
  const type = activityType();
  const [ts, prevDay] = timestamp(cfg.prevDayRatio);
  const body = JSON.stringify({
    user_id: userId(cfg.runId, user),
    activity_type: type,
    timestamp: ts,
    metadata: metadata(type),
  });
  return { body, valid: true, type, prevDay };
}
