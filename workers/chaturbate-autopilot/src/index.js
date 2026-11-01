/**
 * Chaturbate top-channels autopilot — Cloudflare Worker
 * -----------------------------------------------------
 * Every 15 minutes (Cron Trigger) this worker wakes up, but it only performs a
 * real scan every `scan_interval_minutes` (default 45) — see the cadence gate
 * below. Each scan reads Chaturbate's public "female" and "couple" room
 * listings and keeps a healthy BUFFER of the most-viewed PUBLIC rooms (above a
 * viewer threshold) ready in the shared Supabase `channel_assignments` pool
 * (status = "unassigned"). Go DVR nodes claim "unassigned" rows when
 * CHANNEL_POOL_MODE = pooled.
 *
 * Behaviour:
 *   - Never removes channels.
 *   - Keeps ~`target_buffer` unassigned channels ready per category (auto
 *     refills as nodes claim/record them). Configurable without redeploy.
 *   - Registers every channel it adds in the `channels` catalogue (best-effort,
 *     after the assignment). Nothing deletes channels automatically any more:
 *     the Go controller's orphan prune used to drop any `channel_assignments`
 *     row whose username was missing from `channels`, which silently undid
 *     freshly added channels (they have no catalogue row until a node records
 *     them) — the "we add channels but the pool never grows" bug. The prune has
 *     been removed on operator instruction; deletions are manual.
 *   - Manual removals stick: channels the worker has auto-added before are
 *     remembered in `pool_autopilot` and are NOT re-added if they later
 *     disappear from the pool — but only when the channel also reached the
 *     `channels` catalogue. An auto-added entry that never got that far was
 *     removed by the old prune, not by an operator, so it stays eligible.
 *   - Only PUBLIC rooms with num_users > min_viewers qualify.
 *   - Cloudflare/custom anti-bot challenge: a headless Chromium (Browser
 *     Rendering binding) loads Chaturbate, lets the challenge clear, then
 *     fetches the roomlist from inside the browser. The freshly minted
 *     clearance cookie is cached in Workers KV and reused on the next run to
 *     reduce browser launches / rate limits.
 *   - Browser budget is treated as scarce (see browser-rendering/limits; the
 *     account is on Workers Free, so Browser Rendering gets 10 minutes/day).
 *     The strategy is to stay *under* the limit rather than collect a 429:
 *       * a scan only runs every `scan_interval_minutes` (default 45) even
 *         though the cron ticks every 15 minutes ("throttled");
 *       * the browser is only launched when a category is actually below its
 *         buffer — a full buffer short-circuits the scan ("buffer_full");
 *       * the roomlist is first tried with a plain `fetch` reusing the cached
 *         clearance cookie (cheap); the browser is only used for what is still
 *         missing ("direct");
 *       * browser time is metered per UTC day in KV (`browser_daily_seconds`,
 *         default 480 s of the free 600 s). Once the budget is nearly spent the
 *         worker refuses to launch ("budget_exhausted") instead of being 429'd;
 *       * launches are spaced by `browser_min_spacing_seconds` (default 90 s) to
 *         respect the per-account new-instance rate limit ("instance_spacing");
 *       * only one scan may be in flight at a time (KV single-flight lock), so
 *         two close-together invocations can never launch two browsers;
 *       * a transient `429` backs off for minutes (honouring Retry-After, with
 *         an exponential cap of 1 h) instead of blanking the rest of the UTC
 *         day. Only the free-plan daily cap ("Browser time limit exceeded for
 *         today") waits for the next UTC day.
 *     Before this, one 429 locked the worker out until the next UTC midnight.
 *   - `GET /` is a status no-op; a scan must be requested explicitly with
 *     `POST /` or `GET /?scan=1` (&dryrun=1 to preview, &force=1 to ignore a
 *     stale backoff) so uptime probes can never mint a browser session.
 *
 * Config (Supabase `app_settings` key `autopilot_config`, self-seeded):
 *   { min_viewers, target_buffer, categories:[{key,label}], stale_minutes,
 *     scan_interval_minutes, browser_daily_seconds, browser_min_spacing_seconds }
 *
 * Env / secrets (see wrangler.toml + `wrangler secret put`):
 *   SUPABASE_URL       (var)    Supabase project URL
 *   SUPABASE_SERVICE_ROLE_KEY (secret) Worker-only key for backend writes
 *   CB_DOMAIN          (var)    https://chaturbate.com
 *   CB_USER_AGENT      (var)    Chrome UA used to talk to Chaturbate
 *   CB_COOKIES         (secret) chaturbate cookies (optional seed)
 *   CB_ALERT_WEBHOOK   (secret, optional) webhook fired on scan failure / stale
 *   CB_SUCCESS_WEBHOOK (secret, optional) webhook fired when channels are added
 *   BROWSER            (binding) Cloudflare Browser Rendering binding
 *   AUTOPILOT          (binding) Workers KV namespace for cached session
 */

import puppeteer from '@cloudflare/puppeteer';

// ---- Defaults (used until autopilot_config exists in app_settings) ----------
const DEFAULT_CONFIG = {
  min_viewers: 20000,
  target_buffer: 10,
  categories: [
    { key: 'f', label: 'female', target_buffer: 1 },
    { key: 'c', label: 'couple', target_buffer: 2 },
  ],
  stale_minutes: 70,
  scan_interval_minutes: 45,
  // Browser Rendering is the scarce resource. Workers Free gets 10 browser
  // minutes (600 s) per UTC day, so we stop *before* the limit instead of
  // earning a 429: keep a safety margin (480 s) and never launch two browsers
  // closer together than the per-account instance rate limit allows.
  browser_daily_seconds: 480,
  browser_min_spacing_seconds: 90,
};
const DEFAULT_UA =
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 ' +
  '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const SESSION_TTL = 2_592_000; // 30 days — cf_clearance actually lasts months, no need to expire sooner
const RATE_LIMIT_KEY = 'rate_limited_until'; // epoch ms until which scans are skipped
const RATE_LIMIT_STREAK_KEY = 'rate_limit_streak'; // consecutive transient 429s
const RL_BASE_MS = 5 * 60 * 1000; // first backoff after a transient 429
const RL_MAX_MS = 60 * 60 * 1000; // cap for exponential backoff
const SCAN_LOCK_KEY = 'scan_lock'; // single-flight guard for browser scans
const SCAN_LOCK_TTL_SEC = 180; // auto-expires if a run dies before releasing
const SCAN_INTERVAL_KEY = 'last_scan_at'; // epoch ms of the last scan that actually ran
const DEFAULT_SCAN_INTERVAL_MIN = 45; // cron tick is 15 min; scans run every 45 min

// Browser-time accountant. Browser Rendering is metered per UTC day; we track
// our own usage and refuse to launch once the budget is nearly gone, so the
// Worker self-limits instead of being 429'd by Cloudflare.
const BROWSER_BUDGET_DAY_KEY = 'browser_budget_day'; // YYYY-MM-DD (UTC)
const BROWSER_USED_KEY = 'browser_seconds_used'; // browser-seconds used that day
const BROWSER_LAST_LAUNCH_KEY = 'browser_last_launch_at'; // epoch ms of last launch
const BROWSER_LAUNCH_ESTIMATE_SEC = 20; // conservative cost of one scan, reserved up front
const ROOMLIST_MAX_ATTEMPTS = 6; // was 15 x 2 s = up to 30 s of billed browser time
const ROOMLIST_RETRY_MS = 1500;
const STALE_ALERT_KEY = 'stale_alert_at'; // throttle repeated staleness alerts
const STALE_ALERT_INTERVAL_MS = 6 * 60 * 60 * 1000;
const CONFIG_KEY = 'autopilot_config';
const SCANS_KEY = 'autopilot_scans';
const HEARTBEAT_KEY = 'autopilot_heartbeat';
// ---- End config ------------------------------------------------------------

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

// ---- app_settings helpers ---------------------------------------------------
function sbHeaders(env, extra = {}) {
  return {
    apikey: env.SUPABASE_SERVICE_ROLE_KEY,
    Authorization: `Bearer ${env.SUPABASE_SERVICE_ROLE_KEY}`,
    'Content-Type': 'application/json',
    ...extra,
  };
}

async function sbGet(env, path) {
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/${path}`, {
    headers: sbHeaders(env),
  });
  if (!resp.ok) throw new Error(`GET ${path} -> HTTP ${resp.status} ${await resp.text()}`);
  return resp.json();
}

async function sbPost(env, path, body, extra = {}) {
  const resp = await fetch(`${env.SUPABASE_URL}/rest/v1/${path}`, {
    method: 'POST',
    headers: sbHeaders(env, extra),
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(`POST ${path} -> HTTP ${resp.status} ${await resp.text()}`);
  return resp;
}

// POST an array of rows in a single request (keeps subrequest count low).
async function sbPostMany(env, path, rows, extra = {}) {
  if (!rows || rows.length === 0) return;
  await sbPost(env, path, rows, extra);
}

async function getSetting(env, key) {
  const rows = await sbGet(env, `app_settings?key=eq.${encodeURIComponent(key)}&select=value&limit=1`);
  if (Array.isArray(rows) && rows[0]) return rows[0].value;
  return null;
}

async function setSetting(env, key, value) {
  await sbPost(
    env,
    'app_settings?on_conflict=key',
    { key, value },
    { Prefer: 'resolution=merge-duplicates' }
  );
}

async function loadConfig(env) {
  let cfg = null;
  try {
    cfg = await getSetting(env, CONFIG_KEY);
  } catch (e) {
    console.error('loadConfig failed, using defaults:', e.message);
  }
  if (!cfg || typeof cfg !== 'object' || !Array.isArray(cfg.categories)) {
    cfg = JSON.parse(JSON.stringify(DEFAULT_CONFIG));
    try {
      await setSetting(env, CONFIG_KEY, cfg); // self-seed so it's editable
    } catch (e) {
      console.error('self-seed config failed:', e.message);
    }
  }
  cfg.min_viewers = Number(cfg.min_viewers) || DEFAULT_CONFIG.min_viewers;
  cfg.target_buffer = Number(cfg.target_buffer) || DEFAULT_CONFIG.target_buffer;
  cfg.stale_minutes = Number(cfg.stale_minutes) || DEFAULT_CONFIG.stale_minutes;
  // Minimum spacing between real scans, in minutes (0 = scan on every cron tick).
  cfg.scan_interval_minutes =
    Number(cfg.scan_interval_minutes) >= 0
      ? Number(cfg.scan_interval_minutes)
      : DEFAULT_CONFIG.scan_interval_minutes;
  // Browser-time safety valve and launch spacing (see DEFAULT_CONFIG).
  cfg.browser_daily_seconds =
    Number(cfg.browser_daily_seconds) > 0
      ? Number(cfg.browser_daily_seconds)
      : DEFAULT_CONFIG.browser_daily_seconds;
  cfg.browser_min_spacing_seconds =
    Number(cfg.browser_min_spacing_seconds) > 0
      ? Number(cfg.browser_min_spacing_seconds)
      : DEFAULT_CONFIG.browser_min_spacing_seconds;
  if (!Array.isArray(cfg.categories) || cfg.categories.length === 0) {
    cfg.categories = DEFAULT_CONFIG.categories;
  }
  // Per-category buffer size (fall back to the global default).
  for (const cat of cfg.categories) {
    cat.target_buffer = Number(cat.target_buffer) || cfg.target_buffer || 10;
  }
  return cfg;
}

// ---- Batched Supabase pool helpers ------------------------------------------
function inList(usernames) {
  return 'in.(' + usernames.map((u) => encodeURIComponent(u)).join(',') + ')';
}

async function batchInPool(env, usernames) {
  if (usernames.length === 0) return new Set();
  const rows = await sbGet(
    env,
    `channel_assignments?username=${inList(usernames)}&site=eq.chaturbate&select=username`
  );
  return new Set((rows || []).map((r) => r.username));
}

async function batchAutoAdded(env, usernames) {
  if (usernames.length === 0) return new Set();
  const rows = await sbGet(env, `pool_autopilot?username=${inList(usernames)}&select=username`);
  return new Set((rows || []).map((r) => r.username));
}

async function batchInChannels(env, usernames) {
  if (usernames.length === 0) return new Set();
  const rows = await sbGet(env, `channels?username=${inList(usernames)}&select=username`);
  return new Set((rows || []).map((r) => r.username));
}

/**
 * Count unassigned chaturbate rows that the worker itself added, grouped by
 * gender (gender lives on `pool_autopilot`). This drives the per-category
 * buffer: deficit = target_buffer - autoUnassigned[gender].
 */
async function countAutoUnassigned(env) {
  const unassigned = await sbGet(
    env,
    `channel_assignments?site=eq.chaturbate&status=eq.unassigned&select=username`
  );
  const autopilot = await sbGet(env, `pool_autopilot?select=username,gender`);
  const genderByUser = {};
  for (const a of autopilot || []) {
    if (a.username && a.gender) genderByUser[a.username] = a.gender;
  }
  const autoByGender = {};
  for (const row of unassigned || []) {
    const g = genderByUser[row.username];
    if (g) autoByGender[g] = (autoByGender[g] || 0) + 1;
  }
  return autoByGender;
}

// ---- Lightweight logging (Cloudflare only, no Supabase writes) --------------
// The worker previously wrote an `info` row to Supabase `channel_logs` for
// every autopilot candidate, which grew the table to ~1.5M rows / 700MB in
// <18h and blew past Supabase usage limits. Logging now stays entirely inside
// Cloudflare: structured console.log (visible in Workers logs / tail) plus a
// small KV ring buffer (AUTOPILOT namespace) that auto-cleans itself.
const CF_LOG_KEY = 'autopilot_log';
const CF_LOG_MAX = 200; // ring buffer size — tiny by design

async function logMany(env, entries) {
  if (!entries || !entries.length) return;
  for (const e of entries) {
    const lvl = (e.log_level || 'info').toUpperCase();
    const who = e.username && e.username !== '__autopilot__' ? ` [${e.username}]` : '';
    console.log(`[autopilot:${lvl}]${who} ${e.message}`);
  }
  try {
    const prev = (await env.AUTOPILOT?.get(CF_LOG_KEY, { type: 'json' })) || [];
    const arr = Array.isArray(prev) ? prev : [];
    for (const e of entries) {
      arr.unshift({
        t: new Date().toISOString(),
        level: e.log_level || 'info',
        user: e.username || null,
        msg: e.message,
      });
    }
    while (arr.length > CF_LOG_MAX) arr.pop();
    await env.AUTOPILOT?.put(CF_LOG_KEY, JSON.stringify(arr));
  } catch (e) {
    console.error('cf log buffer write failed:', e.message);
  }
}

// Keep the KV log buffer bounded (autoclean). Called after each scan.
async function trimChannelLogs(env) {
  try {
    const prev = await env.AUTOPILOT?.get(CF_LOG_KEY, { type: 'json' });
    const arr = Array.isArray(prev) ? prev : [];
    if (arr.length > CF_LOG_MAX) {
      arr.length = CF_LOG_MAX;
      await env.AUTOPILOT?.put(CF_LOG_KEY, JSON.stringify(arr));
    }
  } catch (e) {
    console.error('trimChannelLogs failed:', e.message);
  }
}

// ---- Chaturbate fetch -------------------------------------------------------
function topRooms(data, minViewers) {
  const rooms = Array.isArray(data?.rooms) ? data.rooms : [];
  return rooms
    .filter((r) => (r.current_show || r.room_status) === 'public' && (r.num_users ?? 0) > minViewers)
    .sort((a, b) => (b.num_users ?? 0) - (a.num_users ?? 0));
}

function roomListUrl(genderKey, env) {
  return (
    `${env.CB_DOMAIN}/api/ts/roomlist/room-list/` +
    `?enable_recommendations=false&genders=${genderKey}&limit=100`
  );
}

/** Fetch one category's roomlist from inside the (already-challenged) page. */
async function fetchRoomList(page, genderKey, env) {
  const apiUrl = roomListUrl(genderKey, env);
  return await page.evaluate(
    async ({ url, attempts, retryMs }) => {
      for (let attempt = 0; attempt < attempts; attempt++) {
        try {
          const r = await fetch(url, {
            headers: {
              Accept: 'application/json, text/javascript, */*; q=0.01',
              'X-Requested-With': 'XMLHttpRequest',
            },
          });
          const text = await r.text();
          if (r.ok && text.trim().startsWith('{')) return JSON.parse(text);
        } catch (_) {
          /* retry */
        }
        await new Promise((res) => setTimeout(res, retryMs));
      }
      throw new Error('challenge not cleared / no JSON after retries');
    },
    { url: apiUrl, attempts: ROOMLIST_MAX_ATTEMPTS, retryMs: ROOMLIST_RETRY_MS }
  );
}

/**
 * Cheapest possible roomlist fetch: a plain request from the Worker's own
 * egress, reusing the cached clearance cookie. Chaturbate binds cf_clearance to
 * the browser's IP, so this often fails — but when it does work it saves an
 * entire browser session, which is the scarcest resource we have. Returns null
 * on any failure so the caller can fall back to the browser.
 */
async function fetchRoomListDirect(env, genderKey, cached) {
  try {
    const cookieHeader = (cached?.cookies || [])
      .filter((c) => (c.domain || '').includes('chaturbate'))
      .map((c) => `${c.name}=${c.value}`)
      .join('; ');
    const r = await fetch(roomListUrl(genderKey, env), {
      headers: {
        Accept: 'application/json, text/javascript, */*; q=0.01',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': env.CB_USER_AGENT || DEFAULT_UA,
        ...(cookieHeader ? { Cookie: cookieHeader } : {}),
      },
    });
    if (!r.ok) return null;
    const text = await r.text();
    if (!text.trim().startsWith('{')) return null;
    const data = JSON.parse(text);
    return Array.isArray(data?.rooms) ? data : null;
  } catch (_) {
    return null;
  }
}

// ---- KV session cache -------------------------------------------------------
async function loadCachedSession(env) {
  try {
    if (!env.AUTOPILOT) return null;
    return await env.AUTOPILOT.get('cb_session', { type: 'json' });
  } catch (_) {
    return null;
  }
}

async function saveCachedSession(env, cookies) {
  try {
    if (!env.AUTOPILOT || !cookies) return;
    await env.AUTOPILOT.put('cb_session', JSON.stringify({ cookies, saved_at: Date.now() }), {
      expirationTtl: SESSION_TTL,
    });
  } catch (e) {
    console.error('saveCachedSession failed:', e.message);
  }
}

// ---- Rate-limit backoff (429) -----------------------------------------------
/**
 * Check whether a Browser Rendering rate-limit backoff is currently active.
 * Uses KV so the backoff persists across worker restarts and survives
 * global-scale cold starts.
 */
async function isRateLimited(env) {
  try {
    if (!env.AUTOPILOT) return false;
    const until = await env.AUTOPILOT.get(RATE_LIMIT_KEY);
    if (!until) return false;
    return Date.now() < Number(until);
  } catch (_) {
    return false;
  }
}

/** Next UTC midnight (epoch ms) — only the free-plan daily browser budget waits this long. */
function nextUtcMidnightMs(nowMs = Date.now()) {
  const d = new Date(nowMs);
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() + 1, 0, 0, 0);
}

/** Pull a Retry-After hint out of a puppeteer/Browser Rendering error, if present. */
function retryAfterSeconds(e) {
  try {
    const h = e && e.headers;
    if (h && typeof h.get === 'function') {
      const v = Number(h.get('retry-after') ?? h.get('Retry-After'));
      if (v > 0) return v;
    }
    for (const k of ['retryAfter', 'retry_after']) {
      if (e && Number(e[k]) > 0) return Number(e[k]);
    }
    const m = String((e && e.message) || '').match(/retry[- ]?after\D{0,10}(\d+)/i);
    if (m) return Number(m[1]);
  } catch (_) {}
  return 0;
}

/**
 * Persist a rate-limit backoff.
 *
 * A *transient* 429 from Browser Rendering (the "new browser instance" /
 * concurrency rate limit) must not take the worker out for the rest of the day.
 * The previous implementation always backed off until the next UTC midnight, so
 * a single collision between two close-together invocations killed the
 * autopilot for up to 24 h — which is exactly what happened on 2026-10-14/15
 * (heartbeat stuck at 00:15Z while every later scan bailed out as
 * "rate_limited"). Instead we use a short exponential backoff (5 min → 1 h) and
 * honour any Retry-After the API sends.
 *
 * The only case that legitimately waits for the next UTC day is the
 * free-plan *daily* browser budget: "Browser time limit exceeded for today".
 */
async function setRateLimited(env, { dailyLimit = false, retryAfterSec = 0 } = {}) {
  try {
    if (!env.AUTOPILOT) return;
    const now = Date.now();
    let untilMs;
    let streak = 0;

    if (dailyLimit) {
      untilMs = nextUtcMidnightMs(now);
    } else {
      try {
        streak = Number(await env.AUTOPILOT.get(RATE_LIMIT_STREAK_KEY)) || 0;
      } catch (_) {}
      streak += 1;
      const backoffMs = Math.min(RL_MAX_MS, RL_BASE_MS * 2 ** (streak - 1));
      const retryMs = retryAfterSec > 0 ? retryAfterSec * 1000 : 0;
      untilMs = now + Math.max(backoffMs, retryMs);
      try {
        await env.AUTOPILOT.put(RATE_LIMIT_STREAK_KEY, String(streak), { expirationTtl: 86400 });
      } catch (_) {}
    }

    const ttlSeconds = Math.max(60, Math.ceil((untilMs - now) / 1000));
    await env.AUTOPILOT.put(RATE_LIMIT_KEY, String(untilMs), { expirationTtl: ttlSeconds });
    console.log(
      `rate-limit backoff set until ${new Date(untilMs).toISOString()} ` +
        `(ttl=${ttlSeconds}s, daily=${dailyLimit}, streak=${streak}, retryAfter=${retryAfterSec}s)`
    );
  } catch (e) {
    console.error('setRateLimited failed:', e.message);
  }
}

/** Classify a browser-launch error and record the right kind of backoff. */
async function backoffFromLaunchError(env, e) {
  const msg = String((e && e.message) || '').toLowerCase();
  const dailyLimit = msg.includes('time limit exceeded'); // free-plan daily browser budget
  const transient = msg.includes('429') || msg.includes('rate limit') || msg.includes('too many');
  const retryAfterSec = retryAfterSeconds(e);
  if (!dailyLimit && !transient && !retryAfterSec) return;
  await setRateLimited(env, { dailyLimit, retryAfterSec });
}

/**
 * Clear rate-limit backoff (called after a successful browser launch following
 * a prior rate-limit period). A success also resets the exponential streak.
 */
async function clearRateLimited(env) {
  try {
    if (!env.AUTOPILOT) return;
    await env.AUTOPILOT.delete(RATE_LIMIT_KEY);
    await env.AUTOPILOT.delete(RATE_LIMIT_STREAK_KEY);
    console.log('rate-limit backoff cleared (browser launch succeeded)');
  } catch (_) {}
}

// ---- Single-flight scan lock -------------------------------------------------
/**
 * Only one browser-based scan may be in flight at a time. Cron slots are close
 * together (and the HTTP route can request a scan), and two `puppeteer.launch`
 * calls inside Browser Rendering's minimum instance spacing is precisely what
 * returns `429 ... Rate limit exceeded`.
 */
async function acquireScanLock(env) {
  try {
    if (!env.AUTOPILOT) return true;
    if (await env.AUTOPILOT.get(SCAN_LOCK_KEY)) return false;
    await env.AUTOPILOT.put(SCAN_LOCK_KEY, String(Date.now()), {
      expirationTtl: SCAN_LOCK_TTL_SEC,
    });
    return true;
  } catch (_) {
    return true; // never let lock trouble block a scan
  }
}

async function releaseScanLock(env) {
  try {
    await env.AUTOPILOT?.delete(SCAN_LOCK_KEY);
  } catch (_) {}
}

// ---- Scan cadence (45 min) ---------------------------------------------------
/**
 * The cron tick fires every 15 minutes (cheap: the account is limited to 5 Cron
 * Triggers in total, so we cannot use several offset expressions to build a
 * 45-minute schedule). Instead the worker records the last scan that actually
 * ran and skips the in-between ticks. This keeps the effective browser-scan
 * cadence at `scan_interval_minutes` while using a single cron slot.
 */
async function getLastScanAt(env) {
  try {
    return Number(await env.AUTOPILOT?.get(SCAN_INTERVAL_KEY)) || 0;
  } catch (_) {
    return 0;
  }
}

async function markScanStarted(env) {
  try {
    await env.AUTOPILOT?.put(SCAN_INTERVAL_KEY, String(Date.now()));
  } catch (_) {}
}

// ---- Browser-time budget -----------------------------------------------------
/** UTC day key (YYYY-MM-DD) that the browser budget is accounted against. */
function utcDayKey(nowMs = Date.now()) {
  return new Date(nowMs).toISOString().slice(0, 10);
}

/** Browser seconds already spent today (resets automatically at UTC midnight). */
async function getBrowserSecondsUsed(env) {
  try {
    const storedDay = await env.AUTOPILOT?.get(BROWSER_BUDGET_DAY_KEY);
    if (storedDay !== utcDayKey()) return 0;
    return Number(await env.AUTOPILOT?.get(BROWSER_USED_KEY)) || 0;
  } catch (_) {
    return 0;
  }
}

async function addBrowserSeconds(env, seconds) {
  if (!(seconds > 0)) return;
  try {
    const day = utcDayKey();
    const storedDay = await env.AUTOPILOT?.get(BROWSER_BUDGET_DAY_KEY);
    const used = storedDay === day ? Number(await env.AUTOPILOT?.get(BROWSER_USED_KEY)) || 0 : 0;
    const total = used + seconds;
    await env.AUTOPILOT?.put(BROWSER_BUDGET_DAY_KEY, day, { expirationTtl: 172800 });
    await env.AUTOPILOT?.put(BROWSER_USED_KEY, String(Math.round(total * 10) / 10), {
      expirationTtl: 172800,
    });
  } catch (_) {}
}

async function getLastLaunchAt(env) {
  try {
    return Number(await env.AUTOPILOT?.get(BROWSER_LAST_LAUNCH_KEY)) || 0;
  } catch (_) {
    return 0;
  }
}

async function markBrowserLaunched(env) {
  try {
    await env.AUTOPILOT?.put(BROWSER_LAST_LAUNCH_KEY, String(Date.now()), {
      expirationTtl: 3600,
    });
  } catch (_) {}
}
// ---- End rate-limit helpers -------------------------------------------------

// ---- Heartbeat / stale / metrics -------------------------------------------
async function writeHeartbeat(env, method) {
  try {
    await setSetting(env, HEARTBEAT_KEY, {
      last_success_at: new Date().toISOString(),
      method,
    });
  } catch (e) {
    console.error('writeHeartbeat failed:', e.message);
  }
}

async function checkStale(env, cfg) {
  try {
    const hb = await getSetting(env, HEARTBEAT_KEY);
    if (!hb || !hb.last_success_at) return;
    const ageMin = (Date.now() - new Date(hb.last_success_at).getTime()) / 60000;
    if (ageMin <= cfg.stale_minutes) return;
    // Throttle: without this, every cron tick (and every backoff-window scan)
    // would re-alert, so one long outage could send dozens of notifications.
    try {
      const lastAlert = Number(await env.AUTOPILOT?.get(STALE_ALERT_KEY)) || 0;
      if (Date.now() - lastAlert < STALE_ALERT_INTERVAL_MS) return;
      await env.AUTOPILOT?.put(STALE_ALERT_KEY, String(Date.now()), { expirationTtl: 86400 });
    } catch (_) {}
    await alert(
      env,
      `autopilot heartbeat stale: last successful scan ${ageMin.toFixed(0)} min ago ` +
        `(threshold ${cfg.stale_minutes} min)`
    );
  } catch (e) {
    console.error('checkStale failed:', e.message);
  }
}

async function recordScan(env, scanRow) {
  try {
    const prev = (await getSetting(env, SCANS_KEY)) || [];
    const arr = Array.isArray(prev) ? prev : [];
    arr.unshift(scanRow);
    if (arr.length > 50) arr.length = 50;
    await setSetting(env, SCANS_KEY, arr);
  } catch (e) {
    console.error('recordScan failed:', e.message);
  }
}

async function alert(env, message) {
  if (!env.CB_ALERT_WEBHOOK) return;
  try {
    await fetch(env.CB_ALERT_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: message, level: 'error', source: 'chaturbate-autopilot' }),
    });
  } catch (e) {
    console.error('alert webhook failed:', e.message);
  }
}

async function successAlert(env, added) {
  if (!env.CB_SUCCESS_WEBHOOK || !added || added.length === 0) return;
  try {
    await fetch(env.CB_SUCCESS_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text: 'autopilot added channels', added, source: 'chaturbate-autopilot' }),
    });
  } catch (e) {
    console.error('success webhook failed:', e.message);
  }
}

// ---- Main scan --------------------------------------------------------------
async function runScan(env, dryrun = false, trigger = 'unknown', force = false) {
  const cfg = await loadConfig(env);

  const result = {
    v: 3,
    trigger,
    config: {
      min_viewers: cfg.min_viewers,
      target_buffer: cfg.target_buffer,
      scan_interval_minutes: cfg.scan_interval_minutes,
    },
    method: env.BROWSER ? 'browser' : 'direct',
    female: [],
    couple: [],
    added: [],
    skipped: [],
    errors: [],
  };

  const startedAt = Date.now();
  let browser = null;
  let launchedAt = 0; // epoch ms of the last successful puppeteer.launch

  // A forced run (manual recovery) clears a stale backoff/lock first so an
  // operator is never locked out by an old KV entry.
  if (force) {
    await clearRateLimited(env);
    await releaseScanLock(env);
  }

  // ---- Scan cadence gate -----------------------------------------------------
  // Cron ticks every 15 min; a real scan only happens every
  // `scan_interval_minutes` (default 45). Browser Rendering time is the scarce
  // resource (Workers Free: 10 browser-minutes/day), so the tick stays cheap
  // and the browser work is spread out. Manual runs (`force`) bypass this.
  if (!dryrun && !force && String(trigger).startsWith('cron:')) {
    const intervalMs = (Number(cfg.scan_interval_minutes) || 0) * 60000;
    const lastScanAt = await getLastScanAt(env);
    if (intervalMs > 0 && lastScanAt && Date.now() - lastScanAt < intervalMs) {
      result.method = 'throttled';
      result.next_scan_in_s = Math.round((lastScanAt + intervalMs - Date.now()) / 1000);
      console.log(`autopilot: throttled — next scan in ${result.next_scan_in_s}s`);
      result.duration_ms = Date.now() - startedAt;
      return result;
    }
  }

  // Single-flight guard: never let two invocations launch a browser at once.
  // Dry runs are exempt (they never launch a browser and never write).
  const locked = dryrun || (await acquireScanLock(env));
  if (!locked) {
    result.method = 'skipped';
    result.errors.push('another scan is already in flight — skipping');
    result.duration_ms = Date.now() - startedAt;
    return result;
  }

  try {
    await markScanStarted(env);
    await checkStale(env, cfg); // alert if the previous successful scan was too long ago

    const autoByGender = await countAutoUnassigned(env).catch((e) => {
      result.errors.push(`countAutoUnassigned failed: ${e.message}`);
      return {};
    });

    // Per-category deficit: how many unassigned channels still need adding.
    // No deficit anywhere => no browser launch at all (keeps us well inside
    // Browser Rendering's daily/instance budget) — the cheapest possible scan.
    const deficits = {};
    let needBrowser = false;
    for (const cat of cfg.categories) {
      const d = Math.max(0, cat.target_buffer - (autoByGender[cat.label] || 0));
      deficits[cat.label] = d;
      if (d > 0) needBrowser = true;
    }

    // Gather candidates per category (capped at the per-category deficit).
    const candidates = []; // {category, label, username, num_users}
    let page = null;

    // ---- Phase 1: roomlist fetch (browser is the last resort) ----------------
    // Order of preference, cheapest first:
    //   1. no deficit at all        -> do nothing, no browser, no subrequests
    //   2. plain fetch + cached clearance cookie (saves a whole browser session)
    //   3. headless browser via the Browser Rendering binding
    // The browser is metered (Free plan: 10 browser-minutes/day), so before
    // launching we check our own daily accountant and the launch-spacing guard,
    // which is what keeps us *under* Cloudflare's limit instead of being 429'd.
    const cached = await loadCachedSession(env);
    const topsByCategory = {};

    if (!force && (await isRateLimited(env))) {
      result.method = 'rate_limited';
      const msg = 'rate limited — skipping scan until backoff expires';
      result.errors.push(msg);
      await logMany(env, [{ username: '__autopilot__', log_level: 'warn', message: `autopilot: ${msg}` }]);
    } else if (!needBrowser) {
      result.method = 'buffer_full';
      console.log(
        `autopilot: buffer full (female=${autoByGender.female || 0}, couple=${
          autoByGender.couple || 0
        }) — no browser launch needed`
      );
    } else {
      const wanted = cfg.categories.filter((cat) => (deficits[cat.label] || 0) > 0);

      // Step 2: try the worker's own egress first (free apart from wall time).
      const needingBrowser = [];
      if (cached?.cookies?.length) {
        for (const cat of wanted) {
          const data = await fetchRoomListDirect(env, cat.key, cached);
          if (data) {
            topsByCategory[cat.key] = topRooms(data, cfg.min_viewers);
          } else {
            needingBrowser.push(cat);
          }
        }
        if (Object.keys(topsByCategory).length > 0) result.method = 'direct';
      } else {
        needingBrowser.push(...wanted);
      }

      // Step 3: only fall back to the browser for what is still missing.
      if (needingBrowser.length === 0) {
        console.log('autopilot: roomlist served without a browser session');
      } else if (!env.BROWSER) {
        result.errors.push('BROWSER binding not configured');
      } else {
        const usedSeconds = await getBrowserSecondsUsed(env);
        const budgetLeft = cfg.browser_daily_seconds - usedSeconds;
        const sinceLastLaunchMs = Date.now() - (await getLastLaunchAt(env));
        const minSpacingMs = cfg.browser_min_spacing_seconds * 1000;
        result.browser_seconds_used_today = Math.round(usedSeconds * 10) / 10;

        if (budgetLeft < BROWSER_LAUNCH_ESTIMATE_SEC) {
          // Self-imposed cap: refuse to launch rather than earn a 429. The pool
          // simply waits for the next UTC day.
          result.method = 'budget_exhausted';
          const msg =
            `daily browser budget spent (${usedSeconds.toFixed(1)}s of ` +
            `${cfg.browser_daily_seconds}s) — pausing browser scans until the next UTC day`;
          result.errors.push(msg);
          await logMany(env, [{ username: '__autopilot__', log_level: 'warn', message: `autopilot: ${msg}` }]);
          await setRateLimited(env, { dailyLimit: true });
        } else if (sinceLastLaunchMs < minSpacingMs) {
          // A second launch inside the account's per-instance rate limit is what
          // produced `429 ... Rate limit exceeded`. Defer to the next tick.
          result.method = 'instance_spacing';
          const waitS = Math.ceil((minSpacingMs - sinceLastLaunchMs) / 1000);
          const msg = `browser launched ${Math.round(
            sinceLastLaunchMs / 1000
          )}s ago — deferring ${waitS}s to respect the instance rate limit`;
          result.errors.push(msg);
          await logMany(env, [{ username: '__autopilot__', log_level: 'warn', message: `autopilot: ${msg}` }]);
        } else {
          await markBrowserLaunched(env);
          launchedAt = Date.now();
          try {
            browser = await puppeteer.launch(env.BROWSER);
            await clearRateLimited(env);
          } catch (e) {
            const msg = `browser launch failed: ${e.message}`;
            result.errors.push(msg);
            await logMany(env, [{ username: '__autopilot__', log_level: 'error', message: msg }]);
            await backoffFromLaunchError(env, e);
          }
        }

        if (browser) {
          result.method = result.method === 'direct' ? 'direct+browser' : 'browser';
          page = await browser.newPage();
          await page.setUserAgent(env.CB_USER_AGENT || DEFAULT_UA);

          await page.setRequestInterception(true);
          page.on('request', (req) => {
            const t = req.resourceType();
            if (t === 'document' || t === 'script' || t === 'xhr' || t === 'fetch') {
              req.continue();
            } else {
              req.abort().catch(() => {});
            }
          });

          if (cached?.cookies?.length) {
            try {
              await page.setCookie(...cached.cookies);
            } catch (_) {
              /* ignore bad cookies, page will re-solve */
            }
          }
          await page
            .goto(env.CB_DOMAIN + '/', { waitUntil: 'domcontentloaded', timeout: 20000 })
            .catch(() => {});

          for (const cat of needingBrowser) {
            try {
              const data = await fetchRoomList(page, cat.key, env);
              topsByCategory[cat.key] = topRooms(data, cfg.min_viewers);
            } catch (e) {
              const msg = `browser scan failed for ${cat.label}: ${e.message}`;
              result.errors.push(msg);
              await logMany(env, [{ username: '__autopilot__', log_level: 'error', message: msg }]);
              await alert(env, msg);
            }
          }
        }
      }
    }

    // ---- Phase 3: Build candidates from whatever topsByCategory we have -------
    for (const cat of cfg.categories) {
      const top = topsByCategory[cat.key];
      if (!top || !Array.isArray(top)) continue;

      const deficit = deficits[cat.label] ?? 0;
      const capped = top.slice(0, deficit);
      result[cat.label] = capped.map((r) => ({ username: r.username, num_users: r.num_users }));
      for (const r of capped) {
        candidates.push({ category: cat.label, username: r.username, num_users: r.num_users });
      }
    }

    // ---- Phase 4: Batch dedup + write -----------------------------------------
    const usernames = candidates.map((c) => c.username);
    const [inPoolSet, autoAddedSet, channelsSet] = await Promise.all([
      batchInPool(env, usernames),
      batchAutoAdded(env, usernames),
      batchInChannels(env, usernames),
    ]).catch((e) => {
      result.errors.push(`batch dedup failed: ${e.message}`);
      return [new Set(), new Set(), new Set()];
    });

    const addRows = [];
    const channelRows = [];
    const autoRows = [];
    const logRows = [];
    for (const c of candidates) {
      let reason = null;
      if (inPoolSet.has(c.username)) {
        reason = 'already in pool / claimed by a node';
      } else if (autoAddedSet.has(c.username) && channelsSet.has(c.username)) {
        // Only an operator removal leaves BOTH an auto-add record and a
        // channels-catalogue row.  An auto-added entry that never reached the
        // catalogue was deleted by the controller's orphan prune before any node
        // could record it (the old "fresh add has no channels row" race), so it
        // must stay eligible instead of being blocklisted forever.
        reason = 'previously auto-added then removed (blocklist)';
      }

      if (reason) {
        result.skipped.push({ category: c.category, username: c.username, reason });
        continue;
      }

      if (dryrun) {
        result.added.push({ category: c.category, username: c.username, num_users: c.num_users, dryrun: true });
        continue;
      }

      addRows.push({
        username: c.username,
        site: 'chaturbate',
        status: 'unassigned',
        resolution: 2160,
        framerate: 60,
      });
      // Catalogue row (best-effort, written after the assignment): it keeps the
      // channels table listing every pooled channel for external tools. It is
      // no longer what protects the row from deletion — the controller no longer
      // prunes assignments against the catalogue.
      channelRows.push({
        username: c.username,
        created_at: Math.floor(Date.now() / 1000),
      });
      autoRows.push({
        username: c.username,
        gender: c.category,
        viewers: c.num_users,
        updated_at: new Date().toISOString(),
      });
      logRows.push({
        username: c.username,
        log_level: 'info',
        message: `autopilot: added ${c.category} ${c.username} (${c.num_users} viewers)`,
      });
      result.added.push({ category: c.category, username: c.username, num_users: c.num_users });
    }

    if (!dryrun) {
      try {
        await sbPostMany(
          env,
          'channel_assignments?on_conflict=username,site',
          addRows,
          { Prefer: 'resolution=ignore-duplicates' }
        );
        await sbPostMany(
          env,
          'pool_autopilot?on_conflict=username',
          autoRows,
          { Prefer: 'resolution=merge-duplicates' }
        );
        // Best-effort catalogue registration: keeps the channels table listing
        // every pooled channel. The controller no longer deletes assignments
        // against that table, so a failure here must never lose the add.
        try {
          await sbPostMany(
            env,
            'channels?on_conflict=username',
            channelRows,
            { Prefer: 'resolution=ignore-duplicates' }
          );
        } catch (e) {
          result.errors.push(`catalogue register failed: ${e.message}`);
        }
        await logMany(env, logRows);
      } catch (e) {
        result.errors.push(`batch write failed: ${e.message}`);
      }
    }

    // ---- Phase 5: Cache fresh cookies from browser run (if any) ----------------
    if (page) {
      try {
        const cookies = await page.cookies();
        const cb = cookies.filter((c) => (c.domain || '').includes('chaturbate'));
        if (cb.length) await saveCachedSession(env, cb);
      } catch (e) {
        console.error('cookie extract failed:', e.message);
      }
    }

    const success = result.errors.length === 0;
    if (success && !dryrun) {
      await writeHeartbeat(env, result.method);
      await successAlert(env, result.added);
    }
  } finally {
    if (browser) {
      try {
        await browser.close();
      } catch (_) {
        /* ignore */
      }
      // Charge the session against today's browser budget. Cloudflare meters the
      // browser lifetime, which is (approximately) launch -> close.
      const usedSec = launchedAt ? (Date.now() - launchedAt) / 1000 : 0;
      if (usedSec > 0 && !dryrun) {
        await addBrowserSeconds(env, usedSec);
        result.browser_seconds = Math.round(usedSec * 10) / 10;
      }
    }
    if (!dryrun) await releaseScanLock(env);
  }

  result.duration_ms = Date.now() - startedAt;
  if (!dryrun) await trimChannelLogs(env);
  await recordScan(env, {
    ran_at: new Date().toISOString(),
    trigger: result.trigger,
    method: result.method,
    min_viewers: cfg.min_viewers,
    target_buffer: cfg.target_buffer,
    found: { female: result.female.length, couple: result.couple.length },
    added: result.added,
    skipped: result.skipped,
    errors: result.errors,
    duration_ms: result.duration_ms,
    browser_seconds: result.browser_seconds || 0,
    browser_seconds_used_today: result.browser_seconds_used_today ?? null,
  });

  return result;
}

// ---- Health check (separate cron) ------------------------------------------
async function healthCheck(env) {
  const cfg = await loadConfig(env).catch(() => DEFAULT_CONFIG);
  await checkStale(env, cfg);
}

export default {
  async scheduled(event, env, ctx) {
    if (event.cron === '*/20 * * * *') {
      ctx.waitUntil(healthCheck(env));
      return;
    }
    const trigger = `cron:${event.cron}`;
    ctx.waitUntil(
      runScan(env, false, trigger).then((r) =>
        console.log('autopilot scan complete', JSON.stringify(r))
      )
    );
  },

  async fetch(request, env, _ctx) {
    const url = new URL(request.url);
    const dryrun =
      url.searchParams.get('dryrun') === '1' || url.searchParams.get('dryrun') === 'true';
    const force = url.searchParams.get('force') === '1';
    if (url.pathname === '/health') {
      const hb = await getSetting(env, HEARTBEAT_KEY).catch(() => null);
      const ageMin = hb?.last_success_at
        ? Math.round((Date.now() - new Date(hb.last_success_at).getTime()) / 60000)
        : null;
      const cfg = await loadConfig(env).catch(() => DEFAULT_CONFIG);
      const usedSeconds = await getBrowserSecondsUsed(env);
      const rateLimitedUntil = env.AUTOPILOT
        ? Number(await env.AUTOPILOT.get(RATE_LIMIT_KEY).catch(() => 0)) || 0
        : 0;
      const lastScanAt = await getLastScanAt(env);
      return jsonResponse({
        ok: !!hb,
        last_success_at: hb?.last_success_at || null,
        age_min: ageMin,
        method: hb?.method || null,
        stale_minutes: cfg.stale_minutes,
        scan_interval_minutes: cfg.scan_interval_minutes,
        next_scan_due_in_s: Math.max(
          0,
          Math.round((lastScanAt + cfg.scan_interval_minutes * 60000 - Date.now()) / 1000)
        ),
        browser_budget: {
          used_seconds_today: Math.round(usedSeconds * 10) / 10,
          limit_seconds: cfg.browser_daily_seconds,
          remaining_seconds: Math.max(0, Math.round((cfg.browser_daily_seconds - usedSeconds) * 10) / 10),
        },
        rate_limited_until:
          rateLimitedUntil > Date.now() ? new Date(rateLimitedUntil).toISOString() : null,
      });
    }

    // A plain GET used to run a full scan — so any uptime monitor hitting the
    // root URL was minting a browser session on every probe, which (together
    // with the 15/20-minute cron slots) is what tripped Browser Rendering's
    // `429 ... Rate limit exceeded`. Scans must now be requested explicitly.
    const wantsScan =
      request.method !== 'GET' ||
      url.searchParams.get('scan') === '1' ||
      url.searchParams.get('run') === '1' ||
      force ||
      dryrun;
    if (!wantsScan) {
      const hb = await getSetting(env, HEARTBEAT_KEY).catch(() => null);
      return jsonResponse({
        ok: true,
        message:
          'chaturbate-autopilot is up. Scans run on cron; trigger one manually with ' +
          'POST / (or GET /?scan=1, add &dryrun=1 to preview without writing).',
        last_success_at: hb?.last_success_at || null,
      });
    }

    try {
      const trigger = `http:${request.method} ${url.pathname}${url.search}`;
      const result = await runScan(env, dryrun, trigger, force);
      return jsonResponse(result);
    } catch (e) {
      return jsonResponse({ error: e.message }, 500);
    }
  },
};
