#!/usr/bin/env node
// Fleet-wide diagnostics sweep for the 18-node DVR fleet — the maintained
// tool (plan E3, docs/superpowers/plans/2026-10-17-fleet-remediation.md),
// merged from the two operator temp scripts `_fleet_sweep_tmp.js` +
// `_actions_sweep_tmp.js`. Loads .env locally; never prints secrets.
//
// Usage:
//   node scripts/fleet-sweep.mjs                       # all sections
//   node scripts/fleet-sweep.mjs --sections=uploads    # per-node upload queue only
//   node scripts/fleet-sweep.mjs --sections=supabase,uploads
//   node scripts/fleet-sweep.mjs --sections=actions --since=2026-10-14T00:00:00Z
//
// Sections:
//   supabase  roster + tunnel probes (health/orphans/logs) + Supabase
//             cross-checks (channel_logs, disk_usage, pipeline_states,
//             channel_assignments, upload_journal) + problem summary.
//   actions   GitHub Actions sweep across the node repos via the `gh` CLI
//             (runs inventory, failing-run log tails, green-but-idle check,
//             live state + retry-loop detection).
//   uploads   per-node GET /api/uploads queue table
//             ({active, pending, history, processing} counts per node).
//
// Outputs: fleet-sweep.json + fleet-sweep.log in the cwd (--out= prefix).
// Exit codes: 0 = ran clean (nodes reachable), 1 = fatal (no .env / roster
// fetch failed), 2 = at least one node unreachable via its tunnel.

import fs from 'node:fs';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';

// ── CLI ──────────────────────────────────────────────────────────────────────
const argv = process.argv.slice(2);
const flag = (name, dflt) => {
  const hit = argv.find((a) => a.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : dflt;
};
const SECTIONS = (flag('sections', 'supabase,actions,uploads') || '')
  .split(',').map((s) => s.trim()).filter(Boolean);
const ORG = flag('org', process.env.FLEET_ORG || 'lawdachuss');
const NODE_COUNT = parseInt(flag('nodes', process.env.FLEET_NODE_COUNT || '18'), 10);
const SINCE = flag('since', new Date(Date.now() - 48 * 3600e3).toISOString());
const OUT_PREFIX = flag('out', 'fleet-sweep');
const exec = promisify(execFile);

for (const s of SECTIONS) {
  if (!['supabase', 'actions', 'uploads'].includes(s)) {
    console.error(`[FATAL] unknown section "${s}" (known: supabase, actions, uploads)`);
    process.exit(1);
  }
}

// ── .env load ────────────────────────────────────────────────────────────────
const env = {};
if (fs.existsSync('.env')) {
  for (const raw of fs.readFileSync('.env', 'utf8').split(/\r?\n/)) {
    const m = raw.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$/);
    if (!m || raw.trim().startsWith('#')) continue;
    env[m[1]] = m[2].replace(/^"(.*)"$/, '$1').replace(/^'(.*)'$/, '$1').trim();
  }
}
const needSB = SECTIONS.some((s) => s !== 'actions');
if (needSB && (!env.SUPABASE_URL || !env.SUPABASE_API_KEY)) {
  console.error('[FATAL] .env missing SUPABASE_URL / SUPABASE_API_KEY');
  process.exit(1);
}
const SB = (env.SUPABASE_URL || '').replace(/\/+$/, '');
const H = {
  apikey: env.SUPABASE_API_KEY,
  Authorization: `Bearer ${env.SUPABASE_API_KEY}`,
  'Content-Type': 'application/json',
};

// ── shared helpers ───────────────────────────────────────────────────────────
const now = () => Date.now();
const ago = (ts) => {
  if (!ts) return 'never';
  const ms = now() - Date.parse(ts);
  if (isNaN(ms)) return 'unknown';
  return `${Math.floor(ms / 60000)}min`;
};
const logLines = [];
const out = (s = '') => { console.log(s); logLines.push(s); };

const problems = {}; // node -> [issues]
const add = (nodeId, issue) => { (problems[nodeId] ||= []).push(issue); };

async function sb(table, query) {
  const url = `${SB}/rest/v1/${table}${query ? '?' + query : ''}`;
  const r = await fetch(url, { headers: H });
  if (!r.ok) throw new Error(`${table}: HTTP ${r.status} ${await r.text().catch(() => '')}`);
  return r.json();
}

async function probeJSON(base, path, timeoutMs = 20000) {
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), timeoutMs);
  const t0 = now();
  try {
    const r = await fetch(base + path, { signal: ctrl.signal, headers: { 'User-Agent': 'fleet-sweep/1' } });
    const txt = await r.text();
    clearTimeout(to);
    if (!r.ok) return { ok: false, status: r.status, ms: now() - t0, body: txt };
    try { return { ok: true, status: r.status, ms: now() - t0, json: JSON.parse(txt) }; }
    catch { return { ok: true, status: r.status, ms: now() - t0, json: null, body: txt }; }
  } catch (e) {
    clearTimeout(to);
    return { ok: false, status: 0, ms: now() - t0, err: (e.cause?.message || e.message).slice(0, 120) };
  }
}

// ── roster (needed by supabase + uploads sections) ───────────────────────────
let nodes = [];
let nodeTunnels = new Map();
async function loadRoster() {
  nodes = await sb('nodes', 'select=*&order=node_id.asc');
  nodeTunnels = new Map();
  for (const n of nodes) if (n.web_url) nodeTunnels.set(n.node_id, n.web_url);
  // Legacy fallback: the `tunnels` table has no live writer today, but keep it
  // as a secondary source so a keep-alive outage doesn't blind the sweep.
  // Strictly roster-scoped: the table holds stale rows whose id is not a node
  // (live: `default` / memphis-…, 2026-08-10) — accepting those would invent a
  // phantom 19th node and fail the run.
  try {
    const rosterIds = new Set(nodes.map((n) => n.node_id));
    const tunnels = await sb('tunnels', 'select=*&order=created_at.desc&limit=40');
    for (const t of tunnels) {
      const id = t.node_id || t.instance_id;
      if (id && rosterIds.has(id) && t.url && !nodeTunnels.has(id)) nodeTunnels.set(id, t.url);
    }
  } catch { /* table missing/empty is fine — nodes.web_url is authoritative */ }
  out(`ROSTER: ${nodes.length} nodes, ${nodeTunnels.size} with tunnel URLs`);
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION: supabase — tunnel probes + DB cross-checks
// ═════════════════════════════════════════════════════════════════════════════
async function sectionSupabase() {
  out('\n=== ROSTER ===');
  let online = 0;
  const stale = [];
  for (const n of nodes) {
    const hbOld = n.last_heartbeat && (now() - Date.parse(n.last_heartbeat)) > 5 * 60000;
    if (n.status === 'online') online++;
    if (hbOld || !n.last_heartbeat) stale.push(n.node_id);
    out(`  ${n.node_id}\t${n.status}\tlb=${n.current_load ?? '?'}\thb=${ago(n.last_heartbeat)}${hbOld ? '  <-- STALE HB' : ''}\ttunnel=${n.web_url ? 'yes' : 'NO'}`);
  }
  out(`  online=${online}/${nodes.length}  staleHB=${stale.join(',') || '-'}`);

  out('\n=== TUNNEL PROBES (/api/health, /api/orphans, /api/logs) ===');
  const ids = [...nodeTunnels.keys()].sort();
  const reachable = new Set();
  const BATCH = 6;
  for (let i = 0; i < ids.length; i += BATCH) {
    await Promise.all(ids.slice(i, i + BATCH).map(async (id) => {
      const base = nodeTunnels.get(id).replace(/\/+$/, '');
      const node = nodes.find((n) => n.node_id === id) || { node_id: id, status: '?', last_heartbeat: null };
      const root = await probeJSON(base, '/api/health', 10000);
      const issues = [];
      if (!root.ok) issues.push(`unreachable ${root.err || 'HTTP ' + root.status}`);
      else if (root.ms > 5000) issues.push(`very slow ${root.ms}ms`);
      if (!node.last_heartbeat || (now() - Date.parse(node.last_heartbeat)) > 5 * 60000) issues.push(`stale heartbeat ${ago(node.last_heartbeat)}`);
      if (root.ok && node.status !== 'online') issues.push(`reachable but status=${node.status}`);
      if (!root.ok && node.status === 'online') issues.push('status=online but tunnel DEAD — stale web_url');
      if (root.ok) reachable.add(id);
      if (issues.length) add(id, issues.join(' | '));
      out(`  ${id.padEnd(10)} ${root.ok ? 'OK' : 'FAIL'} ${String(root.status).padStart(3)} ${String(root.ms).padStart(5)}ms${issues.length ? '  ⚠ ' + issues.join(' / ') : ''}`);
      if (!root.ok) return;

      const o = await probeJSON(base, '/api/orphans');
      if (o.ok && o.json) {
        const list = Array.isArray(o.json) ? o.json : (o.json.orphans || o.json.files || []);
        const zero = list.filter((f) => (f.size ?? f.Size ?? 0) === 0);
        out(`  ${id} orphans=${list.length} zeroByte=${zero.length}`);
        if (zero.length) {
          add(id, `${zero.length} zero-byte orphan files (stuck recordings)`);
          for (const z of zero.slice(0, 3)) out(`      0B ${z.filename || z.path || '?'}`);
        }
      } else if (o.ok) {
        out(`  ${id} /api/orphans non-JSON → ${(o.body || '').slice(0, 100)}`);
      } else {
        out(`  ${id} /api/orphans → HTTP ${o.status} ${o.err || ''}`);
      }

      const g = await probeJSON(base, '/api/logs?after=0');
      if (g.ok && g.json) {
        const lines = (g.json.lines || []).map((l) => (typeof l === 'string' ? l : `${l.line || ''} ${l.time || ''}`));
        // Word-boundary match on the LEVEL token only. A bare /ERROR/i
        // substring test false-positived on channel names containing "error"
        // (live: `twins_terror` → every INFO line counted as an error, plan
        // Task H5 — a detector artifact, not an app bug). Carry this fix.
        const err = lines.filter((l) => /\b(ERROR|FATAL|PANIC)\b/i.test(l));
        const warn = lines.filter((l) => /\bWARN\b/i.test(l));
        out(`  ${id} logs buffer: total=${g.json.total ?? lines.length} err=${err.length} warn=${warn.length}`);
        for (const l of err.slice(-3)) out(`      E ${l.slice(0, 170)}`);
        for (const l of warn.slice(-2)) out(`      W ${l.slice(0, 170)}`);
      } else if (g.ok) {
        out(`  ${id} /api/logs non-JSON (auth wall?)`);
      }
    }));
  }
  out(`  probes: ${ids.length} URLs tried, ${reachable.size} reachable`);
  for (const n of nodes) if (!n.web_url) add(n.node_id, 'no tunnel URL published');

  // channel_logs error counts, last 24h, per node
  out('\n=== channel_logs ERROR counts, last 24h, per node ===');
  const since24 = new Date(Date.now() - 24 * 3600e3).toISOString();
  try {
    const clo = await sb('channel_logs', `select=instance_id,log_level,message,created_at&created_at=gte.${since24}&order=created_at.desc&limit=5000`);
    const errByNode = {};
    for (const l of clo) {
      if (/error/i.test(l.log_level || '')) {
        const n = l.instance_id || '?';
        const sig = (l.message || '').replace(/[^\w .[\]]/g, ' ').replace(/\s+/g, ' ').slice(0, 60);
        (errByNode[n] ||= {})[sig] = (errByNode[n][sig] || 0) + 1;
      }
    }
    for (const n of nodes.map((x) => x.node_id)) {
      const m = errByNode[n] || {};
      const total = Object.values(m).reduce((a, b) => a + b, 0);
      out(`  ${n.padEnd(12)} errors24h=${total}`);
      if (total) add(n, `${total} persisted errors in 24h`);
      for (const [sig, c] of Object.entries(m).sort((a, b) => b[1] - a[1]).slice(0, 4)) {
        out(`      ${String(c).padStart(4)}× ${sig}`);
      }
    }
  } catch (e) { out(`  channel_logs: ${e.message.slice(0, 120)}`); }

  // disk usage
  out('\n=== disk_usage (latest per node) ===');
  try {
    const du = await sb('disk_usage', 'select=*&order=recorded_at.desc&limit=60');
    const seen = new Set();
    for (const d of du) {
      if (seen.has(d.node_id)) continue;
      seen.add(d.node_id);
      const hot = d.percent_used && d.percent_used > 85 ? '  ⚠ HIGH' : '';
      out(`  ${String(d.node_id ?? 'null').padEnd(12)} pct=${d.percent_used ?? '?'}% used=${Math.round((d.used_bytes || 0) / 2 ** 30)}GiB free=${Math.round((d.free_bytes || 50) / 2 ** 30)}GiB @${(d.recorded_at || '').slice(0, 19)} (${ago(d.recorded_at)})${hot}`);
      if (hot) add(String(d.node_id), `disk ${d.percent_used}% full`);
    }
  } catch (e) { out(`  disk_usage: ${e.message.slice(0, 120)}`); }

  // pipeline states
  out('\n=== pipeline_states stuck/failed rows, per node ===');
  try {
    const ps = await sb('pipeline_states', 'select=*,updated_at&order=updated_at.desc&limit=2000');
    const byNode = {};
    for (const p of ps) (byNode[p.node_id ?? 'null'] ||= []).push(p);
    for (const [n, list] of Object.entries(byNode)) {
      const failed = list.filter((p) => p.failed);
      const stuck = list.filter((p) => !p.failed && p.updated_at && (now() - Date.parse(p.updated_at)) > 30 * 60000);
      out(`  ${String(n).padEnd(12)} active=${list.length} failed=${failed.length} idle>30min=${stuck.length}`);
      for (const f of failed.slice(0, 3)) out(`      FAIL ${(f.filename || f.file_path || '?').slice(0, 70)} stage=${f.current_stage} :: ${(f.last_error || '').slice(0, 100)}`);
      if (failed.length) add(String(n), `${failed.length} failed pipeline rows`);
    }
  } catch (e) { out(`  pipeline_states: ${e.message.slice(0, 120)}`); }

  // stuck-recording signature
  out('\n=== channel_assignments: status=recording while is_live=false ===');
  try {
    const ta = await sb('channel_assignments', 'select=*&order=updated_at.desc&limit=2000');
    const rec = {};
    for (const a of ta) if ((a.status || '') === 'recording') (rec[a.assigned_node ?? '?'] ||= []).push(a);
    for (const [n, list] of Object.entries(rec)) {
      const dead = list.filter((a) => !a.is_live);
      const staleLive = list.filter((a) => a.is_live && a.live_checked_at && (now() - Date.parse(a.live_checked_at)) > 10 * 60000);
      out(`  ${String(n).padEnd(12)} recording=${list.length} is_live=false=${dead.length} staleLiveCheck=${staleLive.length}`);
      for (const d of dead.slice(0, 4)) out(`      DEAD-LIVE ${d.username} hb=${ago(d.last_heartbeat)}`);
      if (dead.length) add(String(n), `${dead.length} channels status=recording while is_live=false (stuck recordings)`);
    }
  } catch (e) { out(`  channel_assignments: ${e.message.slice(0, 120)}`); }

  // upload journal
  out('\n=== upload_journal failed rows, last 24h, per node ===');
  try {
    const uj = await sb('upload_journal', `select=*,created_at&created_at=gte.${since24}&order=created_at.desc&limit=3000`);
    const byNode = {};
    for (const u of uj) {
      const n = u.node_id ?? u.instance_id ?? '?';
      (byNode[n] ||= {}).cnt = (byNode[n].cnt || 0) + 1;
      if (/(fail|error)/i.test(u.status || u.state || '')) (byNode[n].fails ||= []).push(u);
    }
    for (const [n, v] of Object.entries(byNode)) {
      out(`  ${String(n).padEnd(12)} rows24h=${v.cnt} failedRows=${(v.fails || []).length}`);
    }
  } catch (e) { out(`  upload_journal: ${e.message.slice(0, 120)}`); }
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION: uploads — per-node queue table from GET /api/uploads
// Shape (entity.UploadsResponse): { active: [], pending: [], history: [],
// processing: bool } — pending/history entries carry .failed/.error.
// ═════════════════════════════════════════════════════════════════════════════
async function sectionUploads() {
  out('\n=== UPLOAD QUEUE per node (GET /api/uploads) ===');
  const ids = [...nodeTunnels.keys()].sort();
  const rows = [];
  const BATCH = 6;
  for (let i = 0; i < ids.length; i += BATCH) {
    await Promise.all(ids.slice(i, i + BATCH).map(async (id) => {
      const base = nodeTunnels.get(id).replace(/\/+$/, '');
      const q = await probeJSON(base, '/api/uploads');
      if (!q.ok || !q.json) {
        rows.push({ id, reachable: false, detail: q.err || `HTTP ${q.status}` });
        add(id, `uploads endpoint unreachable (${q.err || 'HTTP ' + q.status})`);
        return;
      }
      const j = q.json;
      const len = (v) => (Array.isArray(v) ? v.length : (typeof v === 'number' ? v : 0));
      const failedPending = (Array.isArray(j.pending) ? j.pending : []).filter((p) => p.failed).length;
      const failedHistory = (Array.isArray(j.history) ? j.history : []).filter((p) => p.failed).length;
      rows.push({
        id, reachable: true,
        active: len(j.active), pending: len(j.pending), history: len(j.history),
        processing: !!j.processing, failedPending, failedHistory, ms: q.ms,
      });
      if (failedPending) add(id, `${failedPending} failed pending pipeline rows`);
    }));
  }
  rows.sort((a, b) => a.id.localeCompare(b.id));
  out('  node       active  pending  history  processing  failedPend  failedHist');
  let okRows = 0;
  for (const r of rows) {
    if (!r.reachable) { out(`  ${r.id.padEnd(10)} UNREACHABLE ${r.detail}`); continue; }
    okRows++;
    out(`  ${r.id.padEnd(10)} ${String(r.active).padStart(6)} ${String(r.pending).padStart(8)} ${String(r.history).padStart(8)} ${String(r.processing).padStart(11)} ${String(r.failedPending).padStart(11)} ${String(r.failedHistory).padStart(11)}`);
  }
  out(`  ${okRows}/${rows.length} nodes answered`);
  return { okRows, total: rows.length, unreachable: rows.length - okRows };
}

// ═════════════════════════════════════════════════════════════════════════════
// SECTION: actions — GitHub Actions sweep across the node repos (needs `gh`)
// ═════════════════════════════════════════════════════════════════════════════
async function gh(args, opts = {}) {
  const { stdout } = await exec('gh', args, { maxBuffer: 512 * 1024 * 1024, timeout: opts.timeout ?? 120000 });
  return stdout;
}
const ghJson = async (args) => JSON.parse(await gh(args));
const REPOS = Array.from({ length: NODE_COUNT }, (_, i) => `node-${i + 1}`);

async function sectionActions() {
  out(`\nACTIONS SWEEP ${ORG} repos=${REPOS.length} since=${SINCE}`);
  out('='.repeat(78));

  // 1. runs inventory
  const store = {};
  for (const repo of REPOS) {
    try {
      const runs = await ghJson(['api', `repos/${ORG}/${repo}/actions/runs?per_page=20`, '--jq',
        '[.workflow_runs[] | {id,name,event,status,conclusion,created_at,attempt}]']);
      const recent = runs.filter((r) => r.created_at >= SINCE);
      store[repo] = { runs: recent };
      const byC = {};
      for (const r of recent) byC[r.conclusion || r.status] = (byC[r.conclusion || r.status] || 0) + 1;
      const names = [...new Set(recent.map((r) => r.name))];
      out(`${repo.padEnd(9)} runs=${recent.length} [${names.join('|') || '-'}] ` +
        Object.entries(byC).map(([k, v]) => `${k}=${v}`).join(' '));
    } catch (e) {
      store[repo] = { error: e.message.slice(0, 150) };
      out(`${repo.padEnd(9)} ERROR ${e.message.slice(0, 120)}`);
    }
  }

  // 2. failing runs: failed jobs + first error line
  out('\n=== FAILING RUNS: failed jobs + first error line ===');
  for (const repo of REPOS) {
    const st = store[repo];
    if (!st.runs) continue;
    for (const r of st.runs.filter((x) => x.conclusion === 'failure')) {
      try {
        const jobs = await ghJson(['api', `repos/${ORG}/${repo}/actions/runs/${r.id}/jobs?per_page=30`, '--jq',
          '[.jobs[] | {id,name,conclusion,started_at,completed_at,steps:(.steps|map({name,conclusion}))}]']);
        for (const j of jobs.filter((x) => x.conclusion === 'failure')) {
          const failedSteps = (j.steps || []).filter((s) => s.conclusion === 'failure').map((s) => s.name).join(', ');
          out(`${repo} run ${r.id} (${r.created_at.slice(0, 16)}, ${r.event}) job "${j.name}" FAILED steps: ${failedSteps || '(none reported)'}`);
          try {
            const tail = await gh(['run', 'view', String(r.id), '--repo', `${ORG}/${repo}`, '--log-failed']);
            const lines = tail.split(/\r?\n/).filter(Boolean);
            const errLine = lines.find((l) => /error|fail|exception|cannot|unable|not found|denied/i.test(l)) || lines[lines.length - 1] || '';
            out(`    first-err: ${errLine.slice(0, 220)}`);
            for (const l of lines.slice(-5)) out(`      | ${l.slice(0, 200)}`);
          } catch (e) { out(`    log fetch failed: ${e.message.slice(0, 100)}`); }
        }
        const other = jobs.filter((j) => j.conclusion !== 'failure');
        if (other.length) out(`  (other jobs in run: ${other.map((j) => `${j.name}=${j.conclusion}`).join(', ')})`);
      } catch (e) { out(`${repo} run ${r.id} jobs fetch failed: ${e.message.slice(0, 100)}`); }
    }
  }

  // 3. green-but-doing-nothing check
  out('\n=== LAST SUCCESS per repo: verify the session actually recorded ===');
  for (const repo of REPOS) {
    const st = store[repo];
    if (!st.runs) continue;
    const okRun = st.runs.filter((r) => r.conclusion === 'success' && r.name === 'Secure RDP').sort((a, b) => b.created_at.localeCompare(a.created_at))[0];
    if (!okRun) { out(`${repo} no successful Secure RDP run in window`); continue; }
    try {
      const jobs = await ghJson(['api', `repos/${ORG}/${repo}/actions/runs/${okRun.id}/jobs?per_page=10`, '--jq',
        '[.jobs[] | {id,name,conclusion}]']);
      const ja = jobs.find((j) => /keep|alive/i.test(j.name)) || jobs[0];
      const logs = await gh(['run', 'view', String(okRun.id), '--repo', `${ORG}/${repo}`, '--job', String(ja.id), '--log'], { timeout: 240000 });
      const lines = logs.split(/\r?\n/);
      const notApiLog = (l) => !/api\/logs|log buffer/i.test(l);
      const markers = {
        tunnelLine: (lines.find((l) => /trycloudflare\.com/.test(l)) || '').trim().slice(0, 140),
        channels: lines.filter((l) => /\brecording\b/i.test(l) && notApiLog(l)).length,
        uploaded: lines.filter((l) => /uploaded|upload complete|persisting partial/i.test(l)).length,
        // word-boundary classifier (plan H5) — never match inside a name
        errors: lines.filter((l) => /\b(ERROR|FATAL|PANIC)\b/.test(l)).length,
        zeroByte: lines.filter((l) => /0-byte|zero-byte|0 B\b/i.test(l)).length,
      };
      out(`${repo} success-run ${okRun.created_at.slice(0, 16)} job=${ja.name}:`);
      out(`    tunnelURL: ${markers.tunnelLine || '(none found!)'}`);
      out(`    recordingLines=${markers.channels} uploadedLines=${markers.uploaded} ERRORLines=${markers.errors} zeroByteLines=${markers.zeroByte} (total ${lines.length} lines)`);
      for (const l of lines.filter((l) => /session|rebalanc|stop.*upload|upload-complete|deadline/i.test(l)).slice(-4)) {
        out(`      | ${l.trim().slice(0, 190)}`);
      }
    } catch (e) { out(`${repo} success-log check failed: ${e.message.slice(0, 120)}`); }
  }

  // 4. live state + retry loops
  out('\n=== LIVE state + retry loops ===');
  for (const repo of REPOS) {
    try {
      const runs = await ghJson(['api', `repos/${ORG}/${repo}/actions/runs?per_page=20`, '--jq',
        '[.workflow_runs[] | {id,event,status,conclusion,name,created_at,run_attempt}]']);
      for (const r of runs.filter((x) => x.status === 'in_progress')) {
        out(`  ${repo} IN_PROGRESS run ${r.id} (${r.name}) since ${r.created_at.slice(0, 16)} attempt=${r.run_attempt}`);
      }
      for (const r of runs.filter((x) => x.status === 'queued')) {
        out(`  ${repo} QUEUED run ${r.id} since ${r.created_at.slice(0, 16)}`);
      }
      const cnt = {};
      for (const r of runs) cnt[`${r.conclusion || r.status}`] = (cnt[`${r.conclusion || r.status}`] || 0) + 1;
      if ((cnt.cancelled || 0) >= 5) out(`  ${repo} cancelled>=5 in last 20 runs (${cnt.cancelled}) — check same-event loop`);
    } catch (e) { out(`  ${repo} live-state fetch failed: ${e.message.slice(0, 100)}`); }
  }
}

// ═════════════════════════════════════════════════════════════════════════════
// main
// ═════════════════════════════════════════════════════════════════════════════
out(`FLEET SWEEP  ${new Date().toISOString()}  sections=${SECTIONS.join(',')}`);
out('='.repeat(78));

let uploadsResult = null;
let fatal = false;
try {
  if (SECTIONS.some((s) => s !== 'actions')) await loadRoster();
  if (SECTIONS.includes('supabase')) await sectionSupabase();
  if (SECTIONS.includes('uploads')) uploadsResult = await sectionUploads();
  if (SECTIONS.includes('actions')) await sectionActions();
} catch (e) {
  out(`\n[FATAL] ${e.message}`);
  fatal = true;
}

// problem summary (any section that ran)
if (SECTIONS.includes('supabase') || SECTIONS.includes('uploads')) {
  out('\n=== PER-NODE PROBLEM SUMMARY ===');
  for (const n of nodes) {
    const p = problems[n.node_id];
    if (p && p.length) out(`  ${n.node_id.padEnd(12)} ${p.length} issue(s):\n${p.map((x) => `      - ${x}`).join('\n')}`);
    else out(`  ${n.node_id.padEnd(12)} no issues detected`);
  }
}

fs.writeFileSync(`${OUT_PREFIX}.json`, JSON.stringify({
  at: new Date().toISOString(), sections: SECTIONS, nodes,
  uploads: uploadsResult, problems,
}, null, 2));
fs.writeFileSync(`${OUT_PREFIX}.log`, logLines.join('\n'));
out(`\nWrote ${OUT_PREFIX}.json + ${OUT_PREFIX}.log`);

if (fatal) process.exit(1);
const unreachable = uploadsResult ? uploadsResult.unreachable
  : nodes.filter((n) => n.web_url && !problems[n.node_id]?.some((i) => i.startsWith('unreachable'))).length !== nodes.filter((n) => n.web_url).length;
process.exit(unreachable ? 2 : 0);
