# Fleet-wide sweep — 18 nodes via Supabase + trycloudflare tunnels (2026-10-20 ~06:47–06:52 UTC)

Method: `_fleet_sweep_tmp.js` (re-runnable; outputs [`_sweep.json`](_sweep.json), [`_sweep.log`](_sweep.log)) — roster + `tunnels`/`channel_logs`/`upload_journal`/`disk_usage`/`pipeline_states`/`channel_assignments` from Supabase REST (anon key from `.env`), then every `nodes.web_url` probed live (`/api/health`, `/api/orphans`, `/api/logs?after=0`, `/api/uploads`).

## Tunnel inventory (live, from Supabase `nodes.web_url`)

| node | status | load | tunnel URL (quick tunnel — rotates each session) | /api/health |
|---|---|---|---|---|
| node-1 | online | 0 | https://specialties-involved-write-survivors.trycloudflare.com | 200, 1.9s |
| node-2 | online | 107 | https://wellington-researchers-done-conferencing.trycloudflare.com | 200, 2.0s |
| node-3 | online | 148 | https://evident-sponsorship-attitudes-conferencing.trycloudflare.com | 200, 1.5s |
| node-4 | online | 0 | https://situations-cold-clothes-delight.trycloudflare.com | 200, 1.6s |
| node-5 | online | 0 | https://showers-upon-got-learning.trycloudflare.com | 200, 1.6s |
| node-6 | online | 0 | https://alike-cities-wines-spell.trycloudflare.com | 200, 1.6s |
| node-7 | online | 5 | https://presentations-outcome-initiated-collection.trycloudflare.com | 200, 1.8s |
| node-8 | online | 4 | https://attempted-even-initially-tel.trycloudflare.com | 200, 1.9s |
| node-9 | online | 0 | https://warriors-farms-wider-feeling.trycloudflare.com | 200, 1.3s |
| node-10 | online | 148 | https://implementation-loop-same-focusing.trycloudflare.com | 200, 2.0s |
| node-11 | online | 152 | https://proof-joining-suggestion-printer.trycloudflare.com | 200, 1.6s |
| node-12 | online | 155 | https://nevertheless-sought-extraordinary-beats.trycloudflare.com | 200, 1.9s |
| node-13 | online | 0 | https://incidence-organizing-calculator-raises.trycloudflare.com | 200, 2.1s |
| node-14 | online | 0 | https://auburn-sitemap-miles-trading.trycloudflare.com | 200, 1.3s |
| node-15 | online | 6 | https://effects-shark-typically-competent.trycloudflare.com | 200, 1.3s |
| node-16 | online | 148 | https://juice-montgomery-philip-maintenance.trycloudflare.com | 200, 2.0s |
| node-17 | online | 9 | https://saying-journalist-long-kai.trycloudflare.com | 200, 2.0s |
| node-18 | online | 148 | https://continuity-rides-existence-broader.trycloudflare.com | 200, 2.3s |

- **18/18 online**, heartbeats <1 min old, **18/18 tunnels answer `/api/health` in 1.3–2.3 s** — no hang (the Oct-17 node-6 C1 class is absent; D1's liveness+re-mint is in the tree but not yet deployed fleet-wide, so this is a good day, not a proven fix).
- `tunnels` table still holds **one stale DNS-dead row** (`memphis-unto-outsourcing-meanwhile.trycloudflare.com`, instance `default`, 2026-08-10); `tunnel_sessions` still 404s → plan Task D2 is still outstanding.
- Disk fleet-wide healthy (1–31 % used, ≥102 GiB free everywhere).

## The headline: the stuck-recording bugs are UNCHANGED because the fixes were never deployed

`channel/asset_late.go` (A1), `manager/stuck_recording.go` dead-run pass (B1) and the slot-release hook (B2) are **green in this checkout but never left it** — plan Task G1 (sync to the 18 `lawdachuss/node-*` repos) is unchecked, so every session since Oct 15 still builds the pre-fix binary. Live evidence today matches Oct 17 almost exactly:

| metric | 2026-10-17 | 2026-10-20 (today) |
|---|---|---|
| 0-byte orphan files | 53 (8 nodes) | **52 (11 nodes)** |
| `status='recording'` + `is_live=false` slots | 45 (6 nodes) | **34 (8 nodes)** |

Today's zero-byte cohort: node-17 **9**, node-11 **7**, node-15 **6**, node-12 **5**, node-7 **5**, node-4 **4**, node-18 **4**, node-2/3/10/16 **3 each**.
Today's ghost slots: node-11 **7**, node-15 **5**, node-7 **5**, node-18 **5**, node-4 **4**, node-3/10 **3 each**, node-16 **2** — every one with a fresh heartbeat and `is_live=false`, i.e. the liveness-clobber signature (findings §11) plus the unreaped-zero-byte signature (findings §1) combined.

## Fleet-wide error signatures (ring + persisted, last window)

| severity | signature | nodes affected | status |
|---|---|---|---|
| CRITICAL | **Streamtape 403 "Authentication failed" / "rejected our credentials"** on every uploading node | 2,5,6,8,9,12,13,14,15,16,17,18 | **dead shared credential — ops rotate (plan C3), not code** |
| HIGH | **ImgChest 403 "You've used 42.4 GB of your 10 GB storage"** — image account quota hard-exceeded | 3,4,6,8,14,17 | NEW — every thumb/sprite/preview on ImgChest fails; needs the VidMoly-style fleet backoff (plan H3) |
| HIGH | **ImgBB 400 "Rate limit reached."** — key ring rotates but the shared bucket is exhausted | 1,3,5,7,10,11,12,13,15 | per-key cooldown still missing (plan C1/H4) |
| HIGH | **`log retention … HTTP 414: URI too long`** on both `channel_logs` and `disk_usage` deletes | 1,4,8,12,15 | NEW — retention is a silent no-op, telemetry tables bloat forever (plan H2) |
| MEDIUM | **`Supabase schema cache stale — column missing (attempt 2–3/3)`** retry loops | 2,3,10 | plan D3 (name the failing write) still outstanding |
| MEDIUM | `forbidden media endpoint (session expired or stream stopped)` | 4,15,16 | session/cookie class (findings #8) |
| MEDIUM | `audio seg download failed: All attempts fail` | 7,17 | parked (needs a targeted log dump) |
| LOW | ~~`[twins_terror] channel is offline…` persisted at **ERROR level with INFO text**~~ | 18 | **CLOSED 2026-10-20: false positive** — the sweep's `/ERROR/i` ring filter matched the channel NAME `twins_terror` ("t**error**"); line is genuinely INFO, DB verified clean (plan H5 carries the full diagnosis; detector fixed with word-boundary match) |
| LOW | `4/7 hosts succeeded … persisting partial links` warnings (Streamtape is the failing 3rd) | many | resolves when Streamtape creds rotate |

`upload_journal` 24 h: **34 failed rows** — worst node-4 **12/45**, node-12 5/15, node-13 4/12, node-16 3/18, node-9 3/20. No active upload backlog anywhere (`/api/uploads` queues drained except minor actives).

## Per-node problem matrix (today)

| node | problems |
|---|---|
| node-1 | ImgBB 400 flood; log-retention 414 |
| node-2 | Streamtape 403; 3 zero-byte + 3 ghost slots; schema-cache retries |
| node-3 | ImgChest 403 quota; ImgBB 400; 3 zero-byte + 3 ghost slots; schema-cache retries |
| node-4 | 4 zero-byte + 4 ghost slots; ImgChest 403; log-retention 414; upload_journal 12/45 failed; media-endpoint forbidden |
| node-5 | Streamtape 403; ImgBB 400; "no URL 2h after abandoned" thumbnail/preview warns |
| node-6 | Streamtape 403; ImgChest 403 (314 errors in ring) |
| node-7 | 5 zero-byte + 5 ghost slots; ImgBB 400; audio-seg failures |
| node-8 | ImgChest 403; Streamtape 403 (13 persisted errors); log-retention 414 |
| node-9 | Streamtape 403 (13 persisted errors) |
| node-10 | 3 zero-byte + 3 ghost slots; ImgBB 400; schema-cache retries |
| node-11 | 7 zero-byte + 7 ghost slots (worst); ImgBB 400 |
| node-12 | 5 zero-byte; Streamtape 403; ImgBB 400; log-retention 414 |
| node-13 | ImgBB 400; Streamtape 403 (4 persisted errors) |
| node-14 | Streamtape 403 (10 persisted errors); ImgChest 403; log-retention class |
| node-15 | 6 zero-byte + 5 ghost slots; ImgBB 400; log-retention 414; media-endpoint forbidden; 4 idle>30m pipeline rows |
| node-16 | 3 zero-byte + 2 ghost slots; Streamtape 403; media-endpoint forbidden; Supabase request retry blip |
| node-17 | 9 zero-byte (worst); Streamtape 403; ImgChest 403; audio-seg failure |
| node-18 | 4 zero-byte + 5 ghost slots; Streamtape 403; ~~INFO-as-ERROR mislabel~~ (false positive, closed) |

## Fix plan

The full, ordered remediation plan — including the new H-series tasks from this sweep (deploy G1, retention 414, ImgChest quota, ImgBB cooldown, log-level fix) — lives in [docs/superpowers/plans/2026-10-17-fleet-remediation.md](docs/superpowers/plans/2026-10-17-fleet-remediation.md). **The single most important line: run Task G1. Every code fix for the worst live problem is already written, tested and waiting in this checkout; the fleet cannot run it until the sync lands.**
