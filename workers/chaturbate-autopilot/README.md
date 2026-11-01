# Chaturbate Autopilot (Cloudflare Worker)

A Cloudflare Worker that keeps the shared DVR channel pool **topped up** with the
highest-traffic Chaturbate rooms. Every 15 minutes it scans the **female** and
**couple** listings and ensures a healthy **buffer** of the most-watched PUBLIC
rooms (above a viewer threshold) is ready in the Supabase `channel_assignments`
pool (`status = "unassigned"`). The Go DVR coordinator claims those rows when
`CHANNEL_POOL_MODE = pooled`.

## Behaviour

- **Never removes** channels.
- **Keeps a buffer ready** — for each category it maintains ~`target_buffer`
  unassigned channels (default 10). As nodes claim/record them, the next scan
  refills the deficit with the next most-watched rooms.
- **Manual removals stick** — channels the worker has auto-added are remembered
  in `pool_autopilot` and are not re-added if they later leave the pool.
- Only **public** rooms with `num_users > min_viewers` qualify.
- Cloudflare's anti-bot challenge is solved with a headless browser (Browser
  Rendering binding). The freshly minted session cookie is cached in Workers KV
  and reused on the next run to avoid re-solving the challenge every time.
- On failure it writes a `channel_logs` error (visible in the admin UI) and, if
  configured, fires `CB_ALERT_WEBHOOK`.

## Configuration (no redeploy needed)

All tunables live in the Supabase `app_settings` row **`autopilot_config`** (the
worker self-seeds defaults on first run):

```json
{
  "min_viewers": 20000,
  "target_buffer": 10,
  "categories": [{ "key": "f", "label": "female" }, { "key": "c", "label": "couple" }],
  "stale_minutes": 25
}
```

Edit that row to change thresholds / categories without redeploying.

## Monitoring

- **`app_settings` → `autopilot_scans`**: rolling last 50 runs (found / added /
  skipped / errors / duration). Use it to spot trends.
- **`app_settings` → `autopilot_heartbeat`**: `last_success_at` of the most
  recent successful scan. The separate `*/20` Cron Trigger checks it and alerts
  (via `CB_ALERT_WEBHOOK`) if a scan hasn't succeeded in `stale_minutes`.
- **`/health`** HTTP endpoint returns `{ ok, last_success_at, age_min }`.
- **`CB_SUCCESS_WEBHOOK`** (optional): fired when channels are auto-added.

## Setup

1. **Config**: set `SUPABASE_URL`, `CB_DOMAIN`, `CB_USER_AGENT` in `wrangler.toml`.
2. **Bindings**: the `[browser]` (Browser Rendering) and `[[kv_namespaces]]`
   (AUTOPILOT) bindings are already declared.
3. **Secrets**:
   ```bash
   wrangler secret put SUPABASE_SERVICE_ROLE_KEY # Worker-only service_role key for writes
   wrangler secret put CB_COOKIES          # chaturbate cookies (optional seed)
   wrangler secret put CB_ALERT_WEBHOOK    # optional failure / stale webhook
   wrangler secret put CB_SUCCESS_WEBHOOK  # optional "added channels" webhook
   ```
   For local dev, copy `.dev.vars.example` -> `.dev.vars` and fill values.
4. **Prereq**: DVR nodes must run with `CHANNEL_POOL_MODE = pooled` so they
   auto-claim the `unassigned` rows.

## Run / Test

```bash
wrangler dev
# open http://localhost:8787/?dryrun=1  -> scans Chaturbate and reports what
#                                            WOULD be added, without writing.
wrangler deploy                          # schedules the 15-min + 20-min Cron Triggers
```

## Free-plan note

The Workers Free plan allows only **50 external subrequests per invocation**.
The browser loads Chaturbate through request interception that blocks images /
media / fonts / stylesheets (keeping only the document, challenge scripts, and
API calls), and all Supabase writes are batched into a handful of requests. On
busy scans this can still approach the limit — upgrading to **Workers Paid**
(10,000 subrequests) gives comfortable headroom.

## Notes

- Room fields use `current_show` (= "public") and `num_users`; the roomlist API
  is `/api/ts/roomlist/room-list/?genders=<f|c>&limit=100`.
- `cf_clearance` cookies are IP-pinned to the Browser Rendering egress; the KV
  cache reuses them across runs but the worker re-solves the challenge if they
  expire.
