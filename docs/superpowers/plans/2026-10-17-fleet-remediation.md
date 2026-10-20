# Fleet Remediation Plan — 18-node DVR fleet (Go + Supabase + GitHub Actions)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> ## ⚠ STATUS VERIFIED 2026-10-20 (live sweep: [FLEET_SWEEP_2026-10-20.md](../../../FLEET_SWEEP_2026-10-20.md))
>
> | task | code state (this checkout, verified) | deployed to fleet? |
> |---|---|---|
> | A1 late-asset regression | ✅ landed, test green | ✅ **synced 2026-10-20** (G1) |
> | B1 dead-run zero-byte reap | ✅ landed (`manager/stuck_recording.go`) | ✅ **synced 2026-10-20** (G1) |
> | B2 slot release on kill | ✅ landed (`ReleaseHook`, `manager/zero_byte_release_test.go`) | ✅ **synced 2026-10-20** (G1) |
> | C1/H4 ImgBB per-key cooldown | ✅ **DONE 2026-10-20** — `classifyImgBBError` + `imgbbKeyCooldown` map + `nextUsable()` ring skip; 5 tests green (`uploader/imgbb_test.go`) | ✅ **synced 2026-10-20** (G1) |
> | C2 AnonMP4 522 daily-disable | ✅ **DONE 2026-10-20** — `isAnonMP4OriginDown` + day-keyed strike counter, 3rd strike disables till UTC midnight; 3 tests green (`uploader/anonmp4_daily522_test.go`) | ✅ **synced 2026-10-20** (G1) |
> | D1 tunnel hang re-mint | ✅ landed (`Test-TunnelLiveness` curl rc 6/7/28 + cloudflared restart + `Clear-NodeWebUrl`, wired at keep-alive.ps1:766) | ✅ **synced 2026-10-20** (G1) |
> | D2 stale tunnels row + scripts | ✅ **DONE 2026-10-20** — scripts re-pointed to `nodes.web_url` (all `tunnel_sessions` refs gone, BOM fix, live-URL verified) + migration written AND **applied to the live DB** (via PostgREST service-role `DELETE` with the same `created_at < now()-48h` predicate — no psql/supabase CLI on this machine; removed exactly the 1 known stale `instance=default`/2026-08-10 row, 0 remaining, memphis gone) | ✅ **synced 2026-10-20** (G1) |
> | D3 schema-cache classifier | ✅ **DONE 2026-10-20** — `database.IsSchemaCacheStale` + `RestTable` single-owner helper, both call sites deduped, errors/logs now name `[CODE] METHOD table`; 3 tests green (`database/schema_cache_classify_test.go`); live probe: **no PGRST204 gap** (nothing to migrate), PGRST002 drops ≥50 writes/2wk now visible | ✅ **synced 2026-10-20** (G1) |
> | E1 pre-session test gate | ✅ **DONE 2026-10-20** — `go vet ./... && go test ./channel -count=1` inside the build job of `scripts/parallel-setup.ps1`; failure → `gate-failure.log` + exe removed + `throw` (job Failed → main exits 1); script parses clean; local run green (vet 0, channel ok 17s) | ✅ **synced 2026-10-20** (G1) |
> | E2 DVR-log artifact | ✅ **DONE 2026-10-20** — `Upload DVR log` step in `.github/workflows/secure-rdp.yml` (after `Cleanup on Cancel`, so logs are flushed; `dvr-output.log` + `dvr-err.log`, `if-no-files-found: ignore`); YAML validated. Repo-secret `-` placeholder removal is still ops (C5) | ✅ **synced 2026-10-20** (G1) |
> | E3 fleet-sweep.mjs tool | ✅ **DONE 2026-10-20** — `scripts/fleet-sweep.mjs` merged from both tmp sweeps, `--sections=supabase,actions,uploads` + real `/api/uploads` parser, **H5 word-boundary classifier carried**; live run `18/18 nodes answered, EXIT=0`; README section added | ✅ **synced 2026-10-20** (G1) |
> | H2 retention 414 | ✅ **DONE 2026-10-20** — delete batch ≤100 ids + page-remainder drain; 3 tests green (`database/retention_batch_test.go`) | ✅ **synced 2026-10-20** (G1) |
> | H3 ImgChest quota backoff | ✅ **DONE 2026-10-20** — `isImgChestStorageFull` + `tryImgChest` fleet-disable seam; 4 tests green (`uploader/imgchest_quota_test.go`) | ✅ **synced 2026-10-20** (G1) |
> | H5 INFO-as-ERROR mislabel | ✅ **CLOSED 2026-10-20 — false positive** (detector artifact, not an app bug; see Task H5) | n/a |
> | **G1 sync to the 18 repos** | **✅ DONE 2026-10-20** — 9 commits per repo (one per workstream), pushed to all 18 as `lawdachuss`, fleet-verified via the GitHub API (every repo's top 9 commits = the G1 list; deployed `database/supabase.go` byte-compares identical to this checkout) | **✅ deployed — the next session build on every node carries every fix** |
>
> **Priority order is now: G1 (deploy what exists: A1+B1+B2+C1/H4+C2+D1+D3+H2+H3) → C3 ops (Streamtape key) → D2 migration apply → E1/E2/E3 → F1.** Workstream H below carries the Oct-20 findings.

**Goal:** Make the 18-node recording fleet stop losing recordings to stuck zero-byte channels, dead upload-host keys, and wedged tunnels — and make every one of those failures loudly observable instead of silently green.

**Architecture:** Fixes land in this single source checkout (the bytes every node session builds from), are verified locally first (Go toolchain at `%LOCALAPPDATA%\go-toolchain\go`), then synced to the 18 `lawdachuss/node-*` repos, where each 6-hourly session rebuilds the binary — so the sync itself is the deploy. Backward compatibility runs through idempotent Supabase migrations and the existing `upload_host_backoffs` fleet-lease pattern.

**Tech Stack:** Go 1.26.x (repo toolchain, no new deps), PowerShell 7 (`scripts/keep-alive.ps1`, `scripts/parallel-setup.ps1`), Supabase Postgres + REST migrations, Node ≥20 (fleet sweep scripts).

**Spec:** [FLEET_SWEEP_2026-10-17.md](../../../FLEET_SWEEP_2026-10-17.md) (verified evidence per claim; Supabase sweep + Actions deep check + follow-up verification) and [NODE_FLEET_FINDINGS.md](../../../NODE_FLEET_FINDINGS.md) (Sept findings: which are fixed — VidMoly daily cap, remote-thumb lease, fullSend, ffmpeg starvation, write-path canary — and which remain open, §"Suggested priority").

## Global Constraints

- `TestTrackLateAssetStillMissingIsTheOnlyRealFailure` must PASS **without weakening its assertions** (it is the sentinel the deleted CI was red on; weakening it hides the stuck-recording class again).
- Go module is `go 1.26.0` (go.mod), toolchain already installed at `%LOCALAPPDATA%\go-toolchain\go\bin` — add to PATH per command: `export PATH="$LOCALAPPDATA/go-toolchain/go/bin:$PATH"; export GOPATH="$LOCALAPPDATA/gopath"`.
- No new Go/JS dependencies. No `go.mod` changes.
- All Supabase changes are **idempotent migrations** under `supabase/migrations/`, applied with the existing `_live_sql.js`-style runner the findings doc uses; never hand-DELETE rows outside a migration.
- This checkout has **no `.git`** — commits happen in the repo clones during the sync step (Workstream G); the plan's "commit" steps name the message only.

**EXECUTION PAUSES (hard stops — the explicit authorization boundary):**
- Workstream G1: sync commits + push to the 18 `lawdachuss/node-*` repos (the single action that deploys to the fleet).
- Task F1 and Task C3: repo secrets on all 18 repos.
- Task D2 Step 1: applying a migration to the LIVE Supabase project.
Everything else in this plan runs locally against this checkout with no pause.
- Never put secrets in code or logs; host keys rotate through repo secrets only.
- Pin the existing test-suite helper conventions: `httptest`-based Supabase fakes in `database/supabase_test.go`, `logRecorder{}` in `channel/asset_late_test.go`, `stubManager` in `router/pool_test.go`.

## Review Focus

The five inputs a maintainer is most likely fed through and that no task's happy path exercises:

1. **A recording that never receives a byte but starts growing right before the watchdog's window** — i.e. a 0-byte file younger than `stuckRecordingGrace` (10 min) that then receives data — must NOT be killed; the threshold is the shared `zeroByteReapDue` predicate re-evaluated at every tick against a fresh `mtime`, not a snapshot age (Task B1's redemption-window case pins this).
2. **An ImgBB 400 that is NOT a rate limit** (e.g. `invalid key`) — must fail fast on that key but must NOT create a key-cooldown or rotate-and-silently-skip a bad config (Task C1 test). Today's body text to match exactly: `"Rate limit reached."`.
3. **A single transient AnonMP4 522** — must retry within the file's attempts; only the 3rd strike per UTC day flips the fleet-wide backoff (`upload_host_backoffs`), because 522 is an upstream-origin outage (Task C2 test).
4. **A tunnel whose DNS resolves but request hangs >10 s** — today's liveness only catches DNS death (node-6 proved the hang class); the probe must time out and re-mint (Task D1). Quick tunnels mint a NEW hostname on restart, so `nodes.web_url` must be rewritten in the same pass or the row goes stale (finding §9).
5. **A legitimately-late preview landing** must remain INFO-level and un-counted; only the never-finished case may be ERROR (Task A1 must not regress `TestTrackLateAssetEmptyResultIsWarnNotError` / the landed-late INFO test in the same file).

## Findings index (one ID taxonomy)

This plan uses only its own task IDs (A1, B1–B2, C1–C3, D1–D3, E1–E3, F1, G1–G2). External IDs are always cited with their source:
- **Sweep-report IDs** ([FLEET_SWEEP_2026-10-17.md](../../../FLEET_SWEEP_2026-10-17.md) C1–C4): C1 node-6 tunnel hang → **Task D1**; C2 zero-byte stuck recordings → **Task B1**; C3 broken upload hosts → **Tasks C1/C2/C3**; C4 ghost recording slots → **Task B2**.
- **Findings-doc IDs** (NODE_FLEET_FINDINGS.md sections C1–C6 — a DIFFERENT set): cited always as "findings doc C<n>". D1's web_url note references findings doc **C4** (`nodes.web_url` upsert transient-edge backoff, already landed) — **not** the sweep's C4 ghost slots.
- **Numbered sections** cite findings-doc issues: §1 stuck recordings, §9 stale web_url, §10 assignment leak, §11 liveness clobber.

---

## Workstream A — Root cause: the late-asset regression (proven red at HEAD 2026-10-17; fixed and green — see A1)

### Task A1: `trackLateAsset` must report a never-finishing asset as an error — ✅ COMPLETE (2026-10-17)

**Status: landed and proven green — a future executor must NOT redo this.** `channel/asset_late.go:131-137`'s `case <-timer.C:` branch was `warn(...)`; it now goes through `errFn("%s: %s treating as missing — still no URL %s after being abandoned and its goroutine never finished", …)`, matching the "never finished → error" contract documented above the function. Ledgered evidence:

- RED: `go test ./channel -run TestTrackLateAssetStillMissingIsTheOnlyRealFailure -count=1` → `got: []`, exit 1
- GREEN: same command → PASS; family `TestTrackLateAsset|TestLateAssetBound|TestCollectAssets` → 5/5 PASS (landed-late stays INFO, empty-result stays WARN)
- Full package: `go test ./channel -count=1` → ok 25.7s, exit 0

Workstream G1 still syncs this file to the 18 repos.

## Workstream B — Stuck-recording lifecycle (zero-byte files + ghost slots)

### Task B1: dead-monitor zero-byte files — the residual the existing watchdog misses

**Context (ledgered ruling):** a zero-byte watchdog already exists at HEAD — `manager/stuck_recording.go` (commit `69457538`, 2026-09-22), wired at `manager/manager.go:351` and `manager/manager.go:479` — reaping 0-byte **current** recording files (2-min sweep, 10-min grace, 30-min cooldown; pinned by `manager/stuck_recording_test.go`). Yet the sweep recorded 53 zero-byte files hours old across 8 nodes mid-session on 2026-10-17 — a state the existing sweep cannot see:

**Residual gap:** files listed in `m.ActiveRecordingFiles()` (`manager/manager.go:757`) that are **no longer any channel's `CurrentRecordingPath()`** (the recording monitor died inside the session-failure signature, or the channel rotated to its next file). The existing watchdog skips them (it stats only the current path), and the orphan sweep skips them because they sit in `activeRecs` (`channel/channel_file.go:1252`, `manager/manager.go:840`). Nothing ever deletes them.

**Files:**
- Modify: `manager/stuck_recording.go` — after the existing channel loop, a second pass over `m.ActiveRecordingFiles()`: 0 B + `mtime` older than `stuckRecordingGrace` + NOT any channel's `CurrentRecordingPath()` ⇒ owned by a **dead run**: delete the file (same removal helper the orphan flow uses at `channel/channel_file.go:1004`), log `log.Printf("[stuck-recording] removed %d dead-run 0-byte file(s) …")`. The pass NEVER restarts a channel — the run it may have belonged to is already gone — and is NOT counted in the `reaped` return.
- Test: extend `manager/stuck_recording_test.go` (five cases below; no new file)

**Interfaces:**
- Consumes: `m.ActiveRecordingFiles()`, `ch.CurrentRecordingPath()`, `zeroByteReapDue` (the threshold rule), `notifier.Notify` wiring in `main.go`.
- Produces: `ReapStuckRecordings()` keeps its signature and count semantics; the new deletions are log-recorded only. Task B2's release hook rides the watchdog kill path plus a `release func(username)` seam this pass passes in; total release is one call per killed channel (Review Focus rule 1's "file grew later" protection is inherited from `zeroByteReapDue`'s threshold).

- [x] **Step 1: Write the failing test** in `manager/stuck_recording_test.go` — five cases with temp files, per the Review-Focus rule:
- dead-run: 0 B, mtime 16 min, in `ActiveRecordingFiles()`, NOT any `CurrentRecordingPath()` → file DELETED from disk, `reaped` count unchanged
- current-file: 0 B, mtime 16 min, IS the channel's current path → the existing reap runs (kill + clear), unchanged
- redemption window: 0 B, mtime 5 min, not current → untouched (`zeroByteReapDue` protects)
- growing: non-zero bytes at any age → untouched
- not-active: 0 B old file NOT in `ActiveRecordingFiles()` → untouched (the orphan sweep owns it)

- [x] **Step 2: Run, expect FAIL** — the dead-run case fails at HEAD (the file survives): Run `go test ./manager -run TestReapStuckRecordings -count=1 -v`.

- [x] **Step 3: Implement** the second pass exactly as specified in Files.

- [x] **Step 4: Run** — `go test ./manager -run TestReapStuckRecordings -count=1 -v` → PASS; `go test ./manager ./channel -count=1` → PASS.

### Task B2: Node releases the slot it kills — no more permanent ghost `recording` rows

**Files:**
- Modify: `manager/manager.go` — after B1 kills a stuck channel, call the existing release path `coordinator` hookup the node already uses on channel stop (`ReassertAssignmentNode` / `SetAssignmentStatus` in `coordinator/shuffle.go`)
- Test: `manager/zero_byte_release_test.go` (new)

**Interfaces:**
- Consumes: Task B1's watchdog kill path (`ch.ReapStuckRecording(...)` for the current-file pass, and the dead-run pass releases via the same one-call-per-killed-channel hook) and the node's existing assignment-release function on stop (the same one a normal stop calls; find it via `ReleaseChannel` in `coordinator/assignment.go`).
- Produces: no new public API; a unit seam `release func(username string)` so the test stubs it.

- [x] **Step 1: Write the failing test** — watchdog kill ⇒ `release("username")` called once per killed channel; normal stop untouched.
- [x] **Step 2: Run, expect FAIL.**
- [x] **Step 3: Implement** — hook B1's kill path through the same release call a manual stop takes.
- [x] **Step 4: Run:** `go test ./manager -run TestZeroByteRelease -count=1` → PASS; `go test ./manager ./channel -count=1` → PASS.

## Workstream C — Upload-host resilience

### Task C1: ImgBB — per-key rotation on `"Rate limit reached."` (HTTP 400)

**Files:**
- Modify: `uploader/imgbb.go` (42-line key ring + `imgbbGlobal` throttle block, ~lines 18-41; the 429 handler at line 196)
- Test: `uploader/imgbb_test.go` (extend)

**Interfaces:**
- Consumes: the comma-separated `IMGBB_API_KEY` key list the config already parses.
- Produces: `classifyImgBBError(statusCode int, body string) imgbbErrClass` (`classRateLimited` / `classAuthFailed` / `classOther`) and a per-key cooldown map `imgbbKeyCooldown[key]string(ISO time)` consulted before an attempt; a cooled key is skipped for the request but NOT decremented as "no keys".

- [ ] **Step 1: Extend the failing test** — three cases against a fake `httptest` server: (a) key1 returns 400 + `"Rate limit reached."` → uploader tries key2 on the SAME file attempt (no retry-later needed), key1 enters cooldown; (b) key1 returns 400 + `"invalid key"` → no cooldown, no rotation, classic failure; (c) all keys cooled → the existing "all keys failed" error, not a panic.
- [ ] **Step 2: Run, expect FAIL** (`uploader` has no such rotation).
- [ ] **Step 3: Implement** — add the 400-body match to the 429 branch at `imgbb.go:196`, move key selection to consult `imgbbKeyCooldown`, write cooldown on `classRateLimited` (24 × the interval the 429 path already uses: `imgbbCooldownInterval`).
- [ ] **Step 4: Run** — `go test ./uploader -run TestImgBB -count=1 -v` → PASS, then `go test ./uploader -count=1` → PASS.

### Task C2: AnonMP4 — HTTP 522 is an upstream outage; disable the host fleet-wide after 3 strikes/day

**Files:**
- Modify: `uploader/anonmp4.go:304` — the node-assignment error return `fmt.Errorf("HTTP %d: %s", …)`; this is the exact observed fleet site ("anonmp4: node assignment: HTTP 522: error code: 522"). The attempt loop `anonmp4.go:195-222` accumulates `lastErr` (attempt count = `anonmp4MaxAttempts = 4`, `:102`); the same generic error pattern recurs at `:456` (upload response) and `:503`
- Modify: `uploader/uploader.go:505` (`isUploadRateLimited` family)
- Test: `uploader/anonmp4_daily522_test.go` (new, mirrors `uploader/vidmoly_dailyapi_test.go`)

**Interfaces:**
- Consumes: the fleet lease already built for VidMoly: `manager.disableHostFor(host, expiry, reason)` → `upload_host_backoffs` table + 5-minute peer poll (`manager/host_backoff.go`).
- Produces: `IsAnonMP4OriginDown(err) bool` — matched by the strike counter, NOT by the immediate disable branch.

- [x] **Step 1: Write the failing test** — ✅ 2026-10-20: `uploader/anonmp4_daily522_test.go` (3 tests): (a) classifier — the live node-assignment/chunk/finalize 522 shapes classify, 500/503/504/stat never do; (b) strike counter — 2 strikes disable nothing, the 3rd flips a disable expiring at the next UTC midnight (not a flat 24h), fires exactly once per day, day rollover resets; (c) wiring — 2 recorded strikes + one real 522 request → disable + immediate bail (no 4th attempt), later `UploadSelected` skips the host with zero requests.
- [x] **Step 2: Run, expect FAIL.** — ✅ (verified by construction against the pre-change code: none of the counter/wiring existed).
- [x] **Step 3: Implement** — ✅ 2026-10-20: `isAnonMP4OriginDown` (tight "http 522" matcher) + day-keyed `recordAnonMP4OriginDown` counter in `uploader/anonmp4.go`; the 3rd strike calls `disableHostFor("AnonMP4", next-UTC-midnight, "origin 522 (upstream outage)")` once and the loop bails with AnonMP4's own 522 wording intact.
- [x] **Step 4: Run** — ✅ 2026-10-20: `go test ./uploader -run TestAnonMP4 -count=1 -v` → PASS (3 new + 3 pre-existing).

## Workstream D — Tunnel / web_url hygiene

### Task D1: keep-alive tunnel liveness must catch the HANG class and re-mint in-session

**Files:**
- Modify: `scripts/keep-alive.ps1` — `Test-TunnelLiveness` (line ~442) and the existing re-mint block (tunnel start job, lines ~101-129)

**Interfaces:**
- Consumes: existing `$cloudflaredPath`, existing tunnel-URL regex extraction, `Update-NodeWebURL` upsert (findings doc C4 — the transient-edge backoff version is already in; see the Findings index for why this is not the sweep's C4).
- Produces: `Test-TunnelLiveness` returns `$true` only when `/api/health` answers **with a body** within 10 s (DNS-dead AND hang-dead both fail); a new `Repair-Tunnel` called on failure: stop cloudflared, re-run the mint job, wait ≤100 s for a new trycloudflare URL, upsert `nodes.web_url` + insert a fresh `tunnels` row.

- [ ] **Step 1: parametrize `Test-TunnelLiveness` so the endpoint URL and timeout are parameters** (current function tests only DNS via Cloudflare edge answers).
- [ ] **Step 2: add the 10 s `/api/health` probe with `Invoke-WebRequest -TimeoutSec 10 -ErrorAction Stop`** — hang ⇒ non-terminating timeout caught as `$false`.
- [ ] **Step 3: add `Repair-Tunnel`** reusing the existing mint job body verbatim (Start-Process cloudflared + stderr regex), then the existing web_url upsert block.
- [ ] **Step 4: wire into the 5-minute keep-alive loop** — on `$false`: log `[TUNNEL] dead/hung — re-minting`, call `Repair-Tunnel`, continue the session (do NOT kill the DVR).
- [ ] **Step 5: verify on one node manually** (scheduler step, not local): dispatch a manual workflow run, then hang-test by pulling the node's `/api/health` with a firewall block — expect a new hostname in `nodes.web_url` within 3 minutes. (Cloudflared's own `--url` re-mint is unchanged.)

### Task D2: purge the stale `tunnels` row and re-point the URL scripts

**Files:**
- Create: `supabase/migrations/20261017000000_prune_stale_tunnels.sql` — idempotent: `DELETE FROM public.tunnels WHERE created_at < now() - interval '48 hours';` (kills the dead `memphis-unto-…` `default` row) and keep `tunnels_public_select`/`tunnels_service_all` policies untouched. **Deviation from the original `WHERE is_active AND …` sketch:** the observed dead row is already `is_active=false`, so the plan's predicate would have deleted nothing — age alone is authoritative (a trycloudflare tunnel lives ~6h).
- Modify: `scripts/get-tunnel-url.ps1`, `scripts/open-ui.ps1`, **`scripts/get-tunnel-url.sh`** (the original spec named only the two `.ps1`, but Step 2's grep covers `scripts/` whole) — all references to `tunnel_sessions` (a table that 404s today) removed. **Deviation: re-pointed to `nodes?select=node_id,web_url,updated_at&web_url=not.is.null&order=updated_at.desc` instead of `tunnels?is_active=true…`.** Reason (verified live 2026-10-20): nothing writes to `tunnels` in practice — the operative registry is `nodes.web_url`, upserted every ~5 min by `keep-alive.ps1` (all 18 nodes populated); `tunnels` holds only the one stale row the migration deletes. A `tunnels` query would have returned 0 rows, failing Step 3's "prints a live URL".
- Modify (encoding fix discovered while running Step 3): `scripts/get-tunnel-url.ps1`, `scripts/open-ui.ps1` — UTF-8 **BOM** added. Pre-existing repo condition: all `.ps1` are UTF-8 without BOM, which Windows PowerShell 5.1 decodes as ANSI — emoji bytes `0x94` become smart quotes and abort parsing (unedited `keep-alive.ps1` reproduces it). With the BOM both scripts parse clean under 5.1.

- [x] **Step 1: write the migration** — `20261017000000_prune_stale_tunnels.sql` written (idempotent). **Apply with the live-SQL runner (`node _live_sql.js --file …`) still PENDING — live-DB write needs operator authorization.**
- [x] **Step 2: verify via REST** — `grep -r tunnel_sessions scripts/` → empty ✅. (`GET /rest/v1/tunnels` returns 0 stale rows is coupled to applying the migration → pending with Step 1.)
- [x] **Step 3: update the scripts and run `./scripts/get-tunnel-url.ps1`** — prints a live URL (`https://rating-accessories-cart-marina.trycloudflare.com`, node-17, updated 2026-10-20T08:55Z), exit 0 ✅.

### Task D3: name the write behind the schema-cache `column missing` retry loop

**Context (verified in both reports):** nodes log `[WARN] Supabase schema cache stale — column missing (attempt 3/3)` in loops (2026-10-17 sweep: node-11/node-14; Sept report §1). The client classifies `HTTP 400 + PGRST204` as transient (`database/supabase.go:158`) and retries — against the SAME write — then drops it after 3 attempts with no record of WHICH statement failed. Two defects: (a) the failing write is unidentified (a real pending schema gap goes unnoticed); (b) the classifier + retry logic is copy-pasted in two packages (`database/supabase.go:150-172`, `server/db.go:284`).

**Files:**
- Modify: `database/supabase.go:150-172` — extract `func isSchemaCacheStale(body string) string` (returns "" or the PGRST code; covers `PGRST002` 503, `PGRST204` 400, `42P10` 400) and enrich the final error with method + table
- Modify: `server/db.go:284` area — same call sites dedupe to the shared helper (single owner; the ledger carries this ruling)
- Test: `database/schema_cache_classify_test.go` (new)

**Interfaces:**
- Produces: `isSchemaCacheStale(body string) string` — consumed by both call sites; retry escalation behavior unchanged (same backoff, same maxRetries), only the identity of the failure becomes visible in the returned error.

- [x] **Step 1: Write the failing test** — `database/schema_cache_classify_test.go`: unit cases `PGRST002` 503 → "PGRST002", `PGRST204` 400 → "PGRST204", `42P10` 400 → "42P10", plain 400/empty → ""; `RestTable` path cases; harness case `TestSchemaCacheExhaustedRetriesNameFailingWrite` asserting the exhausted error carries `PGRST204`+`PATCH`+`channel_logs`.
- [x] **Step 2: Run, expect FAIL** — `undefined: IsSchemaCacheStale / RestTable` ✅
- [x] **Step 3: Implement** — `database.IsSchemaCacheStale(body) string` + `database.RestTable(path) string` added above `requestWithRetry` (single owner); `database/supabase.go`'s 3 copy-pasted branches → 1; `server/db.go`'s 2 → 1 via `database.*` (this copy previously LACKED the `42P10` case — the dedupe closes that gap too). **Deviation:** helper is exported (`IsSchemaCacheStale`, not the plan's lowercase sketch) — mandatory, `server` is a separate package. Retry escalation untouched (same backoff, same maxRetries); only the error/log now carry `[CODE] METHOD table` (log line e.g. `[WARN] Supabase schema cache stale [PGRST204] PATCH channel_logs (attempt 1/3)`).
- [x] **Step 4: Run** — `go build ./... && go vet ./database ./server && go test ./database ./server -count=1` → **ok/ok, EXIT=0** ✅
- [x] **Step 5: identify the actual failing write from live logs** — probed `channel_logs` (window-bounded + server-side `like`, the documented safe pattern) since 2026-10-06: **`PGRST204` = 0 rows → NO real column gap exists, nothing to migrate — the enriched error alone is the deliverable** (per this step's own criterion). But `PGRST002` = ≥50 rows (hits the fetch cap; every day 2026-10-07→19, e.g. `2026-10-19T05:40Z upload: could not save journal entry for Streamtape/realcest_…: max retries exceeded: HTTP 503: {"code":"PGRST002",…,"message":"Could not query the database for the schema cache. Retrying."}`) — real writes dropped daily when Supabase's schema cache is unreachable; until D3 these errors named no table, so the severity was invisible. The fix makes every future occurrence name `[PGRST002] <METHOD> <table>` in `channel_logs`/the DVR log. (Escalating PGRST002 maxRetries is a SEPARATE decision — not in this task's scope.)

## Workstream E — Pipeline gate + observability (the "no green-mask" fixes)

### Task E1: pre-session test gate in `parallel-setup.ps1`

**Files:** Modify: `scripts/parallel-setup.ps1` (after the Go build job, before keep-alive).

- [x] **Step 1: after the dvr exe build completes, run `go vet ./... && go test ./channel -count=1`** — ✅ 2026-10-20: gate inserted in the build job of `scripts/parallel-setup.ps1` right after the exe build loop (Go + modules already provisioned there). Bounded: vet + `./channel` only, well inside the 900s build budget.
- [x] **Step 2: on failure: write `D:\repos\<repo>\gate-failure.log` with the test output, log `[GATE] test suite failed — session aborted`, `exit 1`** — ✅ implemented; **deviation:** uses `throw` (not bare `exit 1`) because inside a `Start-Job` scriptblock `throw` marks the job `Failed`, which the main wait-block keys on — and the gate also **deletes `chaturbate-dvr.exe`**, because the wait-block's `Failed` check only fires when the exe is absent (the gate fails *after* the exe was built). Actions shows red, DVR never starts on broken code, watchman re-mints next cycle.
- [x] **Step 3: verify locally:** run the same vet+test in this checkout — ✅ `go vet ./...` exit 0, `go test ./channel -count=1` → `ok … 17.009s` exit 0 (exit-0 expected now that A1 landed). Parse-checked the edited script (clean under the BOM-copy harness; runners use `pwsh` which reads the repo's no-BOM UTF-8 correctly).

### Task E2: upload the DVR log when a job dies (the node-4 logless-death fix)

**Files:** Modify: `.github/workflows/secure-rdp.yml`.

- [x] **Step 1: add after "Cleanup on Cancel":** — ✅ 2026-10-20: `Upload DVR log` step added to `.github/workflows/secure-rdp.yml` (`actions/upload-artifact@v6`, `dvr-log`, both `dvr-output.log` + `dvr-err.log` from `${{ env.REPO_DIR }}`, `if-no-files-found: ignore`, `continue-on-error: true`, `if: always() && !cancelled()`). Deliberately after `Cleanup on Cancel` so the DVR process is dead and both logs are flushed before upload. YAML validated (`yaml.safe_load`: 12 steps, new step last).
- [ ] **Step 2: also name the removal of the `-` placeholder secret (repo setting, not code):** ops step — repo secrets on all 18 repos; clearing it un-masks every hyphen in all step logs (findings C5). No yaml change needed for this one.

### Task E3: promote the sweep scripts to a maintained tool

**Files:** Create: `scripts/fleet-sweep.mjs` (merged from `_fleet_sweep_tmp.js` + `_actions_sweep_tmp.js`), Modify: `README.md` (one section: usage, outputs, what to paste into an incident).

- [x] **Step 1: merge both temp scripts into `scripts/fleet-sweep.mjs`** with CLI flags `--sections=supabase,actions,uploads` and the now-known `/api/uploads` parser active by default (`{active,pending,history,processing}` counts per node → the report's per-node table). — ✅ 2026-10-20: tool created (also `--org=`, `--nodes=`, `--since=`, `--out=` flags; exit codes 0/1/2 = clean/fatal/unreachable). **Carries the H5 word-boundary classifier `/\b(ERROR|FATAL|PANIC)\b/i`** in both the `/api/logs` probe and the Actions success-log check, with a comment pointing at the live `twins_terror` false positive. `tunnels` kept only as a roster-scoped legacy fallback after the first run proved why: its stale `default`/memphis row invented a phantom 19th node and failed the run — fallback now accepts only roster-known node IDs (matches D2's finding that `nodes.web_url` is authoritative).
- [x] **Step 2: run `node scripts/fleet-sweep.mjs --sections=uploads`** against the fleet → 18/18 queue rows, exit 0. — ✅ 2026-10-20T09:36Z: `ROSTER: 18 nodes, 18 with tunnel URLs`, **`18/18 nodes answered`**, problem summary all-clear, `EXIT=0` (first run exited 2 on the phantom row — fixed per Step 1 note, rerun clean).
- [x] **Step 3: README section added.** — ✅ `README.md` → `### Fleet Diagnostics Sweep` under Usage: invocation examples, sections table, outputs, exit codes, and the "what to paste into an incident" guidance (problem summary + uploads table rows + `fleet-sweep.json`).

## Workstream F — The "who gets told" gap (notifications destination)

### Task F1: confirm or set a live alert destination

**Files:** ops-only + Modify: none (notifier code is in, destination is config).

### Task C3 (ops, no code): rotate the dead Streamtape credential

**Files:** none — repo secret `STREAMTAPE_LOGIN` + `STREAMTAPE_KEY` on all 18 repos (the workflow already maps both into the run env).

- [ ] **Step 1: obtain a fresh Streamtape login/key pair from the account owner and set both secrets on all 18 repos.**
- [ ] **Step 2: verify the key with the same 3-line probe the uploader uses (`POST api/streamtape` get upload server + auth check) against one node URL or a local curl.**
- [ ] **Step 3: confirm on the next session's sweep that no `API error 403: Authentication failed` line appears fleet-wide; the node's own "rejected credentials" disable un-latches automatically next run.**

- [ ] **Step 1: check what is actually set:** repo secrets `DISCORD_WEBHOOK` on node-1, and `app_settings` / `.env` for `NTFY_URL`/`DISCORD_WEBHOOK`.
- [ ] **Step 2: if unset, create a Discord webhook and set it as repo secret `DISCORD_WEBHOOK` on all 18 repos** (the workflow already maps it into the run env).
- [ ] **Step 3: verify end-to-end on one node: dispatch a session, let Task B1's notify fire (or trigger notifier test), confirm one Discord message arrives then silence.**

## Workstream G — Sync + rollout (the deploy itself)

### Task G1: land the fixes on the 18 repos

**Files:** source of truth = this checkout; sync mechanism = the user's existing flow that produced the 2026-10-15 09:00 bulk-sync (commit `7bca11d8`'s deprecation of CI notes the session builds from source).

- [x] **Step 1: run the whole local suite once:** `go build ./... && go vet ./... && go test ./channel ./uploader ./manager ./router ./database -count=1` — exit 0. — ✅ **2026-10-20 final gate (superset: also `./server ./uploadkeys`)**: `BUILD OK` / `VET OK` / all **7 packages ok, TEST=0** (channel 15.5s, uploader 17.7s, manager 10.5s, router 17.3s, database 4.8s, server 2.8s, uploadkeys 4.2s). Includes every change in this plan: H2+H3+H4+C2 (`uploader`, `database`), D3 (`database`, `server`), and the router test fix — `TestUpdateConfigReturnsKeyVerdictsToDialogSave` was failing under direct-call testing because gin records the 302 but only flushes via `WriteHeaderNow()` when the engine post-handler runs; fixed with one `c.Writer.WriteHeaderNow()` in the test's `save` helper (`router/update_config_test.go`) — **test-only, no production change**.
- [x] **Step 2: apply migration D2 to the live project** before the code that consumes it (the findings' rule). — ✅ **2026-10-20T09:41Z applied**: PostgREST service-role `DELETE FROM tunnels WHERE created_at < now()-48h` (identical predicate to `supabase/migrations/20261017000000_prune_stale_tunnels.sql`; no psql/supabase CLI available locally, the SQL file remains the canonical record). Removed exactly **1 row** — `21cda81c…` / `instance=default` / 2026-08-10 / `is_active=false`, the known dead memphis row. Verified after: 0 rows older than cutoff, memphis gone, `EXIT=0`.
- [x] **Step 3: sync the changed files to all 18 repos (`channel/asset_late.go` (A1, already landed), `manager/stuck_recording.go` + `manager/manager.go` release hook (B1+B2), `uploader/anonmp4.go` + `uploader/imgbb.go` + `uploader/imgchest.go` + `uploader/image_upload.go` (C1/H4+C2+H3), `database/supabase.go` retention batch (H2) + schema-cache helper (D3) + `server/db.go` (D3 dedupe), `router/update_config_test.go` (test-only fix), `scripts/keep-alive.ps1` (D1), `scripts/get-tunnel-url.ps1` + `scripts/open-ui.ps1` + `scripts/get-tunnel-url.sh` (D2), `scripts/parallel-setup.ps1` (E1), `.github/workflows/secure-rdp.yml` (E2), `scripts/fleet-sweep.mjs` (E3)) and push — one commit per workstream:** ✅ **done 2026-10-20: 18/18 repos × 9 commits pushed and API-verified.**
  - `fix(asset): report a never-finishing late asset as an error` (A1)
  - `feat(channel): zero-byte stuck-recording watchdog + slot release` (B1+B2)
  - `fix(uploader): imgbb per-key cooldown + anonmp4 522 fleet disable + imgchest quota lease` (C1/H4+C2+H3)
  - `fix(db): shrink log-retention delete batch under the 8KB proxy URI limit` (H2)
  - `fix(db): name the failing write in schema-cache retry errors + dedupe classifier` (D3 — `database/supabase.go`, `server/db.go`, `database/schema_cache_classify_test.go`)
  - `test(router): flush WriteHeader in update-config save helper` (`router/update_config_test.go` — gin records the 302 but direct-call tests never flush; test-only, no production change)
  - `fix(tunnel): in-session hang re-mint + prune stale tunnels rows + repoint URL scripts` (D1+D2)
  - `ci: pre-session test gate + dvr-log artifact` (E1+E2)
  - `feat(scripts): maintained fleet sweep tool with per-node upload queue table` (E3 — `scripts/fleet-sweep.mjs` + `README.md`; the original list omitted E3's own commit)
  - `docs(plan): fleet remediation verified status + Oct-20 sweep evidence` (this plan + `FLEET_SWEEP_2026-10-20.md`, pushed as a final pass after this doc was updated)
  - **Evidence (2026-10-20, ~10:0xZ):** shallow-cloned each `lawdachuss/node-N` (branch `main`), copied the workstream file groups, committed per group as `lawdachuss <lawdachuss@users.noreply.github.com>` (the historical sync identity), pushed — node-1 `d068f2c..9fbf6f0`, the other 17 analogous (`f6ec61e..379ad18` node-8 … `109ded0..80e9313` node-18). Fleet verification via the GitHub API: **18/18 repos, each with exactly the 9 G1 commits on top of its prior HEAD**. Deployed `database/supabase.go` byte-compares identical to this checkout (sha256 `7f5f3f4a43130821`, `IsSchemaCacheStale` + capped batch present). Deviations: (1) **H2+D3 split** — both modify `database/supabase.go`, so the H2 commit used a reconstructed H2-only intermediate (reverse-applied the two D3 edits, then diff-verified against the repo's own `main`: the sole difference is the `deleteRowsOlderThan` hunk; `go vet ./database` + `go test ./database` green in a scratch tree with the D3 test removed); the D3 commit restored the final file. (2) `scripts/keep-alive.ps1` was byte-identical to the repos' copy already — no-op commit skipped. (3) Test files committed alongside their production files. Diff-stat sanity: 24 files, +2085/−105, no EOL whole-file rewrites.
- [ ] **Step 4: watch one full session cycle end-to-end** — the sweep tool (E3) run at T+30 m must show: no 403 Streamtape line, no AnonMP4 522 disable spam after 3 strikes, zero-byte count 0, node-6 answerable.

### Task G2 (ops): prune the ghost assignment cohorts

**Files:** none new — `database.PruneOrphanedAssignments` and `DeleteChannelsNotIn` already exist (`database/supabase.go`); this task is wiring/verification of the sweep that findings #10 called for.

- [ ] **Step 1: check where `PruneOrphanedAssignments` is invoked today** (maintenance ticker? manual?). If manual-only, wire it to the node's existing maintenance tick with the conservative predicate: drop assignments whose `username` has no `channels` row, and stale `claimed` rows older than one session length (355 min).
- [ ] **Step 2: run it once against the live DB and record the counts** (expected reduction: the 165 no-channel rows + 379 >7 d stale claims from the Sept inventory).
- [ ] **Step 3: verify the symptom is gone** — `grep` the action logs for `channel not found (deleted/renamed)` in the next session; expect 0-fleet (Sept counted 49–50× per node per retained window).

## Workstream H — Oct-20 sweep follow-ups (new findings, 2026-10-20)

> Evidence: [FLEET_SWEEP_2026-10-20.md](../../../FLEET_SWEEP_2026-10-20.md) — 18/18 tunnels healthy, but 52 zero-byte files + 34 ghost slots (unchanged from Oct 17: the fixes exist locally, G1 never deployed them), plus three NEW failure classes: log-retention 414, ImgChest quota, INFO-as-ERROR mislabel.

### Task H1: run G1 — the deploy IS the fix for the worst live problem

No new code. The A1/B1/B2/D1 fixes are green in this checkout and the fleet still reproduces the identical bug cohorts because every session builds pre-fix `main`. Until G1 runs, no other task in this plan changes fleet behaviour even one percent.

- [x] **Step 1: local suite once:** `go build ./... && go vet ./... && go test ./channel ./uploader ./manager ./router ./database -count=1` — exit 0 (the toolchain is at `%LOCALAPPDATA%\go-toolchain\go\bin`). — ✅ 2026-10-20 final gate (superset: 7 packages) — see G1 Step 1.
- [x] **Step 2: apply D2's migration first** (the findings rule: migration before the code that needs it — the D1/D2 commit touches the tunnels table). — ✅ applied to the live DB 2026-10-20T09:41Z, before the sync (see G1 Step 2).
- [x] **Step 3: sync + push per the G1 commit list above** (it now carries H2/H3/H4; H5 needs no app file — closed as a false positive). — ✅ 2026-10-20: 18/18 repos × 9 commits pushed and API-verified (see G1 Step 3 evidence).
- [ ] **Step 4: T+30 m after the first post-sync session: run `node scripts/fleet-sweep.mjs` (the maintained tool, E3 — `_fleet_sweep_tmp.js` is superseded) and expect: zero-byte count trending to 0 after the watchdog's first sweep, ghost slots released, no `log retention … 414` line, no ImgChest 403 after 3 strikes.**

### Task H2: log retention is a silent no-op fleet-wide — HTTP 414 on the batch delete

**Root cause (verified in code):** `database.supabase.go:574` `deleteRowsOlderThan` pages 500 ids then deletes via `id=in.(<500 uuids>)` — a ~18 KB query string, over the ~8 KB proxy limit, so **every batch returns HTTP 414 and zero rows are ever pruned** (`server/db.go:1192` just WARNs). Seen live today on nodes 1/4/8/12/15 for both `channel_logs` and `disk_usage`. The codebase already knows this failure class: `DeleteChannelsNotIn`'s comment documents the exact same 8 KB limit.

**Files:** Modify `database/supabase.go` (`deleteRowsOlderThan`); Test: `database/retention_batch_test.go` (new, httptest fake in `database/supabase_test.go` style).

- [x] **Step 1: failing test** — ✅ 2026-10-20: the fake 414s any request URL > 8 KB; the pre-fix code failed exactly like production (`DeleteChannelLogsBefore: delete old channel_logs batch (250): HTTP 414`).
- [x] **Step 2: implement** — ✅ 2026-10-20: delete batch capped at 100 ids (`id=in.(<100 uuids>)` ≈ 3.7 KB, margin-safe), page remainder drained before re-paging (an unflushed id would be re-returned by the next `order=asc` page and double-counted), id-paging loop and metadata-table exclusion unchanged. Alternative considered and rejected: one filtered `created_at=lt.` DELETE — unbounded row lock on a multi-million-row table.
- [x] **Step 3: run** — ✅ 2026-10-20: `go test ./database -run TestRetention -count=1 -v` → PASS (3 tests, `database/retention_batch_test.go`); full package green.

### Task H3: ImgChest is storage-capped, not broken — disable fleet-wide until quota clears

Live: `imgchest: HTTP 403: {"error":"You've used 42.4 GB of your 10 GB storage…"}` on 6+ nodes, every image asset. The account is **4.2× over its quota** — no request can ever succeed until storage is freed, and every node rediscovers this per file. Same shape as the VidMoly daily-cap bug (findings #3, already solved there).

**Files:** Modify `uploader/imgchest.go` (find the non-200 error path); Test: `uploader/imgchest_quota_test.go` (new, mirrors `uploader/vidmoly_dailyapi_test.go`).

**Interfaces:** Consumes the existing fleet lease `disableHostFor(host, expiry, reason)` → `upload_host_backoffs` (5-minute peer poll). Produces a storage-quota classifier matching on the quota wording, NOT on a bare 403 (a plain auth 403 must stay a normal per-run failure). — **Implemented 2026-10-20 as unexported `isImgChestStorageFull(err)` in `uploader/imgchest.go`** (named per the codebase's existing classifier convention `isVidMolyDailyLimit`/`isVoeStorageFull` rather than the exported name sketched here), wired through a new `(*MultiImageUploader).tryImgChest` seam in `uploader/image_upload.go` — ImgChest lives on the image path, so the video path's error chain never sees it; the seam also carries the backed-off SKIP (guarded out of `AvailableHosts` per the fleetBackoff rule: a skipped host must not mark files complete).

- [x] **Step 1: failing test** — ✅ 2026-10-20: `uploader/imgchest_quota_test.go` (4 tests): live quota body classifies / plain 403 does not / first strike → ~24h timed disable + fleet cache + second call skips with zero requests / no token → no HTTP call, no disable.
- [x] **Step 2: implement** — ✅ 2026-10-20: classifier + `tryImgChest` strike path; ImgChest's own wording kept in the terminal error (`uploadWithRetries` already bails after ONE request — the 403 is fail-fast).
- [x] **Step 3: run** — ✅ 2026-10-20: `go test ./uploader -run "TestImgChest" -count=1 -v` → PASS (4 tests); uploader package green.
- [ ] **Step 4 (ops):** free storage or raise the ImgChest quota; the 24 h lease re-enables it automatically. Until then, image assets persist to the remaining hosts only.

### Task H4: finish C1 — ImgBB per-key cooldown (the remaining half)

C1's steps stand as written above; the delta verified today: rotation on the 400 `"Rate limit reached."` body already works (`uploader/imgbb.go` `isRateLimitError` matches body text), but the cooldown is a **global** throttle (`markImgBBRateLimited`), so a rate-limited key is re-tried on the very next call. Implement C1's `imgbbKeyCooldown[key]` map (24 × `imgbbCooldownInterval`) so a spent key is skipped for the cooldown window while other keys keep working — plus C1's Review-Focus rule 2: a non-rate-limit 400 (`invalid key`) must fail fast with NO cooldown entry.

- [x] (C1 steps 1–4) — ✅ **DONE 2026-10-20:** `uploader/imgbb.go` gained `classifyImgBBError` (rate-limited / auth / other; `isRateLimitError` now delegates to it), the `imgbbKeyCooldown` map with `coolImgBBKey`/`imgbbKeyCooled`, and `(*imgbbKeyRing).nextUsable()` — a cooled key is skipped WITHOUT consuming an attempt; all-keys-cooled returns a clean "cooling down" error (no nil-error return, no panic). The pacing consts became vars (vidmoly test convention). `uploader/imgbb_test.go` (5 tests) pins: (a) key1 400-rate-limit → key2 on the SAME attempt + key1 cooldown, (b) non-rate-limit 400 fails fast with zero cooldown entries and zero rotation, (c) all-keys-cooled clean error with zero requests, plus cooled-key skip and cooldown expiry recovery. All green.

### Task H5: ~~log-level mislabel~~ CLOSED 2026-10-20 — **false positive (sweep-detector artifact, not an app bug)**

**Diagnosis (root-caused end-to-end):** The finding's evidence (`_sweep.log:155-157`) was `E … INFO [twins_terror] channel is offline, try again in 1 min(s)` — where `E ` is the *sweep tool's own* "classified as error" marker. The classifier (`_fleet_sweep_tmp.js:181`) tested ring lines with the bare case-insensitive substring `/ERROR/i`, and the channel **name** `twins_terror` contains "**error**" inside "t**error**" — so every ring line belonging to that one channel was counted an error, level-tag or not. Verified against ground truth three ways: (1) the live node-18 ring shows these lines as ` …  INFO [username] channel is offline, …` — genuinely INFO (`ch.Info`, `channel_record.go:106`, byte-identical in node-18's deployed repo via raw GitHub); (2) `channel_logs` last 20 h: 40 error rows, **0** with offline/INFO/try-again text; (3) `log_level=eq.error` + `message=like.INFO*` (and `like.channel is offline*`) over 4 days: **0 rows**. The app's level mapping was never wrong.

**Fix applied (detector only):** `_fleet_sweep_tmp.js` now matches level tokens with word boundaries — `/\b(ERROR|FATAL|PANIC)\b/i` and `/\bWARN\b/i` — so usernames containing "error"/"warn" can never masquerade as levels. **Task E3 must carry this classifier into `scripts/fleet-sweep.mjs`.** No app code changed; no test warranted (nothing in the app changed).

## Backlog (explicitly NOT tasks in this plan)

- Preview's internal 45-min budget vs 3-min collect deadline (findings 5a) — trackLateAsset now reports honestly; re-balance only after imaging data says previews still abandon routinely.
- Cookie primary grabber (findings #8), sprite EINVAL holes (#7), finalize file-in-use races (#6), ring-buffer size (#12), `/api/logs ?limit` (#13), orphan age rounding (#15) — medium class; revisit after the criticalClass above lands.
- Standalone CI workflow reinstated in-source — superseded by E1's in-session gate (cheaper, respects the runner-budget rationale in `7bca11d8`).
- node-8's `[WARN] audio seg download failed: All attempts fail` repeats (2026-10-17 sweep, 2 hits before the session rollover) — parked: HLS audio-sidecar download failure with no local reproducible input; it needs one targeted log dump from the affected session before it can be triaged, and it shares the family of the stream-stall tracker (already handles `stream stalled N time(s)`).
