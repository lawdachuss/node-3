# MiniDelectableService

A high-performance, always-on stream recorder and video manager for Chaturbate and Stripchat. Written in Go with a built-in web UI, GPU-accelerated compression, multi-host uploading, and crash-recoverable pipelines.

## Features

- **Multi-site recording** — Chaturbate and Stripchat via a pluggable `site.Site` interface
- **HLS segment downloading** — Downloads raw TS/M4S segments directly from CDN edges with automatic failover across multiple CDN regions (lax, fra, AMS, sin, hnd)
- **GPU-accelerated compression** — Automatic encoder detection: NVENC (NVIDIA) > AMF (AMD) > QSV (Intel) > VideoToolbox (macOS) > CPU fallback via mp4ff muxer
- **Multi-host uploading** — Parallel uploads to GoFile, Streamtape, VOE.sx, MixDrop with file-hash deduplication
- **Crash-recoverable pipeline** — Post-recording pipeline (Thumbnail → Metadata → Cleanup) persisted in Supabase, survives restarts
- **Thumbnail generation** — Static thumbnails (1280×720), sprite sheets (4×4 grid), and animated GIF previews (40 frames, 8fps)
- **Built-in web UI** — Dark-mode dashboard with live channel logs (SSE), video browser, and player with HLS support
- **Adaptive rate limiting** — Token-bucket rate limiter + circuit breaker for resilient API calls
- **Chrome TLS fingerprinting** — Uses httpcloak to spoof Chrome 146 TLS fingerprints, bypassing Cloudflare WAF
- **Cloudflare tunnels** — One-command public access via cloudflared
- **Scheduled task persistence** — Windows Task Scheduler for auto-restart on reboot
- **File watcher** — fsnotify-based watcher for external video processing

## Quick Start

```bash
# Clone the repo
git clone https://github.com/YOUR_USERNAME/MiniDelectableService.git
cd MiniDelectableService

# Run automated setup (Windows)
.\setup.bat

# Or PowerShell
.\setup.ps1
```

The setup script installs all dependencies (FFmpeg, cloudflared, Go, Node.js, Python), compiles the binary, builds Tailwind CSS, and configures GitHub Actions deployment.

## Manual Setup

### Prerequisites

| Dependency | Purpose | Install |
|---|---|---|
| **Go 1.23+** | Build the binary | [go.dev](https://go.dev/dl/) |
| **FFmpeg** | Stream compression/muxing | [ffmpeg.org](https://ffmpeg.org/download.html) |
| **Node.js 20+** | Tailwind CSS build | [nodejs.org](https://nodejs.org/) |
| **Python 3** | Cookie refresh scripts | [python.org](https://www.python.org/) |
| **cloudflared** | Public tunnel access | [developers.cloudflare.com](https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/) |

### Build

```bash
# Install Go dependencies
go mod download

# Build Tailwind CSS
npm install
npm run build:css

# Compile binary
go build -o chaturbate-dvr.exe .
```

### Configure

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials
notepad .env
```

## Configuration

All configuration is done via environment variables (`.env` file) and JSON config files in `conf/`.

### Environment Variables

| Variable | Required | Description |
|---|---|---|
| `SUPABASE_URL` | Yes | Supabase project URL |
| `SUPABASE_API_KEY` | Yes | Supabase anon/service key |
| `USER_AGENT` | No | Custom User-Agent for API requests |

### Config Files

Place these in the `conf/` directory:

**`conf/settings.json`** — Global settings:
```json
{
  "cookies": "csrftoken=...; sessionid=...",
  "user_agent": "Mozilla/5.0 ...",
  "enable_gofile_upload": true,
  "enable_streamtape_upload": false,
  "enable_voesx_upload": false,
  "enable_mixdrop_upload": false,
}
```

**`conf/channels.json`** — Channel list:
```json
[
  { "username": "channel_name", "is_paused": false },
  { "username": "another_channel", "is_paused": true }
]
```

## Usage

### CLI Commands

```bash
# Start recording (default)
.\chaturbate-dvr.exe

# Start without tunnel
.\chaturbate-dvr.exe --no-tunnel

# Start with debug logging
.\chaturbate-dvr.exe --debug

# Specify output directory
.\chaturbate-dvr.exe --output-dir D:\videos

# Show version
.\chaturbate-dvr.exe --version
```

### Channel Management

```bash
# Add a channel
.\chaturbate-dvr.exe add <username>

# Remove a channel
.\chaturbate-dvr.exe remove <username>

# Pause a channel
.\chaturbate-dvr.exe pause <username>

# Resume a channel
.\chaturbate-dvr.exe resume <username>
```

### Other Commands

```bash
# Upload a local video file
.\chaturbate-dvr.exe upload <file_path>

# Run database migration
.\chaturbate-dvr.exe migrate

# Start a recording session
.\chaturbate-dvr.exe session <username>

# Recover orphaned files
.\chaturbate-dvr.exe orphan

# Start cloudflare tunnel only
.\chaturbate-dvr.exe tunnel
```

### Fleet Diagnostics Sweep

`scripts/fleet-sweep.mjs` is the maintained operator sweep for the 18-node fleet (merged from the one-off `_fleet_sweep_tmp.js` + `_actions_sweep_tmp.js`). It needs Node 18+, a `.env` with `SUPABASE_URL`/`SUPABASE_API_KEY`, and — for the `actions` section — an authenticated `gh` CLI. No secrets are ever printed.

```bash
# Everything: tunnel probes + Supabase cross-checks + upload queues + Actions
node scripts/fleet-sweep.mjs

# Only the per-node upload queue table (GET /api/uploads on every tunnel)
node scripts/fleet-sweep.mjs --sections=uploads

# Supabase checks + upload queues, skip the GitHub Actions section
node scripts/fleet-sweep.mjs --sections=supabase,uploads

# GitHub Actions deep sweep for a specific window
node scripts/fleet-sweep.mjs --sections=actions --since=2026-10-14T00:00:00Z
```

**Sections**

| Section | What it checks |
|---|---|
| `supabase` | roster + heartbeat freshness, tunnel probes (`/api/health`, `/api/orphans`, `/api/logs`), `channel_logs` error counts, disk usage, stuck/failed `pipeline_states`, dead-live `channel_assignments`, failed `upload_journal` rows |
| `uploads` | per-node queue table: `active` / `pending` / `history` / `processing` counts plus failed pending/history rows |
| `actions` | runs inventory, failing-run job logs + first error line, green-but-idle check, live/queued runs and cancel-loop detection (needs `gh`) |

**Outputs:** `fleet-sweep.json` (machine-readable, `--out=` prefix to change) + `fleet-sweep.log` + a `PER-NODE PROBLEM SUMMARY` on stdout.

**Exit codes:** `0` = all nodes reachable, `1` = fatal (missing `.env` / roster fetch failed), `2` = at least one node unreachable through its tunnel.

**What to paste into an incident:** the `PER-NODE PROBLEM SUMMARY` block plus the `node`/`active/pending/history` rows from the uploads table — together they show which node is sick and what its pipeline is holding. Attach `fleet-sweep.json` for full detail.

## Architecture

```
MiniDelectableService/
├── main.go                    # Entry point, CLI parsing, signal handling
├── config/                    # FFmpeg detection, concurrency config
├── entity/                    # Data models (ChannelConfig, Events)
├── server/                    # Global state, Supabase client, cache, disk monitor
├── manager/                   # Channel lifecycle, SSE, file watcher, sessions
├── channel/                   # Core recording logic
│   ├── channel.go             # Channel struct, context lifecycle
│   ├── channel_record.go      # HLS download, stream monitoring
│   ├── channel_file.go        # File creation, segment writing
│   ├── channel_compress.go    # GPU detection, MP4 muxing
│   ├── channel_upload.go      # Multi-host upload, dedup
│   ├── channel_thumbnail.go   # Thumbnail/sprite/GIF generation
│   ├── pipeline.go            # Crash-recoverable post-recording pipeline
│   └── upload_tracker.go      # Upload journal for crash recovery
├── chaturbate/                # Chaturbate API, HLS parsing, CDN failover
├── stripchat/                 # Stripchat API client
├── site/                      # Site interface (FetchStream, GetRoomStatus)
├── router/                    # Gin web server, API routes, SSE
│   ├── view/templates/        # Embedded HTML (Tailwind CSS, dark mode)
│   └── videos_handler.go      # Video browser with Supabase + local scan
├── uploader/                  # Upload hosts (GoFile, Streamtape, VOE.sx, etc.)
├── database/                  # Supabase REST client, migrations
├── internal/                  # HTTP client (httpcloak), rate limiter, errors
├── watcher/                   # fsnotify file watcher
├── scripts/                   # Diagnostic and utility scripts
├── docs/                      # Cookie/setup documentation
├── setup.bat                  # Automated Windows setup
└── setup.ps1                  # PowerShell equivalent
```

### Recording Pipeline

```
Chaturbate/Stripchat CDN
        │
        ▼
  ┌─────────────┐
  │ HLS Download │ ← Segment-by-segment with CDN failover
  └──────┬──────┘
         │
         ▼
  ┌─────────────┐
  │ Raw TS/M4S  │ ← Written to pending queue
  └──────┬──────┘
         │
         ▼
  ┌─────────────────┐
  │ GPU Compress    │ ← NVENC/AMF/QSV/VideoToolbox/CPU
  │ (MP4 Mux)       │
  └──────┬──────────┘
         │
         ▼
  ┌─────────────────┐
  │ Upload Pipeline │ ← GoFile, Streamtape, etc. (parallel)
  └──────┬──────────┘
         │
         ▼
  ┌─────────────────┐
  │ Thumbnail Gen   │ ← Static + Sprite + Animated GIF
  └──────┬──────────┘
         │
         ▼
  ┌─────────────────┐
  │ Save Metadata   │ ← Supabase database
  └──────┬──────────┘
         │
         ▼
  ┌─────────────────┐
  │ Cleanup         │ ← Remove temp files
  └─────────────────┘
```

## Web UI

The built-in web server (port 8080) provides:

- **Dashboard** — Live channel status, recording progress, disk usage
- **Live logs** — Real-time SSE stream per channel with filtering
- **Admin Dashboard** — Channels, nodes, pool assignments, upload queue, orphans, system health
- **Nodes Dashboard** — All registered nodes with status, load, heartbeat, web UI links
- **Pool Editor** — Add/remove channels from the shared channel pool
- **Video browser** — Search, filter, and play recorded videos
- **Video player** — HLS.js-powered player with quality selection and theater mode

Access locally at `http://localhost:8080` or via Tailscale/Tunnel.

## Distributed Shards (Pooled Mode)

Distribute channel recording across multiple GitHub Actions runner nodes sharing one Supabase database.

### Architecture

```
Template Repo (MiniDelectableService)
  │  push to main
  ▼
sync-nodes.yml  ──force-push──►  MiniDelectableService-node-a
                ──force-push──►  MiniDelectableService-node-b
                ──force-push──►  MiniDelectableService-node-c
                                    │
                         each runs secure-rdp.yml
                         with CHANNEL_POOL_MODE=pooled
                                    │
                                    ▼
                          ┌─────────────────┐
                          │  Supabase DB     │
                          │  ┌───────────┐   │
                          │  │ nodes      │   │
                          │  │ channel_   │   │
                          │  │ assignments│   │
                          │  └───────────┘   │
                          └─────────────────┘
```

### How It Works

1. **Template repo** — main development happens here. The `.github/workflows/sync-nodes.yml` workflow automatically force-pushes `main` to each node repo on every push.
2. **Node repos** — each has its own `secure-rdp.yml` that provisions a Windows RDP runner and runs the DVR with `CHANNEL_POOL_MODE=pooled`.
3. **Coordinator** — each node runs background loops for heartbeat (30s), channel claiming (60s), liveness checking (120s), and orphan reclamation (120s).
4. **Fair-share algorithm** — `ceil(total_live_channels / total_alive_nodes)` channels per node. Unassigned channels are claimed atomically via Supabase PATCH.

### Setup

#### 1. Run Database Migration

Run `database/migrate-v2.sql` in your Supabase SQL editor. This creates the `nodes`, `channel_assignments` tables and adds `node_id` to `pipeline_states`.

#### 2. Add Node Repos

Create GitHub repos named after your template repo with a `-node-X` suffix:

```
MiniDelectableService
MiniDelectableService-node-a
MiniDelectableService-node-b
MiniDelectableService-node-c
```

#### 3. Set Up Sync

Set a GitHub Personal Access Token with `repo` scope as `SYNC_PAT` secret on the template repo. Push to `main` — the `sync-nodes.yml` workflow force-pushes to all node repos.

#### 4. Configure Node Secrets

For each node repo, set these secrets in GitHub Actions:

| Secret | Value |
|---|---|
| `SUPABASE_URL` | Shared Supabase project URL |
| `SUPABASE_API_KEY` | Shared Supabase anon/service key |
| `CHANNEL_POOL_MODE` | `pooled` |
| `NODE_ID` | Unique node ID (e.g., `node-a`) |
| `INSTANCE_LABEL` | Optional human-readable label |
| `NODE_WEB_URL` | Optional public URL for this node's web UI |

All other secrets (`COOKIES`, `USER_AGENT`, uploader keys, etc.) can be shared or per-node.

#### 5. Trigger Node Workflows

```bash
gh workflow run secure-rdp.yml --repo YOUR_ORG/MiniDelectableService-node-a
gh workflow run secure-rdp.yml --repo YOUR_ORG/MiniDelectableService-node-b
```

#### 6. Add Channels to Pool

Open the web UI at any node's `/pool` page and add channels. The coordinator on each node will claim unassigned live channels every 60 seconds.

### Web UI Pages

| Route | Description |
|---|---|
| `/` | Main dashboard with channel selection |
| `/admin` | Unified admin: channels, nodes, pool, uploads, orphans, system health |
| `/nodes` | Standalone nodes dashboard |
| `/pool` | Pool editor — add/remove channels from shared pool |

### Node Web URL

Set `NODE_WEB_URL` env var on each node to enable cross-node navigation. Each node's tunnel or VPS address is shown as a clickable "Visit" link in the admin nodes table.

### Failure Modes

| Scenario | Recovery |
|---|---|
| **Node crash** | 180s heartbeat timeout → reaper releases its channels → other nodes claim them |
| **Supabase outage** | Heartbeats fail → nodes perceived dead → on recovery, first heartbeat revives node |
| **Split-brain** | Ownership verification every 60s during recording + file-hash dedup |
| **Cold start race** | Random jitter on claim interval + atomic SQL PATCH row locking |

### Rollback

Set `CHANNEL_POOL_MODE=isolated` on all nodes and restart. The existing `channel_assignments` rows become inert. All nodes revert to instance-scoped config files.

## Deployment

### GitHub Actions (Recommended)

The project includes a GitHub Actions workflow that provisions a Windows RDP runner:

1. Installs FFmpeg, Tailscale, cloudflared
2. Clones the repo and runs `setup.bat`
3. Creates scheduled tasks for DVR and tunnel persistence
4. Provides RDP access via Tailscale

**Required Secrets:**
| Secret | Description |
|---|---|
| `env` | Contents of your `.env` file |
| `TAILSCALE_AUTH_KEY` | Tailscale auth key for networking |

**Trigger:**
```bash
gh workflow run secure-rdp.yml
```

### Manual Server Deployment

```bash
# On your server
git clone https://github.com/YOUR_USERNAME/MiniDelectableService.git
cd MiniDelectableService
.\setup.bat -NoAppStart

# DVR runs as a Windows Scheduled Task (auto-restarts on reboot)
```

## Tech Stack

| Component | Technology |
|---|---|
| **Language** | Go 1.23 |
| **Web Framework** | Gin |
| **Database** | Supabase (PostgreSQL REST API) |
| **CSS** | Tailwind CSS |
| **TLS Fingerprinting** | httpcloak (Chrome 146) |
| **Video Muxing** | mp4ff |
| **HLS Parsing** | Go standard library |
| **File Watching** | fsnotify |
| **Upload Hosts** | GoFile, Streamtape, VOE.sx, MixDrop |
| **Tunnel** | Cloudflare (cloudflared) |
| **Networking** | Tailscale |
| **CI/CD** | GitHub Actions |

## License

MIT License — Copyright 2024 TeaCat

See [LICENSE](LICENSE) for details.
