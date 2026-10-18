package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/coordinator"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/logs"
	"github.com/teacat/chaturbate-dvr/manager"
	"github.com/teacat/chaturbate-dvr/notifier"
	"github.com/teacat/chaturbate-dvr/router"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/site"
	"github.com/teacat/chaturbate-dvr/uploadkeys"
	"github.com/teacat/chaturbate-dvr/uploader"
	"github.com/urfave/cli/v2"
)

var tunnelCancel atomic.Value

// diskMonitorStop is closed during graceful shutdown to stop the background disk monitor.
var diskMonitorStop = make(chan struct{})

const logo = `
 ██████╗██╗  ██╗ █████╗ ████████╗██╗   ██╗██████╗ ██████╗  █████╗ ████████╗███████╗
██╔════╝██║  ██║██╔══██╗╚══██╔══╝██║   ██║██╔══██╗██╔══██╗██╔══██╗╚══██╔══╝██╔════╝
██║     ███████║███████║   ██║   ██║   ██║██████╔╝██████╔╝███████║   ██║   █████╗
██║     ██╔══██║██╔══██║   ██║   ██║   ██║██╔══██╗██╔══██╗██╔══██║   ██║   ██╔══╝
╚██████╗██║  ██║██║  ██║   ██║   ╚██████╔╝██║  ██║██████╔╝██║  ██║   ██║   ███████╗
 ╚═════╝╚═╝  ╚═╝╚═╝  ╚═╝   ╚═╝    ╚═════╝ ╚═╝  ╚═╝╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝
██████╗ ██╗   ██╗██████╗
██╔══██╗██║   ██║██╔══██╗
██║  ██║██║   ██║██████╔╝
██║  ██║╚██╗ ██╔╝██╔══██╗
██████╔╝ ╚████╔╝ ██║  ██║
╚═════╝   ╚═══╝  ╚═╝  ╚═╝`

var version = "dev"

// loadDotEnv loads KEY=VALUE pairs from a .env file into the process environment,
// but does NOT overwrite existing environment variables.
// It tries the given path first, then falls back to the executable's directory.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		// Try relative to executable directory
		exe, err2 := os.Executable()
		if err2 == nil {
			f, err = os.Open(filepath.Join(filepath.Dir(exe), path))
		}
		if err != nil {
			return
		}
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])
		v = strings.Trim(v, `"'`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}

// refreshCookies runs scripts/cookie_refresher.py so the app starts with
// fresh cookies from the site. It is best-effort: if Python or the script is
// missing, or the refresh fails, the app continues with whatever cookies are
// already stored in Supabase/.env.
func refreshCookies() {
	// Locate a Python interpreter: python, python3, or the Windows launcher (py -3).
	py := ""
	var pyArgs []string
	for _, cand := range [][]string{{"python"}, {"python3"}, {"py", "-3"}} {
		if p, err := exec.LookPath(cand[0]); err == nil {
			py = p
			pyArgs = append([]string{}, cand[1:]...)
			break
		}
	}
	if py == "" {
		fmt.Println("⚠️  Python not found — skipping automatic cookie refresh (install Python + pip install -r requirements.txt)")
		return
	}

	fmt.Printf("🍪 Refreshing cookies from %s...\n", server.Config.Domain)
	// 1) fast anonymous refresh (csrftoken/__cf_bm) ...
	runCookieScript(py, pyArgs, "cookie_refresher.py")
	// 2) ... then a real-browser grab for a fresh, IP-bound cf_clearance.
	//    cb.xxx Turnstile-challenges datacenter IPs (e.g. GitHub runners) and
	//    only a browser executing JS can pass, so a stale cf_clearance blocks
	//    all API calls. Best-effort: if the browser is unavailable the DVR
	//    continues with whatever cookies it already has.
	runCookieScript(py, pyArgs, "cookie_grabber.py")
}

// runCookieScript locates scripts/<name> (next to the executable, then CWD)
// and runs it with the given Python interpreter. Best-effort: failures are
// logged but never fatal, so a missing dep can't stop the recorder.
func runCookieScript(py string, pyArgs []string, name string) {
	script := ""
	if exe, exeErr := os.Executable(); exeErr == nil {
		candidate := filepath.Join(filepath.Dir(exe), "scripts", name)
		if _, statErr := os.Stat(candidate); statErr == nil {
			script = candidate
		}
	}
	if script == "" {
		candidate := filepath.Join("scripts", name)
		if _, statErr := os.Stat(candidate); statErr == nil {
			script = candidate
		}
	}
	if script == "" {
		fmt.Printf("⚠️  scripts/%s not found — skipping\n", name)
		return
	}
	start := time.Now()
	// Give the cookie scripts enough headroom: cookie_refresher.py runs a
	// curl_cffi fetch (≤60s) and cookie_grabber.py runs the Scrapling browser
	// solve under its own GRAB_TOTAL_TIMEOUT watchdog (default 480s). The Go
	// context must exceed that watchdog, else Go kills the solve before the
	// script's own budget — a fresh cf_clearance would never be minted.
	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	args := append(append([]string{}, pyArgs...), script)
	cmd := exec.CommandContext(ctx, py, args...)
	// Inject the per-node cookie settings key so the Python script stores under
	// exactly the same app_settings key the Go side reads (dvr_settings:<node_id>).
	// This keeps Go's detectNodeID() and Python's per_node_settings_key() in sync.
	cmd.Env = append(os.Environ(), "COOKIE_SETTINGS_KEY="+server.CookieSettingsKey())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("⚠️  %s failed (continuing with existing cookies): %v\n", name, err)
		return
	}
	fmt.Printf("🍪 %s completed in %v\n", name, time.Since(start).Round(time.Millisecond))
}

// autoRefreshCookiesAndReload re-mints this node's cookies (the same scripts
// that run at startup) and then reloads the persisted cookies from Supabase
// into the live config, so running channels immediately use the fresh set.
// It is registered on the manager and fired (rate-limited) when the node is
// detected as Cloudflare-starved. Best-effort: failures only log.
func autoRefreshCookiesAndReload() {
	fmt.Println("[cf-recovery] persistent Cloudflare blocks detected — refreshing cookies")
	refreshCookies()
	if server.Config != nil && server.Config.SupabaseURL != "" && server.Config.SupabaseAPIKey != "" {
		if err := server.LoadSettings(); err != nil {
			fmt.Printf("[cf-recovery] reload cookies after refresh: %v\n", err)
		} else {
			fmt.Println("[cf-recovery] cookies reloaded from Supabase — channels will re-probe")
		}
	}
}

func main() {
	loadDotEnv(".env")
	// package init() ran before .env was loaded, so re-derive the cached node
	// id / pool mode now that NODE_ID & GITHUB_REPOSITORY are present. Keeps
	// NodeID(), the per-node cookie key, and the coordinator in sync.
	server.SyncNodeEnvironment()
	app := &cli.App{
		Name:    "chaturbate-dvr",
		Version: version,
		Usage:   "Record your favorite Chaturbate streams automatically. 😎🫵",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "username",
				Aliases: []string{"u"},
				Usage:   "The username of the channel to record",
			},
			&cli.StringFlag{
				Name:  "site",
				Usage: "Site to record from: chaturbate or stripchat (default: chaturbate)",
				Value: "chaturbate",
			},
			&cli.StringFlag{
				Name:  "admin-username",
				Usage: "Username for web authentication (optional)",
				Value: "",
			},
			&cli.StringFlag{
				Name:  "admin-password",
				Usage: "Password for web authentication (optional)",
				Value: "",
			},
			&cli.IntFlag{
				Name:  "framerate",
				Usage: "Desired framerate (FPS)",
				Value: 60,
			},
			&cli.IntFlag{
				Name:  "resolution",
				Usage: "Desired resolution (e.g., 2160 for 4K)",
				Value: 2160,
			},
			&cli.StringFlag{
				Name:  "pattern",
				Usage: "Template for naming recorded videos",
				Value: "videos/{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}",
			},
			&cli.IntFlag{
				Name:  "max-duration",
				Usage: "Split video into segments every N minutes ('0' to disable)",
				Value: 90,
			},
			&cli.IntFlag{
				Name:  "max-filesize",
				Usage: "Split video into segments every N MB ('0' to disable)",
				Value: 0,
			},
			&cli.StringFlag{
				Name:    "port",
				Aliases: []string{"p"},
				Usage:   "Port for the web interface and API",
				Value:   "8080",
			},
			&cli.IntFlag{
				Name:  "interval",
				Usage: "Check if the channel is online every N minutes",
				Value: 1,
			},
			&cli.StringFlag{
				Name:    "cookies",
				Usage:   "Cookies to use in the request (format: key=value; key2=value2)",
				EnvVars: []string{"COOKIES"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "user-agent",
				Usage:   "Custom User-Agent for the request",
				EnvVars: []string{"USER_AGENT"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "domain",
				Usage:   "Chaturbate domain to use (set DOMAIN in .env to override)",
				EnvVars: []string{"DOMAIN"},
				Value:   "https://www.cb.xxx/",
			},
			&cli.StringFlag{
				Name:    "ffmpeg-path",
				Usage:   "Path to ffmpeg executable (e.g. C:\\ffmpeg\\bin\\ffmpeg.exe). If not set, PATH is used.",
				EnvVars: []string{"FFMPEG_PATH"},
				Value:   "",
			},
			&cli.BoolFlag{
				Name:  "no-tunnel",
				Usage: "Skip automatic Cloudflare tunnel startup (useful when script manages it separately)",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "compress",
				Usage: "Compress recorded files (.ts or .mp4) to .mkv using ffmpeg after recording (auto-enabled if ffmpeg is installed)",
				Value: false,
			},
			&cli.StringFlag{
				Name:    "output-dir",
				Usage:   "Directory to move completed recordings to (empty = keep in place)",
				EnvVars: []string{"OUTPUT_DIR"},
				Value:   "",
			},
			&cli.BoolFlag{
				Name:    "per-model-folder",
				Usage:   "Create a subdirectory per model inside --output-dir",
				EnvVars: []string{"PER_MODEL_FOLDER"},
				Value:   false,
			},
			&cli.BoolFlag{
				Name:    "delete-local-after-upload",
				Usage:   "Delete local recordings and preview files after successful remote upload",
				EnvVars: []string{"DELETE_LOCAL_AFTER_UPLOAD"},
				Value:   true,
			},
			&cli.IntFlag{
				Name:    "orphan-cleanup-interval",
				Usage:   "Minutes between periodic orphan file cleanup and thumbnail scans (0 = disabled, run once at startup)",
				EnvVars: []string{"ORPHAN_CLEANUP_INTERVAL"},
				// Default to 60 min so locally-kept files without thumbnails get
				// a periodic ScanThumbnails retry on every deployment (CI runners
				// have no .env to set this). Set 0 to disable.
				Value: 60,
			},
			&cli.IntFlag{
				Name:    "log-retention-days",
				Usage:   "Prune pure telemetry/log tables (channel_logs, disk_usage) older than this many days in the periodic sweep (0 = disabled; never touches recordings/upload_links/preview_images/metadata)",
				EnvVars: []string{"LOG_RETENTION_DAYS"},
				// 3 days matches the widely-referenced "logs are safe to drop after
				// 3 days" rule; channel_logs is 96% old rows by volume.
				Value: 3,
			},
			&cli.IntFlag{
				Name:    "disk-warning-percent",
				Usage:   "Log warning when disk usage exceeds this percentage (0 = disabled)",
				EnvVars: []string{"DISK_WARNING_PERCENT"},
				Value:   80,
			},
			&cli.IntFlag{
				Name:    "disk-critical-percent",
				Usage:   "Auto-delete oldest local recordings when disk usage exceeds this percentage (0 = disabled)",
				EnvVars: []string{"DISK_CRITICAL_PERCENT"},
				Value:   90,
			},
			&cli.StringFlag{
				Name:    "session-duration",
				Usage:   "Recording session length (e.g. \"5h20m0s\"); after this elapses the system stops, processes all pending files, then restarts (empty = continuous recording)",
				EnvVars: []string{"SESSION_DURATION"},
				Value:   "",
			},
			&cli.BoolFlag{
				Name:    "disable-early-drain",
				Usage:   "Never force-stop channels before the session duration elapses, even if the upload backlog can't finish before the run deadline",
				EnvVars: []string{"DISABLE_EARLY_DRAIN"},
				Value:   false,
			},
			&cli.IntFlag{
				Name:    "max-local-age-days",
				Usage:   "Delete local recordings older than this many days if already uploaded (0 = disabled)",
				EnvVars: []string{"MAX_LOCAL_AGE_DAYS"},
				Value:   0,
			},
			&cli.IntFlag{
				Name:    "min-duration-before-upload",
				Usage:   "Minimum video duration in seconds before uploading; shorter videos wait and merge with the next recording (0 = disabled)",
				EnvVars: []string{"MIN_DURATION_BEFORE_UPLOAD"},
				Value:   1200,
			},
			&cli.IntFlag{
				Name:    "min-file-size-before-upload",
				Usage:   "Minimum file size in MB before a recording can be uploaded; smaller files are treated as crash artifacts and discarded (0 = disabled)",
				EnvVars: []string{"MIN_FILE_SIZE_BEFORE_UPLOAD"},
				Value:   1,
			},
			&cli.StringFlag{
				Name:    "voesx-api-key",
				Usage:   "API key for VOE.sx uploads",
				EnvVars: []string{"VOESX_API_KEY"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "streamtape-login",
				Usage:   "Login username for Streamtape uploads",
				EnvVars: []string{"STREAMTAPE_LOGIN"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "streamtape-key",
				Usage:   "API key for Streamtape uploads",
				EnvVars: []string{"STREAMTAPE_KEY", "STREAMTAPE_API_KEY"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "mixdrop-email",
				Usage:   "Email for Mixdrop uploads",
				EnvVars: []string{"MIXDROP_EMAIL"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "mixdrop-token",
				Usage:   "API token for Mixdrop uploads",
				EnvVars: []string{"MIXDROP_TOKEN", "MIXDROP_KEY"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "vidara-key",
				Usage:   "API key for Vidara uploads",
				EnvVars: []string{"VIDARA_KEY"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "vidmoly-key",
				Usage:   "API key for VidMoly uploads (free accounts cap at ~50/day — host auto-skips until the next day)",
				EnvVars: []string{"VIDMOLY_KEY"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "disabled-upload-hosts",
				Usage:   "Comma-separated upload hosts to never attempt this run (e.g. \"AnonMP4\")",
				EnvVars: []string{"DISABLED_UPLOAD_HOSTS"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "anonmp4-view-base",
				Usage:   "Base URL that AnonMP4 archive links are normalized to (e.g. https://anonmp4.help)",
				EnvVars: []string{"ANONMP4_VIEW_BASE"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "catbox-proxy-url",
				Usage:   "Cloudflare Worker proxy URL for Catbox uploads (avoids direct IP blocks)",
				EnvVars: []string{"CATBOX_PROXY_URL"},
				Value:   "",
			},
			&cli.IntFlag{
				Name:    "upload-max-concurrent",
				Usage:   "Maximum number of video files uploading at once (0 = auto, VM-sized)",
				EnvVars: []string{"UPLOAD_MAX_CONCURRENT"},
				Value:   0,
			},
			&cli.IntFlag{
				Name:    "upload-host-concurrency",
				Usage:   "Maximum concurrent uploads per file host (0 = auto, VM-sized)",
				EnvVars: []string{"UPLOAD_HOST_CONCURRENCY"},
				Value:   0,
			},
			&cli.IntFlag{
				Name:    "pipeline-workers",
				Usage:   "Concurrent upload pipelines per channel queue (0 = auto, VM-sized)",
				EnvVars: []string{"PIPELINE_WORKERS"},
				Value:   0,
			},
			&cli.IntFlag{
				Name:    "retry-workers",
				Usage:   "Concurrent upload jobs in the retry pool (0 = auto, VM-sized)",
				EnvVars: []string{"RETRY_WORKERS"},
				Value:   0,
			},

			&cli.StringFlag{
				Name:    "supabase-url",
				Usage:   "Supabase project URL for remote data persistence (REST API fallback)",
				EnvVars: []string{"SUPABASE_URL"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "supabase-api-key",
				Usage:   "Supabase anon/public API key for REST API fallback",
				EnvVars: []string{"SUPABASE_API_KEY"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "supabase-service-role-key",
				Usage:   "Supabase service_role key (bypasses RLS) for database writes",
				EnvVars: []string{"SUPABASE_SERVICE_ROLE_KEY"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "affiliate-wm",
				Usage:   "Webmaster code for the affiliate onlinerooms bulk liveness check",
				EnvVars: []string{"AFFILIATE_WM"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "stripchat-pdkey",
				Usage:   "MOUFLON v2 decryption key for Stripchat HLS streams",
				EnvVars: []string{"STRIPCHAT_PDKEY"},
				Value:   "",
			},

			// ── Distributed shards/nodes ────────────────────────────────────
			&cli.StringFlag{
				Name:    "channel-pool-mode",
				Usage:   "Channel distribution mode: 'isolated' (default) or 'pooled'",
				EnvVars: []string{"CHANNEL_POOL_MODE"},
				Value:   "isolated",
			},
			&cli.StringFlag{
				Name:    "node-id",
				Usage:   "Unique node identifier for distributed mode (auto-detected if unset)",
				EnvVars: []string{"NODE_ID"},
				Value:   "",
			},

			// ── Finalization ────────────────────────────────────────────────
			&cli.StringFlag{
				Name:    "completed-dir",
				Usage:   "Directory to move fully closed recordings into (default: <recording dir>/completed)",
				EnvVars: []string{"COMPLETED_DIR"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "finalize-mode",
				Usage:   "Post-process closed recordings: none (fast seek index), remux, or transcode",
				EnvVars: []string{"FINALIZE_MODE"},
				// Default to remux so every deployment (including CI runners
				// without a .env) produces browser-playable MP4s. Explicitly
				// set FINALIZE_MODE=none to keep the old raw-file behavior.
				Value: "remux",
			},
			&cli.StringFlag{
				Name:  "ffmpeg-encoder",
				Usage: "FFmpeg video encoder for transcode mode (e.g. libx264, libx265, h264_nvenc)",
				Value: "libx264",
			},
			&cli.StringFlag{
				Name:  "ffmpeg-container",
				Usage: "FFmpeg output container for remux/transcode mode (mp4 or mkv)",
				Value: "mp4",
			},
			&cli.IntFlag{
				Name:  "ffmpeg-quality",
				Usage: "FFmpeg quality value (CRF for software encoders, CQ for many hardware encoders)",
				Value: 23,
			},
			&cli.StringFlag{
				Name:  "ffmpeg-preset",
				Usage: "FFmpeg preset for transcode mode",
				Value: "medium",
			},
			&cli.BoolFlag{
				Name:  "debug",
				Usage: "Write full HTML responses to temp files when stream detection fails and log verbose details",
				Value: false,
			},
			&cli.BoolFlag{
				Name:    "refresh-cookies",
				Usage:   "Automatically refresh cookies from the site at startup by running scripts/cookie_refresher.py (best-effort; requires Python + curl_cffi)",
				EnvVars: []string{"REFRESH_COOKIES"},
				Value:   true,
			},

			// ── Notifications ───────────────────────────────────────────────
			&cli.StringFlag{
				Name:    "ntfy-url",
				Usage:   "ntfy.sh server URL for notifications (e.g. https://ntfy.sh)",
				EnvVars: []string{"NTFY_URL"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "ntfy-topic",
				Usage:   "ntfy.sh topic for notifications",
				EnvVars: []string{"NTFY_TOPIC"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "ntfy-token",
				Usage:   "ntfy.sh access token (optional)",
				EnvVars: []string{"NTFY_TOKEN"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "discord-webhook-url",
				Usage:   "Discord webhook URL for notifications",
				EnvVars: []string{"DISCORD_WEBHOOK_URL"},
				Value:   "",
			},
			&cli.IntFlag{
				Name:    "cf-channel-threshold",
				Usage:   "Consecutive Cloudflare blocks per channel before a notification fires (default 5)",
				EnvVars: []string{"CF_CHANNEL_THRESHOLD"},
				Value:   5,
			},
			&cli.IntFlag{
				Name:    "cf-global-threshold",
				Usage:   "Channels blocked simultaneously before a global Cloudflare notification fires (default 3)",
				EnvVars: []string{"CF_GLOBAL_THRESHOLD"},
				Value:   3,
			},
			&cli.IntFlag{
				Name:    "cf-retry-minutes",
				Usage:   "How long a Cloudflare-blocked channel waits before retrying (default 5 min; reduces hammering and gives cookie refresh time to work)",
				EnvVars: []string{"CF_RETRY_MINUTES"},
				Value:   5,
			},
			&cli.IntFlag{
				Name:    "cf-starved-threshold",
				Usage:   "Channels blocked simultaneously before the node sheds its claims to the pool and re-mints cookies (default 5)",
				EnvVars: []string{"CF_STARVED_THRESHOLD"},
				Value:   5,
			},
			&cli.IntFlag{
				Name:    "cf-session-cut-threshold",
				Usage:   "Distinct channels hitting a session-failure signature (CDN 403/404 + failing site probe, or a CF-block burst) within the window before the node re-mints cookies early (default 3; 0 = use default)",
				EnvVars: []string{"CF_SESSION_CUT_THRESHOLD"},
				Value:   3,
			},
			&cli.IntFlag{
				Name:    "cf-refresh-min",
				Usage:   "Minimum minutes between automatic cookie refresh attempts after persistent Cloudflare blocks (default 10)",
				EnvVars: []string{"CF_REFRESH_MIN"},
				Value:   10,
			},
			&cli.IntFlag{
				Name:    "notify-cooldown-hours",
				Usage:   "Hours between repeated notifications of the same type (default 4)",
				EnvVars: []string{"NOTIFY_COOLDOWN_HOURS"},
				Value:   4,
			},
			&cli.BoolFlag{
				Name:    "notify-stream-online",
				Usage:   "Send a notification when a watched channel goes live",
				EnvVars: []string{"NOTIFY_STREAM_ONLINE"},
				Value:   false,
			},
		},
		Action: start,
	}
	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func start(c *cli.Context) error {
	started := time.Now()

	// Capture all stdout/stderr output so it is available via GET /api/logs
	// and the /logs page. Must run before any logging or the web router.
	logs.Install()
	fmt.Println(logo)
	fmt.Println("[startup] log capture enabled — view logs at /logs (API: /api/logs)")

	var err error
	server.Config, err = config.New(c)
	if err != nil {
		return fmt.Errorf("new config: %w", err)
	}
	fmt.Printf("[startup] config loaded in %v\n", time.Since(started).Round(time.Millisecond))

	// Apply upload throughput tuning before any upload goroutines start.
	// Every pool is VM-sized: derived from this runner's vCPU count
	// (GitHub-hosted VMs come in fixed sizes — standard windows-latest is
	// 2 vCPU, larger runners are 4/8/16/32/64), so a 2-core runner gets the
	// tuned baseline and larger runners scale up.  An explicit flag/env
	// value (non-zero) always wins over the auto value.
	retryW, uploadSem, goFile, other, pipelineW := config.VMSizedConcurrency()
	if server.Config.RetryWorkers > 0 {
		retryW = server.Config.RetryWorkers
	}
	if server.Config.UploadMaxConcurrent > 0 {
		uploadSem = server.Config.UploadMaxConcurrent
	}
	if server.Config.UploadHostConcurrency > 0 {
		goFile = server.Config.UploadHostConcurrency
		other = server.Config.UploadHostConcurrency
	}
	if server.Config.PipelineWorkers > 0 {
		pipelineW = server.Config.PipelineWorkers
	}
	channel.SetRetryWorkers(retryW)
	channel.SetUploadConcurrency(uploadSem)
	uploader.SetHostConcurrencyTiered(goFile, other)
	uploader.SetDisabledHosts(server.Config.DisabledUploadHosts)
	if len(server.Config.DisabledUploadHosts) > 0 {
		fmt.Printf("[startup] disabled upload hosts (never attempted): %v\n", server.Config.DisabledUploadHosts)
	}
	uploader.SetAnonMP4ViewBase(server.Config.AnonMP4ViewBase)
	if server.Config.AnonMP4ViewBase != "" {
		fmt.Printf("[startup] AnonMP4 archive links normalized to %s\n", server.Config.AnonMP4ViewBase)
	}
	channel.SetPipelineWorkers(pipelineW)
	fmt.Printf("[startup] upload concurrency (VM-sized, %d vCPU): %d files max, %d/%d per host (GoFile/other), %d retry workers, %d pipelines/channel\n",
		runtime.NumCPU(), uploadSem, goFile, other, retryW, pipelineW)

	// Refresh cookies from the site before loading settings so every start
	// begins with fresh cookies (best-effort; requires Python + curl_cffi).
	if c.Bool("refresh-cookies") {
		refreshCookies()
	}

	// Load cookies from Supabase if available (overrides .env)
	if server.Config.SupabaseURL != "" && server.Config.SupabaseAPIKey != "" {
		fmt.Println("📦 Loading cookies from Supabase...")
		if err := server.LoadSettings(); err != nil {
			fmt.Printf("⚠️  Failed to load cookies from Supabase: %v\n", err)
			fmt.Println("   Falling back to .env cookies")
		} else {
			fmt.Println("✅ Cookies loaded from Supabase")
		}
		// Persist the merged config (env + Supabase) back to Supabase so that
		// upload credentials set in .env survive on subsequent runs where .env
		// is absent (e.g. GitHub Actions). Best-effort — a failure here is not fatal.
		if err := server.SaveSettings(); err != nil {
			fmt.Printf("⚠️  Failed to persist merged settings to Supabase: %v\n", err)
		}

		// AFFILIATE_WM is seeded into the global dvr_settings blob on every
		// start, making Supabase the single source of truth: the first node
		// that boots with a value (env/GitHub secret) writes it centrally, and
		// every later node's LoadSettings applies that central value over its
		// own env (an empty stored value is skipped, so it never wipes a
		// locally-set one). Only the effective value is persisted.
		if server.Config.AffiliateWM != "" {
			fmt.Println("✅ AFFILIATE_WM persisted to Supabase — central single source of truth")
		}
	}

	if server.Config.Cookies == "" || server.Config.UserAgent == "" {
		fmt.Println("⚠️  Chaturbate API requests may fail — COOKIES and USER_AGENT not set in .env or Supabase")
		fmt.Printf("   Open .env and fill in your browser cookies from %s:\n", server.Config.Domain)
		fmt.Printf("   Chrome 146: F12 → Application → Cookies → %s → copy all as string\n", server.Config.Domain)
		fmt.Println("   OR update cookies in Supabase via the web UI")
		fmt.Println("   IMPORTANT: Use Chrome 146+ on Windows for cookie collection so the TLS")
		fmt.Println("   fingerprint matches the httpcloak preset.")
		fmt.Println()
	}

	// The affiliate onlinerooms API gives a fast, single-call bulk liveness
	// check that covers ALL channels (coordinator Phase 1 + liveChecker Tier 0).
	// Without AFFILIATE_WM every channel falls back to a slower per-channel
	// check, so surface a clear startup warning instead of silent degradation.
	if server.Config.AffiliateWM == "" {
		fmt.Println("⚠️  AFFILIATE_WM not set — affiliate bulk liveness check disabled (every channel uses the slower per-channel check)")
		fmt.Println("   Set AFFILIATE_WM in .env, as a GitHub Actions secret, or via the web UI Settings dialog")
		fmt.Println("   (persisted centrally to Supabase) to enable the fast onlinerooms bulk check.")
		fmt.Println()
	}

	// Warm up TLS sessions with Cloudflare in the background so server
	// startup is not delayed by the first connection.
	go func() {
		warmupT := time.Now()
		warmupCtx, warmupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		internal.WarmupChaturbate(warmupCtx)
		warmupCancel()
		warmupCtx, warmupCancel = context.WithTimeout(context.Background(), 10*time.Second)
		internal.WarmupStripchat(warmupCtx)
		warmupCancel()
		fmt.Printf("[startup] TLS warmup completed in %v\n", time.Since(warmupT).Round(time.Millisecond))
	}()

	server.Manager, err = manager.New()
	if err != nil {
		return fmt.Errorf("new manager: %w", err)
	}
	fmt.Printf("[startup] manager created in %v\n", time.Since(started).Round(time.Millisecond))

	// Wire the auto cookie refresh (re-mint + reload) so a node that goes
	// Cloudflare-starved mid-session can recover without a restart.
	server.Manager.SetCookieRefreshFunc(autoRefreshCookiesAndReload)

	// Reconcile the session length before the coordinator registers its
	// session_deadline: local SESSION_DURATION wins, else the central Supabase
	// value, else a CI-safe fallback — so every node stops recording and
	// uploads at the same time, even with no env set.
	server.ApplyCentralSessionDuration()

	// Log the resolved session config so /api/logs shows at a glance whether
	// this node will stop recording at a deadline (and when) or run
	// continuously — the key diagnostic for unexplained short recordings.
	if d := server.Config.SessionDurationParsed; d > 0 {
		fmt.Printf("[startup] session duration resolved: %s — next stop at %s\n", d.Round(time.Second), time.Now().Add(d).Format(time.RFC3339))
	} else {
		fmt.Println("[startup] session duration resolved: none — continuous recording (no deadline, no migration chop)")
	}

	// Route disk-threshold alerts through the notifier (Discord/ntfy).
	server.DiskAlert = notifier.Notify

	// Same wiring for the upload-link write-path canary.  The recovery hook clears
	// the alert cooldown, so a path that breaks again after a repair is reported
	// immediately rather than after the previous alert's cooldown.
	server.LinkPathWriteAlert = notifier.Notify
	server.LinkPathWriteRecovered = notifier.Default.ResetCooldown

	// An alert with nowhere to go is just a log line, and the upload-link write
	// path is exactly the kind of failure that needs to leave the node.  Say so
	// once at startup rather than assuming someone configured ntfy/Discord.
	if !((server.Config.NtfyURL != "" && server.Config.NtfyTopic != "") || server.Config.DiscordWebhookURL != "") {
		fmt.Println("[WARN] no notification destination configured (ntfy-url + ntfy-topic, or discord-webhook) — " +
			"alerts such as a broken upload-link write path will only appear in this node's log and on /api/health")
	}

	// Verify the upload_links write path once at startup, before the pipelines can
	// produce recordings that could never be marked as uploaded.  A broken write
	// path is reported here (and by every later check) instead of surfacing hours
	// later as a pile of host-less recordings.
	go func() {
		if err := server.CheckUploadLinkWritePath(); err != nil {
			fmt.Printf("[startup] upload-link write path is BROKEN: %v\n", err)
		}
	}()

	// ── Startup orphan cleanup ──────────────────────────────────────────
	// Delete DB rows left behind when a previous runner was killed mid-pipeline.
	// SaveRecordingBasics creates a row at enqueue time; if the runner dies
	// before stageSaveMetadata, the row has no thumbnail, no embed_url, and
	// no upload links — an orphan that shows as a broken entry on the site.
	// Only deletes rows older than 30 min (so in-flight recordings survive).
	go func() {
		orphanAge := 30 * time.Minute
		if deleted := server.CleanupOrphanedRecordings(orphanAge); deleted > 0 {
			fmt.Printf("[startup] orphan cleanup: deleted %d stale recording(s) from previous run\n", deleted)
		}
	}()

	// ── Central upload-key store ─────────────────────────────────────────
	// Apply fleet-wide upload credentials from Supabase (app_settings
	// upload_host_keys) over this node's env-derived values, seed the store
	// from env on first use, and start the background prober so dead keys
	// surface on the admin page in real time. Idempotent + best-effort: a
	// Supabase outage only delays the sync to the next refresh cycle.
	go uploadkeys.Start()

	// ── Distributed coordinator ──────────────────────────────────────────
	var coord *coordinator.Coordinator
	var mgr *manager.Manager
	if m, ok := server.Manager.(*manager.Manager); ok {
		mgr = m
	}
	if server.ChannelPoolMode() == entity.PoolModePooled && mgr != nil {
		dbClient := server.GetDBClient()
		if dbClient != nil {
			coord = coordinator.New(dbClient, mgr)
			coord.LiveCheck = &liveChecker{}
			mgr.Coordinator = coord
			fmt.Printf("[startup] coordinator created for node %q (pooled mode)\n", coord.NodeID)
		} else {
			fmt.Println("[WARN] Supabase not configured — pooled mode requires Supabase")
		}
	}

	// Graceful shutdown: catch SIGTERM/SIGINT, stop all recording
	// channels first (so their Cleanup() runs and queues files), then
	// wait for post-processing + uploads + Supabase
	// saves to finish before exiting.  A progress ticker logs every
	// 30 s so the GitHub Actions log shows the process is still alive.
	go func() {
		sigCh := make(chan os.Signal, 2)
		signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
		sig := <-sigCh

		channels := server.Manager.ChannelInfo()
		fmt.Printf("\n[SHUTDOWN] received %v - stopping %d channel(s)...\n", sig, len(channels))
		for _, ch := range channels {
			fmt.Printf("[SHUTDOWN]   stopping %s\n", ch.Username)
		}

		// Listen for a second Ctrl+C to force exit immediately
		go func() {
			<-sigCh
			fmt.Println("\n[SHUTDOWN] received second interrupt - forcing immediate exit")
			os.Exit(1)
		}()

		// In pooled mode: start draining so other nodes stop assigning to us
		if coord != nil {
			coord.StartDraining()
		}

		// Permanently stop the session loop so it can't restart a recording
		// cycle while we're tearing down (node-3 session-system parity).
		server.Manager.StopSession()
		server.Manager.StopAllChannels()
		server.Manager.StopWatcher()
		fmt.Println("[SHUTDOWN] all channels stopped - waiting for mux/thumbnail/upload/Supabase to finish...")

		shutdownDone := make(chan struct{})
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			elapsed := 30
			for {
				select {
				case <-ticker.C:
					fmt.Printf("[SHUTDOWN] still finalizing... (%ds elapsed)\n", elapsed)
					elapsed += 30
				case <-shutdownDone:
					return
				}
			}
		}()

		done := make(chan struct{}, 1)
		go func() {
			server.Manager.WaitForAllChannels()
			fmt.Println("[SHUTDOWN] all recordings finalized - waiting for uploads and Supabase saves...")
			server.Manager.WaitForUploads()

			// In pooled mode: release channels after all uploads complete
			if coord != nil {
				fmt.Println("[SHUTDOWN] releasing channel assignments...")
				coord.Stop()
			}

			close(shutdownDone)
			fmt.Println("[SHUTDOWN] all uploads and Supabase saves complete - exiting cleanly")
			close(diskMonitorStop)
			if c, ok := tunnelCancel.Load().(context.CancelFunc); ok && c != nil {
				c()
			}
			done <- struct{}{}
		}()

		select {
		case <-done:
			os.Exit(0)
		case <-time.After(55 * time.Minute):
			fmt.Println("[SHUTDOWN] timeout (55 min) - forcing exit")
			os.Exit(1)
		}
	}()

	// init web interface if username is not provided
	if server.Config.Username == "" {
		fmt.Printf("👋 Visit http://localhost:%s to use the Web UI\n", c.String("port"))
		if !c.Bool("no-tunnel") {
			go startTunnel(c.String("port"))
		} else {
			fmt.Println("🚇 Tunnel disabled (--no-tunnel) — script will manage it separately")
		}

		loadT := time.Now()
		if server.IsPooledMode() {
			if mgr == nil {
				return fmt.Errorf("pooled mode requires manager (unexpected nil)")
			}
			if err := mgr.LoadPooledConfig(); err != nil {
				return fmt.Errorf("load pooled config: %w", err)
			}
			if coord != nil {
				coord.Start(context.Background())
			}
		} else {
			if err := server.Manager.LoadConfig(); err != nil {
				return fmt.Errorf("load config: %w", err)
			}
		}
		fmt.Printf("[startup] LoadConfig completed in %v\n", time.Since(loadT).Round(time.Millisecond))

		server.Manager.StartSession(server.Config.SessionDurationParsed)

		// Start background disk monitor
		go server.StartDiskMonitor(diskMonitorStop)

		bindT := time.Now()
		err := router.SetupRouter().Run(":" + c.String("port"))
		fmt.Printf("[startup] HTTP server listened for %v before returning\n", time.Since(bindT).Round(time.Millisecond))
		return err
	}

	// else create a channel with the provided username
	channel.CleanupOrphanedFiles()
	go server.StartDiskMonitor(diskMonitorStop)

	if err := server.Manager.CreateChannel(&entity.ChannelConfig{
		Site:                    c.String("site"),
		Username:                c.String("username"),
		Framerate:               c.Int("framerate"),
		Resolution:              c.Int("resolution"),
		Pattern:                 c.String("pattern"),
		MaxDuration:             c.Int("max-duration"),
		MaxFilesize:             c.Int("max-filesize"),
		Compress:                c.Bool("compress"),
		MinDurationBeforeUpload: c.Int("min-duration-before-upload"),
	}, false); err != nil {
		return fmt.Errorf("create channel: %w", err)
	}

	server.Manager.StartSession(server.Config.SessionDurationParsed)

	// block forever
	select {}
}

// liveChecker implements coordinator.LivenessChecker using the site adapters.
type liveChecker struct{}

func (l *liveChecker) CheckLive(ctx context.Context, siteName, username string) coordinator.LivenessResult {
	// Tier 0: Affiliate API (fastest, single cached call covers all channels).
	// The onlinerooms endpoint is served on the cb.xxx domain this deployment
	// uses. A model CONFIRMED live here skips the per-channel check entirely.
	if server.Config != nil && server.Config.AffiliateWM != "" {
		affiliateLive, _, err := internal.CheckAffiliateLive(ctx, server.Config.AffiliateWM, server.Config.Domain, username)
		if err == nil && affiliateLive {
			return coordinator.LivenessLive
		}
	}

	// Absence from the affiliate list is NOT proof of offline (the cached list
	// can be stale or miss a just-online model), so fall through to the
	// per-channel room check.
	var siteImpl site.Site
	switch siteName {
	case "stripchat":
		siteImpl = site.NewStripchatSite()
	default:
		siteImpl = site.NewChaturbateSite()
	}

	status, err := siteImpl.GetRoomStatus(ctx, internal.NewReq(), username)
	if err != nil {
		// A failed probe is UNKNOWN, never offline: a transient error (rate
		// limit, Cloudflare block, timeout) must not stop a live recording.
		return coordinator.LivenessUnknown
	}

	switch status {
	case site.StatusPublic, site.StatusPrivate, site.StatusHidden:
		// Treat hidden (limitcam) as live — the model is streaming.
		return coordinator.LivenessLive
	case site.StatusOffline, site.StatusAway:
		return coordinator.LivenessOffline
	}
	// Empty or unrecognized status (e.g. room not found, stripchat's "unknown")
	// is ambiguous — never a reason to stop recording.
	return coordinator.LivenessUnknown
}

func startTunnel(port string) {
	cloudflaredPath, err := exec.LookPath("cloudflared")
	if err != nil {
		// Check for a cached cloudflared binary.
		if p := config.CachedCloudflaredBin(); p != "" {
			cloudflaredPath = p
		} else {
			fmt.Println("💡 Install cloudflared (winget install Cloudflare.cloudflared) for a public tunnel URL")
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	tunnelCancel.Store(cancel)

	cmd := exec.CommandContext(ctx, cloudflaredPath, "tunnel", "--url", "http://localhost:"+port, "--protocol", "http2")

	stderr, err := cmd.StderrPipe()
	if err != nil {
		fmt.Printf("⚠️  tunnel pipe: %v\n", err)
		return
	}

	if err := cmd.Start(); err != nil {
		fmt.Printf("⚠️  tunnel: %v\n", err)
		tunnelCancel.Store(context.CancelFunc(nil))
		return
	}

	tunnelURLCh := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stderr)
		re := regexp.MustCompile(`https://[a-zA-Z0-9-]+\.trycloudflare\.com`)
		for scanner.Scan() {
			if m := re.FindString(scanner.Text()); m != "" {
				tunnelURLCh <- m
				return
			}
		}
	}()

	select {
	case tunnelURL := <-tunnelURLCh:
		fmt.Printf("🌍 Public: %s\n\n", tunnelURL)
	case <-time.After(30 * time.Second):
		fmt.Println("⚠️  Tunnel URL not obtained within 30s")
	}

	cmd.Wait()
}
