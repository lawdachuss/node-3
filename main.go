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
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/teacat/chaturbate-dvr/channel"
	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/manager"
	"github.com/teacat/chaturbate-dvr/router"
	"github.com/teacat/chaturbate-dvr/server"
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

func main() {
	loadDotEnv(".env")
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
				Value: 60,
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
				Name:  "domain",
				Usage: "Chaturbate domain to use",
				Value: "https://chaturbate.com/",
			},
			&cli.StringFlag{
				Name:    "proxy-url",
				Usage:   "HTTP/SOCKS5 proxy URL for Chaturbate requests",
				EnvVars: []string{"PROXY_URL", "PROXY_SERVER", "ALL_PROXY", "all_proxy", "SOCKS_PROXY"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "proxy-username",
				Usage:   "Proxy username",
				EnvVars: []string{"PROXY_USERNAME"},
				Value:   "",
			},
			&cli.StringFlag{
				Name:    "proxy-password",
				Usage:   "Proxy password",
				EnvVars: []string{"PROXY_PASSWORD"},
				Value:   "",
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
				Value:   0,
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
				Name:    "pixeldrain-token",
				Usage:   "API token for PixelDrain uploads",
				EnvVars: []string{"PIXELDRAIN_TOKEN", "PIXELDRAIN_API_KEY"},
				Value:   "",
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
				Name:    "stripchat-pdkey",
				Usage:   "MOUFLON v2 decryption key for Stripchat HLS streams",
				EnvVars: []string{"STRIPCHAT_PDKEY"},
				Value:   "",
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
	fmt.Println(logo)

	var err error
	server.Config, err = config.New(c)
	if err != nil {
		return fmt.Errorf("new config: %w", err)
	}
	fmt.Printf("[startup] config loaded in %v\n", time.Since(started).Round(time.Millisecond))

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
	}

	if server.Config.Cookies == "" || server.Config.UserAgent == "" {
		fmt.Println("⚠️  Chaturbate API requests may fail — COOKIES and USER_AGENT not set in .env or Supabase")
		fmt.Println("   Open .env and fill in your browser cookies from chaturbate.com:")
		fmt.Println("   Chrome 146: F12 → Application → Cookies → chaturbate.com → copy all as string")
		fmt.Println("   OR update cookies in Supabase via the web UI")
		fmt.Println("   IMPORTANT: Use Chrome 146+ on Windows for cookie collection so the TLS")
		fmt.Println("   fingerprint matches the httpcloak preset.")
		fmt.Println()
	}

	// Warm up TLS sessions with Cloudflare in the background so server
	// startup is not delayed by slow/unreachable SOCKS5 proxies.
	// The httpcloak pool-level dial doesn't propagate the context deadline
	// during the SOCKS5 handshake, causing it to block for the OS TCP
	// timeout (~30-120s) instead of the desired 10s context deadline.
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
		if err := server.Manager.LoadConfig(); err != nil {
			return fmt.Errorf("load config: %w", err)
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

func startTunnel(port string) {
	cloudflaredPath, err := exec.LookPath("cloudflared")
	if err != nil {
		fmt.Println("💡 Install cloudflared (winget install Cloudflare.cloudflared) for a public tunnel URL")
		return
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
