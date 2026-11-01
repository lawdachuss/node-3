package config

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"time"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/urfave/cli/v2"
)

var (
	ffmpegPath       string
	autoDetectedFF   string
	autoDetectedOnce sync.Once

	// ffmpegSem limits concurrent lightweight ffmpeg/ffprobe processes
	// across all channels: thumbnails, sprite sheets, GIF previews,
	// and A/V muxing. These are I/O-bound and fast, so the pool is
	// generous: NumCPU * 3, minimum 4.
	ffmpegSem chan struct{}

	// ffmpegHeavySem limits concurrent CPU-bound compression (re-encode)
	// across all channels. Only one file per channel is compressed at a
	// time (CompressFile serialises internally), but across N channels
	// we risk thrashing the CPU.  Pool: max(1, NumCPU/2), capped at 4.
	ffmpegHeavySem chan struct{}
)

func init() {
	n := runtime.NumCPU()
	light := n * 3
	if light < 4 {
		light = 4
	}
	ffmpegSem = make(chan struct{}, light)

	heavy := n / 2
	if heavy < 1 {
		heavy = 1
	}
	if heavy > 4 {
		heavy = 4
	}
	ffmpegHeavySem = make(chan struct{}, heavy)
}

// SetFFmpegPath sets a custom path for the ffmpeg binary.
func SetFFmpegPath(path string) {
	ffmpegPath = path
}

// autoDetectFFmpeg searches common ffmpeg install locations when PATH lookup
// fails. Runs once and caches the result.
func autoDetectFFmpeg() string {
	autoDetectedOnce.Do(func() {
		// Try PATH lookup first.
		if p, err := exec.LookPath("ffmpeg"); err == nil {
			autoDetectedFF = p
			return
		}

		candidates := []string{
			// WinGet shim directory
		}

		localAppData := os.Getenv("LOCALAPPDATA")
		if localAppData != "" {
			candidates = append(candidates,
				filepath.Join(localAppData, "Microsoft", "WinGet", "Links", "ffmpeg.exe"),
			)
			// WinGet packages directory with version glob
			wgDir := filepath.Join(localAppData, "Microsoft", "WinGet", "Packages")
			if entries, err := os.ReadDir(wgDir); err == nil {
				for _, e := range entries {
					if matched, _ := filepath.Match("Gyan.FFmpeg.Essentials*", e.Name()); matched {
						candidates = append(candidates,
							filepath.Join(wgDir, e.Name(), "bin", "ffmpeg.exe"),
						)
					}
				}
			}
		}

		candidates = append(candidates,
			`C:\ProgramData\chocolatey\bin\ffmpeg.exe`,
			`C:\Program Files\FFmpeg\bin\ffmpeg.exe`,
			`C:\Program Files\ffmpeg\bin\ffmpeg.exe`,
		)

		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				autoDetectedFF = c
				return
			}
		}
	})
	return autoDetectedFF
}

// ffmpegBin returns the configured ffmpeg path, auto-detected path, or
// "ffmpeg" as final fallback (which relies on PATH lookup by exec.Command).
func ffmpegBin() string {
	if ffmpegPath != "" {
		if _, err := os.Stat(ffmpegPath); err == nil {
			return ffmpegPath
		}
	}
	if p := autoDetectFFmpeg(); p != "" {
		return p
	}
	if p := cachedFFmpegBin(); p != "" {
		return p
	}
	return "ffmpeg"
}

// cacheDir returns the local cache directory for downloaded binaries.
func cacheDir() string {
	dir := filepath.Join(os.Getenv("LOCALAPPDATA"), "chaturbate-dvr", "bin")
	os.MkdirAll(dir, 0o755)
	return dir
}

// cachedFFmpegBin returns the path to a cached FFmpeg static build, or ""
// if none is cached.
func cachedFFmpegBin() string {
	candidates := []string{
		filepath.Join(cacheDir(), "ffmpeg.exe"),
		filepath.Join(cacheDir(), "ffmpeg"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

// EnsureFFmpegCached downloads a static FFmpeg build to the local cache
// directory if no cached binary is found.  Returns the path to the cached
// binary or "" if the download fails.
func EnsureFFmpegCached() string {
	if p := cachedFFmpegBin(); p != "" {
		return p
	}

	if runtime.GOOS != "windows" {
		return ""
	}

	url := "https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-win64-gpl.zip"
	zipPath := filepath.Join(cacheDir(), "ffmpeg-static.zip")
	extractDir := filepath.Join(cacheDir(), "ffmpeg-extract")

	fmt.Println("[FFMPEG] Downloading static build to cache...")
	if err := downloadFile(url, zipPath); err != nil {
		fmt.Printf("[FFMPEG] Download failed: %v\n", err)
		return ""
	}

	fmt.Println("[FFMPEG] Extracting...")
	if err := extractZip(zipPath, extractDir); err != nil {
		fmt.Printf("[FFMPEG] Extract failed: %v\n", err)
		return ""
	}

	// Find the ffmpeg.exe inside the extracted directory.
	var ffmpegExe string
	filepath.WalkDir(extractDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if !d.IsDir() && strings.EqualFold(d.Name(), "ffmpeg.exe") {
			ffmpegExe = path
			return io.EOF // stop walking
		}
		return nil
	})

	if ffmpegExe == "" {
		fmt.Println("[FFMPEG] ffmpeg.exe not found in archive")
		return ""
	}

	dest := filepath.Join(cacheDir(), "ffmpeg.exe")
	if err := os.Rename(ffmpegExe, dest); err != nil {
		// Copy instead of rename if across volumes
		if copyErr := copyFile(ffmpegExe, dest); copyErr != nil {
			fmt.Printf("[FFMPEG] Could not cache binary: %v\n", copyErr)
			return ""
		}
	}

	// Clean up the zip and extract dir.
	os.Remove(zipPath)
	os.RemoveAll(extractDir)

	fmt.Printf("[FFMPEG] Cached to %s\n", dest)
	return dest
}

// CachedCloudflaredBin returns the path to a cached cloudflared binary, or ""
// if none is cached.
func CachedCloudflaredBin() string {
	return cachedCloudflaredBin()
}

// cachedCloudflaredBin returns the path to a cached cloudflared binary, or ""
// if none is cached.
func cachedCloudflaredBin() string {
	candidates := []string{
		filepath.Join(cacheDir(), "cloudflared.exe"),
		filepath.Join(cacheDir(), "cloudflared"),
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	return ""
}

// EnsureCloudflaredCached downloads cloudflared to the local cache directory
// if no cached binary is found.  Returns the path to the cached binary or ""
// if the download fails.
func EnsureCloudflaredCached() string {
	if p := cachedCloudflaredBin(); p != "" {
		return p
	}

	url := "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
	dest := filepath.Join(cacheDir(), "cloudflared.exe")

	fmt.Println("[CLOUDFLARED] Downloading to cache...")
	if err := downloadFile(url, dest); err != nil {
		fmt.Printf("[CLOUDFLARED] Download failed: %v\n", err)
		return ""
	}

	fmt.Printf("[CLOUDFLARED] Cached to %s\n", dest)
	return dest
}

// downloadFile downloads a file from url to dst.
func downloadFile(url, dst string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}

	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	return err
}

// copyFile copies src to dst.
func copyFile(src, dst string) error {
	from, err := os.Open(src)
	if err != nil {
		return err
	}
	defer from.Close()

	to, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	return err
}

// extractZip extracts a zip file to dst.
func extractZip(zipPath, dst string) error {
	r, err := os.Open(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	fi, err := r.Stat()
	if err != nil {
		return err
	}
	z, err := zip.NewReader(r, fi.Size())
	if err != nil {
		return err
	}

	for _, f := range z.File {
		target := filepath.Join(dst, f.Name)
		if f.FileInfo().IsDir() {
			os.MkdirAll(target, 0o755)
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		w, err := os.Create(target)
		if err != nil {
			rc.Close()
			return err
		}
		_, err = io.Copy(w, rc)
		rc.Close()
		w.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// ffprobeBin returns a working ffprobe path, trying in order:
//  1. Same directory as the configured ffmpeg path
//  2. PATH lookup via LookPath
//  3. Same directory as the auto-detected ffmpeg
//  4. Bare name ("ffprobe"/"ffprobe.exe") as final fallback
func ffprobeBin() string {
	probeName := "ffprobe"
	if runtime.GOOS == "windows" {
		probeName = "ffprobe.exe"
	}

	if ffmpegPath != "" {
		if _, err := os.Stat(ffmpegPath); err == nil {
			dir := filepath.Dir(ffmpegPath)
			p := filepath.Join(dir, probeName)
			if _, err := os.Stat(p); err == nil {
				return p
			}
		}
	}

	if p, err := exec.LookPath(probeName); err == nil {
		return p
	}

	if p := autoDetectFFmpeg(); p != "" {
		dir := filepath.Dir(p)
		probePath := filepath.Join(dir, probeName)
		if _, err := os.Stat(probePath); err == nil {
			return probePath
		}
	}

	return probeName
}

// FFmpegCommand returns an exec.Cmd that runs ffmpeg with the given arguments.
func FFmpegCommand(args ...string) *exec.Cmd {
	return exec.Command(ffmpegBin(), args...)
}

// FFmpegCommandContext is like FFmpegCommand but with a context.
func FFmpegCommandContext(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, ffmpegBin(), args...)
}

// FFprobeCommand returns an exec.Cmd that runs ffprobe with the given arguments.
func FFprobeCommand(args ...string) *exec.Cmd {
	return exec.Command(ffprobeBin(), args...)
}

// FFprobeCommandContext is like FFprobeCommand but with a context.
func FFprobeCommandContext(ctx context.Context, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, ffprobeBin(), args...)
}

// FFmpegAcquireTimeout bounds how long a caller waits for a free lightweight
// ffmpeg slot before giving up on the operation.  Without a bound, an acquire
// on a saturated semaphore (or against a wedged holder) blocks forever: the
// per-command context timeout never starts because the process never launches,
// and every pipeline waiting on that thumbnail stalls at "uploaded X/6 hosts
// — processing" for the full pipelineThumbnailTimeout while files pile up on
// disk.  The bound is generous enough to ride out brief contention waves but
// short enough that a genuinely starved pool degrades fast (skip the
// thumbnail, backfill later) instead of hanging the fleet.
const FFmpegAcquireTimeout = 5 * time.Minute

// FFmpegHeavyAcquireTimeout bounds how long a caller waits for a free
// CPU-bound compression slot.  Same rationale as FFmpegAcquireTimeout.
// 15m (not the light pool's 5m): the heavy pool is only max(1, NumCPU/2)
// slots shared by transcode finalize, compress, min-duration merges and
// session merges — one large transcode legitimately holds a slot for tens
// of minutes, so a 5m budget produced "could not acquire ffmpeg slot:
// context deadline exceeded" and dropped merges to fragmented individual
// uploads.  All heavy callers are async goroutines and the wait is bounded,
// so the longer ride-out cannot wedge a pipeline; failing the merge never
// loses content (both inputs are kept), it only fragments it.
const FFmpegHeavyAcquireTimeout = 15 * time.Minute

// AcquireFFmpeg acquires a lightweight ffmpeg slot, blocking until one is
// available or ctx is done.  It returns nil on acquisition and ctx.Err()
// when the caller's wait budget expires first, so a saturated pool fails
// fast instead of hanging the pipeline.
func AcquireFFmpeg(ctx context.Context) error {
	select {
	case ffmpegSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AcquireFFmpegFor blocks up to timeout for a lightweight ffmpeg slot.
func AcquireFFmpegFor(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return AcquireFFmpeg(ctx)
}

// ReleaseFFmpeg releases a lightweight ffmpeg slot.
func ReleaseFFmpeg() {
	<-ffmpegSem
}

// AcquireFFmpegHeavy acquires a CPU-bound compression slot, blocking until one
// is available or ctx is done.  Returns nil on acquisition and ctx.Err() when
// the caller's wait budget expires first.
func AcquireFFmpegHeavy(ctx context.Context) error {
	select {
	case ffmpegHeavySem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// AcquireFFmpegHeavyFor blocks up to timeout for a CPU-bound compression slot.
func AcquireFFmpegHeavyFor(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return AcquireFFmpegHeavy(ctx)
}

// ReleaseFFmpegHeavy releases a CPU-bound compression slot.
func ReleaseFFmpegHeavy() {
	<-ffmpegHeavySem
}

func New(c *cli.Context) (*entity.Config, error) {
	compress := c.Bool("compress")

	cfg := &entity.Config{
		Version:                 c.App.Version,
		Username:                c.String("username"),
		AdminUsername:           c.String("admin-username"),
		AdminPassword:           c.String("admin-password"),
		Framerate:               c.Int("framerate"),
		Resolution:              c.Int("resolution"),
		Pattern:                 c.String("pattern"),
		MaxDuration:             c.Int("max-duration"),
		MaxFilesize:             c.Int("max-filesize"),
		Compress:                compress,
		Port:                    c.String("port"),
		Interval:                c.Int("interval"),
		Cookies:                 c.String("cookies"),
		UserAgent:               c.String("user-agent"),
		Domain:                  c.String("domain"),
		OutputDir:               c.String("output-dir"),
		PerModelFolder:          c.Bool("per-model-folder"),
		DeleteLocalAfterUpload:  c.Bool("delete-local-after-upload"),
		OrphanCleanupInterval:   c.Int("orphan-cleanup-interval"),
		LogRetentionDays:        c.Int("log-retention-days"),
		DiskWarningPercent:      c.Int("disk-warning-percent"),
		DiskCriticalPercent:     c.Int("disk-critical-percent"),
		MaxLocalAgeDays:         c.Int("max-local-age-days"),
		MinDurationBeforeUpload: c.Int("min-duration-before-upload"),
		MinFileSizeBeforeUpload: c.Int("min-file-size-before-upload"),
		VoeSXAPIKey:             c.String("voesx-api-key"),
		StreamtapeLogin:         c.String("streamtape-login"),
		StreamtapeKey:           c.String("streamtape-key"),
		MixdropEmail:            c.String("mixdrop-email"),
		MixdropToken:            c.String("mixdrop-token"),
		VidaraKey:               c.String("vidara-key"),
		DisabledUploadHosts:     splitCommaList(c.String("disabled-upload-hosts")),
		AnonMP4ViewBase:         strings.TrimSpace(c.String("anonmp4-view-base")),
		CatboxProxyURL:          c.String("catbox-proxy-url"),
		UploadMaxConcurrent:     c.Int("upload-max-concurrent"),
		UploadHostConcurrency:   c.Int("upload-host-concurrency"),
		PipelineWorkers:         c.Int("pipeline-workers"),
		RetryWorkers:            c.Int("retry-workers"),

		SupabaseURL:            c.String("supabase-url"),
		SupabaseAPIKey:         c.String("supabase-api-key"),
		SupabaseServiceRoleKey: c.String("supabase-service-role-key"),
		StripchatPDKey:         c.String("stripchat-pdkey"),
		AffiliateWM:            c.String("affiliate-wm"),

		CompletedDir:          c.String("completed-dir"),
		FinalizeMode:          entity.NormalizeFinalizeMode(c.String("finalize-mode")),
		FFmpegEncoder:         c.String("ffmpeg-encoder"),
		FFmpegContainer:       c.String("ffmpeg-container"),
		FFmpegQuality:         c.Int("ffmpeg-quality"),
		FFmpegPreset:          c.String("ffmpeg-preset"),
		Debug:                 c.Bool("debug"),
		NtfyURL:               c.String("ntfy-url"),
		NtfyTopic:             c.String("ntfy-topic"),
		NtfyToken:             c.String("ntfy-token"),
		DiscordWebhookURL:     c.String("discord-webhook-url"),
		CFChannelThreshold:    c.Int("cf-channel-threshold"),
		CFGlobalThreshold:     c.Int("cf-global-threshold"),
		CFRetryMinutes:        c.Int("cf-retry-minutes"),
		CFStarvedThreshold:    c.Int("cf-starved-threshold"),
		CFSessionCutThreshold: c.Int("cf-session-cut-threshold"),
		CFRefreshMin:          c.Int("cf-refresh-min"),
		NotifyCooldownHours:   c.Int("notify-cooldown-hours"),
		NotifyStreamOnline:    c.Bool("notify-stream-online"),
		DisableEarlyDrain:     c.Bool("disable-early-drain"),
	}

	// If user provided a custom ffmpeg path, set it globally
	if path := c.String("ffmpeg-path"); path != "" {
		cfg.FFmpegPath = path
		SetFFmpegPath(path)
	}

	sessionDuration := strings.TrimSpace(c.String("session-duration"))
	// When SESSION_DURATION is not set, leave as 0 for continuous recording.
	// The flag default is "".  Only parse when a non-empty, non-zero value is given.
	// (Node-3 session-system parity.)
	if sessionDuration != "" && sessionDuration != "0" {
		parsed, err := time.ParseDuration(sessionDuration)
		if err != nil {
			// A bad SESSION_DURATION (e.g. trailing newline in a GitHub secret)
			// must never kill the recorder. Warn and continue with continuous
			// recording (0) instead of crashing the node.
			fmt.Printf("⚠️  Invalid session-duration %q: %v — running continuous (no session stop)\n", sessionDuration, err)
		} else {
			cfg.SessionDuration = sessionDuration
			cfg.SessionDurationParsed = parsed
		}
	}

	return cfg, nil
}

// splitCommaList splits a comma-separated string into trimmed, non-empty
// entries (e.g. DISABLED_UPLOAD_HOSTS "AnonMP4,VOE.sx" -> [AnonMP4, VOE.sx]).
func splitCommaList(s string) []string {
	if s == "" {
		return nil
	}
	var out []string
	for _, part := range strings.Split(s, ",") {
		if v := strings.TrimSpace(part); v != "" {
			out = append(out, v)
		}
	}
	return out
}
