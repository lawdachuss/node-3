package config

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/urfave/cli/v2"
)

// splitCS splits a comma-separated string, trimming whitespace and
// discarding empty entries. Used for multi-key config values.
func splitCS(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

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
	return "ffmpeg"
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

// ffmpegAcquireTimeout bounds how long a caller waits for a lightweight ffmpeg
// slot. The old code blocked forever on the channel send, so a saturated
// semaphore (e.g. a burst of concurrent preview generations) would hang the
// entire thumbnail/preview pipeline indefinitely — the "shows a little preview
// then got stuck" symptom. We now time out, over-subscribe deliberately, and
// pair the release via ffmpegOverflow so ReleaseFFmpeg never deadlocks.
const ffmpegAcquireTimeout = 2 * time.Minute

// ffmpegOverflow counts callers that timed out waiting for a slot and therefore
// never actually took one. ReleaseFFmpeg decrements this instead of pulling from
// the channel, keeping acquire/release balanced without a per-goroutine token.
var ffmpegOverflow int64

// AcquireFFmpeg waits (bounded) for a lightweight ffmpeg slot.
func AcquireFFmpeg() {
	select {
	case ffmpegSem <- struct{}{}:
	case <-time.After(ffmpegAcquireTimeout):
		atomic.AddInt64(&ffmpegOverflow, 1)
		log.Printf("WARN [config] ffmpeg semaphore saturated for %s; proceeding without a slot (oversubscribed)", ffmpegAcquireTimeout)
	}
}

// ReleaseFFmpeg releases a lightweight ffmpeg slot.
func ReleaseFFmpeg() {
	if atomic.LoadInt64(&ffmpegOverflow) > 0 {
		atomic.AddInt64(&ffmpegOverflow, -1)
		return
	}
	<-ffmpegSem
}

// AcquireFFmpegHeavy waits (bounded) for a CPU-bound compression slot.
func AcquireFFmpegHeavy() {
	select {
	case ffmpegHeavySem <- struct{}{}:
	case <-time.After(ffmpegAcquireTimeout):
		atomic.AddInt64(&ffmpegOverflow, 1)
		log.Printf("WARN [config] ffmpeg-heavy semaphore saturated for %s; proceeding without a slot (oversubscribed)", ffmpegAcquireTimeout)
	}
}

// ReleaseFFmpegHeavy releases a CPU-bound compression slot.
func ReleaseFFmpegHeavy() {
	if atomic.LoadInt64(&ffmpegOverflow) > 0 {
		atomic.AddInt64(&ffmpegOverflow, -1)
		return
	}
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
		Cookies:                 entity.SanitizeCookieString(c.String("cookies")),
		UserAgent:               c.String("user-agent"),
		Domain:                  c.String("domain"),
		ProxyURL:                c.String("proxy-url"),
		ProxyUsername:           c.String("proxy-username"),
		ProxyPassword:           c.String("proxy-password"),
		OutputDir:               c.String("output-dir"),
		PerModelFolder:          c.Bool("per-model-folder"),
		DeleteLocalAfterUpload:  c.Bool("delete-local-after-upload"),
		OrphanCleanupInterval:   c.Int("orphan-cleanup-interval"),
		DiskWarningPercent:      c.Int("disk-warning-percent"),
		DiskCriticalPercent:     c.Int("disk-critical-percent"),
		MaxLocalAgeDays:         c.Int("max-local-age-days"),
		MinDurationBeforeUpload: c.Int("min-duration-before-upload"),
		VoeSXAPIKey:             c.String("voesx-api-key"),
		StreamtapeLogin:         c.String("streamtape-login"),
		StreamtapeKey:           c.String("streamtape-key"),
		MixdropEmail:            c.String("mixdrop-email"),
		MixdropToken:            c.String("mixdrop-token"),
		SeekStreamingKey:        c.String("seekstreaming-key"),
		VidHideAPIKeys:          splitCS(c.String("vidhide-api-key")),
		StreamWishAPIKeys:       splitCS(c.String("streamwish-api-key")),
		UpnshareKeys:            splitCS(c.String("upnshare-key")),

		SupabaseURL:    c.String("supabase-url"),
		SupabaseAPIKey: c.String("supabase-api-key"),
		StripchatPDKey: c.String("stripchat-pdkey"),
	}

	// If user provided a custom ffmpeg path, set it globally
	if path := c.String("ffmpeg-path"); path != "" {
		cfg.FFmpegPath = path
		SetFFmpegPath(path)
	}

	sessionDuration := c.String("session-duration")
	// When SESSION_DURATION is not set, leave as 0 for continuous recording.
	// The flag default is "".  Only parse when a non-empty, non-zero value is given.
	if sessionDuration != "" && sessionDuration != "0" {
		parsed, err := time.ParseDuration(sessionDuration)
		if err != nil {
			return nil, fmt.Errorf("parse session-duration %q: %w", sessionDuration, err)
		}
		cfg.SessionDuration = sessionDuration
		cfg.SessionDurationParsed = parsed
	}

	return cfg, nil
}
