// Command backfillst regenerates missing static thumbnails for recordings
// whose local video is gone but whose file still exists on Streamtape.
//
// Streamtape's official API (api.streamtape.com) provides download tickets
// (file/dlticket) and direct download links (file/dl) that support HTTP Range
// requests. Cam recordings are remuxed with the moov atom near the front, so
// downloading only the first few MB is enough to seek to an early frame and
// render a thumbnail — no need to pull the whole (sometimes 700MB+) file.
//
// The Streamtape partial download + frame extraction lives in the shared
// github.com/teacat/chaturbate-dvr/recovery package (the same logic used by the
// automated thumbnail sweep on the fleet). This command is the standalone,
// manifest-driven front end for on-demand recovery.
//
// For each manifest entry it:
//  1. delegates the head Range grab + (moov-at-end) tail fallback + frame
//     render to recovery.StreamtapeThumb,
//  2. uploads through the standard MultiImageUploader
//     (Catbox -> Pixhost -> freeimage.host fallback),
//  3. PATCHes thumbnail_url onto the recordings row AND the preview_images row.
//
// Usage:
//
//	go run ./cmd/backfillst [-partial 10485760] [-seek 2] backfill_manifest.json
//
// Manifest format (JSON array):
//
//	[{"filename": "user_2026-08-14_18-23-39.mp4", "filecode": "P3zVwykJMlF0723"}]
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/recovery"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

type manifestEntry struct {
	Filename  string `json:"filename"`
	Filecode  string `json:"filecode"`
	PartialMB int    `json:"partial_mb,omitempty"` // per-entry override (MB)
}

func main() {
	partialMB := flag.Int("partial", 4, "MB to download from the start of each file")
	seek := flag.Float64("seek", 2, "seconds into the video to grab the thumbnail frame")
	tailMB := flag.Int("tail", 0, "MB to grab from the end as fallback for moov-at-end mp4s (0 = off)")
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("usage: go run ./cmd/backfillst [-partial 10] [-seek 2] <manifest.json>")
	}

	loadDotEnv(".env")
	cfg := configFromEnv()
	if cfg.SupabaseURL == "" || cfg.SupabaseAPIKey == "" {
		log.Fatal("SUPABASE_URL / SUPABASE_API_KEY not set")
	}
	if cfg.SupabaseServiceRoleKey == "" {
		log.Fatal("SUPABASE_SERVICE_ROLE_KEY not set (required for preview_images writes)")
	}
	if cfg.StreamtapeLogin == "" || cfg.StreamtapeKey == "" {
		log.Fatal("STREAMTAPE_LOGIN / STREAMTAPE_API_KEY not set")
	}
	server.Config = cfg
	server.SyncNodeEnvironment()

	data, err := os.ReadFile(flag.Arg(0))
	if err != nil {
		log.Fatalf("read manifest: %v", err)
	}
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF}) // strip UTF-8 BOM
	var entries []manifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Fatalf("parse manifest: %v", err)
	}
	if len(entries) == 0 {
		log.Fatal("manifest is empty")
	}
	log.Printf("backfilling %d thumbnails from Streamtape (partial=%dMB seek=%gs)", len(entries), *partialMB, *seek)

	dir, err := os.MkdirTemp("", "backfillst")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	okCount := 0
	for i, e := range entries {
		mb := *partialMB
		if e.PartialMB > 0 {
			mb = e.PartialMB
		}
		log.Printf("[%d/%d] %s (filecode %s, partial %dMB)", i+1, len(entries), e.Filename, e.Filecode, mb)
		start := time.Now()
		thumbURL, err := backfillOne(e, mb, *seek, *tailMB, dir)
		if err != nil {
			log.Printf("  FAIL after %s: %v", time.Since(start).Round(time.Second), err)
			continue
		}
		log.Printf("  OK in %s: %s", time.Since(start).Round(time.Second), thumbURL)
		okCount++
		time.Sleep(2 * time.Second) // pace image-host uploads
	}
	log.Printf("done: %d/%d recordings backfilled", okCount, len(entries))
}

// backfillOne handles a single recording: recover a thumbnail via the shared
// Streamtape routine, upload it, and patch both DB tables.
func backfillOne(e manifestEntry, partialMB int, seek float64, tailMB int, workDir string) (string, error) {
	fn := e.Filename
	thumbPath, err := recovery.StreamtapeThumb(e.Filecode, workDir, server.Config.StreamtapeLogin, server.Config.StreamtapeKey, partialMB, seek, tailMB)
	if err != nil {
		return "", fmt.Errorf("recover thumbnail: %w", err)
	}

	// Upload through the standard image pipeline (all hosts in parallel).
	imgUploader := uploader.NewMultiImageUploader()
	thumbURLs := imgUploader.UploadToAllURLs(thumbPath, nil)
	var thumbURL string
	for _, host := range []string{"Catbox", "Pixhost", "freeimage.host"} {
		if url, ok := thumbURLs[host]; ok {
			thumbURL = url
			break
		}
	}
	if thumbURL == "" {
		return "", fmt.Errorf("upload thumbnail: all hosts failed")
	}

	// Patch both tables (these files have no sprite/preview to preserve).
	if err := server.UpdateRecordingThumbnails(fn, thumbURL, "", ""); err != nil {
		return thumbURL, fmt.Errorf("patch recordings row: %w", err)
	}
	if err := server.SavePreviewLinks(fn, thumbURL, "", "", nil, nil, nil); err != nil {
		return thumbURL, fmt.Errorf("patch preview_images row: %w", err)
	}
	return thumbURL, nil
}

// configFromEnv mirrors cmd/backfillthumbs' minimal config wiring.
func configFromEnv() *entity.Config {
	cfg := &entity.Config{
		SupabaseURL:            env("SUPABASE_URL"),
		SupabaseAPIKey:         env("SUPABASE_API_KEY"),
		SupabaseServiceRoleKey: env("SUPABASE_SERVICE_ROLE_KEY"),
		VoeSXAPIKey:            env("VOESX_API_KEY"),
		StreamtapeLogin:        env("STREAMTAPE_LOGIN"),
		StreamtapeKey:          or(env("STREAMTAPE_KEY"), env("STREAMTAPE_API_KEY")),
		MixdropEmail:           env("MIXDROP_EMAIL"),
		MixdropToken:           or(env("MIXDROP_TOKEN"), env("MIXDROP_KEY")),
		VidaraKey:              env("VIDARA_KEY"),
		Domain:                 or(env("DOMAIN"), "https://www.cb.xxx/"),
		FFmpegPath:             env("FFMPEG_PATH"),
	}
	if cfg.FFmpegPath != "" {
		config.SetFFmpegPath(cfg.FFmpegPath)
	}
	return cfg
}

func env(k string) string { return os.Getenv(k) }

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// loadDotEnv loads KEY=VALUE pairs from a .env file into the environment,
// without overwriting existing variables. Falls back to the executable dir.
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		if exe, e2 := os.Executable(); e2 == nil {
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
