// Command backfillthumbs regenerates the missing static thumbnail for
// recordings whose local video was already deleted (so ScanThumbnails can
// never reach them) but whose uploaded SPRITE SHEET still exists on the image
// host.  The sprite is a 4x4 grid of 640x360 frames sampled across the full
// video, so cropping one tile gives a real frame from the recording.
//
// For each manifest entry it:
//  1. downloads the sprite sheet (2560x1440),
//  2. crops the tile closest to the 10% mark (the same spot the normal
//     thumbnail generator seeks to),
//  3. scales + pads it to 1280x720,
//  4. uploads it through the standard MultiImageUploader
//     (Pixhost -> ImgBB -> Catbox fallback),
//  5. PATCHes thumbnail_url onto the recordings row AND the preview_images row
//     (preserving the existing sprite/preview URLs).
//
// Usage:
//
//	go run ./cmd/backfillthumbs backfill_manifest.json
//
// Manifest format (JSON array):
//
//	[{"filename": "user_2026-08-08_13-38-01.mp4",
//	  "sprite_url": "https://img2.pixhost.to/images/.../sprite.jpg",
//	  "preview_url": "https://files.catbox.moe/xxx.webp"}]
package main

import (
	"bufio"
	"context"
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
	"github.com/teacat/chaturbate-dvr/internal"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

const (
	thumbWidth  = 1280
	thumbHeight = 720
	// The sprite sheet layout produced by generateThumbnailForFile:
	// spriteCols x spriteRows tiles of spriteFrameW x spriteFrameH px.
	spriteCols   = 4
	spriteRows   = 4
	spriteFrameW = 640
	spriteFrameH = 360
)

type manifestEntry struct {
	Filename   string `json:"filename"`
	SpriteURL  string `json:"sprite_url"`
	PreviewURL string `json:"preview_url"`
}

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("usage: go run ./cmd/backfillthumbs <manifest.json>")
	}

	loadDotEnv(".env")
	cfg := configFromEnv()
	if cfg.SupabaseURL == "" || cfg.SupabaseAPIKey == "" {
		log.Fatal("SUPABASE_URL / SUPABASE_API_KEY not set")
	}
	server.Config = cfg
	server.SyncNodeEnvironment()

	data, err := os.ReadFile(flag.Arg(0))
	if err != nil {
		log.Fatalf("read manifest: %v", err)
	}
	var entries []manifestEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		log.Fatalf("parse manifest: %v", err)
	}
	if len(entries) == 0 {
		log.Fatal("manifest is empty")
	}
	log.Printf("backfilling thumbnails for %d recordings", len(entries))

	dir, err := os.MkdirTemp("", "backfillthumbs")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	okCount := 0
	for i, e := range entries {
		fn := e.Filename
		log.Printf("[%d/%d] %s", i+1, len(entries), fn)
		if e.SpriteURL == "" {
			log.Printf("  SKIP: no sprite_url for %s", fn)
			continue
		}
		thumbURL, err := backfillOne(e, dir)
		if err != nil {
			log.Printf("  FAIL: %v", err)
			continue
		}
		log.Printf("  OK: thumbnail -> %s", thumbURL)
		okCount++
		// Space out uploads so the image hosts' rate limits are not tripped
		// (the same pacing ScanThumbnails uses).
		time.Sleep(2 * time.Second)
	}
	log.Printf("done: %d/%d recordings backfilled", okCount, len(entries))
}

// backfillOne handles a single recording: download sprite, crop a frame,
// upload it, and patch both DB tables.  Returns the new thumbnail URL.
func backfillOne(e manifestEntry, workDir string) (string, error) {
	fn := e.Filename
	spritePath := filepath.Join(workDir, "sprite_"+sanitizeName(fn)+".jpg")
	thumbPath := filepath.Join(workDir, "thumb_"+sanitizeName(fn)+".jpg")

	// 1. Download the sprite sheet.
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	body, err := internal.NewReq().GetBytesWithTimeout(ctx, e.SpriteURL, 60*time.Second)
	if err != nil {
		return "", fmt.Errorf("download sprite: %w", err)
	}
	if len(body) < 10_000 {
		return "", fmt.Errorf("sprite too small (%d bytes) — likely a block page", len(body))
	}
	if err := os.WriteFile(spritePath, body, 0o666); err != nil {
		return "", fmt.Errorf("write sprite: %w", err)
	}

	// 2. Crop the tile nearest the 10% mark.  Frames are sampled evenly, so
	//    tile i covers i/16 of the video; 0.1*16 = 1.6 -> tile 1 (col 1, row 0).
	//    The scale+pad matches the generator's thumbnail vf exactly.
	x := spriteFrameW // col 1
	y := 0            // row 0
	args := []string{
		"-y", "-hide_banner", "-loglevel", "error",
		"-i", spritePath,
		"-vf", fmt.Sprintf(
			"crop=%d:%d:%d:%d,scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2",
			spriteFrameW, spriteFrameH, x, y,
			thumbWidth, thumbHeight, thumbWidth, thumbHeight),
		"-frames:v", "1",
		"-c:v", "mjpeg",
		"-q:v", "5",
		thumbPath,
	}
	if out, err := config.FFmpegCommandContext(ctx, args...).CombinedOutput(); err != nil {
		return "", fmt.Errorf("ffmpeg crop: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	if _, err := os.Stat(thumbPath); err != nil {
		return "", fmt.Errorf("thumbnail file not produced: %v", err)
	}

	// 3. Upload through the standard image pipeline (all hosts in parallel).
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

	// 4. Patch both tables, preserving the existing sprite/preview URLs.
	if err := server.UpdateRecordingThumbnails(fn, thumbURL, e.SpriteURL, e.PreviewURL); err != nil {
		return thumbURL, fmt.Errorf("patch recordings row: %w", err)
	}
	if err := server.SavePreviewLinks(fn, thumbURL, e.SpriteURL, e.PreviewURL, nil, nil, nil); err != nil {
		return thumbURL, fmt.Errorf("patch preview_images row: %w", err)
	}
	return thumbURL, nil
}

func sanitizeName(name string) string {
	var b strings.Builder
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

// configFromEnv mirrors cmd/uploadtest's minimal config wiring.
func configFromEnv() *entity.Config {
	cfg := &entity.Config{
		SupabaseURL:    env("SUPABASE_URL"),
		SupabaseAPIKey: env("SUPABASE_API_KEY"),
		VoeSXAPIKey:    env("VOESX_API_KEY"),
		StreamtapeLogin: env("STREAMTAPE_LOGIN"),
		StreamtapeKey:  or(env("STREAMTAPE_KEY"), env("STREAMTAPE_API_KEY")),
		MixdropEmail:   env("MIXDROP_EMAIL"),
		MixdropToken:   or(env("MIXDROP_TOKEN"), env("MIXDROP_KEY")),
		VidaraKey:      env("VIDARA_KEY"),
		Domain:         or(env("DOMAIN"), "https://www.cb.xxx/"),
		FFmpegPath:     env("FFMPEG_PATH"),
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
