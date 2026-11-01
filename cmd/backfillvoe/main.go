// Command backfillvoe recovers a missing thumbnail for a recording whose
// only recoverable source is an external mirror. It supports three kinds of
// manifest entries:
//
//  1. VOE.sx embeds ("url"): fetches the player page, deobfuscates the player
//     JSON blob to obtain the HLS source URL, then extracts a frame.
//  2. Direct HLS/MP4 streams ("source"): skips the player-fetch/decode step
//     and streams the frame straight from the given URL.
//  3. Pre-generated assets ("thumb"/"preview"/"sprite"): instead of (or as a
//     fallback to) extracting a frame, these Worker/CDN URLs are re-hosted to
//     Pixhost so the resulting links stay durable, then PATCHed into the DB.
//
// In every case the resolved thumbnail (and optional preview/sprite) URLs are
// persisted to BOTH the recordings row and the preview_images row.
//
// Usage:
//
//	go run ./cmd/backfillvoe manifest.json
//
// Manifest format (JSON array):
//
//	[{"filename": "user_2026-09-06_20-55-22.mp4",
//	  "url": "https://voe.sx/e/h9a1b2c3d4e5"},                    // VOE player
//	 {"filename": "user2.mp4",
//	  "source": "https://…/master.m3u8?t=…",                      // direct stream
//	  "duration": 5400,                                           // seconds (optional)
//	  "thumb": "https://…/thumb.webp",                            // optional re-hosts
//	  "preview": "https://…/preview.webp",
//	  "sprite": "https://…/sprite.webp"}]
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/config"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

const (
	browserUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
	thumbW    = 1280
	thumbH    = 720
)

type manifestEntry struct {
	Filename string  `json:"filename"`
	URL      string  `json:"url"`      // VOE player page
	Source   string  `json:"source"`   // pre-resolved HLS/MP4 stream
	Duration float64 `json:"duration"` // stream duration in seconds (optional)
	Thumb    string  `json:"thumb"`    // pre-generated thumbnail/webp URL
	Preview  string  `json:"preview"`  // animated preview webp URL
	Sprite   string  `json:"sprite"`   // sprite sheet URL
}

var redirectRe = regexp.MustCompile(`window\.location\.href\s*=\s*'([^']+)'`)
var blobRe = regexp.MustCompile(`<script type="application/json"[^>]*>\s*\["([^"]+)"\]`)

func main() {
	flag.Parse()
	if flag.NArg() < 1 {
		log.Fatal("usage: go run ./cmd/backfillvoe <manifest.json>")
	}

	loadDotEnv(".env")
	cfg := configFromEnv()
	if cfg.SupabaseURL == "" || cfg.SupabaseAPIKey == "" {
		log.Fatal("SUPABASE_URL / SUPABASE_API_KEY not set")
	}
	server.Config = cfg
	server.SyncNodeEnvironment()
	if cfg.FFmpegPath != "" {
		config.SetFFmpegPath(cfg.FFmpegPath)
	}

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

	dir, err := os.MkdirTemp("", "backfillvoe")
	if err != nil {
		log.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	client := &http.Client{Timeout: 60 * time.Second}

	okCount := 0
	for i, e := range entries {
		log.Printf("[%d/%d] %s", i+1, len(entries), e.Filename)
		if err := backfillOne(client, e, dir); err != nil {
			log.Printf("  FAIL: %v", err)
			continue
		}
		okCount++
	}
	log.Printf("done: %d/%d thumbnails backfilled", okCount, len(entries))
}

func backfillOne(client *http.Client, e manifestEntry, workDir string) error {
	// Resolve the stream to extract the card thumbnail from.
	var streamURL string
	var direct bool
	var seek string
	switch {
	case e.Source != "":
		streamURL, direct = e.Source, true
		if e.Duration > 0 {
			seek = formatTimecode(thumbs(e.Duration))
		}
	case e.URL != "":
		log.Printf("  fetching VOE player: %s", e.URL)
		html, err := fetchPlayerHTML(client, e.URL)
		if err != nil {
			return err
		}
		m := blobRe.FindStringSubmatch(html)
		if m == nil {
			return fmt.Errorf("encoded player blob not found on %s", e.URL)
		}
		dec, err := decryptF7(m[1])
		if err != nil {
			return fmt.Errorf("decode player blob: %w", err)
		}
		if dec.Source == "" {
			return fmt.Errorf("decoded player JSON has no source URL")
		}
		streamURL, direct = dec.Source, false
		log.Printf("  hls source: %.150s", streamURL)
	default:
		return fmt.Errorf("manifest entry has neither url nor source")
	}

	// Thumbnail: extract a still near the 10% mark. Fall back to the
	// pre-generated video thumbnail if the stream cannot be decoded.
	thumbURL := ""
	framePath := filepath.Join(workDir, "frame_"+sanitizeName(e.Filename)+".jpg")
	if seek == "" {
		if s, err := seekPoint(streamURL); err == nil {
			seek = s
		} else {
			seek = "00:02:00"
			log.Printf("  probe failed (%v) — using fixed seek of 2m", err)
		}
	}
	if err := extractFrame(streamURL, seek, framePath); err != nil {
		if !direct {
			// VOE HLS sometimes 400s while the direct MP4 still works.
			if decSource, _ := decryptFromURL(client, e.URL); decSource.DirectAccessURL != "" {
				log.Printf("  hls failed (%v) — falling back to direct MP4", err)
				if s, e2 := seekPoint(decSource.DirectAccessURL); e2 == nil {
					seek = s
				}
				if extractFrame(decSource.DirectAccessURL, seek, framePath) == nil {
					err = nil
				}
			}
		}
		if err != nil {
			if e.Thumb != "" {
				log.Printf("  stream decode failed (%v) — re-hosting provided thumbnail", err)
				tu, uerr := rehost(client, e.Thumb, ".webp", workDir, e.Filename, "thumb")
				if uerr != nil {
					return fmt.Errorf("extract frame: %v; rehost thumb: %w", err, uerr)
				}
				thumbURL = tu
			} else {
				return fmt.Errorf("extract frame: %w", err)
			}
		}
	}
	if thumbURL == "" {
		tu, err := uploader.NewThumbnailUploader("").Upload(framePath)
		if err != nil {
			return fmt.Errorf("upload thumbnail: %w", err)
		}
		thumbURL = tu
		log.Printf("  thumbnail -> %s", thumbURL)
	}

	// Optional preview/sprite: re-host the mirror's webps to Pixhost so the
	// stored links are durable instead of tokenized worker URLs.
	previewURL := ""
	if e.Preview != "" {
		if u, err := rehost(client, e.Preview, ".webp", workDir, e.Filename, "prev"); err != nil {
			log.Printf("  preview rehost failed: %v", err)
		} else {
			previewURL = u
		}
	}
	spriteURL := ""
	if e.Sprite != "" {
		if u, err := rehost(client, e.Sprite, ".webp", workDir, e.Filename, "sprite"); err != nil {
			log.Printf("  sprite rehost failed: %v", err)
		} else {
			spriteURL = u
		}
	}

	// Persist to both tables.
	if err := server.UpdateRecordingThumbnails(e.Filename, thumbURL, spriteURL, previewURL); err != nil {
		return fmt.Errorf("patch recordings row: %w", err)
	}
	if err := server.SavePreviewLinks(e.Filename, thumbURL, spriteURL, previewURL, nil, nil, nil); err != nil {
		return fmt.Errorf("patch preview_images row: %w", err)
	}
	return nil
}

// decryptFromURL re-fetches a VOE player page and returns its decoded JSON.
func decryptFromURL(client *http.Client, eURL string) (struct {
	Source          string `json:"source"`
	DirectAccessURL string `json:"direct_access_url"`
}, error) {
	html, err := fetchPlayerHTML(client, eURL)
	if err != nil {
		return struct {
			Source          string `json:"source"`
			DirectAccessURL string `json:"direct_access_url"`
		}{}, err
	}
	m := blobRe.FindStringSubmatch(html)
	if m == nil {
		return struct {
			Source          string `json:"source"`
			DirectAccessURL string `json:"direct_access_url"`
		}{}, fmt.Errorf("encoded player blob not found on %s", eURL)
	}
	return decryptF7(m[1])
}

// rehost downloads a URL and uploads it to Pixhost, returning the durable URL.
func rehost(client *http.Client, src, ext, workDir, filename, tag string) (string, error) {
	resp, err := client.Get(src)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", src, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("GET %s: HTTP %d", src, resp.StatusCode)
	}
	tmp := filepath.Join(workDir, tag+"_"+sanitizeName(filename)+ext)
	f, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return "", err
	}
	f.Close()
	u, err := uploader.NewThumbnailUploader("").Upload(tmp)
	if err != nil {
		return "", err
	}
	return u, nil
}

func thumbs(sec float64) float64 {
	t := sec * 0.1
	if t < 5 {
		t = 5
	}
	return t
}

// fetchPlayerHTML GETs the embed URL and, if the page is VOE's JS
// meta-refresh stub, re-requests the redirect target.
func fetchPlayerHTML(client *http.Client, raw string) (string, error) {
	u := raw
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest("GET", u, nil)
		if err != nil {
			return "", err
		}
		req.Header.Set("User-Agent", browserUA)
		req.Header.Set("Referer", "https://voe.sx/")
		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("GET %s: %w", u, err)
		}
		var sb strings.Builder
		_, rerr := io.Copy(&sb, io.LimitReader(resp.Body, 2<<20))
		resp.Body.Close()
		if rerr != nil {
			return "", rerr
		}
		if rm := redirectRe.FindStringSubmatch(sb.String()); rm != nil {
			u = rm[1]
			if strings.HasPrefix(u, "/") {
				segs := strings.SplitN(raw, "/", 4)
				u = strings.Join(segs[:3], "/") + u
			}
			continue
		}
		return sb.String(), nil
	}
	return "", fmt.Errorf("too many redirects for %s", raw)
}

// decryptF7 reverses VOE's player blob obfuscation:
//
//	rot13 -> split on ["@$","^^","~@","%?","*~","!!","#&"] -> strip '_' ->
//	base64 -> shift each char -3 -> reverse -> base64 -> JSON
func decryptF7(blob string) (struct {
	Source          string `json:"source"`
	DirectAccessURL string `json:"direct_access_url"`
}, error) {
	var out struct {
		Source          string `json:"source"`
		DirectAccessURL string `json:"direct_access_url"`
	}

	step1 := rot13(blob)
	for _, p := range []string{"@$", "^^", "~@", "%?", "*~", "!!", "#&"} {
		step1 = strings.ReplaceAll(step1, p, "_")
	}
	step1 = strings.ReplaceAll(step1, "_", "")
	step4, err := b64(step1)
	if err != nil {
		return out, fmt.Errorf("b64 step1: %w", err)
	}
	var b strings.Builder
	for _, r := range step4 {
		b.WriteRune(rune(r - 3))
	}
	rev := reverse(b.String())
	final, err := b64(rev)
	if err != nil {
		return out, fmt.Errorf("b64 final: %w", err)
	}
	if err := json.Unmarshal([]byte(final), &out); err != nil {
		return out, err
	}
	return out, nil
}

func rot13(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune('a' + (r-'a'+13)%26)
		case r >= 'A' && r <= 'Z':
			b.WriteRune('A' + (r-'A'+13)%26)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func b64(s string) (string, error) {
	if pad := len(s) % 4; pad != 0 {
		s += strings.Repeat("=", 4-pad)
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}

// seekPoint probes the stream duration and returns a timecode at ~10%.
func seekPoint(streamURL string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	out, err := config.FFprobeCommandContext(ctx,
		"-v", "error", "-show_entries", "format=duration",
		"-of", "json", streamURL).Output()
	if err != nil {
		return "", err
	}
	var pr struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if err := json.Unmarshal(out, &pr); err != nil {
		return "", err
	}
	dur, err := strconv.ParseFloat(pr.Format.Duration, 64)
	if err != nil || dur <= 0 {
		return "", fmt.Errorf("bad duration %q", pr.Format.Duration)
	}
	return formatTimecode(thumbs(dur)), nil
}

func formatTimecode(sec float64) string {
	i := int(sec)
	return fmt.Sprintf("%02d:%02d:%02d", i/3600, (i/60)%60, i%60)
}

// extractFrame seeks into a stream and writes a single 1280x720 JPEG frame.
func extractFrame(streamURL, seek, outPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	args := []string{
		"-y", "-hide_banner", "-loglevel", "error",
		"-ss", seek,
		"-i", streamURL,
		"-frames:v", "1",
		"-vf", fmt.Sprintf("scale=%d:%d:force_original_aspect_ratio=decrease,pad=%d:%d:(ow-iw)/2:(oh-ih)/2", thumbW, thumbH, thumbW, thumbH),
		"-c:v", "mjpeg",
		"-q:v", "5",
		outPath,
	}
	if out, err := config.FFmpegCommandContext(ctx, args...).CombinedOutput(); err != nil {
		return fmt.Errorf("ffmpeg: %v\n%s", err, strings.TrimSpace(string(out)))
	}
	if _, err := os.Stat(outPath); err != nil {
		return fmt.Errorf("frame not produced: %v", err)
	}
	return nil
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