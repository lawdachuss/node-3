// Command backfillmirrors re-uploads images from Catbox to all hosts
// and stores all mirror URLs in the database.
//
// It finds all recordings whose thumbnail_url, sprite_url, or preview_url
// points to Catbox, downloads the image, re-uploads it to all hosts
// (Catbox, Pixhost, freeimage.host, ImgChest, Imgbox), and updates the
// database with the primary URL and all mirror URLs for ALL three assets.
//
// Usage:
//
//	go run ./cmd/backfillmirrors [flags]
//
// Flags:
//
//	-dry-run    Show what would be done without making changes
//	-limit N    Process at most N recordings (0 = all)
//	-delay D    Delay between records (default 2s)
//	-proxy URL  HTTP proxy URL for Catbox downloads
//	-token T    Cloudflare Access token for proxy auth
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

var proxyFlag string

func main() {
	dryRun := flag.Bool("dry-run", false, "Show what would be done without making changes")
	limit := flag.Int("limit", 0, "Process at most N recordings (0 = all)")
	delay := flag.Duration("delay", 2*time.Second, "Delay between records")
	flag.StringVar(&proxyFlag, "proxy", "", "HTTP proxy URL for Catbox downloads (e.g. http://127.0.0.1:7890)")
	flag.StringVar(&proxyToken, "token", "", "Cloudflare Access token for proxy auth (or set CF_ACCESS_TOKEN env)")
	skipImgBB := flag.Bool("skip-imgbb", false, "Skip ImgBB uploads (use when keys are rate-limited)")
	since := flag.String("since", "", "Only process recordings at/after this point: an RFC3339-ish date (2026-09-01) or a duration back from now (168h). Empty = no filter.")
	force := flag.Bool("force", false, "Re-mirror assets even when a mirror set already exists (default: skip assets that already have >=1 mirror)")
	skipHostsFlag := flag.String("skip-hosts", "", "Comma-separated image hosts to skip (e.g. \"Imgbox,ImgPile\" when they are down — avoids their retry stalls)")
	flag.Parse()

	var skipHostsList []string
	if *skipHostsFlag != "" {
		for _, h := range strings.Split(*skipHostsFlag, ",") {
			if h = strings.TrimSpace(h); h != "" {
				skipHostsList = append(skipHostsList, h)
			}
		}
	}

	// Parse -since into a cutoff time: a duration (e.g. "168h") counts back
	// from now, anything else is tried as a date. Invalid values abort rather
	// than silently processing the whole backlog.
	var sinceCutoff time.Time
	if *since != "" {
		if d, derr := time.ParseDuration(*since); derr == nil {
			sinceCutoff = time.Now().Add(-d)
		} else if t, terr := time.Parse("2006-01-02", *since); terr == nil {
			sinceCutoff = t
		} else if t, terr := time.Parse(time.RFC3339, *since); terr == nil {
			sinceCutoff = t
		} else {
			log.Fatalf("backfillmirrors: invalid -since value %q (want a duration like 168h or a date like 2026-09-01)", *since)
		}
		log.Printf("backfillmirrors: only processing recordings with timestamp >= %s", sinceCutoff.Format(time.RFC3339))
	}

	// Fall back to environment proxy if not specified via flag
	if proxyFlag == "" {
		proxyFlag = os.Getenv("HTTP_PROXY")
	}
	if proxyFlag == "" {
		proxyFlag = os.Getenv("HTTPS_PROXY")
	}
	if proxyFlag == "" {
		proxyFlag = os.Getenv("ALL_PROXY")
	}

	log.SetFlags(log.Ltime)
	log.Printf("backfillmirrors: dry-run=%v limit=%d delay=%v proxy=%q", *dryRun, *limit, *delay, proxyFlag)

	initDownloadClient()

	// Load config from environment
	loadDotEnv(".env")
	cfg := configFromEnv()
	if cfg.SupabaseURL == "" || cfg.SupabaseAPIKey == "" {
		log.Fatal("SUPABASE_URL / SUPABASE_API_KEY not set")
	}
	server.Config = cfg
	server.SyncNodeEnvironment()

	client := server.GetDBClient()
	if client == nil {
		log.Fatal("Supabase not configured")
	}

	// Get recordings with ANY Catbox URL (thumbnail, sprite, or preview)
	log.Printf("querying recordings with Catbox URLs...")
	candidates, err := client.GetRecordingsWithCatboxURLs()
	if err != nil {
		log.Fatalf("get catbox recordings: %v", err)
	}
	log.Printf("found %d recordings with Catbox URLs (any asset type)", len(candidates))

	// Also query for recordings with catbox sprites or previews
	// that might not have catbox thumbnails
	candidateMap := make(map[string]*database.Recording)
	for i := range candidates {
		candidateMap[candidates[i].Filename] = &candidates[i]
	}

	// Convert to a slice FIRST, newest-first (the query already orders by
	// timestamp desc; the map dedups but must not randomize selection), then
	// apply -since and -limit in that order so the flags compose correctly:
	// -since narrows to the target window, -limit caps the count. Previously
	// -limit was applied to an unordered map, which selected a random subset
	// of the backlog instead of the newest recordings.
	candidatePtrs := make([]*database.Recording, 0, len(candidateMap))
	for i := range candidates {
		rec := &candidates[i]
		if _, seen := candidateMap[rec.Filename]; !seen {
			continue
		}
		delete(candidateMap, rec.Filename) // dedup: keep first (newest) occurrence
		if !sinceCutoff.IsZero() {
			ts, terr := time.Parse(time.RFC3339, rec.Timestamp)
			if terr != nil {
				// Timestamps are written in RFC3339 by the DVR; keep anything
				// that fails to parse rather than silently dropping it.
				log.Printf("WARN: could not parse timestamp %q for %s — including", rec.Timestamp, rec.Filename)
			} else if ts.Before(sinceCutoff) {
				continue
			}
		}
		candidatePtrs = append(candidatePtrs, rec)
	}
	candidateMap = nil // free

	if *limit > 0 && len(candidatePtrs) > *limit {
		candidatePtrs = candidatePtrs[:*limit]
	}

	log.Printf("processing %d recordings", len(candidatePtrs))

	// Create the image uploader
	imgUploader := uploader.NewMultiImageUploader()

	// Apply host skips: -skip-imgbb plus any -skip-hosts entries.  Skipping a
	// DOWN host matters at scale: each asset would otherwise stall through 3
	// retry rounds against it (~20s) before UploadToAll returns.
	if *skipImgBB {
		log.Printf("skipping ImgBB uploads (--skip-imgbb)")
		imgUploader.SetSkipHosts("ImgBB")
	}
	if len(skipHostsList) > 0 {
		log.Printf("skipping hosts: %s (--skip-hosts)", strings.Join(skipHostsList, ", "))
		imgUploader.SetSkipHosts(skipHostsList...)
	}

	// Pre-check ImgBB availability — but only when ImgBB is not already
	// skipped AND no other host is being skipped (the pre-check uploads a
	// test image to ALL hosts, which would hit skip-listed hosts anyway and
	// is pointless when the caller explicitly tuned the host set).
	if !*skipImgBB && len(skipHostsList) == 0 {
		log.Printf("pre-checking ImgBB availability...")
		tmpImg, _ := os.CreateTemp("", "imgbb-check-*.jpg")
		tmpImg.Write([]byte{0xFF, 0xD8, 0xFF, 0xD9})
		tmpImg.Close()
		// Test ImgBB directly (not the full fallback chain)
		results := imgUploader.UploadToAll(tmpImg.Name(), nil)
		os.Remove(tmpImg.Name())
		imgbbOK := false
		for _, r := range results {
			if r.Host == "ImgBB" {
				if r.Err != nil {
					log.Printf("WARNING: ImgBB pre-check failed: %v", r.Err)
				} else {
					imgbbOK = true
				}
			}
		}
		if !imgbbOK {
			log.Printf("ImgBB not available, skipping ImgBB uploads")
			*skipImgBB = true
			imgUploader.SetSkipHosts("ImgBB")
		} else {
			log.Printf("ImgBB is available")
		}
	}

	var (
		processed  int
		succeeded  int
		failed     int
		totalAsset int
		mu         sync.Mutex
	)

	for i, rec := range candidatePtrs {
		log.Printf("[%d/%d] processing %s", i+1, len(candidatePtrs), rec.Filename)

		// Collect all Catbox URLs for this recording.  Idempotency: an asset
		// that already has a mirror set is skipped (unless -force) so a re-run
		// only fills actual gaps instead of uploading duplicate copies of the
		// whole Catbox-era library on every invocation.
		//
		// preview_images is the AUTHORITATIVE mirror store (the pipeline writes
		// mirrors there; the recordings-row mirror columns are sparsely
		// populated), so the has-mirror check consults preview_images — falling
		// back to the recordings row only when no preview_images row exists.
		// Without this the first run already-processed rows would be re-mirrored
		// (duplicate image-host uploads) on every subsequent invocation.
		prevImg, perr := client.GetPreviewImage(rec.Filename)
		hasPreviewRow := perr == nil && prevImg != nil

		type asset struct {
			name     string
			url      string
			field    string // "thumbnail", "sprite", "preview"
			isCatbox bool   // primary URL is the Catbox original — reuse it as the Catbox mirror instead of re-uploading a duplicate copy
		}
		var assets []asset

		thumbMirrors, spriteMirrors, previewMirrors := rec.ThumbnailMirrors, rec.SpriteMirrors, rec.PreviewMirrors
		if hasPreviewRow {
			if len(prevImg.ThumbnailMirrors) > 0 {
				thumbMirrors = prevImg.ThumbnailMirrors
			}
			if len(prevImg.SpriteMirrors) > 0 {
				spriteMirrors = prevImg.SpriteMirrors
			}
			if len(prevImg.PreviewMirrors) > 0 {
				previewMirrors = prevImg.PreviewMirrors
			}
		}

		if strings.Contains(rec.ThumbnailURL, "catbox.moe") {
			if *force || len(thumbMirrors) == 0 {
				assets = append(assets, asset{"thumbnail", rec.ThumbnailURL, "thumbnail", true})
			}
		}
		if strings.Contains(rec.SpriteURL, "catbox.moe") {
			if *force || len(spriteMirrors) == 0 {
				assets = append(assets, asset{"sprite", rec.SpriteURL, "sprite", true})
			}
		}
		if strings.Contains(rec.PreviewURL, "catbox.moe") {
			if *force || len(previewMirrors) == 0 {
				assets = append(assets, asset{"preview", rec.PreviewURL, "preview", true})
			}
		}

		if len(assets) == 0 {
			log.Printf("  SKIP: no Catbox URLs needing mirrors")
			continue
		}
		mirroredAny := false

		log.Printf("  found %d Catbox assets to mirror: %v", len(assets), func() []string {
			var names []string
			for _, a := range assets {
				names = append(names, a.name)
			}
			return names
		}())

		// Process each asset
		for _, asset := range assets {
			mu.Lock()
			totalAsset++
			mu.Unlock()

			// Reuse the Catbox PRIMARY as the Catbox mirror entry — re-uploading
			// the same bytes to Catbox would just mint a second, unreferenced
			// URL.  This saves one upload per Catbox-primary asset (~30% of all
			// uploads across the library backfill).  Only assets whose primary
			// is Catbox (isCatbox=true, the only candidates this tool selects)
			// take this path.
			successURLs := map[string]string{}
			if asset.isCatbox {
				successURLs["Catbox"] = asset.url
			}

			// Download from Catbox
			tmpFile, err := downloadThumbnail(asset.url, rec.Filename+"_"+asset.name)
			if err != nil {
				log.Printf("  ERROR: download %s failed: %v", asset.name, err)
				failed++
				continue
			}
			defer os.Remove(tmpFile)

			if *dryRun {
				log.Printf("  DRY-RUN: would upload %s to all hosts", filepath.Base(tmpFile))
				processed++
				continue
			}

			// Upload to all hosts in parallel
			results := imgUploader.UploadToAll(tmpFile, nil)

			// Collect successful URLs and log failures
			for _, r := range results {
				if r.Err == nil && r.URL != "" {
					successURLs[r.Host] = r.URL
				} else if r.Err != nil {
					log.Printf("  WARN: %s failed: %v", r.Host, r.Err)
				}
			}

			if len(successURLs) == 0 {
				log.Printf("  ERROR: all hosts failed for %s", asset.name)
				failed++
				continue
			}

			log.Printf("  %s: uploaded to %d hosts: %v", asset.name, len(successURLs), successURLs)

			// The EXISTING primary URL is kept as-is (only the mirror set is
			// filled below) — overwriting a known-good, referenced primary with
			// a nondeterministic first-success host could downgrade it.

			// Update the mirror set on the recording
			switch asset.field {
			case "thumbnail":
				rec.ThumbnailMirrors = successURLs
			case "sprite":
				rec.SpriteMirrors = successURLs
			case "preview":
				rec.PreviewMirrors = successURLs
			}

			mu.Lock()
			processed++
			mu.Unlock()
			mirroredAny = true

			// Small delay between asset uploads
			time.Sleep(500 * time.Millisecond)
		}

		// Save the recording with all updated mirrors — only when something
		// was actually mirrored this pass.  An all-assets-skipped row is
		// already complete, so rewriting it would be a pure no-op write.
		if *dryRun {
			// Asset-level DRY-RUN lines above already counted `processed`.
			continue
		}
		if !mirroredAny {
			continue
		}
		if err := client.SaveRecording(rec); err != nil {
			log.Printf("  ERROR: save recording: %v", err)
			failed++
			continue
		}

		// Update preview_images table
		img := &database.PreviewImage{
			Filename:         rec.Filename,
			ThumbnailURL:     rec.ThumbnailURL,
			ThumbnailMirrors: rec.ThumbnailMirrors,
			SpriteURL:        rec.SpriteURL,
			SpriteMirrors:    rec.SpriteMirrors,
			PreviewURL:       rec.PreviewURL,
			PreviewMirrors:   rec.PreviewMirrors,
		}
		if err := client.SavePreviewImage(img); err != nil {
			log.Printf("  ERROR: save preview image: %v", err)
			failed++
			continue
		}

		log.Printf("  OK: saved recording with mirrors (thumb=%d, sprite=%d, preview=%d)",
			len(rec.ThumbnailMirrors), len(rec.SpriteMirrors), len(rec.PreviewMirrors))
		succeeded++

		if *delay > 0 && i < len(candidatePtrs)-1 {
			time.Sleep(*delay)
		}
	}

	log.Printf("backfillmirrors: done — %d recordings, %d assets processed, %d succeeded, %d failed",
		len(candidatePtrs), totalAsset, succeeded, failed)
}

// downloadClient is an HTTP client that optionally uses a proxy for Catbox downloads.
var downloadClient = &http.Client{
	Timeout: 120 * time.Second,
	Transport: &http.Transport{
		TLSHandshakeTimeout:   30 * time.Second,
		ResponseHeaderTimeout: 60 * time.Second,
		MaxIdleConns:          10,
		IdleConnTimeout:       90 * time.Second,
	},
}

var proxyToken string

func initDownloadClient() {
	if proxyFlag != "" {
		parsedProxy, err := url.Parse(proxyFlag)
		if err != nil {
			log.Printf("WARN: invalid proxy URL %q: %v, using direct connection", proxyFlag, err)
		} else {
			downloadClient.Transport = &http.Transport{
				Proxy: http.ProxyURL(parsedProxy),
			}
			log.Printf("using proxy %s for Catbox downloads", proxyFlag)
		}
	}
	if proxyToken == "" {
		proxyToken = os.Getenv("CF_ACCESS_TOKEN")
	}
}

// downloadThumbnail downloads an image from a URL to a temporary file.
func downloadThumbnail(imgURL, filename string) (string, error) {
	imgURL = strings.Replace(imgURL, "http://", "https://", 1)

	req, err := http.NewRequest("GET", imgURL, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	if proxyToken != "" {
		req.Header.Set("Authorization", "Bearer "+proxyToken)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "image/webp,image/apng,image/*,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := downloadClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("GET %s: %w", imgURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("GET %s: status %d", imgURL, resp.StatusCode)
	}

	ext := filepath.Ext(filename)
	if ext == "" {
		ext = ".jpg"
	}

	tmpFile, err := os.CreateTemp("", "backfill-mirrors-*"+ext)
	if err != nil {
		return "", fmt.Errorf("create temp: %w", err)
	}
	defer tmpFile.Close()

	if _, err := io.Copy(tmpFile, resp.Body); err != nil {
		os.Remove(tmpFile.Name())
		return "", fmt.Errorf("download: %w", err)
	}

	return tmpFile.Name(), nil
}

func configFromEnv() *entity.Config {
	return &entity.Config{
		SupabaseURL:            env("SUPABASE_URL"),
		SupabaseAPIKey:         env("SUPABASE_API_KEY"),
		SupabaseServiceRoleKey: env("SUPABASE_SERVICE_ROLE_KEY"),
	}
}

func env(k string) string { return os.Getenv(k) }

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
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		v = strings.Trim(v, `"`)
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
