// Command syncthumbs backfills recordings.thumbnail_url from the matching
// preview_images row.  The video card on the site reads recordings.thumbnail_url,
// but historically only preview_images was written during pipeline failures, so
// recordings whose thumbnail was generated/saved into preview_images still show
// a NULL thumbnail on the site.  This tool copies the existing thumbnail URL
// (and sprite/preview) from preview_images onto the recordings row — no
// regeneration, no image-host uploads, just a DB sync.
//
// Usage:
//
//	go run ./cmd/syncthumbs [-dry-run] [-limit N] [-delay D]
package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

func main() {
	dryRun := flag.Bool("dry-run", false, "Show what would be synced without writing")
	limit := flag.Int("limit", 0, "Process at most N recordings (0 = all)")
	delay := flag.Duration("delay", 300*time.Millisecond, "Delay between PATCHes")
	flag.Parse()

	log.SetFlags(log.Ltime)
	log.Printf("syncthumbs: dry-run=%v limit=%d delay=%v", *dryRun, *limit, *delay)

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

	log.Printf("loading all preview_images links...")
	previews := server.LoadAllPreviewLinks()
	log.Printf("preview_images rows loaded: %d", len(previews))

	log.Printf("loading all recordings...")
	recordings, err := client.GetAllRecordings()
	if err != nil {
		log.Fatalf("get recordings: %v", err)
	}
	log.Printf("recordings loaded: %d", len(recordings))

	type fix struct {
		filename                 string
		thumb, sprite, preview   string
	}
	var fixes []fix
	for i := range recordings {
		rec := &recordings[i]
		if rec.ThumbnailURL != "" {
			continue
		}
		links, ok := previews[rec.Filename]
		if !ok {
			continue
		}
		thumb, sprite, preview := links[0], links[1], links[2]
		if thumb == "" {
			continue
		}
		fixes = append(fixes, fix{rec.Filename, thumb, sprite, preview})
	}

	log.Printf("recordings missing thumbnail_url that have a preview_images thumbnail: %d", len(fixes))

	if *limit > 0 && len(fixes) > *limit {
		fixes = fixes[:*limit]
	}

	okCount := 0
	for i, f := range fixes {
		if *dryRun {
			log.Printf("[%d/%d] %s -> %s", i+1, len(fixes), f.filename, f.thumb)
			continue
		}
		if err := server.UpdateRecordingThumbnails(f.filename, f.thumb, f.sprite, f.preview); err != nil {
			log.Printf("  FAIL %s: %v", f.filename, err)
			continue
		}
		log.Printf("[%d/%d] OK %s -> %s", i+1, len(fixes), f.filename, f.thumb)
		okCount++
		if *delay > 0 {
			time.Sleep(*delay)
		}
	}

	if *dryRun {
		log.Printf("done (dry-run): would sync %d recordings", len(fixes))
	} else {
		log.Printf("done: synced %d/%d recordings", okCount, len(fixes))
	}
}

// configFromEnv mirrors backfillthumbs' minimal config wiring.
func configFromEnv() *entity.Config {
	cfg := &entity.Config{
		SupabaseURL:           env("SUPABASE_URL"),
		SupabaseAPIKey:        env("SUPABASE_API_KEY"),
		SupabaseServiceRoleKey: env("SUPABASE_SERVICE_ROLE_KEY"),
		VoeSXAPIKey:           env("VOESX_API_KEY"),
		StreamtapeLogin:       env("STREAMTAPE_LOGIN"),
		StreamtapeKey:         or(env("STREAMTAPE_KEY"), env("STREAMTAPE_API_KEY")),
		MixdropEmail:          env("MIXDROP_EMAIL"),
		MixdropToken:          or(env("MIXDROP_TOKEN"), env("MIXDROP_KEY")),
		VidaraKey:             env("VIDARA_KEY"),
		Domain:                or(env("DOMAIN"), "https://www.cb.xxx/"),
		FFmpegPath:            env("FFMPEG_PATH"),
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
