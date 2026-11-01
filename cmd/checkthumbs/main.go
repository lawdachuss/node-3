package main

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

func main() {
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

	log.Printf("loading all recordings...")
	recordings, err := client.GetAllRecordings()
	if err != nil {
		log.Fatalf("get recordings: %v", err)
	}
	log.Printf("recordings loaded: %d", len(recordings))

	log.Printf("loading all preview_images links...")
	previews := server.LoadAllPreviewLinks()
	log.Printf("preview_images rows loaded: %d", len(previews))

	noThumbInRecordings := 0
	noThumbInPreview := 0
	noThumbInEither := 0

	for _, rec := range recordings {
		hasInRecordings := rec.ThumbnailURL != ""
		hasInPreview := false
		if links, ok := previews[rec.Filename]; ok {
			hasInPreview = links[0] != ""
		}

		if !hasInRecordings {
			noThumbInRecordings++
		}
		if !hasInPreview {
			noThumbInPreview++
		}
		if !hasInRecordings && !hasInPreview {
			noThumbInEither++
			hasSprite := false
			if links, ok := previews[rec.Filename]; ok {
				hasSprite = links[1] != "" // sprite URL
			}
			log.Printf("  MISSING IN BOTH: %s (hasSprite=%v)", rec.Filename, hasSprite)
		}
	}

	log.Printf("Recordings missing thumbnail_url in recordings table: %d", noThumbInRecordings)
	log.Printf("Recordings missing thumbnail_url in preview_images table: %d", noThumbInPreview)
	log.Printf("Recordings missing thumbnail in BOTH tables: %d", noThumbInEither)
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
	return cfg
}

func env(k string) string { return os.Getenv(k) }

func or(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

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