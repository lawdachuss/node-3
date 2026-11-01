package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

type manifestEntry struct {
	Filename   string `json:"filename"`
	SpriteURL  string `json:"sprite_url"`
	PreviewURL string `json:"preview_url"`
}

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

	var entries []manifestEntry
	for _, rec := range recordings {
		hasInRecordings := rec.ThumbnailURL != ""
		hasInPreview := false
		var spriteURL, previewURL string
		if links, ok := previews[rec.Filename]; ok {
			hasInPreview = links[0] != ""
			spriteURL = links[1]
			previewURL = links[2]
		}

		if !hasInRecordings && !hasInPreview && spriteURL != "" {
			entries = append(entries, manifestEntry{
				Filename:   rec.Filename,
				SpriteURL:  spriteURL,
				PreviewURL: previewURL,
			})
		}
	}

	log.Printf("Recordings missing thumbnail in BOTH tables but HAVE sprite: %d", len(entries))

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}

	outputPath := "backfill_manifest.json"
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		log.Fatalf("write manifest: %v", err)
	}
	log.Printf("Manifest written to %s", outputPath)
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