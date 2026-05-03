package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/HeapOfChaos/goondvr/supabase"
)

func main() {
	// Read settings.json
	data, err := os.ReadFile("settings.json")
	if err != nil {
		log.Fatalf("Failed to read settings.json: %v", err)
	}

	// Parse settings
	var settings map[string]interface{}
	if err := json.Unmarshal(data, &settings); err != nil {
		log.Fatalf("Failed to parse settings.json: %v", err)
	}

	// Extract Supabase credentials
	supabaseURL, ok := settings["supabase_url"].(string)
	if !ok || supabaseURL == "" {
		log.Fatal("supabase_url not found in settings.json")
	}

	supabaseAPIKey, ok := settings["supabase_api_key"].(string)
	if !ok || supabaseAPIKey == "" {
		log.Fatal("supabase_api_key not found in settings.json")
	}

	// Create Supabase client
	client := supabase.NewClient(supabaseURL, supabaseAPIKey)

	// Test connection
	log.Println("Testing Supabase connection...")
	if err := client.Ping(); err != nil {
		log.Fatalf("Failed to connect to Supabase: %v", err)
	}
	log.Println("✓ Connected to Supabase")

	// Convert to AppSettings struct
	appSettings := supabase.AppSettings{
		Cookies:             getStringOrEmpty(settings, "cookies"),
		UserAgent:           getStringOrEmpty(settings, "user_agent"),
		CompletedDir:        getStringOrEmpty(settings, "completed_dir"),
		FinalizeMode:        getStringOrEmpty(settings, "finalize_mode"),
		FFmpegEncoder:       getStringOrEmpty(settings, "ffmpeg_encoder"),
		FFmpegContainer:     getStringOrEmpty(settings, "ffmpeg_container"),
		FFmpegQuality:       getIntOrZero(settings, "ffmpeg_quality"),
		FFmpegPreset:        getStringOrEmpty(settings, "ffmpeg_preset"),
		NtfyURL:             getStringOrEmpty(settings, "ntfy_url"),
		NtfyTopic:           getStringOrEmpty(settings, "ntfy_topic"),
		NtfyToken:           getStringOrEmpty(settings, "ntfy_token"),
		DiscordWebhookURL:   getStringOrEmpty(settings, "discord_webhook_url"),
		DiskWarningPercent:  getIntOrZero(settings, "disk_warning_percent"),
		DiskCriticalPercent: getIntOrZero(settings, "disk_critical_percent"),
		CFChannelThreshold:  getIntOrZero(settings, "cf_channel_threshold"),
		CFGlobalThreshold:   getIntOrZero(settings, "cf_global_threshold"),
		NotifyCooldownHours: getIntOrZero(settings, "notify_cooldown_hours"),
		NotifyStreamOnline:  getBoolOrFalse(settings, "notify_stream_online"),
		StripchatPDKey:      getStringOrEmpty(settings, "stripchat_pdkey"),
		EnableGoFileUpload:  getBoolOrFalse(settings, "enable_gofile_upload"),
		TurboViPlayAPIKey:   getStringOrEmpty(settings, "turboviplay_api_key"),
		VoeSXAPIKey:         getStringOrEmpty(settings, "voesx_api_key"),
		StreamtapeLogin:     getStringOrEmpty(settings, "streamtape_login"),
		StreamtapeAPIKey:    getStringOrEmpty(settings, "streamtape_api_key"),
		SupabaseURL:         supabaseURL,
		SupabaseAPIKey:      supabaseAPIKey,
		ImgBBAPIKey:         getStringOrEmpty(settings, "imgbb_api_key"),
	}

	// Push to Supabase
	log.Println("Pushing settings to Supabase...")
	if err := client.SaveSettings(appSettings); err != nil {
		log.Fatalf("Failed to save settings to Supabase: %v", err)
	}

	log.Println("✓ Settings successfully pushed to Supabase!")
	log.Println()
	log.Println("Settings uploaded:")
	log.Printf("  • Cookies: %s... (%d chars)", truncate(appSettings.Cookies, 20), len(appSettings.Cookies))
	log.Printf("  • User-Agent: %s", appSettings.UserAgent)
	log.Printf("  • Enable GoFile Upload: %v", appSettings.EnableGoFileUpload)
	log.Printf("  • Finalize Mode: %s", appSettings.FinalizeMode)
	log.Printf("  • FFmpeg Container: %s", appSettings.FFmpegContainer)
	log.Printf("  • FFmpeg Quality: %d", appSettings.FFmpegQuality)
	log.Printf("  • FFmpeg Preset: %s", appSettings.FFmpegPreset)
	log.Printf("  • Disk Warning: %d%%", appSettings.DiskWarningPercent)
	log.Printf("  • Disk Critical: %d%%", appSettings.DiskCriticalPercent)
	log.Printf("  • TurboViPlay API Key: %s", maskKey(appSettings.TurboViPlayAPIKey))
	log.Printf("  • VOE.sx API Key: %s", maskKey(appSettings.VoeSXAPIKey))
	log.Printf("  • Streamtape Login: %s", maskKey(appSettings.StreamtapeLogin))
	log.Printf("  • ImgBB API Key: %s", maskKey(appSettings.ImgBBAPIKey))
	log.Printf("  • Discord Webhook: %s", maskURL(appSettings.DiscordWebhookURL))
	log.Println()
	log.Println("GitHub Actions will now fetch settings from Supabase on deployment!")
}

func getStringOrEmpty(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getIntOrZero(m map[string]interface{}, key string) int {
	if v, ok := m[key].(float64); ok {
		return int(v)
	}
	return 0
}

func getBoolOrFalse(m map[string]interface{}, key string) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}
	return false
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen]
}

func maskKey(key string) string {
	if key == "" {
		return "✗ not configured"
	}
	if len(key) <= 6 {
		return "✓ configured"
	}
	return fmt.Sprintf("✓ %s...%s", key[:3], key[len(key)-3:])
}

func maskURL(url string) string {
	if url == "" {
		return "✗ not configured"
	}
	return "✓ configured"
}
