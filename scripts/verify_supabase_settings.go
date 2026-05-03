package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/HeapOfChaos/goondvr/supabase"
)

func main() {
	// Read settings.json to get Supabase credentials
	data, err := os.ReadFile("settings.json")
	if err != nil {
		log.Fatalf("Failed to read settings.json: %v", err)
	}

	var settings map[string]interface{}
	if err := json.Unmarshal(data, &settings); err != nil {
		log.Fatalf("Failed to parse settings.json: %v", err)
	}

	supabaseURL := settings["supabase_url"].(string)
	supabaseAPIKey := settings["supabase_api_key"].(string)

	// Create Supabase client
	client := supabase.NewClient(supabaseURL, supabaseAPIKey)

	// Fetch settings from Supabase
	log.Println("Fetching settings from Supabase...")
	appSettings, err := client.GetSettings()
	if err != nil {
		log.Fatalf("Failed to fetch settings: %v", err)
	}

	if appSettings == nil {
		log.Fatal("No settings found in Supabase!")
	}

	log.Println("✓ Settings retrieved from Supabase successfully!")
	log.Println()
	log.Println("═══════════════════════════════════════════════════════════")
	log.Println("                  SUPABASE SETTINGS                        ")
	log.Println("═══════════════════════════════════════════════════════════")
	log.Println()
	
	log.Println("🔐 Authentication:")
	log.Printf("  • Cookies: %s... (%d chars)", truncate(appSettings.Cookies, 30), len(appSettings.Cookies))
	log.Printf("  • User-Agent: %s", appSettings.UserAgent)
	log.Println()
	
	log.Println("📤 Upload Configuration:")
	log.Printf("  • Enable GoFile Upload: %v", appSettings.EnableGoFileUpload)
	log.Printf("  • TurboViPlay API Key: %s", maskKey(appSettings.TurboViPlayAPIKey))
	log.Printf("  • VOE.sx API Key: %s", maskKey(appSettings.VoeSXAPIKey))
	log.Printf("  • Streamtape Login: %s", maskKey(appSettings.StreamtapeLogin))
	log.Printf("  • Streamtape API Key: %s", maskKey(appSettings.StreamtapeAPIKey))
	log.Printf("  • ImgBB API Key: %s", maskKey(appSettings.ImgBBAPIKey))
	log.Println()
	
	log.Println("🎬 FFmpeg Settings:")
	log.Printf("  • Finalize Mode: %s", appSettings.FinalizeMode)
	log.Printf("  • Container: %s", appSettings.FFmpegContainer)
	log.Printf("  • Encoder: %s", appSettings.FFmpegEncoder)
	log.Printf("  • Quality (CRF): %d", appSettings.FFmpegQuality)
	log.Printf("  • Preset: %s", appSettings.FFmpegPreset)
	log.Println()
	
	log.Println("💾 Disk Monitoring:")
	log.Printf("  • Warning Threshold: %d%%", appSettings.DiskWarningPercent)
	log.Printf("  • Critical Threshold: %d%%", appSettings.DiskCriticalPercent)
	log.Println()
	
	log.Println("🔔 Notifications:")
	log.Printf("  • Discord Webhook: %s", maskURL(appSettings.DiscordWebhookURL))
	log.Printf("  • Ntfy URL: %s", maskURL(appSettings.NtfyURL))
	log.Printf("  • Ntfy Topic: %s", appSettings.NtfyTopic)
	log.Printf("  • Stream Online Notifications: %v", appSettings.NotifyStreamOnline)
	log.Printf("  • Cooldown Hours: %d", appSettings.NotifyCooldownHours)
	log.Println()
	
	log.Println("☁️ Cloudflare Protection:")
	log.Printf("  • Channel Threshold: %d", appSettings.CFChannelThreshold)
	log.Printf("  • Global Threshold: %d", appSettings.CFGlobalThreshold)
	log.Println()
	
	log.Println("═══════════════════════════════════════════════════════════")
	log.Println()
	log.Println("✅ All settings are stored in Supabase and ready for GitHub Actions!")
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
	if len(url) > 40 {
		return fmt.Sprintf("✓ %s...", url[:40])
	}
	return fmt.Sprintf("✓ %s", url)
}
