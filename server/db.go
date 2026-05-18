package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ─── Supabase client ──────────────────────────────────────────────────────────

func supabaseRestURL() string {
	if Config == nil || Config.SupabaseURL == "" {
		return ""
	}
	return Config.SupabaseURL + "/rest/v1"
}

func supabaseRestAPIKey() string {
	if Config == nil {
		return ""
	}
	return Config.SupabaseAPIKey
}

func supabaseRequest(method, path string, body []byte) (*http.Response, error) {
	baseURL := supabaseRestURL()
	apiKey := supabaseRestAPIKey()
	if baseURL == "" || apiKey == "" {
		return nil, fmt.Errorf("Supabase not configured")
	}

	req, err := http.NewRequest(method, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("apikey", apiKey)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	return client.Do(req)
}

// CheckSupabase verifies the app_settings table is reachable via the REST API.
func CheckSupabase() error {
	resp, err := supabaseRequest("GET",
		"/app_settings?key=eq.__healthcheck__&select=key&limit=1", nil)
	if err != nil {
		return fmt.Errorf("health check request failed: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		return nil
	case 404:
		return fmt.Errorf("app_settings table not found (HTTP 404) — run the SQL migration first")
	case 401, 403:
		return fmt.Errorf("authentication failed (HTTP %d) — check SUPABASE_API_KEY and RLS policies", resp.StatusCode)
	default:
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected response (HTTP %d): %s", resp.StatusCode, string(body))
	}
}

// ─── app_settings helpers ─────────────────────────────────────────────────────

// saveJSONSetting upserts a JSON value into the app_settings table via REST.
func saveJSONSetting(key string, data []byte) error {
	var rawJSON json.RawMessage
	if err := json.Unmarshal(data, &rawJSON); err != nil {
		return fmt.Errorf("parse json: %w", err)
	}

	body := map[string]interface{}{
		"key":   key,
		"value": rawJSON,
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	resp, err := supabaseRequest("POST", "/app_settings", bodyBytes)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("api returned %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// loadJSONSetting reads a JSON value from the app_settings table via REST.
// Returns nil if the key is not found or on any error.
func loadJSONSetting(key string) []byte {
	resp, err := supabaseRequest("GET",
		"/app_settings?key=eq."+key+"&select=value", nil)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}

	var entries []struct {
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(body, &entries); err != nil {
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	return []byte(string(entries[0].Value))
}

// ─── Channels ─────────────────────────────────────────────────────────────────

type channelConfig struct {
	IsPaused    bool   `json:"is_paused"`
	Username    string `json:"username"`
	Framerate   int    `json:"framerate"`
	Resolution  int    `json:"resolution"`
	Pattern     string `json:"pattern"`
	MaxDuration int    `json:"max_duration"`
	MaxFilesize int    `json:"max_filesize"`
	Compress    bool   `json:"compress"`
	CreatedAt   int64  `json:"created_at"`
}

func saveChannelsViaRestAPI(data []byte) error {
	return saveJSONSetting("dvr_channels", data)
}

func loadChannelsViaRestAPI() []byte {
	return loadJSONSetting("dvr_channels")
}

func SaveChannelsToDB(data []byte) error {
	if err := saveChannelsViaRestAPI(data); err != nil {
		return fmt.Errorf("save channels to Supabase: %w", err)
	}
	return nil
}

func LoadChannelsFromDB() []byte {
	return loadChannelsViaRestAPI()
}

// ─── Settings ─────────────────────────────────────────────────────────────────

func SaveSettingsToDB(data []byte) error {
	if err := saveJSONSetting("dvr_settings", data); err != nil {
		return fmt.Errorf("save settings to Supabase: %w", err)
	}
	return nil
}

func LoadSettingsFromDB() []byte {
	return loadJSONSetting("dvr_settings")
}

// ─── Recordings ───────────────────────────────────────────────────────────────

func SaveRecordingsToDB(data []byte) error {
	if err := saveJSONSetting("recordings_db", data); err != nil {
		return fmt.Errorf("save recordings to Supabase: %w", err)
	}
	return nil
}

func LoadRecordingsFromDB() []byte {
	return loadJSONSetting("recordings_db")
}

// ─── Tunnels ──────────────────────────────────────────────────────────────────

func SaveTunnelToDB(tunnelURL string, runID int) error {
	tunnelJSON := fmt.Sprintf(`"%s"`, tunnelURL)
	if err := saveJSONSetting("tunnel_url", []byte(tunnelJSON)); err != nil {
		return fmt.Errorf("save tunnel to Supabase: %w", err)
	}
	return nil
}

func LoadCurrentTunnel() (string, error) {
	raw := loadJSONSetting("tunnel_url")
	if raw == nil {
		return "", nil
	}
	var tunnelURL string
	if err := json.Unmarshal(raw, &tunnelURL); err != nil {
		return "", fmt.Errorf("parse tunnel URL: %w", err)
	}
	return tunnelURL, nil
}

// ─── Preview Links ────────────────────────────────────────────────────────────

func SavePreviewLinks(filename, thumbnailURL, spriteURL string) error {
	data, err := json.Marshal(map[string]string{
		"thumbnail_url": thumbnailURL,
		"sprite_url":    spriteURL,
	})
	if err != nil {
		return fmt.Errorf("marshal preview links: %w", err)
	}
	return saveJSONSetting("preview_link:"+filename, data)
}

func LoadPreviewLinks(filename string) (thumbnailURL, spriteURL string) {
	raw := loadJSONSetting("preview_link:" + filename)
	if raw == nil {
		return "", ""
	}
	var entry struct {
		ThumbnailURL string `json:"thumbnail_url"`
		SpriteURL    string `json:"sprite_url"`
	}
	if err := json.Unmarshal(raw, &entry); err != nil {
		return "", ""
	}
	return entry.ThumbnailURL, entry.SpriteURL
}
