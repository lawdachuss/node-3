package server

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// ─── Instance ID & Node ID ───────────────────────────────────────────────────

var instanceID string
var nodeID string
var channelPoolMode string

func init() {
	instanceID = os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		instanceID = "default"
	}
	nodeID = detectNodeID()
	channelPoolMode = detectPoolMode()
}

// detectPoolMode returns the channel pool mode.
// Only uses the explicit CHANNEL_POOL_MODE env var; defaults to "isolated".
func detectPoolMode() string {
	if mode := os.Getenv("CHANNEL_POOL_MODE"); mode != "" {
		return mode
	}
	return entity.PoolModeIsolated
}

// detectNodeID auto-detects the node identity using a priority chain:
// 1. NODE_ID env var (explicit)
// 2. GITHUB_REPOSITORY env var (set by GitHub Actions)
// 3. os.Hostname() (VPS / local)
// 4. Random fallback (defensive)
func detectNodeID() string {
	if id := os.Getenv("NODE_ID"); id != "" {
		return id
	}
	if repo := os.Getenv("GITHUB_REPOSITORY"); repo != "" {
		parts := strings.Split(repo, "-")
		if len(parts) > 1 {
			return parts[len(parts)-1]
		}
		return strings.ReplaceAll(repo, "/", "-")
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(1<<48))
	return fmt.Sprintf("node-%x", n)
}

// NodeID returns the current node's unique identifier.
func NodeID() string { return nodeID }

// ChannelPoolMode returns the current channel pool mode ("isolated" or "pooled").
func ChannelPoolMode() string { return channelPoolMode }

// IsPooledMode returns true if the system is running in distributed pool mode.
func IsPooledMode() bool { return channelPoolMode == "pooled" }

func channelsKey() string {
	return "channels_" + instanceID
}

func ClearLegacyChannelsBlob() error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}
	return client.DeleteLegacyChannelsBlob(channelsKey())
}

// ─── Supabase client ──────────────────────────────────────────────────────────

var dbClient *database.Client

// GetDBClient returns the Supabase database client
func GetDBClient() *database.Client {
	if dbClient == nil && Config != nil && Config.SupabaseURL != "" && Config.SupabaseAPIKey != "" {
		dbClient = database.NewClient(Config.SupabaseURL, Config.SupabaseAPIKey)
	}
	return dbClient
}

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

// supabaseRequest makes an authenticated REST call to Supabase with default
// headers. For writes (POST/PATCH/DELETE with a body) it sets
// Prefer: resolution=merge-duplicates. Use supabaseRequestWithPrefer when you
// need explicit control over the Prefer header.
func supabaseRequest(method, path string, body []byte) (*http.Response, error) {
	prefer := ""
	if body != nil {
		prefer = "resolution=merge-duplicates"
	}
	return supabaseRequestWithPrefer(method, path, body, prefer)
}

// Shared HTTP client with connection pooling for the supabaseRequest helper.
// Avoids creating a new TCP+TLS connection on every call.
var supabaseHTTPClient = &http.Client{Timeout: 60 * time.Second}

// fastHTTPClient is used for startup calls (LoadSettingsFromDB, LoadChannelsFromDB)
// so the web server starts quickly even when Supabase is slow or unreachable.
var fastHTTPClient = &http.Client{Timeout: 10 * time.Second}

// supabaseRequestWithPrefer is the low-level HTTP helper. Pass an empty string
// for prefer to omit the header entirely.
func supabaseRequestWithPrefer(method, path string, body []byte, prefer string) (*http.Response, error) {
	return supabaseRequestWithClient(method, path, body, prefer, supabaseHTTPClient)
}

// supabaseRequestFast is like supabaseRequestWithPrefer but uses a shorter 10s
// timeout.  Used during startup so the web server binds quickly even when
// Supabase is unreachable or slow.
func supabaseRequestFast(method, path string, body []byte, prefer string) (*http.Response, error) {
	return supabaseRequestWithClient(method, path, body, prefer, fastHTTPClient)
}

// supabaseRequestWithClient is the low-level HTTP helper using the given client.
func supabaseRequestWithClient(method, path string, body []byte, prefer string, client *http.Client) (*http.Response, error) {
	baseURL := supabaseRestURL()
	apiKey := supabaseRestAPIKey()
	if baseURL == "" || apiKey == "" {
		return nil, fmt.Errorf("Supabase not configured")
	}

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("apikey", apiKey)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if prefer != "" {
		req.Header.Set("Prefer", prefer)
	}
	return client.Do(req)
}

// CheckSupabase verifies the app_settings table is reachable via the REST API.
func CheckSupabase() error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}
	return client.HealthCheck()
}

// ─── app_settings helpers ─────────────────────────────────────────────────────

// saveJSONSetting writes a JSON value into the app_settings table.
//
// Strategy: PATCH the existing row (returns the updated row as JSON when using
// Prefer: return=representation). If Supabase returns an empty array the key
// does not exist yet, so we fall back to a plain POST INSERT. This is more
// reliable than the upsert POST+on_conflict approach, which silently skips the
// UPDATE when certain Prefer header combinations are used.
func saveJSONSetting(key string, data []byte) error {
	var rawJSON json.RawMessage
	if err := json.Unmarshal(data, &rawJSON); err != nil {
		return fmt.Errorf("parse json: %w", err)
	}

	// Build separate bodies: the UPDATE only touches value; the INSERT needs key too.
	updateBody, err := json.Marshal(map[string]interface{}{"value": rawJSON})
	if err != nil {
		return fmt.Errorf("marshal update body: %w", err)
	}
	insertBody, err := json.Marshal(map[string]interface{}{"key": key, "value": rawJSON})
	if err != nil {
		return fmt.Errorf("marshal insert body: %w", err)
	}

	// Try PATCH first. Ask for the representation so we can tell whether any
	// row was actually matched (empty array ⟹ no row yet).
	patchResp, err := supabaseRequestWithPrefer(
		"PATCH", "/app_settings?key=eq."+key,
		updateBody, "return=representation",
	)
	if err != nil {
		return fmt.Errorf("patch request: %w", err)
	}
	defer patchResp.Body.Close()
	patchRespBody, _ := io.ReadAll(patchResp.Body)
	if patchResp.StatusCode >= 400 {
		return fmt.Errorf("patch returned %d: %s", patchResp.StatusCode, string(patchRespBody))
	} // Supabase returns "[]" when PATCH matched zero rows.
	if strings.TrimSpace(string(patchRespBody)) == "[]" {
		// Row doesn't exist yet — INSERT it.
		// Include resolution=merge-duplicates so a concurrent writer that
		// inserted the row between our PATCH (found none) and this POST
		// does not cause a unique-constraint violation.
		insertResp, err := supabaseRequestWithPrefer("POST", "/app_settings", insertBody, "resolution=merge-duplicates, return=minimal")
		if err != nil {
			return fmt.Errorf("insert request: %w", err)
		}
		defer insertResp.Body.Close()
		if insertResp.StatusCode >= 400 {
			b, _ := io.ReadAll(insertResp.Body)
			return fmt.Errorf("insert returned %d: %s", insertResp.StatusCode, string(b))
		}
		fmt.Printf("[DEBUG] saveJSONSetting(%q): inserted new row\n", key)
		return nil
	}

	fmt.Printf("[DEBUG] saveJSONSetting(%q): updated existing row (%d bytes)\n", key, len(patchRespBody))
	return nil
}

// loadJSONSetting reads a JSON value from the app_settings table via REST.
// Returns nil if the key is not found or on any error.
func loadJSONSetting(key string) []byte {
	return loadJSONSettingWithClient(key, supabaseHTTPClient)
}

// loadJSONSettingFast is like loadJSONSetting but uses the fast (10s) client.
// Used during startup so the web server binds quickly even when Supabase is
// unreachable or slow.
func loadJSONSettingFast(key string) []byte {
	return loadJSONSettingWithClient(key, fastHTTPClient)
}

func loadJSONSettingWithClient(key string, client *http.Client) []byte {
	resp, err := supabaseRequestWithClient("GET",
		"/app_settings?key=eq."+key+"&select=value", nil, "", client)
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

// SaveChannelsToDB saves channels to Supabase.
//
// Primary path (synchronous, authoritative): upserts the entire channel list
// as a single JSON blob in app_settings (key = "channels"). This PATCH is the
// only thing the caller needs to wait for — once it returns, the deletion or
// state change is durable.
//
// Secondary path (async, best-effort): individual channel rows in the channels
// table are kept in sync so FK lookups from recordings still work. This runs in
// a background goroutine so it never blocks the HTTP handler. Stale rows here
// are harmless because LoadChannelsFromDB reads from app_settings first.
func SaveChannelsToDB(data []byte) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	// ── Primary (blocking): update the authoritative channel list blob. ──────
	if err := saveJSONSetting(channelsKey(), data); err != nil {
		return fmt.Errorf("save channels to app_settings: %w", err)
	}

	// ── Secondary (non-blocking): sync individual rows for FK integrity. ─────
	// Deliberately fire-and-forget — a slow or failed upsert must never block
	// the delete/pause/resume HTTP response.
	// NOTE: These rows are shared across instances and are no longer read by
	// LoadChannelsFromDB (the fallback was removed). They are kept only for
	// backward compatibility with external tools that query the channels table.
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	go func() {
		var configs []*entity.ChannelConfig
		if err := json.Unmarshal(dataCopy, &configs); err != nil {
			return
		}
		for _, conf := range configs {
			ch := &database.Channel{
				Username:    conf.Username,
				IsPaused:    conf.IsPaused.Load(),
				Framerate:   conf.Framerate,
				Resolution:  conf.Resolution,
				Pattern:     conf.Pattern,
				MaxDuration: conf.MaxDuration,
				MaxFilesize: conf.MaxFilesize,
				Compress:    conf.Compress,
				CreatedAt:   conf.CreatedAt,
			}
			if err := client.SaveChannel(ch); err != nil {
				fmt.Printf("[WARN] SaveChannelsToDB: failed to sync channel %s to channels table: %v\n", conf.Username, err)
			}
		}
	}()

	return nil
}

// LoadChannelsFromDB loads channels from Supabase.
// It reads from app_settings using the instance-namespaced key (channels_<INSTANCE_ID>),
// which correctly reflects deletions without needing DELETE permission.
// The legacy fallback to the channels table has been removed because the channels
// table is shared across all instances and would leak other instances' channels.
func LoadChannelsFromDB() []byte {
	client := GetDBClient()
	if client == nil {
		return nil
	}

	// Read the instance-namespaced channel list blob from app_settings.
	// Use the fast (10s) client so the web server starts quickly even when
	// Supabase is unreachable or slow.
	if data := loadJSONSettingFast(channelsKey()); data != nil {
		return data
	}

	// No channels configured yet for this instance.
	return nil
}

// ─── Channel Pool (distributed mode) ────────────────────────────────────────

// PoolKey returns the app_settings key for the shared channel pool.
func PoolKey() string { return database.PoolKey() }

// LoadPoolFromDB reads the shared channel pool from app_settings.
func LoadPoolFromDB() []byte {
	client := GetDBClient()
	if client == nil {
		return nil
	}
	pool, err := client.LoadPoolFromDB()
	if err != nil {
		fmt.Printf("[WARN] load channel pool: %v\n", err)
		return nil
	}
	return pool
}

// SavePoolToDB writes the shared channel pool to app_settings.
func SavePoolToDB(data []byte) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}
	return client.SavePoolToDB(data)
}

// ─── Settings ─────────────────────────────────────────────────────────────────

func SaveSettingsToDB(data []byte) error {
	if err := saveJSONSetting("dvr_settings", data); err != nil {
		return fmt.Errorf("save settings to Supabase: %w", err)
	}
	return nil
}

func LoadSettingsFromDB() []byte {
	// Use the fast (10s) client so the web server starts quickly even when
	// Supabase is unreachable or slow.
	return loadJSONSettingFast("dvr_settings")
}

// ─── Recordings ───────────────────────────────────────────────────────────────

// SaveRecordingsToDB saves recordings to Supabase
func SaveRecordingsToDB(data []byte) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	// Parse the JSON data
	type RecordingEntry struct {
		Filename               string            `json:"filename"`
		Timestamp              string            `json:"timestamp"`
		RoomTitle              string            `json:"room_title"`
		Tags                   []string          `json:"tags"`
		Viewers                int               `json:"viewers"`
		Resolution             string            `json:"resolution"`
		Framerate              int               `json:"framerate"`
		Links                  map[string]string `json:"links"`
		ThumbnailURL           string            `json:"thumbnail_url"`
		SpriteURL              string            `json:"sprite_url"`
		PreviewURL             string            `json:"preview_url"`
		EmbedURL               string            `json:"embed_url"`
		SeekStreamingPosterURL  string            `json:"seekstreaming_poster_url"`
		SeekStreamingPreviewURL string            `json:"seekstreaming_preview_url"`
		Filesize               int64             `json:"filesize"`
	}

	type ChannelRecordings struct {
		Gender     string           `json:"gender"`
		Recordings []RecordingEntry `json:"recordings"`
	}

	type RecordingsDB struct {
		Version  int                           `json:"version"`
		Channels map[string]*ChannelRecordings `json:"channels"`
	}

	var db RecordingsDB
	if err := json.Unmarshal(data, &db); err != nil {
		return fmt.Errorf("parse recordings: %w", err)
	}

	for username, chanData := range db.Channels {
		for _, rec := range chanData.Recordings {
			recording := &database.Recording{
				Username:     username,
				Filename:     rec.Filename,
				Timestamp:    rec.Timestamp,
				RoomTitle:    rec.RoomTitle,
				Tags:         rec.Tags,
				Viewers:      rec.Viewers,
				Resolution:   rec.Resolution,
				Framerate:    rec.Framerate,
				Filesize:     rec.Filesize,
				Gender:       chanData.Gender,
				ThumbnailURL: rec.ThumbnailURL,
				SpriteURL:    rec.SpriteURL,
				EmbedURL:     rec.EmbedURL,
			}

			if err := client.SaveRecording(recording); err != nil {
				return fmt.Errorf("save recording %s: %w", rec.Filename, err)
			}

			savedRec, err := client.GetRecording(rec.Filename)
			if err != nil {
				return fmt.Errorf("get recording %s after save: %w", rec.Filename, err)
			}

			for host, url := range rec.Links {
				link := &database.UploadLink{
					RecordingID: savedRec.ID,
					Host:        host,
					URL:         url,
				}
				if err := client.SaveUploadLink(link); err != nil {
					return fmt.Errorf("save upload link %s/%s: %w", rec.Filename, host, err)
				}
			}

		if rec.ThumbnailURL != "" || rec.SpriteURL != "" || rec.PreviewURL != "" {
			img := &database.PreviewImage{
				RecordingID:  savedRec.ID,
				Filename:     rec.Filename,
				ThumbnailURL: rec.ThumbnailURL,
				SpriteURL:    rec.SpriteURL,
				PreviewURL:   rec.PreviewURL,
			}
				if err := client.SavePreviewImage(img); err != nil {
					return fmt.Errorf("save preview image %s: %w", rec.Filename, err)
				}
			}
		}
	}

	InvalidateAllCaches()
	return nil
}

// LoadRecordingsFromDB loads recordings from Supabase
func LoadRecordingsFromDB() []byte {
	if data := cacheGet("recordings"); data != nil {
		return data
	}

	client := GetDBClient()
	if client == nil {
		return nil
	}

	recordings, err := client.GetAllRecordings()
	if err != nil {
		fmt.Printf("[WARN] Failed to load recordings from Supabase: %v\n", err)
		return nil
	}

	// Convert to the old JSON format for compatibility
	type RecordingEntry struct {
		Filename               string            `json:"filename"`
		Timestamp              string            `json:"timestamp"`
		RoomTitle              string            `json:"room_title"`
		Tags                   []string          `json:"tags"`
		Viewers                int               `json:"viewers"`
		Resolution             string            `json:"resolution"`
		Framerate              int               `json:"framerate"`
		Links                  map[string]string `json:"links"`
		ThumbnailURL           string            `json:"thumbnail_url"`
		SpriteURL              string            `json:"sprite_url"`
		PreviewURL             string            `json:"preview_url"`
		EmbedURL               string            `json:"embed_url"`
		SeekStreamingPosterURL  string            `json:"seekstreaming_poster_url"`
		SeekStreamingPreviewURL string            `json:"seekstreaming_preview_url"`
		Filesize               int64             `json:"filesize"`
	}

	type ChannelRecordings struct {
		Gender     string           `json:"gender"`
		Recordings []RecordingEntry `json:"recordings"`
	}

	type RecordingsDB struct {
		Version  int                           `json:"version"`
		Channels map[string]*ChannelRecordings `json:"channels"`
	}

	// Batch-fetch all upload links at once, grouped by recording_id
	allUploadLinks := map[string]map[string]string{} // recording_id → {host: url}
	if allLinks, err := client.GetAllUploadLinks(); err == nil {
		for _, link := range allLinks {
			m := allUploadLinks[link.RecordingID]
			if m == nil {
				m = make(map[string]string)
				allUploadLinks[link.RecordingID] = m
			}
			m[link.Host] = link.URL
		}
	}

	db := RecordingsDB{
		Version:  2,
		Channels: make(map[string]*ChannelRecordings),
	}

	// Group recordings by username
	for _, rec := range recordings {
		chanData, ok := db.Channels[rec.Username]
		if !ok {
			chanData = &ChannelRecordings{
				Gender:     rec.Gender,
				Recordings: []RecordingEntry{},
			}
			db.Channels[rec.Username] = chanData
		}

		// Look up links from batch map (O(1), no per-recording API call)
		links := allUploadLinks[rec.ID]
		if links == nil {
			links = make(map[string]string)
		}

		entry := RecordingEntry{
			Filename:               rec.Filename,
			Timestamp:              rec.Timestamp,
			RoomTitle:              rec.RoomTitle,
			Tags:                   rec.Tags,
			Viewers:                rec.Viewers,
			Resolution:             rec.Resolution,
			Framerate:              rec.Framerate,
			Links:                  links,
			ThumbnailURL:           rec.ThumbnailURL,
			SpriteURL:              rec.SpriteURL,
			PreviewURL:             rec.PreviewURL,
			EmbedURL:               rec.EmbedURL,
			SeekStreamingPosterURL:  rec.SeekStreamingPosterURL,
			SeekStreamingPreviewURL: rec.SeekStreamingPreviewURL,
			Filesize:               rec.Filesize,
		}

		chanData.Recordings = append(chanData.Recordings, entry)
	}

	data, err := json.Marshal(db)
	if err != nil {
		fmt.Printf("[WARN] Failed to marshal recordings: %v\n", err)
		return nil
	}

	cacheSet("recordings", data, 5*time.Minute)
	return data
}

func RecordingExists(filename string) bool {
	client := GetDBClient()
	if client == nil {
		return false
	}
	_, err := client.GetRecording(filename)
	return err == nil
}

// SaveRecordingWithLinks saves a recording and its upload links directly to Supabase.
// Preview URLs should be saved separately via SavePreviewLinks before calling this.
// This function only saves the recording metadata and upload links.
func SaveRecordingWithLinks(username, filename, timestamp, roomTitle string, tags []string, viewers int, resolution string, framerate int, filesize int64, duration float64, gender, embedURL, thumbnailURL, spriteURL, previewURL string, links map[string]string, seekPosterURL ...string) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	var seekPoster, seekPreview string
	if len(seekPosterURL) > 0 {
		seekPoster = seekPosterURL[0]
	}
	if len(seekPosterURL) > 1 {
		seekPreview = seekPosterURL[1]
	}

	rec := &database.Recording{
		Username:               username,
		Filename:               filename,
		Timestamp:              timestamp,
		RoomTitle:              roomTitle,
		Tags:                   tags,
		Viewers:                viewers,
		Resolution:             resolution,
		Framerate:              framerate,
		Filesize:               filesize,
		Duration:               duration,
		Gender:                 gender,
		EmbedURL:               embedURL,
		ThumbnailURL:           thumbnailURL,
		SpriteURL:              spriteURL,
		PreviewURL:             previewURL,
		SeekStreamingPosterURL:  seekPoster,
		SeekStreamingPreviewURL: seekPreview,
	}
	// Skip channel_id lookup — the channels table is shared across instances
	// and the FK would point to the wrong instance's row.
	// Recordings are uniquely identified by filename, so channel_id is cosmetic.

	// Save recording first (try with duration, fall back without if column missing)
	if err := client.SaveRecording(rec); err != nil && strings.Contains(err.Error(), "PGRST204") {
		fmt.Printf("[WARN] duration column missing in Supabase — saving without duration: %v\n", err)
		rec.Duration = 0
		if err := client.SaveRecording(rec); err != nil {
			return fmt.Errorf("save recording (fallback): %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("save recording: %w", err)
	}

	// Get the saved recording to get its ID for upload links
	savedRec, err := client.GetRecording(filename)
	if err != nil {
		return fmt.Errorf("get recording after save: %w", err)
	}

	// Save upload links — batch upsert is atomic: either all succeed or
	// none do, so partial failures cannot orphan individual host URLs.
	var uploadLinks []database.UploadLink
	for host, url := range links {
		uploadLinks = append(uploadLinks, database.UploadLink{
			RecordingID: savedRec.ID,
			Host:        host,
			URL:         url,
		})
	}
	if len(uploadLinks) > 0 {
		if err := client.SaveUploadLinks(uploadLinks); err != nil {
			return fmt.Errorf("save upload links: %w", err)
		}
	}

	cacheClear()
	return nil
}

// SaveRecordingBasics saves minimal recording metadata before upload starts.
// This ensures the recording is never lost even if the process is killed
// during upload. The full metadata (thumbnails, upload links) is saved
// later by stageSaveMetadata via the upsert on filename.
func SaveRecordingBasics(username, filename, timestamp, roomTitle string, tags []string, viewers int, gender, resolution string, framerate int, filesize int64, duration float64) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}
	rec := &database.Recording{
		Username:   username,
		Filename:   filename,
		Timestamp:  timestamp,
		RoomTitle:  roomTitle,
		Tags:       tags,
		Viewers:    viewers,
		Gender:     gender,
		Resolution: resolution,
		Framerate:  framerate,
		Filesize:   filesize,
		Duration:   duration,
	}
	if err := client.SaveRecording(rec); err != nil {
		return err
	}
	return nil
}

// ─── Pipeline States ──────────────────────────────────────────────────────────

// SavePipelineState persists the current pipeline state for crash recovery.
func SavePipelineState(state *database.PipelineState) error {
	client := GetDBClient()
	if client == nil {
		return nil
	}
	err := client.SavePipelineState(state)
	if err == nil {
		InvalidatePipelineStatesCache()
	}
	return err
}

// LoadAllPipelineStates returns all incomplete pipeline states for recovery.
func LoadAllPipelineStates() ([]database.PipelineState, error) {
	client := GetDBClient()
	if client == nil {
		return nil, nil
	}
	return client.LoadAllPipelineStates()
}

// pipelineStatesCache caches the (slow-changing) pipeline-state list so the
// reaper loop doesn't re-scan the whole Supabase table on every tick. The
// reaper runs every 10m and previously caused tens of thousands of sequential
// table scans; a short TTL keeps the data fresh while cutting DB load ~10x.
var (
	pipelineStatesCacheMu sync.Mutex
	pipelineStatesCache   []database.PipelineState
	pipelineStatesExpiry  time.Time
)

const pipelineStatesCacheTTL = 90 * time.Second

// LoadAllPipelineStatesCached returns the cached pipeline states, refreshing
// from Supabase only when the cache is empty or expired.
func LoadAllPipelineStatesCached() ([]database.PipelineState, error) {
	pipelineStatesCacheMu.Lock()
	defer pipelineStatesCacheMu.Unlock()
	if time.Now().Before(pipelineStatesExpiry) && pipelineStatesCache != nil {
		return pipelineStatesCache, nil
	}
	states, err := LoadAllPipelineStates()
	if err != nil {
		// On error, serve stale data if we have any rather than hammering DB.
		if pipelineStatesCache != nil {
			return pipelineStatesCache, nil
		}
		return nil, err
	}
	pipelineStatesCache = states
	pipelineStatesExpiry = time.Now().Add(pipelineStatesCacheTTL)
	return states, nil
}

// InvalidatePipelineStatesCache forces the next read to hit Supabase. Call
// after mutating (save/delete) a pipeline state.
func InvalidatePipelineStatesCache() {
	pipelineStatesCacheMu.Lock()
	pipelineStatesCache = nil
	pipelineStatesExpiry = time.Time{}
	pipelineStatesCacheMu.Unlock()
}

// DeletePipelineState removes a completed or failed pipeline state.
func DeletePipelineState(fileHash string) error {
	client := GetDBClient()
	if client == nil {
		return nil
	}
	err := client.DeletePipelineState(fileHash)
	if err == nil {
		InvalidatePipelineStatesCache()
	}
	return err
}

// ─── Tunnels ──────────────────────────────────────────────────────────────────

// SaveTunnelToDB saves a tunnel URL to Supabase
func SaveTunnelToDB(tunnelURL string, runID int) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	if err := client.DeactivateOldTunnels(instanceID); err != nil {
		fmt.Printf("[WARN] failed to deactivate old tunnels: %v\n", err)
	}

	tunnel := &database.Tunnel{
		URL:        tunnelURL,
		RunID:      runID,
		InstanceID: instanceID,
		IsActive:   true,
	}

	if err := client.SaveTunnel(tunnel); err != nil {
		return err
	}

	// Also mirror the tunnel URL onto this node's row so the admin panel's
	// per-node "Visit" link reflects the current public address.  The cloudflared
	// Quick Tunnel URL rotates every run, so this must be refreshed each session.
	if nodeID := NodeID(); nodeID != "" {
		if err := client.UpdateNodeWebURL(nodeID, tunnelURL); err != nil {
			fmt.Printf("[WARN] failed to update node web_url: %v\n", err)
		} else {
			fmt.Printf("[OK] updated node %s web_url to %s\n", nodeID, tunnelURL)
		}
	}
	return nil
}

// LoadCurrentTunnel loads the active tunnel URL from Supabase
func LoadCurrentTunnel() (string, error) {
	client := GetDBClient()
	if client == nil {
		return "", nil
	}

	tunnel, err := client.GetActiveTunnel(instanceID)
	if err != nil {
		return "", err
	}

	return tunnel.URL, nil
}

// ─── Preview Links ────────────────────────────────────────────────────────────

// SavePreviewLinks saves preview image URLs to Supabase
func SavePreviewLinks(filename, thumbnailURL, spriteURL, previewURL string) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	img := &database.PreviewImage{
		Filename:     filename,
		ThumbnailURL: thumbnailURL,
		SpriteURL:    spriteURL,
		PreviewURL:   previewURL,
		UploadedAt:   time.Now().UTC().Format("2006-01-02T15:04:05Z"),
	}

	if err := client.SavePreviewImage(img); err != nil {
		return err
	}

	InvalidateAllCaches()
	return nil
}

// LoadPreviewLinks loads preview image URLs from Supabase
func LoadPreviewLinks(filename string) (thumbnailURL, spriteURL, previewURL string) {
	client := GetDBClient()
	if client == nil {
		return "", "", ""
	}

	img, err := client.GetPreviewImage(filename)
	if err != nil {
		return "", "", ""
	}

	return img.ThumbnailURL, img.SpriteURL, img.PreviewURL
}

// LoadAllPreviewLinks returns a map of filename -> [thumbnailURL, spriteURL, previewURL] for all preview images.
// Use this instead of calling LoadPreviewLinks in a loop to avoid N+1 queries.
func LoadAllPreviewLinks() map[string][3]string {
	if data := cacheGet("preview_links"); data != nil {
		var result map[string][3]string
		if err := json.Unmarshal(data, &result); err == nil {
			return result
		}
	}

	client := GetDBClient()
	if client == nil {
		return nil
	}

	images, err := client.GetAllPreviewImages()
	if err != nil {
		fmt.Printf("[WARN] Failed to load all preview images: %v\n", err)
		return nil
	}

	result := make(map[string][3]string, len(images))
	for _, img := range images {
		if img.Filename != "" && (img.ThumbnailURL != "" || img.SpriteURL != "" || img.PreviewURL != "") {
			result[img.Filename] = [3]string{img.ThumbnailURL, img.SpriteURL, img.PreviewURL}
		}
	}

	if data, err := json.Marshal(result); err == nil {
		cacheSet("preview_links", data, 5*time.Minute)
	}
	return result
}

// DeleteChannelFromDB removes a channel record from Supabase.
func DeleteChannelFromDB(username string) error {
	client := GetDBClient()
	if client == nil {
		return nil
	}
	return client.DeleteChannel(username)
}

// DeleteChannelsNotInDB removes all Supabase channel rows whose username is NOT
// in the provided list. Pass an empty slice to delete all channels.
func DeleteChannelsNotInDB(usernames []string) error {
	client := GetDBClient()
	if client == nil {
		return nil
	}
	return client.DeleteChannelsNotIn(usernames)
}

// UpdateRecordingThumbnails patches the thumbnail_url, sprite_url and preview_url on an
// existing recording row identified by filename.
func UpdateRecordingThumbnails(filename, thumbnailURL, spriteURL, previewURL string) error {
	if thumbnailURL == "" && spriteURL == "" && previewURL == "" {
		return nil
	}
	body, err := json.Marshal(map[string]string{
		"thumbnail_url": thumbnailURL,
		"sprite_url":    spriteURL,
		"preview_url":   previewURL,
	})
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	resp, err := supabaseRequest("PATCH",
		fmt.Sprintf("/recordings?filename=eq.%s", url.QueryEscape(filename)),
		body,
	)
	if err != nil {
		return fmt.Errorf("patch request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

// DeleteVideoCompletely removes all data associated with a video:
// - Recording from Supabase recordings table
// - Preview images from Supabase preview_images table
// - Upload links from Supabase upload_links table
// Returns a combined error if any deletion fails.
func DeleteVideoCompletely(filename string) error {
	client := GetDBClient()
	if client == nil {
		return nil // No DB configured, nothing to delete
	}

	var errs []string

	// Get recording ID first (needed for upload links)
	rec, err := client.GetRecording(filename)
	if err == nil && rec != nil {
		// Delete upload links by recording ID
		if err := client.DeleteUploadLinksByRecordingID(rec.ID); err != nil {
			errs = append(errs, fmt.Sprintf("upload links: %v", err))
		}
	}

	// Delete preview images
	if err := client.DeletePreviewImage(filename); err != nil {
		errs = append(errs, fmt.Sprintf("preview images: %v", err))
	}

	// Delete recording
	if err := client.DeleteRecording(filename); err != nil {
		errs = append(errs, fmt.Sprintf("recording: %v", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("delete errors: %s", strings.Join(errs, "; "))
	}

	cacheClear()
	return nil
}

// ─── Upload Journal ───────────────────────────────────────────────────────────

// SaveJournalEntry records the upload state for a file on a specific host.
func SaveJournalEntry(fileHash, filename, host, status string, fileSize int64, errMsg string) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	entry := &database.UploadJournal{
		FileHash:   fileHash,
		Filename:   filename,
		Host:       host,
		Status:     status,
		ErrorMsg:   errMsg,
		FileSize:   fileSize,
		InstanceID: instanceID,
	}

	return client.SaveJournalEntry(entry)
}

// LoadJournalByHash returns all journal entries for a given file hash.
func LoadJournalByHash(fileHash string) ([]database.UploadJournal, error) {
	client := GetDBClient()
	if client == nil {
		return nil, fmt.Errorf("Supabase not configured")
	}
	return client.GetJournalByHash(fileHash)
}

// LoadCompletedHosts returns the list of hosts that have successfully received
// the file identified by fileHash.
func LoadCompletedHosts(fileHash string) ([]string, error) {
	entries, err := LoadJournalByHash(fileHash)
	if err != nil {
		return nil, err
	}
	var hosts []string
	for _, e := range entries {
		if e.Status == "success" {
			hosts = append(hosts, e.Host)
		}
	}
	return hosts, nil
}

// DeleteJournalByHash removes all journal entries for a file hash.
func DeleteJournalByHash(fileHash string) error {
	client := GetDBClient()
	if client == nil {
		return nil
	}
	return client.DeleteJournalByHash(fileHash)
}
