package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
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
	syncNodeEnvironment()
}

// syncNodeEnvironment recomputes the cached node identity and pool mode from
// the CURRENT process environment. init() runs before .env is loaded (main.go
// calls loadDotEnv after package init), so NODE_ID / GITHUB_REPOSITORY values
// that live only in .env would otherwise be missed and the cached NodeID()
// would silently diverge from detectNodeID() (used everywhere else). Call this
// once after loadDotEnv to keep NodeID(), the cookie settings key, and the
// coordinator's detectNodeID() consistent.
func syncNodeEnvironment() {
	nodeID = detectNodeID()
	channelPoolMode = detectPoolMode()
}

// DBInstanceID returns the value stamped on Supabase rows for per-node
// attribution. An explicit INSTANCE_ID env var wins; otherwise the
// auto-detected node ID (node-1..node-N) is used instead of the generic
// "default", so recordings / upload links / previews / tunnel / journal rows
// can be attributed to the node that produced them.
//
// NOTE: this deliberately does NOT change the package-level instanceID var —
// channelsKey() derives the local channel-list blob key from it, and silently
// switching that to a per-node value would orphan every node's existing
// "channels_default" config. Attribution stamps use DBInstanceID instead.
func DBInstanceID() string {
	if instanceID != "" && instanceID != "default" {
		return instanceID
	}
	if id := NodeID(); id != "" {
		return id
	}
	return instanceID
}

// detectPoolMode auto-detects pooled mode:
// 1. CHANNEL_POOL_MODE env var (explicit override)
// 2. GITHUB_REPOSITORY env var — auto-enable if path contains "node-"
// 3. Default to "isolated"
func detectPoolMode() string {
	if mode := os.Getenv("CHANNEL_POOL_MODE"); mode != "" {
		return mode
	}
	if repo := os.Getenv("GITHUB_REPOSITORY"); repo != "" {
		// Auto-enable pooled mode for repos named node-*
		if strings.Contains(repo, "node-") {
			return entity.PoolModePooled
		}
	}
	return entity.PoolModeIsolated
}

// detectNodeID auto-detects the node identity using a priority chain:
// 1. NODE_ID env var (explicit)
// 2. GITHUB_REPOSITORY env var (set by GitHub Actions)
// 3. os.Hostname() (VPS / local)
// 4. Random fallback (defensive)
func detectNodeID() string {
	// Ignore placeholder values (empty/whitespace/"-") so a "-" NODE_ID secret
	// can never register a bogus "-" row in the Supabase nodes table.
	if id := strings.TrimSpace(os.Getenv("NODE_ID")); id != "" && id != "-" {
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
	return fmt.Sprintf("node-%x", time.Now().UnixNano())
}

// NodeID returns the current node's unique identifier.
func NodeID() string { return nodeID }

// SyncNodeEnvironment re-derives the cached node id / pool mode after .env has
// been loaded. See syncNodeEnvironment.
func SyncNodeEnvironment() { syncNodeEnvironment() }

// ChannelPoolMode returns the current channel pool mode ("isolated" or "pooled").
func ChannelPoolMode() string { return channelPoolMode }

// IsPooledMode returns true if the system is running in distributed pool mode.
func IsPooledMode() bool { return channelPoolMode == "pooled" }

func channelsKey() string {
	return "channels_" + instanceID
}

// CookieSettingsKey returns the app_settings key that stores THIS node's
// cookies and user-agent. Cookies (especially cf_clearance) are IP + TLS-bound,
// so a single shared "dvr_settings" blob mints on one node's IP then 403s on
// every other node. Each node reads/writes only its own "dvr_settings:<node_id>".
// Non-cookie upload credentials stay in the shared global key.
func CookieSettingsKey() string {
	id := detectNodeID()
	if id == "" || id == "-" {
		id = "unknown"
	}
	return "dvr_settings:" + id
}

// ─── Supabase client ──────────────────────────────────────────────────────────

var dbClient *database.Client

// supabaseKey returns the service_role key when configured (bypasses RLS so
// writes succeed), falling back to the anon/public key.
func supabaseKey() string {
	if Config != nil && Config.SupabaseServiceRoleKey != "" {
		return Config.SupabaseServiceRoleKey
	}
	return supabaseRestAPIKey()
}

// ResetDBClient drops the cached Supabase client so the next GetDBClient call
// rebuilds it from the current Config.  Needed whenever Config changes at runtime
// (and by callers that point the fleet at a different project).
func ResetDBClient() {
	dbClient = nil
}

// GetDBClient returns the Supabase database client
func GetDBClient() *database.Client {
	if dbClient == nil && Config != nil && Config.SupabaseURL != "" && Config.SupabaseAPIKey != "" {
		dbClient = database.NewClient(Config.SupabaseURL, supabaseKey())
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
// headers and retry logic for transient errors. For writes (POST/PATCH/DELETE
// with a body) it sets Prefer: resolution=merge-duplicates. Use
// supabaseRequestWithPrefer when you need explicit control over the Prefer header.
func supabaseRequest(method, path string, body []byte) (*http.Response, error) {
	prefer := ""
	if body != nil {
		prefer = "resolution=merge-duplicates"
	}
	return supabaseRequestWithRetry(method, path, body, prefer, supabaseHTTPClient)
}

// Shared HTTP client with connection pooling for the supabaseRequest helper.
// Avoids creating a new TCP+TLS connection on every call.
var supabaseHTTPClient = &http.Client{Timeout: 60 * time.Second}

// fastHTTPClient is used for startup calls (LoadSettingsFromDB, LoadChannelsFromDB)
// so the web server starts quickly even when Supabase is slow or unreachable.
var fastHTTPClient = &http.Client{Timeout: 10 * time.Second}

// supabaseRequestWithPrefer is the low-level HTTP helper with retry. Pass an empty string
// for prefer to omit the header entirely.
func supabaseRequestWithPrefer(method, path string, body []byte, prefer string) (*http.Response, error) {
	return supabaseRequestWithRetry(method, path, body, prefer, supabaseHTTPClient)
}

// supabaseRequestFast is like supabaseRequestWithPrefer but uses a shorter 10s
// timeout and NO retry. Used during startup so the web server binds quickly even when
// Supabase is unreachable or slow.
func supabaseRequestFast(method, path string, body []byte, prefer string) (*http.Response, error) {
	return supabaseRequestWithClient(method, path, body, prefer, fastHTTPClient)
}

// supabaseRequestWithClient is the low-level HTTP helper using the given client.
func supabaseRequestWithClient(method, path string, body []byte, prefer string, client *http.Client) (*http.Response, error) {
	baseURL := supabaseRestURL()
	apiKey := supabaseKey()
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

// supabaseRequestWithRetry executes the request and retries on transient errors:
// - 503 PGRST002 — schema cache rebuilding after migration
// - 400 PGRST204 — column not in schema cache yet
// - 408 Request Timeout
// - 429 Too Many Requests
// - 5xx Server Errors
func supabaseRequestWithRetry(method, path string, body []byte, prefer string, client *http.Client) (*http.Response, error) {
	const maxRetries = 3
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := supabaseRequestWithClient(method, path, body, prefer, client)
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				backoff := supabaseRetryBackoff(attempt)
				fmt.Printf("[WARN] Supabase request failed (attempt %d/%d), retrying in %v: %v\n", attempt+1, maxRetries, backoff, err)
				time.Sleep(backoff)
				continue
			}
			return nil, err
		}

		// Check for transient errors that need retry
		if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 || resp.StatusCode == 400 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			bodyStr := string(bodyBytes)

			// Schema-cache transients via the shared classifier (PGRST002
			// 503 rebuilding, PGRST204 400 column missing, 42P10 400 ON
			// CONFLICT constraint missing — this copy previously lacked the
			// 42P10 case; single owner fixes that too). The error names the
			// failing method+table (plan D3).
			if code := database.IsSchemaCacheStale(bodyStr); code != "" {
				lastErr = fmt.Errorf("HTTP %d [%s] %s %s: %s", resp.StatusCode, code, method, database.RestTable(path), bodyStr)
				backoff := supabaseRetryBackoff(attempt)
				fmt.Printf("[WARN] Supabase schema cache stale [%s] %s %s (attempt %d/%d), retrying in %v\n", code, method, database.RestTable(path), attempt+1, maxRetries, backoff)
				resp.Body.Close()
				time.Sleep(backoff)
				continue
			}

			// Non-retryable error — return as-is
			if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
				lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, bodyStr)
				resp.Body.Close()
				if attempt < maxRetries-1 {
					backoff := supabaseRetryBackoff(attempt)
					fmt.Printf("[WARN] Supabase transient HTTP %d (attempt %d/%d), retrying in %v\n", resp.StatusCode, attempt+1, maxRetries, backoff)
					time.Sleep(backoff)
					continue
				}
				return nil, lastErr
			}

			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, bodyStr)
		}

		return resp, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

func supabaseRetryBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 5 {
		attempt = 5
	}
	return time.Duration(1<<attempt) * 2 * time.Second
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
	// The key (e.g. "dvr_settings:node-99") must be URL-escaped so reserved
	// characters (":","&","=",...) never corrupt the query filter.
	patchResp, err := supabaseRequestWithPrefer(
		"PATCH", "/app_settings?key=eq."+url.QueryEscape(key),
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
		"/app_settings?key=eq."+url.QueryEscape(key)+"&select=value", nil, "", client)
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

// ─── Central session duration ────────────────────────────────────────────────

// sessionDurationKey is the app_settings key holding the single central
// recording-session length shared by every node. Storing it apart from the
// per-node dvr_settings blob means nodes never overwrite it with their own
// (possibly empty) env value on startup.
const sessionDurationKey = "session_duration"

// SaveSessionDurationToDB persists the central session duration (a Go duration
// string such as "5h20m0s") to Supabase. Empty values are ignored so a node
// can never wipe the shared setting.
func SaveSessionDurationToDB(value string) error {
	if value == "" {
		return nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal session duration: %w", err)
	}
	return saveJSONSetting(sessionDurationKey, b)
}

// LoadSessionDurationFromDB returns the central session duration string, or ""
// if unset/unreachable. Uses the fast startup client so node boot is not
// blocked on a slow Supabase round trip.
func LoadSessionDurationFromDB() string {
	b := loadJSONSettingFast(sessionDurationKey)
	if b == nil {
		return ""
	}
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		// Tolerate a value stored without surrounding JSON quotes.
		return strings.Trim(string(b), "\" \t\r\n")
	}
	return strings.TrimSpace(v)
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

// SaveSettingsToDBForKey writes the supplied JSON settings blob into the
// given app_settings key (PATCH + INSERT fallback).
func SaveSettingsToDBForKey(key string, data []byte) error {
	if err := saveJSONSetting(key, data); err != nil {
		return fmt.Errorf("save settings to Supabase (%s): %w", key, err)
	}
	return nil
}

// LoadSettingsFromDBKey reads a settings blob from the given app_settings key.
func LoadSettingsFromDBKey(key string) []byte {
	return loadJSONSettingWithClient(key, fastHTTPClient)
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
		Filename         string            `json:"filename"`
		Timestamp        string            `json:"timestamp"`
		RoomTitle        string            `json:"room_title"`
		Tags             []string          `json:"tags"`
		Viewers          int               `json:"viewers"`
		Resolution       string            `json:"resolution"`
		Framerate        int               `json:"framerate"`
		Links            map[string]string `json:"links"`
		ThumbnailURL     string            `json:"thumbnail_url"`
		SpriteURL        string            `json:"sprite_url"`
		PreviewURL       string            `json:"preview_url"`
		ThumbnailMirrors map[string]string `json:"thumbnail_mirrors,omitempty"`
		SpriteMirrors    map[string]string `json:"sprite_mirrors,omitempty"`
		PreviewMirrors   map[string]string `json:"preview_mirrors,omitempty"`
		EmbedURL         string            `json:"embed_url"`
		Filesize         int64             `json:"filesize"`
		EndReason        string            `json:"end_reason,omitempty"`
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
				Username:         username,
				Filename:         rec.Filename,
				Timestamp:        rec.Timestamp,
				RoomTitle:        rec.RoomTitle,
				Tags:             rec.Tags,
				Viewers:          rec.Viewers,
				Resolution:       rec.Resolution,
				Framerate:        rec.Framerate,
				Filesize:         rec.Filesize,
				Gender:           chanData.Gender,
				ThumbnailURL:     rec.ThumbnailURL,
				SpriteURL:        rec.SpriteURL,
				EmbedURL:         rec.EmbedURL,
				ThumbnailMirrors: rec.ThumbnailMirrors,
				SpriteMirrors:    rec.SpriteMirrors,
				PreviewMirrors:   rec.PreviewMirrors,
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

			if rec.ThumbnailURL != "" || rec.SpriteURL != "" {
				img := &database.PreviewImage{
					RecordingID:      savedRec.ID,
					Filename:         rec.Filename,
					ThumbnailURL:     rec.ThumbnailURL,
					SpriteURL:        rec.SpriteURL,
					ThumbnailMirrors: rec.ThumbnailMirrors,
					SpriteMirrors:    rec.SpriteMirrors,
					PreviewMirrors:   rec.PreviewMirrors,
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
		Filename         string            `json:"filename"`
		Timestamp        string            `json:"timestamp"`
		RoomTitle        string            `json:"room_title"`
		Tags             []string          `json:"tags"`
		Viewers          int               `json:"viewers"`
		Resolution       string            `json:"resolution"`
		Framerate        int               `json:"framerate"`
		Links            map[string]string `json:"links"`
		ThumbnailURL     string            `json:"thumbnail_url"`
		SpriteURL        string            `json:"sprite_url"`
		PreviewURL       string            `json:"preview_url"`
		ThumbnailMirrors map[string]string `json:"thumbnail_mirrors,omitempty"`
		SpriteMirrors    map[string]string `json:"sprite_mirrors,omitempty"`
		PreviewMirrors   map[string]string `json:"preview_mirrors,omitempty"`
		EmbedURL         string            `json:"embed_url"`
		Filesize         int64             `json:"filesize"`
		EndReason        string            `json:"end_reason,omitempty"`
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
			Filename:     rec.Filename,
			Timestamp:    rec.Timestamp,
			RoomTitle:    rec.RoomTitle,
			Tags:         rec.Tags,
			Viewers:      rec.Viewers,
			Resolution:   rec.Resolution,
			Framerate:    rec.Framerate,
			Links:        links,
			ThumbnailURL: rec.ThumbnailURL,
			SpriteURL:    rec.SpriteURL,
			PreviewURL:   rec.PreviewURL,
			EmbedURL:     rec.EmbedURL,
			Filesize:     rec.Filesize,
			EndReason:    rec.EndReason,
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

// hashtagRegex matches broadcaster hashtags embedded in room titles (e.g.
// "#lovense #anal"). The Chaturbate API's separate `tags` array is routinely
// empty, so hashtags parsed from the title are the reliable source of tags.
var hashtagRegex = regexp.MustCompile(`#[A-Za-z0-9_]+`)

// mergeHashtags combines scraper-provided tags with #hashtags parsed from the
// room title. Every tag is normalized to lowercase and deduplicated so the tag
// cloud is not fragmented by case variants.
func mergeHashtags(roomTitle string, tags []string) []string {
	seen := make(map[string]struct{}, len(tags)+6)
	out := make([]string, 0, len(tags)+6)
	add := func(tag string) {
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" {
			return
		}
		if _, ok := seen[tag]; ok {
			return
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	for _, t := range tags {
		add(t)
	}
	for _, m := range hashtagRegex.FindAllString(roomTitle, -1) {
		add(m[1:])
	}
	return out
}

// SaveChannelProfile persists scraped full-profile data for a channel into the
// existing channels table. Best-effort: failures are logged and ignored so a
// profile scrape can never break recording.
func SaveChannelProfile(p *database.ChannelProfile) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}
	return client.SaveChannelProfile(p)
}

// SaveRecordingWithLinks saves a recording and its upload links directly to Supabase.
// Preview URLs should be saved separately via SavePreviewLinks before calling this.
// This function only saves the recording metadata and upload links.
func SaveRecordingWithLinks(username, filename, timestamp, roomTitle string, tags []string, viewers int, resolution string, framerate int, filesize int64, duration float64, gender, endReason, embedURL, thumbnailURL, spriteURL, previewURL string, links map[string]string) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	rec := &database.Recording{
		Username:     username,
		Filename:     filename,
		Timestamp:    timestamp,
		RoomTitle:    roomTitle,
		Tags:         mergeHashtags(roomTitle, tags),
		Viewers:      viewers,
		Resolution:   resolution,
		Framerate:    framerate,
		Filesize:     filesize,
		Duration:     duration,
		Gender:       gender,
		EndReason:    endReason,
		EmbedURL:     embedURL,
		ThumbnailURL: thumbnailURL,
		SpriteURL:    spriteURL,
		PreviewURL:   previewURL,
		InstanceID:   DBInstanceID(),
	}

	var uploadLinks []database.UploadLink
	for host, u := range links {
		uploadLinks = append(uploadLinks, database.UploadLink{
			Host:       host,
			URL:        u,
			InstanceID: DBInstanceID(),
		})
	}

	preview := &database.PreviewImage{
		Filename:     filename,
		ThumbnailURL: thumbnailURL,
		SpriteURL:    spriteURL,
		PreviewURL:   previewURL,
		UploadedAt:   time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		InstanceID:   DBInstanceID(),
	}

	// ── Fast path: single atomic RPC (recording + links + preview) ────
	if _, err := client.SaveRecordingWithLinksRPC(rec, uploadLinks, preview); err != nil {
		if strings.Contains(err.Error(), "PGRST204") || strings.Contains(err.Error(), "function save_recording_with_links") {
			return saveRecordingWithLinksLegacy(client, rec, uploadLinks, preview, filename)
		}
		fmt.Printf("[WARN] atomic metadata RPC failed, falling back to sequential save: %v\n", err)
		return saveRecordingWithLinksLegacy(client, rec, uploadLinks, preview, filename)
	}

	cacheClear()
	return nil
}

// saveRecordingWithLinksLegacy is the original sequential path (SaveRecording →
// GetRecording → SaveUploadLinks), kept as fallback when the atomic RPC is not
// yet deployed.  It also persists the preview_images row so that the two-table
// thumbnail invariant (recordings.thumbnail_url ↔ preview_images.thumbnail_url)
// is never broken by a degraded RPC fallback.
func saveRecordingWithLinksLegacy(client *database.Client, rec *database.Recording, uploadLinks []database.UploadLink, preview *database.PreviewImage, filename string) error {
	if err := client.SaveRecording(rec); err != nil && strings.Contains(err.Error(), "PGRST204") {
		fmt.Printf("[WARN] end_reason column missing in Supabase — saving without it: %v\n", err)
		rec.EndReason = ""
		if err := client.SaveRecording(rec); err != nil && strings.Contains(err.Error(), "PGRST204") {
			fmt.Printf("[WARN] duration column missing in Supabase — saving without duration: %v\n", err)
			rec.Duration = 0
			if err := client.SaveRecording(rec); err != nil {
				return fmt.Errorf("save recording (fallback): %w", err)
			}
		} else if err != nil {
			return fmt.Errorf("save recording (fallback): %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("save recording: %w", err)
	}

	savedRec, err := client.GetRecording(filename)
	if err != nil {
		return fmt.Errorf("get recording after save: %w", err)
	}

	for i := range uploadLinks {
		uploadLinks[i].RecordingID = savedRec.ID
	}
	if len(uploadLinks) > 0 {
		if err := client.SaveUploadLinks(uploadLinks); err != nil {
			return fmt.Errorf("save upload links: %w", err)
		}
	}

	// Persist preview_images so the UI's two-table thumbnail join stays intact.
	// The atomic RPC handles this inside its transaction, but the legacy path
	// previously skipped it — causing recordings to carry a thumbnail while
	// preview_images remained empty, which is the root cause of missing
	// thumbnail reports.
	if preview != nil && (preview.ThumbnailURL != "" || preview.SpriteURL != "" || preview.PreviewURL != "") {
		preview.RecordingID = savedRec.ID
		if err := client.SavePreviewImage(preview); err != nil {
			fmt.Printf("[WARN] legacy fallback: could not save preview images for %s: %v\n", filename, err)
		}
	}

	cacheClear()
	return nil
}

// VerifyRecordingThumbnails checks whether the recording identified by filename
// has non-empty thumbnail/sprite/preview URLs in the database.  The pipeline
// cleanup calls this before deleting local files to prevent data loss when DB
// writes fail silently (e.g. RLS blocks the anon key, network timeout).
func VerifyRecordingThumbnails(filename string) (hasThumb, hasSprite, hasPreview bool) {
	client := GetDBClient()
	if client == nil {
		// No DB configured — cannot verify, assume thumbnails are present
		// to avoid blocking cleanup on standalone nodes.
		return true, true, true
	}
	h, s, p, err := client.HasRecordingThumbnails(filename)
	if err != nil {
		fmt.Printf("[WARN] could not verify thumbnails for %s: %v — keeping local file\n", filename, err)
		return false, false, false
	}
	return h, s, p
}

// SaveRecordingBasics saves minimal recording metadata before upload starts.
// This ensures the recording is never lost even if the process is killed
// during upload. The full metadata (thumbnails, upload links) is saved
// later by stageSaveMetadata via the upsert on filename.
func SaveRecordingBasics(username, filename, timestamp, roomTitle string, tags []string, viewers int, gender, endReason, resolution string, framerate int, filesize int64, duration float64) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}
	rec := &database.Recording{
		Username:   username,
		Filename:   filename,
		Timestamp:  timestamp,
		RoomTitle:  roomTitle,
		Tags:       mergeHashtags(roomTitle, tags),
		Viewers:    viewers,
		Gender:     gender,
		EndReason:  endReason,
		Resolution: resolution,
		Framerate:  framerate,
		Filesize:   filesize,
		Duration:   duration,
		InstanceID: DBInstanceID(),
	}
	// Fall back gracefully when the end_reason column does not exist yet.
	if err := client.SaveRecording(rec); err != nil && strings.Contains(err.Error(), "PGRST204") {
		fmt.Printf("[WARN] end_reason column missing in Supabase — saving basics without it: %v\n", err)
		rec.EndReason = ""
		if err := client.SaveRecording(rec); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	return nil
}

// GetRecordingID returns the Supabase ID for a recording by filename.
// Call once and reuse the result for all per-host saves to avoid N+1
// GetRecording queries.
func GetRecordingID(filename string) (string, error) {
	client := GetDBClient()
	if client == nil {
		return "", fmt.Errorf("Supabase not configured")
	}
	rec, err := client.GetRecording(filename)
	if err != nil {
		return "", fmt.Errorf("get recording for upload link: %w", err)
	}
	return rec.ID, nil
}

// SaveUploadLinkByID persists a single upload link to Supabase immediately.
// The recordingID should be obtained once via GetRecordingID and reused for
// all hosts to avoid redundant lookups.
func SaveUploadLinkByID(recordingID, host, url string) error {
	return SaveUploadLinkByIDWithFilename(recordingID, host, url, "")
}

// SaveUploadLinkByIDWithFilename persists a single upload link and, when the
// recordings row was deleted mid-upload (FK 23503), re-creates it from the
// filename before retrying once.  The delete races are real: orphan cleanup
// on another node can remove a row this node is actively uploading against
// (observed live: "Key (recording_id) is not present in table \"recordings\"").
// The uploaded video is the source of truth — its link must not be lost to a
// stale cleanup decision.  A re-created row gets a NEW id, so the caller's
// stale recordingID is re-resolved by filename before the retry.
func SaveUploadLinkByIDWithFilename(recordingID, host, url, filename string) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}
	err := client.SaveUploadLinks([]database.UploadLink{{
		RecordingID: recordingID,
		Host:        host,
		URL:         url,
		InstanceID:  DBInstanceID(),
	}})
	if err == nil || filename == "" || !strings.Contains(err.Error(), "23503") {
		return err
	}
	// Row vanished mid-upload: re-create the basics (upsert-by-filename is
	// idempotent if the row was concurrently recreated by someone else) and
	// resolve the (possibly new) ID.
	if err := SaveRecordingBasics(extractUsernameFromFilenameParts(filename), filename, time.Now().UTC().Format("2006-01-02T15:04:05Z"), "", nil, 0, "", "", "", 0, 0, 0); err != nil {
		return fmt.Errorf("upload link lost (recording row deleted mid-upload; recreate failed): %w", err)
	}
	newID, idErr := GetRecordingID(filename)
	if idErr != nil || newID == "" {
		return fmt.Errorf("upload link lost (recording row deleted mid-upload; re-resolve failed): %w", idErr)
	}
	return client.SaveUploadLinks([]database.UploadLink{{
		RecordingID: newID,
		Host:        host,
		URL:         url,
		InstanceID:  DBInstanceID(),
	}})
}

// extractUsernameFromFilenameParts derives the channel username from a
// recording filename of the form "<username>_<timestamp>.mp4" (best-effort).
func extractUsernameFromFilenameParts(filename string) string {
	base := filepath.Base(filename)
	if i := strings.LastIndex(base, "."); i > 0 {
		base = base[:i]
	}
	// Filename format is username_YYYY-MM-DD_HH-MM-SS; the timestamp part
	// contains two underscore-separated date/time tokens.
	parts := strings.Split(base, "_")
	if len(parts) > 3 {
		base = strings.Join(parts[:len(parts)-3], "_")
	}
	return base
}

// ─── Pipeline States ──────────────────────────────────────────────────────────

// SavePipelineState persists the current pipeline state for crash recovery.
func SavePipelineState(state *database.PipelineState) error {
	client := GetDBClient()
	if client == nil {
		return nil
	}
	return client.SavePipelineState(state)
}

// LoadAllPipelineStates returns all incomplete pipeline states for recovery.
func LoadAllPipelineStates() ([]database.PipelineState, error) {
	client := GetDBClient()
	if client == nil {
		return nil, nil
	}
	return client.LoadAllPipelineStates()
}

// DeletePipelineState removes a completed or failed pipeline state.
func DeletePipelineState(fileHash string) error {
	client := GetDBClient()
	if client == nil {
		return nil
	}
	return client.DeletePipelineState(fileHash)
}

// ─── Orphan Cleanup ─────────────────────────────────────────────────────

// CleanupOrphanedRecordings deletes DB rows for recordings that have no
// thumbnail_url, no embed_url, and no upload links, AND are older than
// orphanAge.  These are created when SaveRecordingBasics runs at enqueue
// time but the runner is killed before the pipeline completes.  The
// threshold avoids touching recordings that are still being processed.
func CleanupOrphanedRecordings(orphanAge time.Duration) int {
	client := GetDBClient()
	if client == nil {
		return 0
	}
	deleted, err := client.DeleteOrphanedRecordings(orphanAge)
	if err != nil {
		fmt.Printf("[startup] orphan cleanup: %v\n", err)
		return 0
	}
	return deleted
}

// CleanupOldLogs prunes pure telemetry tables (channel_logs, disk_usage) older
// than retentionDays.  It NEVER touches recordings, upload_links,
// preview_images, upload_journal, channels, channel_assignments,
// pipeline_states, nodes or any other important metadata.  retentionDays <= 0
// disables the sweep.  Returns total rows pruned across both log tables.
func CleanupOldLogs(retentionDays int) int {
	if retentionDays <= 0 {
		return 0
	}
	client := GetDBClient()
	if client == nil {
		return 0
	}
	cutoff := time.Now().UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
	total := 0
	if n, err := client.DeleteChannelLogsBefore(cutoff); err != nil {
		fmt.Printf("[WARN] log retention (channel_logs): %v\n", err)
	} else {
		total += n
	}
	if n, err := client.DeleteDiskUsageBefore(cutoff); err != nil {
		fmt.Printf("[WARN] log retention (disk_usage): %v\n", err)
	} else {
		total += n
	}
	return total
}

// ─── Tunnels ──────────────────────────────────────────────────────────────────

// SaveTunnelToDB saves a tunnel URL to Supabase
func SaveTunnelToDB(tunnelURL string, runID int) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	if err := client.DeactivateOldTunnels(DBInstanceID()); err != nil {
		fmt.Printf("[WARN] failed to deactivate old tunnels: %v\n", err)
	}

	tunnel := &database.Tunnel{
		URL:        tunnelURL,
		RunID:      runID,
		InstanceID: DBInstanceID(),
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
		}
	}
	return nil
}

// LoadCurrentTunnel loads the active tunnel URL from Supabase, falling back
// to the node's own web_url when no active tunnels row exists.  The tunnel URL
// is mirrored onto nodes.web_url at save time, so either source should be
// sufficient — but the cloudflared quick-tunnel URL rotates each run and the
// tunnels insert can race/lose a row, leaving nodes.web_url as the fresher
// source of truth. Returning the node's web_url instead of an error keeps the
// admin panel's tunnel link live even when the tunnels table is empty.
func LoadCurrentTunnel() (string, error) {
	client := GetDBClient()
	if client == nil {
		return "", nil
	}

	tunnel, err := client.GetActiveTunnel(DBInstanceID())
	if err == nil && tunnel != nil && tunnel.URL != "" {
		return tunnel.URL, nil
	}

	// Fall back to this node's web_url, which SaveTunnelToDB keeps in sync.
	if id := NodeID(); id != "" {
		if node, nerr := client.GetNode(id); nerr == nil && node != nil && node.WebURL != "" {
			return node.WebURL, nil
		}
	}

	return "", err
}

// ─── Preview Links ────────────────────────────────────────────────────────────

// SavePreviewLinks saves preview image URLs to Supabase.
// The mirrors parameters are optional maps of host -> URL for redundancy.
func SavePreviewLinks(filename, thumbnailURL, spriteURL, previewURL string, thumbnailMirrors, spriteMirrors, previewMirrors map[string]string) error {
	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	img := &database.PreviewImage{
		Filename:         filename,
		ThumbnailURL:     thumbnailURL,
		SpriteURL:        spriteURL,
		PreviewURL:       previewURL,
		ThumbnailMirrors: thumbnailMirrors,
		SpriteMirrors:    spriteMirrors,
		PreviewMirrors:   previewMirrors,
		UploadedAt:       time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		InstanceID:       DBInstanceID(),
	}

	// Best-effort FK: the recordings row for this filename is normally written
	// by SaveRecordingBasics before thumbnails upload, so attach recording_id
	// now rather than leaving the row orphaned.  If the lookup fails (row not
	// created yet, DB hiccup) the save still succeeds — stageSaveMetadata's
	// save_recording_with_links upsert backfills recording_id via ON CONFLICT.
	if recID, ridErr := GetRecordingID(filename); ridErr == nil {
		img.RecordingID = recID
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

// VerifyPreviewImage checks whether the preview_images row for the given
// filename exists and carries a non-empty thumbnail URL.  The pipeline
// cleanup calls this in addition to VerifyRecordingThumbnails so that
// local files are never deleted when one table is missing thumbnails.
func VerifyPreviewImage(filename string) (hasThumb bool) {
	client := GetDBClient()
	if client == nil {
		return true
	}
	img, err := client.GetPreviewImage(filename)
	if err != nil || img == nil {
		return false
	}
	return img.ThumbnailURL != ""
}

// LoadRecordingThumbnails returns the thumbnail, sprite, and preview URLs from
// the recordings row for a filename (empty strings when missing/not found).
func LoadRecordingThumbnails(filename string) (thumbURL, spriteURL, previewURL string) {
	client := GetDBClient()
	if client == nil {
		return "", "", ""
	}
	rec, err := client.GetRecording(filename)
	if err != nil || rec == nil {
		return "", "", ""
	}
	return rec.ThumbnailURL, rec.SpriteURL, rec.PreviewURL
}

// UpdateRecordingThumbnails patches the thumbnail_url, sprite_url and preview_url on an
// existing recording row identified by filename. Only non-empty values are written;
// empty fields are left untouched so a caller that only has a thumbnail never
// clobbers an existing sprite/preview that may carry richer mirrors.
func UpdateRecordingThumbnails(filename, thumbnailURL, spriteURL, previewURL string) error {
	if thumbnailURL == "" && spriteURL == "" && previewURL == "" {
		return nil
	}
	fields := map[string]string{}
	if thumbnailURL != "" {
		fields["thumbnail_url"] = thumbnailURL
	}
	if spriteURL != "" {
		fields["sprite_url"] = spriteURL
	}
	if previewURL != "" {
		fields["preview_url"] = previewURL
	}
	body, err := json.Marshal(fields)
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

// Throttle + re-entrancy guard for SyncRecordingsThumbnails so the periodic
// ticker (and startup sweep) never trigger overlapping syncs or hammer Supabase.
var (
	recThumbSyncMu   sync.Mutex
	recThumbSyncing  bool
	recThumbSyncLast time.Time
)

// recThumbSyncCooldown is the minimum interval between automatic backfill
// sweeps. Each sweep PATCHes only recordings whose thumbnail is still missing
// but has a preview_images row, so it is light; the cooldown just prevents the
// ticker from re-scanning on adjacent ticks.
const recThumbSyncCooldown = 10 * time.Minute

// recThumbUnfixableBackoff is how long a node skips a recording confirmed to
// have no fixable preview (no preview_images row, or one with an empty
// thumbnail). It is bounded (not permanent) so a thumbnail generated later by
// ScanThumbnails still gets picked up on a later sweep, but it stops every node
// from re-examining the same permanently-unfixable rows every 30 minutes.
const recThumbUnfixableBackoff = 24 * time.Hour

// recThumbUnfixable tracks recently-confirmed-unfixable filenames so the sweep
// skips them without re-querying their previews. Per-node in-memory state;
// clearing happens lazily as entries age past the backoff.
var (
	recThumbUnfixableMu sync.Mutex
	recThumbUnfixable   = map[string]time.Time{}
)

// markRecThumbUnfixable records that filename currently has no thumbnail to
// backfill, so subsequent sweeps skip it until the backoff elapses.
func markRecThumbUnfixable(filename string) {
	recThumbUnfixableMu.Lock()
	recThumbUnfixable[filename] = time.Now()
	recThumbUnfixableMu.Unlock()
}

// recThumbUnfixableSkips returns the set of filenames still inside the backoff
// window, pruning expired entries in the process.
func recThumbUnfixableSkips() map[string]bool {
	recThumbUnfixableMu.Lock()
	defer recThumbUnfixableMu.Unlock()
	now := time.Now()
	skips := make(map[string]bool, len(recThumbUnfixable))
	for fn, ts := range recThumbUnfixable {
		if now.Sub(ts) < recThumbUnfixableBackoff {
			skips[fn] = true
		} else {
			delete(recThumbUnfixable, fn)
		}
	}
	return skips
}

// lookupPreviewLinks returns the preview_images-derived asset URLs for a
// recording filename. Merged recordings store preview_images under the
// *original* filename (the source HLS/stream recording), so a row recorded as
// "<file>.merged.mp4" won't match directly — fall back to the pre-merge name so
// the thumbnail still gets backfilled. Returns ok=false when no row matches.
func lookupPreviewLinks(previews map[string][3]string, filename string) ([3]string, bool) {
	if links, ok := previews[filename]; ok {
		return links, true
	}
	if strings.HasSuffix(filename, ".merged.mp4") {
		if original := strings.TrimSuffix(filename, ".merged.mp4"); original != "" {
			if links, ok := previews[original]; ok {
				return links, true
			}
		}
	}
	return [3]string{}, false
}

// mergeThumbAssets keeps the recording's existing sprite/preview URLs when they
// are already populated, so a backfill of a missing thumbnail never clobbers a
// richer (already-mirrored) sprite/preview with a stale preview_images value.
func mergeThumbAssets(existingSprite, newSprite, existingPreview, newPreview string) (sprite, preview string) {
	if existingSprite != "" {
		sprite = existingSprite
	} else {
		sprite = newSprite
	}
	if existingPreview != "" {
		preview = existingPreview
	} else {
		preview = newPreview
	}
	return sprite, preview
}

// SyncRecordingsThumbnails is the automatic backfill: for every recording whose
// recordings.thumbnail_url is empty but whose preview_images row has a thumbnail
// (which the earlier generation/pipeline wrote), copy that thumbnail (plus
// sprite/preview) onto the recordings row.
//
// This is the fleet-wide fix for thumbnails that land in preview_images but
// never reach recordings.thumbnail_url — the column the video card reads. The
// DVR's own thumbnail writer uses the anon key whose RLS silently rejects
// recordings writes, so a periodic sweep with the service-role key is required
// to keep the UI thumbnails populated.
func SyncRecordingsThumbnails() {
	recThumbSyncMu.Lock()
	if recThumbSyncing {
		recThumbSyncMu.Unlock()
		return
	}
	if time.Since(recThumbSyncLast) < recThumbSyncCooldown {
		recThumbSyncMu.Unlock()
		return
	}
	recThumbSyncing = true
	recThumbSyncMu.Unlock()
	defer func() {
		recThumbSyncMu.Lock()
		recThumbSyncing = false
		recThumbSyncLast = time.Now()
		recThumbSyncMu.Unlock()
	}()

	client := GetDBClient()
	if client == nil {
		return
	}
	previews := LoadAllPreviewLinks()
	if len(previews) == 0 {
		return
	}
	recordings, err := client.GetRecordingsMissingThumbnails()
	if err != nil {
		log.Printf("[thumb-sync] could not load recordings: %v", err)
		return
	}

	// Small delay so a mass backfill never trips image-host/Supabase rate
	// limits (mirrors the pacing ScanThumbnails uses).
	const pacing = 150 * time.Millisecond

	fixed := 0
	skips := recThumbUnfixableSkips()
	for i := range recordings {
		rec := &recordings[i]
		if rec.ThumbnailURL != "" {
			continue
		}
		if skips[rec.Filename] {
			continue
		}
		links, ok := lookupPreviewLinks(previews, rec.Filename)
		if !ok {
			// No preview_images row exists for this file, so it cannot be
			// backfilled on this sweep. Remember it so we don't re-examine it
			// on adjacent sweeps; give ScanThumbnails a window to generate one.
			markRecThumbUnfixable(rec.Filename)
			continue
		}
		thumb, sprite, preview := links[0], links[1], links[2]
		if thumb == "" {
			// Preview row exists but carries no thumbnail yet — not fixable now.
			markRecThumbUnfixable(rec.Filename)
			continue
		}
		// Merge conservatively: the thumbnail is what's missing, so always write
		// it; but never clobber an existing (possibly richer) sprite/preview that
		// already survived on the recording row with a stale preview_images value.
		sprite, preview = mergeThumbAssets(rec.SpriteURL, sprite, rec.PreviewURL, preview)
		if err := UpdateRecordingThumbnails(rec.Filename, thumb, sprite, preview); err != nil {
			log.Printf("[thumb-sync] failed to sync %s: %v", rec.Filename, err)
			continue
		}
		fixed++
		time.Sleep(pacing)
	}

	// Second sweep: recordings that already have a thumbnail but lack sprite
	// and/or preview.  These are invisible to the first sweep above (its query
	// requires a null thumbnail) even though preview_images commonly holds the
	// missing sprite/preview — the three assets are generated independently and
	// a sprite/preview failure (timeout on a long video, image-host outage) does
	// not block the thumbnail.  Copying from preview_images is a pure DB->DB
	// merge (no regeneration, no image-host upload), so it cannot burn host
	// quota the way regeneration would.
	fixedSP := 0
	spRecs, err := client.GetRecordingsMissingSpriteOrPreview()
	if err != nil {
		log.Printf("[thumb-sync] could not load sprite/preview-missing recordings: %v", err)
	} else {
		for i := range spRecs {
			rec := &spRecs[i]
			if rec.ThumbnailURL == "" {
				continue
			}
			if skips[rec.Filename] {
				continue
			}
			links, ok := lookupPreviewLinks(previews, rec.Filename)
			if !ok {
				// No preview_images row exists for this file.  The recording
				// already has its thumbnail, so nothing about it will be
				// regenerated (ScanThumbnails only acts when the THUMBNAIL is
				// missing) and there is nothing to merge.  Mark it unfixable so
				// nodes don't re-query it on every sweep.
				markRecThumbUnfixable(rec.Filename)
				continue
			}
			sprite, preview := links[1], links[2]
			// Merge conservatively: only fill what the recording row is missing;
			// never clobber an existing (possibly richer) sprite/preview with a
			// stale preview_images value.
			sprite, preview = mergeThumbAssets(rec.SpriteURL, sprite, rec.PreviewURL, preview)
			if sprite == rec.SpriteURL && preview == rec.PreviewURL {
				// Nothing to fill — the recording already has everything the
				// preview_images row offers for these fields.
				continue
			}
			if err := UpdateRecordingThumbnails(rec.Filename, "", sprite, preview); err != nil {
				log.Printf("[thumb-sync] failed to sync sprite/preview for %s: %v", rec.Filename, err)
				continue
			}
			fixedSP++
			time.Sleep(pacing)
		}
	}
	if fixed > 0 || fixedSP > 0 {
		log.Printf("[thumb-sync] backfilled thumbnail_url onto %d recording(s) and sprite/preview onto %d", fixed, fixedSP)
	}

	// ── Reverse sweep: recordings → preview_images ────────────────────────
	// When the legacy fallback path is used (atomic RPC unavailable), the
	// recording gets its thumbnail but preview_images is never written.
	// This sweep copies thumbnails from recordings INTO preview_images so
	// the two-table invariant stays intact and ScanThumbnails never has to
	// regenerate an asset that already exists in the DB.
	fixedPreview := 0
	recsWithThumbs, err := client.GetRecordingsWithThumbnails()
	if err != nil {
		log.Printf("[thumb-sync] could not load recordings with thumbnails: %v", err)
	} else {
		for i := range recsWithThumbs {
			rec := &recsWithThumbs[i]
			if skips[rec.Filename] {
				continue
			}
			if links, ok := previews[rec.Filename]; ok && links[0] != "" {
				// preview_images already has a thumbnail — nothing to backfill.
				continue
			}
			// preview_images is missing (or has empty thumbnail) — create it
			// from the recording row.
			err := client.SavePreviewImage(&database.PreviewImage{
				Filename:     rec.Filename,
				ThumbnailURL: rec.ThumbnailURL,
				SpriteURL:    rec.SpriteURL,
				PreviewURL:   rec.PreviewURL,
				UploadedAt:   time.Now().UTC().Format("2006-01-02T15:04:05Z"),
				InstanceID:   DBInstanceID(),
			})
			if err != nil {
				log.Printf("[thumb-sync] failed to backfill preview_images for %s: %v", rec.Filename, err)
				continue
			}
			fixedPreview++
			time.Sleep(pacing)
		}
	}
	if fixedPreview > 0 {
		log.Printf("[thumb-sync] backfilled preview_images from recordings for %d file(s)", fixedPreview)
	}
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
//
// Local-first: the entry is ALWAYS written to the local on-disk journal before
// any Supabase attempt.  A Supabase outage therefore cannot destroy the dedup
// record — the root cause of duplicate re-uploads.  The link is persisted
// locally too so recording metadata can be rebuilt without re-uploading.
// The Supabase write remains best-effort (it is the cross-node durable copy).
func SaveJournalEntry(fileHash, filename, host, status, link string, fileSize int64, errMsg string) error {
	// Local-first: always try to persist the dedup record locally. But a local
	// write failure (full/readonly runner disk) must NOT block the Supabase
	// journal — that's our cross-node source of truth for visibility and dedup.
	// Previously a local failure returned early and skipped the Supabase write
	// entirely, going silent exactly when we most need the log.
	if lErr := saveLocalJournalEntry(fileHash, host, status, link, fileSize, errMsg); lErr != nil {
		fmt.Printf("warn: saveLocalJournalEntry failed for %s/%s: %v\n", host, filename, lErr)
	}

	client := GetDBClient()
	if client == nil {
		return fmt.Errorf("Supabase not configured")
	}

	entry := &database.UploadJournal{
		FileHash:   fileHash,
		Filename:   filename,
		Host:       host,
		Status:     status,
		Link:       link,
		ErrorMsg:   errMsg,
		FileSize:   fileSize,
		InstanceID: DBInstanceID(),
	}

	return client.SaveJournalEntry(entry)
}

// LoadJournalByHash returns all journal entries for a given file hash,
// merging the local on-disk journal with Supabase (union of both).
func LoadJournalByHash(fileHash string) ([]database.UploadJournal, error) {
	client := GetDBClient()
	if client == nil {
		// Supabase unavailable — fall back to the local journal alone so dedup
		// and recovery keep working during an outage.
		var out []database.UploadJournal
		for _, e := range loadLocalJournal(fileHash) {
			out = append(out, database.UploadJournal{
				FileHash:  fileHash,
				Host:      e.Host,
				Status:    e.Status,
				Link:      e.Link,
				ErrorMsg:  e.ErrMsg,
				FileSize:  e.FileSize,
				UpdatedAt: e.UpdatedAt,
			})
		}
		if len(out) == 0 {
			return nil, fmt.Errorf("Supabase not configured")
		}
		return out, nil
	}
	return client.GetJournalByHash(fileHash)
}

// LoadCompletedHosts returns the list of hosts that have successfully received
// the file identified by fileHash.  The local journal is the source of truth
// during a Supabase outage; remote entries are merged in when reachable.
func LoadCompletedHosts(fileHash string) ([]string, error) {
	hosts := make(map[string]bool)
	mergeLocalJournalSuccessHosts(fileHash, hosts)

	client := GetDBClient()
	if client != nil {
		if entries, err := client.GetJournalByHash(fileHash); err == nil {
			for _, e := range entries {
				if e.Status == "success" {
					hosts[e.Host] = true
				}
			}
		}
	}

	if len(hosts) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(hosts))
	for h := range hosts {
		out = append(out, h)
	}
	return out, nil
}

// DeleteJournalByHash removes all journal entries for a file hash from BOTH
// the local on-disk journal and Supabase.
func DeleteJournalByHash(fileHash string) error {
	if err := deleteLocalJournal(fileHash); err != nil {
		return err
	}
	client := GetDBClient()
	if client == nil {
		return nil
	}
	return client.DeleteJournalByHash(fileHash)
}

// ReconcileMissingUploadLinks restores upload_links rows for recordings whose
// metadata was saved but whose per-host link save never landed, rebuilding
// them from upload-journal successes that carry a persisted download link.
//
// Background: when a host upload succeeds the pipeline (a) writes a journal
// entry and (b) immediately saves the link to the recording row.  If the
// runner dies between (a) and (b) — or Supabase is down longer than the
// in-flight save retries — the recording stays at zero upload_links forever.
// On restart LoadCompletedHosts reads the journal's successes and considers
// those hosts done, so the missing links are never re-uploaded AND never
// re-persisted.  This sweep closes that gap: every journal success that
// carries a link re-creates the corresponding upload_links row (idempotent
// upsert by recording_id, host).  It is safe to run alongside active
// pipelines — upsert semantics mean a concurrent link save ends the same way.
//
// The sweep is driven by the recordings that currently have ZERO upload links
// (~160 fleet-wide), not by a recent journal window: a gap left by a crash weeks
// ago is repaired by the next sweep, and the cost scales with the broken set
// instead of with the journal's whole history.  Only those filenames are asked
// about, in releaseBatchSize-sized batches.
//
// Returns the number of links restored.
func ReconcileMissingUploadLinks() int {
	client := GetDBClient()
	if client == nil {
		return 0
	}

	// Preflight: while the database is rejecting upload_links writes there is
	// nothing to restore, and marching through hundreds of doomed inserts would
	// bury the one line that matters (see server/write_path_check.go).
	if !uploadLinkWritePathReady() {
		return 0
	}

	missing, err := client.RecordingsWithoutUploadLinks()
	if err != nil {
		if database.NoHostsFuncMissing(err) {
			fmt.Printf("[WARN] reconcile: recordings_without_upload_links() is not deployed (apply migration 20260923000000) — sweep skipped\n")
		} else {
			fmt.Printf("[WARN] reconcile: could not list recordings with no host: %v\n", err)
		}
		return 0
	}
	sort.Strings(missing) // deterministic batching keeps the request count stable
	if len(missing) == 0 {
		return 0
	}

	entries, err := client.GetJournalSuccessWithLinksFor(missing)
	if err != nil {
		fmt.Printf("[WARN] reconcile: could not load journal successes: %v\n", err)
		return 0
	}
	if len(entries) == 0 {
		return 0
	}

	// Group journal successes by filename, keeping the most recent link per host.
	byFilename := make(map[string]map[string]string) // filename -> host -> link
	order := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.Filename == "" || e.Host == "" || e.Link == "" {
			continue
		}
		if _, ok := byFilename[e.Filename]; !ok {
			byFilename[e.Filename] = make(map[string]string)
			order = append(order, e.Filename)
		}
		byFilename[e.Filename][e.Host] = e.Link
	}
	if len(order) == 0 {
		return 0
	}

	// Every filename here came from the zero-link set, so there is no per-host
	// "already linked" check to make: SaveUploadLink upserts on
	// (recording_id, host), which also covers a link a live pipeline wrote
	// between the two reads.
	ids, _, err := client.LookupRecordingLinkState(order)
	if err != nil {
		fmt.Printf("[WARN] reconcile: could not resolve recording ids: %v\n", err)
		return 0
	}

	restored, attempted, failed := 0, 0, 0
	var lastErr error
	for _, filename := range order {
		recID := ids[filename]
		if recID == "" {
			// No recording row (e.g. the temp-file upload anomaly or the row was
			// never created) — nothing to attach links to.
			continue
		}
		for host, link := range byFilename[filename] {
			attempted++
			if err := client.SaveUploadLink(&database.UploadLink{
				RecordingID: recID,
				Host:        host,
				URL:         link,
				InstanceID:  DBInstanceID(),
			}); err != nil {
				failed++
				lastErr = err
				continue
			}
			restored++
		}
	}

	if failed > 0 {
		fmt.Printf("[WARN] reconcile: %d of %d link restore(s) failed\n", failed, attempted)
	}
	// The canary insert succeeded, but not one real restore did: report it as the
	// same broken write path rather than letting the sweep look like "nothing to
	// repair".
	if attempted > 0 && restored == 0 {
		alertUploadLinkWriteFailure("Upload-link restore failed for every recording that needs one", lastErr)
	}

	if restored > 0 {
		fmt.Printf("[startup] reconcile: restored %d upload link(s) from journal\n", restored)
		cacheClear()
	}
	return restored
}

// recordingUploadLinkIndexTTL is how long the fleet-wide link index below is
// reused.  The admin panel polls /api/orphans (which needs it) every few seconds
// while the panel is open, and the index is a whole-table read.
const recordingUploadLinkIndexTTL = 60 * time.Second

var (
	recordingUploadLinkIndexMu   sync.Mutex
	recordingUploadLinkIndexAt   time.Time
	recordingUploadLinkIndexAll  map[string]bool
	recordingUploadLinkIndexSafe map[string]bool
)

// RecordingUploadLinkIndex returns, for the whole fleet:
//
//   - all: every filename that has a recordings row;
//   - safeInCloud: the subset whose recording has at least one upload link, i.e.
//     whose content is actually safe on a host.
//
// The two differ for a recording whose upload leg died before its first link was
// persisted.  A recordings row is written at ENQUEUE time, so row existence alone
// cannot answer "is this file in the cloud?" — and the difference is exactly the
// set /api/orphans must report and the admin panel counts as "no host at all".
//
// Cost is one paged read of recordings.filename plus one call to the
// recordings_without_upload_links() function, which returns only the broken set
// (~160 names).  Diffing client-side is not an option: upload_links holds ~125k
// rows, and an embedded upload_links(host) select fails outright because the
// deployed schema has no foreign key from upload_links to recordings (PGRST200).
//
// The result is cached for recordingUploadLinkIndexTTL and the maps are shared:
// callers must not modify them.  A caller that needs to add names (the preview
// prune adds local files to its keep set) must copy first.
func RecordingUploadLinkIndex() (map[string]bool, map[string]bool, error) {
	client := GetDBClient()
	if client == nil {
		return nil, nil, fmt.Errorf("Supabase not configured")
	}

	recordingUploadLinkIndexMu.Lock()
	defer recordingUploadLinkIndexMu.Unlock()

	if recordingUploadLinkIndexAll != nil && time.Since(recordingUploadLinkIndexAt) < recordingUploadLinkIndexTTL {
		return recordingUploadLinkIndexAll, recordingUploadLinkIndexSafe, nil
	}

	all, err := client.GetRecordingFilenames()
	if err != nil {
		return nil, nil, fmt.Errorf("load recordings index: %w", err)
	}
	noHosts, err := client.RecordingsWithoutUploadLinks()
	if err != nil {
		if database.NoHostsFuncMissing(err) {
			return nil, nil, fmt.Errorf("recordings_without_upload_links() is not deployed (apply migration 20260923000000): %w", err)
		}
		return nil, nil, fmt.Errorf("load recordings with no host: %w", err)
	}

	safe := make(map[string]bool, len(all))
	for name := range all {
		safe[name] = true
	}
	for _, name := range noHosts {
		delete(safe, name)
	}

	recordingUploadLinkIndexAll, recordingUploadLinkIndexSafe = all, safe
	recordingUploadLinkIndexAt = time.Now()
	return all, safe, nil
}
