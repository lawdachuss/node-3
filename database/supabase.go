package database

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Client represents a Supabase database client
type Client struct {
	URL    string
	APIKey string
	client *http.Client
}

// NewClient creates a new Supabase client
func NewClient(url, apiKey string) *Client {
	return &Client{
		URL:    url,
		APIKey: apiKey,
		client: &http.Client{Timeout: 60 * time.Second},
	}
}

// releaseBatchSize caps how many usernames go into one PostgREST PATCH filter
// URL. Supabase sits behind a proxy with an ~8KB URL limit (HTTP 414 "URI too
// long"); a single PATCH carrying the whole excess list (~850 usernames in the
// wild) was rejected every claim cycle, so an overloaded node could never shed
// channels and the fair-share rebalancer deadlocked the pool (one node hoarding
// ~900 channels while the rest of the fleet sat idle). 40 usernames is well
// under 1KB even with long names.
const releaseBatchSize = 40

// livenessBatchSize caps how many (username, site) pairs go into one
// SetChannelsLive/SetChannelsNotLive or= filter. Each escaped pair is ~45-55
// chars, so 30 pairs stay far below the ~8KB URL limit.
const livenessBatchSize = 30

// ============================================================================
// HTTP HELPERS
// ============================================================================

func (c *Client) request(method, path string, body interface{}) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest(method, c.URL+"/rest/v1"+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("apikey", c.APIKey)
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", "resolution=merge-duplicates,return=representation")
	}

	return c.client.Do(req)
}

// requestPrefer is request() with an explicit PostgREST Prefer header.  Needed
// for `resolution=ignore-duplicates`, where request()'s merge-duplicates default
// would overwrite existing columns with whatever the caller happened to send.
func (c *Client) requestPrefer(method, path string, body interface{}, prefer string) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal body: %w", err)
		}
		bodyReader = bytes.NewReader(jsonData)
	}

	req, err := http.NewRequest(method, c.URL+"/rest/v1"+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("apikey", c.APIKey)
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Prefer", prefer)
	}

	return c.client.Do(req)
}

// defaultMaxRetries is used for all Supabase calls unless a call opts into
// more (metadata saves use metadataSaveMaxRetries so a transient outage at
// finalize time cannot strand a recording without its DB row).
const defaultMaxRetries = 3

// metadataSaveMaxRetries applies to the SaveRecordingWithLinks path
// (SaveRecording, GetRecording, SaveUploadLinks): a recording's metadata is
// the last write before the local copy is deleted, so it gets extra attempts.
const metadataSaveMaxRetries = 10

// IsSchemaCacheStale classifies a PostgREST error body as a transient
// schema-cache condition and returns the offending code, or "" when the
// body carries none of them. Known codes: PGRST002 (503, cache rebuilding),
// PGRST204 (400, column not in cache yet), 42P10 (400, ON CONFLICT
// constraint not in cache yet). Single owner for both the database and
// server retry loops (plan D3); retry escalation is unchanged — the codes
// only decide classification, the callers keep their own backoff.
func IsSchemaCacheStale(body string) string {
	switch {
	case strings.Contains(body, "PGRST002"):
		return "PGRST002"
	case strings.Contains(body, "PGRST204"):
		return "PGRST204"
	case strings.Contains(body, "42P10"):
		return "42P10"
	}
	return ""
}

// RestTable extracts the table (or rpc procedure) name from a PostgREST
// request path so schema-cache failures can name the write that failed:
// "/channel_logs?select=id" → "channel_logs", "/rpc/foo" → "rpc/foo".
func RestTable(path string) string {
	p := path
	if i := strings.IndexAny(p, "?"); i >= 0 {
		p = p[:i]
	}
	return strings.Trim(p, "/")
}

// requestWithRetry executes the request and retries on transient errors:
// - 503 PGRST002 — schema cache rebuilding after migration
// - 400 PGRST204 — column not in schema cache yet (PostgREST needs to refresh)
func (c *Client) requestWithRetry(method, path string, body interface{}) (*http.Response, error) {
	return c.requestWithRetryN(method, path, body, defaultMaxRetries)
}

// requestWithRetryN is requestWithRetry with an explicit retry budget.
func (c *Client) requestWithRetryN(method, path string, body interface{}, maxRetries int) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := c.request(method, path, body)
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				backoff := retryBackoff(attempt)
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

			// Schema-cache transients (IsSchemaCacheStale: PGRST002 503
			// rebuilding, PGRST204 400 column missing, 42P10 400 ON CONFLICT
			// constraint missing). The error names the failing method+table
			// so a real schema gap stays visible after retries exhaust
			// (plan D3).
			if code := IsSchemaCacheStale(bodyStr); code != "" {
				lastErr = fmt.Errorf("HTTP %d [%s] %s %s: %s", resp.StatusCode, code, method, RestTable(path), bodyStr)
				backoff := retryBackoff(attempt)
				fmt.Printf("[WARN] Supabase schema cache stale [%s] %s %s (attempt %d/%d), retrying in %v\n", code, method, RestTable(path), attempt+1, maxRetries, backoff)
				resp.Body.Close()
				time.Sleep(backoff)
				continue
			}

			// Non-retryable error — return as-is
			if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
				lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, bodyStr)
				resp.Body.Close()
				if attempt < maxRetries-1 {
					backoff := retryBackoff(attempt)
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

func retryBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 5 {
		attempt = 5
	}
	return time.Duration(1<<attempt) * 2 * time.Second
}

func (c *Client) get(path string, result interface{}) error {
	return c.getN(path, result, defaultMaxRetries)
}

func (c *Client) getN(path string, result interface{}, maxRetries int) error {
	resp, err := c.requestWithRetryN("GET", path, nil, maxRetries)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return json.NewDecoder(resp.Body).Decode(result)
}

// getAllPaginated fetches every row from a PostgREST collection by paging in
// batches of 1000.  This is independent of the server's max_rows setting
// (Supabase defaults to 1000), so a single limit=N request that silently
// truncates at 1000 is replaced by a correct full fetch.  dst must be a pointer
// to a slice (*[]T); batches are decoded into a fresh slice of the same element
// type and appended, so it works for any T.
func (c *Client) getAllPaginated(path string, dst interface{}) error {
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr || rv.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("getAllPaginated: dst must be *[]T, got %T", dst)
	}
	sliceType := rv.Elem().Type()
	const page = 1000
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	for offset := 0; ; offset += page {
		batchPtr := reflect.New(sliceType)
		sub := fmt.Sprintf("%s%slimit=%d&offset=%d", path, sep, page, offset)
		if err := c.get(sub, batchPtr.Interface()); err != nil {
			return err
		}
		batch := batchPtr.Elem()
		for i := 0; i < batch.Len(); i++ {
			rv.Elem().Set(reflect.Append(rv.Elem(), batch.Index(i)))
		}
		if batch.Len() < page {
			break
		}
	}
	return nil
}

func (c *Client) post(path string, body interface{}, result interface{}) error {
	resp, err := c.requestWithRetry("POST", path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	if result != nil && resp.StatusCode != http.StatusNoContent {
		return json.NewDecoder(resp.Body).Decode(result)
	}
	return nil
}

func (c *Client) patch(path string, body interface{}) error {
	resp, err := c.requestWithRetry("PATCH", path, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

func (c *Client) delete(path string) error {
	resp, err := c.requestWithRetry("DELETE", path, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// ============================================================================
// CHANNELS
// ============================================================================

type Channel struct {
	ID          string `json:"id,omitempty"`
	Username    string `json:"username"`
	IsPaused    bool   `json:"is_paused"`
	Framerate   int    `json:"framerate"`
	Resolution  int    `json:"resolution"`
	Pattern     string `json:"pattern"`
	MaxDuration int    `json:"max_duration"`
	MaxFilesize int    `json:"max_filesize"`
	Compress    bool   `json:"compress"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

// SaveChannel creates or updates a channel using Supabase's upsert functionality.
// Uses on_conflict to atomically upsert by username, avoiding TOCTOU race conditions.
func (c *Client) SaveChannel(ch *Channel) error {
	resp, err := c.requestWithRetry("POST", "/channels?on_conflict=username", ch)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// EnsureChannelExists registers username in the channels table if it is not
// there yet, leaving an existing row (and its scraped profile) untouched.
//
// In pooled mode channel_assignments is what the fleet records from, and the
// channels table is the shared catalogue (external tools read it, and the
// isolated-mode "already exists" check consults it).  A pooled channel used to
// get its catalogue row only as a side effect of the on-demand profile scrape —
// i.e. AFTER a node had already claimed and started monitoring it.  That gap
// was what made the controller's old orphan prune delete freshly added
// channels; the prune is gone now, and this is a best-effort consistency write
// so the catalogue lists every pooled channel too.
//
// created_at is the one NOT NULL channel column with no database default, so it
// is supplied explicitly.  resolution=ignore-duplicates makes concurrent adds
// (and re-adds of an already-recorded channel) no-ops instead of clobbering
// stored config/profile columns.
func (c *Client) EnsureChannelExists(username string) error {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil
	}

	body := map[string]interface{}{
		"username":   username,
		"created_at": time.Now().Unix(),
	}

	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		resp, err := c.requestPrefer("POST", "/channels?on_conflict=username", body, "resolution=ignore-duplicates")
		if err != nil {
			lastErr = err
		} else {
			if resp.StatusCode < 400 {
				resp.Body.Close()
				return nil
			}
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
			// Only 408/429/5xx are worth another try.
			if resp.StatusCode != 408 && resp.StatusCode != 429 && resp.StatusCode < 500 {
				return lastErr
			}
		}
		if attempt < maxAttempts-1 {
			time.Sleep(retryBackoff(attempt))
		}
	}
	return fmt.Errorf("ensure channel %q: %w", username, lastErr)
}

// ChannelProfile holds the full-profile fields scraped from the site's
// biocontext API and stored in the existing channels table (see
// database/migrate-combined.sql). Only the profile columns are
// touched — config columns (is_paused, framerate, pattern, …) stay intact.
//
// Every field except Username is omitempty so a partial API response never
// clobbers previously-stored values with zeros/empties: missing fields are
// simply left out of the PATCH body and the DB keeps its prior value.
// PhotoSets/SocialMedias are json.RawMessage because the site returns arrays
// of objects (not strings) that we pass through verbatim into the JSONB
// columns.
type ChannelProfile struct {
	Username         string          `json:"username"`
	FollowerCount    int             `json:"follower_count,omitempty"`
	Location         string          `json:"location,omitempty"`
	RealName         string          `json:"real_name,omitempty"`
	BodyDecorations  string          `json:"body_decorations,omitempty"`
	SmokeDrink       string          `json:"smoke_drink,omitempty"`
	BodyType         string          `json:"body_type,omitempty"`
	DisplayBirthday  string          `json:"display_birthday,omitempty"`
	DisplayAge       int             `json:"display_age,omitempty"`
	AboutMe          string          `json:"about_me,omitempty"`
	WishList         string          `json:"wish_list,omitempty"`
	FanClubCost      int             `json:"fan_club_cost,omitempty"`
	Sex              string          `json:"sex,omitempty"`
	Subgender        string          `json:"subgender,omitempty"`
	InterestedIn     []string        `json:"interested_in,omitempty"`
	PhotoSets        json.RawMessage `json:"photo_sets,omitempty"`
	SocialMedias     json.RawMessage `json:"social_medias,omitempty"`
	LastBroadcast    string          `json:"last_broadcast,omitempty"`
	RoomStatus       string          `json:"room_status,omitempty"`
	AvatarURL        string          `json:"avatar_url,omitempty"`
	ProfileScrapedAt string          `json:"profile_scraped_at,omitempty"`
}

// SaveChannelProfile PATCHes the profile columns of the existing channels row
// for the given username, and INSERTs a minimal row when none exists yet.
//
// Rows normally exist for every configured channel (created by
// SaveChannelsToDB), so the PATCH only ever updates profile fields and never
// clobbers recorder config columns. In distributed pool mode, however,
// LoadPooledConfig never syncs the channels table — so a pooled channel has
// no row and the old code dropped every scraped profile with a per-channel
// warning. The fallback INSERT (upsert by username, with created_at — the one
// NOT NULL column without a database default) keeps the profile data instead.
func (c *Client) SaveChannelProfile(p *ChannelProfile) error {
	p.ProfileScrapedAt = time.Now().UTC().Format(time.RFC3339)
	path := "/channels?username=eq." + url.QueryEscape(p.Username)

	// requestWithRetry sets Prefer: return=representation, so the response
	// body lists the rows that were patched — an empty list means no row
	// matched yet.
	resp, err := c.requestWithRetry("PATCH", path, p)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var patched []map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&patched)
	if len(patched) > 0 {
		return nil // row existed, profile columns updated in place
	}

	// No channels row yet (pooled mode) — create one. Every profile column is
	// omitempty and every config column has a database default, so the only
	// schema-required extra field is created_at (BIGINT NOT NULL, no default).
	bodyBytes, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal insert body: %w", err)
	}
	var insert map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &insert); err != nil {
		return fmt.Errorf("unmarshal insert body: %w", err)
	}
	insert["created_at"] = time.Now().Unix()

	// on_conflict=username makes a concurrent SaveChannelsToDB / another
	// scrape safe: if the row appeared between our PATCH and this POST, the
	// upsert just updates the profile columns again.
	insertResp, err := c.requestWithRetry("POST", "/channels?on_conflict=username", insert)
	if err != nil {
		return err
	}
	defer insertResp.Body.Close()
	if insertResp.StatusCode >= 400 {
		b, _ := io.ReadAll(insertResp.Body)
		return fmt.Errorf("insert returned %d: %s", insertResp.StatusCode, string(b))
	}
	fmt.Printf("[DEBUG] SaveChannelProfile(%q): inserted new channels row\n", p.Username)
	return nil
}

// GetChannel retrieves a channel by username
func (c *Client) GetChannel(username string) (*Channel, error) {
	var channels []Channel
	err := c.get(fmt.Sprintf("/channels?username=eq.%s&limit=1", url.QueryEscape(username)), &channels)
	if err != nil {
		return nil, err
	}
	if len(channels) == 0 {
		return nil, fmt.Errorf("channel not found")
	}
	return &channels[0], nil
}

// GetAllChannels retrieves all channels
func (c *Client) GetAllChannels() ([]Channel, error) {
	var channels []Channel
	err := c.getAllPaginated("/channels?order=created_at.desc", &channels)
	return channels, err
}

// ChannelExists reports whether a channel with the given username exists in
// the channels table, matching case-insensitively (cam-site usernames are
// case-insensitive, so "Alice" and "alice" are the same channel).
func (c *Client) ChannelExists(username string) (bool, error) {
	var channels []Channel
	err := c.get(fmt.Sprintf("/channels?username=ilike.%s&limit=1", url.QueryEscape(username)), &channels)
	if err != nil {
		return false, err
	}
	return len(channels) > 0, nil
}

// DeleteChannel removes a channel
func (c *Client) DeleteChannel(username string) error {
	return c.delete(fmt.Sprintf("/channels?username=eq.%s", url.QueryEscape(username)))
}

// deleteByUsernamesChunked DELETEs rows by username in small batches so each
// filter URL stays far below the ~8KB proxy limit (HTTP 414).
func (c *Client) deleteByUsernamesChunked(table string, usernames []string) error {
	for start := 0; start < len(usernames); start += releaseBatchSize {
		end := start + releaseBatchSize
		if end > len(usernames) {
			end = len(usernames)
		}
		batch := usernames[start:end]
		if err := c.delete(fmt.Sprintf("/%s?username=in.(%s)", table, joinEscaped(batch))); err != nil {
			return fmt.Errorf("delete %s batch %d/%d: %w", table, start/releaseBatchSize+1, (len(usernames)+releaseBatchSize-1)/releaseBatchSize, err)
		}
	}
	return nil
}

// ============================================================================
// LOG/STAMP RETENTION
// ============================================================================

// DeleteChannelLogsBefore removes channel_logs rows whose created_at is older
// than cutoff.  channel_logs is a pure telemetry/log table — the 508 MB / 1.8M
// rows accumulated by SaveLog/SaveLogBestEffort.  Deleting these does NOT touch
// recordings, upload_links, preview_images, upload_journal, channels,
// channel_assignments, pipeline_states or nodes (all "important metadata").
func (c *Client) DeleteChannelLogsBefore(cutoff time.Time) (int, error) {
	return c.deleteRowsOlderThan("channel_logs", "created_at", cutoff)
}

// DeleteDiskUsageBefore removes disk_usage rows whose recorded_at is older than
// cutoff.  disk_usage is a pure telemetry table (the periodic SaveDiskUsage
// snapshots) and contains no important metadata.
func (c *Client) DeleteDiskUsageBefore(cutoff time.Time) (int, error) {
	return c.deleteRowsOlderThan("disk_usage", "recorded_at", cutoff)
}

// deleteRowsOlderThan deletes rows from a single pure-log table whose timestamp
// column is older than cutoff.  It mirrors the DeleteOrphanedRecordings shape:
// repeatedly page the OLDEST matching ids (order=asc), then delete them in
// id=in.(...) batches.  Bounded statements + tiny memory — safe for millions of
// rows and never issues a giant unpaginated DELETE in one request.
//
// MUST only ever be called on log/telemetry tables (channel_logs, disk_usage).
// Metadata tables are deliberately never reachable from here.
//
// The delete batch is capped at 100 ids (not the 500-row page): 500 UUIDs in
// an id=in.(...) filter is ~18 KB of query string, over the ~8 KB proxy URI
// limit, so every batch answered HTTP 414 and retention silently pruned zero
// rows (fleet sweep 2026-10-20; same limit documented on DeleteChannelsNotIn).
// 100 UUIDs ≈ 3.7 KB — comfortably under the limit.
func (c *Client) deleteRowsOlderThan(table, timeField string, cutoff time.Time) (int, error) {
	const pageSize = 500
	const deleteBatch = 100
	total := 0
	var pending []string
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := c.delete(fmt.Sprintf("/%s?id=in.(%s)", table, strings.Join(pending, ","))); err != nil {
			return fmt.Errorf("delete old %s batch (%d): %w", table, len(pending), err)
		}
		total += len(pending)
		pending = pending[:0]
		return nil
	}
	for {
		var ids []struct {
			ID string `json:"id"`
		}
		err := c.get(fmt.Sprintf(
			"/%s?select=id&%s=lt.%s&order=%s.asc&limit=%d",
			table, timeField, cutoff.UTC().Format(time.RFC3339), timeField, pageSize), &ids)
		if err != nil {
			return total, fmt.Errorf("page old %s ids: %w", table, err)
		}
		if len(ids) == 0 {
			return total, nil
		}
		for _, r := range ids {
			pending = append(pending, r.ID)
			if len(pending) >= deleteBatch {
				if err := flush(); err != nil {
					return total, err
				}
			}
		}
		// Drain the page remainder BEFORE re-paging: an id not yet deleted is
		// still the oldest row and would be returned (and counted) again by
		// the next `order=<timeField>.asc` page.
		if err := flush(); err != nil {
			return total, err
		}
	}
}

// DeleteChannelsNotIn removes all channel rows whose username is NOT in the
// provided list. Pass an empty slice to delete all channels.
//
// The old implementation built a single not.in.(keep...) filter listing every
// kept username — at scale that URL exceeds the ~8KB proxy limit (HTTP 414),
// and chunking an exclusion filter is unsafe (each chunk would delete the
// other chunks' keep rows). So we fetch the existing channels, compute the
// exact to-delete set, and DELETE it in small in.(...) batches instead.
func (c *Client) DeleteChannelsNotIn(usernames []string) error {
	if len(usernames) == 0 {
		return c.delete("/channels")
	}

	keep := make(map[string]bool, len(usernames))
	for _, u := range usernames {
		keep[u] = true
	}

	var existing []Channel
	if err := c.getAllPaginated("/channels?select=username", &existing); err != nil {
		return err
	}
	var toDelete []string
	for _, ch := range existing {
		if !keep[ch.Username] {
			toDelete = append(toDelete, ch.Username)
		}
	}
	if len(toDelete) == 0 {
		return nil
	}
	return c.deleteByUsernamesChunked("channels", toDelete)
}

// ============================================================================
// RECORDINGS
// ============================================================================

type Recording struct {
	ID         string   `json:"id,omitempty"`
	ChannelID  string   `json:"channel_id,omitempty"`
	Username   string   `json:"username"`
	Filename   string   `json:"filename"`
	Timestamp  string   `json:"timestamp"`
	RoomTitle  string   `json:"room_title,omitempty"`
	Tags       []string `json:"tags,omitempty"`
	Viewers    int      `json:"viewers"`
	Resolution string   `json:"resolution,omitempty"`
	Framerate  int      `json:"framerate"`
	Filesize   int64    `json:"filesize"`
	Duration   float64  `json:"duration,omitempty"`
	Gender     string   `json:"gender,omitempty"`
	// EndReason records why the recording stopped (model went offline, stream
	// session expired, max duration/filesize rotation, paused/stopped, session
	// boundary). Empty when unknown (e.g. orphan-recovery uploads).
	EndReason        string            `json:"end_reason,omitempty"`
	ThumbnailURL     string            `json:"thumbnail_url,omitempty"`
	SpriteURL        string            `json:"sprite_url,omitempty"`
	PreviewURL       string            `json:"preview_url,omitempty"`
	ThumbnailMirrors map[string]string `json:"thumbnail_mirrors,omitempty"` // host -> URL
	SpriteMirrors    map[string]string `json:"sprite_mirrors,omitempty"`    // host -> URL
	PreviewMirrors   map[string]string `json:"preview_mirrors,omitempty"`   // host -> URL
	EmbedURL         string            `json:"embed_url,omitempty"`
	InstanceID       string            `json:"instance_id,omitempty"`
	// ThumbAttemptAt is the fleet-wide lease stamp for remote-thumbnail
	// recovery: the last time (by any node) an attempt was started for this
	// recording. See ClaimRecordingThumbAttempt. Empty when the column has not
	// been deployed yet (the queries degrade to lease-less behaviour).
	ThumbAttemptAt string `json:"thumb_attempt_at,omitempty"`
	CreatedAt      string `json:"created_at,omitempty"`
	UpdatedAt      string `json:"updated_at,omitempty"`
}

// SaveRecording creates or updates a recording using Supabase's upsert functionality.
// Uses on_conflict to atomically upsert by filename, avoiding TOCTOU race conditions.
// Retries metadataSaveMaxRetries times: a recording's DB row is written before the
// local copy is deleted, so it must survive transient Supabase outages.
func (c *Client) SaveRecording(rec *Recording) error {
	resp, err := c.requestWithRetryN("POST", "/recordings?on_conflict=filename", rec, metadataSaveMaxRetries)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// GetRecording retrieves a recording by filename
func (c *Client) GetRecording(filename string) (*Recording, error) {
	var recordings []Recording
	err := c.getN(fmt.Sprintf("/recordings?filename=eq.%s&limit=1", url.QueryEscape(filename)), &recordings, metadataSaveMaxRetries)
	if err != nil {
		return nil, err
	}
	if len(recordings) == 0 {
		return nil, fmt.Errorf("recording not found")
	}
	return &recordings[0], nil
}

// HasUploadedLinks returns true when the recording identified by filename has
// at least one persisted upload_link.  The recordings row is created at enqueue
// time (SaveRecordingBasics) — BEFORE any upload — so "row exists" is not proof
// that the file's content reached a host.  Disk cleanup must only delete local
// files whose content is actually safe in the cloud.
//
// It resolves through LookupRecordingLinkState rather than an embedded
// `upload_links(host)` select: the deployed schema has no foreign key between
// upload_links and recordings, so PostgREST rejects that embed with PGRST200 and
// an embed-based check answers "no links" (with an error) for every file.
func (c *Client) HasUploadedLinks(filename string) (bool, error) {
	_, hasLinks, err := c.LookupRecordingLinkState([]string{filename})
	if err != nil {
		return false, err
	}
	return hasLinks[filename], nil
}

// GetRecordingFilenames returns every filename in the recordings table.
//
// Used by callers that need "which files does the database know about?" — e.g.
// deciding which preview_images rows belong to a recording.  It deliberately
// selects nothing but the filename: the table is large and the answer is a set.
func (c *Client) GetRecordingFilenames() (map[string]bool, error) {
	var rows []struct {
		Filename string `json:"filename"`
	}
	if err := c.getAllPaginated("/recordings?select=filename", &rows); err != nil {
		return nil, err
	}
	filenames := make(map[string]bool, len(rows))
	for _, r := range rows {
		if r.Filename != "" {
			filenames[r.Filename] = true
		}
	}
	return filenames, nil
}

// LookupRecordingLinkState resolves, for each given filename, the recordings row
// id and whether that recording has at least one upload link.  A filename missing
// from ids has no recordings row at all; a name in ids but not in hasLinks has a
// row but zero upload links — the state a recording is left in when its upload leg
// died and nothing ever repaired it.
//
// This is the FK-free way to ask "does this recording have a host?": upload_links
// has no foreign key to recordings in the deployed schema, so PostgREST rejects an
// embedded `upload_links(host)` select with PGRST200 ("no relationship between
// recordings and upload_links") — an embed-based check cannot work against this
// project at all.  Both sides are queried by hand instead, two small requests per
// releaseBatchSize filenames, and only the ids are sent to the second query.
func (c *Client) LookupRecordingLinkState(filenames []string) (map[string]string, map[string]bool, error) {
	ids := make(map[string]string, len(filenames))
	hasLinks := make(map[string]bool, len(filenames))
	if len(filenames) == 0 {
		return ids, hasLinks, nil
	}

	for start := 0; start < len(filenames); start += releaseBatchSize {
		end := start + releaseBatchSize
		if end > len(filenames) {
			end = len(filenames)
		}
		batch := filenames[start:end]

		var rows []struct {
			ID       string `json:"id"`
			Filename string `json:"filename"`
		}
		path := "/recordings?select=id,filename&filename=in.(" + joinEscaped(batch) + ")"
		if err := c.get(path, &rows); err != nil {
			return nil, nil, fmt.Errorf("lookup recordings batch %d: %w", start/releaseBatchSize+1, err)
		}

		byID := make(map[string]string, len(rows))
		recIDs := make([]string, 0, len(rows))
		for _, r := range rows {
			if r.ID == "" || r.Filename == "" {
				continue
			}
			ids[r.Filename] = r.ID
			byID[r.ID] = r.Filename
			recIDs = append(recIDs, r.ID)
		}

		for s := 0; s < len(recIDs); s += releaseBatchSize {
			e := s + releaseBatchSize
			if e > len(recIDs) {
				e = len(recIDs)
			}
			var links []struct {
				RecordingID string `json:"recording_id"`
			}
			filter := fmt.Sprintf("/upload_links?select=recording_id&recording_id=in.(%s)", strings.Join(recIDs[s:e], ","))
			if err := c.get(filter, &links); err != nil {
				return nil, nil, fmt.Errorf("lookup upload links batch %d: %w", s/releaseBatchSize+1, err)
			}
			for _, l := range links {
				if name := byID[l.RecordingID]; name != "" {
					hasLinks[name] = true
				}
			}
		}
	}
	return ids, hasLinks, nil
}

// GetRecordingsByUsername retrieves all recordings for a username
func (c *Client) GetRecordingsByUsername(username string) ([]Recording, error) {
	var recordings []Recording
	err := c.get(fmt.Sprintf("/recordings?username=eq.%s&order=timestamp.desc", url.QueryEscape(username)), &recordings)
	return recordings, err
}

// GetAllRecordings retrieves all recordings
func (c *Client) GetAllRecordings() ([]Recording, error) {
	var recordings []Recording
	err := c.getAllPaginated("/recordings?order=timestamp.desc", &recordings)
	return recordings, err
}

// ThumbAttemptColumn is the recordings column that carries the fleet-wide
// remote-thumbnail lease stamp (see the 20260922 migration).
const ThumbAttemptColumn = "thumb_attempt_at"

// RecordingThumbAttemptColumnMissing reports whether err means the
// thumb_attempt_at column is absent from the schema, i.e. the 20260922
// migration has not been applied.  Callers use it to degrade to the lease-less
// behaviour instead of failing, so deploying the code before the migration
// never stops thumbnail recovery.
func RecordingThumbAttemptColumnMissing(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.Contains(msg, ThumbAttemptColumn) {
		return false
	}
	// 42703 = undefined_column (Postgres); PGRST204 = PostgREST schema cache miss.
	return strings.Contains(msg, "42703") || strings.Contains(msg, "PGRST204")
}

// GetRecordingsMissingThumbnails retrieves only the recording rows whose
// thumbnail_url is NULL/empty, together with the fleet-wide attempt lease.  This
// is a small subset of the full table, so the periodic thumbnail-backfill sweep
// can avoid downloading every row on every node on every tick.
//
// When the lease column has not been deployed yet the query falls back to the
// pre-lease projection: recovery still works, it just cannot coordinate the
// fleet (and the per-node cooldown is all that applies).
func (c *Client) GetRecordingsMissingThumbnails() ([]Recording, error) {
	const base = "/recordings?select=id,filename,thumbnail_url,sprite_url,preview_url"
	var withLease []Recording
	if err := c.getAllPaginated(base+","+ThumbAttemptColumn+"&thumbnail_url=is.null", &withLease); err != nil {
		if !RecordingThumbAttemptColumnMissing(err) {
			return nil, err
		}
		var withoutLease []Recording
		return withoutLease, c.getAllPaginated(base+"&thumbnail_url=is.null", &withoutLease)
	}
	return withLease, nil
}

// HostBackoffTable is the table carrying fleet-wide upload-host backoffs.
const HostBackoffTable = "upload_host_backoffs"

// HostBackoff is one fleet-wide upload-host backoff row.  See the 20260922
// upload_host_backoffs migration: upload credentials are shared by every node,
// so a per-account quota (VidMoly's 50 API requests/day) is really a fleet cap
// and one node's discovery must stop the rest from re-discovering it.
type HostBackoff struct {
	Host         string `json:"host"`
	BackoffUntil string `json:"backoff_until"`
	Reason       string `json:"reason,omitempty"`
	UpdatedBy    string `json:"updated_by,omitempty"`
}

// HostBackoffTableMissing reports whether err means the backoff table is not
// deployed on this project (migration not applied, or not yet in the PostgREST
// schema cache).  Callers degrade to the in-process timed-disable behaviour
// instead of failing.
func HostBackoffTableMissing(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if !strings.Contains(msg, HostBackoffTable) {
		return false
	}
	// PGRST205 = table not in the schema cache; 42P01 = undefined_table.
	return strings.Contains(msg, "PGRST205") || strings.Contains(msg, "42P01")
}

// UntilTime parses a backoff row's expiry.  ok is false when the stamp is
// missing or unparseable, which callers treat as "no backoff" — a malformed row
// must never be able to skip a host forever.
func (b HostBackoff) UntilTime() (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, b.BackoffUntil)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

// ListHostBackoffs returns every stored host backoff row, including the reason
// and reporting node.  The uploader only needs host -> expiry (GetHostBackoffs),
// but the admin page shows operators WHY a host is being skipped, so the full
// row is needed there.
//
// Rows with a blank host or an unparseable expiry are dropped here rather than
// pushed onto callers, so both consumers share one definition of a usable row.
func (c *Client) ListHostBackoffs() ([]HostBackoff, error) {
	var rows []HostBackoff
	if err := c.getAllPaginated("/"+HostBackoffTable+"?select=host,backoff_until,reason,updated_by", &rows); err != nil {
		return nil, err
	}
	usable := make([]HostBackoff, 0, len(rows))
	for _, r := range rows {
		r.Host = strings.TrimSpace(r.Host)
		if r.Host == "" {
			continue
		}
		if _, ok := r.UntilTime(); !ok {
			continue
		}
		usable = append(usable, r)
	}
	return usable, nil
}

// GetHostBackoffs returns every fleet-wide host backoff as host -> expiry.
// Expired rows are returned too (the caller compares against now), so a clock
// skew on one node cannot make it drop a still-valid entry.
func (c *Client) GetHostBackoffs() (map[string]time.Time, error) {
	rows, err := c.ListHostBackoffs()
	if err != nil {
		return nil, err
	}
	backoffs := make(map[string]time.Time, len(rows))
	for _, r := range rows {
		if until, ok := r.UntilTime(); ok {
			backoffs[r.Host] = until
		}
	}
	return backoffs, nil
}

// SetHostBackoff upserts one host's fleet-wide backoff so every other node
// skips it until the expiry.  Best-effort: a failure only means peers learn
// about the cap from their own attempt instead of from us.
func (c *Client) SetHostBackoff(host string, until time.Time, reason, nodeID string) error {
	if strings.TrimSpace(host) == "" || until.IsZero() {
		return nil
	}
	row := HostBackoff{
		Host:         host,
		BackoffUntil: until.UTC().Format(time.RFC3339),
		Reason:       reason,
		UpdatedBy:    nodeID,
	}
	// post() sends Prefer: resolution=merge-duplicates, so this upserts on the
	// host primary key rather than duplicating the row.
	var out []HostBackoff
	return c.post("/"+HostBackoffTable, row, &out)
}

// ClaimRecordingThumbAttempt tries to take the exclusive, fleet-wide right to
// attempt remote-thumbnail recovery for one recording within cooldown.
//
// It is a conditional PATCH, not a read-then-write: the filter only matches a
// row whose lease is unset or older than the cutoff, and PostgREST returns the
// rows it actually updated (Prefer: return=representation).  Two nodes racing
// therefore produce one winner — the loser's PATCH matches zero rows — which is
// what turns "all 15 nodes re-download the same dead file" into one attempt per
// cooldown.  The stamp is written BEFORE the download begins, so a crashed or
// timed-out attempt still backs the fleet off instead of re-hammering the host.
//
// Returns (false, nil) when another node holds the lease.
func (c *Client) ClaimRecordingThumbAttempt(filename string, cooldown time.Duration) (bool, error) {
	if filename == "" {
		return false, nil
	}
	cutoff := time.Now().UTC().Add(-cooldown).Format(time.RFC3339)
	// The thumbnail_url guard is what makes the claim precise: a node whose
	// recording list was already stale (another node recovered the thumbnail
	// moments ago) matches zero rows and stops instead of re-downloading.
	path := fmt.Sprintf(
		"/recordings?filename=eq.%s&thumbnail_url=is.null&or=(%s.is.null,%s.lt.%s)&select=id",
		url.QueryEscape(filename), ThumbAttemptColumn, ThumbAttemptColumn, url.QueryEscape(cutoff))

	resp, err := c.requestWithRetry("PATCH", path, map[string]interface{}{
		ThumbAttemptColumn: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var claimed []struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&claimed); err != nil {
		// A 204/empty body means no row matched (PostgREST returns 200 with
		// [] for representation PATCHes, so this is defensive).
		return false, nil
	}
	return len(claimed) > 0, nil
}

// GetRecordingsMissingSpriteOrPreview retrieves recordings that already show a
// thumbnail but are still missing sprite_url and/or preview_url.  These rows
// are invisible to GetRecordingsMissingThumbnails (their thumbnail exists), yet
// preview_images may already hold the sprite/preview the video card needs, so
// the DB->DB thumb-sync sweep can merge them without regenerating anything.
func (c *Client) GetRecordingsMissingSpriteOrPreview() ([]Recording, error) {
	var recordings []Recording
	err := c.getAllPaginated("/recordings?select=id,filename,thumbnail_url,sprite_url,preview_url&thumbnail_url=not.eq.&or=(sprite_url.is.null,sprite_url.eq.,preview_url.is.null,preview_url.eq.)", &recordings)
	return recordings, err
}

// GetRecordingsWithThumbnails returns all recordings that have a non-empty
// thumbnail_url.  Used by the reverse-sync sweep to find recordings whose
// thumbnails should be backfilled into preview_images.
func (c *Client) GetRecordingsWithThumbnails() ([]Recording, error) {
	var recordings []Recording
	err := c.getAllPaginated("/recordings?select=id,filename,thumbnail_url,sprite_url,preview_url&thumbnail_url=not.eq.", &recordings)
	return recordings, err
}

// HasRecordingThumbnails returns true when the recording identified by filename
// has non-empty thumbnail_url, sprite_url, and preview_url in the database.
// Used by the pipeline cleanup to verify thumbnails actually persisted before
// deleting local files — prevents data loss when DB writes fail silently.
func (c *Client) HasRecordingThumbnails(filename string) (hasThumb, hasSprite, hasPreview bool, err error) {
	var recs []struct {
		ThumbnailURL string `json:"thumbnail_url"`
		SpriteURL    string `json:"sprite_url"`
		PreviewURL   string `json:"preview_url"`
	}
	err = c.getN(
		fmt.Sprintf("/recordings?filename=eq.%s&select=thumbnail_url,sprite_url,preview_url&limit=1",
			url.QueryEscape(filename)),
		&recs, defaultMaxRetries,
	)
	if err != nil {
		return false, false, false, err
	}
	if len(recs) == 0 {
		return false, false, false, nil
	}
	return recs[0].ThumbnailURL != "", recs[0].SpriteURL != "", recs[0].PreviewURL != "", nil
}

// CountRecordings returns the total number of recording rows in Supabase using
// PostgREST's exact-count header, so we never have to download every row just
// to show a tally on the admin panel.
func (c *Client) CountRecordings() (int, error) {
	req, err := http.NewRequest("GET", c.URL+"/rest/v1/recordings?select=id", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("apikey", c.APIKey)
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Prefer", "count=exact")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	// PostgREST returns the total in "Content-Range: <start>-<end>/<total>"
	// (or "*/<total>" when no rows match).
	cr := resp.Header.Get("Content-Range")
	if idx := strings.LastIndex(cr, "/"); idx >= 0 {
		if n, err := strconv.Atoi(strings.TrimSpace(cr[idx+1:])); err == nil {
			return n, nil
		}
	}
	return 0, fmt.Errorf("could not parse Content-Range: %q", cr)
}

// DeleteRecording removes a recording
func (c *Client) DeleteRecording(filename string) error {
	return c.delete(fmt.Sprintf("/recordings?filename=eq.%s", url.QueryEscape(filename)))
}

// DeleteOrphanedRecordings finds and deletes recording rows that have no
// thumbnail_url, no embed_url, no upload links, and are older than orphanAge.
// These are created when SaveRecordingBasics runs at enqueue time but the
// runner is killed before the pipeline completes.  Returns the number deleted.
func (c *Client) DeleteOrphanedRecordings(orphanAge time.Duration) (int, error) {
	cutoff := time.Now().UTC().Add(-orphanAge).Format(time.RFC3339)

	// 1. Find candidates: no thumbnail, no embed, old enough.
	var candidates []Recording
	err := c.get(fmt.Sprintf(
		"/recordings?select=id,username,filename,instance_id"+
			"&thumbnail_url=is.null&embed_url=is.null"+
			"&created_at=lt.%s"+
			"&order=created_at.desc&limit=500",
		url.QueryEscape(cutoff)), &candidates)
	if err != nil {
		return 0, fmt.Errorf("query orphans: %w", err)
	}
	if len(candidates) == 0 {
		return 0, nil
	}

	// 2. Determine which nodes are still online.  A row's instance_id is the
	// node that enqueued it (DBInstanceID); if that node is still heartbeating,
	// its pipeline may simply be stuck in the thumbnail/upload queue behind a
	// backlog (thumbnails are generated AFTER the row is created, and a big
	// batch can take well over 30 min).  Deleting such a row mid-upload makes
	// every subsequent per-host link save fail with a 23503 FK violation
	// (observed live: "Key (recording_id) is not present in table \"recordings\"").
	// Only rows from dead/unknown instances are treated as true orphans.
	online := map[string]bool{}
	if nodes, err := c.GetNodes(); err == nil {
		for _, n := range nodes {
			if n.Status == "online" || n.Status == "draining" {
				online[n.NodeID] = true
			}
		}
	}

	// 2b. For each candidate from a non-online instance, verify zero upload_links.
	var orphanIDs []string
	var orphanFiles []string
	for _, rec := range candidates {
		if rec.InstanceID != "" && online[rec.InstanceID] {
			continue // owner node is alive — its pipeline still owns this row
		}
		var links []UploadLink
		err := c.get(fmt.Sprintf("/upload_links?select=id&recording_id=eq.%s&limit=1", rec.ID), &links)
		if err != nil {
			continue
		}
		if len(links) == 0 {
			orphanIDs = append(orphanIDs, rec.ID)
			orphanFiles = append(orphanFiles, rec.Filename)
		}
	}

	if len(orphanIDs) == 0 {
		return 0, nil
	}

	// 3. Delete in batches.
	deleted := 0
	batchSize := 50
	for i := 0; i < len(orphanIDs); i += batchSize {
		end := i + batchSize
		if end > len(orphanIDs) {
			end = len(orphanIDs)
		}
		batch := orphanIDs[i:end]
		idFilter := strings.Join(batch, ",")
		err := c.delete(fmt.Sprintf("/recordings?id=in.(%s)", idFilter))
		if err != nil {
			continue
		}
		deleted += len(batch)
	}

	// 4. Cascade: remove the orphaned recordings' preview_images rows.  These
	// are keyed by filename (no FK), so deleting only the recordings row leaves
	// a stray preview_images row behind — the exact source of the ~30 orphan
	// rows observed in the DB (a thumbnail stored on a recording that orphan
	// cleanup then deleted).  Match by the same filename to keep the two tables
	// consistent.
	if deleted > 0 {
		fileFilter := make([]string, 0, len(orphanFiles))
		for _, f := range orphanFiles {
			if f != "" {
				fileFilter = append(fileFilter, f)
			}
		}
		for i := 0; i < len(fileFilter); i += batchSize {
			end := i + batchSize
			if end > len(fileFilter) {
				end = len(fileFilter)
			}
			files := fileFilter[i:end]
			err := c.delete(fmt.Sprintf("/preview_images?filename=in.(%s)", strings.Join(files, ",")))
			if err != nil {
				continue
			}
		}
	}

	return deleted, nil
}

// GetRecordingsWithCatboxThumbnails retrieves only recordings whose thumbnail_url
// contains "catbox.moe" — used by backfillmirrors to avoid loading all 11K+ rows.
func (c *Client) GetRecordingsWithCatboxThumbnails() ([]Recording, error) {
	var recordings []Recording
	err := c.getAllPaginated("/recordings?thumbnail_url=like.*catbox.moe*&order=timestamp.desc", &recordings)
	return recordings, err
}

// GetRecordingsWithCatboxURLs retrieves recordings where ANY of thumbnail_url,
// sprite_url, or preview_url contains "catbox.moe". Used by backfillmirrors
// to find all Catbox-hosted images across all asset types.
func (c *Client) GetRecordingsWithCatboxURLs() ([]Recording, error) {
	// Use PostgREST's or() filter to find recordings where ANY of thumbnail_url,
	// sprite_url, or preview_url contains "catbox.moe" in a single query.
	var recordings []Recording
	err := c.getAllPaginated(
		"/recordings?or=(thumbnail_url.like.*catbox.moe*,sprite_url.like.*catbox.moe*,preview_url.like.*catbox.moe*)&order=timestamp.desc",
		&recordings,
	)
	if err != nil {
		return nil, fmt.Errorf("get catbox URLs: %w", err)
	}
	return recordings, nil
}

// DeletePreviewImage removes a preview image by filename
func (c *Client) DeletePreviewImage(filename string) error {
	return c.delete(fmt.Sprintf("/preview_images?filename=eq.%s", url.QueryEscape(filename)))
}

// DeleteOrphanedPreviewImages removes preview_images rows that no longer belong
// to a recording: older than cutoff and whose filename is not in keep.
//
// These rows are invisible litter that nothing cleans up on its own.  The table
// has `recording_id UUID REFERENCES recordings(id) ON DELETE CASCADE`, but every
// stale row in practice has recording_id NULL (it was written by the preview-link
// paths, which know only the filename), so the cascade can never reach them.  A
// thumbnail-less recordings row can't be the trigger either: the row is gone.
//
// keep is the caller's "do not touch" set — filenames that still have a
// recordings row (their DB assets are the last copy of the presentation images
// once the local file is gone) plus any video file still present on disk.
//
// The scan is cursor-based (id > last-seen) rather than offset-based on purpose:
// kept rows stay in the table, so an offset scan would revisit them forever.
func (c *Client) DeleteOrphanedPreviewImages(cutoff time.Time, keep map[string]bool) (int, error) {
	return c.pruneOrphanedPreviewImages(cutoff, keep, true)
}

// CountOrphanedPreviewImages reports how many rows DeleteOrphanedPreviewImages
// would remove, without deleting anything.  It shares the scan, so a dry run can
// never drift from what the real sweep does.
func (c *Client) CountOrphanedPreviewImages(cutoff time.Time, keep map[string]bool) (int, error) {
	return c.pruneOrphanedPreviewImages(cutoff, keep, false)
}

// pruneOrphanedPreviewImages walks preview_images older than cutoff and, per row
// whose filename is not in keep, either deletes it (in small id=in.(...) batches,
// well under the proxy's URL limit) or merely counts it.
func (c *Client) pruneOrphanedPreviewImages(cutoff time.Time, keep map[string]bool, remove bool) (int, error) {
	const (
		pageSize    = 500
		deleteBatch = 50
	)

	total := 0
	cursor := ""
	for {
		path := fmt.Sprintf("/preview_images?select=id,filename&uploaded_at=lt.%s&order=id.asc&limit=%d",
			url.QueryEscape(cutoff.UTC().Format(time.RFC3339)), pageSize)
		if cursor != "" {
			path += "&id=gt." + url.QueryEscape(cursor)
		}

		var page []struct {
			ID       string `json:"id"`
			Filename string `json:"filename"`
		}
		if err := c.get(path, &page); err != nil {
			return total, fmt.Errorf("page orphaned preview images: %w", err)
		}
		if len(page) == 0 {
			return total, nil
		}
		cursor = page[len(page)-1].ID

		ids := make([]string, 0, len(page))
		for _, row := range page {
			if row.ID == "" || keep[row.Filename] {
				continue
			}
			ids = append(ids, row.ID)
		}
		if remove {
			for start := 0; start < len(ids); start += deleteBatch {
				end := start + deleteBatch
				if end > len(ids) {
					end = len(ids)
				}
				batch := ids[start:end]
				if err := c.delete(fmt.Sprintf("/preview_images?id=in.(%s)", strings.Join(batch, ","))); err != nil {
					return total, fmt.Errorf("delete orphaned preview images (%d row batch): %w", len(batch), err)
				}
			}
		}
		total += len(ids)

		if len(page) < pageSize {
			return total, nil
		}
	}
}

// DeleteUploadLinksByRecordingID removes all upload links for a recording
func (c *Client) DeleteUploadLinksByRecordingID(recordingID string) error {
	return c.delete(fmt.Sprintf("/upload_links?recording_id=eq.%s", url.QueryEscape(recordingID)))
}

// DeleteUploadLinksByHost removes every upload link recorded for one host.  Used
// by the upload-link write-path canary (server.VerifyUploadLinkWritePath) to
// remove its probe row — and, because it filters on the host rather than the
// probe's id, to clear up a probe row left behind by a run that was killed
// between its insert and its delete.
func (c *Client) DeleteUploadLinksByHost(host string) error {
	return c.delete(fmt.Sprintf("/upload_links?host=eq.%s", url.QueryEscape(host)))
}

// ============================================================================
// UPLOAD LINKS
// ============================================================================

type UploadLink struct {
	ID          string `json:"id,omitempty"`
	RecordingID string `json:"recording_id"`
	Host        string `json:"host"`
	URL         string `json:"url"`
	UploadedAt  string `json:"uploaded_at,omitempty"`
	InstanceID  string `json:"instance_id,omitempty"`
}

// SaveUploadLink creates or updates an upload link.
// Uses on_conflict to atomically upsert by (recording_id, host), making
// repeated calls idempotent and preventing duplicate rows.
func (c *Client) SaveUploadLink(link *UploadLink) error {
	resp, err := c.requestWithRetry("POST", "/upload_links?on_conflict=recording_id,host", link)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// SaveUploadLinks batch-saves all upload links in a single request.
// Uses on_conflict to upsert by (recording_id, host).
// Part of the SaveRecordingWithLinks metadata path, so it retries
// metadataSaveMaxRetries times like SaveRecording/GetRecording.
func (c *Client) SaveUploadLinks(links []UploadLink) error {
	resp, err := c.requestWithRetryN("POST", "/upload_links?on_conflict=recording_id,host", links, metadataSaveMaxRetries)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// SaveRecordingWithLinksRPC atomically upserts a recording, its upload links,
// and preview images in a single Supabase RPC call.  This replaces the 3-call
// sequential path (SaveRecording → GetRecording → SaveUploadLinks) which could
// stall pipelines for minutes under Supabase load (each call retries up to 10
// times with exponential backoff).
//
// The RPC function (save_recording_with_links) runs SECURITY DEFINER inside a
// single Postgres transaction — either all three writes succeed or none do.
// Returns the recording ID on success.
func (c *Client) SaveRecordingWithLinksRPC(rec *Recording, links []UploadLink, preview *PreviewImage) (string, error) {
	type rpcParams struct {
		Rec     json.RawMessage `json:"p_rec"`
		Links   json.RawMessage `json:"p_links"`
		Preview json.RawMessage `json:"p_preview"`
	}

	recJSON, err := json.Marshal(rec)
	if err != nil {
		return "", fmt.Errorf("marshal recording: %w", err)
	}

	var linksJSON json.RawMessage
	if len(links) > 0 {
		linksJSON, err = json.Marshal(links)
		if err != nil {
			return "", fmt.Errorf("marshal links: %w", err)
		}
	} else {
		linksJSON = json.RawMessage("null")
	}

	var previewJSON json.RawMessage
	if preview != nil {
		previewJSON, err = json.Marshal(preview)
		if err != nil {
			return "", fmt.Errorf("marshal preview: %w", err)
		}
	} else {
		previewJSON = json.RawMessage("null")
	}

	params := rpcParams{
		Rec:     recJSON,
		Links:   linksJSON,
		Preview: previewJSON,
	}

	var recordingID string
	if err := c.rpcN("save_recording_with_links", params, &recordingID, metadataSaveMaxRetries); err != nil {
		return "", err
	}
	return recordingID, nil
}

// rpcN calls a Supabase PostgRPC function and decodes the scalar result.
// It retries up to maxRetries times on transient errors.
func (c *Client) rpcN(name string, params interface{}, result interface{}, maxRetries int) error {
	jsonBody, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal rpc params: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := c.request("POST", "/rpc/"+name, json.RawMessage(jsonBody))
		if err != nil {
			lastErr = err
			if attempt < maxRetries-1 {
				time.Sleep(retryBackoff(attempt))
				continue
			}
			return err
		}

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode >= 400 {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
			// Retry on transient Supabase errors
			if resp.StatusCode == 503 || resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
				if attempt < maxRetries-1 {
					time.Sleep(retryBackoff(attempt))
					continue
				}
			}
			return lastErr
		}

		// Decode scalar result (text, int, etc.)
		if result != nil {
			if err := json.Unmarshal(bodyBytes, result); err != nil {
				// Some RPCs return "" (empty string) — treat as empty result
				if string(bodyBytes) == "null" || string(bodyBytes) == `""` || len(bodyBytes) == 0 {
					return nil
				}
				return fmt.Errorf("decode rpc result: %w (body: %s)", err, string(bodyBytes))
			}
		}
		return nil
	}
	return lastErr
}

// GetUploadLinks retrieves all upload links for a recording
func (c *Client) GetUploadLinks(recordingID string) ([]UploadLink, error) {
	var links []UploadLink
	err := c.get(fmt.Sprintf("/upload_links?recording_id=eq.%s", url.QueryEscape(recordingID)), &links)
	return links, err
}

// GetAllUploadLinks retrieves ALL upload links in a single batch query.
// The caller can group by recording_id for O(1) per-recording lookup.
func (c *Client) GetAllUploadLinks() ([]UploadLink, error) {
	var links []UploadLink
	err := c.getAllPaginated("/upload_links", &links)
	return links, err
}

// CountUploadedVideosBelowDuration returns how many recordings with a probed
// duration strictly between 0 and thresholdSeconds seconds have been uploaded
// (i.e. have at least one row in upload_links). Recordings whose duration is
// 0 or NULL (ffprobe miss) are excluded.
//
// It delegates to ListUploadedVideosBelowDuration so both helpers share the
// same two-query logic, threshold guard and 50000-row cap. The two reads are
// not atomic, so a video uploaded in the brief window between them may be
// missed — fine for a monitoring/stats count.
func (c *Client) CountUploadedVideosBelowDuration(thresholdSeconds float64) (int, error) {
	list, err := c.ListUploadedVideosBelowDuration(thresholdSeconds)
	if err != nil {
		return 0, err
	}
	return len(list), nil
}

// RecordingWithLinks is one uploaded recording paired with the URLs it was
// uploaded to, keyed by upload host (e.g. "GoFile", "Streamtape", "Vidara").
type RecordingWithLinks struct {
	ID        string            `json:"id"`
	Username  string            `json:"username"`
	Filename  string            `json:"filename"`
	Timestamp string            `json:"timestamp"`
	Duration  float64           `json:"duration"`
	Filesize  int64             `json:"filesize"`
	Links     map[string]string `json:"links"` // upload host -> URL
}

// ListUploadedVideosBelowDuration returns the recordings with a probed
// duration strictly between 0 and thresholdSeconds seconds that have been
// uploaded (i.e. have at least one row in upload_links), each with its upload
// links keyed by host. Results are ordered by duration ascending (shortest
// first) so the smallest uploads surface first for review. Recordings whose
// duration is 0 or NULL (ffprobe miss) and recordings with no upload links
// are excluded.
//
// Like CountUploadedVideosBelowDuration it uses two batched GETs (upload_links
// then duration-filtered recordings) intersected in memory, because PostgREST's
// embedded-resource join is not usable on deployments whose schema cache lacks
// the recordings → upload_links foreign key (PGRST200). The result is capped
// at 50000 matching recordings, matching the other GetAll* helpers.
func (c *Client) ListUploadedVideosBelowDuration(thresholdSeconds float64) ([]RecordingWithLinks, error) {
	if thresholdSeconds <= 0 || math.IsNaN(thresholdSeconds) || math.IsInf(thresholdSeconds, 0) {
		return nil, nil
	}

	// Step 1: group upload links by recording id.
	links, err := c.GetAllUploadLinks()
	if err != nil {
		return nil, err
	}
	byRecording := make(map[string][]UploadLink)
	for _, l := range links {
		if l.RecordingID != "" {
			byRecording[l.RecordingID] = append(byRecording[l.RecordingID], l)
		}
	}
	if len(byRecording) == 0 {
		return nil, nil
	}

	// Step 2: recordings with 0 < duration < threshold, keeping only uploaded ones.
	// The duration filters run server-side so only eligible rows are transferred.
	path := fmt.Sprintf("/recordings?select=id,username,filename,timestamp,duration,filesize&duration=gt.0&duration=lt.%s",
		strconv.FormatFloat(thresholdSeconds, 'f', -1, 64))
	var recs []Recording
	if err := c.getAllPaginated(path, &recs); err != nil {
		return nil, err
	}

	var out []RecordingWithLinks
	for _, r := range recs {
		recLinks, ok := byRecording[r.ID]
		if !ok || len(recLinks) == 0 {
			continue
		}
		linksMap := make(map[string]string, len(recLinks))
		for _, l := range recLinks {
			if l.Host != "" {
				linksMap[l.Host] = l.URL
			}
		}
		out = append(out, RecordingWithLinks{
			ID:        r.ID,
			Username:  r.Username,
			Filename:  r.Filename,
			Timestamp: r.Timestamp,
			Duration:  r.Duration,
			Filesize:  r.Filesize,
			Links:     linksMap,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Duration < out[j].Duration })
	return out, nil
}

// ============================================================================
// APP SETTINGS
// ============================================================================

type AppSetting struct {
	Key       string          `json:"key"`
	Value     json.RawMessage `json:"value"`
	UpdatedAt string          `json:"updated_at,omitempty"`
}

// SaveSetting creates or updates an app setting
func (c *Client) SaveSetting(key string, value interface{}) error {
	jsonValue, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal value: %w", err)
	}

	setting := &AppSetting{
		Key:   key,
		Value: jsonValue,
	}

	// Upsert using Prefer header
	resp, err := c.requestWithRetry("POST", "/app_settings", setting)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// GetSetting retrieves an app setting
func (c *Client) GetSetting(key string, result interface{}) error {
	var settings []AppSetting
	err := c.get(fmt.Sprintf("/app_settings?key=eq.%s&limit=1", url.QueryEscape(key)), &settings)
	if err != nil {
		return err
	}
	if len(settings) == 0 {
		return fmt.Errorf("setting not found")
	}

	return json.Unmarshal(settings[0].Value, result)
}

// ============================================================================
// TUNNELS
// ============================================================================

type Tunnel struct {
	ID         string `json:"id,omitempty"`
	URL        string `json:"url"`
	RunID      int    `json:"run_id"`
	InstanceID string `json:"instance_id,omitempty"`
	IsActive   bool   `json:"is_active"`
	CreatedAt  string `json:"created_at,omitempty"`
	ExpiresAt  string `json:"expires_at,omitempty"`
}

// SaveTunnel creates a new tunnel
func (c *Client) SaveTunnel(tunnel *Tunnel) error {
	var result []Tunnel
	return c.post("/tunnels", tunnel, &result)
}

// GetActiveTunnel retrieves the most recent active tunnel for the given instance
func (c *Client) GetActiveTunnel(instanceID string) (*Tunnel, error) {
	var tunnels []Tunnel
	err := c.get(fmt.Sprintf("/tunnels?is_active=eq.true&instance_id=eq.%s&order=created_at.desc&limit=1", url.QueryEscape(instanceID)), &tunnels)
	if err != nil {
		return nil, err
	}
	if len(tunnels) == 0 {
		return nil, fmt.Errorf("no active tunnel found")
	}
	return &tunnels[0], nil
}

// DeactivateOldTunnels marks all tunnels as inactive for the given instance
func (c *Client) DeactivateOldTunnels(instanceID string) error {
	return c.patch(fmt.Sprintf("/tunnels?is_active=eq.true&instance_id=eq.%s", url.QueryEscape(instanceID)), map[string]interface{}{
		"is_active": false,
	})
}

// ============================================================================
// CHANNEL LOGS
// ============================================================================

type ChannelLog struct {
	ID         string `json:"id,omitempty"`
	ChannelID  string `json:"channel_id,omitempty"`
	Username   string `json:"username"`
	LogLevel   string `json:"log_level"`
	Message    string `json:"message"`
	InstanceID string `json:"instance_id,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
}

// SaveLog creates a new log entry
func (c *Client) SaveLog(log *ChannelLog) error {
	var result []ChannelLog
	return c.post("/channel_logs", log, &result)
}

// SaveLogBestEffort persists a channel log line with a single HTTP attempt
// (no retries), so the log forwarder can fire-and-forget without spawning
// long-lived goroutines during a Supabase outage. Log lines are diagnostics,
// not durability-critical: a dropped line is fine. When the instance_id
// column does not exist yet (migration not applied), it retries once without
// that field so the line still lands.
func (c *Client) SaveLogBestEffort(log *ChannelLog) error {
	resp, err := c.request("POST", "/channel_logs", log)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		bodyStr := string(bodyBytes)
		if strings.Contains(bodyStr, "PGRST204") && log.InstanceID != "" {
			log.InstanceID = ""
			return c.SaveLogBestEffort(log)
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, bodyStr)
	}
	return nil
}

// GetLogs retrieves logs for a channel
func (c *Client) GetLogs(username string, limit int) ([]ChannelLog, error) {
	var logs []ChannelLog
	err := c.get(fmt.Sprintf("/channel_logs?username=eq.%s&order=created_at.desc&limit=%d", url.QueryEscape(username), limit), &logs)
	return logs, err
}

// ============================================================================
// PREVIEW IMAGES
// ============================================================================

type PreviewImage struct {
	ID               string            `json:"id,omitempty"`
	RecordingID      string            `json:"recording_id,omitempty"`
	Filename         string            `json:"filename"`
	ThumbnailURL     string            `json:"thumbnail_url,omitempty"`
	SpriteURL        string            `json:"sprite_url,omitempty"`
	PreviewURL       string            `json:"preview_url,omitempty"`
	ThumbnailMirrors map[string]string `json:"thumbnail_mirrors,omitempty"` // host -> URL
	SpriteMirrors    map[string]string `json:"sprite_mirrors,omitempty"`    // host -> URL
	PreviewMirrors   map[string]string `json:"preview_mirrors,omitempty"`   // host -> URL
	InstanceID       string            `json:"instance_id,omitempty"`
	UploadedAt       string            `json:"uploaded_at,omitempty"`
}

// SavePreviewImage creates or updates preview image metadata using Supabase's upsert functionality.
// Uses on_conflict to atomically upsert by filename, avoiding TOCTOU race conditions.
func (c *Client) SavePreviewImage(img *PreviewImage) error {
	resp, err := c.requestWithRetry("POST", "/preview_images?on_conflict=filename", img)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// GetPreviewImage retrieves preview image metadata
func (c *Client) GetPreviewImage(filename string) (*PreviewImage, error) {
	var images []PreviewImage
	err := c.get(fmt.Sprintf("/preview_images?filename=eq.%s&limit=1", url.QueryEscape(filename)), &images)
	if err != nil {
		return nil, err
	}
	if len(images) == 0 {
		return nil, fmt.Errorf("preview image not found")
	}
	return &images[0], nil
}

// GetAllPreviewImages returns all preview images from the database.
func (c *Client) GetAllPreviewImages() ([]PreviewImage, error) {
	var images []PreviewImage
	err := c.getAllPaginated("/preview_images", &images)
	return images, err
}

// ============================================================================
// DISK USAGE
// ============================================================================

type DiskUsage struct {
	ID          string `json:"id,omitempty"`
	NodeID      string `json:"node_id,omitempty"`
	TotalBytes  int64  `json:"total_bytes"`
	UsedBytes   int64  `json:"used_bytes"`
	FreeBytes   int64  `json:"free_bytes"`
	PercentUsed int    `json:"percent_used"`
	RecordedAt  string `json:"recorded_at,omitempty"`
}

// SaveDiskUsage records current disk usage
func (c *Client) SaveDiskUsage(usage *DiskUsage) error {
	var result []DiskUsage
	return c.post("/disk_usage", usage, &result)
}

// GetLatestDiskUsage retrieves the most recent disk usage record
func (c *Client) GetLatestDiskUsage() (*DiskUsage, error) {
	var usages []DiskUsage
	err := c.get("/disk_usage?order=recorded_at.desc&limit=1", &usages)
	if err != nil {
		return nil, err
	}
	if len(usages) == 0 {
		return nil, fmt.Errorf("no disk usage records found")
	}
	return &usages[0], nil
}

// ============================================================================
// UPLOAD JOURNAL
// ============================================================================

type UploadJournal struct {
	ID         string `json:"id,omitempty"`
	FileHash   string `json:"file_hash"`
	Filename   string `json:"filename"`
	Host       string `json:"host"`
	Status     string `json:"status"`
	Link       string `json:"link,omitempty"`
	ErrorMsg   string `json:"error_msg"`
	FileSize   int64  `json:"file_size,omitempty"`
	InstanceID string `json:"instance_id,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
}

// SaveJournalEntry creates or updates an upload journal entry.
// Uses on_conflict to upsert by (file_hash, host).
func (c *Client) SaveJournalEntry(entry *UploadJournal) error {
	resp, err := c.requestWithRetry("POST", "/upload_journal?on_conflict=file_hash,host", entry)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// GetJournalByHash retrieves all journal entries for a given file hash.
func (c *Client) GetJournalByHash(fileHash string) ([]UploadJournal, error) {
	var entries []UploadJournal
	err := c.get(fmt.Sprintf("/upload_journal?file_hash=eq.%s&order=host.asc", url.QueryEscape(fileHash)), &entries)
	return entries, err
}

// GetJournalEntriesByStatus retrieves all journal entries with a given status.
func (c *Client) GetJournalEntriesByStatus(status string) ([]UploadJournal, error) {
	var entries []UploadJournal
	err := c.get(fmt.Sprintf("/upload_journal?status=eq.%s&order=created_at.desc", url.QueryEscape(status)), &entries)
	return entries, err
}

// GetJournalSuccessWithLinksFor retrieves the upload-journal success entries
// that carry a persisted download link for exactly the given filenames — the rows
// from which a recording's missing upload_links can be rebuilt after a crash (see
// server.ReconcileMissingUploadLinks).  `link=like.*` matches every non-null,
// non-empty string; NULL rows are excluded automatically.
//
// Deliberately not limited by recency (no limit, no order): the previous "most
// recent N successes" window left every gap older than the window permanently
// unfixable.  Scoping by filename is what keeps that affordable — the caller asks
// only about the recordings that currently have zero upload links (a few hundred
// out of ~27k), so the sweep never pages the whole journal, and each request stays
// inside the proxy's URL budget at releaseBatchSize filenames.
func (c *Client) GetJournalSuccessWithLinksFor(filenames []string) ([]UploadJournal, error) {
	entries := make([]UploadJournal, 0, len(filenames))
	for start := 0; start < len(filenames); start += releaseBatchSize {
		end := start + releaseBatchSize
		if end > len(filenames) {
			end = len(filenames)
		}
		var page []UploadJournal
		path := "/upload_journal?select=file_hash,filename,host,link&status=eq.success&link=like.*&filename=in.(" +
			joinEscaped(filenames[start:end]) + ")"
		if err := c.get(path, &page); err != nil {
			return nil, fmt.Errorf("lookup journal successes batch %d: %w", start/releaseBatchSize+1, err)
		}
		entries = append(entries, page...)
	}
	return entries, nil
}

// RecordingsWithoutUploadLinks returns the filenames of every recording that has
// no upload link at all, via the recordings_without_upload_links() SQL function
// (migration 20260923000000).
//
// A row's presence is not proof of an upload — the row is written at ENQUEUE time
// — and upload_links has no foreign key to recordings, so this question cannot be
// asked with an embedded select (PGRST200) or by diffing client-side without
// downloading ~125k link rows.  The function answers it exactly in one request;
// it is additive and read-only, so callers degrade to "unavailable" when it has
// not been applied yet (see NoHostsFuncMissing).
func (c *Client) RecordingsWithoutUploadLinks() ([]string, error) {
	var rows []struct {
		Filename string `json:"filename"`
	}
	if err := c.get("/rpc/recordings_without_upload_links", &rows); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(rows))
	for _, r := range rows {
		if r.Filename != "" {
			names = append(names, r.Filename)
		}
	}
	return names, nil
}

// NoHostsFuncMissing reports whether err means the
// recordings_without_upload_links() SQL function is not available — either the
// migration has not been applied or PostgREST has not reloaded its schema cache
// yet (PGRST202 "function not found", 42883 undefined_function).
func NoHostsFuncMissing(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "recordings_without_upload_links") &&
		(strings.Contains(msg, "PGRST202") || strings.Contains(msg, "42883"))
}

// GetUploadLinkHosts returns the set of hosts for which a recording already
// has a persisted upload link.  Used by reconciliation to avoid re-inserting
// links that are already present.
func (c *Client) GetUploadLinkHosts(recordingID string) ([]string, error) {
	var links []struct {
		Host string `json:"host"`
	}
	err := c.get(fmt.Sprintf("/upload_links?select=host&recording_id=eq.%s", url.QueryEscape(recordingID)), &links)
	if err != nil {
		return nil, err
	}
	hosts := make([]string, 0, len(links))
	for _, l := range links {
		hosts = append(hosts, l.Host)
	}
	return hosts, nil
}

// DeleteJournalByHash removes all journal entries for a file hash (e.g. after local file is deleted).
func (c *Client) DeleteJournalByHash(fileHash string) error {
	return c.delete(fmt.Sprintf("/upload_journal?file_hash=eq.%s", url.QueryEscape(fileHash)))
}

// ============================================================================
// PIPELINE STATES
// ============================================================================
//
// Schema defined in migrate-combined.sql (CREATE TABLE pipeline_states).

type PipelineState struct {
	FileHash       string            `json:"file_hash"`
	FilePath       string            `json:"file_path"`
	Filename       string            `json:"filename"`
	Username       string            `json:"username"`
	FileSize       int64             `json:"file_size"`
	CurrentStage   string            `json:"current_stage"`
	Failed         bool              `json:"failed"`
	LastError      string            `json:"last_error,omitempty"`
	ThumbURL       string            `json:"thumb_url,omitempty"`
	SpriteURL      string            `json:"sprite_url,omitempty"`
	PreviewURL     string            `json:"preview_url,omitempty"`
	ThumbMirrors   map[string]string `json:"thumb_mirrors,omitempty"`   // host -> URL
	SpriteMirrors  map[string]string `json:"sprite_mirrors,omitempty"`  // host -> URL
	PreviewMirrors map[string]string `json:"preview_mirrors,omitempty"` // host -> URL
	EmbedURL       string            `json:"embed_url,omitempty"`
	LinksJSON      string            `json:"links,omitempty"` // JSON-encoded map[string]string
	Retries        int               `json:"retries,omitempty"`
	NodeID         string            `json:"node_id,omitempty"`
	CreatedAt      string            `json:"created_at,omitempty"`
	UpdatedAt      string            `json:"updated_at,omitempty"`
}

// SavePipelineState upserts a pipeline state by file_hash.
func (c *Client) SavePipelineState(state *PipelineState) error {
	resp, err := c.requestWithRetry("POST", "/pipeline_states?on_conflict=file_hash", state)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return nil
}

// LoadAllPipelineStates retrieves all pipeline states (for crash recovery on restart).
func (c *Client) LoadAllPipelineStates() ([]PipelineState, error) {
	var states []PipelineState
	err := c.get("/pipeline_states?order=created_at.asc", &states)
	return states, err
}

// DeletePipelineState removes a pipeline state by file hash.
func (c *Client) DeletePipelineState(fileHash string) error {
	return c.delete(fmt.Sprintf("/pipeline_states?file_hash=eq.%s", url.QueryEscape(fileHash)))
}

// ============================================================================
// NODES (distributed shards)
// ============================================================================

// Node represents a worker node in the distributed recording system.
type Node struct {
	NodeID          string     `json:"node_id"`
	Hostname        string     `json:"hostname"`
	InstanceLabel   string     `json:"instance_label"`
	SoftwareVersion string     `json:"software_version"`
	Status          string     `json:"status"`
	CurrentLoad     int        `json:"current_load"`
	LastHeartbeat   string     `json:"last_heartbeat,omitempty"`
	WebURL          string     `json:"web_url"`
	SessionDeadline *time.Time `json:"session_deadline,omitempty"`
	CreatedAt       string     `json:"created_at,omitempty"`
	UpdatedAt       string     `json:"updated_at,omitempty"`
}

// UpsertNode registers or updates a node.
func (c *Client) UpsertNode(node *Node) error {
	resp, err := c.requestWithRetry("POST", "/nodes?on_conflict=node_id", node)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// HeartbeatNode updates the last_heartbeat timestamp and current load for a node.
func (c *Client) HeartbeatNode(nodeID string, currentLoad int) error {
	return c.HeartbeatNodeWithDeadline(nodeID, currentLoad, nil)
}

// HeartbeatNodeWithDeadline updates the last_heartbeat timestamp, current load, and
// confirms/heals session_deadline for a node.
func (c *Client) HeartbeatNodeWithDeadline(nodeID string, currentLoad int, deadline *time.Time) error {
	payload := map[string]interface{}{
		"last_heartbeat": "now()",
		"current_load":   currentLoad,
	}
	if deadline != nil && !deadline.IsZero() {
		payload["session_deadline"] = deadline.Format(time.RFC3339)
	}
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s", url.QueryEscape(nodeID)), payload)
}

// EnsureNodeOnline sets status=online for a node that is currently offline or
// unknown.  Used by the heartbeat loop to recover from a "stuck offline"
// state (e.g. reaper marked offline during a restart gap).  Does nothing if
// the node is already online or draining.
func (c *Client) EnsureNodeOnline(nodeID string) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s&status=neq.online&status=neq.draining", url.QueryEscape(nodeID)), map[string]interface{}{
		"status": "online",
	})
}

// UpdateNodeStatus changes the node's status (online/offline/draining).
func (c *Client) UpdateNodeStatus(nodeID, status string) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s", url.QueryEscape(nodeID)), map[string]interface{}{
		"status": status,
	})
}

// ResetNodeLoad zeroes a node's current_load. current_load is only ever
// written by the node's own heartbeat, so a node that goes offline or dies
// would otherwise carry a frozen load forever — inflating the dashboard's
// "Total Load" with channels that were already reclaimed. Called by the
// reaper and graceful shutdown after the node's assignments are released.
func (c *Client) ResetNodeLoad(nodeID string) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s", url.QueryEscape(nodeID)), map[string]interface{}{
		"current_load": 0,
	})
}

// UpdateNodeWebURL sets the public web URL for a node.  Used by the cloudflared
// tunnel reporter so the admin panel's "Visit" link reflects the live tunnel.
func (c *Client) UpdateNodeWebURL(nodeID, webURL string) error {
	return c.patch(fmt.Sprintf("/nodes?node_id=eq.%s", url.QueryEscape(nodeID)), map[string]interface{}{
		"web_url": webURL,
	})
}

// GetNode retrieves a single node by ID.
func (c *Client) GetNode(nodeID string) (*Node, error) {
	var nodes []Node
	err := c.get(fmt.Sprintf("/nodes?node_id=eq.%s&limit=1", url.QueryEscape(nodeID)), &nodes)
	if err != nil {
		return nil, err
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("node %s not found", nodeID)
	}
	return &nodes[0], nil
}

// GetAllNodes returns all registered nodes, ordered by node_id.
func (c *Client) GetAllNodes() ([]Node, error) {
	var nodes []Node
	err := c.get("/nodes?order=node_id.asc", &nodes)
	return nodes, err
}

// GetAliveNodes returns all nodes with status=online and recent heartbeat.
func (c *Client) GetAliveNodes() ([]Node, error) {
	cutoff := time.Now().Add(-180 * time.Second).UTC().Format(time.RFC3339)
	var nodes []Node
	err := c.get(fmt.Sprintf("/nodes?status=eq.online&last_heartbeat=gt.%s&order=node_id.asc", url.QueryEscape(cutoff)), &nodes)
	return nodes, err
}

// GetDeadNodes returns node IDs whose heartbeat is older than the timeout.
// Includes draining/offline nodes — if a draining node hasn't heartbeated
// inside the timeout it's effectively dead (e.g. killed by the GitHub 6h
// limit), so its channels must be reclaimed or they stay stuck assigned to a
// node that will never release them.
func (c *Client) GetDeadNodes(timeout time.Duration) ([]string, error) {
	cutoff := time.Now().Add(-timeout).UTC().Format(time.RFC3339)
	var nodes []Node
	err := c.get(fmt.Sprintf("/nodes?last_heartbeat=lt.%s&select=node_id", url.QueryEscape(cutoff)), &nodes)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(nodes))
	for i, n := range nodes {
		ids[i] = n.NodeID
	}
	return ids, nil
}

// GetNodesWithImminentDeadline returns online nodes whose session_deadline is
// within `window` from now (strictly in the FUTURE). Used to migrate a node's
// channels away BEFORE the node is killed (e.g. GitHub's 6-hour runner limit).
//
// session_deadline=gt.now() is critical: a deadline that has already PASSED
// must not count as "imminent", otherwise the 60s migration cycle keeps
// draining a node that is still alive and heartbeating (its session restart
// simply hasn't fired) while its claim loop re-claims the channels — an
// infinite claim→migrate→reclaim churn that pins channels to no node and
// overloads the migration targets. Past deadlines are the reaper's job: if the
// node truly dies, its channels are reclaimed after the heartbeat timeout.
func (c *Client) GetNodesWithImminentDeadline(window time.Duration) ([]Node, error) {
	cutoff := time.Now().Add(window).UTC().Format(time.RFC3339)
	var nodes []Node
	err := c.get(fmt.Sprintf("/nodes?session_deadline=not.is.null&session_deadline=gt.now()&session_deadline=lt.%s&status=eq.online&order=node_id.asc",
		url.QueryEscape(cutoff)), &nodes)
	if err != nil {
		return nil, err
	}
	return nodes, nil
}

// GetNodes returns every registered node (no filtering). The assignment
// controller uses it to compute the active/draining/dead fleet membership for
// balanced channel distribution.
func (c *Client) GetNodes() ([]Node, error) {
	var nodes []Node
	if err := c.get("/nodes?order=node_id.asc", &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// ============================================================================
// CONTROLLER LEADER LEASE
// ============================================================================

// TryAcquireControllerLease attempts to become the sole assignment controller.
// It calls the claim_controller_lease RPC, which only grants the lease when it
// is unowned or expired (or already held by this node), so exactly one node
// wins even if several race. Returns true if this node now holds the lease.
// The RPC must exist in the database (see migrate_controller_lease.sql) and be
// GRANTed to anon.
func (c *Client) TryAcquireControllerLease(nodeID string, ttlSeconds int) (bool, error) {
	resp, err := c.requestWithRetry("POST", "/rpc/claim_controller_lease", map[string]interface{}{
		"p_node_id": nodeID,
		"p_ttl":     ttlSeconds,
	})
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, fmt.Errorf("read lease response: %w", err)
	}

	// 1. Try decoding as scalar bool (standard PostgREST RPC return for RETURNS boolean)
	var scalarBool bool
	if err := json.Unmarshal(body, &scalarBool); err == nil {
		return scalarBool, nil
	}

	// 2. Fallback: table format ([]struct)
	var rows []struct {
		ClaimControllerLease bool `json:"claim_controller_lease"`
	}
	if err := json.Unmarshal(body, &rows); err == nil {
		return len(rows) > 0 && rows[0].ClaimControllerLease, nil
	}

	return false, fmt.Errorf("decode lease response: %s", string(body))
}

// ReleaseControllerLease expires the lease early if this node currently holds
// it. Called on graceful shutdown so another node can take over promptly.
//
// It goes through the release_controller_lease RPC (SECURITY DEFINER) rather
// than a direct PATCH, because the anon API role is RLS-blocked from updating
// controller_lease directly — a raw PATCH silently affects 0 rows and leaves
// the lease valid, which would block every other node from ever acquiring it.
func (c *Client) ReleaseControllerLease(nodeID string) error {
	resp, err := c.requestWithRetry("POST", "/rpc/release_controller_lease",
		map[string]interface{}{"p_node_id": nodeID})
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// SetAssignmentStatus updates the status column of a single channel assignment.
// Used by the controller to reset a stale "recording" row back to "claimed"
// once the stream is confirmed offline.
//
// Plain anon PATCH is RLS-blocked on channel_assignments (only service_role can
// write via REST), so this goes through the reset_channel_status RPC
// (SECURITY DEFINER), which is the only path that actually persists.
func (c *Client) SetAssignmentStatus(username, site, status string) error {
	resp, err := c.requestWithRetry("POST", "/rpc/reset_channel_status", map[string]interface{}{
		"p_username": username,
		"p_site":     site,
		"p_status":   status,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// ResetStaleRecordingAssignments resets expired recording markers.
// An active recording continuously refreshes its heartbeat every 30-60s via
// MarkChannelRecording. Any row whose status is 'recording' and whose heartbeat
// is older than `before` (or is null) is no longer recording, so it is safely
// reset back to 'claimed'.
//
// protectedNodeIDs are NEVER reset. A stale channel-row heartbeat alone does
// not mean the recording ended: the refresh (assignment-sync's
// MarkChannelRecording) can lag behind a DB slowdown or a skipped cycle while
// the owner node is alive and still writing the file. Unpinning such a row
// would let the live-load rebalancer hand the channel to a second node
// mid-file (duplicate capture). The caller passes exactly the owners it treats
// as alive/protected (online or draining with a recent node heartbeat), which
// matches the grace window used everywhere else; a genuinely dead runner falls
// out of that set once its heartbeat ages past the grace, after which its
// leftover marker becomes movable again.
func (c *Client) ResetStaleRecordingAssignments(before time.Time, protectedNodeIDs []string) ([]ChannelAssignment, error) {
	path := fmt.Sprintf("/channel_assignments?status=eq.recording&or=(last_heartbeat.lt.%s,last_heartbeat.is.null)",
		url.QueryEscape(before.UTC().Format(time.RFC3339Nano)))
	if len(protectedNodeIDs) > 0 {
		path += "&assigned_node=not.in.(" + joinEscaped(protectedNodeIDs) + ")"
	}
	resp, err := c.requestWithRetry("PATCH", path, map[string]interface{}{"status": "claimed"})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	var reset []ChannelAssignment
	if err := json.NewDecoder(resp.Body).Decode(&reset); err != nil {
		return nil, fmt.Errorf("decode stale recording reset: %w", err)
	}
	return reset, nil
}

// SetSiteLiveness bulk-updates is_live for a single site. Only the explicitly
// listed usernames are touched (chunked in-list PATCHes), so channels that
// could not be evaluated this cycle (e.g. a Stripchat geo-ban) keep their
// previous flag. Recording channels are excluded from the "not live" set so a
// transient offline reading can never flip a live recording's flag.
func (c *Client) SetSiteLiveness(site string, liveUsernames, deadUsernames []string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	if len(liveUsernames) > 0 {
		if err := c.patchSiteLive(site, liveUsernames, true, now); err != nil {
			return err
		}
	}
	if len(deadUsernames) > 0 {
		if err := c.patchSiteLive(site, deadUsernames, false, now); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) patchSiteLive(site string, usernames []string, live bool, now string) error {
	const chunk = 200
	for start := 0; start < len(usernames); start += chunk {
		end := start + chunk
		if end > len(usernames) {
			end = len(usernames)
		}
		in := joinEscaped(usernames[start:end])
		filter := fmt.Sprintf("/channel_assignments?site=eq.%s&username=in.(%s)&is_live=eq.%v",
			url.QueryEscape(site), in, !live)
		if err := c.patch(filter, map[string]interface{}{
			"is_live":         live,
			"live_checked_at": now,
		}); err != nil {
			return fmt.Errorf("patch site liveness: %w", err)
		}
	}
	return nil
}

// ReassignChannel atomically moves a channel_assignments row from one node to
// another via the reassign_channel RPC (SELECT ... FOR UPDATE SKIP LOCKED), so
// even when several nodes race to migrate the same channel only one wins.
// ReassignChannel moves an already-assigned channel from one node to another
// via the reassign_channel RPC (SELECT ... FOR UPDATE SKIP LOCKED), so
// concurrent moves serialize safely. The RPC reports whether the row actually
// moved: its UPDATE is guarded on assigned_node = p_from_node AND status <>
// 'recording', so a stale snapshot (row already moved, or it flipped to
// 'recording' in between) matches zero rows and the RPC returns false instead
// of silently succeeding.
func (c *Client) ReassignChannel(username, site, fromNode, toNode string) (bool, error) {
	body := map[string]interface{}{
		"p_username":  username,
		"p_site":      site,
		"p_from_node": fromNode,
		"p_to_node":   toNode,
	}
	resp, err := c.requestWithRetry("POST", "/rpc/reassign_channel", body)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}
	// Tolerant decode: the RPC may return SETOF boolean ([true]), a scalar
	// boolean (true), or — against an older DB still exposing the void RPC —
	// an empty body. Empty/unknown shapes conservatively report "moved" so the
	// controller keeps its previous behavior until the migration lands.
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return true, nil // legacy void RPC: no payload
	}
	var arr []bool
	if err := json.Unmarshal(raw, &arr); err == nil {
		return len(arr) > 0 && arr[0], nil
	}
	var single bool
	if err := json.Unmarshal(raw, &single); err == nil {
		return single, nil
	}
	return true, nil // unexpected shape: conservative
}

// ============================================================================
// CHANNEL ASSIGNMENTS
// ============================================================================

// ChannelAssignment represents the assignment of a channel to a node.
type ChannelAssignment struct {
	Username      string `json:"username"`
	Site          string `json:"site"`
	AssignedNode  string `json:"assigned_node,omitempty"`
	Status        string `json:"status"`
	IsLive        bool   `json:"is_live"`
	LiveCheckedAt string `json:"live_checked_at,omitempty"`
	AssignedAt    string `json:"assigned_at,omitempty"`
	LastHeartbeat string `json:"last_heartbeat,omitempty"`
	// Config snapshot
	Framerate               int    `json:"framerate"`
	Resolution              int    `json:"resolution"`
	Pattern                 string `json:"pattern"`
	MaxDuration             int    `json:"max_duration"`
	MaxFilesize             int    `json:"max_filesize"`
	Compress                bool   `json:"compress"`
	MinDurationBeforeUpload int    `json:"min_duration_before_upload"`
	CreatedAt               string `json:"created_at,omitempty"`
	UpdatedAt               string `json:"updated_at,omitempty"`
}

// AssignmentStats holds summary statistics for fair-share calculation.
type AssignmentStats struct {
	TotalPoolChannels  int `json:"total_pool_channels"`
	TotalLiveChannels  int `json:"total_live_channels"`
	TotalUnassigned    int `json:"total_unassigned"`
	TotalAssignedNodes int `json:"total_assigned_nodes"`
	TotalAliveNodes    int `json:"total_alive_nodes"`
}

// claimRPC posts to a claim_* RPC with the standard (p_node_id, p_limit)
// payload and decodes the claimed rows. All claim RPCs use SELECT ... FOR
// UPDATE SKIP LOCKED so two nodes can never claim the same channel
// concurrently (no TOCTOU between a GET and a PATCH).
//
// Fair-share cap: the claim RPCs no longer trust p_limit as the real budget —
// they compute the node's remaining fair share server-side
// (share = ceil(pool / eligible_nodes), budget = min(p_limit, share - load))
// and a BEFORE UPDATE trigger (enforce_fair_share_claim) backstops EVERY
// writer, re-pointing a claim that would push a live-channel node past its
// share to the coldest eligible node. This is the data-layer guarantee that
// no node can ever claim all/most of the pool, regardless of which binary or
// external tool issues the claim. p_limit still bounds a single RPC call.
func (c *Client) claimRPC(rpcName, nodeID string, limit int) ([]ChannelAssignment, error) {
	body := map[string]interface{}{
		"p_node_id": nodeID,
		"p_limit":   limit,
	}

	resp, err := c.requestWithRetry("POST", "/rpc/"+rpcName, body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var claimed []ChannelAssignment
	if err := json.NewDecoder(resp.Body).Decode(&claimed); err != nil {
		return nil, fmt.Errorf("decode claimed: %w", err)
	}
	return claimed, nil
}

// ClaimChannels atomically claims up to `limit` unassigned channels for this
// node, regardless of is_live. Kept for compatibility; the claim cycle now
// prefers ClaimOfflineChannels/ClaimLiveChannels so offline budget claims can
// never sweep live channels. The server caps every claim at the node's
// remaining fair share (see claimRPC) — an oversized limit can never sweep
// the pool onto one node.
func (c *Client) ClaimChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	return c.claimRPC("claim_channels", nodeID, limit)
}

// ClaimOfflineChannels atomically claims up to `limit` unassigned OFFLINE
// channels (is_live=false) for this node via the claim_offline_channels RPC.
// Used by the claim cycle to fill a node's offline fair-share budget without
// accidentally absorbing live channels that should be spread across nodes.
func (c *Client) ClaimOfflineChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	return c.claimRPC("claim_offline_channels", nodeID, limit)
}

// ClaimLiveChannels atomically claims up to `limit` unassigned LIVE channels
// (is_live=true) for this node via the claim_live_channels RPC. The claim
// cycle uses it to claim live channels only up to the node's live fair share
// (ceil(totalLive / aliveNodes)), so live channels are spread across nodes
// instead of being swept wholesale by whichever node had offline budget room
// after a reclaim.
func (c *Client) ClaimLiveChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	return c.claimRPC("claim_live_channels", nodeID, limit)
}

// ClaimSpecificChannel atomically claims one specific channel for this node.
// Uses the PostgreSQL claim_specific_channel RPC (SELECT ... FOR UPDATE SKIP
// LOCKED) to prevent two nodes from claiming the same channel concurrently.
// Returns true if the channel was successfully claimed, false if it was
// already taken.
func (c *Client) ClaimSpecificChannel(username, site, nodeID string) (bool, error) {
	body := map[string]interface{}{
		"p_username": username,
		"p_site":     site,
		"p_node_id":  nodeID,
	}

	resp, err := c.requestWithRetry("POST", "/rpc/claim_specific_channel", body)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var claimed []ChannelAssignment
	if err := json.NewDecoder(resp.Body).Decode(&claimed); err != nil {
		return false, fmt.Errorf("decode claimed: %w", err)
	}
	if len(claimed) > 0 {
		return true, nil
	}

	// A few self-hosted PostgREST deployments retain an old RPC definition
	// after a schema-cache reload.  The service-role client used by recorder
	// nodes can safely make the identical, conditional REST update as a
	// compatibility fallback.  The filters preserve the atomic claim rule: a
	// row is changed only while it is still genuinely unassigned.
	return c.claimSpecificChannelFallback(username, site, nodeID)
}

func (c *Client) claimSpecificChannelFallback(username, site, nodeID string) (bool, error) {
	path := fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s&assigned_node=is.null&status=eq.unassigned",
		url.QueryEscape(username), url.QueryEscape(site))
	resp, err := c.requestWithRetry("PATCH", path, map[string]interface{}{
		"assigned_node": nodeID,
		"status":        "claimed",
	})
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	// PostgREST normally returns 204 for a PATCH without return=representation.
	// Re-read exactly one row: success is only reported if it now belongs to the
	// requested node, which avoids pretending a concurrently claimed row moved.
	var rows []ChannelAssignment
	if err := c.get(fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s&assigned_node=eq.%s&limit=1",
		url.QueryEscape(username), url.QueryEscape(site), url.QueryEscape(nodeID)), &rows); err != nil {
		return false, err
	}
	return len(rows) == 1, nil
}

// ReleaseNodeChannels releases all channels currently assigned to a node.
//
// Plain anon PATCH is RLS-blocked on channel_assignments, so the actual write
// goes through the reclaim_node_channels RPC (SECURITY DEFINER), which frees
// every channel owned by the node (defensively skipping status='recording' so
// an in-progress recording is never interrupted mid-write).
func (c *Client) ReleaseNodeChannels(nodeID string) error {
	resp, err := c.requestWithRetry("POST", "/rpc/reclaim_node_channels", map[string]interface{}{
		"p_node_id": nodeID,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// ReleaseNodeOfflineChannels releases a node's OFFLINE channels back to the
// pool with a single filter-based PATCH. Unlike ReleaseExcessOfflineChannels
// there is no username in-list, so the URL stays tiny and can never hit the
// ~8KB proxy limit (HTTP 414) no matter how many channels the node holds. Only
// is_live=false, non-recording channels match, so live broadcasts and
// in-progress recordings are never disturbed. excludeUsernames (e.g.
// user-paused channels) stay assigned to the node. Returns the number of
// channels released.
func (c *Client) ReleaseNodeOfflineChannels(nodeID string, excludeUsernames []string) (int, error) {
	filter := fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&is_live=eq.false&status=neq.recording&status=neq.unassigned",
		url.QueryEscape(nodeID))
	if len(excludeUsernames) > 0 {
		filter += "&username=not.in.(" + joinEscaped(excludeUsernames) + ")"
	}

	// Count what the PATCH will release (drives the caller's log message).
	var matches []ChannelAssignment
	if err := c.getAllPaginated(filter+"&select=username", &matches); err != nil {
		return 0, err
	}
	if len(matches) == 0 {
		return 0, nil
	}

	// Release them. The filter re-evaluates atomically inside the UPDATE, so a
	// channel that went live since the count is not swept.
	if err := c.patch(filter, map[string]interface{}{
		"assigned_node": nil,
		"status":        "unassigned",
	}); err != nil {
		return 0, err
	}
	return len(matches), nil
}

// releaseChunked PATCHes the given usernames back to unassigned for this node
// in small batches (releaseBatchSize per request). The release is idempotent —
// a row already released simply matches nothing — so on a failed batch the
// caller receives the rows released so far plus the error, and the next claim
// cycle picks up the remainder. Without chunking, one giant username=in.(...)
// filter exceeds the ~8KB proxy URL limit (HTTP 414) and the release fails
// wholesale, which is what wedged the fleet's rebalancer.
func (c *Client) releaseChunked(nodeID string, usernames []string) ([]ChannelAssignment, error) {
	var released []ChannelAssignment
	totalBatches := (len(usernames) + releaseBatchSize - 1) / releaseBatchSize
	for start := 0; start < len(usernames); start += releaseBatchSize {
		end := start + releaseBatchSize
		if end > len(usernames) {
			end = len(usernames)
		}
		batch := usernames[start:end]
		batchNo := start/releaseBatchSize + 1

		resp, err := c.requestWithRetry("PATCH",
			fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&username=in.(%s)",
				url.QueryEscape(nodeID), joinEscaped(batch)),
			map[string]interface{}{
				"assigned_node": nil,
				"status":        "unassigned",
			})
		if err != nil {
			return released, fmt.Errorf("release batch %d/%d: %w", batchNo, totalBatches, err)
		}
		if resp.StatusCode >= 400 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return released, fmt.Errorf("release batch %d/%d: HTTP %d: %s", batchNo, totalBatches, resp.StatusCode, string(bodyBytes))
		}
		var batchReleased []ChannelAssignment
		decodeErr := json.NewDecoder(resp.Body).Decode(&batchReleased)
		resp.Body.Close()
		if decodeErr != nil {
			return released, fmt.Errorf("decode released batch %d/%d: %w", batchNo, totalBatches, decodeErr)
		}
		released = append(released, batchReleased...)
	}
	return released, nil
}

// ReleaseExcessChannels releases up to `limit` channels from this node back to unassigned.
// Uses a two-step approach (GET usernames, then PATCH by username) because
// PostgREST PATCH ignores the `limit` parameter.
func (c *Client) ReleaseExcessChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	// Step 1: GET the usernames we want to release.
	// Prioritise releasing offline channels first (is_live=false), then online
	// ones.  Within each group we pick alphabetically to be deterministic.
	var offline, online []ChannelAssignment

	err := c.get(
		fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned&is_live=eq.false&select=username,site&order=username.asc&limit=%d",
			url.QueryEscape(nodeID), limit), &offline)
	if err != nil {
		return nil, err
	}

	remaining := limit - len(offline)
	if remaining > 0 {
		err = c.get(
			fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned&is_live=eq.true&select=username,site&order=username.asc&limit=%d",
				url.QueryEscape(nodeID), remaining), &online)
		if err != nil {
			return nil, err
		}
	}

	target := append(offline, online...)
	if len(target) == 0 {
		return nil, nil
	}

	// Step 2: PATCH only those specific channels, in small batches so the
	// PostgREST filter URL can never blow past the ~8KB proxy limit (HTTP 414).
	usernames := make([]string, len(target))
	for i, ca := range target {
		usernames[i] = ca.Username
	}
	return c.releaseChunked(nodeID, usernames)
}

// ReleaseExcessOfflineChannels releases up to `limit` OFFLINE channels from this
// node back to unassigned. Unlike ReleaseExcessChannels it NEVER releases a
// live or recording channel, so a node's in-progress recordings are left alone
// during fair-share rebalancing. Channels are selected offline-first, then
// alphabetically, to be deterministic.
func (c *Client) ReleaseExcessOfflineChannels(nodeID string, limit int) ([]ChannelAssignment, error) {
	if limit <= 0 {
		return nil, nil
	}

	// Offline channels only, and exclude any still marked 'recording' (a node may
	// briefly lag the liveness flag, so this protects a channel that is actually
	// being recorded from being interrupted).
	var offline []ChannelAssignment
	err := c.get(
		fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned&status=neq.recording&is_live=eq.false&select=username,site&order=username.asc&limit=%d",
			url.QueryEscape(nodeID), limit), &offline)
	if err != nil {
		return nil, err
	}
	if len(offline) == 0 {
		return nil, nil
	}

	usernames := make([]string, len(offline))
	for i, ca := range offline {
		usernames[i] = ca.Username
	}
	return c.releaseChunked(nodeID, usernames)
}

// ReleaseChannel releases a single channel back to the pool.
func (c *Client) ReleaseChannel(username, site string) error {
	return c.patch(fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s", url.QueryEscape(username), url.QueryEscape(site)),
		map[string]interface{}{
			"assigned_node": nil,
			"status":        "unassigned",
		})
}

// MarkChannelRecording marks a channel as actively recording on its node and
// bumps its recording heartbeat. Called by the liveness loop for this node's
// live channels so "live+recording" is authoritative in the DB.
//
// Plain anon PATCH is RLS-blocked on channel_assignments, so this goes through
// the mark_channel_recording RPC (SECURITY DEFINER). This status='recording'
// mark is what makes the controller's rebalancer refuse to move the row (via the
// reassign_channel RPC's status guard and the protect_recording_assignment
// trigger), so an in-progress recording is never interrupted.
func (c *Client) MarkChannelRecording(username, site string) error {
	resp, err := c.requestWithRetry("POST", "/rpc/mark_channel_recording", map[string]interface{}{
		"p_username": username,
		"p_site":     site,
	})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// TouchChannelRecordings refreshes last_recorded_at for channels this node
// OWNS but is not actively recording (status stays 'claimed').
//
// mark_channel_recording only fires while a channel is actually recording, so
// last_recorded_at on a claimed channel freezes at the end of its last session
// and looks stale forever — which hides genuinely dead owners behind noise and
// makes "how long since this channel last captured" unreadable. This is the
// owned-but-idle counterpart: it proves the row's owner is alive and the
// channel is healthy-but-idle, without flipping status back to 'recording' or
// touching the recording pin (last_heartbeat).
//
// Chunks of (username, site) pairs go through the touch_channel_recordings RPC
// (SECURITY DEFINER — plain anon PATCH is RLS-blocked on channel_assignments).
// The RPC filters server-side on assigned_node=nodeID AND status='claimed', so
// a row that was released or reassigned between the caller's read and this
// call is left untouched (the update simply matches zero rows).
//
// Degradation: on PGRST202 (RPC not yet in the PostgREST schema cache, e.g.
// migration not applied on some projects) the error is swallowed — staleness
// visibility is best-effort and must never break the assignment-sync cycle.
func (c *Client) TouchChannelRecordings(nodeID string, channels []ChannelAssignment) error {
	const chunk = 200
	for start := 0; start < len(channels); start += chunk {
		end := start + chunk
		if end > len(channels) {
			end = len(channels)
		}
		batch := channels[start:end]
		pairs := make([]map[string]string, len(batch))
		for i, ca := range batch {
			pairs[i] = map[string]string{"username": ca.Username, "site": ca.Site}
		}
		body := map[string]interface{}{
			"p_node_id":  nodeID,
			"p_channels": pairs,
		}
		resp, err := c.requestWithRetry("POST", "/rpc/touch_channel_recordings", body)
		if err != nil {
			if strings.Contains(err.Error(), "PGRST202") {
				return nil // RPC not deployed on this project — skip silently
			}
			return fmt.Errorf("touch channel recordings: %w", err)
		}
		resp.Body.Close()
	}
	return nil
}

// ReassertAssignmentNode forces a channel's owning node back to nodeID, but
// only while the row is still owned by expectedCurrentNode. Used by the sync
// loop to keep a channel pinned to the node that is actively recording it, so
// a reassignment (coordinator rebalance or an external autopilot, possibly via
// a raw DB UPDATE) cannot fragment an in-progress recording by moving it to —
// and spawning a duplicate on — another node.
//
// The conditional filter is what makes this safe under races: if the row moved
// again between the caller's read and this PATCH (e.g. a user pause released it
// to NULL), the PATCH matches zero rows and becomes a no-op instead of
// resurrecting a released/paused channel into the orphaned "assigned_node set
// but status unassigned" state that RepairOrphanedAssignments exists to fix.
// It updates by (username, site) and is a no-op if the row does not exist.
func (c *Client) ReassertAssignmentNode(username, site, nodeID, expectedCurrentNode string) error {
	return c.patch(
		fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s&assigned_node=eq.%s",
			url.QueryEscape(username), url.QueryEscape(site), url.QueryEscape(expectedCurrentNode)),
		map[string]interface{}{
			"assigned_node": nodeID,
		})
}

// RepairOrphanedAssignments fixes rows where assigned_node is set but
// status is still 'unassigned'. This can happen if a claim was partially
// rolled back (assigned_node written, status not updated) or if a
// release set status=unassigned without nulling assigned_node. These rows
// are invisible to both ClaimChannels (which requires assigned_node IS NULL)
// and ReleaseExcessChannels (which requires status != unassigned), causing
// a permanent deadlock.
//
// Returns the number of rows repaired.
func (c *Client) RepairOrphanedAssignments() (int, error) {
	// Step 1: count the broken rows
	var orphaned []ChannelAssignment
	err := c.getAllPaginated("/channel_assignments?assigned_node=not.is.null&status=eq.unassigned&select=username", &orphaned)
	if err != nil {
		return 0, err
	}
	if len(orphaned) == 0 {
		return 0, nil
	}

	// Step 2: null out assigned_node on all broken rows
	err = c.patch("/channel_assignments?assigned_node=not.is.null&status=eq.unassigned",
		map[string]interface{}{
			"assigned_node": nil,
		})
	if err != nil {
		return 0, err
	}

	return len(orphaned), nil
}

// assignmentPruneBatchSize caps how many usernames go into one DELETE filter
// URL.  Same ~8KB proxy limit as releaseBatchSize: 40 usernames stay well under
// 1KB even with long performer names, so a prune can never 414.
const assignmentPruneBatchSize = 40

// assignmentPruneGrace is how long a channel_assignments row is immune from
// PruneOrphanedAssignments after it was created.  A brand-new pooled channel
// gets its channels-catalogue row from EnsureChannelExists (add paths) or from
// the on-demand profile scrape once a node records it — the latter can take
// several controller cycles — so an explicitly invoked prune must never treat
// "not in channels" as "orphan" for a row that has not had a chance to be
// recorded.
const assignmentPruneGrace = 30 * time.Minute

// channelUsernameRow is the minimal projection of the channels table used to
// decide whether an assignment still has a channel behind it.
type channelUsernameRow struct {
	Username string `json:"username"`
}

// GetChannelUsernames returns the set of usernames present in the channels
// table.  Paginated, so the answer stays correct past the server's max_rows cap
// (Supabase defaults to 1000) rather than silently truncating.
func (c *Client) GetChannelUsernames() (map[string]bool, error) {
	var rows []channelUsernameRow
	if err := c.getAllPaginated("/channels?select=username", &rows); err != nil {
		return nil, err
	}
	usernames := make(map[string]bool, len(rows))
	for _, r := range rows {
		if r.Username != "" {
			usernames[r.Username] = true
		}
	}
	return usernames, nil
}

// PruneOrphanedAssignments deletes channel_assignments rows whose username no
// longer exists in the channels table.  Nothing else removes them: when a
// channel is deleted from the pool its assignment row survives, so the node
// holding it keeps polling a channel that can never come back and logs
// "channel not found (deleted/renamed), try again in 1 min(s)" forever.  The
// live fleet carried 165 such rows (the oldest with a heartbeat 7 weeks old).
//
// NOT CALLED AUTOMATICALLY ANY MORE.  This used to run every controller cycle,
// which silently deleted freshly added channels: a brand-new pool entry has no
// channels row until a node records it, so the next cycle pruned it before any
// node could claim it ("we add channels but the pool never grows").  The
// operator's rule is now "no channel is ever deleted automatically — deletions
// are done by hand from the node dashboards", so the controller no longer calls
// this.  It is kept (and still tested) as an explicitly-invoked one-shot
// cleanup for a fleet that has accumulated rows for channels deleted by hand.
//
// Rows with status='recording' are deliberately spared — a live recording must
// never be cut off by a cleanup pass (the same defensive rule
// reclaim_node_channels applies), and a genuinely stuck recording is reaped by
// the recording watchdog instead.  For the same reason the DELETE filter carries
// the status guard rather than relying on the in-memory snapshot alone, so a row
// that flips to 'recording' between the read and the delete is not removed.
//
// Rows created within assignmentPruneGrace are also spared, so an explicitly
// invoked prune can never delete a pool entry that has not yet had a chance to
// be recorded (and therefore registered in the catalogue).
//
// Returns the number of assignment rows removed (counted from the snapshot that
// matched the delete filter; a row that changed state in between is skipped by
// the server-side status guard and therefore not counted).
func (c *Client) PruneOrphanedAssignments() (int, error) {
	channels, err := c.GetChannelUsernames()
	if err != nil {
		return 0, fmt.Errorf("load channels: %w", err)
	}

	var assignments []ChannelAssignment
	if err := c.getAllPaginated("/channel_assignments?select=username,site,status,created_at", &assignments); err != nil {
		return 0, fmt.Errorf("load assignments: %w", err)
	}

	graceCutoff := time.Now().Add(-assignmentPruneGrace)

	// orphanRows counts the rows the prune targets, keyed by username so a
	// username present on both sites is removed once but counted per row.
	orphanRows := map[string]int{}
	for _, a := range assignments {
		if a.Username == "" || channels[a.Username] || a.Status == "recording" {
			continue
		}
		// Spare anything added recently: the channels-catalogue row for a pooled
		// channel only appears once a node starts recording it, which can take
		// longer than one controller cycle.
		if t, err := time.Parse(time.RFC3339Nano, a.CreatedAt); err == nil && t.After(graceCutoff) {
			continue
		}
		orphanRows[a.Username]++
	}
	if len(orphanRows) == 0 {
		return 0, nil
	}

	names := make([]string, 0, len(orphanRows))
	for name := range orphanRows {
		names = append(names, name)
	}
	sort.Strings(names) // deterministic batching keeps the request count stable

	removed := 0
	for start := 0; start < len(names); start += assignmentPruneBatchSize {
		end := start + assignmentPruneBatchSize
		if end > len(names) {
			end = len(names)
		}
		chunk := names[start:end]
		if err := c.delete(fmt.Sprintf(
			"/channel_assignments?username=in.(%s)&status=neq.recording",
			joinEscaped(chunk))); err != nil {
			return removed, fmt.Errorf("delete orphaned assignments: %w", err)
		}
		for _, name := range chunk {
			removed += orphanRows[name]
		}
	}
	return removed, nil
}

// DeleteAssignment removes a channel assignment entirely from the pool.
func (c *Client) DeleteAssignment(username, site string) error {
	return c.delete(fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s", url.QueryEscape(username), url.QueryEscape(site)))
}

// GetNodeAssignments returns all channel assignments for a given node.
func (c *Client) GetNodeAssignments(nodeID string) ([]ChannelAssignment, error) {
	var assignments []ChannelAssignment
	err := c.get(fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&order=username.asc", url.QueryEscape(nodeID)), &assignments)
	return assignments, err
}

// GetAssignment returns the assignment for a specific channel.
func (c *Client) GetAssignment(username, site string) (*ChannelAssignment, error) {
	var assignments []ChannelAssignment
	err := c.get(fmt.Sprintf("/channel_assignments?username=eq.%s&site=eq.%s&limit=1",
		url.QueryEscape(username), url.QueryEscape(site)), &assignments)
	if err != nil {
		return nil, err
	}
	if len(assignments) == 0 {
		return nil, nil
	}
	return &assignments[0], nil
}

// AssignmentExistsFold reports whether the pool already holds this channel under
// any casing.
//
// channel_assignments is keyed on (username, site) case-SENSITIVELY, but cam-site
// usernames are case-insensitive — chaturbate.com/LunaLeonCoupleWife and
// /lunaleoncouplewife are the same room — so an exact lookup (GetAssignment)
// misses a differently-cased duplicate and the add then creates a second row,
// i.e. a second node recording the same room.  The catalogue really does carry
// such a pair today (LunaLeonCoupleWife / lunaleoncouplewife), so this is not
// hypothetical.
//
// The ilike filter can over-match, because '_' is a SQL single-character
// wildcard and performer names use it, so the rows are matched again in Go with
// EqualFold and only an actual (case-insensitive) name match counts.
func (c *Client) AssignmentExistsFold(username, site string) (bool, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return false, nil
	}

	path := fmt.Sprintf("/channel_assignments?select=username&username=ilike.%s",
		url.QueryEscape(username))
	if site != "" {
		path += fmt.Sprintf("&site=eq.%s", url.QueryEscape(site))
	}

	var rows []ChannelAssignment
	if err := c.get(path, &rows); err != nil {
		return false, err
	}
	for _, r := range rows {
		if strings.EqualFold(r.Username, username) {
			return true, nil
		}
	}
	return false, nil
}

// GetAssignmentsByStatus returns all assignments with a given status.
func (c *Client) GetAssignmentsByStatus(status string) ([]ChannelAssignment, error) {
	var assignments []ChannelAssignment
	err := c.getAllPaginated(fmt.Sprintf("/channel_assignments?status=eq.%s&order=username.asc", url.QueryEscape(status)), &assignments)
	return assignments, err
}

// GetAllAssignments returns all channel assignments.
func (c *Client) GetAllAssignments() ([]ChannelAssignment, error) {
	var assignments []ChannelAssignment
	err := c.getAllPaginated("/channel_assignments?order=username.asc", &assignments)
	return assignments, err
}

// countRows returns the exact number of rows matching the given PostgREST
// filter path, using a HEAD request with Prefer: count=exact (the total is
// returned in the Content-Range header).  This avoids fetching up to 50k rows
// just to count them and never silently truncates at the fetch limit.
// Transient failures (5xx, 429, 408, network) are retried so a schema-cache
// rebuild or blip doesn't abort the caller's cycle.
func (c *Client) countRows(path string) (int, error) {
	const maxRetries = 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		n, retryable, err := c.countRowsOnce(path)
		if err == nil {
			return n, nil
		}
		if !retryable || attempt >= maxRetries-1 {
			return 0, err
		}
		time.Sleep(retryBackoff(attempt))
	}
	return 0, fmt.Errorf("count failed after %d attempts", maxRetries)
}

func (c *Client) countRowsOnce(path string) (int, bool, error) {
	req, err := http.NewRequest(http.MethodHead, c.URL+"/rest/v1"+path, nil)
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("apikey", c.APIKey)
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	req.Header.Set("Prefer", "count=exact")

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, true, err // transport error — retryable
	}
	defer resp.Body.Close()

	if resp.StatusCode == 408 || resp.StatusCode == 429 || resp.StatusCode >= 500 {
		return 0, true, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 {
		return 0, false, fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// Content-Range: <start>-<end>/<total>  (total is "*" without count=exact)
	cr := resp.Header.Get("Content-Range")
	if cr == "" {
		return 0, false, fmt.Errorf("missing Content-Range header")
	}
	idx := strings.LastIndex(cr, "/")
	if idx < 0 {
		return 0, false, fmt.Errorf("malformed Content-Range: %s", cr)
	}
	total := strings.TrimSpace(cr[idx+1:])
	if total == "*" {
		return 0, false, fmt.Errorf("count not provided")
	}
	n, err := strconv.Atoi(total)
	if err != nil {
		return 0, false, fmt.Errorf("parse Content-Range total %q: %w", total, err)
	}
	return n, false, nil
}

// GetAssignmentStats returns total live channels and total alive nodes for fair-share calculation.
func (c *Client) GetAssignmentStats() (*AssignmentStats, error) {
	stats := &AssignmentStats{}
	var err error

	// Exact counts via HEAD (no 50k-row fetches, no silent truncation).
	if stats.TotalPoolChannels, err = c.countRows("/channel_assignments"); err != nil {
		return nil, err
	}
	if stats.TotalLiveChannels, err = c.countRows("/channel_assignments?is_live=eq.true"); err != nil {
		return nil, err
	}
	if stats.TotalUnassigned, err = c.countRows("/channel_assignments?status=eq.unassigned"); err != nil {
		return nil, err
	}

	// Distinct assigned nodes can't be counted via REST, so fetch and dedupe.
	var assigned []ChannelAssignment
	if err := c.getAllPaginated("/channel_assignments?assigned_node=not.is.null&select=assigned_node", &assigned); err != nil {
		return nil, err
	}
	assignedNodes := make(map[string]bool)
	for _, a := range assigned {
		assignedNodes[a.AssignedNode] = true
	}
	stats.TotalAssignedNodes = len(assignedNodes)

	aliveNodes, err := c.GetAliveNodes()
	if err != nil {
		return nil, err
	}
	stats.TotalAliveNodes = len(aliveNodes)

	return stats, nil
}

// CountMyAssignments returns the number of channels assigned to a node.
// Uses an exact HEAD count rather than loading all rows.
func (c *Client) CountMyAssignments(nodeID string) (int, error) {
	return c.countRows(fmt.Sprintf("/channel_assignments?assigned_node=eq.%s&status=neq.unassigned",
		url.QueryEscape(nodeID)))
}

// compositeOrFilter builds a PostgREST or=(and(a.eq.x,b.eq.y),...) filter for a
// list of composite-key (username, site) pairs.  channel_assignments has a
// composite primary key (username, site), so filters must address both columns
// or they would wrongly touch the same username on the other site.
func compositeOrFilter(pairs [][2]string) string {
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = fmt.Sprintf("and(username.eq.%s,site.eq.%s)",
			url.QueryEscape(p[0]), url.QueryEscape(p[1]))
	}
	return "or=(" + strings.Join(parts, ",") + ")"
}

// SetChannelsLive bulk-updates is_live=true for the given (username, site)
// pairs.  Composite filters ensure a same-named channel on the other site is
// never toggled.  The pair list is chunked because a single or=(...) filter
// listing every live pair can exceed the ~8KB proxy URL limit (HTTP 414) — the
// same failure mode that left is_live flags stale fleet-wide.
func (c *Client) SetChannelsLive(pairs [][2]string) error {
	if len(pairs) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	return c.setChannelsLiveChunked(pairs, now)
}

func (c *Client) setChannelsLiveChunked(pairs [][2]string, now string) error {
	for start := 0; start < len(pairs); start += livenessBatchSize {
		end := start + livenessBatchSize
		if end > len(pairs) {
			end = len(pairs)
		}
		chunk := pairs[start:end]
		if err := c.patch(
			fmt.Sprintf("/channel_assignments?%s&is_live=eq.false", compositeOrFilter(chunk)),
			map[string]interface{}{
				"is_live":         true,
				"live_checked_at": now,
			}); err != nil {
			return fmt.Errorf("set live batch %d: %w", start/livenessBatchSize+1, err)
		}
	}
	return nil
}

// SetChannelsNotLive bulk-updates is_live=false for every channel EXCEPT the
// given (username, site) pairs.  An empty pair list marks the whole pool
// not-live.
//
// The old implementation built a single PostgREST not.or(...) filter listing
// every live pair — with hundreds of live channels that URL exceeds the ~8KB
// proxy limit (HTTP 414), leaving stale is_live=true rows that make the whole
// pool look live and stall claiming.  The exclusion filter cannot be chunked
// safely (each chunk would clear the other chunks' live flags), so we invert
// it: clear the entire pool with a tiny filter first, then re-mark the live
// pairs in small chunks.  The transient all-not-live window is harmless —
// is_live only biases fair-share claiming (claim RPCs require assigned_node IS
// NULL, so assigned channels are never disturbed) and the liveness loop
// re-runs every cycle.
//
// The clear-all excludes status='recording' rows.  A channel that is actively
// recording IS live by construction, but step 2 only re-marks the pairs the
// probe confirmed live in THIS cycle — so a recording channel whose probe
// failed or returned unknown was left with status='recording' + is_live=false,
// contradicting what the node was actually doing.  Anything keying off is_live
// (ReleaseNodeOfflineChannels, the fair-share claim RPCs) then treats a live
// recording as an offline idle row.  Recording rows keep whatever is_live they
// already had; SetSiteLiveness already applies the same exclusion via
// filterNonRecording.
func (c *Client) SetChannelsNotLive(pairs [][2]string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	// Step 1: clear every non-recording live flag — a single tiny PATCH.
	if err := c.patch("/channel_assignments?is_live=eq.true&status=neq.recording",
		map[string]interface{}{
			"is_live":         false,
			"live_checked_at": now,
		}); err != nil {
		return err
	}
	// Step 2: re-mark the still-live pairs in chunks.
	return c.setChannelsLiveChunked(pairs, now)
}

// ReclaimChannels sets assigned_node=NULL for all channels belonging to a dead node.
// Returns the number of channels reclaimed.
//
// Plain anon PATCH is RLS-blocked on channel_assignments, so the write goes
// through the reclaim_node_channels RPC (SECURITY DEFINER), which frees every
// channel owned by the node (defensively skipping status='recording').
func (c *Client) ReclaimChannels(deadNodeID string) (int, error) {
	resp, err := c.requestWithRetry("POST", "/rpc/reclaim_node_channels", map[string]interface{}{
		"p_node_id": deadNodeID,
	})
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	var rows []struct {
		Reclaimed int `json:"reclaimed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return 0, fmt.Errorf("decode reclaim: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return rows[0].Reclaimed, nil
}

// BulkInsertAssignments creates channel_assignments rows for channels that don't have one yet.
func (c *Client) BulkInsertAssignments(assignments []ChannelAssignment) error {
	if len(assignments) == 0 {
		return nil
	}
	resp, err := c.requestWithRetry("POST", "/channel_assignments?on_conflict=username,site", assignments)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// ============================================================================
// CHANNEL POOL (shared app_settings key)
// ============================================================================

// PoolKey returns the app_settings key for the shared channel pool.
func PoolKey() string {
	return "channel_pool"
}

// LoadPoolFromDB reads the shared channel pool from app_settings.
func (c *Client) LoadPoolFromDB() ([]byte, error) {
	var settings []AppSetting
	err := c.get(fmt.Sprintf("/app_settings?key=eq.%s&limit=1", PoolKey()), &settings)
	if err != nil {
		return nil, err
	}
	if len(settings) == 0 {
		return nil, nil
	}
	return settings[0].Value, nil
}

// SavePoolToDB writes the shared channel pool to app_settings.
func (c *Client) SavePoolToDB(data []byte) error {
	return c.SaveSetting(PoolKey(), json.RawMessage(data))
}

// GetAllSettingKeys returns all app_settings keys matching a LIKE pattern.
// The prefix should be like "channels_" to get all instance-scoped keys.
func (c *Client) GetAllSettingKeys(likePattern string) ([]string, error) {
	// Supabase REST doesn't support LIKE directly, so we fetch all keys
	// and filter client-side. For typical deployments this is < 100 keys.
	var settings []AppSetting
	err := c.getAllPaginated("/app_settings?select=key", &settings)
	if err != nil {
		return nil, err
	}
	var matches []string
	for _, s := range settings {
		if strings.HasPrefix(s.Key, likePattern) {
			matches = append(matches, s.Key)
		}
	}
	return matches, nil
}

// joinEscaped joins strings with Supabase-compatible CSV escaping.
func joinEscaped(items []string) string {
	escaped := make([]string, len(items))
	for i, item := range items {
		escaped[i] = url.QueryEscape(item)
	}
	return strings.Join(escaped, ",")
}

// ============================================================================
// HEALTH CHECK
// ============================================================================

// HealthCheck verifies the database connection
func (c *Client) HealthCheck() error {
	resp, err := c.request("GET", "/app_settings?key=eq.__healthcheck__&select=key&limit=1", nil)
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
