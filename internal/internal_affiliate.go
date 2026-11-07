package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
)

// AffiliateModel represents a single model from the affiliate onlinerooms API.
// Endpoint: GET {domain}/affiliates/api/onlinerooms/?format=json&wm={wm}
//
// The endpoint is served by chaturbate.com AND by the cb.xxx domain this
// deployment uses (both are the same platform), so the caller passes the
// configured base domain (server.Config.Domain, default "https://www.cb.xxx/").
type AffiliateModel struct {
	Username      string `json:"username"`
	DisplayName   string `json:"display_name"`
	Gender        string `json:"gender"`
	Age           int    `json:"age"`
	NumUsers      int    `json:"num_users"`
	CurrentShow   string `json:"current_show"`
	ImageURL      string `json:"image_url"`
	ChatRoomURL   string `json:"chat_room_url"`
	RoomSubject   string `json:"room_subject"`
	IsHD          bool   `json:"is_hd"`
	IsNew         bool   `json:"is_new"`
	SecondsOnline int    `json:"seconds_online"`
	Tags          string `json:"tags"`
	Countries     string `json:"countries"`
}

// AffiliateAPIResult caches affiliate API results with a TTL.
type AffiliateAPIResult struct {
	mu        sync.RWMutex
	models    map[string]AffiliateModel
	fetchedAt time.Time
	ttl       time.Duration
}

// affiliateUA matches the TLS/UA fingerprint that mints cf_clearance in
// cookie_grabber.py (Chrome 146). Cloudflare binds the clearance to that
// fingerprint, so the same UA must be presented when reusing the cookie.
const affiliateUA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"

var (
	affiliateCache  = &AffiliateAPIResult{ttl: 30 * time.Second}
	affiliateClient = &http.Client{
		Timeout:   20 * time.Second,
		Transport: sharedTransport(),
	}
	defaultAffiliateBase = "https://www.cb.xxx/"
)

// FetchAffiliateOnlineModels fetches all currently online models from the
// affiliate onlinerooms API on the given base domain (e.g. "https://www.cb.xxx/").
// Results are cached for ttl duration; a fresh cache returns without a network
// call, and a stale cache is served on error so a transient 5xx never marks
// every model offline. Returns a map keyed by lowercased username.
func FetchAffiliateOnlineModels(ctx context.Context, wmCode, baseURL string) (map[string]AffiliateModel, error) {
	if wmCode == "" {
		return nil, fmt.Errorf("affiliate WM code is empty")
	}
	if baseURL == "" {
		baseURL = defaultAffiliateBase
	}

	affiliateCache.mu.RLock()
	cached := affiliateCache.models
	cachedTime := affiliateCache.fetchedAt
	affiliateCache.mu.RUnlock()

	if cached != nil && time.Since(cachedTime) < affiliateCache.ttl {
		return cached, nil
	}

	affiliateCache.mu.Lock()
	defer affiliateCache.mu.Unlock()

	// Double-check after acquiring write lock
	if affiliateCache.models != nil && time.Since(affiliateCache.fetchedAt) < affiliateCache.ttl {
		return affiliateCache.models, nil
	}

	models, err := fetchAffiliateAPI(ctx, wmCode, baseURL)
	if err != nil {
		// Return stale cache on error if we have it
		if affiliateCache.models != nil {
			return affiliateCache.models, nil
		}
		return nil, err
	}

	affiliateCache.models = models
	affiliateCache.fetchedAt = time.Now()
	return models, nil
}

func fetchAffiliateAPI(ctx context.Context, wmCode, baseURL string) (map[string]AffiliateModel, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	apiURL := fmt.Sprintf("%s/affiliates/api/onlinerooms/?format=json&wm=%s", baseURL, wmCode)

	req, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("affiliate: create request: %w", err)
	}
	// Present the node's minted cf_clearance (IP+TLS-bound) and a matching UA so
	// the request clears Cloudflare on datacenter IPs (GitHub runners). Without
	// this the bare client is challenged and the whole bulk liveness check fails.
	req.Header.Set("User-Agent", affiliateUA)
	req.Header.Set("Accept", "application/json")
	if server.Config != nil {
		if server.Config.Cookies != "" {
			for name, value := range ParseCookies(server.Config.Cookies) {
				req.AddCookie(&http.Cookie{Name: name, Value: value})
			}
		} else if server.Config.CfClearance != "" {
			req.AddCookie(&http.Cookie{Name: "cf_clearance", Value: server.Config.CfClearance})
		}
	}

	resp, err := affiliateClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("affiliate: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("affiliate: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("affiliate: read body: %w", err)
	}

	var models []AffiliateModel
	if err := json.Unmarshal(body, &models); err != nil {
		return nil, fmt.Errorf("affiliate: parse response: %w", err)
	}

	result := make(map[string]AffiliateModel, len(models))
	for _, m := range models {
		result[strings.ToLower(m.Username)] = m
	}

	return result, nil
}

// CheckAffiliateLive checks a single username against the affiliate API.
// Returns (isLive bool, currentShow string, error). The affiliate list is
// authoritative for offline (a model not in the online list is offline);
// when present, currentShow tells us the type of broadcast.
func CheckAffiliateLive(ctx context.Context, wmCode, baseURL, username string) (bool, string, error) {
	models, err := FetchAffiliateOnlineModels(ctx, wmCode, baseURL)
	if err != nil {
		return false, "", err
	}

	model, found := models[strings.ToLower(username)]
	if !found {
		return false, "offline", nil
	}

	isLive := model.CurrentShow != "away" && model.CurrentShow != "offline"
	return isLive, model.CurrentShow, nil
}

// InvalidateAffiliateCache forces a re-fetch on the next call.
func InvalidateAffiliateCache() {
	affiliateCache.mu.Lock()
	defer affiliateCache.mu.Unlock()
	affiliateCache.models = nil
}
