package uploader

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const imgbbAPIURLDefault = "https://api.imgbb.com/1/upload"

// imgbbAPIURL is a var (vidmolyAPIBase convention) so tests can point the
// uploader at an httptest server.
var imgbbAPIURL = imgbbAPIURLDefault

// imgbbGlobal throttling: ImgBB's free tier rate-limits by IP address, not
// by API key.  All keys from the same server share one rate-limit bucket.
// The exact limit is undocumented but empirically ~50–100 uploads/h per IP;
// exceeding it returns HTTP 400 {"error":{"message":"Rate limit reached.","code":100}}.
// During a host outage or mass backfill every preview falls back to ImgBB;
// an unthrottled burst exhausts the bucket in seconds and all keys fail
// simultaneously.  Spacing calls process-wide keeps the throughput within
// quota.
var (
	imgbbMu         sync.Mutex
	imgbbLastUpload time.Time
	// imgbbBackoffUntil records when a rate-limit error was last seen.
	// While active, throttleImgBB uses the longer imgbbCooldownInterval
	// instead of imgbbMinInterval to give the IP-level bucket time to
	// refill.
	imgbbBackoffUntil time.Time
)

// imgbbMinInterval is the normal minimum spacing between ImgBB API calls.
// ~50–100 uploads/h per IP → 36–72 s between calls; 12 s gives ~300/h
// ceiling which stays well under quota during normal (non-backfill) usage.
// (vars, not consts: tests shrink them to keep the suite fast.)
var imgbbMinInterval = 12 * time.Second

// imgbbCooldownInterval is the spacing used after a rate-limit error.
// It is intentionally longer than imgbbMinInterval to let the IP-level
// bucket recover before resuming normal pacing.
var imgbbCooldownInterval = 72 * time.Second

// imgbbKeyCooldownDuration is how long ONE key stays out of rotation after a
// rate-limit hit.  The bucket refills on the order of an hour (see
// imgbbMinInterval), so 24 cooldown intervals (~29 min at the production
// spacing) keeps a spent key out until the bucket has realistically recovered
// while still letting it back in the same session.
var imgbbKeyCooldownDuration = 24 * imgbbCooldownInterval

// imgbbKeyCooldown tracks per-key cooldown expiries.  A rate-limited key is
// skipped by every uploader until its expiry instead of being re-tried on the
// next call (the pre-existing global throttle only changed SPACING; the spent
// key was still selected immediately).  Auth failures do NOT enter this map:
// a bad key must fail fast and visibly, not be silently skipped.
var imgbbKeyCooldown = struct {
	mu     sync.Mutex
	expiry map[string]time.Time
}{expiry: make(map[string]time.Time)}

// coolImgBBKey puts a key into per-key cooldown for imgbbKeyCooldownDuration.
func coolImgBBKey(key string) {
	if key == "" {
		return
	}
	imgbbKeyCooldown.mu.Lock()
	defer imgbbKeyCooldown.mu.Unlock()
	imgbbKeyCooldown.expiry[key] = time.Now().Add(imgbbKeyCooldownDuration)
}

// imgbbKeyCooled reports whether key is still inside its cooldown window.
// An expired entry is removed so it can be reused immediately.
func imgbbKeyCooled(key string) bool {
	imgbbKeyCooldown.mu.Lock()
	defer imgbbKeyCooldown.mu.Unlock()
	exp, ok := imgbbKeyCooldown.expiry[key]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(imgbbKeyCooldown.expiry, key)
		return false
	}
	return true
}

// clearImgBBKeyCooldownForTest empties the cooldown map (tests share the
// package-level state).
func clearImgBBKeyCooldownForTest() {
	imgbbKeyCooldown.mu.Lock()
	defer imgbbKeyCooldown.mu.Unlock()
	imgbbKeyCooldown.expiry = make(map[string]time.Time)
}

// markImgBBRateLimited records a rate-limit hit so throttleImgBB uses the
// longer cooldown interval for subsequent calls.
func markImgBBRateLimited() {
	imgbbMu.Lock()
	defer imgbbMu.Unlock()
	// Keep the cooldown window alive: extend it on every hit so a sustained
	// burst doesn't resume too early.
	imgbbBackoffUntil = time.Now().Add(imgbbCooldownInterval)
}

// throttleImgBB sleeps until the appropriate interval has elapsed since the
// previous ImgBB API call, then records the call time.  Safe for concurrent
// callers.  After a rate-limit error the longer cooldown interval is used.
func throttleImgBB() {
	imgbbMu.Lock()
	defer imgbbMu.Unlock()
	interval := imgbbMinInterval
	if time.Now().Before(imgbbBackoffUntil) {
		interval = imgbbCooldownInterval
	}
	if d := interval - time.Since(imgbbLastUpload); d > 0 {
		time.Sleep(d)
	}
	imgbbLastUpload = time.Now()
}

type imgbbResponse struct {
	Data struct {
		URL string `json:"url"`
	} `json:"data"`
	Status int             `json:"status"`
	Error  json.RawMessage `json:"error,omitempty"`
}

// imgbbKeyRing manages multiple API keys and rotates through them on
// rate-limit errors.  Keys are read from the IMGBB_API_KEY env var,
// which may be a comma-separated list (e.g. "key1,key2,key3").
//
// The ring is a package-level singleton so that the rotation index persists
// across NewImgBBUploader instances — every uploader shares the same ring
// and continues where the previous one left off.
type imgbbKeyRing struct {
	mu    sync.Mutex
	keys  []string
	index int
}

var globalImgbbKeyRing = sync.OnceValue(func() *imgbbKeyRing {
	raw := os.Getenv("IMGBB_API_KEY")
	var keys []string
	for _, k := range strings.Split(raw, ",") {
		k = strings.TrimSpace(k)
		if k != "" {
			keys = append(keys, k)
		}
	}
	return &imgbbKeyRing{keys: keys}
})

func (kr *imgbbKeyRing) current() string {
	kr.mu.Lock()
	defer kr.mu.Unlock()
	if len(kr.keys) == 0 {
		return ""
	}
	return kr.keys[kr.index]
}

func (kr *imgbbKeyRing) rotate() {
	kr.mu.Lock()
	defer kr.mu.Unlock()
	if len(kr.keys) > 1 {
		kr.index = (kr.index + 1) % len(kr.keys)
	}
}

func (kr *imgbbKeyRing) count() int {
	kr.mu.Lock()
	defer kr.mu.Unlock()
	return len(kr.keys)
}

// nextUsable returns the current key when it is not cooling down; otherwise it
// advances the ring (bounded by the key count) and returns the first usable
// key.  Returns "" when every key is cooling down (or no keys are set).
// A cooled key is skipped WITHOUT consuming an upload attempt — the attempt
// belongs to whichever usable key gets picked.
func (kr *imgbbKeyRing) nextUsable() string {
	kr.mu.Lock()
	defer kr.mu.Unlock()
	if len(kr.keys) == 0 {
		return ""
	}
	for i := 0; i < len(kr.keys); i++ {
		k := kr.keys[kr.index]
		if !imgbbKeyCooled(k) {
			return k
		}
		kr.index = (kr.index + 1) % len(kr.keys)
	}
	return ""
}

// ImgBBUploader handles uploading images to ImgBB with automatic
// key rotation on rate-limit errors.
type ImgBBUploader struct {
	keys   *imgbbKeyRing
	client *http.Client
}

func NewImgBBUploader() *ImgBBUploader {
	return &ImgBBUploader{
		keys:   globalImgbbKeyRing(),
		client: newNoProxyClient(60 * time.Second),
	}
}

// imgbbErrClass classifies an ImgBB API failure by body/status so the caller
// can decide between per-key cooldown (rate limit), rotation (auth) and a
// plain fail-fast (anything else — usually a bad request that every key
// rejects identically).
type imgbbErrClass int

const (
	classOther imgbbErrClass = iota
	classRateLimited
	classAuthFailed
)

// classifyImgBBError matches the observed ImgBB failure shapes:
//   - rate limit: HTTP 429, or HTTP 400 with body "Rate limit reached." (code 100)
//   - auth:       HTTP 403, or a body naming an invalid/unauthorized/suspended key
//   - other:      everything else (invalid image, file too large, …) — identical
//     for every key, so rotating wastes the attempt budget
func classifyImgBBError(statusCode int, body string) imgbbErrClass {
	lower := strings.ToLower(body)
	if statusCode == 429 || strings.Contains(lower, "rate limit") {
		return classRateLimited
	}
	if statusCode == 403 ||
		strings.Contains(lower, "invalid") ||
		strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "suspended") {
		return classAuthFailed
	}
	return classOther
}

// isRateLimitError returns true if the response indicates a rate-limit hit
// (HTTP 429 or the 400 "Rate limit reached." body).
func isRateLimitError(statusCode int, body []byte) bool {
	return classifyImgBBError(statusCode, string(body)) == classRateLimited
}

// Upload uploads an image file to ImgBB.  On rate-limit errors the key ring
// is rotated and the upload retried with the next key.  Each key is tried at
// most once per call to avoid hammering a rate-limited key with backoff.
func (u *ImgBBUploader) Upload(filePath string) (string, error) {
	if u.keys.count() == 0 {
		return "", fmt.Errorf("imgbb: IMGBB_API_KEY not set")
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("imgbb: read file: %w", err)
	}

	encoded := base64.StdEncoding.EncodeToString(data)

	// Try each key at most once (rotate on rate-limit).
	attempts := u.keys.count()
	var lastErr error
	for i := 0; i < attempts; i++ {
		// Space API calls process-wide so a burst can't exhaust every key
		// in a single tick (see throttleImgBB).
		throttleImgBB()
		// Skip keys still inside their post-rate-limit cooldown; the attempt
		// is re-pointed at the next usable key instead of being burned on a
		// key whose bucket is still empty.
		key := u.keys.nextUsable()
		if key == "" {
			if lastErr != nil {
				return "", lastErr
			}
			return "", fmt.Errorf("imgbb: all %d API keys are cooling down after rate limits", u.keys.count())
		}
		form := url.Values{
			"key":   {key},
			"image": {encoded},
		}

		resp, err := u.client.PostForm(imgbbAPIURL, form)
		if err != nil {
			lastErr = fmt.Errorf("imgbb: post: %w", err)
			u.keys.rotate()
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
		resp.Body.Close()

		if readErr != nil {
			lastErr = fmt.Errorf("imgbb: read response: %w", readErr)
			u.keys.rotate()
			continue
		}

		if resp.StatusCode == 429 {
			lastErr = fmt.Errorf("imgbb: rate limited (HTTP 429)")
			markImgBBRateLimited()
			coolImgBBKey(key)
			u.keys.rotate()
			continue
		}

		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("imgbb: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
			// Rotate on rate-limit responses (429 or the 400 "Rate limit
			// reached." body) or 403 (key-specific "Forbidden").  A
			// rate-limited key also enters per-key cooldown so the NEXT
			// call starts on a usable key instead of re-paying the failure.
			// Do NOT rotate on a generic 400 — that's usually a bad request
			// (invalid image format, file too large) which fails for every
			// key and rotating would waste attempts.
			switch classifyImgBBError(resp.StatusCode, string(body)) {
			case classRateLimited:
				markImgBBRateLimited()
				coolImgBBKey(key)
				u.keys.rotate()
				continue
			case classAuthFailed:
				if resp.StatusCode == 403 {
					u.keys.rotate()
					continue
				}
			}
			return "", lastErr
		}

		var result imgbbResponse
		if err := json.Unmarshal(body, &result); err != nil {
			lastErr = fmt.Errorf("imgbb: parse response: %w", err)
			u.keys.rotate()
			continue
		}

		if result.Status != 200 {
			msg := string(result.Error)
			var errObj struct {
				Message string `json:"message"`
			}
			if json.Unmarshal(result.Error, &errObj) == nil && errObj.Message != "" {
				msg = errObj.Message
			}
			if msg == "" || msg == "null" {
				msg = string(body)
			}
			err = fmt.Errorf("imgbb: error: %s", msg)
			lastErr = err
			if isRateLimitError(result.Status, []byte(msg)) {
				markImgBBRateLimited()
				coolImgBBKey(key)
				u.keys.rotate()
				continue
			}
			// Rotate on key-specific errors so the next key gets a chance.
			lower := strings.ToLower(msg)
			if strings.Contains(lower, "invalid") || strings.Contains(lower, "unauthorized") || strings.Contains(lower, "suspended") {
				u.keys.rotate()
				continue
			}
			return "", err
		}

		if result.Data.URL == "" {
			return "", fmt.Errorf("imgbb: empty image URL in response")
		}

		return result.Data.URL, nil
	}

	return "", lastErr
}
