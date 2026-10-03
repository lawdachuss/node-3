package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// imgPileThrottle mirrors ImgBB's process-wide spacing: ImgPile rate-limits
// per IP/Key with a daily quota (500 uploads/day free), and UploadToAll fires
// thumb+sprite+preview for every video in parallel.  Without spacing, a burst
// of a few uploads burns the window and every asset fails with HTTP 429 —
// observed live on nodes 8/10/13.  8s spacing ≈ 450/h ceiling; after a 429 a
// longer cooldown lets the window refill.
var (
	imgPileMu         sync.Mutex
	imgPileLastUpload time.Time
	imgPileBackoff    time.Time
)

const (
	imgPileMinInterval      = 8 * time.Second
	imgPileCooldownInterval = 90 * time.Second
)

// markImgPileRateLimited extends the cooldown window after a 429.
func markImgPileRateLimited() {
	imgPileMu.Lock()
	defer imgPileMu.Unlock()
	imgPileBackoff = time.Now().Add(imgPileCooldownInterval)
}

// throttleImgPile sleeps until the configured spacing has elapsed since the
// previous ImgPile API call.  Safe for concurrent callers.
func throttleImgPile() {
	imgPileMu.Lock()
	defer imgPileMu.Unlock()
	interval := imgPileMinInterval
	if time.Now().Before(imgPileBackoff) {
		interval = imgPileCooldownInterval
	}
	if d := interval - time.Since(imgPileLastUpload); d > 0 {
		time.Sleep(d)
	}
	imgPileLastUpload = time.Now()
}

// ImgPileUploader handles uploading images to imgpile.com.
// Requires an API key (created on the settings page), sent as a Bearer token.
// NSFW-friendly, permanent hosting, direct hotlinkable CDN URLs.
// API: POST https://imgpile.com/uploads  (raw file body, ?filename= query)
// Limits: 500 uploads/day free (2000 premium), up to 97 MB per file.
type ImgPileUploader struct {
	client *http.Client
	key    string
}

// imgPileResponse is the JSON response from the imgpile.com API.
type imgPileResponse struct {
	Data struct {
		Slug   string `json:"slug"`
		URLs   struct {
			Original string `json:"original"`
			Thumb    string `json:"thumb"`
		} `json:"urls"`
	} `json:"data"`
}

// NewImgPileUploader creates a new imgpile.com uploader.
// The key is read from the IMGPILE_KEY environment variable.
func NewImgPileUploader() *ImgPileUploader {
	return &ImgPileUploader{
		client: newNoProxyClient(2 * time.Minute),
		key:    os.Getenv("IMGPILE_KEY"),
	}
}

// HasToken returns true if an imgpile key is configured.
func (u *ImgPileUploader) HasToken() bool {
	return u.key != ""
}

// Upload uploads an image file to imgpile.com and returns the direct CDN URL.
// Requires a valid API key.
//
// API: POST https://imgpile.com/uploads?filename=<name>
// Auth: Bearer <key>
// Body: raw file bytes
// Response: JSON with data.urls.original containing the direct file URL.
func (u *ImgPileUploader) Upload(filePath string) (string, error) {
	if u.key == "" {
		return "", fmt.Errorf("imgpile: no key configured (set IMGPILE_KEY)")
	}

	release, ok := acquireHostSem("ImgPile")
	if !ok {
		return "", fmt.Errorf("imgpile: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second // 2s, 4s
			time.Sleep(backoff)
		}

		// Space calls process-wide so parallel asset uploads can't exhaust
		// the rate-limit window in a single tick (see imgPileThrottle).
		throttleImgPile()

		url, err := u.uploadOnce(filePath)
		if err == nil {
			return url, nil
		}
		lastErr = err

		if isUploadRateLimited(err) {
			// A 429 means the shared window is exhausted: mark the cooldown
			// so subsequent calls (in THIS retry loop and other goroutines)
			// pace at the slower interval, then try once more after backing
			// off instead of giving the file up entirely.
			markImgPileRateLimited()
			time.Sleep(imgPileCooldownInterval)
			continue
		}
		if isFailFastError(err) {
			return "", err
		}
	}
	return "", fmt.Errorf("imgpile: all 3 attempts failed, last: %w", lastErr)
}

// uploadOnce performs a single upload attempt without retry logic.
func (u *ImgPileUploader) uploadOnce(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("imgpile: open file: %w", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("imgpile: stat file: %w", err)
	}

	endpoint := "https://imgpile.com/uploads?" + url.Values{"filename": {filepath.Base(filePath)}}.Encode()
	req, err := http.NewRequest("POST", endpoint, file)
	if err != nil {
		return "", fmt.Errorf("imgpile: create request: %w", err)
	}
	req.ContentLength = fi.Size()
	req.Header.Set("Authorization", "Bearer "+u.key)
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("imgpile: send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB max response
	if err != nil {
		return "", fmt.Errorf("imgpile: read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return "", fmt.Errorf("imgpile: rate limited (HTTP 429)")
	}

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("imgpile: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var result imgPileResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("imgpile: parse response: %w", err)
	}

	imageURL := strings.TrimSpace(result.Data.URLs.Original)
	if imageURL == "" {
		return "", fmt.Errorf("imgpile: empty image URL in response")
	}

	// Ensure HTTPS.
	imageURL = strings.Replace(imageURL, "http://", "https://", 1)

	log.Printf("ImgPile: uploaded %s → %s", filepath.Base(filePath), imageURL)
	return imageURL, nil
}
