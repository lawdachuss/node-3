package uploader

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

// imgboxBreaker is a process-wide circuit breaker: Imgbox's token endpoint
// has had multi-week 500 outages, and UploadToAll attempts it for every
// thumb/sprite/preview of every video on every node — each attempt paying a
// homepage fetch + token POST before failing.  After imgboxBreakerThreshold
// consecutive failures the host is skipped instantly for
// imgboxBreakerCooldown; a success resets the streak.
var (
	imgboxMu        sync.Mutex
	imgboxFailures  int
	imgboxOpenUntil time.Time
)

const (
	imgboxBreakerThreshold = 3
	imgboxBreakerCooldown  = 15 * time.Minute
)

// imgboxBreakerOpen reports whether uploads should be skipped right now.
func imgboxBreakerOpen() bool {
	imgboxMu.Lock()
	defer imgboxMu.Unlock()
	return time.Now().Before(imgboxOpenUntil)
}

// imgboxRecordResult updates the breaker streak; it opens after the
// consecutive-failure threshold and resets on success.
func imgboxRecordResult(ok bool) {
	imgboxMu.Lock()
	defer imgboxMu.Unlock()
	if ok {
		imgboxFailures = 0
		return
	}
	imgboxFailures++
	if imgboxFailures >= imgboxBreakerThreshold {
		imgboxOpenUntil = time.Now().Add(imgboxBreakerCooldown)
		imgboxFailures = 0
	}
}

// ImgboxUploader handles uploading images to imgbox.com.
// No API key required — uses CSRF token + guest gallery token.
// Supports adult/NSFW content (content_type=2). Permanent hosting, 10 MB limit.
// CDN URLs: https://images2.imgbox.com/{hash}/{filename}
type ImgboxUploader struct {
	client *http.Client
}

// imgboxTokenResponse is the JSON response from the token generate endpoint.
type imgboxTokenResponse struct {
	TokenID       string `json:"token_id"`
	TokenSecret   string `json:"token_secret"`
	GalleryID     string `json:"gallery_id"`
	GallerySecret string `json:"gallery_secret"`
}

// imgboxUploadResponse is the JSON response from the upload process endpoint.
type imgboxUploadResponse struct {
	Files []struct {
		OriginalURL  string `json:"original_url"`
		ThumbnailURL string `json:"thumbnail_url"`
		URL          string `json:"url"`
	} `json:"files"`
}

// csrfTokenRe matches the CSRF meta tag in imgbox.com HTML.
var csrfTokenRe = regexp.MustCompile(`<meta[^>]+name="csrf-token"[^>]+content="([^"]+)"`)

// NewImgboxUploader creates a new imgbox.com uploader.
func NewImgboxUploader() *ImgboxUploader {
	return &ImgboxUploader{
		client: newNoProxyClient(2 * time.Minute),
	}
}

// Upload uploads an image file to imgbox.com and returns the direct image URL.
// Steps:
//  1. GET imgbox.com homepage → extract CSRF token
//  2. POST /ajax/token/generate → get gallery token
//  3. POST /upload/process → upload file, get image URL
func (u *ImgboxUploader) Upload(filePath string) (string, error) {
	// Circuit breaker: skip instantly while the host is known-broken
	// (multi-week token-endpoint 500s) instead of paying the homepage +
	// token round-trips on every asset.
	if imgboxBreakerOpen() {
		return "", fmt.Errorf("imgbox: skipped — circuit breaker open after repeated failures")
	}

	release, ok := acquireHostSem("Imgbox")
	if !ok {
		return "", fmt.Errorf("imgbox: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("imgbox: stat file: %w", err)
	}

	// Check 10 MB limit
	if fi.Size() > 10*1024*1024 {
		return "", fmt.Errorf("imgbox: file exceeds 10 MB limit (%d bytes)", fi.Size())
	}

	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second // 2s, 4s
			time.Sleep(backoff)
		}

		url, err := u.uploadOnce(filePath)
		if err == nil {
			imgboxRecordResult(true)
			return url, nil
		}
		lastErr = err

		// Bail immediately on a dead host (HTTP 5xx from the token/upload
		// endpoint, connection refused, no such host) or an explicitly
		// fail-fast error — retrying a service that's down on every attempt
		// (Imgbox's token endpoint has returned 500 for weeks) just burns the
		// retry budget before the fallback chain moves on.
		if isFailFastError(err) || isHostDead(err) {
			imgboxRecordResult(false)
			return "", err
		}
	}
	imgboxRecordResult(false)
	return "", fmt.Errorf("imgbox: all 3 attempts failed, last: %w", lastErr)
}

// uploadOnce performs a single upload attempt.
func (u *ImgboxUploader) uploadOnce(filePath string) (string, error) {
	// Step 1: Get CSRF token from homepage
	csrfToken, err := u.getCSRFToken()
	if err != nil {
		return "", fmt.Errorf("imgbox: get CSRF token: %w", err)
	}

	// Step 2: Get gallery token
	token, err := u.getToken(csrfToken)
	if err != nil {
		return "", fmt.Errorf("imgbox: get token: %w", err)
	}

	// Step 3: Upload file
	return u.uploadFile(filePath, csrfToken, token)
}

// getCSRFToken fetches the imgbox.com homepage and extracts the CSRF token.
func (u *ImgboxUploader) getCSRFToken() (string, error) {
	req, err := http.NewRequest("GET", "https://imgbox.com/", nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "text/html")

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch homepage: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 5*1024*1024)) // 5MB limit
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}

	// Try regex first (faster)
	if matches := csrfTokenRe.FindSubmatch(body); len(matches) >= 2 {
		return string(matches[1]), nil
	}

	// Fall back to HTML parser
	token := extractCSRFToken(string(body))
	if token == "" {
		return "", fmt.Errorf("CSRF token not found in HTML")
	}
	return token, nil
}

// extractCSRFToken parses HTML to find <meta name="csrf-token" content="...">
func extractCSRFToken(htmlStr string) string {
	tokenizer := html.NewTokenizer(strings.NewReader(htmlStr))
	for {
		tt := tokenizer.Next()
		if tt == html.ErrorToken {
			return ""
		}
		if tt == html.StartTagToken || tt == html.SelfClosingTagToken {
			token := tokenizer.Token()
			if token.Data == "meta" {
				var name, content string
				for _, attr := range token.Attr {
					switch attr.Key {
					case "name":
						name = attr.Val
					case "content":
						content = attr.Val
					}
				}
				if name == "csrf-token" && content != "" {
					return content
				}
			}
		}
	}
}

// getToken requests a guest gallery token from imgbox.com.
func (u *ImgboxUploader) getToken(csrfToken string) (*imgboxTokenResponse, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	mw.WriteField("gallery", "true")
	mw.WriteField("gallery_title", "")
	mw.WriteField("comments_enabled", "0")
	mw.Close()

	req, err := http.NewRequest("POST", "https://imgbox.com/ajax/token/generate", &buf)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-CSRF-Token", csrfToken)
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Origin", "https://imgbox.com")
	req.Header.Set("Referer", "https://imgbox.com/")

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1*1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var token imgboxTokenResponse
	if err := json.Unmarshal(raw, &token); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}

	if token.TokenID == "" || token.TokenSecret == "" {
		return nil, fmt.Errorf("empty token in response: %s", string(raw))
	}

	return &token, nil
}

// uploadFile uploads the actual image file to imgbox.com.
func (u *ImgboxUploader) uploadFile(filePath, csrfToken string, token *imgboxTokenResponse) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("stat file: %w", err)
	}

	// Build multipart preamble with form fields.
	var preamble bytes.Buffer
	mw := multipart.NewWriter(&preamble)

	mw.WriteField("token_id", token.TokenID)
	mw.WriteField("token_secret", token.TokenSecret)
	mw.WriteField("gallery_id", token.GalleryID)
	mw.WriteField("gallery_secret", token.GallerySecret)
	mw.WriteField("content_type", "2")      // adult
	mw.WriteField("thumbnail_size", "200r") // 200px, keep aspect ratio
	mw.WriteField("comments_enabled", "0")

	if _, err := mw.CreateFormFile("files[]", filepath.Base(filePath)); err != nil {
		return "", fmt.Errorf("create form file: %w", err)
	}
	closing := fmt.Sprintf("\r\n--%s--\r\n", mw.Boundary())
	contentType := mw.FormDataContentType()

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	totalLen := int64(preamble.Len()) + fi.Size() + int64(len(closing))
	body := io.MultiReader(&preamble, file, bytes.NewReader([]byte(closing)))

	req, err := http.NewRequest("POST", "https://imgbox.com/upload/process", body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.ContentLength = totalLen
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("X-CSRF-Token", csrfToken)
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Origin", "https://imgbox.com")
	req.Header.Set("Referer", "https://imgbox.com/")

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024)) // 10MB max
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode == 429 {
		return "", fmt.Errorf("imgbox: rate limited (HTTP 429)")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("imgbox: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var result imgboxUploadResponse
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("parse response: %w", err)
	}

	if len(result.Files) == 0 {
		return "", fmt.Errorf("imgbox: no files in response: %s", string(raw))
	}

	// Prefer original_url (full resolution), fall back to url
	imageURL := strings.TrimSpace(result.Files[0].OriginalURL)
	if imageURL == "" {
		imageURL = strings.TrimSpace(result.Files[0].URL)
	}
	if imageURL == "" {
		return "", fmt.Errorf("imgbox: empty image URL in response")
	}

	// Ensure HTTPS
	imageURL = strings.Replace(imageURL, "http://", "https://", 1)

	log.Printf("Imgbox: uploaded %s → %s", filepath.Base(filePath), imageURL)
	return imageURL, nil
}
