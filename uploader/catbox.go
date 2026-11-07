package uploader

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
)

// CatboxUploader handles uploading images to Catbox.moe.
// No API key required — anonymous uploads are supported.
// When a CatboxProxyURL is configured, uploads are routed through
// the proxy Cloudflare Worker instead of going directly to catbox.moe.
type CatboxUploader struct {
	client   *http.Client
	proxyURL string
	userHash string
}

// NewCatboxUploader creates a new Catbox.moe uploader.
// When a proxy URL is configured (via the catbox-proxy-url setting),
// uploads are routed through the Cloudflare Worker proxy to avoid
// direct IP blocks from Catbox.
func NewCatboxUploader() *CatboxUploader {
	transport := &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	proxyURL := ""
	if server.Config != nil {
		proxyURL = strings.Trim(server.Config.CatboxProxyURL, `"`)
	}

	return &CatboxUploader{
		client: &http.Client{
			Timeout:   2 * time.Minute,
			Transport: transport,
		},
		proxyURL: proxyURL,
		userHash: os.Getenv("CATBOX_USERHASH"),
	}
}

// Upload uploads a file to Catbox.moe and returns the direct file URL.
// Retries up to 3 times with exponential backoff (2s, 4s) on transient errors
// (network failures and 5xx server errors). Client errors (4xx) are fatal.
//
// API: POST https://catbox.moe/user/api.php
// Fields: reqtype=fileupload, fileToUpload=@file (multipart)
// Response on success: plain text URL like "https://files.catbox.moe/abc123.webp"
// Response on error: plain text error message starting with an error description.
func (u *CatboxUploader) Upload(filePath string) (string, error) {
	release, ok := acquireHostSem("Catbox")
	if !ok {
		return "", fmt.Errorf("catbox: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	var lastErr error

	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			backoff := time.Duration(1<<uint(attempt)) * time.Second // 2s, 4s
			time.Sleep(backoff)
		}

		url, err := u.uploadOnce(filePath)
		if err == nil {
			return url, nil
		}

		lastErr = err

		// Only retry on transient errors: network/IO failures or server errors.
		// Client-side errors (file not found, bad response format) and a dead
		// or rate-limiting host are fatal here — bail so the caller's fallback
		// (Catbox → ImgBB) can try the next host instead of hammering this one.
		if isRetryableCatboxError(err) && !isFailFastError(err) {
			continue
		}

		return "", err
	}

	return "", fmt.Errorf("catbox: all 3 attempts failed, last: %w", lastErr)
}

// uploadOnce performs a single upload attempt without retry logic.
// The file is streamed through an io.Pipe so large images are never buffered
// in RAM — only the multipart preamble (headers + form fields) is assembled
// upfront, which is always small (< 1 KB).
func (u *CatboxUploader) uploadOnce(filePath string) (string, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("catbox: stat file: %w", err)
	}

	// Build the multipart preamble (all headers and form fields, but NOT the
	// file bytes) into a small buffer so we can compute the exact Content-Length
	// and avoid chunked transfer encoding.
	var preamble bytes.Buffer
	mw := multipart.NewWriter(&preamble)

	if err := mw.WriteField("reqtype", "fileupload"); err != nil {
		return "", fmt.Errorf("catbox: write reqtype: %w", err)
	}
	if u.userHash != "" {
		if err := mw.WriteField("userhash", u.userHash); err != nil {
			return "", fmt.Errorf("catbox: write userhash: %w", err)
		}
	}
	if _, err := mw.CreateFormFile("fileToUpload", filepath.Base(filePath)); err != nil {
		return "", fmt.Errorf("catbox: create form file: %w", err)
	}
	closing := fmt.Sprintf("\r\n--%s--\r\n", mw.Boundary())
	contentType := mw.FormDataContentType()

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("catbox: open file: %w", err)
	}
	defer file.Close()

	totalLen := int64(preamble.Len()) + fi.Size() + int64(len(closing))
	body := io.MultiReader(&preamble, file, bytes.NewReader([]byte(closing)))

	req, err := http.NewRequest("POST", u.uploadURL(), body)
	if err != nil {
		return "", fmt.Errorf("catbox: create request: %w", err)
	}
	req.ContentLength = totalLen
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://catbox.moe")
	req.Header.Set("Referer", "https://catbox.moe/")

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("catbox: send request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 64*1024)) // 64KB max response
	if err != nil {
		return "", fmt.Errorf("catbox: read response: %w", err)
	}

	text := strings.TrimSpace(string(raw))

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("catbox: status %d: %s", resp.StatusCode, text)
	}

	if text == "" {
		return "", fmt.Errorf("catbox: empty response")
	}

	if !strings.HasPrefix(text, "https://") {
		return "", fmt.Errorf("catbox: unexpected response: %s", text)
	}

	return text, nil
}

// uploadURL returns the endpoint for Catbox uploads.
// When a proxy URL is configured, uploads route through the
// Cloudflare Worker proxy to avoid direct IP blocks from Catbox.
func (u *CatboxUploader) uploadURL() string {
	if u.proxyURL != "" {
		return u.proxyURL
	}
	return "https://catbox.moe/user/api.php"
}

// isRetryableCatboxError returns true if the error represents a transient
// failure that might succeed on retry (network glitch, server overload,
// or IP-based rate limiting).
func isRetryableCatboxError(err error) bool {
	errStr := err.Error()

	// Server-side HTTP errors (5xx) — retry
	if strings.Contains(errStr, "status 5") {
		return true
	}

	// HTTP 412 from Catbox is a blanket rejection whose body varies
	// ("Invalid uploader", "Not signed in", ...). It is often a transient IP
	// block or rate-limit from Catbox's abuse prevention — observed fleet-wide
	// as a short burst of "Not signed in!" rejections that resolved minutes
	// later. Treat any 412 (and 429 rate limits) as retryable so a brief
	// rejection window doesn't abort after one attempt and cascade into
	// "all hosts failed" + ImgBB rate-limit. 5xx stays covered below.
	if strings.Contains(errStr, "status 412") || strings.Contains(errStr, "status 429") {
		return true
	}

	// File stat errors — retry (AV scanner race on Windows)
	if strings.Contains(errStr, "stat file") {
		return true
	}

	// Network/connection errors — retry
	if strings.Contains(errStr, "send request") {
		return true
	}

	// Read errors — retry (could be temporary server reset)
	if strings.Contains(errStr, "read response") {
		return true
	}

	return false
}
