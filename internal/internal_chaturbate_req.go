package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/server"
)

// PostChaturbateAPI makes a POST request to Chaturbate API
func PostChaturbateAPI(ctx context.Context, username, csrfToken string) (string, error) {
	apiURL := fmt.Sprintf("%sget_edge_hls_url_ajax/", server.Config.Domain)

	// Build POST data
	postData := url.Values{}
	postData.Set("room_slug", username)
	postData.Set("bandwidth", "high")

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBufferString(postData.Encode()))
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	// Set headers
	userAgent := server.Config.UserAgent
	if userAgent == "" {
		userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
	}

	req.Header.Set("User-Agent", strings.TrimSpace(userAgent))
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("DNT", "1")

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Pragma", "no-cache")
	req.Header.Set("Sec-Fetch-Dest", "empty")
	req.Header.Set("Sec-Fetch-Mode", "cors")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Header.Set("Sec-Ch-Ua", `"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"`)
	req.Header.Set("Sec-Ch-Ua-Mobile", "?0")
	req.Header.Set("Sec-Ch-Ua-Platform", `"Windows"`)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("X-CSRFToken", csrfToken)
	req.Header.Set("Referer", fmt.Sprintf("https://chaturbate.com/%s", username))
	req.Header.Set("Origin", "https://chaturbate.com")

	// Add cookies - sanitize to remove any invalid characters
	cookieStr := fmt.Sprintf("csrftoken=%s", csrfToken)
	if server.Config.Cookies != "" {
		// Remove any newlines, carriage returns, or other control characters
		sanitized := strings.Map(func(r rune) rune {
			if r == '\n' || r == '\r' || r == '\t' || r < 32 {
				return -1 // drop the character
			}
			return r
		}, server.Config.Cookies)
		sanitized = strings.TrimSpace(sanitized)
		if sanitized != "" {
			cookieStr = sanitized + "; " + cookieStr
		}
	}
	req.Header.Set("Cookie", cookieStr)

	// Create client with timeout
	client := &http.Client{
		Timeout:   30 * time.Second,
		Transport: CreateTransport(),
	}

	// Make request
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return "", ErrNotFound
	}

	// Read response body first to check for Cloudflare
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read body: %w", err)
	}

	bodyStr := string(body)

	// Check for Cloudflare block - only if body contains CF-specific markers
	isCFBlock := strings.Contains(bodyStr, "<title>Just a moment...</title>") ||
		strings.Contains(bodyStr, "Checking your browser") ||
		strings.Contains(bodyStr, "cloudflare")

	if isCFBlock {
		return "", ErrCloudflareBlocked
	}

	// A plain 403 without CF markers is a private/forbidden room, not a CF block
	if resp.StatusCode == 403 {
		return "", fmt.Errorf("forbidden: %w", ErrPrivateStream)
	}

	return bodyStr, nil
}
