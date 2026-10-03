package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// buildStreamtapeCredList assembles the credential list handed to the ring
// constructor: env `STREAMTAPE_CREDS` (comma-separated `login:key` pairs) takes
// precedence, then the single login/key passed by the caller.  This mirrors
// buildRingFromEnv/buildVidMolyKeyList so operators can hand each node its own
// pool without code changes.
func buildStreamtapeCredList(login, key string) string {
	if login == "" || key == "" {
		return strings.TrimSpace(os.Getenv("STREAMTAPE_CREDS"))
	}
	if raw := strings.TrimSpace(os.Getenv("STREAMTAPE_CREDS")); raw != "" {
		return raw
	}
	return login + ":" + key
}

// StreamtapeUploader handles uploading files to Streamtape
type StreamtapeUploader struct {
	creds  *keyRing // "login:key" pairs; rotates on authentication failure
	client *http.Client
}

// NewStreamtapeUploader creates a new Streamtape uploader instance. The
// login/key may be overridden by a per-node pool in STREAMTAPE_CREDS, a
// comma-separated list of `login:key` pairs that are rotated through each time
// a credential is invalidated (mirrors the VIDMOLY_KEYS ring). A single
// login+key retains the existing single-credential behavior.
func NewStreamtapeUploader(login, key string) *StreamtapeUploader {
	ring := newKeyRing(buildStreamtapeCredList(login, key))
	if ring.count() == 0 {
		// Keep a single dead credential in the ring so callers get a clear
		// "authentication failed" instead of an empty-key panic.
		ring = newKeyRing(login + ":" + key)
	}
	return &StreamtapeUploader{
		creds:  ring,
		client: &http.Client{
			Timeout: 120 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				DisableCompression:    true,
				TLSHandshakeTimeout:   30 * time.Second,
				ResponseHeaderTimeout: 120 * time.Second,
				DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

type streamtapeServerResp struct {
	Status int    `json:"status"`
	Msg    string `json:"msg"`
	Result struct {
		URL string `json:"url"`
	} `json:"result"`
}

type streamtapeUploadResp struct {
	Status int    `json:"status"`
	Msg    string `json:"msg"`
	Result struct {
		ID    string `json:"id"`
		URL   string `json:"url"`
		Embed string `json:"embed"`
	} `json:"result"`
}

// Upload uploads a file to Streamtape and returns the embed/view link
func (u *StreamtapeUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to Streamtape and reports progress through fn.
func (u *StreamtapeUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	release, ok := acquireHostSem("Streamtape")
	if !ok {
		return "", fmt.Errorf("streamtape: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	uploadURL, err := u.getUploadURL()
	if err != nil {
		return "", fmt.Errorf("get upload URL: %w", err)
	}

	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		link, err := u.uploadFile(filePath, uploadURL, progress)
		if err != nil {
			lastErr = fmt.Errorf("upload file: %w", err)
			if isUploadRateLimited(err) {
				time.Sleep(uploadBackoff(attempt, err))
				lastErr = nil
				continue
			}
			if attempt < 3 {
				continue
			}
			return "", lastErr
		}
		return link, nil
	}
	return "", lastErr
}

func (u *StreamtapeUploader) getUploadURL() (string, error) {
	if u.creds.count() == 0 {
		return "", fmt.Errorf("Streamtape credentials not configured")
	}

	var lastErr error
	for k := 0; k < u.creds.count(); k++ {
		login, key, ok := splitStreamtapeCred(u.creds.current())
		if !ok {
			lastErr = fmt.Errorf("invalid streamtape credential (want login:key)")
			u.creds.rotate()
			continue
		}

		url := fmt.Sprintf("https://api.streamtape.com/file/ul?login=%s&key=%s", login, key)
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			return "", fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("User-Agent", defaultUserAgent)

		resp, err := u.client.Do(req)
		if err != nil {
			return "", fmt.Errorf("request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return "", fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}

		var serverResp streamtapeServerResp
		decodeErr := json.NewDecoder(resp.Body).Decode(&serverResp)
		resp.Body.Close()
		if decodeErr != nil {
			return "", fmt.Errorf("decode response: %w", decodeErr)
		}
		if serverResp.Status != 200 {
			err = fmt.Errorf("API error %d: %s", serverResp.Status, serverResp.Msg)
			// A rejected credential — rotate to the next pair in the pool.
			if isUploadAuthError(err) {
				if u.creds.count() > 1 {
					u.creds.rotate()
					lastErr = err
					continue
				}
				// Single credential is auth-rejected - mark host as dead for this run
				// by returning the error so caller can handle it
				return "", err
			}
			return "", err
		}
		if serverResp.Result.URL == "" {
			return "", fmt.Errorf("empty upload URL in response")
		}
		return serverResp.Result.URL, nil
	}
	return "", lastErr
}

// splitStreamtapeCred splits a ring entry of the form `login:key` into its two
// parts.  The login/key pair is joined by the first `:` so a key containing a
// colon is preserved.
func splitStreamtapeCred(cred string) (login, key string, ok bool) {
	idx := strings.IndexByte(cred, ':')
	if idx < 0 {
		return "", "", false
	}
	return cred[:idx], cred[idx+1:], true
}

func (u *StreamtapeUploader) uploadFile(filePath, uploadURL string, progress ProgressFunc) (string, error) {
	// Build multipart body with exact Content-Length — Streamtape rejects chunked encoding.
	body, contentLen, contentType, closer, err := multipartStreamWithProgress(nil, "file", filePath, "Streamtape", progress)
	if err != nil {
		return "", fmt.Errorf("build multipart: %w", err)
	}
	defer closer.Close()

	req, err := http.NewRequest("POST", uploadURL, body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.ContentLength = contentLen
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload status 429: rate limit — %s", strings.TrimSpace(string(body)))
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload status %d: %s", resp.StatusCode, string(body))
	}

	var uploadResp streamtapeUploadResp
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}
	if uploadResp.Status != 200 {
		return "", fmt.Errorf("upload API error %d: %s", uploadResp.Status, uploadResp.Msg)
	}
	if uploadResp.Result.ID == "" {
		return "", fmt.Errorf("empty file ID in upload response")
	}

	embedURL := uploadResp.Result.Embed
	if embedURL == "" {
		embedURL = fmt.Sprintf("https://streamtape.com/e/%s/", uploadResp.Result.ID)
	}
	return embedURL, nil
}
