package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// vidaraCodeRe is an allowlist for plausible Vidara file codes. Codes are
// base62-ish alphanumeric and at least 6 chars, so a structural path segment
// ("v", "e", "embed", "watch", ...) can never pass. This is strictly more
// robust than enumerating known bad segments: a route Vidara adds tomorrow
// (e.g. /play/CODE) still resolves to the real code because "play" fails the
// pattern.
var vidaraCodeRe = regexp.MustCompile(`^[A-Za-z0-9]{6,}$`)

// vidaraAPIBase is a var (not const) so tests can point it at a fake server.
var vidaraAPIBase = "https://api.vidara.so/v1"

// VidaraUploader handles uploading files to vidara.so
type VidaraUploader struct {
	keys   *keyRing
	client *http.Client
}

// NewVidaraUploader creates a new Vidara uploader instance.  The API key may
// be a comma-separated list ("key1,key2,key3") to enable rotation when a key
// is invalidated.  Each instance builds its own ring from the passed value /
// env, so concurrent uploaders each rotate independently.
func NewVidaraUploader(apiKey string) *VidaraUploader {
	ring := buildRingFromEnv("VIDARA_KEY", apiKey)
	ring.host = "Vidara"
	return &VidaraUploader{
		keys:   ring,
		client: &http.Client{
			Timeout: 120 * time.Minute, // Long timeout for large video uploads
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
				DialContext:         (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

type vidaraServerResponse struct {
	Msg    string `json:"msg"`
	Status int    `json:"status"`
	Result struct {
		UploadServer string `json:"upload_server"`
	} `json:"result"`
}

type vidaraUploadResponse struct {
	URL      string `json:"url"`
	Title    string `json:"title"`
	VideoID  int64  `json:"video_id"`
	Filecode string `json:"filecode"`
	Msg      string `json:"msg"`
	Status   int    `json:"status"`
}

// Upload uploads a file to Vidara and returns the view link
func (u *VidaraUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to Vidara and reports progress through fn.
func (u *VidaraUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.keys.count() == 0 {
		return "", fmt.Errorf("Vidara API key not configured")
	}

	release, ok := acquireHostSem("Vidara")
	if !ok {
		return "", fmt.Errorf("vidara: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	// Detect when the ENTIRE file body has been handed to the transport on
	// this attempt.  A failure after that point (5xx after upload, EOF,
	// connection reset, response-decode error) means Vidara already received
	// every byte of the file — retrying would push the SAME file again and
	// stack duplicate uploads (the "uploaded 100%, then it started uploading
	// again" loop).  In that case bail on this host immediately instead of
	// re-streaming the file up to 3 times per key.
	var bodyFullySent bool
	wrapped := func(host string, current, total int64) {
		if total > 0 && current >= total {
			bodyFullySent = true
		}
		if progress != nil {
			progress(host, current, total)
		}
	}

	var lastErr error

	// Try each key at most once per call.  A single-key ring degenerates to a
	// single attempt loop (rotate is a no-op), preserving prior behavior.
	keyAttempts := u.keys.count()
	if keyAttempts < 1 {
		keyAttempts = 1
	}
	maxAttempts := 3 // per-key upload retries (backoff on rate-limit)

	for k := 0; k < keyAttempts; k++ {
		key := u.keys.current()

		for attempt := 1; attempt <= maxAttempts; attempt++ {
			if attempt > 1 {
				time.Sleep(uploadBackoff(attempt-2, lastErr))
			}

			downloadLink, err := u.uploadFile(filePath, key, wrapped)
			if err != nil {
				lastErr = fmt.Errorf("upload file: %w", err)
				// A fully-transmitted body already reached the server; the
				// failure is only in reading the response.  Re-streaming the
				// same file would create duplicate uploads, so surface the
				// error now and let the caller's outer retry cadence (journal
				// state + pipeline backoff) decide what happens next.
				if bodyFullySent {
					return "", lastErr
				}
				if isUploadRateLimited(err) {
					time.Sleep(uploadBackoff(attempt, err))
					lastErr = nil
					continue
				}
				// Fail fast on host-side capacity failures: "no healthy upload
				// server" (503) and nginx 504 gateway timeouts mean Vidara itself
				// is saturated — burning the remaining attempts on it just stalls
				// the file while other hosts could have finished. Bail immediately
				// and let the caller's parallel host chain take the load.
				if isVidaraCapacityError(err) {
					return "", lastErr
				}
				// Invalid/expired key for THIS key: rotate to the next key.
				if isVidaraAuthError(err) {
					u.keys.rotateReason("authentication rejected")
					lastErr = nil
					break
				}
				if attempt < maxAttempts {
					continue
				}
				return "", lastErr
			}

			return downloadLink, nil
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("Vidara upload failed: all keys exhausted")
	}
	return "", lastErr
}

// isVidaraCapacityError reports whether the error is a Vidara-side capacity
// failure (503 no healthy upload server / 504 gateway timeout). Retrying
// within the same upload window cannot fix these — the site's own upstream
// is down or saturated.
func isVidaraCapacityError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no healthy upload server") ||
		strings.Contains(msg, "status 503") ||
		strings.Contains(msg, "status 504") ||
		strings.Contains(msg, "504 gateway time-out")
}

// isVidaraAuthError returns true when the Vidara error indicates the API key
// is invalid, expired, or revoked.  A bad key will never succeed within a run,
// so callers should rotate to the next key instead of retrying the same one.
//
// The matcher deliberately uses explicit credential-rejection wording ONLY and
// NOT broad substrings like "403", "forbidden", or a bare "api key".  Vidara's
// API is fronted by nginx/Cloudflare, and block pages / WAF 403 responses
// embed those words; matching them mislabels a site-side block as a dead key,
// which rotates a perfectly valid key to "Vidara upload failed: all keys
// exhausted" for every file (seen in production: 460 such journal rows while
// the configured key still returned HTTP 200 on /upload/server).
func isVidaraAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Vidara has returned the rejection in every one of these spellings
	// ("invalid api key", "invalid_api_key", "invalid api_key").
	return strings.Contains(msg, "invalid api key") ||
		strings.Contains(msg, "invalid api_key") ||
		strings.Contains(msg, "invalid_api_key") ||
		strings.Contains(msg, "api key is invalid") ||
		strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "unauthorized")
}

// getUploadServer gets the upload server URL from the Vidara API
func (u *VidaraUploader) getUploadServer(key string) (string, error) {
	req, err := http.NewRequest("GET", vidaraAPIBase+"/upload/server?api_key="+key, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request upload server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get upload server failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var serverResp vidaraServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode server response: %w", err)
	}

	if serverResp.Status != 200 || serverResp.Result.UploadServer == "" {
		return "", fmt.Errorf("server status not ok: %d (msg: %s)", serverResp.Status, serverResp.Msg)
	}

	return serverResp.Result.UploadServer, nil
}

func (u *VidaraUploader) uploadFile(filePath, key string, progress ProgressFunc) (string, error) {
	uploadServer, err := u.getUploadServer(key)
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	body, contentLen, contentType, file, err := multipartStreamWithProgress(
		map[string]string{"api_key": key},
		"file", filePath, "Vidara", progress,
	)
	if err != nil {
		return "", fmt.Errorf("multipart stream: %w", err)
	}
	defer file.Close()

	req, err := http.NewRequest("POST", uploadServer, body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = contentLen

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var uploadResp vidaraUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}

	// The API has returned every one of these shapes at different times:
	//   * {"filecode":"AbC123xY", "url":"https://vidara.so/v/AbC123xY"}  (docs)
	//   * {"filecode":"AbC123xY", "url":"https://vidara.to/e/AbC123xY"}  (embed in url)
	//   * {"filecode":"https://vidara.to/e/AbC123xY", "url":""}          (embed in filecode)// So blindly returning uploadResp.URL (or prefixing vidara.so/v/ onto
// Filecode) produced mangled links like
// "https://vidara.so/v/https://vidara.to/e/CODE". Extract the trailing
// file code from whichever field carries it and build the canonical
// embed link instead.
	code := vidaraFileCode(uploadResp.Filecode)
	if code == "" {
		code = vidaraFileCode(uploadResp.URL)
	}
	if code != "" {
		return "https://vidara.so/e/" + code, nil
	}
	if uploadResp.URL != "" {
		return uploadResp.URL, nil
	}
	return "", fmt.Errorf("no file URL in response")
}

// vidaraFileCode extracts the trailing video file code from any Vidara API
// value. The value may be a plain code ("AbC123xY"), a view link
// ("https://vidara.so/v/AbC123xY"), or an embed link
// ("https://vidara.to/e/AbC123xY") — everything after the last "/" is the
// code. Returns "" for empty values or values whose last segment is not a
// plausible code (contains ".", whitespace, or a query string), so a leaked
// filename or a query-laden URL never becomes a code.
func vidaraFileCode(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimRight(v, "/")
	if v == "" {
		return ""
	}
	// Strip query/fragment BEFORE taking the last path segment: a query can
	// legally contain "/" (e.g. "/e/CODE?redirect=/x") and would otherwise
	// be mistaken for a path separator.
	if i := strings.IndexAny(v, "?#"); i >= 0 {
		v = v[:i]
	}
	if i := strings.LastIndex(v, "/"); i >= 0 {
		v = v[i+1:]
	}
	if !vidaraCodeRe.MatchString(v) {
		return ""
	}
	// Belt-and-suspenders: a bare ".../download" would pass the allowlist
	// pattern, so keep the explicit structural-segment reject too.
	switch v {
	case "v", "e", "embed", "watch", "d", "download", "file", "video":
		return ""
	}
	return v
}
