package uploader

import (
	"encoding/json"
	"fmt"
	"html"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// vidmolyCodeRe is an allowlist for plausible VidMoly file codes
// (base62-ish alphanumeric, >= 6 chars) — see vidaraCodeRe for rationale.
var vidmolyCodeRe = regexp.MustCompile(`^[A-Za-z0-9]{6,}$`)

// vidmolyAPIBase is a var (not const) so tests can point it at a fake server.
var vidmolyAPIBase = "https://vidmoly.me"

// VidMoly's upload flow is a POST to a per-request upload server; the server
// answers with an HTML form handoff containing three textareas (op, fn, st).
// The "st" textarea is "OK" on success (with the new file code in "fn"), or a
// human-readable error message on rejection.
var (
	vidmolyStRe = regexp.MustCompile(`(?i)<textarea[^>]*name=["']st["'][^>]*>(.*?)</textarea>`)
	vidmolyFnRe = regexp.MustCompile(`(?i)<textarea[^>]*name=["']fn["'][^>]*>(.*?)</textarea>`)
)

// vidmolyServerResponse is the JSON returned by GET /api/upload/server.
// "result" is the upload-server URL as a plain string (not an object).
type vidmolyServerResponse struct {
	Msg    string `json:"msg"`
	Status int    `json:"status"`
	Result string `json:"result"`
}

// VidMolyUploader handles uploading files to vidmoly.me
type VidMolyUploader struct {
	keys   *keyRing
	client *http.Client
}

// NewVidMolyUploader creates a new VidMoly uploader instance. The API key may
// be a comma-separated list to enable rotation when a key is invalidated.
func NewVidMolyUploader(apiKey string) *VidMolyUploader {
	ring := buildRingFromEnv("VIDMOLY_KEY", apiKey)
	ring.host = "VidMoly"
	return &VidMolyUploader{
		keys: ring,
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

// Upload uploads a file to VidMoly and returns the view link
func (u *VidMolyUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to VidMoly and reports progress through fn.
func (u *VidMolyUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.keys.count() == 0 {
		return "", fmt.Errorf("VidMoly API key not configured")
	}

	release, ok := acquireHostSem("VidMoly")
	if !ok {
		return "", fmt.Errorf("vidmoly: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	// Detect when the ENTIRE file body has been handed to the transport on
	// this attempt (see the matching short-circuit in vidara.go): a rejection
	// that arrives AFTER the last byte was sent must not be re-streamed, or
	// every hour of quota would be double-counted against the daily cap.
	var bodyFullySent bool
	wrapped := func(host string, current, total int64) {
		if total > 0 && current >= total {
			bodyFullySent = true
		}
		if progress != nil {
			progress(host, current, total)
		}
	}

	var (
		lastErr error
		// lastRejection keeps the most recent failure the host actually
		// reported.  The retry loop clears lastErr when a key is rotated or a
		// rate limit is backed off, so without this the terminal error was a
		// bare "all keys exhausted" that hid the real cause (verified live:
		// {"status":429,"msg":"Daily API limit reached (50)."}) for days.
		lastRejection error
	)

	// Try each key at most once per call (a single-key ring degenerates to a
	// single attempt loop, preserving prior behavior).
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
				lastRejection = err
				// A fully-transmitted body already reached the server; the
				// failure is only in reading the response. Re-streaming the
				// same file would double-count it against the daily cap (and
				// could create duplicate uploads), so surface the error now.
				if bodyFullySent {
					return "", lastErr
				}
				// Daily budget exhausted, checked BEFORE the generic 429
				// matcher below.  VidMoly reports its API cap as
				// {"status":429,"msg":"Daily API limit reached (50)."}, and
				// isUploadRateLimited matches a bare "429" — so previously this
				// branch was unreachable: the loop burned all 3 attempts per key
				// on a limit that resets only at midnight, then reported the
				// misleading "all keys exhausted" and never disabled the host.
				// Verified live: the shared key was at used_today 2416 / limit
				// 50.  Move to the next key; if none is left, the error reaches
				// the caller, which skips VidMoly until tomorrow.
				if isVidMolyDailyLimit(err) {
					u.keys.rotateReason("daily api limit")
					break
				}
				if isUploadRateLimited(err) {
					time.Sleep(uploadBackoff(attempt, err))
					lastErr = nil
					continue
				}
				// Fail fast on host-side capacity failures / dead servers.
				if isVidMolyFailFast(err) {
					return "", lastErr
				}
				// Invalid/expired key for THIS key: rotate to the next key.
				if isVidMolyAuthError(err) {
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
		if lastRejection != nil {
			lastErr = fmt.Errorf("VidMoly upload failed: all keys exhausted (last rejection: %v)", lastRejection)
		} else {
			lastErr = fmt.Errorf("VidMoly upload failed: all keys exhausted")
		}
	}
	return "", lastErr
}

// isVidMolyDailyLimit reports whether the error is the free-account daily
// upload cap (~50 files/day). When the cap is hit the host is skipped
// automatically until the next day and never retried within the same window.
// The matcher is deliberately tight (requires "daily" or "per day" wording) so
// a transient 429 "rate limit" is never mislabeled as a day-long quota hit.
func isVidMolyDailyLimit(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return (strings.Contains(msg, "daily") && strings.Contains(msg, "limit")) ||
		strings.Contains(msg, "per day") ||
		strings.Contains(msg, "limit reached for today") ||
		strings.Contains(msg, "today's upload")
}

// isVidMolyAuthError reports whether the error means the VidMoly API key is
// invalid, expired, or the account is not allowed to upload. Explicit
// credential-rejection wording only (mirrors isVidaraAuthError's lesson: broad
// substrings like "forbidden" mislabel site-side blocks as dead keys).
func isVidMolyAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "invalid api key") ||
		strings.Contains(msg, "invalid key") ||
		strings.Contains(msg, "wrong api key") ||
		strings.Contains(msg, "you are not allowed") ||
		strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "unauthorized")
}

// isVidMolyFailFast reports whether the VidMoly error can never succeed on a
// retry today (rejected file, daily cap, rate limit, dead/capacity server).
// Auth errors are NOT fail-fast here — they trigger key rotation in the
// caller's retry loop instead.
func isVidMolyFailFast(err error) bool {
	if err == nil {
		return false
	}
	if isVidMolyDailyLimit(err) || isUploadRateLimited(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "not video file format") ||
		strings.Contains(msg, "not a video") ||
		strings.Contains(msg, "invalid file") ||
		strings.Contains(msg, "file too large") ||
		strings.Contains(msg, "request entity too large") ||
		strings.Contains(msg, "too large") ||
		strings.Contains(msg, "status 500") ||
		strings.Contains(msg, "status 502") ||
		strings.Contains(msg, "status 503") ||
		strings.Contains(msg, "status 504") ||
		strings.Contains(msg, "no healthy upload server")
}

// getUploadServer gets the upload server URL from the VidMoly API
func (u *VidMolyUploader) getUploadServer(key string) (string, error) {
	req, err := http.NewRequest("GET", vidmolyAPIBase+"/api/upload/server?key="+key, nil)
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
		return "", fmt.Errorf("get upload server failed with status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var serverResp vidmolyServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode server response: %w", err)
	}

	if serverResp.Status != 200 || strings.TrimSpace(serverResp.Result) == "" {
		return "", fmt.Errorf("server status not ok: %d (msg: %s)", serverResp.Status, serverResp.Msg)
	}

	return serverResp.Result, nil
}

func (u *VidMolyUploader) uploadFile(filePath, key string, progress ProgressFunc) (string, error) {
	uploadServer, err := u.getUploadServer(key)
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	body, contentLen, contentType, file, err := multipartStreamWithProgress(
		map[string]string{"api_key": key},
		"file", filePath, "VidMoly", progress,
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

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read upload response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(responseBody))
		if len(msg) > 500 {
			msg = msg[:500]
		}
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, msg)
	}

	filecode, statusMsg, ok := vidmolyUploadResult(string(responseBody))
	if ok {
		return vidmolyViewLink(filecode), nil
	}
	return "", fmt.Errorf("upload failed: %s", statusMsg)
}

// vidmolyViewLink builds the canonical view/page link for a VidMoly file code.
func vidmolyViewLink(code string) string {
	return "https://vidmoly.me/" + code
}

// vidmolyFileCode extracts the trailing file code from any VidMoly value,
// which may be a bare code ("yhap17mcn59m"), a view link
// ("https://vidmoly.me/yhap17mcn59m"), or a URL with a query. Everything after
// the last "/" is the code; empty values and non-code segments return "".
func vidmolyFileCode(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimRight(v, "/")
	if v == "" {
		return ""
	}
	if i := strings.IndexAny(v, "?#"); i >= 0 {
		v = v[:i]
	}
	if i := strings.LastIndex(v, "/"); i >= 0 {
		v = v[i+1:]
	}
	if !vidmolyCodeRe.MatchString(v) {
		return ""
	}
	switch v {
	case "v", "e", "embed", "watch", "d", "download", "file", "video":
		return ""
	}
	return v
}

// vidmolyUploadResult parses the HTML form handoff returned by VidMoly's
// upload server. Success is signaled by the "st" textarea being exactly "OK",
// with the new file code in the "fn" textarea. Any other "st" value is the
// human-readable rejection message.
func vidmolyUploadResult(body string) (filecode, statusMsg string, ok bool) {
	st := strings.TrimSpace(html.UnescapeString(matchVidMolyTextarea(body, "st")))
	fn := strings.TrimSpace(html.UnescapeString(matchVidMolyTextarea(body, "fn")))

	if strings.EqualFold(st, "OK") {
		if code := vidmolyFileCode(fn); code != "" {
			return code, "", true
		}
	}
	if st == "" {
		return "", "no upload status in response", false
	}
	// Cap message length so a noisy rejection page never floods the journal.
	if len(st) > 300 {
		st = st[:300]
	}
	return "", st, false
}

func matchVidMolyTextarea(body, name string) string {
	re := vidmolyFnRe
	if name == "st" {
		re = vidmolyStRe
	}
	if m := re.FindStringSubmatch(body); len(m) == 2 {
		return m[1]
	}
	return ""
}