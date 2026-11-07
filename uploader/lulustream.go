package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// luluStreamAPIBase is a var (not const) so tests can point it at a fake server.
var luluStreamAPIBase = "https://lulustream.com/api"

// luluStreamCodeRe is an allowlist for plausible LuluStream file codes. The
// site uses base62-ish alphanumeric codes; allowing "-" and "_" as well keeps
// this permissive without letting a structural path segment through.
var luluStreamCodeRe = regexp.MustCompile(`^[A-Za-z0-9_-]{4,}$`)

// luluStreamViewBase is the public site base used to build canonical view
// links. It is a var for the same reason as the API base.
var luluStreamViewBase = "https://lulustream.com"

// LuluStreamUploader handles uploading files to lulustream.com.
type LuluStreamUploader struct {
	keys   *keyRing
	client *http.Client
}

// NewLuluStreamUploader creates a new LuluStream uploader instance. The API
// key may be a comma-separated list ("key1,key2,key3") to enable rotation when
// a key is invalidated or its storage quota is exhausted.
func NewLuluStreamUploader(apiKey string) *LuluStreamUploader {
	ring := buildRingFromEnv("LULUSTREAM_KEY", apiKey)
	ring.host = "LuluStream"
	return &LuluStreamUploader{
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

type luluStreamServerResponse struct {
	Msg    string          `json:"msg"`
	Status int             `json:"status"`
	Result json.RawMessage `json:"result"`
}

// uploadServer extracts the upload-server URL from the response's result
// field. The documented shape is a bare string, but some deployments wrap it
// in an object, so both are accepted.
func (r luluStreamServerResponse) uploadServer() string {
	var s string
	if json.Unmarshal(r.Result, &s) == nil {
		return strings.TrimSpace(s)
	}
	var obj struct {
		UploadServer string `json:"upload_server"`
		URL          string `json:"url"`
	}
	if json.Unmarshal(r.Result, &obj) == nil {
		if v := strings.TrimSpace(obj.UploadServer); v != "" {
			return v
		}
		return strings.TrimSpace(obj.URL)
	}
	return ""
}

type luluStreamFile struct {
	Filecode string `json:"filecode"`
	Filename string `json:"filename"`
	Status   string `json:"status"`
	Msg      string `json:"msg"`
	URL      string `json:"url"`
}

type luluStreamUploadResponse struct {
	Status int              `json:"status"`
	Msg    string           `json:"msg"`
	Files  []luluStreamFile `json:"files"`
	Result json.RawMessage  `json:"result"`
}

// fileCode returns the first usable file code in the response. The documented
// shape is files[], but Doodstream-style APIs occasionally return result[]
// (or a single result object) instead, so both are normalised here.
func (r luluStreamUploadResponse) fileCode() string {
	for _, f := range r.Files {
		if c := luluStreamCode(f.Filecode); c != "" {
			return c
		}
		if c := luluStreamCode(f.URL); c != "" {
			return c
		}
	}
	if len(r.Result) > 0 {
		var arr []luluStreamFile
		if json.Unmarshal(r.Result, &arr) == nil {
			for _, f := range arr {
				if c := luluStreamCode(f.Filecode); c != "" {
					return c
				}
				if c := luluStreamCode(f.URL); c != "" {
					return c
				}
			}
		}
		var one luluStreamFile
		if json.Unmarshal(r.Result, &one) == nil {
			if c := luluStreamCode(one.Filecode); c != "" {
				return c
			}
			if c := luluStreamCode(one.URL); c != "" {
				return c
			}
		}
		var s string
		if json.Unmarshal(r.Result, &s) == nil {
			return luluStreamCode(s)
		}
	}
	return ""
}

// luluStreamCode extracts a LuluStream file code from a plain code, a view
// link ("https://lulustream.com/CODE.html") or an embed link
// ("https://lulustream.com/e/CODE").
func luluStreamCode(v string) string {
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
	v = strings.TrimSuffix(v, ".html")
	if !luluStreamCodeRe.MatchString(v) {
		return ""
	}
	return v
}

// Upload uploads a file to LuluStream and returns the view link.
func (u *LuluStreamUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to LuluStream and reports progress through
// fn.
func (u *LuluStreamUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.keys.count() == 0 {
		return "", fmt.Errorf("LuluStream API key not configured")
	}

	release, ok := acquireHostSem("LuluStream")
	if !ok {
		return "", fmt.Errorf("lulustream: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	// As with the other key-rotating video hosts, a failure after the whole
	// file body reached the server must not trigger a retry: the upload may
	// already exist, and re-streaming would create a duplicate.
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
				if bodyFullySent {
					return "", lastErr
				}
				if isLuluStreamQuotaError(err) {
					u.keys.rotateReason("storage quota full")
					lastErr = nil
					break
				}
				if isUploadRateLimited(err) {
					time.Sleep(uploadBackoff(attempt, err))
					lastErr = nil
					continue
				}
				if isLuluStreamAuthError(err) {
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
		lastErr = fmt.Errorf("LuluStream upload failed: all keys exhausted")
	}
	return "", lastErr
}

// isLuluStreamAuthError reports whether the error means LuluStream explicitly
// rejected the API key. The wording is deliberately explicit: broad matches
// like a bare "403" would misclassify a CDN/WAF block page as a dead key.
func isLuluStreamAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "invalid key") ||
		strings.Contains(msg, "invalid api key") ||
		strings.Contains(msg, "invalid_api_key") ||
		strings.Contains(msg, "api key is invalid") ||
		strings.Contains(msg, "key is invalid") ||
		strings.Contains(msg, "wrong api key") ||
		strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "unauthorized")
}

// isLuluStreamQuotaError reports whether the account/key is valid but cannot
// accept the file right now (storage full, daily cap). Such a key is rotated
// away from but must never be deleted as dead.
func isLuluStreamQuotaError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "storage") ||
		strings.Contains(msg, "disk") ||
		strings.Contains(msg, "quota") ||
		strings.Contains(msg, "not enough space") ||
		strings.Contains(msg, "no space left") ||
		strings.Contains(msg, "upload limit") ||
		strings.Contains(msg, "limit reached") ||
		strings.Contains(msg, "maximum upload") ||
		strings.Contains(msg, "over capacity")
}

// getUploadServer asks LuluStream which upload node this key should use.
func (u *LuluStreamUploader) getUploadServer(key string) (string, error) {
	req, err := http.NewRequest("GET", luluStreamAPIBase+"/upload/server?key="+url.QueryEscape(key), nil)
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
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return "", fmt.Errorf("get upload server failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var serverResp luluStreamServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode server response: %w", err)
	}

	server := serverResp.uploadServer()
	if serverResp.Status != 200 || server == "" {
		msg := strings.TrimSpace(serverResp.Msg)
		if msg == "" {
			msg = "empty upload server"
		}
		return "", fmt.Errorf("server status not ok: %d (msg: %s)", serverResp.Status, msg)
	}

	return server, nil
}

func (u *LuluStreamUploader) uploadFile(filePath, key string, progress ProgressFunc) (string, error) {
	uploadServer, err := u.getUploadServer(key)
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	body, contentLen, contentType, file, err := multipartStreamWithProgress(
		map[string]string{"key": key},
		"file", filePath, "LuluStream", progress,
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
	req.Header.Set("Referer", luluStreamViewBase+"/")
	req.ContentLength = contentLen

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var uploadResp luluStreamUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}

	code := uploadResp.fileCode()
	if code == "" {
		msg := strings.TrimSpace(uploadResp.Msg)
		if msg == "" {
			msg = "no file code in response"
		}
		return "", fmt.Errorf("upload rejected: status %d (msg: %s)", uploadResp.Status, msg)
	}
	if uploadResp.Status != 200 {
		msg := strings.TrimSpace(uploadResp.Msg)
		if msg == "" {
			msg = "unexpected status"
		}
		return "", fmt.Errorf("upload failed: status %d (msg: %s)", uploadResp.Status, msg)
	}

	return luluStreamViewBase + "/" + code + ".html", nil
}
