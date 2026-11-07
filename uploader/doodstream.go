package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// doodStreamAPIBase is a var (not const) so tests can point it at a fake server.
var doodStreamAPIBase = "https://doodapi.co/api"

// doodStreamViewBase is the public site base used to build canonical embed
// links. It is a var for the same reason as the API base.
var doodStreamViewBase = "https://dood.to"

// doodStreamCodeRe is an allowlist for plausible DoodStream file codes. The
// site uses base62-ish alphanumeric codes; allowing "-" and "_" as well keeps
// this permissive without letting a structural path segment through.
var doodStreamCodeRe = regexp.MustCompile(`^[A-Za-z0-9_-]{4,}$`)

// DoodStreamUploader handles uploading files to doodstream.com.
type DoodStreamUploader struct {
	keys   *keyRing
	client *http.Client
}

// NewDoodStreamUploader creates a new DoodStream uploader instance. The API
// key may be a comma-separated list ("key1,key2,key3") to enable rotation when
// a key is invalidated or its storage quota is exhausted.
func NewDoodStreamUploader(apiKey string) *DoodStreamUploader {
	ring := buildRingFromEnv("DOODSTREAM_API_KEY", apiKey)
	if ring.count() == 0 {
		// Older deployments (and docs/examples) used DOODSTREAM_KEY.
		ring = buildRingFromEnv("DOODSTREAM_KEY", apiKey)
	}
	ring.host = "DoodStream"
	return &DoodStreamUploader{
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

// doodStreamInt accepts both numeric and quoted status values. DoodStream's
// docs show numeric statuses, but its rate-limit response uses "429" as a
// string; without this a decoding error would hide the real API verdict.
type doodStreamInt int

func (n *doodStreamInt) UnmarshalJSON(b []byte) error {
	s := strings.TrimSpace(string(b))
	s = strings.Trim(s, `"`)
	if s == "" || s == "null" {
		*n = 0
		return nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return err
	}
	*n = doodStreamInt(v)
	return nil
}

type doodStreamServerResponse struct {
	Msg    string          `json:"msg"`
	Status doodStreamInt   `json:"status"`
	Result json.RawMessage `json:"result"`
}

// uploadServer extracts the upload-server URL from the response's result
// field. The documented shape is a bare string, but some deployments wrap it
// in an object, so both are accepted.
func (r doodStreamServerResponse) uploadServer() string {
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

type doodStreamFile struct {
	Filecode       string        `json:"filecode"`
	FileCode       string        `json:"file_code"`
	Filename       string        `json:"filename"`
	Title          string        `json:"title"`
	Status         doodStreamInt `json:"status"`
	Msg            string        `json:"msg"`
	URL            string        `json:"url"`
	DownloadURL    string        `json:"download_url"`
	ProtectedEmbed string        `json:"protected_embed"`
	ProtectedDL    string        `json:"protected_dl"`
}

type doodStreamUploadResponse struct {
	Status doodStreamInt    `json:"status"`
	Msg    string           `json:"msg"`
	Files  []doodStreamFile `json:"files"`
	Result json.RawMessage  `json:"result"`
}

// fileCode returns the first usable file code in the response. The documented
// shape is result[], but some deployments (and the older API) return files[],
// a single result object, or a bare string, so all are normalised here.
func (r doodStreamUploadResponse) fileCode() string {
	fromFile := func(f doodStreamFile) string {
		for _, v := range []string{f.Filecode, f.FileCode, f.ProtectedEmbed, f.URL, f.DownloadURL, f.ProtectedDL} {
			if c := doodStreamCode(v); c != "" {
				return c
			}
		}
		return ""
	}
	for _, f := range r.Files {
		if c := fromFile(f); c != "" {
			return c
		}
	}
	if len(r.Result) > 0 {
		var arr []doodStreamFile
		if json.Unmarshal(r.Result, &arr) == nil {
			for _, f := range arr {
				if c := fromFile(f); c != "" {
					return c
				}
			}
		}
		var one doodStreamFile
		if json.Unmarshal(r.Result, &one) == nil {
			if c := fromFile(one); c != "" {
				return c
			}
		}
		var s string
		if json.Unmarshal(r.Result, &s) == nil {
			return doodStreamCode(s)
		}
	}
	return ""
}

// doodStreamCode extracts a DoodStream file code from a plain code, an embed
// link ("https://dood.to/e/CODE"), a download link (".../d/CODE"), or a
// protected link.
func doodStreamCode(v string) string {
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
	if !doodStreamCodeRe.MatchString(v) {
		return ""
	}
	return v
}

// Upload uploads a file to DoodStream and returns the embed link.
func (u *DoodStreamUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to DoodStream and reports progress through
// fn.
func (u *DoodStreamUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.keys.count() == 0 {
		return "", fmt.Errorf("DoodStream API key not configured")
	}

	release, ok := acquireHostSem("DoodStream")
	if !ok {
		return "", fmt.Errorf("doodstream: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
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
				if isDoodStreamQuotaError(err) {
					u.keys.rotateReason("storage quota full")
					lastErr = nil
					break
				}
				if isUploadRateLimited(err) {
					time.Sleep(uploadBackoff(attempt, err))
					lastErr = nil
					continue
				}
				if isDoodStreamAuthError(err) {
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
		lastErr = fmt.Errorf("DoodStream upload failed: all keys exhausted")
	}
	return "", lastErr
}

// isDoodStreamAuthError reports whether the error means DoodStream explicitly
// rejected the API key. The wording is deliberately explicit: broad matches
// like a bare "403" would misclassify a CDN/WAF block page as a dead key.
func isDoodStreamAuthError(err error) bool {
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

// isDoodStreamQuotaError reports whether the account/key is valid but cannot
// accept the file right now (storage full, daily cap). Such a key is rotated
// away from but must never be deleted as dead.
func isDoodStreamQuotaError(err error) bool {
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

// getUploadServer asks DoodStream which upload node this key should use.
func (u *DoodStreamUploader) getUploadServer(key string) (string, error) {
	req, err := http.NewRequest("GET", doodStreamAPIBase+"/upload/server?key="+url.QueryEscape(key), nil)
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

	var serverResp doodStreamServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode server response: %w", err)
	}

	server := serverResp.uploadServer()
	if int(serverResp.Status) != 200 || server == "" {
		msg := strings.TrimSpace(serverResp.Msg)
		if msg == "" {
			msg = "empty upload server"
		}
		return "", fmt.Errorf("server status not ok: %d (msg: %s)", int(serverResp.Status), msg)
	}

	return server, nil
}

func (u *DoodStreamUploader) uploadFile(filePath, key string, progress ProgressFunc) (string, error) {
	uploadServer, err := u.getUploadServer(key)
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	// DoodStream documents the multipart field as api_key and also shows the
	// key appended to the upload URL; send both so every deployment accepts it.
	sep := "?"
	if strings.Contains(uploadServer, "?") {
		sep = "&"
	}
	uploadURL := uploadServer + sep + "api_key=" + url.QueryEscape(key)

	body, contentLen, contentType, file, err := multipartStreamWithProgress(
		map[string]string{"api_key": key},
		"file", filePath, "DoodStream", progress,
	)
	if err != nil {
		return "", fmt.Errorf("multipart stream: %w", err)
	}
	defer file.Close()

	req, err := http.NewRequest("POST", uploadURL, body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Referer", doodStreamViewBase+"/")
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

	var uploadResp doodStreamUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}

	code := uploadResp.fileCode()
	if code == "" {
		msg := strings.TrimSpace(uploadResp.Msg)
		if msg == "" {
			msg = "no file code in response"
		}
		return "", fmt.Errorf("upload rejected: status %d (msg: %s)", int(uploadResp.Status), msg)
	}
	if int(uploadResp.Status) != 200 {
		msg := strings.TrimSpace(uploadResp.Msg)
		if msg == "" {
			msg = "unexpected status"
		}
		return "", fmt.Errorf("upload failed: status %d (msg: %s)", int(uploadResp.Status), msg)
	}

	return doodStreamViewBase + "/e/" + code, nil
}
