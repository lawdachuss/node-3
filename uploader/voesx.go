package uploader

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// voeSXAPIBase is a var (not const) so tests can point it at a fake server.
var voeSXAPIBase = "https://voe.sx/api"

// errVoeResponseLost marks a failure that happened AFTER the file body was
// committed to the connection, where the server's answer could not be read
// (transport error, or a 2xx whose body would not decode).  That is the one
// case where a retry is not idempotent: VOE.sx may already hold the file, so
// re-streaming the same bytes would create a duplicate upload and charge the
// account twice.  Explicit HTTP rejections (4xx/5xx) are NOT this error — the
// server told us it did not take the file, so retrying them stays safe.
var errVoeResponseLost = errors.New("response lost after the file body was transmitted")

// VoeSXUploader handles uploading files to VOE.sx
type VoeSXUploader struct {
	keys   *keyRing
	client *http.Client
}

// NewVoeSXUploader creates a new VOE.sx uploader instance.  The API key may
// be a comma-separated list ("key1,key2,key3") to enable rotation when a key
// is invalidated or its storage quota is exhausted.  Each instance builds its
// own ring from the passed value / env, so concurrent uploaders each rotate
// independently (the first upload on a bad key rotates it for that instance).
func NewVoeSXUploader(apiKey string) *VoeSXUploader {
	ring := buildRingFromEnv("VOESX_API_KEY", apiKey)
	ring.host = "VoeSX"
	return &VoeSXUploader{
		keys:   ring,
		client: &http.Client{
			Timeout: 120 * time.Minute,
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

type voeSXServerResponse struct {
	ServerTime string `json:"server_time"`
	Msg        string `json:"msg"`
	Message    string `json:"message"`
	Status     int    `json:"status"`
	Success    bool   `json:"success"`
	Result     string `json:"result"`
}

type voeSXUploadResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
	File    struct {
		ID                int    `json:"id"`
		FileCode          string `json:"file_code"`
		FileTitle         string `json:"file_title"`
		EncodingNecessary bool   `json:"encoding_necessary"`
	} `json:"file"`
}

// Upload uploads a file to VOE.sx and returns the view link
func (u *VoeSXUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// UploadWithProgress uploads a file to VOE.sx and reports progress through fn.
func (u *VoeSXUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.keys.count() == 0 {
		return "", fmt.Errorf("VOE.sx API key not configured")
	}

	release, ok := acquireHostSem("VOE.sx")
	if !ok {
		return "", fmt.Errorf("voe.sx: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	// Track whether the whole file body was handed to the transport on this
	// attempt (see the matching guard in vidmoly.go).  VOE.sx is a host we
	// depend on, so this guard is what makes the retry loop idempotent: once the
	// bytes are fully sent, the only thing that can fail is reading the
	// response — and re-streaming the same file would create a duplicate upload
	// and double-count it against the account's allowance.
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
				// The complete body already reached VOE.sx and only the response
				// was lost: re-streaming would duplicate the upload, so surface
				// the error immediately instead of retrying.  Deliberately not a
				// blanket bodyFullySent check — an explicit 429/5xx rejection
				// also happens after the body is written, and that one must still
				// be retried.
				if bodyFullySent && errors.Is(err, errVoeResponseLost) {
					return "", lastErr
				}
				// Storage-full for THIS key is a quota rejection, not a transient
				// one, and must be rotated away from.  Checked BEFORE the generic
				// 429 matcher below (which matches a bare "429") because VOE.sx
				// reports quota errors as 429 — retrying a full account forever
				// is exactly how the daily allowance gets burned (see the
				// VidMoly daily-limit lesson).
				if isVoeStorageFull(err) {
					u.keys.rotateReason("storage quota full")
					lastErr = nil
					break
				}
				if isUploadRateLimited(err) {
					time.Sleep(uploadBackoff(attempt, err))
					lastErr = nil
					continue
				}
				// Invalid auth (expired/revoked key): rotate to the next key and
				// stop retrying the bad one.
				if isVoeAuthError(err) {
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
		lastErr = fmt.Errorf("VOE.sx upload failed: all keys exhausted")
	}
	return "", lastErr
}

// getUploadServer gets the upload server URL from VOE.sx API
func (u *VoeSXUploader) getUploadServer(key string) (string, error) {
	url := fmt.Sprintf("%s/upload/server?key=%s", voeSXAPIBase, key)

	req, err := http.NewRequest("GET", url, nil)
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

	var serverResp voeSXServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode server response: %w", err)
	}

	if !serverResp.Success || serverResp.Status != 200 {
		return "", fmt.Errorf("server status not ok: %s (msg: %s)", serverResp.Msg, serverResp.Message)
	}

	if serverResp.Result == "" {
		return "", fmt.Errorf("no upload server URL in response")
	}

	return serverResp.Result, nil
}

func (u *VoeSXUploader) uploadFile(filePath, key string, progress ProgressFunc) (string, error) {
	// Step 1: Get upload server
	uploadServer, err := u.getUploadServer(key)
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	body, contentLen, contentType, file, err := multipartStreamWithProgress(
		map[string]string{"key": key},
		"file", filePath, "VOE.sx", progress,
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
		return "", fmt.Errorf("%w: do request: %v", errVoeResponseLost, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var uploadResp voeSXUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("%w: decode upload response: %v", errVoeResponseLost, err)
	}

	if !uploadResp.Success {
		return "", fmt.Errorf("upload failed: %s", uploadResp.Message)
	}

	if uploadResp.File.FileCode == "" {
		return "", fmt.Errorf("no file code in response")
	}

	viewURL := fmt.Sprintf("https://voe.sx/%s", uploadResp.File.FileCode)
	return viewURL, nil
}

// isVoeStorageFull returns true when the VOE.sx error indicates the account
// storage is exhausted. This is unrecoverable until the user frees space or
// provides a new API key, so callers should fail immediately instead of retrying.
func isVoeStorageFull(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "storage space") ||
		strings.Contains(msg, "maximum storage") ||
		strings.Contains(msg, "storage full") ||
		strings.Contains(msg, "quota")
}

// isVoeAuthError returns true when the VOE.sx error indicates the API key is
// invalid, expired, or revoked (403 / authentication failures).  A bad key
// will never succeed within a run, so callers should rotate to the next key
// instead of retrying the same one.  Detected as HTTP 403 or auth wording in
// the message body.
func isVoeAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "403") ||
		strings.Contains(msg, "authentication") ||
		strings.Contains(msg, "invalid key") ||
		strings.Contains(msg, "unauthorized") ||
		strings.Contains(msg, "api key") ||
		strings.Contains(msg, "forbidden")
}
