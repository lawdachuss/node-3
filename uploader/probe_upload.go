package uploader

import (
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// probeUploadClient is the HTTP client used by the upload-round-trip checks.
// It is separate from probeHTTPClient because a real upload (even of a ~2 KB
// file) legitimately takes longer than a metadata GET, while still being
// bounded so a hung host cannot stall the admin path.
var probeUploadClient = &http.Client{Timeout: 90 * time.Second}

// writeProbeVideo materializes the embedded clip in a private temp directory
// and returns its path plus a cleanup function. The directory is removed after
// the probe, so a crashed probe cannot leave temp litter behind.
func writeProbeVideo() (string, func(), error) {
	data, err := base64.StdEncoding.DecodeString(probeVideoBase64)
	if err != nil {
		return "", func() {}, fmt.Errorf("decode probe video: %w", err)
	}
	dir, err := os.MkdirTemp("", "uploadkey-probe-")
	if err != nil {
		return "", func() {}, fmt.Errorf("probe temp dir: %w", err)
	}
	cleanup := func() { os.RemoveAll(dir) }
	path := filepath.Join(dir, "uploadkey-probe.mp4")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		cleanup()
		return "", func() {}, fmt.Errorf("write probe video: %w", err)
	}
	return path, cleanup, nil
}

// probeErrText flattens an upload error into a single bounded line suitable
// for the admin page's Detail column (host bodies can be huge and multi-line).
func probeErrText(err error) string {
	if err == nil {
		return ""
	}
	s := strings.Join(strings.Fields(err.Error()), " ")
	if len(s) > 220 {
		s = s[:220] + "…"
	}
	return s
}

// lastPathSegment returns the trailing path segment of a link (the host file
// code/ID), ignoring query strings and fragments.
func lastPathSegment(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimRight(raw, "/")
	if i := strings.IndexAny(raw, "?#"); i >= 0 {
		raw = raw[:i]
	}
	if i := strings.LastIndex(raw, "/"); i >= 0 {
		raw = raw[i+1:]
	}
	return raw
}

// looksLikeBlockPageText reports whether an error/message body is a WAF/CDN
// interstitial rather than the host's own API verdict. Such a response must
// never mark a key dead (see looksLikeBlockPage).
func looksLikeBlockPageText(s string) bool {
	low := strings.ToLower(s)
	return strings.Contains(low, "<html") ||
		strings.Contains(low, "cloudflare") ||
		strings.Contains(low, "cf-ray") ||
		strings.Contains(low, "just a moment")
}

// isAuthHTTPStatus reports a plain 401/403 rejection from the upload endpoint.
// Block pages are excluded: a WAF answers 403 for good keys too.
func isAuthHTTPStatus(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	if looksLikeBlockPageText(msg) {
		return false
	}
	low := strings.ToLower(msg)
	return strings.Contains(low, "status 401") ||
		strings.Contains(low, "status 403") ||
		strings.Contains(low, "http 401") ||
		strings.Contains(low, "http 403")
}

// isDailyLimitError is the host-agnostic "cap hit for today" matcher (Vidara
// reports its cap with different wording than VidMoly).
func isDailyLimitError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return (strings.Contains(msg, "daily") && strings.Contains(msg, "limit")) ||
		strings.Contains(msg, "per day") ||
		strings.Contains(msg, "limit reached for today") ||
		strings.Contains(msg, "today's upload")
}

// deleteProbeUpload removes the tiny test file from the host after a
// successful round-trip. Best-effort by design: a failed cleanup never changes
// the verdict, and each probe file is ~2 KB with a recognizable name so a
// missed delete is easy to spot and remove by hand.
func deleteProbeUpload(host, secret, login, link string) {
	code := lastPathSegment(link)
	if code == "" {
		return
	}
	var endpoint string
	switch host {
	case "VoeSX":
		endpoint = fmt.Sprintf("%s/file/delete?key=%s&del_code=%s", voeSXAPIBase, url.QueryEscape(secret), url.QueryEscape(code))
	case "Vidara":
		endpoint = fmt.Sprintf("%s/video/delete?api_key=%s&filecode=%s", vidaraAPIBase, url.QueryEscape(secret), url.QueryEscape(code))
	case "VidMoly":
		endpoint = fmt.Sprintf("%s/api/file/delete?key=%s&file_code=%s", vidmolyAPIBase, url.QueryEscape(secret), url.QueryEscape(code))
	case "Streamtape":
		endpoint = fmt.Sprintf("%s/file/delete?login=%s&key=%s&file=%s", streamtapeAPIBase, url.QueryEscape(login), url.QueryEscape(secret), url.QueryEscape(code))
	default:
		return
	}
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	resp, err := probeUploadClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
}

// VerifyVoeSX is a genuine upload round-trip check for one VOE.sx key: the
// cheap account check must pass, then the embedded clip is POSTed to the real
// upload server and deleted again. Only a successful upload certifies the key
// as healthy — a key that authenticates but cannot accept files is not healthy.
func VerifyVoeSX(apiKey string) ProbeResult {
	if res := ProbeVoeSX(apiKey); !res.OK || res.Rejected || res.Quota {
		return res
	}
	file, cleanup, err := writeProbeVideo()
	if err != nil {
		return ProbeResult{Message: err.Error()}
	}
	defer cleanup()

	u := &VoeSXUploader{client: probeUploadClient}
	link, err := u.uploadFile(file, apiKey, nil)
	if err != nil {
		msg := probeErrText(err)
		switch {
		case isVoeStorageFull(err):
			return ProbeResult{Quota: true, Message: "storage quota full: " + msg}
		case isVoeAuthError(err) && !looksLikeBlockPageText(msg):
			return ProbeResult{Rejected: true, Message: msg}
		default:
			return ProbeResult{Message: msg}
		}
	}
	deleteProbeUpload("VoeSX", apiKey, "", link)
	return ProbeResult{OK: true, Message: "upload verified"}
}

// VerifyVidara is the Vidara equivalent of VerifyVoeSX.
func VerifyVidara(apiKey string) ProbeResult {
	if res := ProbeVidara(apiKey); !res.OK || res.Rejected || res.Quota {
		return res
	}
	file, cleanup, err := writeProbeVideo()
	if err != nil {
		return ProbeResult{Message: err.Error()}
	}
	defer cleanup()

	u := &VidaraUploader{client: probeUploadClient}
	link, err := u.uploadFile(file, apiKey, nil)
	if err != nil {
		msg := probeErrText(err)
		switch {
		case isDailyLimitError(err):
			return ProbeResult{Quota: true, Message: "daily upload limit: " + msg}
		case isVidaraAuthError(err) || isAuthHTTPStatus(err):
			return ProbeResult{Rejected: true, Message: msg}
		default:
			return ProbeResult{Message: msg}
		}
	}
	deleteProbeUpload("Vidara", apiKey, "", link)
	return ProbeResult{OK: true, Message: "upload verified"}
}

// VerifyVidMoly is the VidMoly equivalent of VerifyVoeSX. Its upload also
// counts against the ~50 requests/day account cap, which is exactly why the
// daily-limit verdict is surfaced as Quota (not dead, and not healthy).
func VerifyVidMoly(apiKey string) ProbeResult {
	if res := ProbeVidMoly(apiKey); !res.OK || res.Rejected || res.Quota {
		return res
	}
	file, cleanup, err := writeProbeVideo()
	if err != nil {
		return ProbeResult{Message: err.Error()}
	}
	defer cleanup()

	u := &VidMolyUploader{client: probeUploadClient}
	link, err := u.uploadFile(file, apiKey, nil)
	if err != nil {
		msg := probeErrText(err)
		switch {
		case isVidMolyDailyLimit(err):
			return ProbeResult{Quota: true, Message: "daily api limit: " + msg}
		case isVidMolyAuthError(err) || isAuthHTTPStatus(err):
			return ProbeResult{Rejected: true, Message: msg}
		default:
			return ProbeResult{Message: msg}
		}
	}
	deleteProbeUpload("VidMoly", apiKey, "", link)
	return ProbeResult{OK: true, Message: "upload verified"}
}

// VerifyStreamtape is the Streamtape equivalent of VerifyVoeSX: obtain an
// upload URL with the explicit login/key pair, POST the clip, then delete it.
func VerifyStreamtape(login, key string) ProbeResult {
	if res := ProbeStreamtape(login, key); !res.OK || res.Rejected || res.Quota {
		return res
	}
	file, cleanup, err := writeProbeVideo()
	if err != nil {
		return ProbeResult{Message: err.Error()}
	}
	defer cleanup()

	u := &StreamtapeUploader{creds: newKeyRing(login + ":" + key), client: probeUploadClient}
	uploadURL, err := u.getUploadURL()
	if err != nil {
		msg := probeErrText(err)
		if isUploadAuthError(err) || isAuthHTTPStatus(err) {
			return ProbeResult{Rejected: true, Message: msg}
		}
		return ProbeResult{Message: msg}
	}
	link, err := u.uploadFile(file, uploadURL, nil)
	if err != nil {
		msg := probeErrText(err)
		if isUploadAuthError(err) || isAuthHTTPStatus(err) {
			return ProbeResult{Rejected: true, Message: msg}
		}
		return ProbeResult{Message: msg}
	}
	deleteProbeUpload("Streamtape", key, login, link)
	return ProbeResult{OK: true, Message: "upload verified"}
}
