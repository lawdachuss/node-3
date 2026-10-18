package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ProbeResult is the outcome of a cheap credential check against an upload
// host's API — a GET of the same "get upload server" endpoint the upload flow
// calls first, which every keyed video host exposes and which costs no upload
// bandwidth.  OK means the key works; Rejected means the host EXPLICITLY
// refused the credential (dead/revoked key — safe to show as "dead"); neither
// set means the answer was ambiguous (transport error, host down, WAF page)
// and the key must NOT be written off.
type ProbeResult struct {
	OK       bool   `json:"ok"`
	Rejected bool   `json:"rejected"`
	Message  string `json:"message,omitempty"`
}

// probeHTTPClient is a short-timeout client for credential probes. Probes run
// on the admin path and the background prober, where a hung host must not
// stall the whole sweep.
var probeHTTPClient = &http.Client{Timeout: 20 * time.Second}

// probeGet performs the probe GET and returns status code + raw body.
func probeGet(rawURL string) (int, []byte, error) {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	resp, err := probeHTTPClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
	return resp.StatusCode, body, nil
}

// ProbeVoeSX checks one VOE.sx API key via GET /upload/server.
func ProbeVoeSX(apiKey string) ProbeResult {
	status, body, err := probeGet(fmt.Sprintf("%s/upload/server?key=%s", voeSXAPIBase, url.QueryEscape(apiKey)))
	if err != nil {
		return ProbeResult{Message: err.Error()}
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return authVerdict(status, body)
	}
	if status != http.StatusOK {
		return ProbeResult{Message: fmt.Sprintf("HTTP %d: %s", status, snippet(body))}
	}
	var r voeSXServerResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return ProbeResult{Message: fmt.Sprintf("decode: %v", err)}
	}
	if r.Success {
		return ProbeResult{OK: true}
	}
	msg := strings.TrimSpace(r.Msg)
	if msg == "" {
		msg = strings.TrimSpace(r.Message)
	}
	if msg == "" {
		msg = snippet(body)
	}
	probeErr := fmt.Errorf("VOE.sx: %s", msg)
	// Quota first, mirroring the upload flow: a storage-full message can also
	// contain "api key" wording that the broad auth matcher would otherwise
	// misclassify as a dead key.
	if isVoeStorageFull(probeErr) {
		// The key authenticates fine; the account is simply full. Not dead.
		return ProbeResult{OK: true, Message: "storage quota full: " + msg}
	}
	if isVoeAuthError(probeErr) {
		return ProbeResult{Rejected: true, Message: msg}
	}
	return ProbeResult{Message: msg}
}

// ProbeVidara checks one Vidara API key via GET /upload/server.
func ProbeVidara(apiKey string) ProbeResult {
	status, body, err := probeGet(fmt.Sprintf("%s/upload/server?api_key=%s", vidaraAPIBase, url.QueryEscape(apiKey)))
	if err != nil {
		return ProbeResult{Message: err.Error()}
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return authVerdict(status, body)
	}
	if status != http.StatusOK {
		return ProbeResult{Message: fmt.Sprintf("HTTP %d: %s", status, snippet(body))}
	}
	var r vidaraServerResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return ProbeResult{Message: fmt.Sprintf("decode: %v", err)}
	}
	if r.Status == 200 && strings.TrimSpace(r.Result.UploadServer) != "" {
		return ProbeResult{OK: true}
	}
	msg := strings.TrimSpace(r.Msg)
	if msg == "" {
		msg = snippet(body)
	}
	if isVidaraAuthError(fmt.Errorf("Vidara: %s", msg)) {
		return ProbeResult{Rejected: true, Message: msg}
	}
	return ProbeResult{Message: msg}
}

// ProbeVidMoly checks one VidMoly API key via GET /api/upload/server.  NOTE:
// this call counts against VidMoly's ~50 requests/day account cap, so the
// background prober must only run it when the stored status is stale.
func ProbeVidMoly(apiKey string) ProbeResult {
	status, body, err := probeGet(fmt.Sprintf("%s/api/upload/server?key=%s", vidmolyAPIBase, url.QueryEscape(apiKey)))
	if err != nil {
		return ProbeResult{Message: err.Error()}
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return authVerdict(status, body)
	}
	if status != http.StatusOK {
		return ProbeResult{Message: fmt.Sprintf("HTTP %d: %s", status, snippet(body))}
	}
	var r vidmolyServerResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return ProbeResult{Message: fmt.Sprintf("decode: %v", err)}
	}
	if r.Status == 200 && strings.TrimSpace(r.Result) != "" {
		return ProbeResult{OK: true}
	}
	msg := strings.TrimSpace(r.Msg)
	if msg == "" {
		msg = snippet(body)
	}
	probeErr := fmt.Errorf("VidMoly: %s", msg)
	// Daily-limit first, mirroring the upload flow: a valid key whose budget
	// is spent must never be shown as dead.
	if isVidMolyDailyLimit(probeErr) {
		return ProbeResult{OK: true, Message: "daily api limit: " + msg}
	}
	if isVidMolyAuthError(probeErr) {
		return ProbeResult{Rejected: true, Message: msg}
	}
	return ProbeResult{Message: msg}
}

// ProbeStreamtape checks one Streamtape login/key pair via GET /file/ul — the
// same call the upload flow makes to obtain an upload URL.
func ProbeStreamtape(login, key string) ProbeResult {
	status, body, err := probeGet(fmt.Sprintf("%s/file/ul?login=%s&key=%s", streamtapeAPIBase, url.QueryEscape(login), url.QueryEscape(key)))
	if err != nil {
		return ProbeResult{Message: err.Error()}
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return authVerdict(status, body)
	}
	if status != http.StatusOK {
		return ProbeResult{Message: fmt.Sprintf("HTTP %d: %s", status, snippet(body))}
	}
	var r streamtapeServerResp
	if err := json.Unmarshal(body, &r); err != nil {
		return ProbeResult{Message: fmt.Sprintf("decode: %v", err)}
	}
	if r.Status == 200 && strings.TrimSpace(r.Result.URL) != "" {
		return ProbeResult{OK: true}
	}
	msg := strings.TrimSpace(r.Msg)
	if msg == "" {
		msg = snippet(body)
	}
	if isUploadAuthError(fmt.Errorf("streamtape: %s", msg)) {
		return ProbeResult{Rejected: true, Message: msg}
	}
	return ProbeResult{Message: msg}
}

// looksLikeBlockPage reports whether an HTTP 401/403 response is a WAF/CDN
// interstitial (Cloudflare challenge, geo-block, rate limit page) rather than
// the API itself rejecting the credential. Those block pages return 403 just
// like a dead key would — treating one as "Rejected" would flip every good key
// on the node to dead during a Cloudflare episode, so it must classify as
// undecided instead.
func looksLikeBlockPage(status int, body []byte) bool {
	if status != http.StatusUnauthorized && status != http.StatusForbidden {
		return false
	}
	low := strings.ToLower(string(body))
	return strings.Contains(low, "<html") ||
		strings.Contains(low, "cloudflare") ||
		strings.Contains(low, "cf-ray") ||
		strings.Contains(low, "just a moment")
}

// authVerdict classifies a 401/403 probe response: Rejected when the body is
// the API's own (non-HTML) answer, undecided when a block page intercepted the
// request.
func authVerdict(status int, body []byte) ProbeResult {
	if looksLikeBlockPage(status, body) {
		return ProbeResult{Message: fmt.Sprintf("HTTP %d (possible WAF block): %s", status, snippet(body))}
	}
	return ProbeResult{Rejected: true, Message: fmt.Sprintf("HTTP %d", status)}
}

// snippet trims a probe response body down to something safe to show on the
// admin page (and to classify errors against).
func snippet(body []byte) string {
	s := strings.TrimSpace(string(body))
	s = strings.Join(strings.Fields(s), " ")
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}
