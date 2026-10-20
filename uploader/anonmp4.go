package uploader

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// AnonMP4 upload flow (as of Sep 2026).
//
// The long-documented API base (https://anonmp4api.xyz, a direct IONOS IP at
// 74.208.192.119) has been unreachable since ~Aug 2026 — TCP 443 silently
// drops from every fleet node — while the public site anonmp4.to keeps
// working behind Cloudflare. The site's own uploader no longer uses the
// documented API either; it assigns a signed upload node and streams the file
// in 2 MiB chunks to a Cloudflare Worker:
//
//  1. POST anonmp4.to/get-upload-node   {client_token, filename}
//     -> {status, node_id, upload_url, node_token}
//  2. POST upload_url?file_id=<node_token>&chunkIndex=&totalChunks=&filename=
//     with raw binary chunk bodies + Content-Range headers, up to 5 in
//     parallel (2 MiB chunks). The Worker answers 201 Created (200 on the
//     final chunk) with a text/plain cumulative bytes-received ack.
//  3. POST anonmp4.to/get-video-node    {node_id, node_token, client_token, filename}
//     -> final watch/embed links
//
// client_token is a static value embedded in the public site (no account or
// API key required — uploads are anonymous, matching the old API contract).
// anonmp4SiteBase is a var (not const) so tests can point it at an httptest
// server.
var anonmp4SiteBase = "https://anonmp4.to"

// anonmp4ViewBase is the base URL that watch/embed links returned by the API
// are rewritten to. The site rotates its public view domain (anonmp4.to →
// anonmp4.help → anonmp4.art), and in Sep 2026 it started answering
// /get-video-node with links on anonmp4.art while the videos still only
// rendered on anonmp4.help (anonmp4.to hard-404'd, anonmp4.art refused/bot-
// challenged connections from every vantage). Archive links on any known
// view domain are normalized to this base so stored URLs keep working after
// the API drifts. Overridable via ANONMP4_VIEW_BASE.
var anonmp4ViewBase = "https://anonmp4.help"

// anonmp4ViewDomains are the public view domains that may appear in finalize
// links. A link whose host is one of these is rewritten to anonmp4ViewBase;
// unknown hosts pass through untouched (assumed to be the current working
// domain).
var anonmp4ViewDomains = []string{"anonmp4.to", "anonmp4.help", "anonmp4.art"}

// SetAnonMP4ViewBase overrides the domain archive links are normalized to
// (wired from ANONMP4_VIEW_BASE). An empty/invalid value restores the default.
func SetAnonMP4ViewBase(base string) {
	base = strings.TrimSpace(base)
	if base != "" {
		if u, err := url.Parse(base); err == nil && u.Host != "" {
			anonmp4ViewBase = strings.TrimSuffix(base, "/")
			return
		}
	}
	anonmp4ViewBase = "https://anonmp4.help"
}

// normalizeAnonMP4Link rewrites a watch/embed URL on one of the known anonmp4
// view domains to anonmp4ViewBase (same path/query), so a domain the site
// stopped serving never poisons the archive. Non-anonmp4 links are returned
// unchanged.
func normalizeAnonMP4Link(link string) string {
	if link == "" {
		return link
	}
	u, err := url.Parse(link)
	if err != nil || u.Host == "" {
		return link
	}
	host := strings.ToLower(u.Host)
	for _, d := range anonmp4ViewDomains {
		if host == d {
			return anonmp4ViewBase + u.RequestURI()
		}
	}
	return link
}

const (
	// anonmp4ClientToken mirrors the token the anonmp4.to homepage embeds in
	// its upload JS. If the site ever rotates it, uploads fail with a clear
	// node-assignment error and the value needs updating here.
	anonmp4ClientToken  = "TVRBYTRMcTBFNE5TNGI1TVR0M2Y5T1RWOE1UdDMzTVRZYTROVHg3M01kNw"
	anonmp4ChunkSize    = 2 * 1024 * 1024 // the site uploads 2 MiB chunks
	anonmp4MaxParallel  = 3               // site's per-batch chunk parallelism, reduced to avoid large-chunk zero-byte rejects
	anonmp4MaxAttempts  = 4               // outer attempts (full node cycle)
	anonmp4MaxChunkHits = 5               // per-chunk retries
	anonmp4ChunkBackoff = 2 * time.Second // base backoff between chunk retries
	// uploadBackoffChunk lives here so the chunk-retry path has a gentler per-chunk
	// pause than the host-level fail-fast path. The node_url is shared across the whole
	// parallel batch, so aggressive backoff is less useful than a steady pause.
)

// AnonMP4Uploader handles uploading videos to anonmp4.to
type AnonMP4Uploader struct {
	client *http.Client
}

// NewAnonMP4Uploader creates a new AnonMP4 uploader instance
func NewAnonMP4Uploader() *AnonMP4Uploader {
	return &AnonMP4Uploader{
		client: &http.Client{
			Timeout: 120 * time.Minute, // Long timeout for large video uploads
			Transport: &http.Transport{
				MaxIdleConns:          100,
				MaxIdleConnsPerHost:   100,
				IdleConnTimeout:       90 * time.Second,
				DisableCompression:    true,
				TLSHandshakeTimeout:   30 * time.Second,
				ResponseHeaderTimeout: 90 * time.Second,
				DialContext:           (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

// anonmp4NodeResponse is the JSON returned by /get-upload-node
type anonmp4NodeResponse struct {
	Status    bool   `json:"status"`
	NodeID    string `json:"node_id"`
	UploadURL string `json:"upload_url"`
	NodeToken string `json:"node_token"`
	Message   string `json:"message"`
}

// anonmp4RejectedError marks a functional rejection (bad client_token, non-
// success status, 4xx) that retrying seconds later cannot fix — the caller
// should fail fast and let the host fallback chain move on.
type anonmp4RejectedError struct {
	msg string
}

func (e *anonmp4RejectedError) Error() string { return e.msg }

// anonmp4UploadResponse is the JSON returned by /get-video-node (final links).
// Mirrors the shape the old API documented; newer endpoints may answer with
// "status":"success" instead of "success":true, so both are tolerated.
type anonmp4UploadResponse struct {
	Success    bool   `json:"success"`
	Status     string `json:"status"`
	VideoID    string `json:"video_id"`
	Title      string `json:"title"`
	Thumbnail  string `json:"thumbnail"`
	WatchURL   string `json:"watch_url"`
	EmbedURL   string `json:"embed_url"`
	DeleteURL  string `json:"delete_url"`
	UploadDate string `json:"upload_date"`
	Message    string `json:"message"`
	Error      *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// Upload uploads a video file to AnonMP4 and returns the embed link
func (u *AnonMP4Uploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

// isAnonMP4OriginDown reports whether the error is Cloudflare's HTTP 522
// ("connection timed out" — the origin didn't answer the edge in time) at any
// stage of the AnonMP4 flow: node assignment (the observed fleet site,
// "anonmp4: node assignment: HTTP 522: error code: 522"), chunk upload, or
// finalize — all three emit the same "HTTP %d: %s" shape.  Deliberately tight
// on 522 alone: a 500/503/504 is a different failure class (server bug /
// overload / edge timeout) and must never count toward the strike that
// disables the host for the rest of the UTC day.
func isAnonMP4OriginDown(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "http 522")
}

// anonmp4522MaxStrikes is how many origin-522 observations in one UTC day
// flip the fleet-wide disable.  A single 522 can be a transient edge hiccup
// (retry within the file's attempts), but repeated ones mean the origin is
// down — and since the credential/endpoint is shared by every node, that is a
// fleet cap, not a per-node one (the VidMoly daily-cap bug, findings #3, all
// over again: without the day lease every node rediscovers the dead origin
// file after file).
const anonmp4522MaxStrikes = 3

var (
	anonmp4StrikeMu    sync.Mutex
	anonmp4StrikeDay   string // UTC yyyy-mm-dd the counter below belongs to
	anonmp4StrikeCount int
	anonmp4StrikeFired bool // the disable already fired for anonmp4StrikeDay
)

// recordAnonMP4OriginDown counts one observed origin-522 failure against the
// current UTC day.  On the strike that reaches anonmp4522MaxStrikes it flips
// the fleet-wide disable exactly once (disableHostFor → upload_host_backoffs,
// 5-minute peer poll) with an expiry at the next UTC midnight, and reports
// whether THIS call fired it.  The day rolls over automatically.
func recordAnonMP4OriginDown() (strikes int, disabled bool) {
	now := time.Now().UTC()
	day := now.Format("2006-01-02")

	anonmp4StrikeMu.Lock()
	if anonmp4StrikeDay != day {
		anonmp4StrikeDay = day
		anonmp4StrikeCount = 0
		anonmp4StrikeFired = false
	}
	anonmp4StrikeCount++
	strikes = anonmp4StrikeCount
	disabled = strikes >= anonmp4522MaxStrikes && !anonmp4StrikeFired
	if disabled {
		anonmp4StrikeFired = true
	}
	anonmp4StrikeMu.Unlock()

	if disabled {
		nextMidnight := now.Truncate(24 * time.Hour).Add(24 * time.Hour)
		disableHostFor("AnonMP4", time.Until(nextMidnight), "origin 522 (upstream outage)")
	}
	return strikes, disabled
}

// resetAnonMP4522StrikesForTest zeroes the strike counter and day key (test
// hook — the counter is package-level state shared across tests).
func resetAnonMP4522StrikesForTest() {
	anonmp4StrikeMu.Lock()
	defer anonmp4StrikeMu.Unlock()
	anonmp4StrikeDay, anonmp4StrikeCount, anonmp4StrikeFired = "", 0, false
}

// UploadWithProgress uploads a video file to AnonMP4 and reports progress
// through fn (called with host "AnonMP4" and cumulative bytes).
func (u *AnonMP4Uploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	release, ok := acquireHostSem("AnonMP4")
	if !ok {
		return "", fmt.Errorf("anonmp4: upload slot busy — host saturated, skipped this attempt (deadline exceeded)")
	}
	defer release()

	fileName := filepath.Base(filePath)
	fi, err := os.Stat(filePath)
	if err != nil {
		return "", fmt.Errorf("anonmp4: stat file: %w", err)
	}
	if fi.Size() == 0 {
		return "", fmt.Errorf("anonmp4: refusing to upload empty file %s", fileName)
	}

	var lastErr error
	for attempt := 1; attempt <= anonmp4MaxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}
		// NOTE: no host-level retry loop is needed here any more — a chunk
		// upload is retried only for genuinely transient errors
		// (uploadChunkWithRetries) and everything else fails fast via
		// isFailFastError / anonmp4RejectedError below.

		link, err := u.uploadFlow(filePath, fileName, fi.Size(), progress)
		if err == nil {
			return link, nil
		}
		lastErr = err

		// A 522 is Cloudflare for "the origin never answered in time" — an
		// upstream outage, not our request.  A single one may be transient
		// (the loop retries within this file's attempts), but every
		// observation is a strike; the 3rd strike in a UTC day disables
		// AnonMP4 fleet-wide until midnight so the other 17 nodes stop
		// rediscovering the dead origin file after file.
		if isAnonMP4OriginDown(err) {
			strikes, disabled := recordAnonMP4OriginDown()
			if disabled {
				log.Printf("ERROR [AnonMP4] origin 522 strike %d/%d — disabling AnonMP4 until next UTC midnight (upstream outage, fleet-wide)", strikes, anonmp4522MaxStrikes)
				return "", err
			}
		}

		// Fail fast on dial failures / HTTP 5xx (service unreachable) and on
		// functional rejections (bad client_token, non-success status) — in
		// both cases burning the remaining attempts can't help.
		if isFailFastError(err) {
			return "", err
		}
		var rejected *anonmp4RejectedError
		if errors.As(err, &rejected) {
			return "", err
		}
	}

	return "", fmt.Errorf("anonmp4: all %d attempts failed, last: %w", anonmp4MaxAttempts, lastErr)
}

// uploadFlow runs one full node cycle: assign node -> stream chunks -> fetch
// final links.
func (u *AnonMP4Uploader) uploadFlow(filePath, fileName string, size int64, progress ProgressFunc) (string, error) {
	node, err := u.assignUploadNode(fileName)
	if err != nil {
		return "", fmt.Errorf("anonmp4: node assignment: %w", err)
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("anonmp4: open file: %w", err)
	}
	defer file.Close()

	if err := u.uploadChunked(file, size, node.UploadURL, node.NodeToken, fileName, progress); err != nil {
		// If the failure looks like the assigned node dropped our chunks
		// (HTTP 201 with zero bytes accepted repeatedly, or 415/413 on the node),
		// do NOT hammer the same node_url on the next attempt — the node is likely
		// saturated. The outer retry will re-assign and get a fresh node_url.
		return "", fmt.Errorf("anonmp4: chunk upload: %w", err)
	}

	res, err := u.fetchVideoNode(node.NodeID, node.NodeToken, fileName)
	if err != nil {
		return "", fmt.Errorf("anonmp4: fetch video info: %w", err)
	}

	var link string
	if res.EmbedURL != "" {
		link = res.EmbedURL
	} else {
		link = res.WatchURL
	}
	if link == "" {
		return "", fmt.Errorf("anonmp4: no watch/embed URL in finalize response (video_id=%s)", res.VideoID)
	}
	// Ensure HTTPS
	link = strings.Replace(link, "http://", "https://", 1)
	// The API sometimes returns links on a view domain that is not serving
	// videos (see anonmp4ViewBase). Pin them to the working domain.
	link = normalizeAnonMP4Link(link)
	return link, nil
}

// assignUploadNode asks the site for a signed upload node for this filename.
func (u *AnonMP4Uploader) assignUploadNode(fileName string) (*anonmp4NodeResponse, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	_ = w.WriteField("client_token", anonmp4ClientToken)
	_ = w.WriteField("filename", fileName)
	if err := w.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", anonmp4SiteBase+"/get-upload-node", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("User-Agent", defaultUserAgent)
	addUploadCookie(req)

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("rate limited (HTTP 429)")
	}
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusBadRequest || resp.StatusCode == http.StatusUnauthorized ||
			resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusNotFound {
			return nil, &anonmp4RejectedError{msg: fmt.Sprintf("node assignment rejected (HTTP %d): %s", resp.StatusCode, strings.TrimSpace(string(raw)))}
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var node anonmp4NodeResponse
	if err := json.Unmarshal(raw, &node); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if !node.Status || node.UploadURL == "" || node.NodeToken == "" {
		msg := node.Message
		if msg == "" {
			msg = "node assignment returned no upload_url"
		}
		return nil, &anonmp4RejectedError{msg: fmt.Sprintf("node assignment failed: %s", msg)}
	}
	return &node, nil
}

// uploadChunked streams the file in 2 MiB chunks, up to 5 at a time, exactly
// like the site's browser uploader. Each chunk is retried up to
// anonmp4MaxChunkHits times with 1s/2s backoff before the whole cycle fails.
func (u *AnonMP4Uploader) uploadChunked(file *os.File, size int64, uploadURL, nodeToken, fileName string, progress ProgressFunc) error {
	totalChunks := int((size + anonmp4ChunkSize - 1) / anonmp4ChunkSize)

	var (
		mu          sync.Mutex
		uploaded    int64
		firstErr    error
		progressNow bool
	)
	if progress != nil {
		progressNow = true
	}
	report := func() {
		if progressNow {
			progress("AnonMP4", uploaded, size)
		}
	}

	for batchStart := 1; batchStart <= totalChunks; batchStart += anonmp4MaxParallel {
		batchEnd := batchStart + anonmp4MaxParallel - 1
		if batchEnd > totalChunks {
			batchEnd = totalChunks
		}

		var wg sync.WaitGroup
		errs := make(chan error, batchEnd-batchStart+1)
		for idx := batchStart; idx <= batchEnd; idx++ {
			wg.Add(1)
			go func(chunkIdx int) {
				defer wg.Done()
				if err := u.uploadChunkWithRetries(file, size, uploadURL, nodeToken, fileName, chunkIdx, totalChunks); err != nil {
					errs <- err
					return
				}
				chunkBytes := int64(anonmp4ChunkSize)
				if int64(chunkIdx)*anonmp4ChunkSize > size {
					chunkBytes = size - int64(chunkIdx-1)*anonmp4ChunkSize
				}
				mu.Lock()
				uploaded += chunkBytes
				mu.Unlock()
				report()
			}(idx)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if firstErr != nil {
			return firstErr
		}
	}
	return nil
}

// uploadChunkWithRetries sends one chunk, retrying on transient failures.
func (u *AnonMP4Uploader) uploadChunkWithRetries(file *os.File, size int64, uploadURL, nodeToken, fileName string, chunkIdx, totalChunks int) error {
	var lastErr error
	for attempt := 1; attempt <= anonmp4MaxChunkHits; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoffChunk(attempt - 1))
		}
		err := u.uploadChunk(file, size, uploadURL, nodeToken, fileName, chunkIdx, totalChunks)
		if err == nil {
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("chunk %d/%d failed after %d attempts: %w", chunkIdx, totalChunks, anonmp4MaxChunkHits, lastErr)
}

// uploadChunk POSTs one raw-binary chunk with Content-Range metadata as query
// params (mirrors the site: body stays pure binary, no multipart boundary).
func (u *AnonMP4Uploader) uploadChunk(file *os.File, size int64, uploadURL, nodeToken, fileName string, chunkIdx, totalChunks int) error {
	start := int64(chunkIdx-1) * anonmp4ChunkSize
	end := start + anonmp4ChunkSize
	if end > size {
		end = size
	}
	chunk := make([]byte, end-start)
	if _, err := file.ReadAt(chunk, start); err != nil {
		return fmt.Errorf("read chunk: %w", err)
	}

	params := url.Values{}
	params.Set("file_id", nodeToken)
	params.Set("chunkIndex", strconv.Itoa(chunkIdx))
	params.Set("totalChunks", strconv.Itoa(totalChunks))
	params.Set("filename", fileName)

	req, err := http.NewRequest("POST", uploadURL+"?"+params.Encode(), bytes.NewReader(chunk))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, url.QueryEscape(fileName)))
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end-1, size))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-Uploading-Mode", "parallel")
	req.Header.Set("User-Agent", defaultUserAgent)
	addUploadCookie(req)
	req.ContentLength = int64(len(chunk))

	resp, err := u.client.Do(req)
	if err != nil {
		return fmt.Errorf("send chunk: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return fmt.Errorf("read chunk response: %w", err)
	}

	// The upload Worker accepts the chunk and answers 201 Created (201 for the
	// first chunks, 200 once the file is complete) with a text/plain body of
	// "0-<received-1>/<total>" — a CUMULATIVE bytes-received ack, not a
	// Content-Range echo of this chunk ("0-2097151/6291456" after chunk 1 of a
	// 6 MiB file means "I hold bytes 0..2097151"). Verified live 2026-09-06:
	// every chunk 201s and the finalize step then returns success + video id.
	// Before that check the non-200 status was treated as a failure and every
	// upload died on its first chunk with the phantom error
	// "chunk N/M failed: HTTP 201: 0-10485759/...".
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		if resp.StatusCode == http.StatusTooManyRequests {
			return fmt.Errorf("rate limited (HTTP 429)")
		}
		msg := strings.TrimSpace(string(raw))
		if len(msg) > 300 {
			msg = msg[:300]
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, msg)
	}
	// The Worker's ack is plain text ("0-2097151/6291456"), not JSON — only
	// treat the body as an ack map when it parses as one. Some deployments
	// answer with {"status":"error",...}, which must still fail the chunk.
	var ack map[string]any
	if err := json.Unmarshal(raw, &ack); err == nil {
		if s, _ := ack["status"].(string); s == "error" {
			msg, _ := ack["message"].(string)
			return fmt.Errorf("chunk rejected: %s", msg)
		}
	}
	return nil
}

// fetchVideoNode retrieves the final watch/embed links for the assembled file.
func (u *AnonMP4Uploader) fetchVideoNode(nodeID, nodeToken, fileName string) (*anonmp4UploadResponse, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	_ = w.WriteField("node_id", nodeID)
	_ = w.WriteField("node_token", nodeToken)
	_ = w.WriteField("client_token", anonmp4ClientToken)
	_ = w.WriteField("filename", fileName)
	if err := w.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", anonmp4SiteBase+"/get-video-node", &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		return nil, fmt.Errorf("rate limited (HTTP 429)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}

	var res anonmp4UploadResponse
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("parse response: %w", err)
	}
	if !res.Success && !strings.EqualFold(res.Status, "success") {
		if res.Error != nil {
			return nil, &anonmp4RejectedError{msg: fmt.Sprintf("finalize rejected: API error %s: %s", res.Error.Code, res.Error.Message)}
		}
		msg := res.Message
		if msg == "" {
			msg = "finalize returned non-success status"
		}
		return nil, &anonmp4RejectedError{msg: fmt.Sprintf("finalize failed: %s", msg)}
	}
	return &res, nil
}

// uploadBackoffChunk returns the backoff to use between AnonMP4 chunk retries.
// The node_url is shared across the whole parallel batch, so a gentle linear
// pause works better than aggressive exponential backoff.
func uploadBackoffChunk(attempt int) time.Duration {
	return anonmp4ChunkBackoff + time.Duration(attempt)*anonmp4ChunkBackoff
}

// addUploadCookie sets a minimal cookie header on AnonMP4 requests, mirroring a
// normal browser session so the site's Cloudflare Worker doesn't flag the
// upload as a bot / empty-session post.
func addUploadCookie(req *http.Request) {
	req.Header.Set("Cookie", "cf_clearance=1; _cfuvid=1")
}
