package uploader

import (
	"bytes"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// newNoProxyClient returns an http.Client that explicitly bypasses any
// environment-configured proxy (ALL_PROXY / HTTP_PROXY / HTTPS_PROXY).
// All connections are direct; image/thumbnail upload services must reach
// the public internet directly.
func newNoProxyClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			Proxy: nil, // never use environment proxy
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   10,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   15 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}
}

// multipartStream builds a multipart request body that streams the file without
// loading it into RAM, while still setting an exact Content-Length so servers
// that reject chunked transfer encoding (Streamtape, Mixdrop) work.
//
// fields is written before the file part (may be nil).
// If host is non-empty the file part is wrapped with a ProgressReader.
// Returns: body reader, content-length, multipart content-type, closer (the opened file), error.
func multipartStream(fields map[string]string, fileField, filePath, host string) (io.Reader, int64, string, io.Closer, error) {
	return multipartStreamWithProgress(fields, fileField, filePath, host, nil)
}

func multipartStreamWithProgress(fields map[string]string, fileField, filePath, host string, progress ProgressFunc) (io.Reader, int64, string, io.Closer, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return nil, 0, "", nil, fmt.Errorf("stat: %w", err)
	}

	// Build the preamble (all multipart headers, but NOT the file bytes).
	var preamble bytes.Buffer
	mw := multipart.NewWriter(&preamble)

	for k, v := range fields {
		if err := mw.WriteField(k, v); err != nil {
			return nil, 0, "", nil, fmt.Errorf("write field %s: %w", k, err)
		}
	}

	// CreateFormFile writes the part header into preamble; we do NOT write file
	// bytes through this writer — they come from the file directly.
	if _, err := mw.CreateFormFile(fileField, filepath.Base(filePath)); err != nil {
		return nil, 0, "", nil, fmt.Errorf("create form file: %w", err)
	}

	// Closing boundary that would normally be written by mw.Close().
	closing := fmt.Sprintf("\r\n--%s--\r\n", mw.Boundary())
	contentType := mw.FormDataContentType()
	totalLen := int64(preamble.Len()) + fi.Size() + int64(len(closing))

	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, "", nil, fmt.Errorf("open: %w", err)
	}

	var fileReader io.Reader = file
	if host != "" {
		fileReader = NewProgressReaderWithCallback(file, fi.Size(), host, progress)
	}

	body := io.MultiReader(&preamble, fileReader, bytes.NewReader([]byte(closing)))
	return body, totalLen, contentType, file, nil
}

// Logger is the interface for logging upload events.
// The channel package implements this with ch.Info/ch.Error.
type Logger interface {
	Info(format string, a ...any)
	Error(format string, a ...any)
}

// UploadResult contains the result of an upload to a specific host
type UploadResult struct {
	Host         string
	DownloadLink string
	Error        error
}

// MultiHostUploader handles uploading to multiple hosts simultaneously
type MultiHostUploader struct {
	gofile        *GoFileUploader
	voesx         *VoeSXUploader
	streamtape    *StreamtapeUploader
	mixdrop       *MixdropUploader
	vidara        *VidaraUploader
	vidmoly       *VidMolyUploader
	anonmp4       *AnonMP4Uploader
	log           Logger
	hostInitOnce  sync.Once
	hosts         map[string]uploaderFunc // host name -> upload function, lazy-init
	progress      ProgressFunc
	disabledHosts map[string]bool // hosts disabled for the rest of this run
	disabledMu    sync.Mutex
	consecFails   failingHosts // consecutive failures per host -> auto-disable at threshold
	// fullSend records, per (host, file), that a previous attempt already
	// transmitted the file's complete body to the server but lost the response
	// (see errBodyFullySent).  Persisting across UploadSelectedWithCallback
	// calls (which stageUploadVideos re-runs once per DoWithRetry attempt,
	// reusing the same MultiHostUploader) prevents re-uploading a file whose
	// bytes were already fully pushed.
	//
	// Keyed by host AND file.  Keying by host alone made one lost response on
	// ONE file a permanent ban on that host, reported with a message claiming
	// the NEXT file had "already been fully transmitted" when it had in fact
	// never been sent — and since the skip produces a failure rather than a
	// success, the marker was never cleared and the host stayed blocked for the
	// life of the process.
	fullSendMu sync.Mutex
	fullSend   map[fullSendKey]time.Time // key -> when the full-send marker expires; markers not in map are not blocked
}

// fullSendKey identifies one (host, file) pair whose complete body has already
// been streamed but whose response was lost.
type fullSendKey struct {
	host string
	file string
}

// package-level set of upload hosts that must never be attempted this run,
// regardless of whether their API keys are configured. Populated once at
// startup from DISABLED_UPLOAD_HOSTS (e.g. a dead "AnonMP4"). The per-instance
// disabledHosts map is for runtime failures; this config-level set is for
// operator-declared deadlist entries and is consulted in initHosts so a listed
// host is never even registered.
var (
	globallyDisabled    map[string]bool
	globallyDisabledRWM sync.RWMutex
)

// SetDisabledHosts records host names that must never be attempted. Call once
// at startup before any upload goroutines run. Names are matched exactly
// against the configured host set (e.g. "AnonMP4", "VOE.sx").
func SetDisabledHosts(names []string) {
	globallyDisabledRWM.Lock()
	defer globallyDisabledRWM.Unlock()
	globallyDisabled = make(map[string]bool, len(names))
	for _, name := range names {
		if v := strings.TrimSpace(name); v != "" {
			globallyDisabled[v] = true
		}
	}
}

// isGloballyDisabled reports whether the named host is operator-deadlisted.
func isGloballyDisabled(name string) bool {
	globallyDisabledRWM.RLock()
	defer globallyDisabledRWM.RUnlock()
	return globallyDisabled[name]
}

// Timed quota disables: hosts that exhaust a per-day budget (e.g. VidMoly's
// ~50 uploads/day free cap) must be skipped for the rest of the day and then
// re-enabled automatically. Kept at PACKAGE level so the window persists
// across the per-file MultiHostUploader instances the channel creates (each
// file builds a fresh uploader, so per-instance state cannot span files).
var (
	timedDisabled    map[string]time.Time // host name -> when it may be retried
	timedDisabledRWM sync.RWMutex
)

// disableHostFor marks a host unavailable for dur (e.g. 24h after a daily
// upload cap is hit). Unlike per-instance DisableHost this is package-wide
// and automatically lifted once dur elapses.  reason is logged and forwarded to
// the fleet (see publishHostBackoff).
func disableHostFor(name string, dur time.Duration, reason string) {
	expiry := time.Now().Add(dur)
	timedDisabledRWM.Lock()
	if timedDisabled == nil {
		timedDisabled = map[string]time.Time{}
	}
	timedDisabled[name] = expiry
	timedDisabledRWM.Unlock()

	publishHostBackoff(name, expiry, reason)
}

// fleetBackoff mirrors the fleet-wide host backoffs stored in Supabase.  The
// upload credentials are shared by every node, so a per-account quota is really
// a fleet cap: without this, all 18 nodes spend their own request to discover
// VidMoly's exhausted 50/day API budget (live: used_today 2416 / limit 50) and
// every CI restart pays again.  Populated by manager/host_backoff.go.
//
// Deliberately consulted only where an ATTEMPT is about to be made, never in
// AvailableHosts: IsAlreadyFullyUploaded uses that list to decide whether the
// watcher may delete a local file, so dropping a backed-off host from it would
// mark a file complete — and delete it — before the host ever received it.
var (
	fleetBackoffMu sync.RWMutex
	fleetBackoff   map[string]time.Time
)

// publishHostBackoff, when set, records a host backoff fleet-wide so peer nodes
// skip the host too.  It is best-effort and must never block an upload, so it
// runs on its own goroutine.
var publishHostBackoffFn func(host string, until time.Time, reason string)

// SetPublishHostBackoff installs the fleet-wide report sink (a Supabase upsert
// in production).  Passing nil disables publishing.
func SetPublishHostBackoff(fn func(host string, until time.Time, reason string)) {
	publishHostBackoffFn = fn
}

// publishHostBackoff reports a host backoff to the fleet without blocking the
// caller, and records it locally so this node is consistent immediately.
func publishHostBackoff(host string, until time.Time, reason string) {
	if host == "" || until.IsZero() {
		return
	}
	setFleetHostBackoff(host, until)
	fn := publishHostBackoffFn
	if fn == nil {
		return
	}
	go fn(host, until, reason)
}

// setFleetHostBackoff records one host's fleet-wide expiry in the local cache.
func setFleetHostBackoff(host string, until time.Time) {
	fleetBackoffMu.Lock()
	defer fleetBackoffMu.Unlock()
	if fleetBackoff == nil {
		fleetBackoff = map[string]time.Time{}
	}
	fleetBackoff[host] = until
}

// SetFleetHostBackoffs replaces the cached fleet-wide backoffs with a fresh read
// of the shared table.  Rows already expired are kept (they compare as inactive)
// so the refresh cannot resurrect a host that another node just took offline.
func SetFleetHostBackoffs(backoffs map[string]time.Time) {
	fleetBackoffMu.Lock()
	defer fleetBackoffMu.Unlock()
	fleetBackoff = make(map[string]time.Time, len(backoffs))
	for host, until := range backoffs {
		fleetBackoff[host] = until
	}
}

// isFleetHostBackedOff reports whether another node has taken this host offline
// for a shared-credential quota.
func isFleetHostBackedOff(name string) bool {
	fleetBackoffMu.RLock()
	defer fleetBackoffMu.RUnlock()
	expiry, ok := fleetBackoff[name]
	return ok && time.Now().Before(expiry)
}

// fleetHostBackoffUntil returns the fleet-wide expiry for a host (zero when
// none), for log messages that name when the host comes back.
func fleetHostBackoffUntil(name string) time.Time {
	fleetBackoffMu.RLock()
	defer fleetBackoffMu.RUnlock()
	return fleetBackoff[name]
}

// isHostTimedOut reports whether the host is inside a timed-disable window
// (daily quota exhausted) and must be skipped.
func isHostTimedOut(name string) bool {
	timedDisabledRWM.RLock()
	defer timedDisabledRWM.RUnlock()
	expiry, ok := timedDisabled[name]
	return ok && time.Now().Before(expiry)
}

// hostTimedOutUntil returns when the host's timed-disable window ends (zero
// time if the host is not currently timed-disabled).
func hostTimedOutUntil(name string) time.Time {
	timedDisabledRWM.RLock()
	defer timedDisabledRWM.RUnlock()
	return timedDisabled[name]
}

// clearTimedDisablesForTest removes all timed disables — and the fleet-wide
// cache they mirror, since disableHostFor records both (test hook).
func clearTimedDisablesForTest() {
	timedDisabledRWM.Lock()
	timedDisabled = nil
	timedDisabledRWM.Unlock()
	clearFleetBackoffsForTest()
}

// clearFleetBackoffsForTest removes all fleet-wide backoffs (test hook).
func clearFleetBackoffsForTest() {
	fleetBackoffMu.Lock()
	fleetBackoff = nil
	fleetBackoffMu.Unlock()
}

// DisableHost marks a host as unavailable for the remainder of this run (e.g.
// VOE.sx once its storage quota is exhausted), so we stop retrying it on every
// file and spamming the same unrecoverable error.
func (m *MultiHostUploader) DisableHost(name string) {
	m.disabledMu.Lock()
	if m.disabledHosts == nil {
		m.disabledHosts = map[string]bool{}
	}
	m.disabledHosts[name] = true
	m.disabledMu.Unlock()
}
func (m *MultiHostUploader) isHostDisabled(name string) bool {
	m.disabledMu.Lock()
	defer m.disabledMu.Unlock()
	return m.disabledHosts[name]
}

// recordHostFailure bumps the host's consecutive-failure streak and returns
// the new count. Failures are only meaningful against OTHER hosts succeeding
// on the same file — the caller (UploadSelectedWithCallback / priority path)
// evaluates that separately; here it is a plain streak.
func (m *MultiHostUploader) recordHostFailure(name string) int {
	m.disabledMu.Lock()
	defer m.disabledMu.Unlock()
	return m.consecFails.recordFailure(name)
} // recordHostSuccess resets a host's consecutive-failure streak after a
// successful upload.
func (m *MultiHostUploader) recordHostSuccess(name string) {
	m.disabledMu.Lock()
	defer m.disabledMu.Unlock()
	m.consecFails.recordSuccess(name)
}

// markFullSend records that the given host's last attempt streamed FILE's
// complete body (progress reached total).  See the fullSend field comment.
func (m *MultiHostUploader) markFullSend(file, name string) {
	m.fullSendMu.Lock()
	defer m.fullSendMu.Unlock()
	if m.fullSend == nil {
		m.fullSend = map[fullSendKey]time.Time{}
	}
	// Keep the full-send guard alive for one retry window in case the server
	// response is lost on the first attempt.  This avoids poisoning the pair
	// for the entire process lifetime (the "VOE.sx upload: file body was
	// already fully transmitted" dead-end) without risking re-streaming bytes
	// that did actually reach the host.
	exp := time.Now().Add(10 * time.Minute)
	m.fullSend[fullSendKey{host: name, file: file}] = exp
}

// hasFullSend reports whether the host already received the complete body of
// THIS file on a prior attempt, so we must not re-stream it.  The file is part
// of the key: see the fullSend field comment.
func (m *MultiHostUploader) hasFullSend(file, name string) bool {
	m.fullSendMu.Lock()
	defer m.fullSendMu.Unlock()
	exp, ok := m.fullSend[fullSendKey{host: name, file: file}]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(m.fullSend, fullSendKey{host: name, file: file})
		return false
	}
	return true
}

// clearFullSend forgets a host's full-send marker for FILE on success.
func (m *MultiHostUploader) clearFullSend(file, name string) {
	m.fullSendMu.Lock()
	delete(m.fullSend, fullSendKey{host: name, file: file})
	m.fullSendMu.Unlock()
}

// errBodyFullySent reports that a previous attempt already handed the file's
// entire body to the host but its response was lost (timeout, EOF, reset, 5xx
// after upload).  Re-uploading the same bytes would create duplicate files.
func errBodyFullySent(host string) error {
	return fmt.Errorf("%s upload: file body was already fully transmitted in a previous attempt but the response was lost — not re-uploading this file to avoid duplicates", host)
}

// trackFullSend wraps a progress callback for FILE so the MultiHostUploader
// records the moment a host's upload stream reaches 100% of that file.  Used by
// UploadSelectedWithCallback and UploadSelectedPriority so that a file whose
// complete body was already pushed to a host on a prior DoWithRetry attempt is
// NOT re-streamed (stageUploadVideos re-runs the uploader once per attempt
// while reusing the same MultiHostUploader).
func (m *MultiHostUploader) trackFullSend(file string, progressFn ProgressFunc) ProgressFunc {
	return func(host string, current, total int64) {
		if total > 0 && current >= total {
			m.markFullSend(file, host)
		}
		if progressFn != nil {
			progressFn(host, current, total)
		}
	}
}

// failingHostsThreshold is how many consecutive files a host may fail before
// it is auto-disabled for the rest of the run. A single failure can be
// transient (one bad file, one blip) — a host failing several files in a row
// while every other host succeeds is outages/rate-limits, and re-attempting it
// on every following file only burns minutes of upload time per file (the
// seven-day AnonMP4 zero-success stretch burned ~2-6 min/file on 17 nodes).
const failingHostsThreshold = 3

// failingHosts tracks consecutive failures per host (guarded by disabledMu,
// shared with the disabledHosts map it feeds). Consecutive = reset to 0 on any
// success; when a host's streak reaches failingHostsThreshold it is disabled
// for the rest of the run.
type failingHosts struct {
	counts map[string]int
}

func (f *failingHosts) recordSuccess(name string) {
	if f.counts != nil {
		delete(f.counts, name)
	}
}

func (f *failingHosts) recordFailure(name string) int {
	if f.counts == nil {
		f.counts = map[string]int{}
	}
	f.counts[name]++
	return f.counts[name]
}

type uploaderFunc func(string, ProgressFunc) (string, error)

func (m *MultiHostUploader) initHosts() {
	m.hostInitOnce.Do(func() {
		// Don't clobber a hosts map that was pre-populated (e.g. by tests that
		// inject fakes).  Only build the default host set when none was provided.
		if m.hosts != nil {
			return
		}
		m.hosts = map[string]uploaderFunc{}
		if !isGloballyDisabled("GoFile") {
			m.hosts["GoFile"] = m.gofile.UploadWithProgress
		}
		if m.voesx != nil && m.voesx.keys.count() > 0 && !isGloballyDisabled("VOE.sx") {
			m.hosts["VOE.sx"] = m.voesx.UploadWithProgress
		}
		if m.streamtape != nil && m.streamtape.creds != nil && m.streamtape.creds.count() > 0 && !isGloballyDisabled("Streamtape") {
			m.hosts["Streamtape"] = m.streamtape.UploadWithProgress
		}
		if m.mixdrop != nil && m.mixdrop.email != "" && m.mixdrop.token != "" && !isGloballyDisabled("Mixdrop") {
			m.hosts["Mixdrop"] = m.mixdrop.UploadWithProgress
		}
		if m.vidara != nil && m.vidara.keys.count() > 0 && !isGloballyDisabled("Vidara") {
			m.hosts["Vidara"] = m.vidara.UploadWithProgress
		}
		if m.vidmoly != nil && m.vidmoly.keys.count() > 0 && !isGloballyDisabled("VidMoly") {
			m.hosts["VidMoly"] = m.vidmoly.UploadWithProgress
		}
		// AnonMP4: always available (no API key required)
		if m.anonmp4 != nil && !isGloballyDisabled("AnonMP4") {
			m.hosts["AnonMP4"] = m.anonmp4.UploadWithProgress
		}
	})
}

// NewMultiHostUploader creates a new multi-host uploader
func NewMultiHostUploader(voeSXAPIKey, streamtapeLogin, streamtapeKey, mixdropEmail, mixdropToken, vidaraKey, vidMolyKey string, log Logger) *MultiHostUploader {
	if log == nil {
		log = &nilLogger{}
	}
	return &MultiHostUploader{
		gofile:     NewGoFileUploader(),
		voesx:      NewVoeSXUploader(voeSXAPIKey),
		streamtape: NewStreamtapeUploader(streamtapeLogin, streamtapeKey),
		mixdrop:    NewMixdropUploader(mixdropEmail, mixdropToken),
		vidara:     NewVidaraUploader(vidaraKey),
		vidmoly:    NewVidMolyUploader(vidMolyKey),
		anonmp4:    NewAnonMP4Uploader(),
		log:        log,
	}
}

// SetProgressCallback sets an upload-local progress callback for this uploader.
func (m *MultiHostUploader) SetProgressCallback(fn ProgressFunc) {
	m.progress = fn
}

const defaultUserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

// isUploadRateLimited returns true if the error indicates a rate-limit hit
// (429 Too Many Requests or similar). Uses a different name than imgbb.go's
// isRateLimitError to avoid redeclaration.
func isUploadRateLimited(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "rate limit") ||
		strings.Contains(msg, "429") ||
		strings.Contains(msg, "too many requests")
}

// isFailFastError reports whether an upload error means the host is dead
// (DNS failure, connection refused/reset, timeout) or actively rate-limiting
// us.  In either case retrying the SAME host is futile and, for rate limits,
// actually makes things worse (it extends the rate-limit window).  Callers
// should bail on the current host and let their fallback chain (Pixhost →
// ImgBB → Catbox, or Catbox → ImgBB) try the next host instead.
func isFailFastError(err error) bool {
	if err == nil {
		return false
	}
	if isUploadRateLimited(err) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "dial tcp") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "deadline exceeded") ||
		strings.Contains(msg, "eof") ||
		// Credential/account failures (e.g. "could not authenticate user. The
		// key pair may be invalid or your account may be locked") will not
		// resolve within a single run; retrying just wastes time. Treat as
		// fatal so the fallback / deathlist can move on immediately.
		strings.Contains(msg, "could not authenticate") ||
		strings.Contains(msg, "account may be locked") ||
		// Streamtape's auth rejection — "get upload URL: API error 403:
		// Authentication failed" — is a dead/rotated key.  Note the matcher
		// below looks for "http 403" which this error shape does not contain,
		// so it must be listed explicitly.  Retrying it burns 3 attempts ×
		// every file before the host is auto-disabled.
		strings.Contains(msg, "authentication failed") ||
		// HTTP 403 rejections (e.g. freeimage.host's "requires authentication"
		// when uploading with the shared guest key) are account-level and will
		// not resolve on retry — bail so the next host in the chain is tried.
		strings.Contains(msg, "http 403") ||
		strings.Contains(msg, "requires authentication") ||
		strings.Contains(msg, "access denied") ||
		strings.Contains(msg, "forbidden")
}

// isUploadAuthError reports whether an upload error means the host rejected
// our credentials (dead/rotated key, locked account).  Unlike transient
// failures this will never succeed again within the run, so the caller can
// disable the host immediately instead of burning the 3-file failure streak
// (each file paying full retries — observed live with a rotated Streamtape
// key: every file retried the 403 three times before the streak disabled it).
func isUploadAuthError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "could not authenticate") ||
		strings.Contains(msg, "account may be locked") ||
		strings.Contains(msg, "invalid api key") ||
		strings.Contains(msg, "api key not configured") ||
		strings.Contains(msg, "streamtape rejected our credentials") ||
		strings.Contains(msg, "rejected our credentials") ||
		strings.Contains(msg, "invalid login") ||
		strings.Contains(msg, "login failed")
}

// keysExhaustedCooldown is how long a host is skipped once EVERY configured key
// has failed.  The three key-rotating hosts (VidMoly, VOE.sx, Vidara) all end
// their retry loop with "<Host> upload failed: all keys exhausted", and no
// retry inside one run can fix that — the keys have to be rotated or the host's
// own quota has to lapse.  Without a window, each file re-walks every dead key
// (VidMoly alone produced ~2,000 such errors and 686 "failed 3 files in a row"
// auto-disables across the fleet in 24h, burning minutes of upload time per
// file).  Six hours stops the churn while still self-healing within the same
// day if the operator rotates keys mid-flight.
const keysExhaustedCooldown = 6 * time.Hour

// isKeysExhausted reports whether an upload error means every key the host was
// configured with was tried and rejected in this attempt.  Matches the shared
// wording all three key-rotating uploaders use as their terminal error.
func isKeysExhausted(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "all keys exhausted")
}

// isHostDead reports whether an upload error indicates the host is permanently
// unreachable (DNS resolution succeeded but TCP connection failed — the server
// is down, not just slow).  Unlike isFailFastError, this excludes transient
// errors (rate limits, EOF) that might succeed on retry.  Hosts flagged by
// this check are auto-disabled for the rest of the run to avoid wasting time
// retrying dead services (e.g. AnonMP4 whose server stopped responding).
func isHostDead(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Connection timeout with a resolved IP = server is down
	if (strings.Contains(msg, "dial tcp") && strings.Contains(msg, "timeout")) ||
		(strings.Contains(msg, "dial tcp") && strings.Contains(msg, "connectex")) ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no such host") {
		return true
	}
	// HTTP 500 from the token/upload endpoint = service-side failure.
	// If ALL 3 retry attempts hit 500, the host is effectively down
	// (e.g. Imgbox's token endpoint has been 500 for weeks).
	if strings.Contains(msg, "http 500") || strings.Contains(msg, "status 500") {
		return true
	}
	return false
}

// uploadBackoff returns the appropriate backoff duration based on whether
// the error was a rate-limit hit. Rate limits get a longer 60s+20s/attempt,
// while other errors use standard exponential delay.
func uploadBackoff(attempt int, err error) time.Duration {
	if isUploadRateLimited(err) {
		// Long backoff for rate limits — wait 60s + 20s per retry
		return 60*time.Second + time.Duration(attempt)*20*time.Second
	}
	// Standard exponential backoff: 10s, 20s, 40s, 80s...
	return time.Duration((1<<uint(attempt))*10) * time.Second
}

// nilLogger discards all log messages when no logger is provided.
type nilLogger struct{}

func (n *nilLogger) Info(format string, a ...any)  {}
func (n *nilLogger) Error(format string, a ...any) {}

// UploadToAll uploads a file to all configured hosts in parallel.
// Returns a slice of results, one for each host.
func (m *MultiHostUploader) UploadToAll(filePath string) []UploadResult {
	m.initHosts()
	hosts := make([]string, 0, len(m.hosts))
	for name := range m.hosts {
		hosts = append(hosts, name)
	}
	return m.UploadSelected(filePath, hosts)
}

// UploadSelected uploads a file to the specified hosts in parallel.
// Host names that are not configured are silently skipped.
func (m *MultiHostUploader) UploadSelected(filePath string, hosts []string) []UploadResult {
	return m.UploadSelectedWithCallback(filePath, hosts, nil)
}

// UploadSelectedWithCallback uploads to all selected hosts in parallel.
// If onHost is non-nil it is called the instant each host succeeds,
// receiving the host name and download URL — so the caller can persist
// the link to Supabase immediately without waiting for slower hosts.
func (m *MultiHostUploader) UploadSelectedWithCallback(filePath string, hosts []string, onHost func(host, url string)) []UploadResult {
	m.initHosts()
	var wg sync.WaitGroup
	var mu sync.Mutex
	results := []UploadResult{}

	progressFn := m.trackFullSend(filePath, m.progress)
	for _, name := range hosts {
		if m.isHostDisabled(name) {
			m.log.Info("upload: skipping disabled host %s for %s", name, filePath)
			continue
		}
		if isHostTimedOut(name) {
			m.log.Info("upload: skipping %s for %s — daily upload limit reached (auto re-enables at %s)", name, filePath, hostTimedOutUntil(name).Format(time.RFC3339))
			continue
		}
		if isFleetHostBackedOff(name) {
			m.log.Info("upload: skipping %s for %s — a peer node reported this host exhausted on the shared credentials (auto re-enables at %s)", name, filePath, fleetHostBackoffUntil(name).Format(time.RFC3339))
			continue
		}
		uploadFn, ok := m.hosts[name]
		if !ok {
			continue
		}
		wg.Add(1)
		go func(host string, fn uploaderFunc) {
			defer wg.Done()
			var link string
			var err error
			if m.hasFullSend(filePath, host) {
				// A previous attempt already pushed the file's complete body to
				// this host but lost the response.  Re-streaming the same bytes
				// would create a duplicate upload on the host, so fail this
				// attempt without touching the socket.
				m.log.Error("upload: skipping %s for %s — file body was already fully transmitted in a previous attempt (response lost)", host, filePath)
				err = errBodyFullySent(host)
			} else {
				m.log.Info("upload: starting %s upload for %s", host, filePath)
				link, err = fn(filePath, progressFn)
			}
			if err != nil {
				m.log.Error("upload: %s failed for %s: %v", host, filePath, err)
				if isVoeStorageFull(err) {
					m.log.Error("upload: %s reported storage full — disabling it for the rest of this run", host)
					m.DisableHost(host)
				} else if isVidMolyDailyLimit(err) {
					m.log.Error("upload: %s hit its daily upload limit — skipping it for ~24 hours (auto re-enables)", host)
					disableHostFor(host, 24*time.Hour, "daily upload limit reached")
					m.DisableHost(host)
				} else if isKeysExhausted(err) {
					m.log.Error("upload: %s has no working keys left — skipping it for %s (auto re-enables; rotate its keys to restore)", host, keysExhaustedCooldown)
					disableHostFor(host, keysExhaustedCooldown, "all configured keys exhausted")
					m.DisableHost(host)
				} else if isUploadAuthError(err) {
					m.log.Error("upload: %s rejected our credentials — disabling it for the rest of this run", host)
					m.DisableHost(host)
				} else if isHostDead(err) {
					m.log.Error("upload: %s is permanently unreachable — disabling it for the rest of this run", host)
					m.DisableHost(host)
				} else if n := m.recordHostFailure(host); n >= failingHostsThreshold {
					m.log.Error("upload: %s failed %d files in a row — disabling it for the rest of this run", host, n)
					m.DisableHost(host)
				}
			} else {
				m.log.Info("upload: %s successful for %s: %s", host, filePath, link)
				m.recordHostSuccess(host)
				m.clearFullSend(filePath, host)
				if onHost != nil {
					onHost(host, link)
				}
			}
			mu.Lock()
			results = append(results, UploadResult{
				Host:         host,
				DownloadLink: link,
				Error:        err,
			})
			mu.Unlock()
		}(name, uploadFn)
	}

	wg.Wait()
	return results
}

// UploadSelectedPriority uploads to the priority host first (sequentially),
// then to remaining hosts in parallel. This ensures the priority host gets
// full bandwidth during shutdown when time is limited.
func (m *MultiHostUploader) UploadSelectedPriority(filePath string, hosts []string, priorityHost string) []UploadResult {
	m.initHosts()

	var priorityHosts []string
	var otherHosts []string
	for _, host := range hosts {
		if host == priorityHost {
			priorityHosts = append(priorityHosts, host)
		} else {
			otherHosts = append(otherHosts, host)
		}
	}

	var results []UploadResult
	progressFn := m.trackFullSend(filePath, m.progress)

	for _, host := range priorityHosts {
		if m.isHostDisabled(host) {
			m.log.Info("upload: skipping disabled host %s for %s", host, filePath)
			continue
		}
		if isHostTimedOut(host) {
			m.log.Info("upload: skipping %s for %s — daily upload limit reached (auto re-enables at %s)", host, filePath, hostTimedOutUntil(host).Format(time.RFC3339))
			continue
		}
		if isFleetHostBackedOff(host) {
			m.log.Info("upload: skipping %s for %s — a peer node reported this host exhausted on the shared credentials (auto re-enables at %s)", host, filePath, fleetHostBackoffUntil(host).Format(time.RFC3339))
			continue
		}
		fn, ok := m.hosts[host]
		if !ok {
			continue
		}
		var link string
		var err error
		if m.hasFullSend(filePath, host) {
			m.log.Error("upload: skipping %s for %s — file body was already fully transmitted in a previous attempt (response lost)", host, filePath)
			err = errBodyFullySent(host)
		} else {
			m.log.Info("upload: priority upload to %s for %s", host, filePath)
			link, err = fn(filePath, progressFn)
		}
		results = append(results, UploadResult{Host: host, DownloadLink: link, Error: err})
		if err != nil {
			m.log.Error("upload: %s (priority) failed for %s: %v", host, filePath, err)
			if isVoeStorageFull(err) {
				m.log.Error("upload: %s reported storage full — disabling it for the rest of this run", host)
				m.DisableHost(host)
			} else if isVidMolyDailyLimit(err) {
				m.log.Error("upload: %s hit its daily upload limit — skipping it for ~24 hours (auto re-enables)", host)
				disableHostFor(host, 24*time.Hour, "daily upload limit reached")
				m.DisableHost(host)
			} else if isKeysExhausted(err) {
				m.log.Error("upload: %s has no working keys left — skipping it for %s (auto re-enables; rotate its keys to restore)", host, keysExhaustedCooldown)
				disableHostFor(host, keysExhaustedCooldown, "all configured keys exhausted")
				m.DisableHost(host)
			} else if isUploadAuthError(err) {
				m.log.Error("upload: %s rejected our credentials — disabling it for the rest of this run", host)
				m.DisableHost(host)
			} else if isHostDead(err) {
				m.log.Error("upload: %s is permanently unreachable — disabling it for the rest of this run", host)
				m.DisableHost(host)
			} else if n := m.recordHostFailure(host); n >= failingHostsThreshold {
				m.log.Error("upload: %s failed %d files in a row — disabling it for the rest of this run", host, n)
				m.DisableHost(host)
			}
		} else {
			m.log.Info("upload: %s (priority) successful for %s: %s", host, filePath, link)
			m.recordHostSuccess(host)
			m.clearFullSend(filePath, host)
		}
	}

	if len(otherHosts) > 0 {
		otherResults := m.UploadSelected(filePath, otherHosts)
		results = append(results, otherResults...)
	}

	return results
}

// AvailableHosts returns the names of all configured upload hosts.
func (m *MultiHostUploader) AvailableHosts() []string {
	m.initHosts()
	hosts := make([]string, 0, len(m.hosts))
	for name := range m.hosts {
		hosts = append(hosts, name)
	}
	return hosts
}

// GetSuccessfulUploads returns only the successful upload results
func GetSuccessfulUploads(results []UploadResult) []UploadResult {
	var successful []UploadResult
	for _, result := range results {
		if result.Error == nil && result.DownloadLink != "" {
			successful = append(successful, result)
		}
	}
	return successful
}

// FormatResults formats upload results into a readable string
func FormatResults(results []UploadResult) string {
	var output string
	successCount := 0

	for _, result := range results {
		if result.Error == nil && result.DownloadLink != "" {
			output += fmt.Sprintf("✓ %s: %s\n", result.Host, result.DownloadLink)
			successCount++
		} else {
			output += fmt.Sprintf("✗ %s: %v\n", result.Host, result.Error)
		}
	}

	output = fmt.Sprintf("Upload completed: %d/%d successful\n%s", successCount, len(results), output)
	return output
}
