package uploader

import (
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// MultiImageUploader uploads images to configured hosts in linear fallback
// order: Catbox.moe → Pixhost.to → freeimage.host.  Each host gets at most
// 3 attempts.
//
// Sequential fallback is preferred over parallel upload because:
//   - Pixhost supports JPEG, PNG, GIF, WebP, and AVIF (all formats we
//     generate), so it succeeds in the vast majority of cases without
//     wasting API calls.
//   - Parallel uploads triggered unnecessary rate limiting and made errors
//     harder to diagnose.
//
// All three hosts support adult/NSFW content and provide permanent hosting.
// freeimage.host is the last resort before giving up (64 MB limit, no API
// key needed, permanent storage, adult-friendly).
//
// Format-specific host skipping:
//   - Imgbox only accepts JPG, PNG, GIF — .webp files are automatically
//     skipped to avoid wasted API calls.
type MultiImageUploader struct {
	catbox    *CatboxUploader
	pixhost   *ThumbnailUploader
	freeimage *FreeImageHostUploader
	imgchest  *ImgChestUploader
	imgbox    *ImgboxUploader
	imgbb     *ImgBBUploader
	imgpile   *ImgPileUploader
	skipHosts map[string]bool // hosts to skip in UploadToAll
}

// SetSkipHosts marks specific hosts to skip in UploadToAll.
func (m *MultiImageUploader) SetSkipHosts(hosts ...string) {
	if m.skipHosts == nil {
		m.skipHosts = make(map[string]bool)
	}
	for _, h := range hosts {
		m.skipHosts[h] = true
	}
}

// hostSupportsExtension reports whether the given host accepts files with
// the specified extension.  This avoids wasting API calls on hosts that
// are known to reject certain formats.
func hostSupportsExtension(host, ext string) bool {
	// Imgbox only accepts JPG, PNG, GIF — no WebP.
	if host == "Imgbox" && strings.EqualFold(ext, ".webp") {
		return false
	}
	return true
}

// NewMultiImageUploader creates a new image uploader that uploads to
// Catbox.moe, Pixhost.to, and freeimage.host (fallback order).
func NewMultiImageUploader() *MultiImageUploader {
	return &MultiImageUploader{
		catbox:    NewCatboxUploader(),
		pixhost:   NewThumbnailUploader(""),
		freeimage: NewFreeImageHostUploader(),
		imgchest:  NewImgChestUploader(),
		imgbox:    NewImgboxUploader(),
		imgbb:     NewImgBBUploader(),
		imgpile:   NewImgPileUploader(),
	}
}

// uploadWithRetries tries fn up to maxAttempts times with exponential
// backoff and returns the result of the first successful call.  If a single
// attempt fails because the host is dead or rate-limiting us, it bails
// (no pointless retries on that host) so the caller's fallback chain can
// move on to the next host.
//
// Rate limits are time-windowed (e.g. ImgBB = N uploads/hour, Pixhost =
// burst limits), so a rate-limited attempt is retried with a short delay the
// first time to give the window a chance to open — but never more than
// maxRateRetry attempts, after which the host is abandoned for this file.
func uploadWithRetries(maxAttempts int, label string, fn func() (string, error)) (url string, err error) {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(1<<attempt) * time.Second)
		}
		u, e := fn()
		if e == nil {
			return u, nil
		}
		lastErr = e
		// Replace an instant-bail on a rate limit with a single short-delay
		// retry: rate limits are transient and the delay may let the window
		// reopen, avoiding a spurious "all hosts rejected" for the whole file.
		if isFailFastError(e) && !isUploadRateLimited(e) {
			return "", e
		}
	}
	return "", lastErr
}

// ImageUploadResult contains the result of an image upload to a specific host.
type ImageUploadResult struct {
	Host string
	URL  string
	Err  error
}

// Upload returns the first successful direct image URL. Order: Catbox.moe
// FIRST, then Pixhost, then freeimage.host as fallbacks.
//
// All three hosts support adult/NSFW content and provide permanent hosting.
// On the GitHub Actions runner Catbox MUST be reached through CATBOX_PROXY_URL
// (a Cloudflare Worker), which leaves from Cloudflare's edge IPs that Catbox
// does not block — making it the only image host that reliably succeeds from
// the datacenter runner IPs (Pixhost.to and freeimage.host otherwise
// block/rate-limit the runner). They are kept only as fallbacks for the rare
// case the proxy/Catbox is unavailable.
func (m *MultiImageUploader) Upload(filePath string) (url, host string, err error) {
	// Catbox.moe first — routed through CATBOX_PROXY_URL on the runner so it
	// succeeds from datacenter IPs where other hosts are blocked.
	url, err = uploadWithRetries(3, "Catbox", func() (string, error) {
		return m.catbox.Upload(filePath)
	})
	if err == nil {
		return url, "Catbox", nil
	}
	catboxErr := err

	url, err = uploadWithRetries(3, "Pixhost", func() (string, error) {
		return m.pixhost.Upload(filePath)
	})
	if err == nil {
		return url, "Pixhost", nil
	}
	pixhostErr := err

	// freeimage.host — only if a real account API key is configured. The
	// shared guest key is rejected with HTTP 403 ("requires authentication"),
	// so without a key this host is skipped rather than retried pointlessly.
	freeimageErr := fmt.Errorf("freeimage.host: FREEIMAGEHOST_API_KEY not set")
	if m.freeimage.HasToken() {
		url, err = uploadWithRetries(3, "freeimage.host", func() (string, error) {
			return m.freeimage.Upload(filePath)
		})
		if err == nil {
			return url, "freeimage.host", nil
		}
		freeimageErr = err
	}

	// ImgChest — only if token is configured.  Its 10 GB account storage
	// quota is a fleet-wide cap on the shared token (fleet sweep 2026-10-20:
	// every node rediscovered "You've used 42.4 GB of your 10 GB storage"
	// once per session); the first strike disables it for ~24h fleet-wide so
	// peers skip it too instead of re-paying one request each.
	imgchestErr := fmt.Errorf("imgchest: IMGCHEST_TOKEN not set")
	if m.imgchest.HasToken() {
		url, err = m.tryImgChest(filePath)
		if err == nil {
			return url, "ImgChest", nil
		}
		imgchestErr = err

		// ImgPile — token host, NSFW-friendly (only if key is configured)
		imgpileErr := fmt.Errorf("imgpile: IMGPILE_KEY not set")
		if m.imgpile.HasToken() {
			url, err = uploadWithRetries(3, "ImgPile", func() (string, error) {
				return m.imgpile.Upload(filePath)
			})
			if err == nil {
				return url, "ImgPile", nil
			}
			imgpileErr = err
		}

		// Imgbox — last resort (skip for .webp which Imgbox rejects)
		if hostSupportsExtension("Imgbox", filepath.Ext(filePath)) {
			url, err = uploadWithRetries(3, "Imgbox", func() (string, error) {
				return m.imgbox.Upload(filePath)
			})
			if err == nil {
				return url, "Imgbox", nil
			}
		}
		imgboxErr := err

		// ImgBB — absolute last resort
		if m.imgbb.keys.count() > 0 {
			url, err = uploadWithRetries(3, "ImgBB", func() (string, error) {
				return m.imgbb.Upload(filePath)
			})
			if err == nil {
				return url, "ImgBB", nil
			}
			imgbbErr := err
			return "", "", fmt.Errorf("catbox: %w (pixhost: %v, freeimage.host: %v, imgchest: %v, imgpile: %v, imgbox: %v, imgbb: %v)", catboxErr, pixhostErr, freeimageErr, imgchestErr, imgpileErr, imgboxErr, imgbbErr)
		}
		return "", "", fmt.Errorf("catbox: %w (pixhost: %v, freeimage.host: %v, imgchest: %v, imgpile: %v, imgbox: %v)", catboxErr, pixhostErr, freeimageErr, imgchestErr, imgpileErr, imgboxErr)
	}

	// ImgPile — token host, tried when ImgChest is not configured
	imgpileErr := fmt.Errorf("imgpile: IMGPILE_KEY not set")
	if m.imgpile.HasToken() {
		url, err = uploadWithRetries(3, "ImgPile", func() (string, error) {
			return m.imgpile.Upload(filePath)
		})
		if err == nil {
			return url, "ImgPile", nil
		}
		imgpileErr = err
	}

	// Imgbox — fallback when ImgChest is not configured (skip for .webp)
	if hostSupportsExtension("Imgbox", filepath.Ext(filePath)) {
		url, err = uploadWithRetries(3, "Imgbox", func() (string, error) {
			return m.imgbox.Upload(filePath)
		})
		if err == nil {
			return url, "Imgbox", nil
		}
	}
	imgboxErr := err

	// ImgBB — absolute last resort
	if m.imgbb.keys.count() > 0 {
		url, err = uploadWithRetries(3, "ImgBB", func() (string, error) {
			return m.imgbb.Upload(filePath)
		})
		if err == nil {
			return url, "ImgBB", nil
		}
		imgbbErr := err
		return "", "", fmt.Errorf("catbox: %w (pixhost: %v, freeimage.host: %v, imgpile: %v, imgbox: %v, imgbb: %v)", catboxErr, pixhostErr, freeimageErr, imgpileErr, imgboxErr, imgbbErr)
	}

	return "", "", fmt.Errorf("catbox: %w (pixhost: %v, freeimage.host: %v, imgpile: %v, imgbox: %v)", catboxErr, pixhostErr, freeimageErr, imgpileErr, imgboxErr)
}

// tryImgChest runs the ImgChest leg of the image fallback chain with the
// fleet-quota guard around it:
//   - a host already backed off (this node's timed disable OR a peer's
//     published fleet backoff) is skipped WITHOUT a request;
//   - an account storage-quota rejection publishes a ~24h fleet-wide backoff
//     (disableHostFor → Supabase host_backoffs) so the other 17 nodes stop
//     rediscovering the dead quota once per session.
//
// Extracted as a seam so the quota wiring is testable without the upstream
// image hosts (which would otherwise hit the network first).
func (m *MultiImageUploader) tryImgChest(filePath string) (string, error) {
	if !m.imgchest.HasToken() {
		return "", fmt.Errorf("imgchest: IMGCHEST_TOKEN not set")
	}
	if isHostTimedOut("ImgChest") || isFleetHostBackedOff("ImgChest") {
		err := fmt.Errorf("imgchest: skipped — storage quota exhausted on the shared account (auto re-enables at %s)", hostTimedOutUntil("ImgChest").Format(time.RFC3339))
		log.Printf("upload: %v", err)
		return "", err
	}
	url, err := uploadWithRetries(3, "ImgChest", func() (string, error) {
		return m.imgchest.Upload(filePath)
	})
	if err == nil {
		return url, nil
	}
	// The quota is unrecoverable within a session — publish the fleet-wide
	// backoff so every node stops attempting ImgChest instead of each
	// rediscovering the dead quota once (uploadWithRetries already bailed
	// after one attempt: the 403 is a fail-fast error).
	if isImgChestStorageFull(err) {
		log.Printf("upload: ImgChest storage quota exceeded — disabling it for ~24 hours fleet-wide (free storage or raise the quota to restore early)")
		disableHostFor("ImgChest", 24*time.Hour, "storage quota exceeded")
	}
	return "", err
}

// OnSuccessFunc is called the moment a single host finishes uploading,
// receiving the host name and URL.  Used to persist metadata (Supabase
// PATCH) immediately instead of waiting for every host to finish.
type OnSuccessFunc func(host, url string)

// UploadToAll uploads the image to ALL configured hosts in parallel.
// Returns a slice of results, one per host. The caller should pick the
// first successful URL for the primary thumbnail_url, and can optionally
// store additional mirror URLs for redundancy.
//
// If onHost is non-nil it is called the instant each host succeeds,
// before the next host finishes — so the caller can persist the URL
// (e.g. Supabase upsert) without waiting for slower hosts.
//
// This provides maximum redundancy: even if one host goes down, the
// thumbnail is still available on the others.
func (m *MultiImageUploader) UploadToAll(filePath string, onHost OnSuccessFunc) []ImageUploadResult {
	sess := m.StartAll(filePath, onHost)
	sess.Wait()
	return sess.snapshot()
}

type hostJob struct {
	name string
	fn   func(string) (string, error)
}

// configuredImageHosts returns the host jobs for filePath, skipping hosts in
// skipHosts and hosts that reject the file's format.  Shared by UploadToAll
// and StartAll so the parallel-with-background mirror set and the
// fire-and-forget primary path always upload to the identical host list.
func (m *MultiImageUploader) configuredImageHosts(filePath string) []hostJob {
	jobs := []hostJob{}
	addJob := func(name string, fn func(string) (string, error)) {
		if m.skipHosts != nil && m.skipHosts[name] {
			return
		}
		jobs = append(jobs, hostJob{name, fn})
	}
	addJob("Catbox", m.catbox.Upload)
	addJob("Pixhost", m.pixhost.Upload)
	if m.freeimage.HasToken() {
		addJob("freeimage.host", m.freeimage.Upload)
	}
	if m.imgchest.HasToken() {
		addJob("ImgChest", m.imgchest.Upload)
	}
	if m.imgpile.HasToken() {
		addJob("ImgPile", m.imgpile.Upload)
	}
	// Skip Imgbox for .webp files — Imgbox only accepts JPG, PNG, GIF.
	if hostSupportsExtension("Imgbox", filepath.Ext(filePath)) {
		addJob("Imgbox", m.imgbox.Upload)
	}
	if m.imgbb.keys.count() > 0 {
		addJob("ImgBB", m.imgbb.Upload)
	}
	return jobs
}

// imageUploadSession tracks a set of in-flight parallel host uploads started
// with MultiImageUploader.StartAll.  It decouples "primary URL known" from
// "all mirrors done": WaitForPrimary returns the instant the primary URL is
// available while the remaining host uploads (mirrors) continue in the
// background, still firing onHost so the DB keeps gaining mirrors.  Wait
// blocks until every host has finished (each job is bounded by the per-host
// HTTP client timeout, retry cap and the new host-sem acquire timeout, so
// Wait itself is finite).
type imageUploadSession struct {
	done    <-chan struct{}
	primary chan ImageUploadResult // first preferred-host success
	any     chan ImageUploadResult // first other-host success

	mu   sync.Mutex
	all  []ImageUploadResult // every job's outcome, one per host
}

// primaryHostWait is how long WaitForPrimary waits for a preferred host
// (Catbox → Pixhost → freeimage.host) to succeed before settling for the
// first success from any other host.
const primaryHostWait = 15 * time.Second

// primaryHostOverallTimeout is the absolute budget for WaitForPrimary.  It
// MUST be far smaller than the thumbnailAssetTimeout the pipeline allows for
// the whole asset, so a round of dead hosts fails the primary fast and the
// pipeline moves on (mirrors keep running in the background either way).
const primaryHostOverallTimeout = 60 * time.Second

// StartAll begins uploading filePath to all configured hosts in parallel.
// The returned session lets the caller wait for the first usable URL without
// blocking on the slowest mirror host; every host still fires onHost on
// success.  The session's mirror goroutines keep running after WaitForPrimary
// returns, so callers must NOT delete the source file until Wait() returns.
func (m *MultiImageUploader) StartAll(filePath string, onHost OnSuccessFunc) *imageUploadSession {
	jobs := m.configuredImageHosts(filePath)
	sess := &imageUploadSession{
		primary: make(chan ImageUploadResult, 1),
		any:     make(chan ImageUploadResult, 1),
	}

	var (
		wg       sync.WaitGroup
		primOnce sync.Once
		anyOnce  sync.Once
	)
	for _, job := range jobs {
		wg.Add(1)
		go func(j hostJob) {
			defer wg.Done()
			url, err := uploadWithRetries(3, j.name, func() (string, error) {
				return j.fn(filePath)
			})
			if err != nil {
				log.Printf("UploadToAll: %s failed for %s: %v", j.name, filepath.Base(filePath), err)
			}
			if err == nil && url != "" {
				if onHost != nil {
					onHost(j.name, url)
				}
				if isPreferredPrimaryHost(j.name) {
					primOnce.Do(func() { sess.primary <- ImageUploadResult{Host: j.name, URL: url} })
				} else {
					anyOnce.Do(func() { sess.any <- ImageUploadResult{Host: j.name, URL: url} })
				}
			}
			sess.mu.Lock()
			sess.all = append(sess.all, ImageUploadResult{
				Host: j.name,
				URL:  url,
				Err:  err,
			})
			sess.mu.Unlock()
		}(job)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	sess.done = done
	return sess
}

// WaitForPrimary returns as soon as a usable primary URL is known: it waits
// up to primaryHostWait for a preferred host (Catbox → Pixhost →
// freeimage.host), then falls back to the first success from any other host,
// and finally gives up on a global budget rather than ever waiting for the
// slowest host.  Mirrors keep uploading in the background regardless.  ok is
// false only when no host succeeded within the budget (caller should treat
// the asset's upload as failed this round).
func (s *imageUploadSession) WaitForPrimary() (url, host string, ok bool) {
	flush := func() (string, string, bool) {
		select {
		case r := <-s.primary:
			return r.URL, r.Host, true
		case r := <-s.any:
			return r.URL, r.Host, true
		default:
			return "", "", false
		}
	}
	// Wait a short grace window for a preferred host — its success yields the
	// best primary (the host the UI/CDNs serve best).  A faster non-preferred
	// success in the same window is still accepted; we never deliberately stall.
	select {
	case r := <-s.primary:
		return r.URL, r.Host, true
	case r := <-s.any:
		return r.URL, r.Host, true
	case <-s.done:
		// All hosts already finished — flush any success that slipped in.
		return flush()
	case <-time.After(primaryHostWait):
		select {
		case r := <-s.primary:
			return r.URL, r.Host, true
		case r := <-s.any:
			return r.URL, r.Host, true
		case <-s.done:
			return flush()
		case <-time.After(primaryHostOverallTimeout):
			// Hard stop: never let a round of dead hosts eat the whole asset
			// budget.  The pipeline retries/backfills; mirrors keep going.
			return "", "", false
		}
	}
}

// Wait blocks until every host upload in the session has finished.  Each job
// is bounded (per-host HTTP client timeout, retry cap, bounded host-sem
// acquire), so this never hangs; callers use it only to know when they may
// delete the temporary source file.
func (s *imageUploadSession) Wait() {
	<-s.done
}

// snapshot returns one result per host (successes AND failures), valid once
// Wait has returned.
func (s *imageUploadSession) snapshot() []ImageUploadResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]ImageUploadResult, len(s.all))
	copy(out, s.all)
	return out
}

// isPreferredPrimaryHost reports whether the named host should be preferred
// as the primary URL.  These three are the reliable, permanent image hosts
// the pipeline tries in order; their success within the grace window beats a
// fast success from a fallback-only host (ImgChest/ImgPile/Imgbox/ImgBB).
func isPreferredPrimaryHost(host string) bool {
	switch host {
	case "Catbox", "Pixhost", "freeimage.host":
		return true
	}
	return false
}

// UploadToAllURLs is a convenience wrapper that returns only the successful
// URLs from UploadToAll, keyed by host name.  If onHost is non-nil it is
// called the instant each host succeeds (see UploadToAll).
func (m *MultiImageUploader) UploadToAllURLs(filePath string, onHost OnSuccessFunc) map[string]string {
	results := m.UploadToAll(filePath, onHost)
	urls := make(map[string]string)
	for _, r := range results {
		if r.Err == nil && r.URL != "" {
			urls[r.Host] = r.URL
		}
	}
	return urls
}
