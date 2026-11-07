package uploader

import (
	"sync"
	"time"
)

// Per-host concurrency caps.  These are the primary throughput throttle for
// uploads: each host only accepts a bounded number of simultaneous requests
// before it starts rate-limiting (HTTP 429).  The defaults are tuned for
// maximum throughput while staying under the rate-limit thresholds; when a
// host does respond 429 the uploader already backs off and retries.
const (
	// defaultGoFileConcurrency is higher because GoFile's fleet tolerates
	// more parallel uploads than the smaller file hosts.
	defaultGoFileConcurrency = 24
	// defaultHostConcurrency applies to every other configured host.
	defaultHostConcurrency = 16
)

var (
	hostSemMu sync.RWMutex
	hostSems  = map[string]chan struct{}{
		// Video upload hosts
		"GoFile":     make(chan struct{}, defaultGoFileConcurrency),
		"VOE.sx":     make(chan struct{}, defaultHostConcurrency),
		"Streamtape": make(chan struct{}, defaultHostConcurrency),
		"Mixdrop":    make(chan struct{}, defaultHostConcurrency),
		"Vidara":     make(chan struct{}, defaultHostConcurrency),
		"LuluStream": make(chan struct{}, defaultHostConcurrency),
		"DoodStream": make(chan struct{}, defaultHostConcurrency),
		"AnonMP4":    make(chan struct{}, defaultHostConcurrency),
		// Image upload hosts (thumbnails, sprites, previews)
		"Catbox":         make(chan struct{}, defaultHostConcurrency),
		"Pixhost":        make(chan struct{}, defaultHostConcurrency),
		"freeimage.host": make(chan struct{}, defaultHostConcurrency),
		"ImgChest":       make(chan struct{}, defaultHostConcurrency),
		"Imgbox":         make(chan struct{}, defaultHostConcurrency),
		"ImgBB":          make(chan struct{}, defaultHostConcurrency),
	}
)

// hostSemAcquireTimeout bounds how long acquireHostSem waits for a free
// upload slot on a saturated host before declaring that host "busy" for the
// file.  Bounded acquisition is mandatory: an unbounded block (the pre-fix
// behavior) let one slow/torpedoed host queue park asset-generation and
// pipeline goroutines forever — observed on node-13 as 10-15 minute
// thumbnail-stage stalls and "proceeding without thumbnails".  When a host
// is saturated we degrade gracefully (skip the host for this file; the
// pipeline/backfill retries later) instead of parking a goroutine
// indefinitely.
const hostSemAcquireTimeout = 2 * time.Minute

// acquireHostSem waits up to hostSemAcquireTimeout for an upload slot on the
// named host.  It returns a non-nil release func (release, true) when a slot
// is acquired, or (nil, false) when the host stayed saturated for the whole
// budget — callers must then fail the host for this file (a transient
// "busy" error → the pipeline progresses and the file is retried/backfilled
// later).  Unknown hosts return a no-op release with ok=true so they are
// never accidentally throttled or failed.
func acquireHostSem(host string) (release func(), ok bool) {
	hostSemMu.RLock()
	sem := hostSems[host]
	hostSemMu.RUnlock()
	if sem == nil {
		return func() {}, true
	}
	// Fast path: slot immediately free.
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, true
	default:
	}
	timer := time.NewTimer(hostSemAcquireTimeout)
	defer timer.Stop()
	select {
	case sem <- struct{}{}:
		return func() { <-sem }, true
	case <-timer.C:
		return nil, false
	}
}

// SetHostConcurrency replaces the per-host upload semaphores with a new
// capacity so operators can trade concurrency for rate-limit headroom.
// Call at startup before any upload goroutines are running.
func SetHostConcurrency(perHost int) {
	if perHost <= 0 {
		return
	}
	hostSemMu.Lock()
	defer hostSemMu.Unlock()
	for h := range hostSems {
		hostSems[h] = make(chan struct{}, perHost)
	}
}

// SetHostConcurrencyTiered replaces the per-host upload semaphores with
// distinct capacities: GoFile (whose fleet tolerates more parallel uploads)
// gets goFile slots, every other host gets other.  Used by the VM-sized
// startup tuning (config.VMSizedConcurrency); an operator who sets
// --upload-host-concurrency explicitly uses SetHostConcurrency instead.
// Call at startup before any upload goroutines are running.
func SetHostConcurrencyTiered(goFile, other int) {
	if goFile <= 0 {
		goFile = defaultGoFileConcurrency
	}
	if other <= 0 {
		other = defaultHostConcurrency
	}
	hostSemMu.Lock()
	defer hostSemMu.Unlock()
	hostSems["GoFile"] = make(chan struct{}, goFile)
	for h := range hostSems {
		if h != "GoFile" {
			hostSems[h] = make(chan struct{}, other)
		}
	}
}
