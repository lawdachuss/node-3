package channel

import (
	"sync"
	"time"
)

// Abandoned presentation assets.
//
// generateThumbnailForFile runs the thumbnail, sprite and preview goroutines in
// parallel and then collects them under ONE shared deadline
// (thumbnailAssetTimeout, 3 minutes).  Collecting must never ride along on a
// wedged image host, so an asset that overruns the budget is abandoned locally
// while its goroutine keeps running in the background — and because the
// goroutine's onHost callback fires the instant a host succeeds, the URL is
// still persisted to the DB after the stage has moved on.
//
// That is not a failure, but it was logged as one.  The abandon line went to
// errFn ("timed out after 3m0s waiting for preview asset — continuing without
// it"), which is what put 6,160 preview "failures" in the fleet's error buckets
// over 7 days while only ~22 recordings actually ended up without a thumbnail
// and the cross-host failure path fired 11 times.  The log was reporting a
// scheduling decision as an upload failure, and it recorded nothing about the
// real outcome — landed late, or never landed at all.
//
// Two changes fix that:
//
//  1. The abandon is informational.  The work is still in flight at that
//     moment, so there is nothing to report as an error yet.
//  2. trackLateAsset follows the abandoned goroutine to its END, so the actual
//     outcome is what gets recorded: a late landing is a success (and says so),
//     and the genuinely-missing case still surfaces as an error — just at the
//     point where we actually know, instead of 3 minutes early and 98% wrong.

// assetCollect pairs an asset name with the done channel its goroutine writes
// the final primary URL to ("" for "produced nothing").
type assetCollect struct {
	name string
	dst  *string
	ch   <-chan string
}

// lateAssetTrackBound caps how long the tracker waits for an abandoned asset
// before calling it genuinely missing.  It deliberately exceeds the longest
// INTERNAL asset budget — assetTimeoutCap (45m) plus the preview's 5-minute
// single-clip fallback and its subsequent encode/upload — so a tracker timeout
// means the goroutine is truly stuck rather than merely still working.
// TestLateAssetBoundOutlastsEveryAssetBudget guards that relationship.
// A var so tests can shrink it.
var lateAssetTrackBound = 120 * time.Minute

// collectAssets waits up to budget for each asset, abandoning — never killing —
// the ones that overrun it, and hands each abandoned asset to trackLateAsset.
//
// The assets are collected CONCURRENTLY under one shared deadline rather than
// one after another: the old sequential 3×10-minute collect serialized three
// waits and blew the pipeline's 15-minute stage cap on node-13 ("timed out
// after 10m0s" then "exceeded 15m0s").
//
// Abandonment is safe for the goroutines themselves.  Each is internally
// bounded (per-ffmpeg-call context timeouts, image-host HTTP client timeouts,
// bounded per-host semaphore acquires, and the first-host-success signal in
// StartAll), and each writes its final URL to a buffered (cap 1) channel, so
// nobody reading it cannot block the send — the goroutine still finishes and
// runs its own temp-file cleanup instead of leaking.
//
// collectAssets waits for the collects only.  The trackers are deliberately not
// part of that WaitGroup: they can outlive the stage by minutes, and stalling
// here is exactly what the shared deadline exists to prevent.
func collectAssets(baseName string, assets []assetCollect, budget time.Duration, info, warn, errFn func(string, ...interface{})) {
	var collectWG sync.WaitGroup

	// The deadline is a CLOSED channel, not a timer's channel.  A timer delivers
	// its single value to exactly ONE ready receiver (its channel has been
	// unbuffered since Go 1.23), so with three assets selecting on one
	// deadline only one of them was ever actually abandoned — the other two
	// sat in the collect until their goroutine finished, which is the stall
	// the shared deadline exists to prevent.  Closing broadcasts to every
	// waiter, so all three are abandoned at the same instant.
	deadline := make(chan struct{})
	deadlineTimer := time.AfterFunc(budget, func() { close(deadline) })
	defer deadlineTimer.Stop()
	for _, a := range assets {
		collectWG.Add(1)
		go func(a assetCollect) {
			defer collectWG.Done()
			select {
			case *a.dst = <-a.ch:
			case <-deadline:
				// Not a failure — the asset is still uploading.  Saying so at
				// error level is what produced the phantom 6,160; the tracker
				// records the outcome that this line can only guess at.
				info("%s: %s is still uploading after %s — collecting without it (not a failure; the late landing is tracked)",
					a.name, baseName, budget)
				go trackLateAsset(a.name, baseName, a.ch, budget, info, warn, errFn)
			}
		}(a)
	}
	collectWG.Wait()
}

// trackLateAsset waits for an abandoned asset's goroutine to finish and reports
// the real outcome.  It only ever receives from done — it must not write to the
// ThumbnailResult, which generateThumbnailForFile has already returned by value.
//
// The three outcomes are deliberately different severities, because collapsing
// them is what caused the miscount:
//
//   - landed later  → info. The asset is delivered; the collect was just
//     impatient.  This is the ~98% case for the preview.
//   - finished empty → warn, NOT error. The goroutine logs its own failure
//     ("preview: failed for …", "all hosts rejected or saturated") before it
//     sends, so an error here would double-count every genuine failure. This
//     line only CORRELATES the abandon with it.
//   - never finished → error. Nothing else will ever report it: the goroutine
//     is still inside a host wait, so its own failure line has not been
//     written. lateAssetTrackBound exceeds every internal asset budget, so
//     this means stuck rather than slow.
func trackLateAsset(assetName, baseName string, done <-chan string, abandonedAfter time.Duration, info, warn, errFn func(string, ...interface{})) {
	started := time.Now()
	timer := time.NewTimer(lateAssetTrackBound)
	defer timer.Stop()

	select {
	case url := <-done:
		elapsed := time.Since(started).Round(time.Second)
		if url == "" {
			warn("%s: %s finished empty %s after being abandoned — the asset's own failure line above is the record of it, not counted again here",
				assetName, baseName, elapsed)
			return
		}
		info("%s: ✓ %s landed late after %s (collect stopped waiting at %s — not a failure)",
			assetName, baseName, elapsed, abandonedAfter)
	case <-timer.C:
		warn("%s: %s still has no URL %s after being abandoned (background upload may still complete later)",
			assetName, baseName, lateAssetTrackBound)
	}
}
