package manager

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/server"
	"github.com/teacat/chaturbate-dvr/uploader"
)

// hostBackoffRefreshInterval is how often a node re-reads the fleet-wide upload
// host backoffs.  The table is tiny (one row per upload host) and the value only
// changes when some node discovers a shared-credential cap, so a slow poll is
// enough: the cost of being up to five minutes late is a single wasted request
// to a host that is already known-exhausted, versus a Supabase round trip on
// every file upload if this were checked live.
const hostBackoffRefreshInterval = 5 * time.Minute

// startHostBackoffSync keeps this node's view of the fleet-wide upload-host
// backoffs fresh, and installs the sink that reports one when this node
// discovers a shared-credential cap.
//
// Why the fleet needs this at all: the upload credentials are the same on every
// node, so an upload host's per-account quota is a fleet quota.
// With this table one node's discovery skips the host everywhere.
func startHostBackoffSync() {
	// Install the report sink before the first upload can run.
	uploader.SetPublishHostBackoff(PublishFleetHostBackoff)

	// Initial read before the first sweep, so a node joining the fleet does not
	// pay the discovery request itself.
	RefreshFleetHostBackoffs()

	go func() {
		ticker := time.NewTicker(hostBackoffRefreshInterval)
		defer ticker.Stop()
		for range ticker.C {
			RefreshFleetHostBackoffs()
		}
	}()
}

// lastHostBackoffLog dedupes the refresh log line: the fleet's 5,000-line log
// ring already drops ~34% of its history under load (see NODE_FLEET_FINDINGS),
// so a line every five minutes per node would be noise that hides real faults.
// Only a change in the active set is logged.
var (
	lastHostBackoffLogMu sync.Mutex
	lastHostBackoffLog   string
)

// RefreshFleetHostBackoffs pulls the shared backoff table into the uploader's
// in-process cache.  Best-effort: on a project where the migration has not been
// applied the error is swallowed and the node keeps the old per-node behaviour,
// so deploying this code before the migration cannot make uploads worse.
func RefreshFleetHostBackoffs() {
	client := server.GetDBClient()
	if client == nil {
		return
	}
	backoffs, err := client.GetHostBackoffs()
	if err != nil {
		if !database.HostBackoffTableMissing(err) {
			log.Printf("[host-backoff] could not refresh fleet host backoffs: %v", err)
		}
		return
	}
	uploader.SetFleetHostBackoffs(backoffs)

	now := time.Now()
	active := make([]string, 0, len(backoffs))
	for host, until := range backoffs {
		if until.After(now) {
			active = append(active, fmt.Sprintf("%s until %s", host, until.UTC().Format(time.RFC3339)))
		}
	}
	sort.Strings(active)
	summary := strings.Join(active, ", ")

	lastHostBackoffLogMu.Lock()
	changed := summary != lastHostBackoffLog
	lastHostBackoffLog = summary
	lastHostBackoffLogMu.Unlock()

	if !changed {
		return
	}
	if summary == "" {
		log.Printf("[host-backoff] no fleet-wide upload-host backoffs active")
		return
	}
	log.Printf("[host-backoff] fleet-wide backoffs active (reported by peer nodes): %s", summary)
}

// PublishFleetHostBackoff records a host backoff in the shared table so peer
// nodes skip the host too.  Called from the uploader on its own goroutine, so
// it must be self-contained and quick; failures are logged, never propagated
// (the node's local disable already took effect).
func PublishFleetHostBackoff(host string, until time.Time, reason string) {
	client := server.GetDBClient()
	if client == nil {
		return
	}
	nodeID := server.NodeID()
	if err := client.SetHostBackoff(host, until, reason, nodeID); err != nil {
		if !database.HostBackoffTableMissing(err) {
			log.Printf("[host-backoff] could not publish %s backoff: %v", host, err)
		}
		return
	}
	log.Printf("[host-backoff] published fleet-wide backoff for %s until %s (%s) so peer nodes skip it",
		host, until.UTC().Format(time.RFC3339), reason)
}
