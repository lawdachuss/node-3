package coordinator

import (
	"context"
	"log"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
)

// assignmentSyncInterval is how often this node reconciles the channels it is
// locally running against the DB channel_assignments rows. The DB row is the
// authority on WHICH node owns a channel; this loop obeys it — it starts
// channels assigned to this node and stops channels no longer assigned to it.
// It never moves, rebalances, or claims NEW channels, but it does defend
// in-progress recordings: a channel this node is ACTIVELY recording that gets
// reassigned away is re-pinned to this node (ReassertAssignmentNode) so the
// new owner never starts an overlapping duplicate capture.
const assignmentSyncInterval = 30 * time.Second

// StartAssignmentSyncLoop keeps the local recorder in sync with the DB
// assignments. It starts channels assigned to this node that aren't running
// yet, stops local channels no longer assigned to this node, and re-pins any
// channel this node is actively recording whose row was reassigned away.
func (c *Coordinator) StartAssignmentSyncLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	const name = "assignment-sync"

	c.runLoopWithRestart(ctx, name, assignmentSyncInterval, func(stopCh <-chan struct{}, tickerC <-chan time.Time) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-stopCh:
				return
			case <-tickerC:
				c.cycleGuardReconcile.tryRun(name, c.runAssignmentSyncCycle)
			}
		}
	})
}

// runAssignmentSyncCycle starts channels assigned to this node and stops local
// channels no longer assigned to this node. On any DB error it returns
// immediately and changes NOTHING — a transient DB hiccup must never cause us to
// drop live recordings. This loop applies DB assignment decisions; it never
// makes its own balancing moves, but it defends in-progress recordings from
// being double-captured (re-pin) and cleans up its own finished recording
// markers (the controller deliberately never resets markers on a live owner).
func (c *Coordinator) runAssignmentSyncCycle() {
	c.runAssignmentSyncCycleWith(c.Client)
}

func (c *Coordinator) runAssignmentSyncCycleWith(db *database.Client) {
	if !c.isActive() {
		return
	}
	if c.Manager == nil {
		return
	}

	dbAssignments, err := db.GetNodeAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] assignment-sync: get node assignments error: %v", err)
		return
	}

	dbMap := make(map[string]bool, len(dbAssignments))
	for _, a := range dbAssignments {
		dbMap[a.Username] = true
	}

	local := c.Manager.GetLocalChannels()
	localSet := make(map[string]bool, len(local))
	for _, lc := range local {
		localSet[lc] = true
	}

	// recordingMarked tracks (in this process) which channels this node has
	// marked status='recording' in the DB. The controller is excluded from
	// resetting markers whose owner node is alive, so the OWNER must clear its
	// own finished markers — the mark phase below resets a previously-marked
	// channel to 'claimed' exactly once, the first cycle after it stops
	// recording. Reset on a process restart: markers left by a killed process
	// age out and are cleaned once the node (or its successor session) drops
	// offline — bounded by the session lifetime.
	if c.recordingMarked == nil {
		c.recordingMarked = map[recMarkKey]bool{}
	}

	// repinned collects channels this node re-asserted ownership of this cycle,
	// so the mark phase re-affirms them as recording even though they were not
	// in this cycle's assignment fetch.
	repinned := make(map[string]bool)

	// Stop local channels no longer assigned to this node.
	for _, lc := range local {
		if dbMap[lc] {
			continue
		}
		// Never interrupt an in-progress live recording: stopping it mid-file
		// would fragment or strand the recording. Instead, defer the stop until
		// the recording ends AND, when the row was genuinely moved to another
		// node, re-pin it back to this node so the new owner never starts a
		// duplicate of the same stream.
		if !c.Manager.IsRecording(lc) {
			log.Printf("[coordinator] assignment-sync: channel %s no longer assigned to this node — stopping", lc)
			if err := c.Manager.RemoveChannelForReassignment(lc); err != nil {
				log.Printf("[coordinator] assignment-sync: remove %s error: %v", lc, err)
			}
			continue
		}

		site, ok := c.Manager.LocalChannelSite(lc)
		if !ok {
			log.Printf("[coordinator] assignment-sync: channel %s is actively recording but its site is unknown — deferring stop", lc)
			continue
		}

		// Fetch the current row to learn WHY the channel left our assignment
		// list. Only a row assigned to ANOTHER node is a reassignment that we
		// must fight: this node holds the actual recording, so ownership should
		// not have moved. A row released to unassigned (user pause/removal,
		// Cloudflare shed) or deleted is respected — we finish the current file
		// and are removed by a later cycle.
		row, err := db.GetAssignment(lc, site)
		if err != nil {
			log.Printf("[coordinator] assignment-sync: channel %s is actively recording — re-pin check error (deferring stop): %v", lc, err)
			continue
		}
		switch {
		case row == nil:
			log.Printf("[coordinator] assignment-sync: channel %s is actively recording but its row is gone — deferring stop until recording ends", lc)
		case row.AssignedNode == "" || row.AssignedNode == c.NodeID:
			log.Printf("[coordinator] assignment-sync: channel %s is actively recording — deferring stop until recording ends", lc)
		default:
			// Reassigned mid-recording: re-assert ownership so the destination
			// node does not start a duplicate. The controller never moves
			// status='recording' rows on purpose, so this is a stale decision
			// (expired recording lease, external edit) — the recording itself
			// is the pin and this node is the one writing the file.
			// Conditional on the observed owner (row.AssignedNode): if the row
			// moved again between the GET above and this PATCH — e.g. a user
			// pause released it to NULL — the PATCH matches zero rows and we
			// must NOT resurrect a released channel.
			if err := db.ReassertAssignmentNode(lc, site, c.NodeID, row.AssignedNode); err != nil {
				log.Printf("[coordinator] assignment-sync: re-pin %s to this node error (deferring stop): %v", lc, err)
				continue
			}
			repinned[lc] = true
			log.Printf("[coordinator] assignment-sync: channel %s actively recording but reassigned to %s — re-pinned to this node so the new owner will not start a duplicate", lc, row.AssignedNode)
		}
	}

	// Start channels assigned to this node that aren't running locally yet.
	for _, a := range dbAssignments {
		if !localSet[a.Username] {
			if err := c.Manager.CreateChannelFromAssignment(&a); err != nil {
				log.Printf("[coordinator] assignment-sync: start %s error: %v", a.Username, err)
			}
		}
	}

	// Re-affirm "recording" status for channels this node is actively recording
	// and still owns. This is the only place that sets status='recording' now
	// that the controller owns liveness/assignment: it tells the controller's
	// rebalancer (which refuses to move status='recording' rows) not to yank an
	// in-progress recording. Only rows this node still owns (assigned to it, or
	// re-pinned this cycle) are (re)marked — a row that moved to another node
	// must NOT be refreshed by us, or our heartbeat would make the new owner
	// look like it is recording and block the re-pin.
	idleOwned := make([]database.ChannelAssignment, 0)
	for _, lc := range local {
		if !dbMap[lc] && !repinned[lc] {
			continue
		}
		site, ok := c.Manager.LocalChannelSite(lc)
		if !ok {
			continue
		}
		if !c.Manager.IsRecording(lc) {
			// Owned but idle: if this node marked it 'recording' earlier in this
			// process, the broadcast has ended and the marker is ours to clear.
			// The controller never resets markers on a live owner, so without
			// this the finished recording would stay status='recording' with a
			// stale is_live until the node goes offline.
			key := recMarkKey{site: site, username: lc}
			if c.recordingMarkerIs(key) {
				if err := db.SetAssignmentStatus(lc, site, "claimed"); err != nil {
					log.Printf("[coordinator] assignment-sync: reset finished recording %s error: %v", lc, err)
				}
				c.clearRecordingMarker(key)
			}
			// Owned but idle: refresh last_recorded_at so the row's "last capture"
			// timestamp reflects a live owner proving the channel is healthy-idle,
			// not abandoned. mark_channel_recording only fires while actually
			// recording, so without this the timestamp freezes at the end of the
			// last session and staleness is indistinguishable from a dead node.
			// The RPC is owner-filtered server-side (assigned_node=this node AND
			// status='claimed'), so a row released or reassigned mid-cycle is left
			// untouched and this can never pin a channel we no longer own.
			idleOwned = append(idleOwned, database.ChannelAssignment{Username: lc, Site: site})
			continue
		}
		if err := db.MarkChannelRecording(lc, site); err != nil {
			log.Printf("[coordinator] assignment-sync: mark recording error for %s: %v", lc, err)
			continue
		}
		c.setRecordingMarker(recMarkKey{site: site, username: lc})
	}

	// One bulked RPC for the whole owned-idle set instead of a PATCH per
	// channel — with ~55 claimed rows per node at a 30s sync interval this
	// keeps the touch to a single request per cycle.
	if len(idleOwned) > 0 {
		if err := db.TouchChannelRecordings(c.NodeID, idleOwned); err != nil {
			log.Printf("[coordinator] assignment-sync: touch owned-idle last_recorded_at error: %v", err)
		}
	}

	// Ghost-marker cleanup: a marker this process set whose row moved to another
	// node while we could not re-pin it (the re-pin PATCH fails when the DB is
	// unreachable — the same wedge that let the lease reset move the row). The
	// controller never resets markers on a live owner, so without this the row
	// would sit status='recording' on the new owner for its whole session. We
	// only clear it when the new owner is NOT actively capturing: the row must
	// still say 'recording' with a STALE heartbeat. A fresh heartbeat means the
	// new owner's own mark phase is refreshing it — clearing it then would
	// unpin ITS live capture and hand the row to a third node mid-file.
	c.recordingMarkedMu.Lock()
	ghostKeys := make([]recMarkKey, 0, len(c.recordingMarked))
	for key := range c.recordingMarked {
		ghostKeys = append(ghostKeys, key)
	}
	c.recordingMarkedMu.Unlock()
	for _, key := range ghostKeys {
		if dbMap[key.username] || repinned[key.username] {
			continue // still ours — the main loop maintains this row
		}
		if c.Manager.IsRecording(key.username) {
			continue // still capturing locally — the marker stays
		}
		row, err := db.GetAssignment(key.username, key.site)
		if err != nil {
			log.Printf("[coordinator] assignment-sync: ghost marker check %s error: %v", key.username, err)
			continue
		}
		if row == nil {
			c.clearRecordingMarker(key) // row gone — marker is moot
			continue
		}
		if row.AssignedNode == c.NodeID {
			continue // ours again — the main loop owns it
		}
		if row.Status == "recording" && !recordingLeaseFresh(row.LastHeartbeat, time.Now()) {
			if err := db.SetAssignmentStatus(key.username, key.site, "claimed"); err != nil {
				log.Printf("[coordinator] assignment-sync: clear ghost marker %s error: %v", key.username, err)
				continue
			}
			log.Printf("[coordinator] assignment-sync: cleared stale ghost recording marker %s (row now on %s)", key.username, row.AssignedNode)
		}
		c.clearRecordingMarker(key)
	}
}
