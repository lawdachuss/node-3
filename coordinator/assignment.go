package coordinator

import (
	"encoding/json"
	"log"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// ReleaseChannel releases a single channel back to the pool.
// Called when a channel is paused or deleted.
func (c *Coordinator) ReleaseChannel(username, site string) {
	if !c.IsPooled() || c.Client == nil {
		return
	}
	if err := c.Client.ReleaseChannel(username, site); err != nil {
		log.Printf("[coordinator] error releasing channel %s/%s: %v", site, username, err)
	}
}

// DeleteChannelAssignment removes a channel from the shared pool entirely.
// Called when an operator deletes a channel from a node dashboard.
func (c *Coordinator) DeleteChannelAssignment(username, site string) error {
	if !c.IsPooled() || c.Client == nil {
		return nil
	}
	// Deliberately DeleteAssignment, NOT ReleaseChannel: releasing only clears
	// assigned_node/status='unassigned', and the controller re-claims the row
	// onto some node on its next cycle — so the "deleted" channel was recorded
	// again about a minute later.  Deleting the row is what makes the operator's
	// manual delete stick (the same thing the pool editor's Remove does).
	return c.Client.DeleteAssignment(username, site)
}

// CreateChannelAssignment creates a channel_assignments row for a new channel.
// The row is created with status='unassigned' so any node can claim it.
func (c *Coordinator) CreateChannelAssignment(conf *entity.ChannelConfig) error {
	if !c.IsPooled() || c.Client == nil {
		return nil
	}

	ca := database.ChannelAssignment{
		Username:                conf.Username,
		Site:                    conf.Site,
		Status:                  "unassigned",
		IsLive:                  false,
		Framerate:               conf.Framerate,
		Resolution:              conf.Resolution,
		Pattern:                 conf.Pattern,
		MaxDuration:             conf.MaxDuration,
		MaxFilesize:             conf.MaxFilesize,
		Compress:                conf.Compress,
		MinDurationBeforeUpload: conf.MinDurationBeforeUpload,
	}

	if err := c.Client.EnsureChannelExists(conf.Username); err != nil {
		// Best-effort only: the catalogue row keeps the channels table listing
		// every pooled channel (external tools and the isolated-mode duplicate
		// check read it), but it is no longer load-bearing — the controller no
		// longer prunes assignments against the catalogue — so a catalogue
		// hiccup must never block adding a channel.
		log.Printf("[coordinator] register %s/%s in channels catalogue: %v (non-fatal)", conf.Site, conf.Username, err)
	}

	if err := c.Client.BulkInsertAssignments([]database.ChannelAssignment{ca}); err != nil {
		return err
	}

	// IMPORTANT: do NOT claim the new channel to THIS node. The row is created
	// with status='unassigned', and the leader-elected controller's balanceSite
	// sweep claims every unassigned channel into the fleet-wide equal split on
	// its very next cycle. Claiming here would pin the channel to whichever node
	// happened to add it — and if it goes live and records before the controller
	// rebalances, it would be stuck there, breaking the even distribution.
	log.Printf("[coordinator] created channel %s/%s (unassigned) — controller will assign it", conf.Site, conf.Username)

	return nil
}

// ConfigFromAssignment converts a ChannelAssignment back to a ChannelConfig.
func ConfigFromAssignment(ca *database.ChannelAssignment) *entity.ChannelConfig {
	conf := &entity.ChannelConfig{
		Site:                    ca.Site,
		Username:                ca.Username,
		Framerate:               ca.Framerate,
		Resolution:              ca.Resolution,
		Pattern:                 ca.Pattern,
		MaxDuration:             ca.MaxDuration,
		MaxFilesize:             ca.MaxFilesize,
		Compress:                ca.Compress,
		MinDurationBeforeUpload: ca.MinDurationBeforeUpload,
		CreatedAt:               time.Now().Unix(),
	}
	conf.Sanitize()
	return conf
}

// ReleaseDeadRunRecording makes the DB row of a just-cleared DEAD recording
// claimable again. Wired as Manager.ReleaseHook in main.go: after the
// stuck-recording watchdog's dead-run pass closed and deleted a wedged 0-byte
// file, the channel's row still says status='recording' — and ReclaimChannels
// skips 'recording' rows while the controller's stale-marker reset
// (ResetStaleRecordingAssignments) deliberately spares protected (alive) nodes.
// Without an owner-side release the cleared wedge burns a slot until the node
// restarts (2026-10-17 sweep: 12/11/8 ghost slots on node-4/8/16).
//
// Async: the watchdog sweep must never block on the DB. All real work is in
// runDeadRunReleaseWith, which is synchronous so it can be unit-tested.
func (c *Coordinator) ReleaseDeadRunRecording(username string) {
	if !c.IsPooled() || c.Client == nil {
		return
	}
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[coordinator] dead-run release %s panic recovered: %v", username, r)
			}
		}()
		c.runDeadRunReleaseWith(username, c.Client)
	}()
}

// runDeadRunReleaseWith resets this node's own stale 'recording' marker for a
// dead-run channel. Guarded in three ways so a racing controller can never be
// fought: the row must still be assigned to THIS node (a row the controller
// reassigned in the seconds since the kill belongs to the new owner — resetting
// it would un-pin that node's live capture), the row must still say
// 'recording' (an already-reset row is owned by the sync loop's idle phase and
// re-resetting it is meaningless churn), and the channel must not have started
// recording again locally (the stream returned; the mark phase is re-claiming
// a genuinely live capture). The process' own heartbeat marker is dropped in
// every case so the next assignment-sync cycle never re-marks a released row.
func (c *Coordinator) runDeadRunReleaseWith(username string, db *database.Client) {
	if !c.isActive() || db == nil || c.Manager == nil {
		return
	}
	site, ok := c.Manager.LocalChannelSite(username)
	if !ok {
		// The channel object is gone entirely (removed on stop); there is no
		// site key left to address the row by, so leave the row to the
		// ordinary staleness reset on the next cycle.
		return
	}
	c.clearRecordingMarker(recMarkKey{site: site, username: username})

	if c.Manager.IsRecording(username) {
		return // the stream came back between the clear and this release
	}
	row, err := db.GetAssignment(username, site)
	if err != nil {
		log.Printf("[coordinator] dead-run release %s/%s: fetch row error (row stays for the staleness reset): %v", site, username, err)
		return
	}
	if row == nil || row.AssignedNode == "" || row.AssignedNode != c.NodeID {
		return // gone, unassigned, or moved — not this node's marker to clear
	}
	if row.Status != "recording" {
		return // already reset by the sync loop — nothing to release
	}
	if err := db.SetAssignmentStatus(username, site, "claimed"); err != nil {
		log.Printf("[coordinator] dead-run release %s/%s: reset error (row stays for the staleness reset): %v", site, username, err)
		return
	}
	log.Printf("[coordinator] dead-run release: %s/%s was a wedged dead run — status reset to 'claimed', slot reclaimable again", site, username)
}

// recordingMarked accessors. The marker map is written by the assignment-sync
// cycle AND by the asynchronous dead-run release (a kill can land between sync
// cycles), so the old single-cycle-guard assumption no longer holds.

func (c *Coordinator) setRecordingMarker(key recMarkKey) {
	c.recordingMarkedMu.Lock()
	if c.recordingMarked == nil {
		c.recordingMarked = map[recMarkKey]bool{}
	}
	c.recordingMarked[key] = true
	c.recordingMarkedMu.Unlock()
}

func (c *Coordinator) clearRecordingMarker(key recMarkKey) {
	c.recordingMarkedMu.Lock()
	if c.recordingMarked != nil {
		delete(c.recordingMarked, key)
	}
	c.recordingMarkedMu.Unlock()
}

func (c *Coordinator) recordingMarkerIs(key recMarkKey) bool {
	c.recordingMarkedMu.Lock()
	defer c.recordingMarkedMu.Unlock()
	return c.recordingMarked != nil && c.recordingMarked[key]
}

// MarshalPool marshals a slice of ChannelConfig into JSON bytes.
func MarshalPool(pool []*entity.ChannelConfig) ([]byte, error) {
	if pool == nil {
		pool = []*entity.ChannelConfig{}
	}
	return json.MarshalIndent(pool, "", "  ")
}

// UnmarshalPool unmarshals JSON bytes into a slice of ChannelConfig.
func UnmarshalPool(data []byte) ([]*entity.ChannelConfig, error) {
	var pool []*entity.ChannelConfig
	if err := json.Unmarshal(data, &pool); err != nil {
		return nil, err
	}
	if pool == nil {
		pool = []*entity.ChannelConfig{}
	}
	return pool, nil
}
