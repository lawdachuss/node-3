package coordinator

import (
	"context"
	"log"
	"time"
)

// maxHeartbeatFailures is how many consecutive failed heartbeats (at 30s each)
// before we assume a network partition / DB outage and FENCE this node. At 4
// failures that's ~2 minutes — comfortably before the 180s reaper timeout, so
// we stop recording BEFORE another node could reclaim our channels and cause a
// duplicate capture.
const maxHeartbeatFailures = 4

// StartHeartbeatLoop periodically updates the node's last_heartbeat timestamp.
// Runs every 30 seconds until the context is cancelled or Stop() is called.
// If the heartbeat fails repeatedly it fences the node (stops local recording
// and releases channels) to prevent duplicate recording during a partition.
func (c *Coordinator) StartHeartbeatLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		failures := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				// Skip while gracefully shutting down: Stop() has already set
				// status=offline, and we must not let EnsureNodeOnline flip it
				// back to online. (This is the in-memory draining flag, NOT the
				// fence flag — a fenced node still heartbeats to detect recovery.)
				c.mu.Lock()
				draining := c.draining
				c.mu.Unlock()
				if draining {
					continue
				}

				load := c.currentLoad()
				if err := c.Client.HeartbeatNode(c.NodeID, load); err != nil {
					failures++
					log.Printf("[coordinator] heartbeat failed (%d/%d): %v", failures, maxHeartbeatFailures, err)
					if failures >= maxHeartbeatFailures && c.isActive() {
						c.fence()
					}
					continue
				}

				failures = 0
				if c.isFenced() {
					c.unfence()
				} else {
					// Recover from a "stuck offline" state (e.g. reaper marked
					// us offline during a restart gap). Only patches when status
					// is not already online/draining, so it never fights draining.
					if err := c.Client.EnsureNodeOnline(c.NodeID); err != nil {
						log.Printf("[coordinator] ensure-online error: %v", err)
					}
				}
			}
		}
	}()
}

// fence stops all local recording and releases this node's channels so other
// (healthy) nodes take them over. This prevents a partitioned node from
// continuing to record channels that the cluster now considers orphaned.
func (c *Coordinator) fence() {
	c.mu.Lock()
	c.fenced = true
	c.mu.Unlock()

	log.Printf("[coordinator] PARTITION FENCE: DB unreachable %d times — stopping local recording and releasing channels to prevent duplicate capture", maxHeartbeatFailures)

	if c.Client != nil {
		// Mark draining so the reaper won't try to reclaim and so no node
		// assigns us new channels while we're fenced.
		if err := c.Client.UpdateNodeStatus(c.NodeID, "draining"); err != nil {
			log.Printf("[coordinator] fence: update status error: %v", err)
		}
		// Release DB assignments FIRST so the reconcile watchdog (which runs
		// every 15s) sees them as unassigned and does NOT restart them locally
		// between this step and the local stop below.
		if c.Manager != nil {
			if err := c.Client.ReleaseNodeChannels(c.NodeID); err != nil {
				log.Printf("[coordinator] fence: release channels error: %v", err)
			}
		}
	}

	if c.Manager != nil {
		for _, username := range c.Manager.GetLocalChannels() {
			if err := c.Manager.RemoveChannelForReassignment(username); err != nil {
				log.Printf("[coordinator] fence: remove channel %s error: %v", username, err)
			}
		}
	}
}

// unfence resumes normal operation after a partition recovers: clear the fence
// flag and mark the node online so claim/migrate loops run again.
func (c *Coordinator) unfence() {
	c.mu.Lock()
	c.fenced = false
	c.mu.Unlock()

	log.Printf("[coordinator] PARTITION RECOVERED: resuming normal operation")

	if c.Client != nil {
		if err := c.Client.UpdateNodeStatus(c.NodeID, "online"); err != nil {
			log.Printf("[coordinator] unfence: update status error: %v", err)
		}
	}
}

// isFenced reports whether the node is currently fenced due to a partition.
func (c *Coordinator) isFenced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fenced
}
