package coordinator

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"log"
	"math"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// StartClaimLoop periodically claims channels for this node based on fair-share.
// Runs every 60 seconds until the context is cancelled or Stop() is called.
func (c *Coordinator) StartClaimLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		// Stagger initial delay by node-ID hash so nodes don't all
		// claim on the same cycle and race for the same channels.
		// Base delay 5s + up to 10s spread.
		h := fnv.New32a()
		h.Write([]byte(c.NodeID))
		stagger := 5*time.Second + time.Duration(h.Sum32()%10)*time.Second
		time.Sleep(stagger)

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.runClaimCycle()
			}
		}
	}()
}

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

// runClaimCycle executes one iteration of the fair-share claiming algorithm.
// Claims channels if this node has less than its fair share, releases channels
// if it has more than its fair share (only when multiple nodes are alive).
// Skips entirely when draining (graceful shutdown in progress).
func (c *Coordinator) runClaimCycle() {
	// Don't claim new channels during draining — the node is shutting down
	// and new channels would just need to be released immediately.
	c.mu.Lock()
	draining := c.draining
	c.mu.Unlock()
	if draining {
		return
	}
	// Don't claim while fenced (DB unreachable / partitioned) — claiming would
	// fight the healthy nodes that took over our released channels.
	if c.isFenced() {
		return
	}
	// Self-heal: repair rows stuck with assigned_node set but status=unassigned.
	// These rows are invisible to both claim and release, causing a deadlock.
	if repaired, err := c.Client.RepairOrphanedAssignments(); err != nil {
		log.Printf("[coordinator] claim cycle: repair orphaned error: %v", err)
	} else if repaired > 0 {
		log.Printf("[coordinator] repaired %d orphaned assignment(s) (assigned_node set but status=unassigned)", repaired)
	}

	// Reconcile database assignments with local manager channels.
	// This ensures we stop any channel that got reassigned away (e.g. by reaper)
	// and start any channel assigned to us that we missed or failed to start.
	dbAssignments, err := c.Client.GetNodeAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] claim cycle: get node assignments error: %v", err)
		return
	}

	localChannels := c.Manager.GetLocalChannels()

	// 1. Remove local channels that are no longer assigned to this node in DB
	dbMap := make(map[string]database.ChannelAssignment)
	for _, a := range dbAssignments {
		dbMap[a.Username] = a
	}

	for _, lc := range localChannels {
		if _, ok := dbMap[lc]; !ok {
			log.Printf("[coordinator] reconciliation: channel %s is running locally but not assigned to this node in DB. Removing.", lc)
			if err := c.Manager.RemoveChannelForReassignment(lc); err != nil {
				log.Printf("[coordinator] reconciliation error removing channel %s: %v", lc, err)
			}
		}
	}

	// 2. Start channels that are assigned to this node in DB but not running locally
	for _, a := range dbAssignments {
		found := false
		for _, lc := range localChannels {
			if lc == a.Username {
				found = true
				break
			}
		}
		if !found {
			log.Printf("[coordinator] reconciliation: channel %s is assigned in DB but not running locally. Starting.", a.Username)
			if err := c.Manager.CreateChannelFromAssignment(&a); err != nil {
				log.Printf("[coordinator] reconciliation error starting channel %s: %v", a.Username, err)
			}
		}
	}

	stats, err := c.Client.GetAssignmentStats()
	if err != nil {
		log.Printf("[coordinator] claim cycle: get stats error: %v", err)
		return
	}

	totalPool := stats.TotalPoolChannels
	totalNodes := stats.TotalAliveNodes
	if totalNodes == 0 {
		totalNodes = 1
	}
	// In pooled mode, assume at least 2 nodes to prevent one node from claiming
	// everything before other nodes register their heartbeats.
	if strings.HasPrefix(c.NodeID, "node-") && totalNodes < 2 {
		totalNodes = 2
	}
	// Fair-share is based on the total pool, not live-channel count.  A DVR is
	// expected to claim channels that are offline at claim time and record them
	// when they go live, so all pool channels count toward distribution.  Using
	// live-channel count would zero out fair-share whenever the liveness loop
	// (every ~120s) hasn't run, starving all claims.
	fairShare := int(math.Ceil(float64(totalPool) / float64(totalNodes)))

	myLoad, err := c.Client.CountMyAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] claim cycle: count error: %v", err)
		return
	}

	// Release excess channels if we have more than our fair share.
	// Only OFFLINE channels are released, so a live+recording channel is never
	// interrupted just because its node is over fair-share.
	if myLoad > fairShare && totalNodes > 1 {
		excess := myLoad - fairShare
		released, err := c.Client.ReleaseExcessOfflineChannels(c.NodeID, excess)
		if err != nil {
			log.Printf("[coordinator] claim cycle: release excess error: %v", err)
			return
		}
		if len(released) > 0 {
			log.Printf("[coordinator] released %d excess channel(s) (load: %d -> %d, fairShare: %d, totalPool: %d)",
				len(released), myLoad, myLoad-len(released), fairShare, totalPool)
			for _, ca := range released {
				if c.Manager != nil {
					c.Manager.RemoveChannelForReassignment(ca.Username)
				}
			}
		}
		return // let next cycle do the claiming to avoid races
	}

	// Claim channels if we have fewer than our fair share
	if myLoad < fairShare {
		budget := fairShare - myLoad
		claimed, err := c.Client.ClaimChannels(c.NodeID, budget)
		if err != nil {
			log.Printf("[coordinator] claim cycle: claim error: %v", err)
			return
		}
		if len(claimed) > 0 {
			log.Printf("[coordinator] claimed %d new channel(s) (load: %d -> %d, fairShare: %d, totalPool: %d)",
				len(claimed), myLoad, myLoad+len(claimed), fairShare, totalPool)
			for _, ca := range claimed {
				if c.Manager != nil {
					if err := c.Manager.CreateChannelFromAssignment(&ca); err != nil {
						log.Printf("[coordinator] error creating channel from assignment %s: %v", ca.Username, err)
					}
				}
			}
		}
	}
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

	if err := c.Client.BulkInsertAssignments([]database.ChannelAssignment{ca}); err != nil {
		return err
	}

	// Try to claim it for ourselves right away
	claimed, err := c.Client.ClaimSpecificChannel(conf.Username, conf.Site, c.NodeID)
	if err != nil {
		return err
	}

	if claimed {
		log.Printf("[coordinator] claimed new channel %s for this node", conf.Username)
	} else {
		log.Printf("[coordinator] channel %s claimed by another node", conf.Username)
	}

	return nil
}

// DeleteChannelAssignment removes the channel_assignments row for a channel.
func (c *Coordinator) DeleteChannelAssignment(username, site string) error {
	if !c.IsPooled() || c.Client == nil {
		return nil
	}

	return c.Client.ReleaseChannel(username, site)
}

// ConfigFromAssignment converts a ChannelAssignment back to a ChannelConfig.
func ConfigFromAssignment(ca *database.ChannelAssignment) *entity.ChannelConfig {
	return &entity.ChannelConfig{
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
