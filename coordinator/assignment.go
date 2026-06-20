package coordinator

import (
	"context"
	"encoding/json"
	"log"
	"math"
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

		// Small initial delay to let the node register first
		time.Sleep(5 * time.Second)

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
func (c *Coordinator) runClaimCycle() {
	// 1. Get current assignment stats
	stats, err := c.Client.GetAssignmentStats()
	if err != nil {
		log.Printf("[coordinator] claim cycle: get stats error: %v", err)
		return
	}

	// 2. Calculate fair share based on total unassigned channels
	poolSize := stats.TotalUnassigned + stats.TotalAssignedNodes // rough total pool size
	totalNodes := stats.TotalAliveNodes
	if totalNodes == 0 {
		totalNodes = 1
	}
	fairShare := int(math.Ceil(float64(poolSize) / float64(totalNodes)))

	// 3. Get our current load
	myLoad, err := c.Client.CountMyAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] claim cycle: count error: %v", err)
		return
	}

	// 4. If we're at or above fair share, don't claim
	if myLoad >= fairShare {
		return
	}

	// 5. Claim up to (fairShare - myLoad) channels
	budget := fairShare - myLoad
	claimed, err := c.Client.ClaimChannels(c.NodeID, budget)
	if err != nil {
		log.Printf("[coordinator] claim cycle: claim error: %v", err)
		return
	}

	if len(claimed) == 0 {
		return
	}

	log.Printf("[coordinator] claimed %d new channel(s) (load: %d -> %d, fairShare: %d, poolSize: %d)",
		len(claimed), myLoad, myLoad+len(claimed), fairShare, poolSize)

	// 6. Convert assignments to channels via the manager
	for _, ca := range claimed {
		if c.Manager != nil {
			if err := c.Manager.CreateChannelFromAssignment(&ca); err != nil {
				log.Printf("[coordinator] error creating channel from assignment %s: %v", ca.Username, err)
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
		Username:                 conf.Username,
		Site:                     conf.Site,
		Status:                   "unassigned",
		IsLive:                   false,
		Framerate:                conf.Framerate,
		Resolution:               conf.Resolution,
		Pattern:                  conf.Pattern,
		MaxDuration:              conf.MaxDuration,
		MaxFilesize:              conf.MaxFilesize,
		Compress:                 conf.Compress,
		MinDurationBeforeUpload:  conf.MinDurationBeforeUpload,
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
