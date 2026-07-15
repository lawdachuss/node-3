package coordinator

import (
	"context"
	"hash/fnv"
	"log"
	"math"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
)

// shuffleInterval is how often offline channels are re-evaluated and
// redistributed across healthy nodes ("keep shuffling until online").
const shuffleInterval = 5 * time.Minute

// deadlineMigrationInterval is how often we check for nodes whose session
// deadline is imminent and migrate their channels (incl. live+recording) to
// healthy nodes before the node is killed.
const deadlineMigrationInterval = 60 * time.Second

// deadlineMigrationWindow is how far ahead of a node's session_deadline we start
// migrating its channels away.
const deadlineMigrationWindow = 15 * time.Minute

// reconcileInterval is the fast watchdog that stops channels that are no longer
// assigned to this node (e.g. after a deadline migration or reaper reclaim),
// bounding any brief overlap to this interval.
const reconcileInterval = 15 * time.Second

// StartOfflineShuffleLoop periodically rebalances OFFLINE channels across nodes.
// Runs every shuffleInterval (5 min). Offline channels keep migrating node to
// node; the moment a channel goes live it is protected and stays put. Live and
// recording channels are never released here (the fair-share claim loop already
// avoids them via ReleaseExcessOfflineChannels).
func (c *Coordinator) StartOfflineShuffleLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		h := fnv.New32a()
		h.Write([]byte(c.NodeID))
		stagger := 5*time.Second + time.Duration(h.Sum32()%10)*time.Second
		time.Sleep(stagger)

		ticker := time.NewTicker(shuffleInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.runOfflineShuffleCycle()
			}
		}
	}()
}

// runOfflineShuffleCycle rebalances this node's OFFLINE channels to OTHER alive
// nodes. It reassigns (not just "releases to unassigned") so the channels
// actually migrate to a different node instead of being immediately re-claimed
// by the same node (which, after releasing, is under fair-share and would just
// absorb them again — the old behaviour that pinned every channel to its first
// claimer). Offline channels keep migrating node-to-node; the moment one goes
// live the liveness loop marks it 'recording' and ReleaseExcessOfflineChannels
// (now only used as a safety net) won't touch it.
func (c *Coordinator) runOfflineShuffleCycle() {
	if !c.isActive() {
		return
	}

	if repaired, err := c.Client.RepairOrphanedAssignments(); err != nil {
		log.Printf("[coordinator] offline shuffle: repair orphaned error: %v", err)
	} else if repaired > 0 {
		log.Printf("[coordinator] offline shuffle: repaired %d orphaned assignment(s)", repaired)
	}

	stats, err := c.Client.GetAssignmentStats()
	if err != nil {
		log.Printf("[coordinator] offline shuffle: stats error: %v", err)
		return
	}

	aliveNodes, err := c.Client.GetAliveNodes()
	if err != nil {
		log.Printf("[coordinator] offline shuffle: alive nodes error: %v", err)
		return
	}
	totalNodes := len(aliveNodes)
	if totalNodes == 0 {
		totalNodes = 1
	}
	if strings.HasPrefix(c.NodeID, "node-") && totalNodes < 2 {
		totalNodes = 2
	}

	// Candidate targets: alive nodes that are NOT this node.
	var candidates []database.Node
	for _, n := range aliveNodes {
		if n.NodeID == c.NodeID {
			continue
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		return
	}

	// Fair share is based on the total pool so offline channels (which we record
	// when they go live) are distributed evenly.
	fairShare := int(math.Ceil(float64(stats.TotalPoolChannels) / float64(totalNodes)))

	myLoad, err := c.Client.CountMyAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] offline shuffle: count error: %v", err)
		return
	}

	moveCount := myLoad - fairShare
	if moveCount < 0 {
		moveCount = 0
	}

	// Gentle churn: even when balanced, move one offline channel to another node
	// so the pool keeps shuffling until channels come online.
	if moveCount == 0 && len(candidates) > 0 {
		moveCount = 1
	}

	if moveCount <= 0 {
		return
	}

	// Pick our offline (not recording, not live) channels to move.
	myChannels, err := c.Client.GetNodeAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] offline shuffle: get assignments error: %v", err)
		return
	}
	var offline []database.ChannelAssignment
	for _, ca := range myChannels {
		if ca.IsLive {
			continue
		}
		if ca.Status == "recording" {
			continue
		}
		offline = append(offline, ca)
	}
	if len(offline) == 0 {
		return
	}
	if moveCount > len(offline) {
		moveCount = len(offline)
	}

	// Local view of each candidate's load so we spread the moves evenly across
	// them rather than piling onto one node.
	load := make(map[string]int, len(candidates))
	for _, n := range candidates {
		load[n.NodeID] = n.CurrentLoad
	}

	moved := 0
	for i := 0; i < moveCount; i++ {
		ca := offline[i]
		target := leastLoadedCandidate(candidates, load)
		if target.NodeID == c.NodeID {
			continue
		}
		if err := c.Client.ReassignChannel(ca.Username, ca.Site, c.NodeID, target.NodeID); err != nil {
			log.Printf("[coordinator] offline shuffle: reassign %s -> %s error: %v", ca.Username, target.NodeID, err)
			continue
		}
		load[target.NodeID]++
		moved++
		log.Printf("[coordinator] offline shuffle: moved %s/%s from %s -> %s", ca.Site, ca.Username, c.NodeID, target.NodeID)
		if c.Manager != nil {
			if err := c.Manager.RemoveChannelForReassignment(ca.Username); err != nil {
				log.Printf("[coordinator] offline shuffle: remove %s error: %v", ca.Username, err)
			}
		}
	}

	if moved > 0 {
		log.Printf("[coordinator] offline shuffle: moved %d offline channel(s) to other node(s) (load: %d -> %d, fairShare: %d, totalPool: %d)",
			moved, myLoad, myLoad-moved, fairShare, stats.TotalPoolChannels)
	}
}

// leastLoadedCandidate returns the candidate node with the smallest current load
// (using the supplied local load map), ignoring the calling node.
func leastLoadedCandidate(candidates []database.Node, load map[string]int) database.Node {
	best := candidates[0]
	bestLoad := load[best.NodeID]
	for _, n := range candidates[1:] {
		if l := load[n.NodeID]; l < bestLoad {
			best = n
			bestLoad = l
		}
	}
	return best
}

// StartDeadlineMigrationLoop migrates channels off nodes whose session_deadline
// is imminent (the GitHub 6-hour runner limit) to healthy nodes. Runs every
// deadlineMigrationInterval. Reassignment is atomic (SKIP LOCKED) so even if
// several nodes race to migrate the same channel, only one wins.
func (c *Coordinator) StartDeadlineMigrationLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		h := fnv.New32a()
		h.Write([]byte(c.NodeID))
		stagger := time.Duration(h.Sum32()%10) * time.Second
		time.Sleep(stagger)

		ticker := time.NewTicker(deadlineMigrationInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.runDeadlineMigrationCycle()
			}
		}
	}()
}

// runDeadlineMigrationCycle finds nodes about to hit their session deadline and
// reassigns all of their channels (including live+recording) to the least-loaded
// healthy node. A node migrating its OWN channels away is expected — that's the
// whole point of pre-deadline migration.
func (c *Coordinator) runDeadlineMigrationCycle() {
	if !c.isActive() {
		return
	}

	if _, err := c.Client.RepairOrphanedAssignments(); err != nil {
		log.Printf("[coordinator] deadline migration: repair orphaned error: %v", err)
	}

	imminent, err := c.Client.GetNodesWithImminentDeadline(deadlineMigrationWindow)
	if err != nil {
		log.Printf("[coordinator] deadline migration: imminent nodes error: %v", err)
		return
	}
	if len(imminent) == 0 {
		return
	}

	alive, err := c.Client.GetAliveNodes()
	if err != nil {
		log.Printf("[coordinator] deadline migration: alive nodes error: %v", err)
		return
	}

	imminentSet := make(map[string]bool, len(imminent))
	for _, n := range imminent {
		imminentSet[n.NodeID] = true
	}

	// Candidate targets: alive, not draining, not themselves imminent.
	var candidates []database.Node
	for _, n := range alive {
		if imminentSet[n.NodeID] {
			continue
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		log.Printf("[coordinator] deadline migration: no healthy candidates to migrate %d imminent node(s) to", len(imminent))
		return
	}

	for _, imm := range imminent {
		if imm.NodeID == c.NodeID {
			log.Printf("[coordinator] deadline migration: this node's deadline is imminent — migrating channels away")
		}
		assignments, err := c.Client.GetNodeAssignments(imm.NodeID)
		if err != nil {
			log.Printf("[coordinator] deadline migration: get assignments for %s error: %v", imm.NodeID, err)
			continue
		}
		for _, ca := range assignments {
			target := leastLoaded(candidates)
			if target.NodeID == imm.NodeID {
				continue
			}
			if err := c.Client.ReassignChannel(ca.Username, ca.Site, imm.NodeID, target.NodeID); err != nil {
				log.Printf("[coordinator] deadline migration: reassign %s from %s error: %v", ca.Username, imm.NodeID, err)
				continue
			}
			// Bump the local view of the target's load so we spread channels
			// across candidates instead of piling them on one node.
			target.CurrentLoad++
			log.Printf("[coordinator] deadline migration: moved %s/%s from %s -> %s", ca.Site, ca.Username, imm.NodeID, target.NodeID)
		}
	}
}

// leastLoaded returns the candidate node with the smallest current load.
func leastLoaded(candidates []database.Node) database.Node {
	best := candidates[0]
	for _, n := range candidates[1:] {
		if n.CurrentLoad < best.CurrentLoad {
			best = n
		}
	}
	return best
}

// StartReconcileLoop is a fast watchdog that stops any local channel no longer
// assigned to this node in the DB. This bounds the brief recording overlap that
// can occur after a deadline migration or reaper reclaim to reconcileInterval.
func (c *Coordinator) StartReconcileLoop(ctx context.Context) {
	if !c.IsPooled() || c.Client == nil {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(reconcileInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.runReconcileCycle()
			}
		}
	}()
}

// runReconcileCycle stops local channels that are no longer assigned to this node.
// On any DB error it returns immediately and removes NOTHING — a transient DB
// hiccup must never cause us to drop live recordings.
func (c *Coordinator) runReconcileCycle() {
	if !c.isActive() {
		return
	}

	dbAssignments, err := c.Client.GetNodeAssignments(c.NodeID)
	if err != nil {
		log.Printf("[coordinator] reconcile: get node assignments error: %v", err)
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

	// Stop channels no longer assigned to us (e.g. migrated away / reaped).
	for _, lc := range local {
		if !dbMap[lc] {
			log.Printf("[coordinator] reconcile: channel %s no longer assigned to this node — stopping", lc)
			if err := c.Manager.RemoveChannelForReassignment(lc); err != nil {
				log.Printf("[coordinator] reconcile: remove %s error: %v", lc, err)
			}
		}
	}

	// Start channels assigned to us that aren't running locally yet (e.g. a
	// channel migrated here by the deadline loop). CreateChannelFromAssignment
	// is idempotent, so this is safe to run every cycle.
	for _, a := range dbAssignments {
		if !localSet[a.Username] {
			if err := c.Manager.CreateChannelFromAssignment(&a); err != nil {
				log.Printf("[coordinator] reconcile: start %s error: %v", a.Username, err)
			}
		}
	}
}
