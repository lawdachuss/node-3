package coordinator

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

// detectTailscaleIP attempts to get the Tailscale IPv4 address of this node.
func detectTailscaleIP() string {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("tailscale.exe", "ip", "-4")
	default:
		cmd = exec.Command("tailscale", "ip", "-4")
	}
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" || !strings.Contains(ip, ".") {
		return ""
	}
	return ip
}

// ChannelManager is the interface the coordinator uses to create/release channels.
// Implemented by manager.Manager in pooled mode.
type ChannelManager interface {
	CreateChannelFromAssignment(ca *database.ChannelAssignment) error
	RemoveChannelForReassignment(username string) error
	GetLocalChannels() []string
}

// LivenessChecker is the interface for checking if a channel is currently live.
// Implemented by main.go wiring using the site adapters.
type LivenessChecker interface {
	IsLive(ctx context.Context, siteName, username string) bool
}

// Coordinator manages the distributed node lifecycle: registration, heartbeat,
// channel claiming, liveness checking, and orphan reclamation.
type Coordinator struct {
	NodeID    string
	Mode      string
	Client    *database.Client
	Manager   ChannelManager
	LiveCheck LivenessChecker

	stopCh   chan struct{}
	wg       sync.WaitGroup
	started  bool
	draining bool // set during graceful shutdown; prevents heartbeat from clobbering status
	fenced   bool // set when DB is unreachable; stops local recording to avoid duplicate capture
	mu       sync.Mutex
}

// New creates a new Coordinator. If CHANNEL_POOL_MODE=pooled, Start() must
// be called to begin background goroutines.
func New(client *database.Client, mgr ChannelManager) *Coordinator {
	return &Coordinator{
		NodeID:  detectNodeID(),
		Mode:    channelPoolMode(),
		Client:  client,
		Manager: mgr,
		stopCh:  make(chan struct{}),
	}
}

func (c *Coordinator) IsPooled() bool { return c.Mode == entity.PoolModePooled }

// Start begins all background goroutines: heartbeat, claim, live check, reaper.
// Only starts them if mode is "pooled".
func (c *Coordinator) Start(ctx context.Context) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.mu.Unlock()

	if !c.IsPooled() {
		return
	}

	log.Printf("[coordinator] starting node %q in pooled mode", c.NodeID)
	c.Register()
	c.StartHeartbeatLoop(ctx)
	c.StartClaimLoop(ctx)
	c.StartLiveCheckLoop(ctx)
	c.StartReaperLoop(ctx)
	c.StartOfflineShuffleLoop(ctx)
	c.StartDeadlineMigrationLoop(ctx)
	c.StartReconcileLoop(ctx)
}

// isActive reports whether this node is currently able to own/claim channels.
// A draining or fenced (partitioned) node must not claim or migrate channels.
func (c *Coordinator) isActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.draining && !c.fenced
}

// Stop gracefully shuts down all coordinator loops and deregisters the node.
// Safe to call multiple times — the second call is a no-op.
func (c *Coordinator) Stop() {
	if !c.IsPooled() {
		return
	}

	// Guard against double-close panic.
	c.mu.Lock()
	select {
	case <-c.stopCh:
		c.mu.Unlock()
		return // already closed
	default:
		close(c.stopCh)
	}
	c.mu.Unlock()

	log.Printf("[coordinator] stopping node %q", c.NodeID)

	// Wait for goroutines to finish
	c.wg.Wait()

	if c.Client == nil {
		return
	}

	// Release all channel assignments
	if err := c.Client.ReleaseNodeChannels(c.NodeID); err != nil {
		log.Printf("[coordinator] error releasing channels: %v", err)
	}

	// Mark node as offline
	if err := c.Client.UpdateNodeStatus(c.NodeID, "offline"); err != nil {
		log.Printf("[coordinator] error deregistering node: %v", err)
	}

	log.Printf("[coordinator] node %q stopped cleanly", c.NodeID)
}

// Register upserts this node in the nodes table.
func (c *Coordinator) Register() {
	if c.Client == nil {
		log.Printf("[coordinator] WARNING: no database client — skipping node registration")
		return
	}
	host, _ := os.Hostname()
	version := os.Getenv("SOFTWARE_VERSION")
	if version == "" {
		version = "dev"
	}

	webURL := os.Getenv("NODE_WEB_URL")
	if webURL == "" {
		if tsIP := detectTailscaleIP(); tsIP != "" {
			webURL = fmt.Sprintf("http://%s:8080", tsIP)
			log.Printf("[coordinator] auto-detected Tailscale IP: %s", tsIP)
		}
	}

	node := &database.Node{
		NodeID:          c.NodeID,
		Hostname:        host,
		InstanceLabel:   os.Getenv("INSTANCE_LABEL"),
		SoftwareVersion: version,
		Status:          "online",
		CurrentLoad:     0,
		WebURL:          webURL,
		SessionDeadline: computeSessionDeadline(),
	}

	if err := c.Client.UpsertNode(node); err != nil {
		log.Printf("[coordinator] WARNING: failed to register node: %v", err)
	} else {
		log.Printf("[coordinator] registered as node %q on %s", c.NodeID, host)
	}
}

// StartDraining sets the node status to "draining" so other nodes know not to
// assign new channels to this node. Call during graceful shutdown BEFORE stopping
// channels, so new claims go elsewhere.
func (c *Coordinator) StartDraining() {
	if !c.IsPooled() || c.Client == nil {
		return
	}
	c.mu.Lock()
	c.draining = true
	c.mu.Unlock()
	if err := c.Client.UpdateNodeStatus(c.NodeID, "draining"); err != nil {
		log.Printf("[coordinator] error setting draining: %v", err)
	}
}

// currentLoad returns the count of channels this node owns.
func (c *Coordinator) currentLoad() int {
	if c.Client == nil {
		return 0
	}
	count, err := c.Client.CountMyAssignments(c.NodeID)
	if err != nil {
		return 0
	}
	return count
}

// detectNodeID auto-detects the node identity using a priority chain:
// 1. NODE_ID env var (explicit)
// 2. GITHUB_REPOSITORY env var — splits by "-" and takes the last segment
//    so "owner/MiniDelectableService-node-a" yields "a"
// 3. os.Hostname() (VPS / local)
// 4. Random fallback (defensive)
//
// IMPORTANT: this must stay in sync with server/db.go:detectNodeID().
func detectNodeID() string {
	if id := os.Getenv("NODE_ID"); id != "" {
		return id
	}
	if repo := os.Getenv("GITHUB_REPOSITORY"); repo != "" {
		parts := strings.Split(repo, "-")
		if len(parts) > 1 {
			return parts[len(parts)-1]
		}
		return strings.ReplaceAll(repo, "/", "-")
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(1<<48))
	return fmt.Sprintf("node-%x", n)
}

// channelPoolMode returns the pool mode from env var.
func channelPoolMode() string {
	mode := os.Getenv("CHANNEL_POOL_MODE")
	if mode == "" {
		return entity.PoolModeIsolated
	}
	return mode
}

// computeSessionDeadline determines when this node will be forcibly killed so
// the coordinator can migrate its channels away beforehand. Priority:
//  1. SESSION_DURATION env (Go duration string, e.g. "5h20m") — explicit.
//  2. GITHUB_RUN_ID present (CI runner, hard 6h cap) — use a buffer BEFORE the
//     workflow's 348-minute self-cancel so migration fires while we're still up.
//  3. Neither — nil (permanent node, no deadline).
func computeSessionDeadline() *time.Time {
	if d := os.Getenv("SESSION_DURATION"); d != "" {
		if dur, err := time.ParseDuration(d); err == nil && dur > 0 {
			t := time.Now().Add(dur)
			return &t
		}
	}
	if os.Getenv("GITHUB_RUN_ID") != "" {
		// 335m leaves a ~13m buffer before the 348m self-cancel / 360m hard kill.
		t := time.Now().Add(335 * time.Minute)
		return &t
	}
	return nil
}
