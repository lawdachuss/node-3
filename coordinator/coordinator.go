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
	"github.com/teacat/chaturbate-dvr/server"
)

// cycleGuard prevents overlapping coordinator cycles. When the DB is slow,
// a cycle (e.g. reaper running every 120s) could still be in progress when
// the next tick fires, launching a second concurrent cycle that increases
// DB load and makes things worse. cycleGuard skips a cycle tick if the
// previous one hasn't finished, acting as a circuit breaker.
//
// IMPORTANT: cycleGuard only prevents concurrent EXECUTION of the same
// cycle. It does NOT bound the duration of a single cycle — the database
// client's 60s HTTP timeout handles that at a finer granularity, and the
// health check warns if a cycle stops running entirely.
type cycleGuard struct {
	mu        sync.Mutex
	running   bool
	lastRun   time.Time
	lastRunMu sync.Mutex
}

// tryRun attempts to execute fn under the cycle guard, preventing overlapping
// runs of the same cycle. Returns true if the function was executed, false if
// a previous run is still in progress (the cycle tick is silently skipped).
//
// Panic recovery: any panic from fn is caught, logged, and the guard is
// released so the next tick can proceed. Without this recovery, a single bad
// cycle would crash the entire node, dropping every recording and stopping
// all coordinator loops.
func (g *cycleGuard) tryRun(name string, fn func()) bool {
	g.mu.Lock()
	if g.running {
		g.mu.Unlock()
		return false // skip silently — the ticker will retry
	}
	g.running = true
	g.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[coordinator] %s cycle panicked (recovered): %v", name, r)
		}
		g.mu.Lock()
		g.running = false
		g.mu.Unlock()
		g.lastRunMu.Lock()
		g.lastRun = time.Now()
		g.lastRunMu.Unlock()
	}()

	fn()
	return true
}

// lastRunTime returns the last execution time of this guard.
func (g *cycleGuard) lastRunTime() time.Time {
	g.lastRunMu.Lock()
	defer g.lastRunMu.Unlock()
	return g.lastRun
}

// runLoopWithRestart runs a background loop inside a goroutine with automatic
// restart if the loop exits unexpectedly. This is the core watchdog mechanism:
// if a coordinator loop goroutine crashes (unrecovered panic, unexpected return
// from the select, etc.), it is automatically restarted after a 5-second delay
// instead of silently dying and leaving the node without coordinator cycles.
//
// The loopFn receives the ticker channel and should block on it, returning only
// when the context is done or stop is received. If loopFn returns for any other
// reason, it is restarted.
//
// The loopFn sub-goroutine is deliberately NOT tracked by c.wg: the outer
// wrapper waits on loopDone before returning, so Stop() still blocks until an
// in-flight cycle finishes (bounded by the database client's 60s HTTP timeout).
func (c *Coordinator) runLoopWithRestart(ctx context.Context, name string, interval time.Duration, loopFn func(stopCh <-chan struct{}, tickerC <-chan time.Time)) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			ticker := time.NewTicker(interval)
			loopDone := make(chan struct{}, 1)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[coordinator] %s loop panicked (will restart): %v", name, r)
					}
					close(loopDone)
				}()
				loopFn(c.stopCh, ticker.C)
			}()

			select {
			case <-ctx.Done():
				ticker.Stop()
				<-loopDone // wait for the in-flight cycle to wind down before exiting
				return
			case <-c.stopCh:
				ticker.Stop()
				<-loopDone // graceful: Stop() blocks until the in-flight cycle finishes
				return
			case <-loopDone:
				// Loop exited unexpectedly — restart after delay, unless we should
				// actually stop first.
				select {
				case <-ctx.Done():
					return
				case <-c.stopCh:
					return
				default:
				}
				log.Printf("[coordinator] %s loop exited unexpectedly — restarting in 5s", name)
				ticker.Stop()
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				case <-c.stopCh:
					return
				}
				continue // restart the loop
			}
		}
	}()
}

// maxCycleStall is how long a coordinator cycle may go without running before
// the health check warns.  All cycles run on multi-minute tickers, so a stall
// of 5 minutes means the loop is stuck or dead.
const maxCycleStall = 5 * time.Minute

// startHealthCheckLoop runs periodically and logs warnings if any coordinator
// cycle has not run recently (indicating a stuck or dead cycle). This provides
// observability into the health of all background cycles.
func (c *Coordinator) startHealthCheckLoop(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.checkCycleHealth("reconcile", &c.cycleGuardReconcile)
				c.checkCycleHealth("controller", &c.cycleGuardController)
				c.checkCycleHealth("stuck-pause", &c.cycleGuardStuckPause)
			}
		}
	}()
}

// checkCycleHealth logs a warning if the given cycle has not run within
// maxCycleStall.  Guards that have never run (zero timestamp) are skipped so
// startup produces no noise.
func (c *Coordinator) checkCycleHealth(name string, guard *cycleGuard) {
	last := guard.lastRunTime()
	if !last.IsZero() && time.Since(last) > maxCycleStall {
		log.Printf("[coordinator] HEALTH WARNING: %s cycle last ran %v ago (> %v)", name, time.Since(last).Round(time.Second), maxCycleStall)
	}
}

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

// ChannelPause identifies one locally paused channel.
type ChannelPause struct {
	Username string
	Site     string
}

// ChannelManager is the interface the coordinator uses to create/release channels.
// Implemented by manager.Manager in pooled mode.
type ChannelManager interface {
	CreateChannelFromAssignment(ca *database.ChannelAssignment) error
	RemoveChannelForReassignment(username string) error
	GetLocalChannels() []string
	LocalChannelSite(username string) (string, bool)
	HasPendingSegments(username string) bool
	// IsRecording reports whether the channel is actively writing a live
	// recording file on THIS node right now.  The shuffle/reconcile/deadline
	// cycles use it to never reassign, pause, or remove a channel mid-recording
	// (which would strand or fragment an in-progress recording).
	IsRecording(username string) bool

	// ManualPausedChannels returns the channels the USER explicitly paused
	// (pause reason = manual). Automatic paths must never fight these: they
	// are kept assigned to this node across session boundaries and never
	// auto-resumed, released, or flagged as stuck.
	ManualPausedChannels() []ChannelPause

	// CFBlockedCount returns how many channels are currently in a
	// Cloudflare-blocked state. The claim cycle uses it to detect a
	// Cloudflare-starved node (IP flagged) and shed its claims to the pool
	// so healthy nodes can record the channels instead of the starved node
	// hoarding them while retrying forever.
	CFBlockedCount() int

	// RequestCookieRefresh triggers a rate-limited cookie re-mint (scripts +
	// Supabase reload) so a CF-starved node can recover without a restart.
	RequestCookieRefresh()
}

// LivenessResult is the outcome of a liveness probe.
type LivenessResult int

const (
	// LivenessLive means the model is confirmed broadcasting.
	LivenessLive LivenessResult = iota
	// LivenessOffline means the model was confirmed NOT broadcasting.
	LivenessOffline
	// LivenessUnknown means the probe could not be answered (transient error,
	// rate limit, ambiguous room status). Unknown is never a reason to stop
	// recording a channel — only a definitive offline result may be.
	LivenessUnknown
)

// LivenessChecker is the interface for checking if a channel is currently live.
// Implemented by main.go wiring using the site adapters.
type LivenessChecker interface {
	CheckLive(ctx context.Context, siteName, username string) LivenessResult
}

// Coordinator manages the distributed node lifecycle: registration, heartbeat,
// channel claiming, liveness checking, and orphan reclamation.
type Coordinator struct {
	NodeID    string
	Mode      string
	Client    *database.Client
	Manager   ChannelManager
	LiveCheck LivenessChecker

	// ownDeadline is this node's session_deadline, captured at Register() (the
	// same value persisted to the nodes table). The claim cycle reads it to
	// self-drain during the pre-deadline migration window so it doesn't
	// re-claim channels the deadline-migration cycle is moving away. Zero means
	// the node has no deadline (permanent node) and never self-drains.
	ownDeadline time.Time

	// lastHeartbeatOK is the wall-clock time of the last successful heartbeat,
	// written by the heartbeat tick and read by StartHeartbeatWatchdog to
	// detect a wedged/frozen heartbeat path (a hung DVR that keep-alive's
	// $dvr.HasExited check can never see).
	lastHeartbeatOK time.Time
	lastHeartbeatMu sync.Mutex

	stopCh   chan struct{}
	wg       sync.WaitGroup
	started  bool
	draining bool // set during graceful shutdown; prevents heartbeat from clobbering status
	fenced   bool // set when DB is unreachable; stops local recording to avoid duplicate capture
	mu       sync.Mutex

	// cycle guards prevent overlapping background operations when the DB
	// is slow — a cycle that hasn't finished when the next tick fires will
	// skip its tick instead of launching a concurrent cycle that piles on
	// more DB load.  lastRun timestamps feed the health-check watchdog.
	cycleGuardClaim      cycleGuard
	cycleGuardLiveCheck  cycleGuard
	cycleGuardReaper     cycleGuard
	cycleGuardShuffle    cycleGuard
	cycleGuardDeadline   cycleGuard
	cycleGuardReconcile  cycleGuard
	cycleGuardStuckPause cycleGuard
	cycleGuardHoard      cycleGuard
	cycleGuardController cycleGuard

	// cbLiveCache caches per-channel chaturbate liveness probe results so the
	// fallback liveness check (used when the bulk affiliate API is unset or
	// under-reports non-affiliate models) doesn't re-hit Chaturbate for every
	// channel every cycle. Guarded by cbLiveMu.
	cbLiveCache map[string]cbLiveEntry
	cbLiveMu    sync.Mutex

	// assignerCfg is the cached assignment tunables (loaded once from
	// app_settings.key='assigner_config'). Guarded by assignerCfgMu.
	assignerCfg   *assignerConfig
	assignerCfgMu sync.Mutex
	// assignerColdStart marks when a cold-start hold began; while set and within
	// the cold-start window the controller defers assignment so a slow-booting
	// fleet doesn't let its fast nodes grab every channel. Guarded by
	// assignerColdStartMu.
	assignerColdStart   *time.Time
	assignerColdStartMu sync.Mutex

	// assignerAssigned is set once the one-time equal startup assignment has
	// been performed (after every node was live). After that, channels are NOT
	// continuously reshuffled; reassignment happens only when the fleet
	// membership changes (a node joins/leaves). Guarded by assignerAssignMu.
	assignerAssigned bool
	assignerFleetSig string
	assignerAssignMu sync.Mutex

	// stuckPauseSeen tracks consecutive observations of a paused-but-still-
	// assigned channel (key nodeID/site/username → count). A channel must be
	// observed across stuckPauseConfirmCycles consecutive checks before it is
	// considered genuinely stuck and a notification fires, so transient
	// session-boundary pauses never alert.
	stuckPauseSeen map[string]int
	stuckPauseMu   sync.Mutex

	// liveCheckMiss tracks consecutive DEFINITIVE offline live-check results
	// per channel (key site/username → count). A channel must be observed
	// offline across liveCheckReleaseStreak cycles before this node releases
	// it back to the pool, so a single flaky probe (affiliate API blip,
	// rate-limited room status, transient network error) can never pause a
	// live channel. Live and Unknown results reset the streak.
	liveCheckMiss   map[string]int
	liveCheckMissMu sync.Mutex

	// recordingMarked tracks, for THIS process, which channels this node has
	// marked status='recording' in the DB (assignment-sync mark phase). Because
	// the controller never resets a recording marker whose owner node is alive,
	// the owner itself must clear its finished recordings: the sync loop resets
	// a previously-marked channel to 'claimed' exactly once, the first cycle
	// after it stops recording. Keyed by site+username because the same handle
	// can exist on chaturbate AND stripchat as distinct channel_assignments
	// rows, each with its own marker to clear.
	recordingMarked map[recMarkKey]bool

	// recordingMarkedMu guards recordingMarked. The marker map previously ran
	// only inside the single-flight assignment-sync cycle, but the asynchronous
	// dead-run release (Task B2) also clears and drops markers between
	// cycles, so it is guarded explicitly now.
	recordingMarkedMu sync.Mutex
}

// recMarkKey identifies one channel_assignments row (site+username) that this
// node has marked status='recording'. The same username can exist on both
// sites, so a bare username would make one site's cleanup clear (or miss) the
// other site's marker.
type recMarkKey struct {
	site     string
	username string
}

// New creates a new Coordinator. If CHANNEL_POOL_MODE=pooled, Start() must
// be called to begin background goroutines.
func New(client *database.Client, mgr ChannelManager) *Coordinator {
	return &Coordinator{
		NodeID:          detectNodeID(),
		Mode:            channelPoolMode(),
		Client:          client,
		Manager:         mgr,
		stopCh:          make(chan struct{}),
		stuckPauseSeen:  make(map[string]int),
		liveCheckMiss:   make(map[string]int),
		recordingMarked: make(map[recMarkKey]bool),
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
	c.StartHeartbeatWatchdog(ctx)
	c.StartAssignmentSyncLoop(ctx)
	c.StartControllerAssignmentLoop(ctx)
	c.StartStuckPauseMonitorLoop(ctx)

	c.startHealthCheckLoop(ctx)
}

// Stop gracefully shuts down all coordinator loops and deregisters the node.
func (c *Coordinator) Stop() {
	if !c.IsPooled() {
		return
	}

	// Guard against double-close panic — Stop() is safe to call multiple times.
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

	// Keep assignments on graceful shutdown.  A restart must not create a pool
	// of unassigned channels or reshuffle a healthy fleet.  The controller only
	// reclaims non-recording work after nodeReclaimGrace, while a recording
	// remains pinned until its owner is genuinely offline.

	// Mark node as offline
	if err := c.Client.UpdateNodeStatus(c.NodeID, "offline"); err != nil {
		log.Printf("[coordinator] error deregistering node: %v", err)
	}

	// Zero our own load: all assignments were released above, so a stale
	// current_load would otherwise linger on the row and inflate Total Load.
	if err := c.Client.ResetNodeLoad(c.NodeID); err != nil {
		log.Printf("[coordinator] error resetting node load: %v", err)
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

	// Capture the deadline once at registration — the claim cycle's self-drain
	// reads it to pause new claims while migration is moving our channels away.
	deadline := computeSessionDeadline()
	c.mu.Lock()
	if deadline != nil {
		c.ownDeadline = *deadline
	} else {
		c.ownDeadline = time.Time{}
	}
	c.mu.Unlock()

	node := &database.Node{
		NodeID:          c.NodeID,
		Hostname:        host,
		InstanceLabel:   os.Getenv("INSTANCE_LABEL"),
		SoftwareVersion: version,
		Status:          "online",
		CurrentLoad:     0,
		WebURL:          webURL,
		SessionDeadline: deadline,
	}

	// Retry registration: a transient Supabase outage at boot (failover 502/530,
	// schema-cache hiccup 42P10) must not leave this node's row carrying a stale
	// session_deadline — the controller would then permanently deadline-migrate a
	// perfectly healthy node (fresh heartbeats, zero channels, never reassigned).
	// Mirrors the LoadPooledConfig retry budget.
	const maxRegAttempts = 8
	for attempt := 0; ; attempt++ {
		err := c.Client.UpsertNode(node)
		if err == nil {
			if deadline != nil {
				log.Printf("[coordinator] registered as node %q on %s (session deadline: %s)", c.NodeID, host, deadline.Format(time.RFC3339))
			} else {
				log.Printf("[coordinator] registered as node %q on %s (no session deadline — permanent node)", c.NodeID, host)
			}
			break
		}
		if attempt >= maxRegAttempts-1 {
			log.Printf("[coordinator] WARNING: failed to register node after %d attempts: %v — continuing registration in background", maxRegAttempts, err)
			go c.backgroundRegisterNode(node)
			break
		}
		backoff := time.Duration(1<<uint(attempt)) * 2 * time.Second
		if backoff > 15*time.Second {
			backoff = 15 * time.Second
		}
		log.Printf("[coordinator] node registration failed (attempt %d/%d), retrying in %v: %v", attempt+1, maxRegAttempts, backoff, err)
		time.Sleep(backoff)
	}
}

func (c *Coordinator) backgroundRegisterNode(node *database.Node) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		draining := c.draining
		c.mu.Unlock()
		if draining {
			return
		}
		if err := c.Client.UpsertNode(node); err == nil {
			if node.SessionDeadline != nil {
				log.Printf("[coordinator] background registration succeeded as node %q on %s (session deadline: %s)", node.NodeID, node.Hostname, node.SessionDeadline.Format(time.RFC3339))
			} else {
				log.Printf("[coordinator] background registration succeeded as node %q on %s", node.NodeID, node.Hostname)
			}
			return
		}
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

// isActive reports whether this node is currently able to own/claim channels.
// A draining or fenced (partitioned) node must not claim or migrate channels.
func (c *Coordinator) isActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.draining && !c.fenced
}

// isFenced reports whether the node is currently fenced due to a partition.
func (c *Coordinator) isFenced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fenced
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
//  1. NODE_ID env var (explicit)
//  2. GITHUB_REPOSITORY env var — splits by "-" and takes the last segment
//     so "owner/MiniDelectableService-node-a" yields "a"
//  3. os.Hostname() (VPS / local)
//  4. Random fallback (defensive)
//
// IMPORTANT: this must stay in sync with server/db.go:detectNodeID().
func detectNodeID() string {
	// Ignore placeholder values (empty/whitespace/"-") so a "-" NODE_ID secret
	// can never register a bogus "-" row in the Supabase nodes table.
	if id := strings.TrimSpace(os.Getenv("NODE_ID")); id != "" && id != "-" {
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

// channelPoolMode returns the pool mode from env var, falling back to
// auto-detection for node-* repos (GITHUB_REPOSITORY). MUST stay in sync with
// server/db.go:detectPoolMode() — a mismatch silently runs the coordinator
// isolated while the web UI thinks the node is pooled.
func channelPoolMode() string {
	if mode := os.Getenv("CHANNEL_POOL_MODE"); mode != "" {
		return mode
	}
	if repo := os.Getenv("GITHUB_REPOSITORY"); repo != "" {
		// Auto-enable pooled mode for repos named node-*
		if strings.Contains(repo, "node-") {
			return entity.PoolModePooled
		}
	}
	return entity.PoolModeIsolated
}

// computeSessionDeadline determines when this node will be forcibly killed so
// the coordinator can migrate its channels away beforehand.  The value is
// persisted on the node row (session_deadline) at registration; the deadline
// migration loop reads it.  Priority:
//  1. The effective session length on server.Config (env/flag, central
//     Supabase value, or CI fallback — see ApplyCentralSessionDuration), so the
//     recorder stop and the coordinator deadline always agree.
//  2. SESSION_DURATION env (Go duration string, e.g. "5h20m") — defensive
//     fallback if Config is unavailable.
//  3. GITHUB_RUN_ID present (CI runner, hard 6h cap) — use a buffer BEFORE the
//     workflow's 348-minute self-cancel so migration fires while we're still up.
//  4. None of the above — nil (permanent node, no deadline).
func computeSessionDeadline() *time.Time {
	if server.Config != nil && server.Config.SessionDurationParsed > 0 {
		t := time.Now().Add(server.Config.SessionDurationParsed)
		return &t
	}
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
