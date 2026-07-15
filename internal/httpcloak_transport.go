package internal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/sardanioss/httpcloak"
	"github.com/teacat/chaturbate-dvr/internal/proxy"
	"github.com/teacat/chaturbate-dvr/server"
)

// httpcloakTransport wraps httpcloak.Client as an http.RoundTripper.
// It emulates a Chrome 146 TLS/HTTP2 fingerprint to bypass Cloudflare WAF
// TCP RST that Go's default crypto/tls triggers.
// ECH (Encrypted Client Hello) hides the SNI from network observers for
// better Cloudflare bot scores.
//
// When the SOCKS5 proxy is unreachable (i/o timeout, connection refused),
// automatically rotates to the next proxy URL in the list. This handles
// the case where free proxy servers are intermittently available.
//
// On proxy rotation, fresh cookies are extracted through the new proxy via
// Scrapling (headless browser solving Cloudflare challenges), so the
// IP-bound cf_clearance always matches the current egress IP.
type httpcloakTransport struct {
	mu               sync.Mutex
	client           *httpcloak.Client
	proxyURLs        []string
	proxyIdx         int
	currentProxy     string
	refreshCancel    context.CancelFunc // cancels any in-flight Scrapling refresh
	lastRefreshProxy string             // proxy URL for which we have fresh cookies

	// refreshActive / refreshStartedAt / refreshProxy track the in-flight
	// Scrapling cookie refresh so RoundTrip can wait for it to finish instead
	// of rotating (which would cancel it and trigger a thrash loop on flaky
	// proxy pools where rotations happen faster than Scrapling can solve a
	// Cloudflare challenge).
	refreshActive    bool
	refreshStartedAt time.Time
	refreshProxy     string

	// deadUntil records proxies that recently failed with a connection error
	// (SOCKS5 unreachable / timeout / refused). They are skipped during
	// rotation for proxyDeadCooldown so a batch of dead free proxies doesn't
	// burn the per-request timeout on every request and spam cookie-refresh
	// cancellations. Cleared whenever the proxy list is refreshed.
	deadUntil map[string]time.Time
}

// proxyDeadCooldown is how long a proxy that failed with a connection error is
// avoided before being retried. Free SOCKS5 proxies die often, but a short
// cooldown lets a flaky one recover without us hammering it on every request.
const proxyDeadCooldown = 5 * time.Minute

// proxyIsDead reports whether proxyURL is currently inside its dead-cooldown
// window. Caller must have already determined the proxy is the one to test;
// this is a pure read of the supplied map (no locking) so it can be called
// while t.mu is already held.
func proxyIsDead(deadUntil map[string]time.Time, proxyURL string) bool {
	if proxyURL == "" || deadUntil == nil {
		return false
	}
	until, ok := deadUntil[proxyURL]
	if !ok {
		return false
	}
	if time.Now().Before(until) {
		return true
	}
	delete(deadUntil, proxyURL)
	return false
}

// markDead records proxyURL as temporarily unusable due to a connection
// failure so rotation skips it for proxyDeadCooldown.
func (t *httpcloakTransport) markDead(proxyURL string) {
	if proxyURL == "" {
		return
	}
	t.mu.Lock()
	if t.deadUntil == nil {
		t.deadUntil = make(map[string]time.Time)
	}
	t.deadUntil[proxyURL] = time.Now().Add(proxyDeadCooldown)
	t.mu.Unlock()
}

// cookieRefreshCooldown is how long RoundTrip will wait for an in-flight
// Scrapling cookie refresh to finish before giving up and rotating to another
// proxy. Scrapling needs 30-90s to solve a Cloudflare challenge, so rotating
// faster than this just cancels every refresh and no proxy ever gets a valid
// cf_clearance (the "proxy thrash" failure mode).
const cookieRefreshCooldown = 90 * time.Second

// sharedTransportSingleton is a singleton http.RoundTripper for the shared transport.
var sharedTransportSingleton http.RoundTripper
var sharedTransportOnce sync.Once

func getSharedTransport() http.RoundTripper {
	sharedTransportOnce.Do(func() {
		proxyURLs := configuredProxyURLs()
		if len(proxyURLs) == 0 {
			fmt.Println("[proxy] no env-configured proxies — attempting dynamic discovery...")
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			results, err := proxy.FetchProxies(ctx, 5)
			if err == nil {
				for _, r := range results {
					if r.OK {
						proxyURLs = append(proxyURLs, r.URL)
					}
				}
			}
			fmt.Printf("[proxy] dynamically discovered %d proxies\n", len(proxyURLs))
		}
		client := newCloakClient(proxyURLAt(proxyURLs, 0))
		sharedTransportSingleton = &httpcloakTransport{
			client:    client,
			proxyURLs: proxyURLs,
			deadUntil: make(map[string]time.Time),
		}
	})
	return sharedTransportSingleton
}

func proxyURLAt(urls []string, idx int) string {
	if len(urls) == 0 {
		return ""
	}
	return urls[idx%len(urls)]
}

// newCloakClient creates a new httpcloak client with the given proxy URL.
func newCloakClient(proxyURL string) *httpcloak.Client {
	opts := []httpcloak.Option{
		httpcloak.WithTimeout(120 * time.Second),
	}
	if proxyURL != "" {
		opts = append(opts, httpcloak.WithProxy(proxyURL))
	}
	return httpcloak.New("chrome-146-windows", opts...)
}

// configuredProxyURLs returns all proxy URLs (supports comma-separated for failover).
func configuredProxyURLs() []string {
	if server.Config == nil {
		return nil
	}
	raw := strings.TrimSpace(server.Config.ProxyURL)
	if raw == "" {
		return nil
	}

	username := strings.TrimSpace(server.Config.ProxyUsername)
	password := strings.TrimSpace(server.Config.ProxyPassword)

	var urls []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		part = applyProxyAuth(part, username, password)
		urls = append(urls, part)
	}
	return urls
}

func applyProxyAuth(proxyURL, username, password string) string {
	if username == "" && password == "" {
		return proxyURL
	}
	u, err := url.Parse(proxyURL)
	if err != nil || u.Scheme == "" || u.Host == "" || u.User != nil {
		return proxyURL
	}
	if password != "" {
		u.User = url.UserPassword(username, password)
	} else {
		u.User = url.User(username)
	}
	return u.String()
}

// rotateProxy recreates the httpcloak client with the next proxy in the list.
// Returns true if a different proxy was selected.
// When a new proxy is selected, cookies are refreshed asynchronously via
// Scrapling so the IP-bound cf_clearance matches the new egress IP.
func (t *httpcloakTransport) rotateProxy() bool {
	t.mu.Lock()

	if len(t.proxyURLs) <= 1 {
		t.mu.Unlock()
		return false
	}

	// Advance to the next non-dead proxy so we don't immediately retry one
	// that just failed with a connection error (which would re-trigger a
	// Scrapling cookie refresh only to cancel it on the next failure).
	start := t.proxyIdx
	chosen := -1
	for i := 1; i <= len(t.proxyURLs); i++ {
		idx := (start + i) % len(t.proxyURLs)
		if !proxyIsDead(t.deadUntil, proxyURLAt(t.proxyURLs, idx)) {
			chosen = idx
			break
		}
	}
	if chosen == -1 {
		// Every proxy is currently marked dead — fall back to the next one
		// anyway so we don't get permanently stuck (the cooldown will expire).
		chosen = (start + 1) % len(t.proxyURLs)
	}
	t.proxyIdx = chosen
	proxyURL := proxyURLAt(t.proxyURLs, t.proxyIdx)

	// Close old client if it exposes a Close method
	if c, ok := interface{}(t.client).(interface{ Close() error }); ok {
		c.Close()
	}

	t.client = newCloakClient(proxyURL)
	t.currentProxy = proxyURL

	// Cancel any in-flight Scrapling refresh for the old proxy
	if t.refreshCancel != nil {
		t.refreshCancel()
		t.refreshCancel = nil
	}

	// Debounce: skip refresh if we already have fresh cookies for this proxy
	needsRefresh := proxyURL != "" && proxyURL != t.lastRefreshProxy
	var refreshCtx context.Context
	if needsRefresh {
		refreshCtx, t.refreshCancel = context.WithCancel(context.Background())
	}
	t.mu.Unlock()

	if needsRefresh {
		go t.refreshCookies(refreshCtx, proxyURL, "proxy rotation")
	}

	return true
}

// refreshProxies re-reads the proxy list from the current config and
// resets the client to use the first (presumably freshest) proxy.
// Returns true if new proxies were loaded, false if the list is empty.
// This is called when all proxies in the current list have failed,
// allowing the DVR to pick up environment variable updates without a restart
// or dynamically discover new proxies.
//
// After loading new proxies, cookies are refreshed asynchronously via
// Scrapling so the IP-bound cf_clearance matches the new egress IP.
func (t *httpcloakTransport) refreshProxies() bool {
	t.mu.Lock()

	// All current proxies failed — collect env-configured ones first, then
	// also attempt dynamic discovery as a fallback so dead secrets don't
	// prevent the DVR from finding working proxies.
	newProxies := configuredProxyURLs()
	if len(newProxies) > 0 {
		fmt.Printf("[proxy] %d env-configured proxies present (may be stale) — also attempting dynamic discovery...\n", len(newProxies))
	} else {
		fmt.Println("[proxy] no env-configured proxies — attempting dynamic discovery...")
	}

	// Always try dynamic discovery when we're in a refresh cycle
	// (all previous proxies failed). Release the lock while fetching.
	t.mu.Unlock()
	proxy.ResetCache()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	results, err := proxy.FetchProxies(ctx, 5)
	t.mu.Lock()

	if err == nil {
		for _, r := range results {
			if r.OK {
				newProxies = append(newProxies, r.URL)
			}
		}
		fmt.Printf("[proxy] dynamically discovered %d working proxies\n", len(results))
	} else {
		fmt.Printf("[proxy] dynamic discovery failed: %v\n", err)
	}

	if len(newProxies) == 0 {
		t.mu.Unlock()
		fmt.Println("[proxy] no proxies available from env or discovery")
		return false
	}

	// Deduplicate while preserving order (env proxies first, discovered after)
	seen := make(map[string]bool)
	deduped := make([]string, 0, len(newProxies))
	for _, p := range newProxies {
		if !seen[p] {
			seen[p] = true
			deduped = append(deduped, p)
		}
	}
	newProxies = deduped

	fmt.Printf("[proxy] total proxy pool: %d URLs\n", len(newProxies))

	// Close old client if it exposes a Close method
	if c, ok := interface{}(t.client).(interface{ Close() error }); ok {
		c.Close()
	}

	t.proxyURLs = newProxies
	t.deadUntil = make(map[string]time.Time)
	t.proxyIdx = 0
	firstProxy := proxyURLAt(newProxies, 0)
	t.client = newCloakClient(firstProxy)
	t.currentProxy = firstProxy

	// Cancel any in-flight Scrapling refresh for the old proxy
	if t.refreshCancel != nil {
		t.refreshCancel()
		t.refreshCancel = nil
	}

	// Debounce: skip refresh if we already have fresh cookies for this proxy
	needsRefresh := firstProxy != "" && firstProxy != t.lastRefreshProxy
	var refreshCtx context.Context
	if needsRefresh {
		refreshCtx, t.refreshCancel = context.WithCancel(context.Background())
	}
	t.mu.Unlock()

	if needsRefresh {
		go t.refreshCookies(refreshCtx, firstProxy, "proxy refresh")
	}

	return true
}

// refreshCookies runs Scrapling through the given proxy to extract fresh
// cookies and updates server.Config so subsequent requests carry cf_clearance
// matching the new egress IP. Runs in a background goroutine — may take
// 30-90 seconds to solve a Cloudflare challenge.
// Only applies the cookies if this proxy is still the current one (no
// further rotations happened while Scrapling was running).
func (t *httpcloakTransport) refreshCookies(ctx context.Context, proxyURL, reason string) {
	fmt.Printf("[proxy] refreshing cookies through %s (%s)...\n", maskProxyHost(proxyURL), reason)

	t.mu.Lock()
	t.refreshActive = true
	t.refreshStartedAt = time.Now()
	t.refreshProxy = proxyURL
	t.mu.Unlock()
	defer func() {
		t.mu.Lock()
		// Only clear if this is still the same refresh (a rotation may have
		// started another one for a different proxy in the meantime).
		if t.refreshProxy == proxyURL {
			t.refreshActive = false
		}
		t.mu.Unlock()
	}()

	if err := UpdateCookiesFromProxyContext(ctx, proxyURL); err != nil {
		if ctx.Err() != nil {
			fmt.Printf("[proxy] cookie refresh cancelled for %s (%s)\n", maskProxyHost(proxyURL), reason)
		} else {
			fmt.Printf("[proxy] cookie refresh failed for %s: %v — requests may get 403 until manual cookie update\n",
				maskProxyHost(proxyURL), err)
		}
		return
	}

	// Only record this proxy as refreshed if it is still the current one.
	t.mu.Lock()
	if t.currentProxy == proxyURL {
		t.lastRefreshProxy = proxyURL
	}
	t.mu.Unlock()

	fmt.Printf("[proxy] cookies refreshed successfully through %s (%s)\n", maskProxyHost(proxyURL), reason)
}

// waitForActiveRefresh blocks while a Scrapling cookie refresh for currentProxy
// is in flight and within the cooldown window. Returns true if cookies for
// currentProxy are now ready (and the caller should retry the SAME proxy
// without rotating); false if there is no active refresh or it has overrun the
// cooldown (and the caller should rotate to a different proxy).
func (t *httpcloakTransport) waitForActiveRefresh(currentProxy string) bool {
	t.mu.Lock()
	active := t.refreshActive && t.refreshProxy == currentProxy
	started := t.refreshStartedAt
	t.mu.Unlock()
	if !active {
		return false
	}

	deadline := started.Add(cookieRefreshCooldown)
	for {
		t.mu.Lock()
		active = t.refreshActive && t.refreshProxy == currentProxy
		ready := t.lastRefreshProxy == currentProxy
		t.mu.Unlock()

		if !active {
			// Refresh finished — only worth retrying the same proxy if it
			// actually produced cookies for this proxy.
			return ready
		}
		if time.Now().After(deadline) {
			// Gave it the full cooldown and it's still going — give up and
			// let the caller rotate (which will cancel this refresh).
			return false
		}
		time.Sleep(2 * time.Second)
	}
}

// WarmupChaturbate makes an initial request to chaturbate.com to establish
// TLS session tickets with Cloudflare before any API calls are made.
// This gives subsequent requests TLS session resumption, making them look
// more like a returning browser visitor.
// Uses a single-attempt round trip — warmup is best-effort and should not
// retry through multiple proxies (that can delay startup by 30s per domain).
func WarmupChaturbate(ctx context.Context) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", "https://chaturbate.com/", nil)
	if err != nil {
		return
	}
	SetRequestHeaders(req)
	t, ok := getSharedTransport().(*httpcloakTransport)
	if !ok {
		return
	}
	resp, err := t.roundTripOnce(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// WarmupStripchat makes an initial request to stripchat.com to establish TLS
// session tickets before any API calls are made. This is the same idea as
// WarmupChaturbate but for Stripchat's domain.
func WarmupStripchat(ctx context.Context) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", "https://stripchat.com/", nil)
	if err != nil {
		return
	}
	SetRequestHeaders(req)
	t, ok := getSharedTransport().(*httpcloakTransport)
	if !ok {
		return
	}
	resp, err := t.roundTripOnce(req)
	if err != nil {
		return
	}
	resp.Body.Close()
}

// isProxyError checks if an error is a proxy connection failure (SOCKS5 unreachable).
// These errors trigger automatic proxy rotation.
func isProxyError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SOCKS5 CONNECT failed") ||
		strings.Contains(msg, "connect to SOCKS5 proxy") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no reachable proxy") ||
		strings.Contains(msg, "tls_handshake")
}

// cdnHostSuffixes lists CDN hostname suffixes that serve HLS segments
// with signed URLs (pkey/token). These hosts are directly reachable from
// any region — the proxy is only needed for geo-unblocking API requests
// (chaturbate.com, stripchat.com). Bypassing the proxy for CDN eliminates
// the slow-proxy → timeout → pkey-expiry failure chain.
var cdnHostSuffixes = []string{
	".doppiocdn.net",
	".doppiocdn.com",
	".live.mmcdn.com",
}

// proxyBypassHosts lists hosts that should never use the proxy.
// Stripchat doesn't need a Netherlands proxy — it has no age verification.
var proxyBypassHosts = []string{
	"stripchat.com",
	".stripchat.com",
}

func isCDNHost(host string) bool {
	host = strings.ToLower(host)
	for _, suffix := range cdnHostSuffixes {
		if strings.HasSuffix(host, suffix) {
			return true
		}
	}
	return false
}

func isProxyBypassHost(host string) bool {
	host = strings.ToLower(host)
	for _, h := range proxyBypassHosts {
		if host == h || strings.HasSuffix(host, h) {
			return true
		}
	}
	return false
}

// roundTripOnce executes a single request attempt using the current httpcloak
// client. No proxy rotation — used by warmup functions (best-effort).
func (t *httpcloakTransport) roundTripOnce(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "http" || isCDNHost(req.URL.Host) || isProxyBypassHost(req.URL.Host) {
		return http.DefaultTransport.RoundTrip(req)
	}

	ctx := req.Context()
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	t.mu.Lock()
	client := t.client
	t.mu.Unlock()

	cloakReq := &httpcloak.Request{
		Method:  req.Method,
		URL:     req.URL.String(),
		Headers: req.Header,
	}
	if len(bodyBytes) > 0 {
		cloakReq.Body = bytes.NewReader(bodyBytes)
	}

	cloakResp, err := client.Do(ctx, cloakReq)
	if err != nil {
		return nil, err
	}

	body, bodyErr := cloakResp.Bytes()
	if bodyErr != nil {
		cloakResp.Close()
		return nil, bodyErr
	}

	resp := &http.Response{
		StatusCode: cloakResp.StatusCode,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    req,
	}
	if cloakResp.Headers != nil {
		for k, vs := range cloakResp.Headers {
			for _, v := range vs {
				resp.Header.Add(k, v)
			}
		}
	}
	return resp, nil
}

// RoundTrip implements http.RoundTripper. CDN requests bypass the proxy
// entirely. API requests use httpcloak with the SOCKS5 proxy, and
// automatically rotate to the next proxy on connection failure.
// Returns error immediately if no proxies are configured — never falls back
// to direct connection (which would fail face-id verification).
func (t *httpcloakTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme == "http" || isCDNHost(req.URL.Host) || isProxyBypassHost(req.URL.Host) {
		return http.DefaultTransport.RoundTrip(req)
	}

	t.mu.Lock()
	noProxy := len(t.proxyURLs) == 0
	t.mu.Unlock()
	if noProxy {
		return nil, fmt.Errorf("no proxy available — cannot reach %s without SOCKS5 proxy (direct connection blocked by face-id)", req.URL.Host)
	}

	ctx := req.Context()
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	// Prepare request body once, reuse across retries
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	// Try up to len(proxyURLs) attempts, rotating proxy on connection failures.
	// If all proxies fail, try to refresh the proxy list from env and retry once.
	// Track per-proxy errors for better diagnostics.
	for refresh := 0; refresh < 2; refresh++ {
		for attempt := 0; attempt < max(1, len(t.proxyURLs)); attempt++ {
			t.mu.Lock()
			client := t.client
			currentProxy := proxyURLAt(t.proxyURLs, t.proxyIdx)
			t.mu.Unlock()

			cloakReq := &httpcloak.Request{
				Method:  req.Method,
				URL:     req.URL.String(),
				Headers: req.Header,
			}
			if len(bodyBytes) > 0 {
				cloakReq.Body = bytes.NewReader(bodyBytes)
			}

			cloakResp, err := client.Do(ctx, cloakReq)

			if err == nil {
				body, bodyErr := cloakResp.Bytes()
				if bodyErr != nil {
					cloakResp.Close()
					return nil, bodyErr
				}

				resp := &http.Response{
					StatusCode: cloakResp.StatusCode,
					Header:     make(http.Header),
					Body:       io.NopCloser(bytes.NewReader(body)),
					Request:    req,
				}
				if cloakResp.Headers != nil {
					for k, vs := range cloakResp.Headers {
						for _, v := range vs {
							resp.Header.Add(k, v)
						}
					}
				}
				return resp, nil
			}

			// Proxy connection failure — rotate to next proxy in the list
			if isProxyError(err) {
				// Remember this proxy as dead so we don't keep retrying it on
				// every request (and spinning up/cancelling Scrapling refreshes).
				t.markDead(currentProxy)
				// If a cookie refresh is already in flight for this proxy and
				// hasn't had time to finish, wait for it instead of rotating.
				// Rotating now would cancel the Scrapling refresh, and a flaky
				// proxy pool would then cycle rotations forever without ever
				// obtaining a valid cf_clearance (the "proxy thrash" loop).
				if t.waitForActiveRefresh(currentProxy) {
					fmt.Printf("[proxy] waited for in-flight cookie refresh for %s — retrying same proxy\n",
						maskProxyHost(currentProxy))
					continue
				}
				if t.rotateProxy() {
					continue
				}
				// Only 1 proxy URL configured and it failed — log the specific error
				return nil, fmt.Errorf("proxy %s: %w", maskProxyHost(currentProxy), err)
			}
			// Non-proxy error (e.g. HTTP-level failure) — surface immediately
			return nil, err
		}

		// All proxies in the current list failed. Try to refresh from env.
		// This handles the case where free proxies have died and the env
		// was updated (e.g. by a wrapper script that re-fetches proxy lists).
		if t.refreshProxies() {
			fmt.Printf("[proxy] all proxies failed — refreshed proxy list from env, retrying with %d URLs\n",
				len(t.proxyURLs))
			continue
		}
		break
	}

	// Build a detailed error message with all proxy URLs tried
	t.mu.Lock()
	proxyCount := len(t.proxyURLs)
	firstProxy := ""
	if proxyCount > 0 {
		firstProxy = maskProxyHost(t.proxyURLs[0])
	}
	t.mu.Unlock()

	return nil, fmt.Errorf("all %d proxies failed (first: %s) — proxy URLs may be unreachable; check proxy configuration or refresh the proxy list",
		proxyCount, firstProxy)
}

// maskProxyHost masks the password portion of a proxy URL for safe logging.
// e.g. "socks5://user:pass@host:1080" → "socks5://user:***@host:1080"
func maskProxyHost(proxyURL string) string {
	if proxyURL == "" {
		return "(none)"
	}
	u, err := url.Parse(proxyURL)
	if err != nil {
		// If we can't parse it, just show the scheme + host
		if strings.Contains(proxyURL, "@") {
			parts := strings.SplitN(proxyURL, "@", 2)
			return "***@" + parts[len(parts)-1]
		}
		return proxyURL
	}
	if u.User != nil {
		if _, hasPW := u.User.Password(); hasPW {
			u.User = url.UserPassword(u.User.Username(), "***")
		} else {
			u.User = url.User(u.User.Username())
		}
		return u.String()
	}
	// No auth — just show the host:port
	if u.Host != "" {
		return u.Scheme + "://" + u.Host
	}
	return proxyURL
}

// max returns the larger of a and b.
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
