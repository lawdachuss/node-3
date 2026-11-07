package router

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/teacat/chaturbate-dvr/recovery"
	"github.com/teacat/chaturbate-dvr/server"
)

// Streamtape play relay.
//
// The video page used to embed https://streamtape.com/e/{code}/ in an iframe.
// Streamtape's own player refuses to negotiate the stream there — the browser
// gets "Video not available" whenever third-party cookies are blocked or the
// embedding origin has no trusted referer/session. Instead of fighting cookie
// policies, this relay makes the DVR node itself resolve the tokenized direct
// CDN URL (the same dlticket->dl dance the thumbnail recovery already uses,
// which is proven to work from this IP space) and pipes the bytes back over a
// same-origin HTTP response. The browser then plays it with the native <video>
// element — no Streamtape iframe, no cookies, no embedding rules involved.

// stPlayTTL is how long a resolved direct URL is reused. Tickets are
// short-lived; the TTL only absorbs the per-play latency of a fresh ticket.
const stPlayTTL = 2 * time.Minute

type stPlayEntry struct {
	url      string
	resolved time.Time
}

var (
	stPlayMu    sync.Mutex
	stPlayCache = map[string]stPlayEntry{}
)

// stPlayAllowedHost reports whether an upstream host may be proxied. Streamtape
// CDN URLs land on <n>.tapecontent.net; nothing else is ever trusted. The port
// (if present) is stripped so both "host" and "host:443" match.
func stPlayAllowedHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if i := strings.LastIndexByte(host, ':'); i > 0 && !strings.Contains(host[i:], "]") {
		host = host[:i]
	}
	return host == "tapecontent.net" || strings.HasSuffix(host, ".tapecontent.net")
}

// streamtapePlayCreds returns the currently effective Streamtape API creds.
func streamtapePlayCreds() (login, key string) {
	server.ConfigMu.Lock()
	defer server.ConfigMu.Unlock()
	return server.Config.StreamtapeLogin, server.Config.StreamtapeKey
}

// StreamtapePlay proxies the tokenized Streamtape CDN stream to the browser.
// GET /api/play/streamtape/:code
func StreamtapePlay(c *gin.Context) {
	code := strings.TrimSpace(c.Param("code"))
	if recovery.ExtractStreamtapeCode(code) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "bad Streamtape code"})
		return
	}

	login, key := streamtapePlayCreds()
	if login == "" || key == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Streamtape API credentials not configured"})
		return
	}

	// Resolve the direct URL, honoring the short cache.
	upstream := ""
	stPlayMu.Lock()
	if e, ok := stPlayCache[code]; ok && time.Since(e.resolved) < stPlayTTL {
		upstream = e.url
	}
	stPlayMu.Unlock()

	if upstream == "" {
		u, err := recovery.ResolveStreamtapePlayURL(code, login, key)
		if err != nil {
			status := http.StatusBadGateway
			msg := err.Error()
			if errors.Is(err, recovery.ErrStreamFileNotFound) {
				// Permanent condition: the file is gone from the account.
				status = http.StatusNotFound
				msg = "streamtape copy no longer exists"
			}
			c.JSON(status, gin.H{"error": msg})
			return
		}
		upstream = u
		stPlayMu.Lock()
		stPlayCache[code] = stPlayEntry{url: upstream, resolved: time.Now()}
		stPlayMu.Unlock()
	}

	parsed, err := url.Parse(upstream)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || !stPlayAllowedHost(parsed.Host) {
		c.JSON(http.StatusBadGateway, gin.H{"error": fmt.Sprintf("refusing to proxy non-Streamtape upstream %q", parsed.Host)})
		return
	}

	req, err := http.NewRequest(http.MethodGet, upstream, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "build request: " + err.Error()})
		return
	}
	req.Header.Set("User-Agent", browserUserAgentForProxy)
	// The native <video> element seeks with Range requests; pass them through so
	// seeking and resumed playback work on the proxied stream.
	if rng := c.Request.Header.Get("Range"); rng != "" {
		req.Header.Set("Range", rng)
	}

	resp, err := recovery.StreamtapeHTTPClient().Do(req)
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": "upstream fetch: " + err.Error()})
		return
	}
	defer resp.Body.Close()

	for _, h := range []string{"Content-Type", "Content-Length", "Accept-Ranges", "Content-Range", "Content-Disposition"} {
		if v := resp.Header.Get(h); v != "" {
			c.Header(h, v)
		}
	}
	c.Status(resp.StatusCode)
	if _, err := io.Copy(c.Writer, resp.Body); err != nil {
		return
	}
}

// browserUserAgentForProxy is Streamtape's own embed player UA shape; the CDN
// serves the same file to any browser UA, so a realistic value avoids an
// upstream rejection based on UA sniffing.
const browserUserAgentForProxy = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
