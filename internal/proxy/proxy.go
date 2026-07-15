package proxy

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sardanioss/httpcloak"
)

type ProxyResult struct {
	URL      string
	EgressIP string
	Country  string
	OK       bool
}

var proxySources = []string{
	"https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&format=text&protocol=socks5&country=nl&timeout=5000",
	"https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&format=text&protocol=socks5&country=in&timeout=5000",
	"https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&format=text&protocol=socks5&country=de&timeout=5000",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt",
	"https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
	"https://cdn.jsdelivr.net/gh/proxyscrape/free-proxy-list@main/proxies/countries/nl/socks5/data.txt",
}

// allowedProxyCountries is the set of egress countries known to NOT trigger
// Chaturbate's face-id verification. Dynamic discovery only accepts proxies
// whose egress IP geolocates to one of these — every request must egress
// through a face-id-free region or Cloudflare will challenge it (and the
// GitHub Actions runner IPs always get challenged). Override with the
// PROXY_ALLOWED_COUNTRIES env var (comma-separated ISO-3166 codes, e.g. "NL").
var allowedProxyCountries = loadAllowedProxyCountries()

func loadAllowedProxyCountries() map[string]bool {
	raw := strings.TrimSpace(os.Getenv("PROXY_ALLOWED_COUNTRIES"))
	if raw == "" {
		raw = "NL"
	}
	m := make(map[string]bool)
	for _, c := range strings.Split(raw, ",") {
		c = strings.TrimSpace(strings.ToUpper(c))
		if c != "" {
			m[c] = true
		}
	}
	return m
}

var (
	cachedProxies []ProxyResult
	cacheTime     time.Time
	cacheMu       sync.Mutex
	cacheTTL      = 5 * time.Minute
)

// ResetCache clears the proxy cache so the next call to FetchProxies
// fetches fresh proxies from all sources instead of returning stale ones.
func ResetCache() {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	cacheTime = time.Time{}
	cachedProxies = nil
}

// FetchProxies fetches SOCKS5 proxies from public lists, tests them
// concurrently using httpcloak (Chrome 146), and returns as soon as
// `limit` working proxies are found (or all are exhausted).
func FetchProxies(ctx context.Context, limit int) ([]ProxyResult, error) {
	cacheMu.Lock()
	if len(cachedProxies) > 0 && time.Since(cacheTime) < cacheTTL {
		result := append([]ProxyResult{}, cachedProxies...)
		cacheMu.Unlock()
		return result, nil
	}
	cacheMu.Unlock()

	allURLs := fetchAllProxyURLs(ctx)
	if len(allURLs) == 0 {
		return nil, fmt.Errorf("no proxies fetched from any source")
	}

	fmt.Printf("[proxy] scanning %d proxies, need %d...\n", len(allURLs), limit)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		results   []ProxyResult
		resultsMu sync.Mutex
		needed    = limit
		found     atomic.Int32
		sem       = make(chan struct{}, 20)
		wg        sync.WaitGroup
	)

	for _, u := range allURLs {
		if found.Load() >= int32(needed) {
			break
		}
		wg.Add(1)

		go func(proxyURL string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if found.Load() >= int32(needed) || ctx.Err() != nil {
				return
			}

			r := testProxy(ctx, proxyURL)
			if !r.OK {
				return
			}

			resultsMu.Lock()
			if found.Load() < int32(needed) && ctx.Err() == nil {
				results = append(results, r)
				n := found.Add(1)
				fmt.Printf("[proxy] found %d/%d: %s [%s]\n", n, needed, proxyURL, r.Country)
				if n >= int32(needed) {
					cancel()
				}
			}
			resultsMu.Unlock()
		}(u)
	}
	wg.Wait()

	if len(results) == 0 {
		return nil, fmt.Errorf("no working proxies found after scanning %d", len(allURLs))
	}

	results = sortProxies(results)

	cacheMu.Lock()
	cachedProxies = append([]ProxyResult{}, results...)
	cacheTime = time.Now()
	cacheMu.Unlock()

	return results, nil
}

func testProxy(ctx context.Context, proxyURL string) ProxyResult {
	opts := []httpcloak.Option{
		httpcloak.WithTimeout(12 * time.Second),
		httpcloak.WithProxy(proxyURL),
	}
	client := httpcloak.New("chrome-146-windows", opts...)
	if c, ok := interface{}(client).(interface{ Close() error }); ok {
		defer c.Close()
	}

	// Egress check
	egressIP := checkEgress(ctx, client)
	if egressIP == "" {
		return ProxyResult{URL: proxyURL, OK: false}
	}

	// Chaturbate reachability + Cloudflare/face-id challenge check. This is the
	// authoritative signal that the proxy's egress can actually bypass the
	// face-id verification the GitHub Actions runner IPs would otherwise hit.
	if !checkChaturbate(ctx, client) {
		return ProxyResult{URL: proxyURL, EgressIP: egressIP, OK: false}
	}

	// Region gate: only keep egress from known face-id-free countries. Geo
	// lookup is done last (and only for proxies that already passed) to limit
	// rate-limited lookups. If geolocation is unavailable we trust the
	// Chaturbate check above; if it resolves to a disallowed region we drop it.
	country := lookupCountry(egressIP)
	if country != "" && !allowedProxyCountries[strings.ToUpper(country)] {
		return ProxyResult{URL: proxyURL, EgressIP: egressIP, Country: country, OK: false}
	}

	return ProxyResult{
		URL:      proxyURL,
		EgressIP: egressIP,
		Country:  country,
		OK:       true,
	}
}

func checkEgress(ctx context.Context, client *httpcloak.Client) string {
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := client.Do(reqCtx, &httpcloak.Request{
		Method:  "GET",
		URL:     "https://api.ipify.org",
		Timeout: 10 * time.Second,
	})
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return strings.TrimSpace(string(body))
}

func checkChaturbate(ctx context.Context, client *httpcloak.Client) bool {
	reqCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	resp, err := client.Do(reqCtx, &httpcloak.Request{
		Method:  "GET",
		URL:     "https://chaturbate.com",
		Timeout: 12 * time.Second,
	})
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return false
	}

	// Reject Cloudflare/face-id redirect challenges.
	if loc, ok := resp.Headers["Location"]; ok && len(loc) > 0 {
		lower := strings.ToLower(loc[0])
		if strings.Contains(lower, "verify") || strings.Contains(lower, "captcha") ||
			strings.Contains(lower, "face") || strings.Contains(lower, "human") {
			return false
		}
	}

	// Reject challenge pages served as HTTP 200 (Cloudflare "Just a moment",
	// "verify you are human", captcha/interstitial forms). The markers appear
	// near the top of the document, so a capped read is sufficient.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	lower := strings.ToLower(string(body))
	for _, marker := range []string{
		"just a moment",
		"verify you are human",
		"verify you're human",
		"checking your browser",
		"attention required",
		"captcha",
		"cf-chl",
		"challenge-platform",
		"why am i seeing this",
		"ddos",
		"face verification",
		"verify your identity",
		"enable javascript and cookies",
	} {
		if strings.Contains(lower, marker) {
			return false
		}
	}

	// A real page has substantial content; tiny stubs are challenge/error pages.
	if len(body) < 500 {
		return false
	}
	return true
}

func fetchAllProxyURLs(ctx context.Context) []string {
	client := &http.Client{Timeout: 15 * time.Second}
	seen := make(map[string]bool)
	var all []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, source := range proxySources {
		wg.Add(1)
		go func(src string) {
			defer wg.Done()
			reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			req, err := http.NewRequestWithContext(reqCtx, "GET", src, nil)
			if err != nil {
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}
			lines := strings.Split(string(body), "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				var proxyURL string
				if strings.HasPrefix(line, "socks5://") {
					proxyURL = line
				} else if strings.Contains(line, ":") {
					proxyURL = "socks5://" + line
				} else {
					continue
				}
				mu.Lock()
				if !seen[proxyURL] {
					seen[proxyURL] = true
					all = append(all, proxyURL)
				}
				mu.Unlock()
			}
		}(source)
	}
	wg.Wait()

	rand.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	return all
}

func lookupCountry(ip string) string {
	if ip == "" {
		return ""
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://ip-api.com/json/" + ip + "?fields=countryCode")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	// response is like {"countryCode":"NL"}
	s := strings.TrimSpace(string(body))
	if idx := strings.Index(s, `":"`); idx > 0 {
		s = s[idx+3:]
		if idx2 := strings.Index(s, `"`); idx2 > 0 {
			s = s[:idx2]
		}
	}
	return s
}

func sortProxies(proxies []ProxyResult) []ProxyResult {
	preferred := map[string]int{"NL": 0, "IN": 1, "DE": 2}
	sorted := append([]ProxyResult{}, proxies...)
	for i := 0; i < len(sorted); i++ {
		for j := i + 1; j < len(sorted); j++ {
			pi := preferred[sorted[i].Country]
			pj := preferred[sorted[j].Country]
			if pj < pi || (pj == pi && sorted[i].Country == "" && sorted[j].Country != "") {
				sorted[i], sorted[j] = sorted[j], sorted[i]
			}
		}
	}
	return sorted
}
