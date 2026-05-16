package internal

import (
        "bytes"
        "context"
        "encoding/json"
        "fmt"
        "io"
        "net/http"
        "os"
        "strings"
        "time"

        "github.com/teacat/chaturbate-dvr/server"
)

// getFlareSolverrURL returns the FlareSolverr/Byparr URL
// Supports both load-balanced (byparr-lb) and direct instances
func getFlareSolverrURL() string {
        baseURL := os.Getenv("FLARESOLVERR_URL")
        if baseURL == "" {
                baseURL = "http://localhost:8191/v1"
        }
        return baseURL
}

type flareSolverrRequest struct {
        Cmd        string `json:"cmd"`
        URL        string `json:"url"`
        MaxTimeout int    `json:"maxTimeout"`
        Session    string `json:"session,omitempty"`
        Proxy      struct {
                URL string `json:"url,omitempty"`
        } `json:"proxy,omitempty"`
}

type flareSolverrResponse struct {
        Status   string `json:"status"`
        Message  string `json:"message"`
        Solution struct {
                URL      string `json:"url"`
                Status   int    `json:"status"`
                Response string `json:"response"` // HTML content
                Cookies  []struct {
                        Name     string  `json:"name"`
                        Value    string  `json:"value"`
                        Domain   string  `json:"domain"`
                        Path     string  `json:"path"`
                        Expires  float64 `json:"expires"`
                        Size     int     `json:"size"`
                        HttpOnly bool    `json:"httpOnly"`
                        Secure   bool    `json:"secure"`
                        SameSite string  `json:"sameSite"`
                } `json:"cookies"`
                UserAgent string `json:"userAgent"`
        } `json:"solution"`
}

// GetFreshCookiesViaFlareSolverr uses FlareSolverr/Byparr to bypass Cloudflare and get fresh cookies
func GetFreshCookiesViaFlareSolverr(ctx context.Context, url string) (string, string, error) {
        flaresolverrURL := getFlareSolverrURL()

        // Create a unique session for this request to avoid conflicts
        sessionID := fmt.Sprintf("session_%d", time.Now().UnixNano())

        // First, create a session
        createSessionReq := flareSolverrRequest{
                Cmd:     "sessions.create",
                Session: sessionID,
        }

        jsonData, err := json.Marshal(createSessionReq)
        if err != nil {
                return "", "", fmt.Errorf("marshal session request: %w", err)
        }

        req, err := http.NewRequestWithContext(ctx, "POST", flaresolverrURL, bytes.NewBuffer(jsonData))
        if err != nil {
                return "", "", fmt.Errorf("create session request: %w", err)
        }
        req.Header.Set("Content-Type", "application/json")

        client := &http.Client{Timeout: 60 * time.Second}
        resp, err := client.Do(req)
        if err != nil {
                return "", "", fmt.Errorf("create session: %w", err)
        }
        resp.Body.Close()

        // Now make the actual request with the session
        // CRITICAL: maxTimeout must be in milliseconds and sent in API request
        // Byparr ignores TIMEOUT env var - this is the only way to extend timeout
        reqBody := flareSolverrRequest{
                Cmd:        "request.get",
                URL:        url,
                MaxTimeout: 180000, // 180 seconds (180000ms) for Cloudflare 2026 challenges
                Session:    sessionID,
        }

        // Add proxy configuration if available
        proxyURL := os.Getenv("PROXY_URL")
        proxyUsername := os.Getenv("PROXY_USERNAME")
        proxyPassword := os.Getenv("PROXY_PASSWORD")

        // Only set proxy if URL is provided and not empty
        if proxyURL != "" {
                reqBody.Proxy.URL = proxyURL
                if proxyUsername != "" && proxyPassword != "" && !strings.Contains(proxyURL, "@") {
                        // Embed credentials in the URL
                        // Format: http://username:password@proxy.com:port
                        if strings.HasPrefix(proxyURL, "http://") {
                                reqBody.Proxy.URL = strings.Replace(proxyURL, "http://", fmt.Sprintf("http://%s:%s@", proxyUsername, proxyPassword), 1)
                        } else if strings.HasPrefix(proxyURL, "https://") {
                                reqBody.Proxy.URL = strings.Replace(proxyURL, "https://", fmt.Sprintf("https://%s:%s@", proxyUsername, proxyPassword), 1)
                        }
                }
        }

        jsonData, err = json.Marshal(reqBody)
        if err != nil {
                return "", "", fmt.Errorf("marshal request: %w", err)
        }

        req, err = http.NewRequestWithContext(ctx, "POST", flaresolverrURL, bytes.NewBuffer(jsonData))
        if err != nil {
                return "", "", fmt.Errorf("create request: %w", err)
        }

        req.Header.Set("Content-Type", "application/json")

        client = &http.Client{Timeout: 360 * time.Second} // 6 minutes for Cloudflare challenges + queue wait
        resp, err = client.Do(req)
        if err != nil {
                // Check if it's a timeout or connection error
                if strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline exceeded") {
                        return "", "", fmt.Errorf("byparr timeout after 360s (cloudflare 2026 challenges are very aggressive): %w", err)
                }
                if strings.Contains(err.Error(), "connection refused") || strings.Contains(err.Error(), "no such host") {
                        return "", "", fmt.Errorf("byparr not accessible (is it running?): %w", err)
                }
                return "", "", fmt.Errorf("byparr request failed: %w", err)
        }
        defer resp.Body.Close()

        body, err := io.ReadAll(resp.Body)
        if err != nil {
                return "", "", fmt.Errorf("read byparr response: %w", err)
        }

        var fsResp flareSolverrResponse
        if err := json.Unmarshal(body, &fsResp); err != nil {
                bodyPreview := string(body)
                if len(bodyPreview) > 200 {
                        bodyPreview = bodyPreview[:200]
                }
                return "", "", fmt.Errorf("parse byparr response: %w (body: %s)", err, bodyPreview)
        }

        // Clean up the session
        defer func() {
                destroyReq := flareSolverrRequest{
                        Cmd:     "sessions.destroy",
                        Session: sessionID,
                }
                destroyData, _ := json.Marshal(destroyReq)
                destroyHttpReq, _ := http.NewRequest("POST", flaresolverrURL, bytes.NewBuffer(destroyData))
                destroyHttpReq.Header.Set("Content-Type", "application/json")
                client.Do(destroyHttpReq)
        }()

        if fsResp.Status != "ok" {
                // Check for specific error patterns
                errMsg := fsResp.Message
                if errMsg == "" {
                        errMsg = "unknown error (empty response)"
                }

                if strings.Contains(errMsg, "%d format") || strings.Contains(errMsg, "NoneType") {
                        return "", "", fmt.Errorf("byparr challenge failed (likely needs residential proxy): %s", errMsg)
                }
                if strings.Contains(errMsg, "timeout") || strings.Contains(errMsg, "timed out") || strings.Contains(errMsg, "Timed out") {
                        return "", "", fmt.Errorf("byparr timeout after 180s (cloudflare 2026 is very aggressive - consider residential proxies): %s", errMsg)
                }
                if strings.Contains(errMsg, "unknown error") {
                        return "", "", fmt.Errorf("byparr failed to solve challenge (may need more memory or residential proxy)")
                }
                return "", "", fmt.Errorf("byparr error: %s", errMsg)
        }

        // Extract cookies
        var cookieParts []string
        for _, cookie := range fsResp.Solution.Cookies {
                if cookie.Name == "cf_clearance" || cookie.Name == "csrftoken" {
                        cookieParts = append(cookieParts, fmt.Sprintf("%s=%s", cookie.Name, cookie.Value))
                }
        }

        if len(cookieParts) == 0 {
                return "", "", fmt.Errorf("no cookies found in response")
        }

        cookieStr := strings.Join(cookieParts, "; ")
        userAgent := fsResp.Solution.UserAgent

        return cookieStr, userAgent, nil
}

// FetchStreamViaFlareSolverr uses FlareSolverr to get the HLS stream URL when Cloudflare blocks direct access
func FetchStreamViaFlareSolverr(ctx context.Context, username string) (string, string, error) {
        flaresolverrURL := getFlareSolverrURL()
        if flaresolverrURL == "" {
                return "", "", fmt.Errorf("FLARESOLVERR_URL not configured")
        }

        // Step 1: Get the page via FlareSolverr to obtain fresh cookies and user-agent
        pageURL := fmt.Sprintf("%s%s/", server.Config.Domain, username)
        cookies, userAgent, err := GetFreshCookiesViaFlareSolverr(ctx, pageURL)
        if err != nil {
                return "", "", fmt.Errorf("get fresh cookies: %w", err)
        }

        // Step 2: Update server config with fresh cookies and user agent
        if cookies != "" {
                server.Config.Cookies = cookies
        }
        if userAgent != "" {
                server.Config.UserAgent = userAgent
        }

        // Step 3: Use the fresh cookies to call the POST API
        // (Same as direct API call, but now we have valid Cloudflare cookies)
        // This is more reliable than trying to extract m3u8 URL from HTML
        csrfToken := fmt.Sprintf("%016x%016x", time.Now().UnixNano(), time.Now().UnixNano()^0xDEADBEEF)
        
        body, err := PostChaturbateAPI(ctx, username, csrfToken)
        if err != nil {
                // If POST API still fails after FlareSolverr bypass, room is truly offline/private
                if strings.Contains(err.Error(), "forbidden") {
                        return "", "private", nil
                }
                return "", "offline", nil
        }

        // Parse POST API response to get HLS URL and room status
        var resp struct {
                HLSSource  string `json:"hls_source"`
                RoomStatus string `json:"room_status"`
        }
        if err := json.Unmarshal([]byte(body), &resp); err != nil {
                return "", "offline", fmt.Errorf("failed to parse POST API response: %w", err)
        }

        // Handle room status
        switch resp.RoomStatus {
        case "private":
                return "", "private", nil
        case "away", "offline":
                return "", "offline", nil
        }

        // If no HLS source, room is offline
        if resp.HLSSource == "" {
                return "", "offline", nil
        }

        // Successfully got HLS URL via FlareSolverr bypass
        return resp.HLSSource, "public", nil
}
