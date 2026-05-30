package internal

import (
        "bytes"
        "context"
        "encoding/json"
        "fmt"
        "io"
        "net/http"
        "net/url"
        "os"
        "strings"
        "time"

        "github.com/teacat/chaturbate-dvr/server"
)

// getFlareSolverrURL returns the FlareSolverr/Byparr URL.
// Priority: FLARESOLVERR_URL env var > UI-configured value (server.Config.ByparrURL) > localhost default.
// Env var is checked first so docker-compose can override a stale Supabase persisted setting.
func getFlareSolverrURL() string {
        if baseURL := os.Getenv("FLARESOLVERR_URL"); baseURL != "" {
                return baseURL
        }
        if server.Config != nil && server.Config.ByparrURL != "" {
                return server.Config.ByparrURL
        }
        return "http://localhost:8191/v1"
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

func isValidHTTPURL(raw string) bool {
        parsed, err := url.ParseRequestURI(strings.TrimSpace(raw))
        if err != nil {
                return false
        }
        if parsed.Scheme != "http" && parsed.Scheme != "https" {
                return false
        }
        return parsed.Host != ""
}

// GetFreshCookiesViaFlareSolverr uses FlareSolverr/Byparr to bypass Cloudflare and get fresh cookies
func GetFreshCookiesViaFlareSolverr(ctx context.Context, url string) (string, string, error) {
        if !isValidHTTPURL(url) {
                return "", "", fmt.Errorf("byparr request URL must be a valid http(s) URL: %q", url)
        }

        flaresolverrURL := getFlareSolverrURL()
        if !isValidHTTPURL(flaresolverrURL) {
                return "", "", fmt.Errorf("FLARESOLVERR_URL must be a valid http(s) URL: %q", flaresolverrURL)
        }

        // Create a unique session for this request to avoid conflicts
        sessionID := fmt.Sprintf("session_%d", time.Now().UnixNano())

        // First, create a session
        // Note: must include a dummy URL because Byparr middleware validates all requests against
        // a LinkRequest model requiring url to match ^https?://
        createSessionReq := flareSolverrRequest{
                Cmd:     "sessions.create",
                URL:     "https://chaturbate.com",
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
        if resp.StatusCode >= 400 {
                return "", "", fmt.Errorf("create session: byparr returned HTTP %d", resp.StatusCode)
        }

        // Now make the actual request with the session
        // CRITICAL: maxTimeout must be in milliseconds and sent in API request
        // Byparr ignores TIMEOUT env var - this is the only way to extend timeout
        reqBody := flareSolverrRequest{
                Cmd:        "request.get",
                URL:        url,
                MaxTimeout: 180000, // 180 seconds for Cloudflare 2026 challenges (compatible with official byparr)
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

        if resp.StatusCode >= 400 {
                bodyPreview := string(body)
                if len(bodyPreview) > 200 {
                        bodyPreview = bodyPreview[:200]
                }
                return "", "", fmt.Errorf("byparr returned HTTP %d: %s", resp.StatusCode, bodyPreview)
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
        destroyReq := flareSolverrRequest{
                Cmd:     "sessions.destroy",
                Session: sessionID,
        }
        destroyData, _ := json.Marshal(destroyReq)
        destroyHttpReq, _ := http.NewRequestWithContext(ctx, "POST", flaresolverrURL, bytes.NewBuffer(destroyData))
        destroyHttpReq.Header.Set("Content-Type", "application/json")
        destroyClient := &http.Client{Timeout: 10 * time.Second}
        destroyClient.Do(destroyHttpReq)

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
// If roomInfo is non-nil, it is populated with room metadata (title, tags, viewers).
func FetchStreamViaFlareSolverr(ctx context.Context, username string, roomInfo *StreamAPIBody) (string, string, error) {
        flaresolverrURL := getFlareSolverrURL()
        if flaresolverrURL == "" {
                return "", "", fmt.Errorf("FLARESOLVERR_URL not configured")
        }
        if !isValidHTTPURL(flaresolverrURL) {
                return "", "", fmt.Errorf("FLARESOLVERR_URL must be a valid http(s) URL: %q", flaresolverrURL)
        }

        // Step 1: Get the page via FlareSolverr to obtain fresh cookies and user-agent
        pageURL := fmt.Sprintf("%s%s/", server.Config.Domain, username)
        cookies, userAgent, err := GetFreshCookiesViaFlareSolverr(ctx, pageURL)
        if err != nil {
                return "", "", fmt.Errorf("get fresh cookies: %w", err)
        }

        // Step 2: Update server config with fresh cookies and user agent, then persist
        if cookies != "" || userAgent != "" {
                server.UpdateByparrCredentials(cookies, userAgent)
        }
        if err := server.SaveSettings(); err != nil {
                fmt.Printf("[WARN] could not persist byparr cookies: %v\n", err)
        }

        // Step 3: POST API with Byparr cookies (csrftoken must match cookie header).
        body, err := PostChaturbateAPI(ctx, username, "")
        if err != nil {
                if strings.Contains(err.Error(), "forbidden") {
                        return "", "private", fmt.Errorf("post api after byparr: %w", err)
                }
                return "", "", fmt.Errorf("post api after byparr: %w", err)
        }

        hlsURL, roomStatus, apiBody, err := parseStreamAPIBody(body)
        if err != nil {
                return "", "", err
        }
        if roomInfo != nil && apiBody != nil {
                roomInfo.RoomTitle = apiBody.RoomTitle
                roomInfo.Tags = apiBody.Tags
                roomInfo.NumUsers = apiBody.NumUsers
        }

        if hlsURL != "" {
                return hlsURL, roomStatus, nil
        }

        // Debug: log raw body when POST API returned no hls_source
        fmt.Printf("[DEBUG] PostChaturbateAPI returned empty hls_source/url for %s (room_status=%q). Raw body: %s\n", username, roomStatus, body)

        // Step 4: POST may return public with empty hls_source; try GET chatvideocontext.
        if roomStatus == "public" {
                if hlsURL, roomStatus, apiBody, err = fetchHLSSourceViaGET(ctx, username); err != nil {
                        return "", "", err
                }
                if roomInfo != nil && apiBody != nil {
                        roomInfo.RoomTitle = apiBody.RoomTitle
                        roomInfo.Tags = apiBody.Tags
                        roomInfo.NumUsers = apiBody.NumUsers
                }
                if hlsURL != "" {
                        return hlsURL, roomStatus, nil
                }
        }

        if roomStatus == "" {
                roomStatus = "offline"
        }
        return "", roomStatus, fmt.Errorf("no hls_source after byparr bypass (room_status=%s)", roomStatus)
}

// StreamAPIBody mirrors chaturbate.APIResponse for use within the internal
// package. The matching field layout allows direct Go struct conversion.
type StreamAPIBody struct {
        HLSSource          string   `json:"hls_source"`
        URL                string   `json:"url"`
        RoomStatus         string   `json:"room_status"`
        RoomTitle          string   `json:"room_title"`
        Tags               []string `json:"tags"`
        NumUsers           int      `json:"num_users"`
        BroadcasterGender  string   `json:"broadcaster_gender"`
}

func parseStreamAPIBody(body string) (hlsURL, roomStatus string, apiBody *StreamAPIBody, err error) {
        var resp StreamAPIBody
        if err := json.Unmarshal([]byte(body), &resp); err != nil {
                return "", "", nil, fmt.Errorf("parse stream API response: %w", err)
        }
        // The get_edge_hls_url_ajax endpoint returns the stream URL in "url" when "hls_source" is empty.
        hlsURL = resp.HLSSource
        if hlsURL == "" {
                hlsURL = resp.URL
        }
        return hlsURL, resp.RoomStatus, &resp, nil
}

func fetchHLSSourceViaGET(ctx context.Context, username string) (hlsURL, roomStatus string, apiBody *StreamAPIBody, err error) {
        apiURL := fmt.Sprintf("%sapi/chatvideocontext/%s/", server.Config.Domain, username)
        body, err := NewReq().Get(ctx, apiURL)
        if err != nil {
                return "", "", nil, fmt.Errorf("get chatvideocontext: %w", err)
        }
        var resp StreamAPIBody
        if err := json.Unmarshal([]byte(body), &resp); err != nil {
                return "", "", nil, fmt.Errorf("parse chatvideocontext: %w", err)
        }
        return resp.HLSSource, resp.RoomStatus, &resp, nil
}
