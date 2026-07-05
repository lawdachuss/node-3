package uploader

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

const (
	streamWishAPIBase = "https://api.streamwish.com/api"
)

type StreamWishUploader struct {
	apiKey string
	client *http.Client
}

func NewStreamWishUploader(apiKey string) *StreamWishUploader {
	return &StreamWishUploader{
		apiKey: apiKey,
		client: &http.Client{
			Timeout: 120 * time.Minute,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  true,
				DialContext:         (&net.Dialer{Timeout: 30 * time.Second}).DialContext,
			},
		},
	}
}

type streamWishServerResponse struct {
	ServerTime string `json:"server_time"`
	Msg        string `json:"msg"`
	Status     int    `json:"status"`
	Result     string `json:"result"`
}

type streamWishUploadFileEntry struct {
	FileCode string `json:"filecode"`
	Filename string `json:"filename"`
	Status   string `json:"status"`
}

type streamWishUploadResponse struct {
	Msg    string                     `json:"msg"`
	Status int                        `json:"status"`
	Files  []streamWishUploadFileEntry `json:"files"`
}

func (u *StreamWishUploader) Upload(filePath string) (string, error) {
	return u.UploadWithProgress(filePath, nil)
}

func (u *StreamWishUploader) UploadWithProgress(filePath string, progress ProgressFunc) (string, error) {
	if u.apiKey == "" {
		return "", fmt.Errorf("StreamWish API key not configured")
	}

	var lastErr error

	maxAttempts := 3
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if attempt > 1 {
			time.Sleep(uploadBackoff(attempt-2, lastErr))
		}

		downloadLink, err := u.uploadFile(filePath, progress)
		if err != nil {
			lastErr = fmt.Errorf("upload file: %w", err)
			if isUploadRateLimited(err) {
				time.Sleep(uploadBackoff(attempt, err))
				lastErr = nil
				continue
			}
			if attempt < maxAttempts {
				continue
			}
			return "", lastErr
		}

		return downloadLink, nil
	}

	return "", lastErr
}

func (u *StreamWishUploader) getUploadServer() (string, error) {
	url := fmt.Sprintf("%s/upload/server?key=%s", streamWishAPIBase, u.apiKey)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request upload server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("get upload server failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var serverResp streamWishServerResponse
	if err := json.NewDecoder(resp.Body).Decode(&serverResp); err != nil {
		return "", fmt.Errorf("decode server response: %w", err)
	}

	if serverResp.Status != 200 {
		return "", fmt.Errorf("server status not ok: %s (msg: %s)", serverResp.Msg, serverResp.Msg)
	}

	if serverResp.Result == "" {
		return "", fmt.Errorf("no upload server URL in response")
	}

	return serverResp.Result, nil
}

func (u *StreamWishUploader) uploadFile(filePath string, progress ProgressFunc) (string, error) {
	uploadServer, err := u.getUploadServer()
	if err != nil {
		return "", fmt.Errorf("get upload server: %w", err)
	}

	body, contentLen, contentType, file, err := multipartStreamWithProgress(
		map[string]string{"key": u.apiKey},
		"file", filePath, "StreamWish", progress,
	)
	if err != nil {
		return "", fmt.Errorf("multipart stream: %w", err)
	}
	defer file.Close()

	req, err := http.NewRequest("POST", uploadServer, body)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", defaultUserAgent)
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = contentLen

	resp, err := u.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var uploadResp streamWishUploadResponse
	if err := json.NewDecoder(resp.Body).Decode(&uploadResp); err != nil {
		return "", fmt.Errorf("decode upload response: %w", err)
	}

	if uploadResp.Status != 200 {
		return "", fmt.Errorf("upload failed: status %d — %s", uploadResp.Status, uploadResp.Msg)
	}

	if len(uploadResp.Files) == 0 {
		return "", fmt.Errorf("no files in upload response")
	}

	fileCode := uploadResp.Files[0].FileCode
	if fileCode == "" {
		return "", fmt.Errorf("no file code in response")
	}

	viewURL := fmt.Sprintf("https://hanerix.com/%s", fileCode)
	return viewURL, nil
}
