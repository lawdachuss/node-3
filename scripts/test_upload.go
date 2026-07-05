//go:build ignore

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/uploader"
)

func loadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(parts[0])
		v := strings.TrimSpace(parts[1])
		v = strings.Trim(v, "'\"")
		if os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
	return s.Err()
}

type testLogger struct{}

func (testLogger) Info(format string, a ...any)  { log.Printf("INFO: "+format, a...) }
func (testLogger) Error(format string, a ...any) { log.Printf("ERROR: "+format, a...) }

func main() {
	if err := loadDotEnv(".env"); err != nil {
		log.Fatalf("loading .env: %v", err)
	}

	videoPath := "videos/completed/357054_medium.mp4"
	if _, err := os.Stat(videoPath); os.IsNotExist(err) {
		log.Fatalf("video not found: %s", videoPath)
	}

	seekStreamingKey := os.Getenv("SEEKSTREAMING_KEY")
	vidHideAPIKey := os.Getenv("VIDHIDE_API_KEY")
	streamWishAPIKey := os.Getenv("STREAMWISH_API_KEY")

	log.Printf("SeekStreaming key: %s", mask(seekStreamingKey))
	log.Printf("VidHide key: %s", mask(vidHideAPIKey))
	log.Printf("StreamWish key: %s", mask(streamWishAPIKey))

	// Test SeekStreaming
	if seekStreamingKey != "" {
		log.Println("=== Testing SeekStreaming ===")
		ss := uploader.NewSeekStreamingUploader(seekStreamingKey)
		start := time.Now()
		url, err := ss.Upload(videoPath)
		if err != nil {
			log.Printf("SeekStreaming FAILED: %v", err)
		} else {
			log.Printf("SeekStreaming SUCCESS (%v): %s", time.Since(start).Round(time.Second), url)
		}
	} else {
		log.Println("SeekStreaming: no key, skipping")
	}

	// Test VidHide
	if vidHideAPIKey != "" {
		log.Println("=== Testing VidHide ===")
		vh := uploader.NewVidHideUploader(vidHideAPIKey)
		start := time.Now()
		url, err := vh.Upload(videoPath)
		if err != nil {
			log.Printf("VidHide FAILED: %v", err)
		} else {
			log.Printf("VidHide SUCCESS (%v): %s", time.Since(start).Round(time.Second), url)
		}
	} else {
		log.Println("VidHide: no key, skipping")
	}

	// Test StreamWish
	if streamWishAPIKey != "" {
		log.Println("=== Testing StreamWish ===")
		sw := uploader.NewStreamWishUploader(streamWishAPIKey)
		start := time.Now()
		url, err := sw.Upload(videoPath)
		if err != nil {
			log.Printf("StreamWish FAILED: %v", err)
		} else {
			log.Printf("StreamWish SUCCESS (%v): %s", time.Since(start).Round(time.Second), url)
		}
	} else {
		log.Println("StreamWish: no key, skipping")
	}

	fmt.Println("\nDone.")
}

func mask(s string) string {
	if s == "" {
		return "<empty>"
	}
	if len(s) < 8 {
		return "<too-short>"
	}
	return s[:4] + "..." + s[len(s)-4:]
}
