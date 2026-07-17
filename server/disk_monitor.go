package server

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
)

const (
	diskMonitorInterval = 5 * time.Minute // how often to check disk usage
	diskHysteresis      = 5               // stop auto-deleting when disk drops below critical - hysteresis
	diskUsageRetention  = 7 * 24 * time.Hour
	diskUsageTrimEvery  = 24 * time.Hour // trim the disk_usage table at most once per day
)

var lastDiskUsageTrim time.Time

// StartDiskMonitor begins periodic disk space monitoring in a background
// goroutine.  It checks disk usage every diskMonitorInterval and:
//   - Logs a warning when usage exceeds DiskWarningPercent
//   - Auto-deletes oldest uploaded local recordings when usage exceeds DiskCriticalPercent
//   - Saves disk usage snapshots to Supabase
//
// Call this once during startup.
func StartDiskMonitor(stop <-chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[DISK] PANIC in disk monitor: %v", r)
		}
	}()
	ticker := time.NewTicker(diskMonitorInterval)
	defer ticker.Stop()

	// Run an initial check immediately
	checkDisk()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			checkDisk()
		}
	}
}

func checkDisk() {
	cfg := Config
	if cfg == nil {
		return
	}

	info := GetDiskInfo()
	if info == nil || info.Percent == 0 {
		return
	}

	// Save disk usage to Supabase (non-fatal on error)
	saveDiskUsageToDB(info)

	// Trim the append-only disk_usage table so it doesn't grow unbounded.
	// Only one node needs to do this; throttle to once per day.
	if time.Since(lastDiskUsageTrim) > diskUsageTrimEvery {
		lastDiskUsageTrim = time.Now()
		if client := GetDBClient(); client != nil {
			if err := client.TrimDiskUsageOlderThan(diskUsageRetention); err != nil {
				log.Printf("[DISK] failed to trim disk_usage: %v", err)
			}
		}
	}

	// Check age-based retention: delete local files older than MaxLocalAgeDays
	if cfg.MaxLocalAgeDays > 0 {
		deleted, err := deleteOldLocalFiles(cfg.MaxLocalAgeDays)
		if err != nil {
			log.Printf("[DISK] age-based cleanup error: %v", err)
		} else if deleted > 0 {
			log.Printf("[DISK] age-based cleanup: deleted %d local file(s) older than %d days", deleted, cfg.MaxLocalAgeDays)
		}
	}

	// Check warning threshold
	if cfg.DiskWarningPercent > 0 && info.Percent >= cfg.DiskWarningPercent {
		log.Printf("[DISK] WARNING: %d%% used (threshold: %d%%)", info.Percent, cfg.DiskWarningPercent)
	}

	// Check critical threshold — auto-delete oldest uploaded recordings
	if cfg.DiskCriticalPercent > 0 && info.Percent >= cfg.DiskCriticalPercent {
		target := cfg.DiskCriticalPercent - diskHysteresis
		if target < 1 {
			target = 1
		}
		deleted, err := freeDiskSpace(target, info)
		if err != nil {
			log.Printf("[DISK] auto-cleanup error: %v", err)
		} else if deleted > 0 {
			log.Printf("[DISK] auto-cleanup: deleted %d local file(s), disk dropped from %d%% to %d%%",
				deleted, info.Percent, target)
		}
	}
}

func saveDiskUsageToDB(info *entity.DiskInfo) {
	client := GetDBClient()
	if client == nil {
		return
	}
	_ = client.SaveDiskUsage(&database.DiskUsage{
		TotalBytes:  int64(info.TotalGB * 1024 * 1024 * 1024),
		UsedBytes:   int64(info.UsedGB * 1024 * 1024 * 1024),
		FreeBytes:   int64((info.TotalGB - info.UsedGB) * 1024 * 1024 * 1024),
		PercentUsed: info.Percent,
		RecordedAt:  time.Now().UTC().Format("2006-01-02T15:04:05Z"),
	})
}

// freeDiskSpace deletes the oldest local video files that have been verified
// as fully uploaded (have a corresponding Supabase recording entry).
// Returns the number of files deleted.
func freeDiskSpace(targetPercent int, diskInfo *entity.DiskInfo) (int, error) {
	dirs := []string{"videos"}
	if Config.OutputDir != "" {
		dirs = append(dirs, Config.OutputDir)
	}

	// Collect all local video files with their mtime
	type videoFile struct {
		path    string
		modTime time.Time
		size    int64
	}
	var candidates []videoFile

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			ext := strings.ToLower(filepath.Ext(name))
			if ext != ".mp4" && ext != ".mkv" {
				continue
			}
			if strings.HasSuffix(name, ".video.mp4") || strings.HasSuffix(name, ".audio.mp4") || strings.Contains(name, ".muxed.") {
				continue
			}

			info, err := e.Info()
			if err != nil {
				continue
			}
			path := filepath.Join(dir, name)

			// Skip files that are currently being uploaded (in-flight).
			if Manager != nil && Manager.IsFileUploadInFlight(path) {
				continue
			}

			// Only consider files that have a recording entry in Supabase
			// (meaning they were successfully uploaded at some point).
			if !isRecordingUploaded(name) {
				continue
			}

			candidates = append(candidates, videoFile{
				path:    path,
				modTime: info.ModTime(),
				size:    info.Size(),
			})
		}
	}

	if len(candidates) == 0 {
		return 0, nil
	}

	// Sort oldest first
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].modTime.Before(candidates[j].modTime)
	})

	// Calculate how much we need to free
	if diskInfo.TotalGB == 0 {
		return 0, fmt.Errorf("could not determine total disk size")
	}

	usedGB := diskInfo.UsedGB
	targetUsedGB := diskInfo.TotalGB * float64(targetPercent) / 100
	neededGB := usedGB - targetUsedGB

	if neededGB <= 0 {
		return 0, nil
	}

	neededBytes := int64(neededGB * 1024 * 1024 * 1024)
	freed := int64(0)
	deleted := 0

	for _, vf := range candidates {
		if freed >= neededBytes {
			break
		}

		// Double-check the file still exists and is still uploaded
		if _, err := os.Stat(vf.path); os.IsNotExist(err) {
			continue
		}
		if !isRecordingUploaded(filepath.Base(vf.path)) {
			continue
		}

		if err := os.Remove(vf.path); err != nil {
			log.Printf("[DISK] failed to delete %s: %v", vf.path, err)
			continue
		}
		// Clean up associated preview sidecar files
		for _, s := range []string{".thumb.webp", ".thumb.jpg", ".sprite.webp", ".sprite.jpg", ".preview.webp", ".preview.mp4", ".thumb", ".sprite"} {
			os.Remove(vf.path + s)
		}
		freed += vf.size
		deleted++
		log.Printf("[DISK] deleted %s (age: %s, size: %.1f MB)",
			filepath.Base(vf.path), time.Since(vf.modTime).Round(time.Hour), float64(vf.size)/(1024*1024))
	}

	return deleted, nil
}

// isRecordingUploaded returns true if the file has been recorded in Supabase.
func isRecordingUploaded(filename string) bool {
	client := GetDBClient()
	if client == nil {
		return false
	}
	rec, err := client.GetRecording(filename)
	return err == nil && rec != nil && rec.Filename == filename
}

// deleteOldLocalFiles removes local video files that are older than maxAgeDays
// and have a corresponding Supabase recording entry. Returns the count deleted.
func deleteOldLocalFiles(maxAgeDays int) (int, error) {
	cutoff := time.Now().AddDate(0, 0, -maxAgeDays)
	dirs := []string{"videos"}
	if Config.OutputDir != "" {
		dirs = append(dirs, Config.OutputDir)
	}

	deleted := 0
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			ext := strings.ToLower(filepath.Ext(name))
			if ext != ".mp4" && ext != ".mkv" {
				continue
			}
			if strings.HasSuffix(name, ".video.mp4") || strings.HasSuffix(name, ".audio.mp4") || strings.Contains(name, ".muxed.") {
				continue
			}

			info, err := e.Info()
			if err != nil {
				continue
			}

			// Only delete files older than the cutoff
			if info.ModTime().After(cutoff) {
				continue
			}

			path := filepath.Join(dir, name)

			// Only delete if successfully uploaded
			if !isRecordingUploaded(name) {
				continue
			}

			if err := os.Remove(path); err != nil {
				log.Printf("[DISK] age-cleanup: failed to delete %s: %v", path, err)
				continue
			}
			// Clean up associated preview sidecar files
			for _, s := range []string{".thumb.webp", ".thumb.jpg", ".sprite.webp", ".sprite.jpg", ".preview.webp", ".preview.mp4", ".thumb", ".sprite"} {
				os.Remove(path + s)
			}
			deleted++
			log.Printf("[DISK] age-cleanup: deleted %s (age: %s, size: %.1f MB)",
				name, time.Since(info.ModTime()).Round(time.Hour), float64(info.Size())/(1024*1024))
		}
	}
	return deleted, nil
}
