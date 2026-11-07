package config

import (
	"context"
	"testing"
	"time"
)

// TestAcquireFFmpegTimeoutOnFullPool verifies the bounded acquire fails fast
// (with an error) instead of blocking forever when every lightweight slot is
// taken — the exact wedge that previously froze pipelines at
// "uploaded X/6 hosts — processing".
func TestAcquireFFmpegTimeoutOnFullPool(t *testing.T) {
	if cap(ffmpegSem) == 0 {
		t.Fatal("ffmpegSem not initialized")
	}
	for i := 0; i < cap(ffmpegSem); i++ {
		ffmpegSem <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(ffmpegSem); i++ {
			<-ffmpegSem
		}
	}()

	start := time.Now()
	err := AcquireFFmpegFor(50 * time.Millisecond)
	if err == nil {
		t.Fatal("AcquireFFmpegFor on a full pool should return an error")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("AcquireFFmpegFor blocked too long on full pool: %v", elapsed)
	}
}

// TestAcquireFFmpegHeavyTimeoutOnFullPool verifies the same bounded behavior
// for the CPU-bound compression pool.
func TestAcquireFFmpegHeavyTimeoutOnFullPool(t *testing.T) {
	if cap(ffmpegHeavySem) == 0 {
		t.Fatal("ffmpegHeavySem not initialized")
	}
	for i := 0; i < cap(ffmpegHeavySem); i++ {
		ffmpegHeavySem <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(ffmpegHeavySem); i++ {
			<-ffmpegHeavySem
		}
	}()

	if err := AcquireFFmpegHeavyFor(50 * time.Millisecond); err == nil {
		t.Fatal("AcquireFFmpegHeavyFor on a full pool should return an error")
	}
}

// TestAcquireFFmpegReturnsWhenSlotFreed verifies a bounded acquire still
// succeeds when a slot frees up within the wait window.
func TestAcquireFFmpegReturnsWhenSlotFreed(t *testing.T) {
	if cap(ffmpegSem) == 0 {
		t.Fatal("ffmpegSem not initialized")
	}
	// Fill the entire pool, then free one slot after a short delay so the
	// waiter must block (not just grab an immediate slot) before acquiring.
	for i := 0; i < cap(ffmpegSem); i++ {
		ffmpegSem <- struct{}{}
	}
	defer drainFFmpegSem()
	go func() {
		time.Sleep(30 * time.Millisecond)
		<-ffmpegSem
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := AcquireFFmpeg(ctx); err != nil {
		t.Fatalf("AcquireFFmpeg should succeed once a slot frees: %v", err)
	}
	ReleaseFFmpeg()
	drainFFmpegSem()
}

// drainFFmpegSem returns the lightweight slot pool to empty. It is safe to
// call twice: after the first drain the pool is empty and the second pass is a
// no-op.
func drainFFmpegSem() {
	for {
		select {
		case <-ffmpegSem:
		default:
			return
		}
	}
}

// TestAcquireFFmpegHonorsContextCancel verifies the context-bounded acquire
// wakes up promptly when its context is cancelled.
func TestAcquireFFmpegHonorsContextCancel(t *testing.T) {
	if cap(ffmpegSem) == 0 {
		t.Fatal("ffmpegSem not initialized")
	}
	for i := 0; i < cap(ffmpegSem); i++ {
		ffmpegSem <- struct{}{}
	}
	defer drainFFmpegSem()

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	err := AcquireFFmpeg(ctx)
	if err != context.Canceled {
		t.Fatalf("AcquireFFmpeg should return context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("AcquireFFmpeg ignored context cancel (took %v)", elapsed)
	}
}
