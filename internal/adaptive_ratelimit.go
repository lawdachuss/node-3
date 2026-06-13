package internal

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveRateLimiter adjusts request rate based on success/error feedback.
// It is used only for Chaturbate API discovery calls; HLS playlist and segment
// downloads stay concurrent once a channel has a stream URL.
type AdaptiveRateLimiter struct {
	tokens   chan struct{}
	maxBurst int64

	currentRate atomic.Int64 // tokens per second (fixed-point *1000)
	minRate     int64
	maxRate     int64

	consecutiveErrors atomic.Int64
	peakRate          atomic.Int64

	mu     sync.Mutex
	stopCh chan struct{}
}

// NewAdaptiveRateLimiter creates an adaptive token-bucket limiter.
func NewAdaptiveRateLimiter(initialRate, minRate, maxRate int, burst int) *AdaptiveRateLimiter {
	if initialRate < 1 {
		initialRate = 1
	}
	if minRate < 1 {
		minRate = 1
	}
	if maxRate < minRate {
		maxRate = minRate
	}
	if initialRate > maxRate {
		initialRate = maxRate
	}
	if burst < 1 {
		burst = 1
	}

	rl := &AdaptiveRateLimiter{
		tokens:   make(chan struct{}, burst),
		minRate:  int64(minRate * 1000),
		maxRate:  int64(maxRate * 1000),
		maxBurst: int64(burst),
		stopCh:   make(chan struct{}),
	}
	rl.currentRate.Store(int64(initialRate * 1000))
	rl.peakRate.Store(int64(initialRate * 1000))

	for i := 0; i < burst; i++ {
		select {
		case rl.tokens <- struct{}{}:
		default:
		}
	}

	go rl.refillLoop()
	return rl
}

// Acquire blocks until a token is available or the context is canceled.
func (rl *AdaptiveRateLimiter) Acquire(done <-chan struct{}) bool {
	select {
	case <-done:
		return false
	case <-rl.tokens:
		return true
	}
}

// TryAcquire returns false immediately when no token is available.
func (rl *AdaptiveRateLimiter) TryAcquire() bool {
	select {
	case <-rl.tokens:
		return true
	default:
		return false
	}
}

// Success gradually increases throughput up to maxRate.
func (rl *AdaptiveRateLimiter) Success() {
	rl.consecutiveErrors.Store(0)

	rate := rl.currentRate.Load()
	if rate >= rl.maxRate {
		return
	}

	rl.currentRate.Add(1000) // +1 req/s
	if newRate := rl.currentRate.Load(); newRate > rl.maxRate {
		rl.currentRate.Store(rl.maxRate)
		if rl.maxRate > rl.peakRate.Load() {
			rl.peakRate.Store(rl.maxRate)
		}
	} else if newRate > rl.peakRate.Load() {
		rl.peakRate.Store(newRate)
	}
}

// Failure backs off aggressively when upstream starts rejecting/failing calls.
func (rl *AdaptiveRateLimiter) Failure() {
	rl.consecutiveErrors.Add(1)

	rate := rl.currentRate.Load()
	newRate := rate / 2
	if newRate < rl.minRate {
		newRate = rl.minRate
	}
	rl.currentRate.Store(newRate)
}

// PeakRate returns the highest rate reached this session.
func (rl *AdaptiveRateLimiter) PeakRate() int {
	return int(rl.peakRate.Load() / 1000)
}

// CurrentRate returns the current rate in req/s.
func (rl *AdaptiveRateLimiter) CurrentRate() int {
	return int(rl.currentRate.Load() / 1000)
}

func (rl *AdaptiveRateLimiter) refillLoop() {
	for {
		rate := rl.currentRate.Load()
		if rate < rl.minRate {
			rate = rl.minRate
		}

		interval := time.Duration(1e12 / rate) // nanos per token (rate is in millitokens/sec)
		if interval < time.Millisecond {
			interval = time.Millisecond
		}

		select {
		case <-rl.stopCh:
			return
		case <-time.After(interval):
			select {
			case rl.tokens <- struct{}{}:
			default:
			}
		}
	}
}

func (rl *AdaptiveRateLimiter) Stop() {
	select {
	case <-rl.stopCh:
	default:
		close(rl.stopCh)
	}
}

func envInt(name string, fallback int) int {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return fallback
	}
	return parsed
}

// chaturbateRateLimiter is the global adaptive limiter for Chaturbate API calls.
// Defaults are tuned for fast startup of large channel lists. Adjust per
// deployment/IP/proxy reputation with:
// CHATURBATE_API_INITIAL_RPS, CHATURBATE_API_MIN_RPS,
// CHATURBATE_API_MAX_RPS, CHATURBATE_API_BURST.
var chaturbateRateLimiter = sync.OnceValue(func() *AdaptiveRateLimiter {
	return NewAdaptiveRateLimiter(
		envInt("CHATURBATE_API_INITIAL_RPS", 25),
		envInt("CHATURBATE_API_MIN_RPS", 5),
		envInt("CHATURBATE_API_MAX_RPS", 100),
		envInt("CHATURBATE_API_BURST", 250),
	)
})
