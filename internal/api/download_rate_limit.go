package api

import (
	"sync"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// downloadRateLimiter is the process-local fixed-window counter for
// upload.getFile. It stores one timestamp and one count, regardless of how
// many accounts use the replica.
//
// Lock ordering: mu is a leaf. It is held only while updating the fixed-window
// state and is never held across a store call or a blob read.
type downloadRateLimiter struct {
	cfg store.RateLimitConfig

	mu          sync.Mutex
	windowStart time.Time
	count       int
}

func newDownloadRateLimiter(cfg store.RateLimitConfig) *downloadRateLimiter {
	return &downloadRateLimiter{cfg: cfg}
}

// allow admits one call and returns zero wait, or returns the remaining window
// duration when the call is denied. A disabled config admits every call without
// touching the counter.
func (l *downloadRateLimiter) allow(now time.Time) (time.Duration, bool) {
	if !l.cfg.Enabled() {
		return 0, true
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.windowStart.IsZero() || now.Sub(l.windowStart) >= l.cfg.Window {
		l.windowStart = now
		l.count = 0
	}
	if l.count >= l.cfg.Limit {
		return l.windowStart.Add(l.cfg.Window).Sub(now), false
	}
	l.count++
	return 0, true
}
