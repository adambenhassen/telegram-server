package mtproto

import (
	"sync/atomic"

	"github.com/adambenhassen/telegram-server/internal/srp"
)

const (
	// DefaultMaxPendingLoginConns caps the process-wide connections waiting for
	// the second factor of a login. It is deliberately finite because each held
	// connection keeps a goroutine and an auth-key hold for the full lease.
	DefaultMaxPendingLoginConns = 1024
	// DefaultPendingLoginLifetime is the connection lease after a password is
	// requested. It is twice the SRP challenge TTL so a client has time to fetch
	// the challenge and submit its proof while the challenge itself still keeps
	// its independent five-minute validity window.
	DefaultPendingLoginLifetime = 2 * srp.DefaultTTL
)

// pendingLoginLimiter counts pending-login connections process-wide. The
// compare-and-swap loop makes the cap a single atomic admission decision without
// adding a lock to the connection path.
type pendingLoginLimiter struct {
	max   int64
	count atomic.Int64
}

func newPendingLoginLimiter(maxConns int) *pendingLoginLimiter {
	return &pendingLoginLimiter{max: int64(maxConns)}
}

// acquire claims one pending-login slot. Zero disables the cap.
func (l *pendingLoginLimiter) acquire() bool {
	if l.max <= 0 {
		return true
	}
	for {
		count := l.count.Load()
		if count >= l.max {
			return false
		}
		if l.count.CompareAndSwap(count, count+1) {
			return true
		}
	}
}

// release returns one pending-login slot to the process-wide pool.
func (l *pendingLoginLimiter) release() {
	if l.max > 0 {
		l.count.Add(-1)
	}
}

// pendingLoginHold is one connection's place in the pending-login cap. It is
// owned by the serving goroutine, so only the limiter's count needs atomic
// protection. The hold remains charged until the connection exits, including
// after a successful password check.
type pendingLoginHold struct {
	lim     *pendingLoginLimiter
	charged bool
}

// acquire claims the hold once. A second observation of the marker cannot
// consume another process-wide slot.
func (h *pendingLoginHold) acquire() bool {
	if h.charged {
		return true
	}
	if !h.lim.acquire() {
		return false
	}
	h.charged = true
	return true
}

// release returns the hold's slot. It is safe to call more than once.
func (h *pendingLoginHold) release() {
	if !h.charged {
		return
	}
	h.charged = false
	h.lim.release()
}
