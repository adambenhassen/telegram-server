package mtproto

import "sync"

// DefaultMaxConnsPerUnboundKey is the shipped bound on what one auth key with
// no signed-in user may hold: 8 concurrent connections.
//
// It is the analogue, for keys nobody has signed in on, of maxUserConns, and it
// closes the gap between that cap and the pre-auth bounds. Those bounds end at
// the first frame that decrypts under a key this server issued, and maxUserConns
// counts only signed-in sessions, so a key that completed one exchange and never
// signed in was counted by neither: one exchange, ever, bought a peer unlimited
// concurrent connections from any number of addresses.
//
// The number is small because the population it bounds is small. A client
// between key exchange and sign-in is waiting on a human reading a code and
// holds one connection under its key; a few more only while a dropped socket
// has not yet been noticed. Eight leaves that room and still makes the hold a
// formula: a peer wanting more concurrent connections needs another key, and
// another key needs another exchange, which the pre-auth bounds already price.
//
// It is a policy constant, not adaptive logic: nothing here reacts to load or
// keeps a reputation. Zero disables it, matching the pre-auth bounds and the
// rate-limit surfaces, and restores the unbounded hold this replaced.
const DefaultMaxConnsPerUnboundKey = 8

// unboundKeyLimiter counts the live connections charged to each auth key that
// has no user signed in on it.
//
// Lock ordering: mu is a leaf, exactly as preAuthLimiter.mu is. It is taken for
// a counter update and released before anything else happens — no callback runs
// under it, no socket is closed under it, and no other lock in this package
// (the session registry's, a conn's write mutex, the pre-auth limiter's) is
// ever acquired while it is held. Nothing can order against it, so nothing can
// deadlock with it.
type unboundKeyLimiter struct {
	max int

	mu    sync.Mutex
	conns map[[8]byte]int
}

func newUnboundKeyLimiter(maxConns int) *unboundKeyLimiter {
	return &unboundKeyLimiter{max: maxConns, conns: map[[8]byte]int{}}
}

// acquire takes a slot for id, reporting whether one was free. A zero max is
// the bound turned off, and everything is admitted.
func (l *unboundKeyLimiter) acquire(id [8]byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.max > 0 && l.conns[id] >= l.max {
		return false
	}
	l.conns[id]++
	return true
}

// release gives a slot back. The map holds an entry only while a key has live
// connections charged to it, so it is bounded by what is connected now rather
// than by how many keys this server has ever issued — which is the number the
// persisted keys grow by and nothing here should.
func (l *unboundKeyLimiter) release(id [8]byte) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n := l.conns[id] - 1; n > 0 {
		l.conns[id] = n
	} else {
		delete(l.conns, id)
	}
}

// unboundKeyHold is one connection's place in that bound: the key it is charged
// to while that key has nobody signed in on it, and the record that it has left
// the bound for good.
//
// One goroutine owns a hold — the one serving its connection, which is also the
// only one that decrypts its frames — so the fields need no lock of their own;
// the counters they move are the limiter's, and those are guarded.
type unboundKeyHold struct {
	lim *unboundKeyLimiter
	// key is the auth key this connection is charged to, meaningful only while
	// charged.
	key     [8]byte
	charged bool
	// signedIn records that this connection's key has resolved to a user. It is
	// never unset: see charge.
	signedIn bool
}

// charge places the connection under the bound for the key its latest frame
// proved, reporting whether that key had room for it.
//
// It must be called only once a frame has decrypted, and never on the key id
// alone: that id travels in cleartext on every frame, so charging on it would
// let anyone who can read one fill a stranger's budget and lock its owner out —
// a bound on anonymous holds turned into a way of closing somebody else's
// sessions.
//
// userID is the user the key resolves to, 0 when it has none, and a key that
// has one takes the connection out from under this bound for good. What a
// signed-in client may hold is maxUserConns' to decide, and this bound must
// never be able to refuse or close a session that has already signed in. That
// is the same one-way transition the pre-auth bounds make, for the same reason,
// and it holds through everything that comes after: a key unbound again by the
// 2FA path, and a connection that renegotiates a new one.
func (h *unboundKeyHold) charge(id [8]byte, userID int64) bool {
	if h.signedIn {
		return true
	}
	if userID != 0 {
		h.signedIn = true
		h.release()
		return true
	}
	if h.charged && h.key == id {
		return true
	}
	// A renegotiated connection is charged to the key it now proves rather than
	// to the one it used to hold.
	h.release()
	if !h.lim.acquire(id) {
		return false
	}
	h.key, h.charged = id, true
	return true
}

// release gives back the slot this connection holds, if it holds one. It is
// idempotent, because it is called both by the frame that ends the charge and
// by the cleanup that runs whatever ends the connection.
func (h *unboundKeyHold) release() {
	if !h.charged {
		return
	}
	h.charged = false
	h.lim.release(h.key)
}
