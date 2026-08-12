package mtproto

import "sync"

// SessionRegistry tracks the live connections for each authenticated user so the
// update-delivery path can push server-initiated messages to a user's sockets. A
// user may have several connections (multiple devices/sessions) at once.
type SessionRegistry struct {
	mu sync.Mutex
	m  map[int64][]*Conn
}

// NewSessionRegistry creates an empty registry.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{m: map[int64][]*Conn{}}
}

// MaxUserConns caps the live connections one user may hold in this process.
// Delivery walks every one of them per notification, so an account holding
// sockets without bound multiplies the cost of each of its own updates. A real
// client holds one socket per session and a handful of sessions, so the cap is
// far above legitimate use and only bites a client opening sockets in a loop.
const MaxUserConns = 20

// Add registers c as a live connection for userID, reporting whether it fit
// under the per-user cap. At the cap the new connection is refused and the live
// ones are left alone: dropping the oldest would take a working session away
// from a user for opening one more, which is the worse failure of the two.
func (r *SessionRegistry) Add(userID int64, c *Conn) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.m[userID]) >= MaxUserConns {
		return false
	}
	r.m[userID] = append(r.m[userID], c)
	return true
}

// Remove deregisters c from userID; the last connection prunes the user entry.
func (r *SessionRegistry) Remove(userID int64, c *Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	conns := r.m[userID]
	for i, x := range conns {
		if x == c {
			r.m[userID] = append(conns[:i], conns[i+1:]...)
			break
		}
	}
	if len(r.m[userID]) == 0 {
		delete(r.m, userID)
	}
}

// Conns returns a snapshot copy of userID's live connections, so callers can
// iterate and write without holding the registry lock.
func (r *SessionRegistry) Conns(userID int64) []*Conn {
	r.mu.Lock()
	defer r.mu.Unlock()
	conns := r.m[userID]
	out := make([]*Conn, len(conns))
	copy(out, conns)
	return out
}

// TotalConns returns the total number of live connections across all users.
func (r *SessionRegistry) TotalConns() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var n int
	for _, conns := range r.m {
		n += len(conns)
	}
	return n
}

// TotalSessions returns the number of distinct users with at least one live
// connection — i.e. authenticated sessions currently attached.
func (r *SessionRegistry) TotalSessions() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.m)
}
