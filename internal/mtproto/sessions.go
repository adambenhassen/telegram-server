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

// Add registers c as a live connection for userID.
func (r *SessionRegistry) Add(userID int64, c *Conn) {
	r.mu.Lock()
	r.m[userID] = append(r.m[userID], c)
	r.mu.Unlock()
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
