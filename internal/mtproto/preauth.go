package mtproto

import (
	"net/netip"
	"sync"
	"time"
)

// preAuthLogInterval bounds how often a pre-auth refusal is logged. Refusals are
// driven by whoever can reach the port, unauthenticated, so a line each is a log
// an attacker writes as fast as it can open sockets; the line that does come out
// says how many it stands for.
const preAuthLogInterval = 10 * time.Second

// PreAuthLimits bounds what connections that have not authenticated yet may hold
// at once. Everything before the first frame that decrypts under a key this
// server issued is reachable by anyone who can open a socket, and each such
// connection costs a descriptor and a goroutine.
//
// The three bounds are layered so that each one holds if the others are
// misconfigured, and together they make the worst case a formula rather than a
// hope: an unauthenticated population spread over k client addresses holds at
// most min(MaxConns, k*MaxConnsPerAddr) descriptors and the same number of
// goroutines, each for at most Lifetime. One address alone holds at most
// MaxConnsPerAddr of each.
//
// They are policy constants, not adaptive logic: nothing here reacts to load,
// keeps a reputation, or rate-limits opens. A connection is inside the bounds or
// it is closed.
//
// A zero field disables that bound, matching the rate-limit surfaces. Disabling
// all three restores the behaviour these bounds replaced, where one peer's hold
// on the server was limited only by its own patience.
type PreAuthLimits struct {
	// MaxConns caps concurrent pre-auth connections in this process. It is
	// checked on the accept loop, before the connection costs a goroutine, a
	// deadline or a read, so that refusing stays cheaper than accepting.
	MaxConns int
	// MaxConnsPerAddr caps them per client address, so that one peer cannot
	// spend the global cap on its own and lock everybody else out. It is checked
	// once the connection layer has established the client address — the one a
	// PROXY v2 header reports when the server runs behind a balancer, never the
	// socket peer, which behind a balancer is the same for every client on
	// earth.
	MaxConnsPerAddr int
	// Lifetime is the hard ceiling on the pre-auth state, measured from accept.
	// A connection that has not authenticated within it is closed whatever it is
	// doing, which is what ends the hold no deadline catches: every read the
	// server does re-derives its own deadline, so a peer sending one small frame
	// just inside each of them keeps its socket indefinitely for a few bytes a
	// minute.
	//
	// It is not a handshake timeout. Transport negotiation has its own, tighter
	// bound; this one covers everything up to and including key exchange, and
	// gotd applies a 60s DefaultTimeout per read inside that, so a ceiling below
	// a minute would start cutting handshakes that are merely slow.
	Lifetime time.Duration
}

// DefaultPreAuthLimits returns the shipped bounds: 1024 concurrent pre-auth
// connections in the process, 64 from any one client address, and 120s before an
// unauthenticated connection is closed regardless of what it is sending.
//
// The per-address number is the one that has to be argued rather than chosen. It
// is a concurrency cap, not a rate: a client completes a handshake in
// milliseconds, so 64 in flight at once is hundreds of new sessions a second
// from a single address — far above a real client and, importantly, above a
// carrier NAT or a corporate egress, where thousands of subscribers share the
// address the server sees. The global number sits well under any descriptor
// ceiling worth running with, so the process keeps descriptors for the
// authenticated sessions that pay for it.
//
// Exported so a test can drive the bounds that actually ship rather than ones it
// invented.
func DefaultPreAuthLimits() PreAuthLimits {
	return PreAuthLimits{
		MaxConns:        1024,
		MaxConnsPerAddr: 64,
		Lifetime:        120 * time.Second,
	}
}

// preAuthLimiter counts the connections that have not authenticated yet, in
// total and per client address.
//
// Lock ordering: mu is a leaf. It is taken for a counter update and released
// before anything else happens — no callback runs under it, no socket is closed
// under it, and no other lock in this package (the session registry's, a conn's
// write mutex) is ever acquired while it is held. Nothing can order against it,
// so nothing can deadlock with it.
type preAuthLimiter struct {
	limits PreAuthLimits

	mu      sync.Mutex
	conns   int
	perAddr map[netip.Addr]int
}

func newPreAuthLimiter(limits PreAuthLimits) *preAuthLimiter {
	return &preAuthLimiter{limits: limits, perAddr: map[netip.Addr]int{}}
}

// admit takes the global slot for a freshly accepted socket and reports whether
// one was free. Above the cap nothing is allocated and nothing is counted, so a
// refusal costs the caller a close and nothing else.
func (l *preAuthLimiter) admit() (*preAuthSlot, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limits.MaxConns > 0 && l.conns >= l.limits.MaxConns {
		return nil, false
	}
	l.conns++
	return &preAuthSlot{lim: l}, true
}

// acquireAddr takes the slot for one client address, reporting whether one was
// free.
func (l *preAuthLimiter) acquireAddr(addr netip.Addr) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.limits.MaxConnsPerAddr > 0 && l.perAddr[addr] >= l.limits.MaxConnsPerAddr {
		return false
	}
	l.perAddr[addr]++
	return true
}

// release gives back the global slot and, when the connection was charged to
// one, the address slot. The map holds an entry only while an address has live
// pre-auth connections, so it is bounded by the caps above rather than by how
// many addresses have ever connected.
func (l *preAuthLimiter) release(addr netip.Addr) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.conns--
	if !addr.IsValid() {
		return
	}
	if n := l.perAddr[addr] - 1; n > 0 {
		l.perAddr[addr] = n
	} else {
		delete(l.perAddr, addr)
	}
}

// preAuthSlot is one connection's place in the pre-auth bounds: the global slot
// it took at accept, the address slot it takes once its client address is known,
// and the timer holding it to the lifetime ceiling.
//
// One goroutine owns a slot — the one serving its connection — from the moment
// the accept loop hands it over. clear is idempotent regardless, because it is
// called twice on the connections that authenticate: once by the frame that
// proves the key, and once by the deferred cleanup that runs whatever ends the
// connection.
type preAuthSlot struct {
	lim *preAuthLimiter
	// addr is the client address this slot is charged to, invalid when the
	// connection carries no address the server may key on.
	addr  netip.Addr
	timer *time.Timer
	once  sync.Once
}

// keyAddr charges the slot to the client address the connection layer
// established, reporting whether the address had a slot free.
//
// A connection with no address the server may key on — a socket whose peer the
// transport could not report, or a PROXY header that names no client, which is
// what a balancer's health check sends — is charged to nothing. The zero address
// is not a bucket to group connections into, for the same reason a request is
// never attributed to it, and grouping them would put every health check in one
// bucket with every transport fault. Those connections stay bounded by the
// global cap and the lifetime ceiling.
func (s *preAuthSlot) keyAddr(addr netip.Addr) bool {
	if !addr.IsValid() {
		return true
	}
	if !s.lim.acquireAddr(addr) {
		return false
	}
	s.addr = addr
	return true
}

// armLifetime starts the pre-auth lifetime ceiling, calling fire once it
// elapses. A zero Lifetime arms nothing.
func (s *preAuthSlot) armLifetime(fire func()) {
	if s.lim.limits.Lifetime <= 0 {
		return
	}
	s.timer = time.AfterFunc(s.lim.limits.Lifetime, fire)
}

// clear ends the connection's pre-auth state: both slots go back and the ceiling
// is disarmed.
//
// It is a one-way transition. A connection that authenticates and later
// renegotiates does not return under bounds it has already passed: re-taking a
// slot could fail, and the connection it would then close belongs to a client
// holding a key this server issued, which is not the population these bounds
// exist to shed.
//
// A ceiling that has already fired is not undone by it: that socket is closing,
// and a connection authenticating in the microseconds after its own ceiling
// elapsed reconnects like any other dropped one.
//
// A nil slot clears nothing, which is what a connection that was never accepted
// through the listener holds.
func (s *preAuthSlot) clear() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		if s.timer != nil {
			s.timer.Stop()
		}
		s.lim.release(s.addr)
	})
}
