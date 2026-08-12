package mtproto

import (
	"context"
	"log/slog"
	"maps"
	"net/netip"
	"slices"
	"time"

	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/transport"
)

// NewTestConn builds a Conn wired to tconn with a ready session (key + session
// id set) so tests can exercise Push/SendResult without a full handshake.
func NewTestConn(tconn transport.Conn, key crypto.AuthKey) *Conn {
	c := newConn(tconn, crypto.NewServerCipher(crypto.DefaultRand()), proto.NewMessageIDGen(clock.System.Now), clock.System, time.Second, slog.New(slog.DiscardHandler))
	c.setKey(key)
	c.setSession(123)
	return c
}

// SetHandshakeTimeout shortens the bound on transport negotiation so a test
// need not wait the production timeout to observe a silent socket being closed.
// Call it before Serve: the accept loop reads the value on every connection.
func (s *Server) SetHandshakeTimeout(d time.Duration) {
	s.handshakeTimeout = d
}

// ServeConn exposes the per-connection serve loop so tests can drive it with a
// scripted transport instead of a real client handshake. The connection carries
// no peer address: a scripted transport is not a socket, and the zero address is
// what a handler must be able to cope with in that case. It holds no pre-auth
// slot either, having never been accepted through a listener.
func (s *Server) ServeConn(ctx context.Context, tconn transport.Conn) error {
	return s.serveConn(ctx, tconn, netip.Addr{}, nil)
}

// SetOwner exposes the conn's ownership hand-off, which only the serve loop
// performs in production.
func (c *Conn) SetOwner(userID int64) {
	c.setOwner(userID)
}

// SetKey exposes the per-frame auth key binding, which only the serve loop
// performs in production, so a test can rebind a live conn's key.
func (c *Conn) SetKey(key crypto.AuthKey) {
	c.setKey(key)
}

// Users returns every user id holding at least one registered connection, so a
// test can assert a connection sits in no bucket at all.
func (r *SessionRegistry) Users() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Collect(maps.Keys(r.m))
}
