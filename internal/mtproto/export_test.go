package mtproto

import (
	"context"
	"log/slog"
	"maps"
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

// ServeConn exposes the per-connection serve loop so tests can drive it with a
// scripted transport instead of a real client handshake.
func (s *Server) ServeConn(ctx context.Context, tconn transport.Conn) error {
	return s.serveConn(ctx, tconn)
}

// Users returns every user id holding at least one registered connection, so a
// test can assert a connection sits in no bucket at all.
func (r *SessionRegistry) Users() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Collect(maps.Keys(r.m))
}
