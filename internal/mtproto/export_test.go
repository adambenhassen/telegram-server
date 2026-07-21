package mtproto

import (
	"log/slog"
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
