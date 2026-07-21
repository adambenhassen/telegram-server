package mtproto

import (
	"context"
	"fmt"
	"sync"

	"github.com/gotd/log/logslog"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/transport"
)

// exchange runs the server side of the MTProto key exchange over conn and
// returns the negotiated auth key.
func (s *Server) exchange(ctx context.Context, conn transport.Conn) (crypto.AuthKey, error) {
	r, err := exchange.NewExchanger(conn, s.dcID).
		WithClock(s.clock).
		WithLogger(logslog.New(s.log.With("subsystem", "exchange"))).
		WithRand(s.cipher.Rand()).
		Server(s.key).
		Run(ctx)
	if err != nil {
		return crypto.AuthKey{}, err
	}
	return r.Key, nil
}

// exchangeConn wraps a transport.Conn for the key-exchange flow, rejecting any
// frame that carries a non-zero auth key ID (a client must not present a
// registered key mid-handshake). Mirrors gotd tgtest/exchange.go.
type exchangeConn struct {
	transport.Conn
}

// Recv reads the next handshake frame, replying with an AuthKeyNotFound proto
// error and retrying if the frame presents a non-zero auth key ID.
func (e exchangeConn) Recv(ctx context.Context, b *bin.Buffer) error {
	for {
		if err := e.Conn.Recv(ctx, b); err != nil {
			return err
		}

		var authKeyID [8]byte
		if err := b.PeekN(authKeyID[:], len(authKeyID)); err != nil {
			return fmt.Errorf("peek id: %w", err)
		}
		if authKeyID != [8]byte{} {
			var buf bin.Buffer
			buf.PutInt32(-codec.CodeAuthKeyNotFound)
			if err := e.Send(ctx, &buf); err != nil {
				return fmt.Errorf("send: %w", err)
			}
			continue
		}
		return nil
	}
}

// bufferedConn wraps a transport.Conn so already-read frames can be replayed to
// the exchanger, which needs to re-read the first frame of the connection.
// Mirrors gotd tgtest/buffered.go.
type bufferedConn struct {
	conn transport.Conn

	mu   sync.Mutex
	recv []bin.Buffer
}

func newBufferedConn(conn transport.Conn) *bufferedConn {
	return &bufferedConn{conn: conn}
}

// Push queues a frame to be replayed on the next Recv.
func (c *bufferedConn) Push(b *bin.Buffer) {
	c.mu.Lock()
	c.recv = append(c.recv, bin.Buffer{Buf: b.Copy()})
	c.mu.Unlock()
}

func (c *bufferedConn) pop() (bin.Buffer, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.recv) < 1 {
		return bin.Buffer{}, false
	}
	var r bin.Buffer
	r, c.recv = c.recv[len(c.recv)-1], c.recv[:len(c.recv)-1]
	return r, true
}

// Send writes to the underlying transport.
func (c *bufferedConn) Send(ctx context.Context, b *bin.Buffer) error {
	return c.conn.Send(ctx, b)
}

// Recv replays a queued frame if present, else reads from the transport.
func (c *bufferedConn) Recv(ctx context.Context, b *bin.Buffer) error {
	if e, ok := c.pop(); ok {
		b.ResetTo(e.Copy())
		return nil
	}
	return c.conn.Recv(ctx, b)
}

// Close closes the underlying transport.
func (c *bufferedConn) Close() error {
	return c.conn.Close()
}
