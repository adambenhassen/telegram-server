package mtproto

import (
	"context"
	"errors"
	"fmt"
	"net/netip"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tgerr"
)

// errInternalRPC is the error a timed-out request is answered with. It is
// deliberately indistinguishable from any other transient failure: a deadline
// must not become a new observable outcome class on any path, least of all the
// download path, where an entitled download of an erased file and a download
// of a file that never existed have to stay identical.
var errInternalRPC = tgerr.New(500, "INTERNAL")

// rpcHandle decrypts an encrypted MTProto frame on an established session and
// dispatches its contents. The connection's auth key must already be set to the
// key matching the frame's auth key ID, and userID is the user bound to that key
// (0 when unbound), resolved by the caller in the same lookup as the key.
// provisional is true when the session is username-mode with no verifier.
// clientAddr is the peer address of the socket the frame arrived on.
//
// slot is the connection's place in the pre-auth bounds, cleared here and
// nowhere else. Clearing is idempotent, so every frame after the first passes a
// slot already given back; it is nil for a connection that was never accepted
// through a listener.
func (s *Server) rpcHandle(ctx context.Context, c *Conn, b *bin.Buffer, userID int64, provisional bool, clientAddr netip.Addr, slot *preAuthSlot) error {
	m := &crypto.EncryptedMessage{}
	if err := m.DecodeWithoutCopy(b); err != nil {
		return fmt.Errorf("decode encrypted message: %w", err)
	}

	msg, err := s.cipher.Decrypt(c.authKey, m)
	if err != nil {
		return fmt.Errorf("decrypt message: %w", err)
	}
	// The moment the connection stops being one of the anonymous ones the
	// pre-auth bounds hold back: the frame's MAC verified under a key this
	// server issued. Here rather than after the dispatch below, because
	// everything after this line is work done on behalf of a proven key —
	// including a handler slow enough to cross what remains of the ceiling,
	// which would otherwise close a connection that had already authenticated.
	slot.clear()
	c.setSession(msg.SessionID)

	if !c.markCreated(msg.SessionID) {
		if err := c.sendSessionCreated(ctx, saltFromKeyID(c.authKey.ID)); err != nil {
			return err
		}
	}

	// Every RPC dispatched from this frame runs under the per-request ceiling.
	// It bounds how long one call — and any transaction it opens — may hold
	// database resources: past it, pgx cancels the in-flight statement
	// server-side, the handler's store calls fail, and handle answers the
	// client with a generic INTERNAL error while the connection stays up. The
	// frame context itself stays clean, so the touch and re-reads after
	// dispatch never inherit what remains of an RPC's budget.
	reqCtx := ctx
	if s.rpcDeadline > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, s.rpcDeadline)
		defer cancel()
	}

	// Buffer now holds the plaintext message body.
	b.ResetTo(msg.Data())

	return s.handle(c, &Request{
		AuthKeyID:   c.authKey.ID,
		UserID:      userID,
		Provisional: provisional,
		ClientAddr:  clientAddr,
		SessionID:   msg.SessionID,
		MsgID:       msg.MessageID,
		Buf:         b,
		Ctx:         reqCtx,
	})
}

// handle processes a single plaintext message: service messages are answered
// directly, containers and gzip are unwrapped, and everything else is passed to
// the RPC handler. Mirrors gotd tgtest/handle.go.
func (s *Server) handle(c *Conn, req *Request) error {
	in := req.Buf
	id, err := in.PeekID()
	if err != nil {
		return fmt.Errorf("peek id: %w", err)
	}

	switch id {
	case mt.PingRequestTypeID:
		ping := mt.PingRequest{}
		if err := ping.Decode(in); err != nil {
			return err
		}
		return c.sendPong(req, ping.PingID)

	case mt.PingDelayDisconnectRequestTypeID:
		ping := mt.PingDelayDisconnectRequest{}
		if err := ping.Decode(in); err != nil {
			return err
		}
		return c.sendPong(req, ping.PingID)

	case mt.GetFutureSaltsRequestTypeID:
		salts := mt.GetFutureSaltsRequest{}
		if err := salts.Decode(in); err != nil {
			return err
		}
		return c.sendEternalSalt(req)

	case mt.MsgsAckTypeID:
		ack := mt.MsgsAck{}
		if err := ack.Decode(in); err != nil {
			return err
		}
		// Acknowledgements need no response.
		return nil

	case proto.GZIPTypeID:
		var content proto.GZIP
		if err := content.Decode(in); err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		req.Buf = &bin.Buffer{Buf: content.Data}

	case proto.MessageContainerTypeID:
		var container proto.MessageContainer
		if err := container.Decode(in); err != nil {
			return fmt.Errorf("container: %w", err)
		}
		for i := range container.Messages {
			m := container.Messages[i]
			if err := s.handle(c, &Request{
				AuthKeyID:   req.AuthKeyID,
				UserID:      req.UserID,
				Provisional: req.Provisional,
				ClientAddr:  req.ClientAddr,
				SessionID:   req.SessionID,
				MsgID:       m.ID,
				Buf:         &bin.Buffer{Buf: m.Body},
				Ctx:         req.Ctx,
			}); err != nil {
				return err
			}
		}
		return nil
	}

	if err := s.handler.OnMessage(c, req); err != nil {
		var rpcErr *tgerr.Error
		if errors.As(err, &rpcErr) {
			return c.SendErr(req, rpcErr)
		}
		// A handler still running when its request deadline fired is abandoned,
		// not fatal to the connection: the client gets the same generic INTERNAL
		// any transient failure produces, and the next frame on this socket
		// starts a fresh request with a full budget. The reply goes out under a
		// detached context — the deadline has already spent itself, and the write
		// timeout is the bound that applies to a socket write.
		if errors.Is(req.Ctx.Err(), context.DeadlineExceeded) {
			reply := *req
			reply.Ctx = context.WithoutCancel(req.Ctx)
			return c.SendErr(&reply, errInternalRPC)
		}
		return err
	}
	return nil
}
