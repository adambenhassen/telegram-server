package mtproto

import (
	"context"
	"errors"
	"fmt"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tgerr"
)

// rpcHandle decrypts an encrypted MTProto frame on an established session and
// dispatches its contents. The connection's auth key must already be set to the
// key matching the frame's auth key ID.
func (s *Server) rpcHandle(ctx context.Context, c *Conn, b *bin.Buffer) error {
	m := &crypto.EncryptedMessage{}
	if err := m.DecodeWithoutCopy(b); err != nil {
		return fmt.Errorf("decode encrypted message: %w", err)
	}

	msg, err := s.cipher.Decrypt(c.authKey, m)
	if err != nil {
		return fmt.Errorf("decrypt message: %w", err)
	}
	c.sessionID = msg.SessionID

	if !c.markCreated(msg.SessionID) {
		if err := c.sendSessionCreated(ctx, saltFromKeyID(c.authKey.ID)); err != nil {
			return err
		}
	}

	// Buffer now holds the plaintext message body.
	b.ResetTo(msg.Data())

	return s.handle(c, &Request{
		AuthKeyID: c.authKey.ID,
		SessionID: msg.SessionID,
		MsgID:     msg.MessageID,
		Buf:       b,
		Ctx:       ctx,
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
		var errs error
		for i := range container.Messages {
			m := container.Messages[i]
			errs = errors.Join(errs, s.handle(c, &Request{
				AuthKeyID: req.AuthKeyID,
				UserID:    req.UserID,
				SessionID: req.SessionID,
				MsgID:     m.ID,
				Buf:       &bin.Buffer{Buf: m.Body},
				Ctx:       req.Ctx,
			}))
		}
		return errs
	}

	if err := s.handler.OnMessage(c, req); err != nil {
		var rpcErr *tgerr.Error
		if errors.As(err, &rpcErr) {
			return c.SendErr(req, rpcErr)
		}
		return err
	}
	return nil
}
