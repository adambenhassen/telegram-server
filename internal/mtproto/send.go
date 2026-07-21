package mtproto

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tgerr"
	"github.com/gotd/td/transport"
)

// Conn is a single served MTProto connection: the transport plus the crypto and
// message-ID state needed to encrypt and send responses on the active session.
type Conn struct {
	transport    transport.Conn
	cipher       crypto.Cipher
	msgID        mtproto.MessageIDSource
	clock        clock.Clock
	writeTimeout time.Duration
	log          *slog.Logger

	// Per-session state, mutated only by the connection's single serve goroutine.
	authKey   crypto.AuthKey
	sessionID int64
	created   map[int64]struct{}
}

func newConn(
	tconn transport.Conn,
	cipher crypto.Cipher,
	msgID mtproto.MessageIDSource,
	c clock.Clock,
	writeTimeout time.Duration,
	log *slog.Logger,
) *Conn {
	return &Conn{
		transport:    tconn,
		cipher:       cipher,
		msgID:        msgID,
		clock:        c,
		writeTimeout: writeTimeout,
		log:          log,
		created:      map[int64]struct{}{},
	}
}

// setKey binds the connection to the auth key for the frame being handled.
func (c *Conn) setKey(key crypto.AuthKey) {
	c.authKey = key
}

// markCreated reports whether new_session_created was already sent for session,
// recording it as sent on the first call.
func (c *Conn) markCreated(session int64) bool {
	if _, ok := c.created[session]; ok {
		return true
	}
	c.created[session] = struct{}{}
	return false
}

// send encrypts message under the session key and writes it to the transport.
func (c *Conn) send(ctx context.Context, t proto.MessageType, message bin.Encoder) error {
	var b bin.Buffer
	if err := message.Encode(&b); err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if b.Len() > math.MaxInt32 {
		return fmt.Errorf("message too large: %d bytes", b.Len())
	}

	data := crypto.EncryptedMessageData{
		SessionID:              c.sessionID,
		MessageID:              c.msgID.New(t),
		MessageDataLen:         int32(b.Len()), //nolint:gosec // bounded above by MaxInt32
		MessageDataWithPadding: b.Copy(),
	}
	if err := c.cipher.Encrypt(c.authKey, data, &b); err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()
	if err := c.transport.Send(ctx, &b); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}

// SendResult sends msg as the RPC result for req.
func (c *Conn) SendResult(req *Request, msg bin.Encoder) error {
	var buf bin.Buffer
	if err := msg.Encode(&buf); err != nil {
		return fmt.Errorf("encode result: %w", err)
	}
	if err := c.send(req.Ctx, proto.MessageServerResponse, &proto.Result{
		RequestMessageID: req.MsgID,
		Result:           buf.Raw(),
	}); err != nil {
		return fmt.Errorf("send result [%T]: %w", msg, err)
	}
	return nil
}

// SendErr sends e as the RPC error result for req.
func (c *Conn) SendErr(req *Request, e *tgerr.Error) error {
	return c.SendResult(req, &mt.RPCError{
		ErrorCode:    e.Code,
		ErrorMessage: e.Message,
	})
}

// sendSessionCreated sends the new_session_created notification.
func (c *Conn) sendSessionCreated(ctx context.Context, serverSalt int64) error {
	if err := c.send(ctx, proto.MessageFromServer, &mt.NewSessionCreated{
		FirstMsgID: c.msgID.New(proto.MessageFromClient),
		ServerSalt: serverSalt,
	}); err != nil {
		return fmt.Errorf("send session created: %w", err)
	}
	return nil
}

// sendPong responds to a ping request.
func (c *Conn) sendPong(req *Request, pingID int64) error {
	if err := c.send(req.Ctx, proto.MessageServerResponse, &mt.Pong{
		MsgID:  req.MsgID,
		PingID: pingID,
	}); err != nil {
		return fmt.Errorf("send pong: %w", err)
	}
	return nil
}

// sendEternalSalt responds to get_future_salts with a single salt valid until
// the maximum representable date.
func (c *Conn) sendEternalSalt(req *Request) error {
	if err := c.send(req.Ctx, proto.MessageServerResponse, &mt.FutureSalts{
		ReqMsgID: req.MsgID,
		Now:      int(c.clock.Now().Unix()),
		Salts: []mt.FutureSalt{{
			ValidSince: 1,
			ValidUntil: math.MaxInt32,
			Salt:       10,
		}},
	}); err != nil {
		return fmt.Errorf("send future salts: %w", err)
	}
	return nil
}

// saltFromKeyID derives the server salt advertised in new_session_created from
// the auth key ID, mirroring gotd tgtest.
func saltFromKeyID(id [8]byte) int64 {
	return int64(binary.LittleEndian.Uint64(id[:])) //nolint:gosec // opaque 64-bit reinterpretation of key id bytes
}
