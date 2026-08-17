package mtproto

import (
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
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

	// writeMu serializes socket writes and guards the mutable session state
	// (authKey, sessionID, owner) it reads, so a server-initiated push from the
	// delivery goroutine cannot interleave with a reply write on the serve
	// goroutine, nor write an update for a user this conn has stopped belonging
	// to. It is the only lock taken here and is never held across the registry
	// lock, in either direction.
	writeMu   sync.Mutex
	authKey   crypto.AuthKey
	sessionID int64
	// owner is the user this conn's auth key is currently bound to, 0 for none.
	// Delivery addresses every push to an owner, so a push built for the user
	// who held the key a moment ago is dropped instead of written.
	owner int64

	// created is touched only by the connection's single serve goroutine.
	created map[int64]struct{}

	// unimplemented bounds what this connection may spend on methods this
	// server does not implement, and thins the line they produce. Touched only
	// by the same serve goroutine as created, which is the only one that
	// dispatches this connection's frames.
	unimplemented unimplementedBudget

	// lastPushedPts is the highest pts already pushed to this conn, so a
	// notification never re-delivers events. Read/written only by the delivery
	// goroutine, but atomic for safety across the registry hand-off.
	lastPushedPts atomic.Int64

	// authKeyID mirrors authKey.IntID() for readers that must not take writeMu.
	// Eviction runs on the single LISTEN goroutine and matches conns by key id,
	// so reading it under writeMu would let one blackholed socket — a push
	// parked in the write timeout — stall every user's delivery.
	authKeyID atomic.Int64
}

// LastPushedPts returns the highest pts already pushed to this connection.
func (c *Conn) LastPushedPts() int {
	return int(c.lastPushedPts.Load())
}

// AuthKeyID returns the id of the auth key this connection last set, 0 before
// the first encrypted frame. Safe to call from another goroutine, and lock-free
// on purpose: it is read while matching an eviction against live conns.
func (c *Conn) AuthKeyID() int64 {
	return c.authKeyID.Load()
}

// Close shuts the underlying transport down, unblocking the serve goroutine's
// pending Recv so it deregisters the conn and exits. It deliberately does not
// take writeMu: a revoked session must not wait on a write already in flight.
// A second close from the serve loop's own defer is a no-op the caller ignores.
func (c *Conn) Close() error {
	return c.transport.Close()
}

// setOwner records the user this conn's auth key is now bound to. Changing
// owner clears the push watermark, since the previous owner's pts means nothing
// in the new owner's space and delivery must treat the conn as freshly
// registered. It blocks until any push already on the wire finishes, which is
// what makes the hand-off atomic against delivery.
func (c *Conn) setOwner(userID int64) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.owner == userID {
		return
	}
	c.owner = userID
	c.lastPushedPts.Store(0)
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
	c.writeMu.Lock()
	c.authKey = key
	c.writeMu.Unlock()
	c.authKeyID.Store(key.IntID())
}

// setSession records the client session id for subsequent server writes.
func (c *Conn) setSession(id int64) {
	c.writeMu.Lock()
	c.sessionID = id
	c.writeMu.Unlock()
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
// The encrypt+write and the session-state reads it depends on are serialized by
// writeMu so reply and Push writes never interleave on one socket.
func (c *Conn) send(ctx context.Context, t proto.MessageType, message bin.Encoder) error {
	var b bin.Buffer
	if err := message.Encode(&b); err != nil {
		return fmt.Errorf("encode: %w", err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.sendLocked(ctx, t, &b)
}

// sendLocked encrypts an already-encoded body under the session key and writes
// it to the transport. writeMu must be held: it guards both the session state
// read here and the write itself.
func (c *Conn) sendLocked(ctx context.Context, t proto.MessageType, b *bin.Buffer) error {
	if b.Len() > math.MaxInt32 {
		return fmt.Errorf("message too large: %d bytes", b.Len())
	}

	data := crypto.EncryptedMessageData{
		SessionID:              c.sessionID,
		MessageID:              c.msgID.New(t),
		MessageDataLen:         int32(b.Len()), //nolint:gosec // bounded above by MaxInt32
		MessageDataWithPadding: b.Copy(),
	}
	if err := c.cipher.Encrypt(c.authKey, data, b); err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()
	if err := c.transport.Send(ctx, b); err != nil {
		return fmt.Errorf("send: %w", err)
	}
	return nil
}

// PushTo encrypts enc under the conn's auth key and writes it as an unsolicited
// server message (fresh msg_id + server seqno), but only while the conn still
// belongs to owner; it reports whether the write happened. The ownership check,
// the write and the watermark advance share one critical section, so a rebind
// cannot land between them and let an update built from an already-stale
// registry snapshot reach the user who has taken the key over.
//
// pts is the pts the batch advertises and is recorded only on a successful
// write; pass 0 for a transient update that carries none, since a persisted
// batch always advertises at least 1. Safe to call from another goroutine.
func (c *Conn) PushTo(ctx context.Context, owner int64, enc bin.Encoder, pts int) (bool, error) {
	var b bin.Buffer
	if err := enc.Encode(&b); err != nil {
		return false, fmt.Errorf("push encode [%T]: %w", enc, err)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.owner != owner {
		return false, nil
	}
	if err := c.sendLocked(ctx, proto.MessageFromServer, &b); err != nil {
		return false, fmt.Errorf("push [%T]: %w", enc, err)
	}
	if pts > 0 {
		c.lastPushedPts.Store(int64(pts))
	}
	return true, nil
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
