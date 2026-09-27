package mtproto

import (
	"context"
	"encoding/binary"
	"errors"
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

// pushEncodeError marks an encoder failure so delivery telemetry can
// distinguish it from a failure after encoding. It carries no additional
// message, preserving the existing error text and unwrap chain.
type pushEncodeError struct {
	err error
}

func (e *pushEncodeError) Error() string { return e.err.Error() }

func (e *pushEncodeError) Unwrap() error { return e.err }

// MarkPushEncodeError marks an error returned while encoding a server push.
// It is useful to in-process delivery adapters that need the same fixed failure
// classification as Conn.PushTo.
func MarkPushEncodeError(err error) error {
	if err == nil {
		return nil
	}
	return &pushEncodeError{err: err}
}

// IsPushEncodeError reports whether err originated while encoding a server
// push. Other PushTo errors are write failures for delivery telemetry.
func IsPushEncodeError(err error) bool {
	var target *pushEncodeError
	return errors.As(err, &target)
}

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

	// lastPushedPts is the highest owner pts delivered to this conn by a push or
	// accounted for by its send RPC result, so a notification never re-delivers
	// events. Delivery writes it and the bounded admin sampler reads it; atomic
	// access keeps the registry hand-off safe.
	lastPushedPts atomic.Int64

	// pendingRPCUpdate is the sender event committed by the request currently
	// being prepared for this connection. A zero pts is a barrier before the
	// store commit returns; delivery must hold the origin back until the event's
	// pts is known. It is guarded by writeMu with the socket state so a generic
	// notification cannot pass the result write between its check and push.
	pendingRPCOwner   int64
	pendingRPCAuthKey int64
	pendingRPCPts     int

	// authKeyID mirrors authKey.IntID() for readers that must not take writeMu.
	// Eviction runs on the single LISTEN goroutine and matches conns by key id,
	// so reading it under writeMu would let one blackholed socket — a push
	// parked in the write timeout — stall every user's delivery.
	authKeyID atomic.Int64

	// pendingLogin is set when auth.signIn stages a user for the password
	// challenge. It belongs to this connection rather than the shared auth-key
	// store, so the serve loop can apply connection-local limits to the socket
	// that received SESSION_PASSWORD_NEEDED.
	pendingLogin atomic.Bool
	// pendingLoginAt is written with the marker and read by the serving
	// goroutine after rpcHandle returns. Unix nanoseconds keep the transition
	// timestamp lock-free while the deadline remains tied to the first marker
	// transition rather than to a later client frame.
	pendingLoginAt atomic.Int64
}

// LastPushedPts returns the highest contiguous owner pts already pushed to this
// connection or accounted for by a successful RPC result.
func (c *Conn) LastPushedPts() int {
	return int(c.lastPushedPts.Load())
}

// AuthKeyID returns the id of the auth key this connection last set, 0 before
// the first encrypted frame. Safe to call from another goroutine, and lock-free
// on purpose: it is read while matching an eviction against live conns.
func (c *Conn) AuthKeyID() int64 {
	return c.authKeyID.Load()
}

// MarkRPCUpdate accounts for the message update identified in the send RPC
// result for this auth key. If that reply is lost, getDifference still replays
// the event because only this connection's push watermark advances. The owner,
// key check and watermark advance share writeMu with pushes and rebinds.
func (c *Conn) MarkRPCUpdate(owner, authKeyID int64, pts int) bool {
	if authKeyID == 0 || pts <= 0 {
		return false
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.markRPCUpdateLocked(owner, authKeyID, pts)
}

// BeginRPCUpdate blocks generic delivery to the originating connection while
// a sender result is being prepared. Pass pts=0 before the store commit and
// set it with SetRPCUpdatePts as soon as the commit returns.
func (c *Conn) BeginRPCUpdate(owner, authKeyID int64, pts int) bool {
	if owner <= 0 || authKeyID == 0 || pts < 0 {
		return false
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.owner != owner || c.authKeyID.Load() != authKeyID {
		return false
	}
	c.pendingRPCOwner = owner
	c.pendingRPCAuthKey = authKeyID
	c.pendingRPCPts = pts
	return true
}

// SetRPCUpdatePts records the committed sender event's pts on an in-flight
// result barrier. It is separate from BeginRPCUpdate because the pts is
// allocated by the store transaction.
func (c *Conn) SetRPCUpdatePts(owner, authKeyID int64, pts int) bool {
	if owner <= 0 || authKeyID == 0 || pts <= 0 {
		return false
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.pendingRPCOwner != owner || c.pendingRPCAuthKey != authKeyID || c.owner != owner || c.authKeyID.Load() != authKeyID {
		return false
	}
	c.pendingRPCPts = pts
	return true
}

// ClearRPCUpdate releases a result barrier after the result write or an
// aborted post-commit path. A failed result can then use a generic nudge to
// recover the event for every live sender session.
func (c *Conn) ClearRPCUpdate(owner, authKeyID int64) bool {
	if owner <= 0 || authKeyID == 0 {
		return false
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.clearRPCUpdateLocked(owner, authKeyID)
}

func (c *Conn) clearRPCUpdateLocked(owner, authKeyID int64) bool {
	if c.pendingRPCOwner != owner || c.pendingRPCAuthKey != authKeyID {
		return false
	}
	c.pendingRPCOwner = 0
	c.pendingRPCAuthKey = 0
	c.pendingRPCPts = 0
	return true
}

// PendingRPCUpdate reports the sender-result barrier for owner. A true result
// with pts=0 means the send is committed-or-in-flight but its event pts is not
// known yet, so generic delivery must wait rather than risk crossing it.
func (c *Conn) PendingRPCUpdate(owner int64) (pts int, pending bool) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.pendingRPCOwner != owner {
		return 0, false
	}
	return c.pendingRPCPts, true
}

func (c *Conn) markRPCUpdateLocked(owner, authKeyID int64, pts int) bool {
	if authKeyID == 0 || pts <= 0 {
		return false
	}
	if c.owner != owner || c.authKeyID.Load() != authKeyID {
		return false
	}
	if current := c.lastPushedPts.Load(); int64(pts) > current {
		c.lastPushedPts.Store(int64(pts))
	}
	return true
}

// PendingLogin reports whether auth.signIn has staged a password challenge on
// this connection.
func (c *Conn) PendingLogin() bool {
	return c.pendingLogin.Load()
}

// MarkPendingLogin marks this connection as waiting for auth.checkPassword.
// The marker is intentionally connection-local and idempotent.
func (c *Conn) MarkPendingLogin() {
	if c.pendingLogin.CompareAndSwap(false, true) {
		c.pendingLoginAt.Store(c.clock.Now().UnixNano())
	}
}

func (c *Conn) pendingLoginSince() time.Time {
	n := c.pendingLoginAt.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
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
	c.pendingRPCOwner = 0
	c.pendingRPCAuthKey = 0
	c.pendingRPCPts = 0
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
	c.pendingRPCOwner = 0
	c.pendingRPCAuthKey = 0
	c.pendingRPCPts = 0
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
		return false, fmt.Errorf("push encode [%T]: %w", enc, MarkPushEncodeError(err))
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
//
// The write runs detached from the request context on purpose. The request
// deadline bounds the work behind a reply, not the reply itself: a result that
// finished just as the deadline fired must still reach the client, and a
// deadline that kills the reply would turn finished work into a dropped
// connection. What bounds a socket write is conn.writeTimeout, applied below.
func (c *Conn) SendResult(req *Request, msg bin.Encoder) error {
	return c.sendResult(req, msg, nil)
}

// SendResultAndMarkRPCUpdate writes msg and advances the originating session's
// contiguous push watermark while the result write still holds writeMu. A
// concurrent push therefore cannot land between the result and its watermark,
// which would expose a later pts before the client has received the result
// update.
func (c *Conn) SendResultAndMarkRPCUpdate(req *Request, msg bin.Encoder, owner, authKeyID int64, pts int) error {
	return c.sendResult(req, msg, func() {
		c.markRPCResultLocked(owner, authKeyID, pts)
	})
}

// markRPCResultLocked advances the contiguous push watermark for an RPC result.
// A result can carry a later pts while this connection still lacks an earlier
// event. Leave that gap visible so the keyed notification can push the prefix
// before MarkRPCUpdate accounts for the origin.
func (c *Conn) markRPCResultLocked(owner, authKeyID int64, pts int) bool {
	if authKeyID == 0 || pts <= 0 {
		return false
	}
	if c.owner != owner || c.authKeyID.Load() != authKeyID {
		return false
	}
	c.clearRPCUpdateLocked(owner, authKeyID)
	current := c.lastPushedPts.Load()
	if int64(pts) == current+1 {
		c.lastPushedPts.Store(int64(pts))
	}
	return true
}

// PushToAtWatermark is the ordered-update variant of PushTo. It drops a batch
// that was built from a stale watermark so the delivery loop can rebuild it
// after a concurrent RPC result or push changes the connection state.
func (c *Conn) PushToAtWatermark(ctx context.Context, owner int64, expectedPts int, enc bin.Encoder, pts int) (bool, bool, error) {
	var b bin.Buffer
	if err := enc.Encode(&b); err != nil {
		return false, false, fmt.Errorf("push encode [%T]: %w", enc, MarkPushEncodeError(err))
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.owner != owner {
		return false, false, nil
	}
	if int(c.lastPushedPts.Load()) != expectedPts {
		return false, true, nil
	}
	if err := c.sendLocked(ctx, proto.MessageFromServer, &b); err != nil {
		return false, false, fmt.Errorf("push [%T]: %w", enc, err)
	}
	if pts > 0 && int64(pts) > c.lastPushedPts.Load() {
		c.lastPushedPts.Store(int64(pts))
	}
	return true, false, nil
}

func (c *Conn) sendResult(req *Request, msg bin.Encoder, onSuccess func()) error {
	var buf bin.Buffer
	if err := msg.Encode(&buf); err != nil {
		req.rpcResult = RPCResultInternal
		return fmt.Errorf("encode result: %w", err)
	}
	var wire bin.Buffer
	if err := (&proto.Result{
		RequestMessageID: req.MsgID,
		Result:           buf.Raw(),
	}).Encode(&wire); err != nil {
		req.rpcResult = RPCResultInternal
		return fmt.Errorf("encode result envelope: %w", err)
	}
	var sendErr error
	func() {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		sendErr = c.sendLocked(context.WithoutCancel(req.Ctx), proto.MessageServerResponse, &wire)
		if sendErr != nil {
			req.rpcResult = RPCResultTransportFailure
			return
		}
		if req.rpcResult == "" {
			req.rpcResult = RPCResultSuccess
		}
		if onSuccess != nil {
			onSuccess()
		}
	}()
	if sendErr != nil {
		return fmt.Errorf("send result [%T]: %w", msg, sendErr)
	}
	return nil
}

// SendErr sends e as the RPC error result for req.
func (c *Conn) SendErr(req *Request, e *tgerr.Error) error {
	req.rpcResult = ClassifyRPCError(e)
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
	if err := c.send(context.WithoutCancel(req.Ctx), proto.MessageServerResponse, &mt.Pong{
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
	if err := c.send(context.WithoutCancel(req.Ctx), proto.MessageServerResponse, &mt.FutureSalts{
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
