package mtproto

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
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

	// pendingRPCUpdate holds sender-result barriers from commit through keyed
	// notification accounting. A zero pts is a barrier before the store commit
	// returns; a known pts remains unready until the RPC result reaches the
	// connection. The slice is ordered by request, so back-to-back sends cannot
	// replace an unresolved earlier result.
	// It is guarded by writeMu with the socket state so a generic notification
	// cannot pass the result write between its check and push.
	pendingRPCOwner   int64
	pendingRPCAuthKey int64
	pendingRPCPts     []int
	pendingRPCIDs     []uint64
	pendingRPCReady   []bool
	nextPendingRPCID  uint64
	// pendingRPCOverflow holds sender-result barriers that arrive after the
	// regular queue reaches its cap. The slice is bounded separately because a
	// burst can have several concurrent cap-refused attempts; each entry keeps
	// its own reservation so an aborted attempt cannot release another one.
	pendingRPCOverflow          []pendingRPCOverflowEntry
	pendingRPCOverflowSaturated bool

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

// RPCUpdateReservation identifies the sender-result barrier registered by one
// request attempt. A deduplicated retry receives a valid reservation without
// owning a queue entry, so its failure cannot clear the original attempt. An
// overflow reservation owns one entry in the bounded origin-suppression hold
// established at reservation time; it does not consume another queue entry.
type RPCUpdateReservation struct {
	owner    int64
	authKey  int64
	id       uint64
	owned    bool
	overflow bool
}

type pendingRPCOverflowEntry struct {
	id    uint64
	pts   int
	ready bool
}

// maxPendingRPCUpdates bounds the regular unresolved sender-result queue. Once
// the bound is reached, BeginRPCUpdate records the attempt in the separately
// bounded overflow hold. The persisted event remains recoverable through
// getDifference instead of growing this connection-local queue without limit.
const maxPendingRPCUpdates = 64

// maxPendingRPCOverflowUpdates keeps the fallback bookkeeping bounded even if
// callers keep starting sends while the regular barrier queue is full. Once
// this cap is reached, a conservative saturated hold remains until rebind;
// getDifference still recovers the durable events.
const maxPendingRPCOverflowUpdates = maxPendingRPCUpdates

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

// WriteTimeout returns the deadline applied to each transport write. Delivery
// fan-out uses it to bound one transient callback without multiplying the
// deadline by the number of sockets it addresses.
func (c *Conn) WriteTimeout() time.Duration {
	return c.writeTimeout
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
	_, ok := c.BeginRPCUpdateAttempt(owner, authKeyID, pts)
	return ok
}

// BeginRPCUpdateAttempt blocks generic delivery to the originating connection
// while one sender result is being prepared and returns ownership for that
// request attempt. A known pts retry deduplicates against the existing barrier;
// it does not own that barrier. Pass pts=0 before the store commit and set it
// with SetRPCUpdatePtsAttempt as soon as the commit returns.
func (c *Conn) BeginRPCUpdateAttempt(owner, authKeyID int64, pts int) (RPCUpdateReservation, bool) {
	if owner <= 0 || authKeyID == 0 || pts < 0 {
		return RPCUpdateReservation{}, false
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.owner != owner || c.authKeyID.Load() != authKeyID {
		return RPCUpdateReservation{}, false
	}
	if pts > 0 {
		if i := slices.Index(c.pendingRPCPts, pts); i >= 0 {
			return RPCUpdateReservation{
				owner:   owner,
				authKey: authKeyID,
				id:      c.pendingRPCIDs[i],
			}, true
		}
		for _, entry := range c.pendingRPCOverflow {
			if entry.pts == pts {
				return RPCUpdateReservation{
					owner:    owner,
					authKey:  authKeyID,
					id:       entry.id,
					overflow: true,
				}, true
			}
		}
	}
	if len(c.pendingRPCOverflow) > 0 || c.pendingRPCOverflowSaturated {
		if c.pendingRPCOverflowSaturated {
			return RPCUpdateReservation{owner: owner, authKey: authKeyID, overflow: true}, false
		}
		if len(c.pendingRPCOverflow) >= maxPendingRPCOverflowUpdates {
			c.pendingRPCOverflowSaturated = true
			return RPCUpdateReservation{owner: owner, authKey: authKeyID, overflow: true}, false
		}
		c.nextPendingRPCID++
		if c.nextPendingRPCID == 0 {
			c.nextPendingRPCID = 1
		}
		c.pendingRPCOverflow = append(c.pendingRPCOverflow, pendingRPCOverflowEntry{id: c.nextPendingRPCID})
		return RPCUpdateReservation{owner: owner, authKey: authKeyID, id: c.nextPendingRPCID, owned: true, overflow: true}, false
	}
	if len(c.pendingRPCPts) >= maxPendingRPCUpdates {
		c.nextPendingRPCID++
		if c.nextPendingRPCID == 0 {
			c.nextPendingRPCID = 1
		}
		c.pendingRPCOverflow = append(c.pendingRPCOverflow, pendingRPCOverflowEntry{id: c.nextPendingRPCID})
		return RPCUpdateReservation{owner: owner, authKey: authKeyID, id: c.nextPendingRPCID, owned: true, overflow: true}, false
	}
	c.nextPendingRPCID++
	if c.nextPendingRPCID == 0 {
		c.nextPendingRPCID = 1
	}
	c.pendingRPCOwner = owner
	c.pendingRPCAuthKey = authKeyID
	c.pendingRPCPts = append(c.pendingRPCPts, pts)
	c.pendingRPCIDs = append(c.pendingRPCIDs, c.nextPendingRPCID)
	c.pendingRPCReady = append(c.pendingRPCReady, false)
	return RPCUpdateReservation{
		owner:   owner,
		authKey: authKeyID,
		id:      c.nextPendingRPCID,
		owned:   true,
	}, true
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
	for i := range slices.Backward(c.pendingRPCPts) {
		if c.pendingRPCPts[i] == 0 {
			c.pendingRPCPts[i] = pts
			return true
		}
	}
	return false
}

// SetRPCUpdatePtsAttempt records the committed sender event's pts on the
// barrier owned by one request attempt. An overflow reservation keeps its own
// pts so the result write and keyed delivery can release only that hold.
func (c *Conn) SetRPCUpdatePtsAttempt(reservation RPCUpdateReservation, pts int) bool {
	if pts <= 0 {
		return false
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.owner != reservation.owner || c.authKeyID.Load() != reservation.authKey {
		return false
	}
	if reservation.overflow {
		if reservation.id == 0 {
			return c.pendingRPCOverflowSaturated
		}
		for i := range c.pendingRPCOverflow {
			if c.pendingRPCOverflow[i].id != reservation.id {
				continue
			}
			if !reservation.owned {
				return c.pendingRPCOverflow[i].pts == pts
			}
			if c.pendingRPCOverflow[i].pts == 0 {
				c.pendingRPCOverflow[i].pts = pts
				return true
			}
			return c.pendingRPCOverflow[i].pts == pts
		}
		return false
	}
	if !reservation.owned {
		return false
	}
	if c.pendingRPCOwner != reservation.owner || c.pendingRPCAuthKey != reservation.authKey {
		return false
	}
	for i, id := range c.pendingRPCIDs {
		if id != reservation.id {
			continue
		}
		if c.pendingRPCPts[i] == 0 {
			c.pendingRPCPts[i] = pts
			return true
		}
		return c.pendingRPCPts[i] == pts
	}
	return false
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

// ClearRPCUpdateAttempt releases only the barrier registered by the given
// request attempt. Deduplicated attempts own nothing. A committed overflow
// attempt is cleared here when its result write fails, allowing the generic
// notification to deliver the durable event instead.
func (c *Conn) ClearRPCUpdateAttempt(reservation RPCUpdateReservation) bool {
	if reservation.overflow {
		c.writeMu.Lock()
		defer c.writeMu.Unlock()
		if reservation.id == 0 || !reservation.owned || c.owner != reservation.owner || c.authKeyID.Load() != reservation.authKey {
			return false
		}
		for i, entry := range c.pendingRPCOverflow {
			if entry.id != reservation.id {
				continue
			}
			c.removePendingRPCOverflowAtLocked(i)
			return true
		}
		return false
	}
	if !reservation.owned {
		return false
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.pendingRPCOwner != reservation.owner || c.pendingRPCAuthKey != reservation.authKey || c.owner != reservation.owner || c.authKeyID.Load() != reservation.authKey {
		return false
	}
	for i, id := range c.pendingRPCIDs {
		if id == reservation.id {
			c.removePendingRPCAtLocked(i)
			return true
		}
	}
	return false
}

func (c *Conn) clearRPCUpdateLocked(owner, authKeyID int64) bool {
	if c.pendingRPCOwner != owner || c.pendingRPCAuthKey != authKeyID || len(c.pendingRPCPts) == 0 {
		return false
	}
	c.removePendingRPCAtLocked(len(c.pendingRPCPts) - 1)
	return true
}

func (c *Conn) removePendingRPCAtLocked(index int) {
	copy(c.pendingRPCPts[index:], c.pendingRPCPts[index+1:])
	copy(c.pendingRPCIDs[index:], c.pendingRPCIDs[index+1:])
	copy(c.pendingRPCReady[index:], c.pendingRPCReady[index+1:])
	c.pendingRPCPts = c.pendingRPCPts[:len(c.pendingRPCPts)-1]
	c.pendingRPCIDs = c.pendingRPCIDs[:len(c.pendingRPCIDs)-1]
	c.pendingRPCReady = c.pendingRPCReady[:len(c.pendingRPCReady)-1]
	if len(c.pendingRPCPts) == 0 {
		c.pendingRPCOwner = 0
		c.pendingRPCAuthKey = 0
		c.pendingRPCIDs = nil
		c.pendingRPCReady = nil
	}
}

func (c *Conn) removePendingRPCOverflowAtLocked(index int) {
	copy(c.pendingRPCOverflow[index:], c.pendingRPCOverflow[index+1:])
	c.pendingRPCOverflow = c.pendingRPCOverflow[:len(c.pendingRPCOverflow)-1]
}

// PendingRPCUpdate reports the sender-result barrier for owner. A true result
// with pts=0 means the send is committed-or-in-flight but its event pts is not
// known yet, so generic delivery must wait rather than risk crossing it.
func (c *Conn) PendingRPCUpdate(owner int64) (pts int, pending bool) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.pendingRPCOwner == owner && len(c.pendingRPCPts) > 0 {
		return c.pendingRPCPts[0], true
	}
	if c.owner == owner {
		if len(c.pendingRPCOverflow) > 0 {
			return c.pendingRPCOverflow[0].pts, true
		}
		if c.pendingRPCOverflowSaturated {
			return 0, true
		}
	}
	return 0, false
}

// PendingRPCUpdateReady reports whether the first sender-result barrier has
// reached the wire. Delivery must hold a known-pts barrier until this becomes
// true: the handler records the committed pts before the RPC result write, so
// a generic notification can otherwise push its prefix while the result is
// still paused or fails.
func (c *Conn) PendingRPCUpdateReady(owner int64) bool {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.pendingRPCOwner == owner && len(c.pendingRPCPts) > 0 {
		return len(c.pendingRPCReady) > 0 && c.pendingRPCReady[0]
	}
	if c.owner == owner && len(c.pendingRPCOverflow) > 0 {
		return c.pendingRPCOverflow[0].ready
	}
	return false
}

func (c *Conn) markRPCUpdateLocked(owner, authKeyID int64, pts int) bool {
	if authKeyID == 0 || pts <= 0 {
		return false
	}
	if c.owner != owner || c.authKeyID.Load() != authKeyID {
		return false
	}
	current := c.lastPushedPts.Load()
	if len(c.pendingRPCPts) > 0 {
		pendingPts := c.pendingRPCPts[0]
		if pendingPts == 0 {
			return false
		}
		if pendingPts != pts {
			return int64(pts) <= current
		}
		if int64(pts) > current {
			c.lastPushedPts.Store(int64(pts))
		}
		c.removePendingRPCAtLocked(0)
		return true
	}
	if len(c.pendingRPCOverflow) > 0 {
		pendingPts := c.pendingRPCOverflow[0].pts
		if pendingPts == 0 {
			return false
		}
		if pendingPts != pts {
			return int64(pts) <= current
		}
		if int64(pts) > current {
			c.lastPushedPts.Store(int64(pts))
		}
		c.removePendingRPCOverflowAtLocked(0)
		return true
	}
	if int64(pts) > current {
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
	c.pendingRPCPts = nil
	c.pendingRPCIDs = nil
	c.pendingRPCReady = nil
	c.pendingRPCOverflow = nil
	c.pendingRPCOverflowSaturated = false
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
	keyID := key.IntID()
	c.writeMu.Lock()
	keyChanged := c.authKey.IntID() != keyID
	c.authKey = key
	if keyChanged {
		c.pendingRPCOwner = 0
		c.pendingRPCAuthKey = 0
		c.pendingRPCPts = nil
		c.pendingRPCIDs = nil
		c.pendingRPCReady = nil
		c.pendingRPCOverflow = nil
		c.pendingRPCOverflowSaturated = false
	}
	c.writeMu.Unlock()
	c.authKeyID.Store(keyID)
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
		if closeErr := c.transport.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close failed push transport: %w", closeErr))
		}
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
// event. Keep the barrier active across that gap so a generic notification
// cannot push the result event before its keyed notification accounts it.
func (c *Conn) markRPCResultLocked(owner, authKeyID int64, pts int) bool {
	if authKeyID == 0 || pts <= 0 {
		return false
	}
	if c.owner != owner || c.authKeyID.Load() != authKeyID {
		return false
	}
	pendingIndex := -1
	if c.pendingRPCOwner == owner && c.pendingRPCAuthKey == authKeyID {
		for i := range slices.Backward(c.pendingRPCPts) {
			if c.pendingRPCPts[i] == pts {
				pendingIndex = i
				break
			}
		}
	}
	overflowIndex := -1
	for i := range slices.Backward(c.pendingRPCOverflow) {
		if c.pendingRPCOverflow[i].pts == pts {
			overflowIndex = i
			break
		}
	}
	current := c.lastPushedPts.Load()
	if pendingIndex >= 0 && pendingIndex < len(c.pendingRPCReady) {
		c.pendingRPCReady[pendingIndex] = true
	}
	if overflowIndex >= 0 {
		c.pendingRPCOverflow[overflowIndex].ready = true
	}
	switch {
	case int64(pts) <= current:
		if pendingIndex >= 0 {
			c.removePendingRPCAtLocked(pendingIndex)
		} else if overflowIndex >= 0 {
			c.removePendingRPCOverflowAtLocked(overflowIndex)
		}
	case int64(pts) == current+1:
		c.lastPushedPts.Store(int64(pts))
		if pendingIndex >= 0 {
			c.removePendingRPCAtLocked(pendingIndex)
		} else if overflowIndex >= 0 {
			c.removePendingRPCOverflowAtLocked(overflowIndex)
		}
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
		if closeErr := c.transport.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close failed push transport: %w", closeErr))
		}
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
