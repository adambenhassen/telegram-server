package api

import (
	"sync/atomic"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

type dialogFilterClock struct {
	sequence    uint64
	globalEpoch uint64
}

// DialogFilterSync tracks committed folder invalidations for this replica.
// Connection snapshots are local; only the small sequence and reconnect epoch
// are shared with the listener and recovery sweeper.
type DialogFilterSync struct {
	clock atomic.Pointer[dialogFilterClock]
}

// DialogFilterCapture is the recovery state captured before an authoritative
// folder read or updates.getDifference query.
type DialogFilterCapture struct {
	owner           int64
	session         int64
	generation      uint64
	firstDifference bool
	pending         bool
}

// NewDialogFilterSync creates an empty per-replica invalidation clock.
func NewDialogFilterSync() *DialogFilterSync {
	s := &DialogFilterSync{}
	s.clock.Store(&dialogFilterClock{})
	return s
}

func (s *DialogFilterSync) current() *dialogFilterClock {
	if current := s.clock.Load(); current != nil {
		return current
	}
	return &dialogFilterClock{}
}

func (s *DialogFilterSync) next() (generation, globalEpoch uint64) {
	for {
		current := s.current()
		next := &dialogFilterClock{sequence: current.sequence + 1, globalEpoch: current.globalEpoch}
		if s.clock.CompareAndSwap(current, next) {
			return next.sequence, next.globalEpoch
		}
	}
}

// ListenerReconnected advances the replica-wide epoch in O(1). Existing
// connections observe it through snapshots and the recovery sweeper; new
// bindings start covered through the current sequence.
func (s *DialogFilterSync) ListenerReconnected() {
	for {
		current := s.current()
		generation := current.sequence + 1
		next := &dialogFilterClock{sequence: generation, globalEpoch: generation}
		if s.clock.CompareAndSwap(current, next) {
			return
		}
	}
}

// EnsureBinding gives a connection a fresh recovery binding without inheriting
// invalidations that predate its current owner/session.
func (s *DialogFilterSync) EnsureBinding(conn *mtproto.Conn, req *mtproto.Request) {
	if conn == nil || req == nil || req.UserID == 0 || req.Provisional {
		return
	}
	clock := s.current()
	conn.EnsureDialogFilterRecoveryBinding(req.UserID, req.SessionID, clock.sequence)
}

// Capture returns the connection state for one owner/session binding.
func (s *DialogFilterSync) Capture(conn *mtproto.Conn, req *mtproto.Request) DialogFilterCapture {
	if conn == nil || req == nil || req.UserID == 0 || req.Provisional {
		return DialogFilterCapture{}
	}
	clock := s.current()
	conn.EnsureDialogFilterRecoveryBinding(req.UserID, req.SessionID, clock.sequence)
	_, _, first, pending, ok := conn.DialogFilterRecoverySnapshot(req.UserID, req.SessionID, clock.globalEpoch)
	if !ok {
		return DialogFilterCapture{}
	}
	generation := max(clock.sequence, clock.globalEpoch)
	return DialogFilterCapture{
		owner:           req.UserID,
		session:         req.SessionID,
		generation:      generation,
		firstDifference: first,
		pending:         pending,
	}
}

// AcknowledgeFetch covers only the generation captured before the database
// read. Invalidations that arrived while it ran remain pending.
func (s *DialogFilterSync) AcknowledgeFetch(conn *mtproto.Conn, req *mtproto.Request, captured DialogFilterCapture) {
	if conn == nil || req == nil || captured.owner != req.UserID || captured.session != req.SessionID {
		return
	}
	clock := s.current()
	conn.AcknowledgeDialogFilterFetch(req.UserID, req.SessionID, captured.generation, clock.globalEpoch)
}

// AcknowledgeDifference consumes the one-time first-difference signal only
// after the response carrying it was written successfully.
func (s *DialogFilterSync) AcknowledgeDifference(conn *mtproto.Conn, req *mtproto.Request, captured DialogFilterCapture, included bool) {
	if conn == nil || req == nil || captured.owner != req.UserID || captured.session != req.SessionID {
		return
	}
	conn.AcknowledgeDialogFilterDifference(req.UserID, req.SessionID, included && captured.firstDifference)
}

// RequesterRepair marks only the authenticated connection whose mutation
// response failed. It does not write a durable marker or notify other replicas.
func (s *DialogFilterSync) RequesterRepair(conn *mtproto.Conn, req *mtproto.Request) {
	if conn == nil || req == nil || req.UserID == 0 || req.Provisional {
		return
	}
	s.EnsureBinding(conn, req)
	generation, epoch := s.next()
	conn.MarkDialogFilterRecovery(req.UserID, req.SessionID, generation, epoch)
}

// OwnerInvalidation marks the owner's current local connections after a
// committed database notification. The payload contains only the owner ID.
func (s *DialogFilterSync) OwnerInvalidation(registry *mtproto.SessionRegistry, owner int64) {
	if registry == nil || owner <= 0 {
		return
	}
	generation, epoch := s.next()
	for _, conn := range registry.Conns(owner) {
		bindingOwner, session := conn.DialogFilterRecoveryBinding()
		if bindingOwner == owner {
			conn.MarkDialogFilterRecovery(owner, session, generation, epoch)
		}
	}
}

// Snapshot returns the shared sequence and reconnect epoch.
func (s *DialogFilterSync) Snapshot() (sequence, globalEpoch uint64) {
	clock := s.current()
	return clock.sequence, clock.globalEpoch
}
