package api

import (
	"sync"
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

	recoveryMu        sync.Mutex
	recoveryQueue     []*mtproto.Conn
	recoveryQueueHead int
	recoveryQueued    map[*mtproto.Conn]struct{}
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
	s.recoveryQueued = make(map[*mtproto.Conn]struct{})
	return s
}

func (s *DialogFilterSync) enqueueRecovery(conn *mtproto.Conn) {
	if conn == nil {
		return
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	if s.recoveryQueued == nil {
		s.recoveryQueued = make(map[*mtproto.Conn]struct{})
	}
	if _, exists := s.recoveryQueued[conn]; exists {
		return
	}
	s.recoveryQueued[conn] = struct{}{}
	s.recoveryQueue = append(s.recoveryQueue, conn)
}

func (s *DialogFilterSync) enqueueRecoveryBatch(conns []*mtproto.Conn) {
	for _, conn := range conns {
		s.enqueueRecovery(conn)
	}
}

func (s *DialogFilterSync) takeRecoveryCandidates(limit int) []*mtproto.Conn {
	if limit <= 0 {
		return nil
	}
	s.recoveryMu.Lock()
	defer s.recoveryMu.Unlock()
	count := min(limit, len(s.recoveryQueue)-s.recoveryQueueHead)
	if count == 0 {
		return nil
	}
	out := make([]*mtproto.Conn, 0, count)
	for range count {
		conn := s.recoveryQueue[s.recoveryQueueHead]
		s.recoveryQueue[s.recoveryQueueHead] = nil
		s.recoveryQueueHead++
		delete(s.recoveryQueued, conn)
		if conn != nil {
			out = append(out, conn)
		}
	}
	if s.recoveryQueueHead == len(s.recoveryQueue) {
		s.recoveryQueue = nil
		s.recoveryQueueHead = 0
	} else if s.recoveryQueueHead >= 256 && s.recoveryQueueHead*2 >= len(s.recoveryQueue) {
		s.recoveryQueue = append([]*mtproto.Conn(nil), s.recoveryQueue[s.recoveryQueueHead:]...)
		s.recoveryQueueHead = 0
	}
	return out
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
	if conn == nil || req == nil || requestExpired(req) || captured.owner != req.UserID || captured.session != req.SessionID {
		return
	}
	clock := s.current()
	if conn.AcknowledgeDialogFilterFetch(req.UserID, req.SessionID, captured.generation, clock.globalEpoch) {
		_, globalEpoch := s.Snapshot()
		if conn.DialogFilterRecoveryAttemptPending(req.UserID, req.SessionID, globalEpoch) {
			s.enqueueRecovery(conn)
		}
	}
}

// AcknowledgeDifference consumes the one-time first-difference signal only
// after the response carrying it was written successfully.
func (s *DialogFilterSync) AcknowledgeDifference(conn *mtproto.Conn, req *mtproto.Request, captured DialogFilterCapture, included bool) {
	if conn == nil || req == nil || requestExpired(req) || captured.owner != req.UserID || captured.session != req.SessionID {
		return
	}
	if conn.AcknowledgeDialogFilterDifference(req.UserID, req.SessionID, included && captured.firstDifference) {
		_, globalEpoch := s.Snapshot()
		if conn.DialogFilterRecoveryAttemptPending(req.UserID, req.SessionID, globalEpoch) {
			s.enqueueRecovery(conn)
		}
	}
}

func requestExpired(req *mtproto.Request) bool {
	return req.Ctx != nil && req.Ctx.Err() != nil
}

// RequesterRepair marks only the authenticated connection whose mutation
// response failed. It does not write a durable marker or notify other replicas.
func (s *DialogFilterSync) RequesterRepair(conn *mtproto.Conn, req *mtproto.Request) {
	if conn == nil || req == nil || req.UserID == 0 || req.Provisional {
		return
	}
	s.EnsureBinding(conn, req)
	generation, epoch := s.next()
	if conn.MarkDialogFilterRecovery(req.UserID, req.SessionID, generation, epoch) {
		s.enqueueRecovery(conn)
	}
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
		if bindingOwner == owner && conn.MarkDialogFilterRecovery(owner, session, generation, epoch) {
			s.enqueueRecovery(conn)
		}
	}
}

// Snapshot returns the shared sequence and reconnect epoch.
func (s *DialogFilterSync) Snapshot() (sequence, globalEpoch uint64) {
	clock := s.current()
	return clock.sequence, clock.globalEpoch
}
