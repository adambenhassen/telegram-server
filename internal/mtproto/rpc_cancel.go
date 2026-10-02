package mtproto

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	rpcCancelUserWindow  = 10 * time.Second
	rpcCancelGlobalRate  = 2.0
	rpcCancelGlobalBurst = 2.0
	maxRPCCancelUsers    = 65_536
)

var (
	errPeerRPCDisconnected = errors.New("peer disconnected during rpc")
	errServerRPCClosed     = errors.New("server closed rpc connection")
)

type rpcUserCancelWindow struct {
	until      time.Time
	generation uint64
}

// rpcCancelBudget is replica-local admission state for peer-triggered RPC
// cancellation. It contains timestamps and counters only: request contexts and
// cancellation callbacks stay with the connection that owns the active RPC.
type rpcCancelBudget struct {
	mu sync.Mutex

	now func() time.Time

	users map[int64]rpcUserCancelWindow
	next  uint64

	tokens   float64
	refilled time.Time
	maxUsers int
}

type rpcCancelReservation struct {
	budget     *rpcCancelBudget
	userID     int64
	generation uint64
	once       sync.Once
}

// peerRPCState separates the peer's read lifecycle from server teardown. A
// peer loss always prevents later queued calls from starting, while only the
// active call may receive budgeted cancellation.
type peerRPCState struct {
	mu sync.Mutex

	budget      *rpcCancelBudget
	changed     chan struct{}
	generation  uint64
	peerGone    bool
	serverClose bool
	active      *activeRPC
}

type activeRPC struct {
	mu sync.Mutex

	ctx        context.Context
	cancel     context.CancelCauseFunc
	userID     int64
	running    bool
	attempting bool
	cancelled  bool
	budget     *rpcCancelBudget
}

func newRPCCancelBudget(now func() time.Time, maxUsers int) *rpcCancelBudget {
	if now == nil {
		now = time.Now
	}
	if maxUsers <= 0 {
		maxUsers = maxRPCCancelUsers
	}
	started := now()
	return &rpcCancelBudget{
		now:      now,
		users:    make(map[int64]rpcUserCancelWindow),
		tokens:   rpcCancelGlobalBurst,
		refilled: started,
		maxUsers: maxUsers,
	}
}

func newPeerRPCState(budget *rpcCancelBudget) *peerRPCState {
	return &peerRPCState{budget: budget, changed: make(chan struct{}, 1)}
}

func (p *peerRPCState) begin(userID int64, parent context.Context) (context.Context, func(), bool) {
	if parent == nil {
		parent = context.Background()
	}
	p.mu.Lock()
	if p.peerGone || p.serverClose || parent.Err() != nil {
		p.mu.Unlock()
		return parent, nil, false
	}
	ctx, cancel := context.WithCancelCause(parent)
	active := &activeRPC{
		ctx:     ctx,
		cancel:  cancel,
		userID:  userID,
		running: true,
		budget:  p.budget,
	}
	p.active = active
	p.signalChangeLocked()
	p.mu.Unlock()

	finish := func() {
		active.mu.Lock()
		active.running = false
		active.mu.Unlock()

		p.mu.Lock()
		if p.active == active {
			p.active = nil
			p.signalChangeLocked()
		}
		p.mu.Unlock()
		cancel(nil)
	}
	return ctx, finish, true
}

func (p *peerRPCState) peerDisconnected() {
	p.mu.Lock()
	p.peerGone = true
	active := p.active
	serverClose := p.serverClose
	p.mu.Unlock()
	if active == nil {
		return
	}
	if serverClose {
		active.cancelForServer()
		return
	}
	active.cancelForPeer()
}

func (p *peerRPCState) serverClosed() {
	p.mu.Lock()
	p.serverClose = true
	p.peerGone = true
	active := p.active
	p.mu.Unlock()
	if active != nil {
		active.cancelForServer()
	}
}

func (p *peerRPCState) disconnected() bool {
	p.mu.Lock()
	disconnected := p.peerGone
	p.mu.Unlock()
	return disconnected
}

func (p *peerRPCState) isServerClosed() bool {
	p.mu.Lock()
	serverClosed := p.serverClose
	p.mu.Unlock()
	return serverClosed
}

// closeIfIdle makes the idle timeout atomic with RPC admission. A request
// admitted before expiry keeps its read watcher; one admitted after it cannot
// start on a connection the server has already decided to close.
func (p *peerRPCState) closeIfIdle(generation uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.active != nil || p.generation != generation {
		return false
	}
	p.serverClose = true
	p.peerGone = true
	return true
}

func (p *peerRPCState) readState() (active bool, generation uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.active != nil, p.generation
}

func (p *peerRPCState) hasActive() bool {
	p.mu.Lock()
	active := p.active != nil
	p.mu.Unlock()
	return active
}

func (p *peerRPCState) changeSignal() <-chan struct{} {
	p.mu.Lock()
	changed := p.changed
	p.mu.Unlock()
	return changed
}

func (p *peerRPCState) signalChangeLocked() {
	p.generation++
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

func (a *activeRPC) cancelForPeer() {
	a.mu.Lock()
	if !a.running || a.attempting || a.cancelled || a.ctx.Err() != nil {
		a.mu.Unlock()
		return
	}
	a.attempting = true
	a.mu.Unlock()

	reservation, ok := a.budget.reserve(a.userID)
	if !ok {
		a.mu.Lock()
		a.attempting = false
		a.mu.Unlock()
		return
	}

	a.mu.Lock()
	if !a.running || a.ctx.Err() != nil {
		a.attempting = false
		a.mu.Unlock()
		reservation.refund()
		return
	}
	a.cancel(errPeerRPCDisconnected)
	applied := errors.Is(context.Cause(a.ctx), errPeerRPCDisconnected)
	a.attempting = false
	a.cancelled = applied
	a.mu.Unlock()
	if applied {
		reservation.commit()
	} else {
		reservation.refund()
	}
}

func (a *activeRPC) cancelForServer() {
	a.mu.Lock()
	if a.running {
		a.cancel(errServerRPCClosed)
	}
	a.mu.Unlock()
}

// reserve admits one cancellation attempt. The caller must either commit the
// reservation after cancellation reached the active request, or refund it when
// the request completed first. The budget lock is released before either action.
func (b *rpcCancelBudget) reserve(userID int64) (*rpcCancelReservation, bool) {
	if b == nil || userID < 0 {
		return nil, false
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	b.refillLocked(now)

	var generation uint64
	if userID > 0 {
		window, exists := b.users[userID]
		if exists && now.Before(window.until) {
			return nil, false
		}
		if !exists && len(b.users) >= b.maxUsers {
			b.pruneExpiredLocked(now)
			if len(b.users) >= b.maxUsers {
				return nil, false
			}
		}
	}
	if b.tokens < 1 {
		return nil, false
	}

	b.tokens--
	if userID > 0 {
		b.next++
		if b.next == 0 {
			b.next++
		}
		generation = b.next
		b.users[userID] = rpcUserCancelWindow{
			until:      now.Add(rpcCancelUserWindow),
			generation: generation,
		}
	}
	return &rpcCancelReservation{budget: b, userID: userID, generation: generation}, true
}

func (b *rpcCancelBudget) refillLocked(now time.Time) {
	if !now.After(b.refilled) {
		return
	}
	b.tokens = min(rpcCancelGlobalBurst, b.tokens+now.Sub(b.refilled).Seconds()*rpcCancelGlobalRate)
	b.refilled = now
}

func (b *rpcCancelBudget) pruneExpiredLocked(now time.Time) {
	for userID, window := range b.users {
		if !now.Before(window.until) {
			delete(b.users, userID)
		}
	}
}

func (r *rpcCancelReservation) commit() {
	if r == nil {
		return
	}
	r.once.Do(func() {})
}

func (r *rpcCancelReservation) refund() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		b := r.budget
		b.mu.Lock()
		defer b.mu.Unlock()

		b.refillLocked(b.now())
		b.tokens = min(rpcCancelGlobalBurst, b.tokens+1)
		if r.userID > 0 {
			if window, ok := b.users[r.userID]; ok && window.generation == r.generation {
				delete(b.users, r.userID)
			}
		}
	})
}
