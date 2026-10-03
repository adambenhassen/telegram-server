package mtproto

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
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

// RPCCancelOutcome is the closed vocabulary recorded for one observed peer
// disconnect and its active request.
type RPCCancelOutcome uint32

const (
	RPCCancelOutcomeNotObserved RPCCancelOutcome = iota
	RPCCancelOutcomeApplied
	RPCCancelOutcomeDeniedPerUser
	RPCCancelOutcomeDeniedGlobal
	RPCCancelOutcomeDeniedCapacity
	RPCCancelOutcomeAlreadyFinished
)

// RPCCancelDiagnosticSnapshot contains only fixed request and cancellation
// state. It carries no connection, user, or request identifier.
type RPCCancelDiagnosticSnapshot struct {
	SendMessage             bool
	SendMessageStoreStarted bool
	DisconnectObserved      bool
	Outcome                 RPCCancelOutcome
}

// RPCCancelDiagnosticHandle is an opaque, bounded handle to one active RPC's
// fixed diagnostic state. It exists only when the server's test diagnostic is
// enabled.
type RPCCancelDiagnosticHandle struct {
	diagnostic *rpcCancelDiagnostic
}

// Snapshot returns the fixed diagnostic fields for this RPC.
func (h *RPCCancelDiagnosticHandle) Snapshot() RPCCancelDiagnosticSnapshot {
	if h == nil || h.diagnostic == nil {
		return RPCCancelDiagnosticSnapshot{Outcome: RPCCancelOutcomeNotObserved}
	}
	d := h.diagnostic
	return RPCCancelDiagnosticSnapshot{
		SendMessage:             d.sendMessage.Load(),
		SendMessageStoreStarted: d.sendMessageStoreStarted.Load(),
		DisconnectObserved:      d.disconnectObserved.Load(),
		Outcome:                 RPCCancelOutcome(d.outcome.Load()),
	}
}

// SendMessageBackendForTesting returns the opaque backend handle captured by
// this RPC's SendMessage transaction.
func (h *RPCCancelDiagnosticHandle) SendMessageBackendForTesting() *store.SendMessageDiagnosticForTesting {
	if h == nil || h.diagnostic == nil {
		return nil
	}
	return h.diagnostic.sendMessageBackend.Load()
}

type rpcCancelDiagnostic struct {
	sendMessage             atomic.Bool
	sendMessageStoreStarted atomic.Bool
	disconnectObserved      atomic.Bool
	outcome                 atomic.Uint32
	sendMessageBackend      atomic.Pointer[store.SendMessageDiagnosticForTesting]
	provider                *rpcCancelDiagnosticProviderForTesting
}

type rpcCancelDiagnosticProviderForTesting struct {
	sendMessageBackend atomic.Pointer[store.SendMessageDiagnosticForTesting]
}

func newRPCCancelDiagnosticProviderForTesting(backend *store.SendMessageDiagnosticForTesting) *rpcCancelDiagnosticProviderForTesting {
	if backend == nil {
		return nil
	}
	provider := &rpcCancelDiagnosticProviderForTesting{}
	provider.sendMessageBackend.Store(backend)
	return provider
}

func (d *rpcCancelDiagnostic) claimSendMessageBackend() *store.SendMessageDiagnosticForTesting {
	if d == nil || d.provider == nil {
		return nil
	}
	backend := d.provider.sendMessageBackend.Swap(nil)
	if backend != nil {
		d.sendMessageBackend.Store(backend)
	}
	return backend
}

type rpcCancelDiagnosticContextKey struct{}

func rpcCancelDiagnosticFromContext(ctx context.Context) *rpcCancelDiagnostic {
	if ctx == nil {
		return nil
	}
	diagnostic, _ := ctx.Value(rpcCancelDiagnosticContextKey{}).(*rpcCancelDiagnostic)
	return diagnostic
}

func (d *rpcCancelDiagnostic) setOutcome(outcome RPCCancelOutcome) {
	if d != nil && outcome != RPCCancelOutcomeNotObserved {
		d.outcome.CompareAndSwap(uint32(RPCCancelOutcomeNotObserved), uint32(outcome))
	}
}

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

	budget             *rpcCancelBudget
	changed            chan struct{}
	generation         uint64
	peerGone           bool
	serverClose        bool
	diagnosticsEnabled bool
	lastDiagnostic     *rpcCancelDiagnostic
	diagnosticProvider *rpcCancelDiagnosticProviderForTesting
	active             *activeRPC
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
	diagnostic *rpcCancelDiagnostic
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

func newPeerRPCStateWithDiagnostics(budget *rpcCancelBudget, enabled bool) *peerRPCState {
	state := newPeerRPCState(budget)
	state.diagnosticsEnabled = enabled
	return state
}

func newPeerRPCStateWithDiagnosticProvider(budget *rpcCancelBudget, provider *rpcCancelDiagnosticProviderForTesting) *peerRPCState {
	state := newPeerRPCStateWithDiagnostics(budget, provider != nil)
	state.diagnosticProvider = provider
	return state
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
	var diagnostic *rpcCancelDiagnostic
	if p.diagnosticsEnabled {
		diagnostic = &rpcCancelDiagnostic{provider: p.diagnosticProvider}
		ctx = context.WithValue(ctx, rpcCancelDiagnosticContextKey{}, diagnostic)
	}
	active := &activeRPC{
		ctx:        ctx,
		cancel:     cancel,
		userID:     userID,
		running:    true,
		budget:     p.budget,
		diagnostic: diagnostic,
	}
	p.active = active
	p.lastDiagnostic = diagnostic
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
	lastDiagnostic := p.lastDiagnostic
	p.mu.Unlock()
	if active == nil {
		if !serverClose && lastDiagnostic != nil {
			lastDiagnostic.disconnectObserved.Store(true)
			lastDiagnostic.setOutcome(RPCCancelOutcomeAlreadyFinished)
		}
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

func (p *peerRPCState) currentDiagnostic() *RPCCancelDiagnosticHandle {
	p.mu.Lock()
	diagnostic := p.lastDiagnostic
	if p.active != nil {
		diagnostic = p.active.diagnostic
	}
	p.mu.Unlock()
	if diagnostic == nil {
		return nil
	}
	return &RPCCancelDiagnosticHandle{diagnostic: diagnostic}
}

func (p *peerRPCState) diagnosticSnapshot() RPCCancelDiagnosticSnapshot {
	return p.currentDiagnostic().Snapshot()
}

func (a *activeRPC) cancelForPeer() {
	if a.diagnostic != nil {
		a.diagnostic.disconnectObserved.Store(true)
	}
	a.mu.Lock()
	if !a.running || a.attempting || a.cancelled || a.ctx.Err() != nil {
		if !a.attempting && !a.cancelled && a.diagnostic != nil {
			a.diagnostic.setOutcome(RPCCancelOutcomeAlreadyFinished)
		}
		a.mu.Unlock()
		return
	}
	a.attempting = true
	a.mu.Unlock()

	reservation, denial := a.budget.reserveForPeer(a.userID)
	if reservation == nil {
		a.mu.Lock()
		a.attempting = false
		if a.diagnostic != nil {
			if !a.running || a.ctx.Err() != nil {
				a.diagnostic.setOutcome(RPCCancelOutcomeAlreadyFinished)
			} else {
				a.diagnostic.setOutcome(denial)
			}
		}
		a.mu.Unlock()
		return
	}

	a.mu.Lock()
	if !a.running || a.ctx.Err() != nil {
		a.attempting = false
		if a.diagnostic != nil {
			a.diagnostic.setOutcome(RPCCancelOutcomeAlreadyFinished)
		}
		a.mu.Unlock()
		reservation.refund()
		return
	}
	a.cancel(errPeerRPCDisconnected)
	applied := errors.Is(context.Cause(a.ctx), errPeerRPCDisconnected)
	a.attempting = false
	a.cancelled = applied
	if a.diagnostic != nil {
		if applied {
			a.diagnostic.setOutcome(RPCCancelOutcomeApplied)
		} else {
			a.diagnostic.setOutcome(RPCCancelOutcomeAlreadyFinished)
		}
	}
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
	reservation, _ := b.reserveForPeer(userID)
	return reservation, reservation != nil
}

func (b *rpcCancelBudget) reserveForPeer(userID int64) (*rpcCancelReservation, RPCCancelOutcome) {
	if b == nil || userID < 0 {
		return nil, RPCCancelOutcomeDeniedCapacity
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.now()
	b.refillLocked(now)

	var generation uint64
	if userID > 0 {
		window, exists := b.users[userID]
		if exists && now.Before(window.until) {
			return nil, RPCCancelOutcomeDeniedPerUser
		}
		if !exists && len(b.users) >= b.maxUsers {
			b.pruneExpiredLocked(now)
			if len(b.users) >= b.maxUsers {
				return nil, RPCCancelOutcomeDeniedCapacity
			}
		}
	}
	if b.tokens < 1 {
		return nil, RPCCancelOutcomeDeniedGlobal
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
	return &rpcCancelReservation{budget: b, userID: userID, generation: generation}, RPCCancelOutcomeNotObserved
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
