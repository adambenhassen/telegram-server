package mtproto

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestPeerRPCStateChargesOneCancellationAcrossConnections(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_300, 0)
	budget := newRPCCancelBudget(func() time.Time { return now }, 8)
	first := newPeerRPCState(budget)
	second := newPeerRPCState(budget)
	firstCtx, finishFirst, started := first.begin(61, context.Background())
	if !started {
		t.Fatal("first RPC did not start")
	}
	secondCtx, finishSecond, started := second.begin(61, context.Background())
	if !started {
		t.Fatal("second RPC did not start")
	}

	first.peerDisconnected()
	second.peerDisconnected()
	finishFirst()
	finishSecond()

	firstCancelled := errors.Is(context.Cause(firstCtx), errPeerRPCDisconnected)
	secondCancelled := errors.Is(context.Cause(secondCtx), errPeerRPCDisconnected)
	if firstCancelled == secondCancelled {
		t.Fatalf("same-user RPC cancellations = %t/%t, want exactly one allowance", firstCancelled, secondCancelled)
	}
	if got := len(budget.users); got != 1 {
		t.Fatalf("charged user states = %d, want one shared user allowance", got)
	}
}

func TestPeerDisconnectWithoutBudgetStopsQueuedRPCButKeepsActiveRPC(t *testing.T) {
	t.Parallel()

	budget := newRPCCancelBudget(time.Now, 8)
	for i := range 2 {
		userID := int64(i + 1)
		state := newPeerRPCState(budget)
		_, finish, started := state.begin(userID, context.Background())
		if !started {
			t.Fatalf("budget-consuming RPC for user %d did not start", userID)
		}
		state.peerDisconnected()
		finish()
	}

	state := newPeerRPCState(budget)
	activeCtx, finish, started := state.begin(3, context.Background())
	if !started {
		t.Fatal("third RPC did not start")
	}
	state.peerDisconnected()
	if got := context.Cause(activeCtx); got != nil {
		t.Fatalf("budget-denied peer disconnect canceled active RPC with %v", got)
	}
	if _, _, started := state.begin(3, context.Background()); started {
		t.Fatal("peer disconnect allowed a queued RPC to start")
	}
	finish()
}

func TestCompletedRPCDisconnectDoesNotConsumeBudget(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_400, 0)
	budget := newRPCCancelBudget(func() time.Time { return now }, 8)
	state := newPeerRPCState(budget)
	ctx, finish, started := state.begin(71, context.Background())
	if !started {
		t.Fatal("RPC did not start")
	}
	finish()
	state.peerDisconnected()

	if got := context.Cause(ctx); !errors.Is(got, context.Canceled) {
		t.Fatalf("completed RPC cause = %v, want completion cancellation", got)
	}
	if got := len(budget.users); got != 0 {
		t.Fatalf("completed disconnect allocated %d user allowance entries", got)
	}
	if budget.tokens != rpcCancelGlobalBurst {
		t.Fatalf("completed disconnect left %.2f global tokens, want %.2f", budget.tokens, rpcCancelGlobalBurst)
	}
}

func TestPeerRPCCancellationDiagnosticUsesFixedOutcomes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(*testing.T) (*peerRPCState, func())
		want    RPCCancelOutcome
	}{
		{
			name: "applied",
			prepare: func(t *testing.T) (*peerRPCState, func()) {
				state := newPeerRPCStateWithDiagnostics(newRPCCancelBudget(time.Now, 8), true)
				_, finish, started := state.begin(1, context.Background())
				if !started {
					t.Fatal("RPC did not start")
				}
				return state, finish
			},
			want: RPCCancelOutcomeApplied,
		},
		{
			name: "denied per user",
			prepare: func(t *testing.T) (*peerRPCState, func()) {
				now := time.Unix(1_800_000_600, 0)
				budget := newRPCCancelBudget(func() time.Time { return now }, 8)
				reservation, ok := budget.reserve(2)
				if !ok {
					t.Fatal("initial reservation was denied")
				}
				reservation.commit()
				state := newPeerRPCStateWithDiagnostics(budget, true)
				_, finish, started := state.begin(2, context.Background())
				if !started {
					t.Fatal("RPC did not start")
				}
				return state, finish
			},
			want: RPCCancelOutcomeDeniedPerUser,
		},
		{
			name: "denied globally",
			prepare: func(t *testing.T) (*peerRPCState, func()) {
				now := time.Unix(1_800_000_600, 0)
				budget := newRPCCancelBudget(func() time.Time { return now }, 8)
				for _, userID := range []int64{3, 4} {
					reservation, ok := budget.reserve(userID)
					if !ok {
						t.Fatal("initial reservation was denied")
					}
					reservation.commit()
				}
				state := newPeerRPCStateWithDiagnostics(budget, true)
				_, finish, started := state.begin(5, context.Background())
				if !started {
					t.Fatal("RPC did not start")
				}
				return state, finish
			},
			want: RPCCancelOutcomeDeniedGlobal,
		},
		{
			name: "denied at capacity",
			prepare: func(t *testing.T) (*peerRPCState, func()) {
				now := time.Unix(1_800_000_600, 0)
				budget := newRPCCancelBudget(func() time.Time { return now }, 1)
				reservation, ok := budget.reserve(6)
				if !ok {
					t.Fatal("initial reservation was denied")
				}
				reservation.commit()
				state := newPeerRPCStateWithDiagnostics(budget, true)
				_, finish, started := state.begin(7, context.Background())
				if !started {
					t.Fatal("RPC did not start")
				}
				return state, finish
			},
			want: RPCCancelOutcomeDeniedCapacity,
		},
		{
			name: "already finished",
			prepare: func(t *testing.T) (*peerRPCState, func()) {
				state := newPeerRPCStateWithDiagnostics(newRPCCancelBudget(time.Now, 8), true)
				_, finish, started := state.begin(8, context.Background())
				if !started {
					t.Fatal("RPC did not start")
				}
				finish()
				return state, func() {}
			},
			want: RPCCancelOutcomeAlreadyFinished,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state, finish := tt.prepare(t)
			defer finish()

			state.peerDisconnected()
			got := state.diagnosticSnapshot()
			if !got.DisconnectObserved {
				t.Fatal("diagnostic did not record the peer disconnect")
			}
			if got.Outcome != tt.want {
				t.Fatalf("diagnostic outcome = %v, want %v", got.Outcome, tt.want)
			}
		})
	}
}

func TestPeerRPCCancellationDiagnosticStartsNotObserved(t *testing.T) {
	t.Parallel()

	state := newPeerRPCStateWithDiagnostics(newRPCCancelBudget(time.Now, 8), true)
	_, finish, started := state.begin(9, context.Background())
	if !started {
		t.Fatal("RPC did not start")
	}
	defer finish()

	got := state.diagnosticSnapshot()
	if got.DisconnectObserved || got.Outcome != RPCCancelOutcomeNotObserved {
		t.Fatalf("initial diagnostic = %+v, want no disconnect and not-observed", got)
	}
}

func TestRequestDiagnosticMarksOnlySendMessageStorageBoundary(t *testing.T) {
	t.Parallel()

	diagnostic := &rpcCancelDiagnostic{}
	req := &Request{rpcMethod: "messages.getHistory", rpcCancelDiagnostic: diagnostic}
	req.MarkSendMessageStoreCallForTesting()
	if diagnostic.sendMessageStoreStarted.Load() {
		t.Fatal("non-send request marked the sendMessage storage boundary")
	}

	req.rpcMethod = "messages.sendMessage"
	req.MarkSendMessageStoreCallForTesting()
	if !diagnostic.sendMessageStoreStarted.Load() {
		t.Fatal("sendMessage request did not mark its storage boundary")
	}
}

func TestRPCCancelDiagnosticProviderClaimsOneTestOwnedHandle(t *testing.T) {
	t.Parallel()

	backend := store.NewSendMessageDiagnosticForTesting()
	provider := newRPCCancelDiagnosticProviderForTesting(backend)
	first := &rpcCancelDiagnostic{provider: provider}
	second := &rpcCancelDiagnostic{provider: provider}

	if got := first.claimSendMessageBackend(); got != backend {
		t.Fatal("first send did not claim the test-owned backend handle")
	}
	if got := second.claimSendMessageBackend(); got != nil {
		t.Fatal("a second send claimed the already-consumed backend handle")
	}
	if got := first.sendMessageBackend.Load(); got != backend {
		t.Fatal("claimed handle was not retained by its RPC diagnostic")
	}
}

func TestServerCloseCancelsWithoutPeerBudget(t *testing.T) {
	t.Parallel()

	budget := newRPCCancelBudget(time.Now, 8)
	state := newPeerRPCState(budget)
	ctx, finish, started := state.begin(72, context.Background())
	if !started {
		t.Fatal("RPC did not start")
	}
	state.serverClosed()
	state.peerDisconnected()
	finish()

	if got := context.Cause(ctx); !errors.Is(got, errServerRPCClosed) {
		t.Fatalf("server close cause = %v, want server-close sentinel", got)
	}
	if got := len(budget.users); got != 0 {
		t.Fatalf("server close charged %d user allowances", got)
	}
	if budget.tokens != rpcCancelGlobalBurst {
		t.Fatalf("server close left %.2f global tokens, want %.2f", budget.tokens, rpcCancelGlobalBurst)
	}
}

func TestConnCloseMarksServerProvenanceBeforeTransportClose(t *testing.T) {
	t.Parallel()

	budget := newRPCCancelBudget(time.Now, 8)
	state := newPeerRPCState(budget)
	ctx, finish, started := state.begin(73, context.Background())
	if !started {
		t.Fatal("RPC did not start")
	}
	defer finish()

	transport := &serverCloseProvenanceTransport{rpcFrameTestConn: newRPCFrameTestConn(), state: state}
	conn := &Conn{transport: transport}
	conn.peerRPC.Store(state)
	if err := conn.Close(); err != nil {
		t.Fatalf("close connection: %v", err)
	}
	if got := context.Cause(ctx); !errors.Is(got, errServerRPCClosed) {
		t.Fatalf("close cause = %v, want server-close sentinel", got)
	}
	if got := len(budget.users); got != 0 {
		t.Fatalf("server close charged %d user allowances", got)
	}
	if budget.tokens != rpcCancelGlobalBurst {
		t.Fatalf("server close left %.2f global tokens, want %.2f", budget.tokens, rpcCancelGlobalBurst)
	}
}

type serverCloseProvenanceTransport struct {
	*rpcFrameTestConn

	state *peerRPCState
}

func (c *serverCloseProvenanceTransport) Close() error {
	c.state.peerDisconnected()
	return nil
}

func TestPeerCancellationCompletionRaceRefundsUnappliedAllowance(t *testing.T) {
	t.Parallel()

	for i := range 100 {
		now := time.Unix(1_800_000_500, 0)
		budget := newRPCCancelBudget(func() time.Time { return now }, 8)
		state := newPeerRPCState(budget)
		parent, cancelParent := context.WithCancel(context.Background())
		ctx, finish, started := state.begin(81, parent)
		if !started {
			t.Fatal("RPC did not start")
		}

		var race sync.WaitGroup
		race.Add(2)
		go func() {
			defer race.Done()
			state.peerDisconnected()
		}()
		go func() {
			defer race.Done()
			cancelParent()
		}()
		finish()
		race.Wait()

		if errors.Is(context.Cause(ctx), errPeerRPCDisconnected) {
			if got := len(budget.users); got != 1 {
				t.Fatalf("iteration %d applied peer cancellation with %d user entries", i, got)
			}
		} else if got := len(budget.users); got != 0 {
			t.Fatalf("iteration %d charged a cancellation that did not win: %d user entries", i, got)
		}
	}
}

func TestRPCCancelBudgetSharesUserWindowAndRefillsGlobalTokens(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_000, 0)
	budget := newRPCCancelBudget(func() time.Time { return now }, 8)

	first, ok := budget.reserve(41)
	if !ok {
		t.Fatal("first authenticated cancellation was denied")
	}
	first.commit()
	if _, ok := budget.reserve(41); ok {
		t.Fatal("second connection for the same user received a fresh allowance")
	}

	second, ok := budget.reserve(42)
	if !ok {
		t.Fatal("another authenticated user was denied the second global token")
	}
	second.commit()
	if _, ok := budget.reserve(43); ok {
		t.Fatal("global burst admitted a third cancellation")
	}

	now = now.Add(500 * time.Millisecond)
	refilled, ok := budget.reserve(43)
	if !ok {
		t.Fatal("global bucket did not refill one token after half a second")
	}
	refilled.commit()

	now = time.Unix(1_800_000_010, 0)
	afterWindow, ok := budget.reserve(41)
	if !ok {
		t.Fatal("user allowance did not expire at the ten-second boundary")
	}
	afterWindow.commit()
}

func TestRPCCancelBudgetRefundsUnappliedCancellationAndBoundsState(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_100, 0)
	budget := newRPCCancelBudget(func() time.Time { return now }, 1)

	unapplied, ok := budget.reserve(51)
	if !ok {
		t.Fatal("first reservation was denied")
	}
	if _, ok := budget.reserve(52); ok {
		t.Fatal("bounded user state admitted a second unexpired user")
	}
	unapplied.refund()

	applied, ok := budget.reserve(52)
	if !ok {
		t.Fatal("refunding an unapplied reservation did not restore the allowance")
	}
	applied.commit()
	if got := len(budget.users); got != 1 {
		t.Fatalf("user allowance state has %d entries, want the configured bound of 1", got)
	}

	now = now.Add(10 * time.Second)
	reused, ok := budget.reserve(51)
	if !ok {
		t.Fatal("expired user state was not reclaimed at capacity")
	}
	reused.commit()
	if got := len(budget.users); got != 1 {
		t.Fatalf("user allowance state grew to %d entries past its bound", got)
	}
}

func TestRPCCancelBudgetChargesPreLoginOnlyToGlobalBucket(t *testing.T) {
	t.Parallel()

	now := time.Unix(1_800_000_200, 0)
	budget := newRPCCancelBudget(func() time.Time { return now }, 1)
	for range 2 {
		reservation, ok := budget.reserve(0)
		if !ok {
			t.Fatal("pre-login cancellation was denied inside the global burst")
		}
		reservation.commit()
	}
	if _, ok := budget.reserve(0); ok {
		t.Fatal("pre-login cancellation bypassed the global bucket")
	}
	if got := len(budget.users); got != 0 {
		t.Fatalf("pre-login cancellation allocated per-user state: %d entries", got)
	}
}

type rpcPushFailureTransport struct {
	closed chan struct{}
	once   sync.Once
}

func (*rpcPushFailureTransport) Send(context.Context, *bin.Buffer) error { return io.ErrClosedPipe }

func (c *rpcPushFailureTransport) Recv(ctx context.Context, _ *bin.Buffer) error {
	select {
	case <-c.closed:
		return net.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *rpcPushFailureTransport) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestPushWriteFailureUsesPeerCancellationBudget(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"ordinary", "watermark", "recovery"} {
		for _, exhausted := range []bool{false, true} {
			name := path + "/available"
			if exhausted {
				name = path + "/exhausted"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				now := time.Unix(1_800_000_600, 0)
				budget := newRPCCancelBudget(func() time.Time { return now }, 8)
				if exhausted {
					reservation, ok := budget.reserve(7)
					if !ok {
						t.Fatal("could not consume initial user allowance")
					}
					reservation.commit()
				}
				state := newPeerRPCState(budget)
				activeCtx, finish, started := state.begin(7, t.Context())
				if !started {
					t.Fatal("active RPC did not start")
				}
				defer finish()
				transport := &rpcPushFailureTransport{closed: make(chan struct{})}
				var raw crypto.Key
				for i := range raw {
					raw[i] = byte(i)
				}
				conn := NewTestConn(transport, raw.WithID())
				conn.setOwner(7)
				conn.setSession(42)
				conn.peerRPC.Store(state)
				reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, t.Context(), peerReadTransport{Conn: transport}, state, nil)
				defer func() {
					reader.stop()
					reader.join()
				}()

				var wrote bool
				var err error
				switch path {
				case "ordinary":
					wrote, err = conn.PushTo(t.Context(), 7, &mt.Pong{PingID: 1}, 1)
				case "watermark":
					wrote, _, err = conn.PushToAtWatermark(t.Context(), 7, 0, &mt.Pong{PingID: 1}, 1)
				case "recovery":
					conn.EnsureDialogFilterRecoveryBinding(7, 42, 0)
					conn.MarkDialogFilterRecovery(7, 42, 1, 0)
					conn.AcknowledgeDialogFilterDifference(7, 42, true)
					claimID, claimed := conn.ClaimDialogFilterRecoveryAttempt(7, 42, 0, now)
					if !claimed {
						t.Fatal("recovery push was not claimable")
					}
					wrote, err = conn.PushDialogFilterRecovery(t.Context(), 7, 42, claimID, &mt.Pong{PingID: 1})
				}
				if wrote || !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("failed push = wrote %t, err %v", wrote, err)
				}
				if _, err, _ := reader.next(); !errors.Is(err, net.ErrClosed) {
					t.Fatalf("read after push failure = %v, want closed", err)
				}
				if state.isServerClosed() {
					t.Fatal("failed peer push acquired server-close exemption")
				}
				if exhausted {
					if activeCtx.Err() != nil {
						t.Fatalf("exhausted peer allowance canceled active RPC: %v", context.Cause(activeCtx))
					}
				} else if !errors.Is(context.Cause(activeCtx), errPeerRPCDisconnected) {
					t.Fatalf("push cancellation = %v, want peer cancellation", context.Cause(activeCtx))
				}
				if budget.tokens != 1 || len(budget.users) != 1 {
					t.Fatalf("push budget = %.0f tokens, %d users; want one charge", budget.tokens, len(budget.users))
				}
			})
		}
	}
}
