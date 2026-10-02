package mtproto

import (
	"context"
	"sync"
	"testing"
	"time"
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

	firstCancelled := context.Cause(firstCtx) == errPeerRPCDisconnected
	secondCancelled := context.Cause(secondCtx) == errPeerRPCDisconnected
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
	for userID := int64(1); userID <= 2; userID++ {
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

	if got := context.Cause(ctx); got != context.Canceled {
		t.Fatalf("completed RPC cause = %v, want completion cancellation", got)
	}
	if got := len(budget.users); got != 0 {
		t.Fatalf("completed disconnect allocated %d user allowance entries", got)
	}
	if budget.tokens != rpcCancelGlobalBurst {
		t.Fatalf("completed disconnect left %.2f global tokens, want %.2f", budget.tokens, rpcCancelGlobalBurst)
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

	if got := context.Cause(ctx); got != errServerRPCClosed {
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
	if got := context.Cause(ctx); got != errServerRPCClosed {
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

	for i := 0; i < 100; i++ {
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

		if context.Cause(ctx) == errPeerRPCDisconnected {
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
