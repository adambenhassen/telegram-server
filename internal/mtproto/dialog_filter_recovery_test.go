//nolint:testpackage // These tests exercise connection-private recovery and dependency state.
package mtproto

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"
)

func recoveryConn(owner, session int64) *Conn {
	conn := &Conn{clock: clock.System}
	conn.dialogFilterRecovery.Store(&dialogFilterRecovery{firstDifference: true})
	conn.recoveryBinding.Store(&recoveryBinding{})
	conn.setOwner(owner)
	conn.setSession(session)
	return conn
}

func TestDialogFilterRecoveryCoverageAndRetryBudget(t *testing.T) {
	conn := recoveryConn(7, 123)
	conn.EnsureDialogFilterRecoveryBinding(7, 123, 0)
	generation, covered, first, pending, ok := conn.DialogFilterRecoverySnapshot(7, 123, 0)
	if !ok || generation != 0 || covered != 0 || !first || pending {
		t.Fatalf("initial recovery = generation %d covered %d ok %v first %v pending %v", generation, covered, ok, first, pending)
	}
	if !conn.MarkDialogFilterRecovery(7, 123, 1, 0) {
		t.Fatal("owner invalidation was not marked")
	}
	generation, covered, first, pending, ok = conn.DialogFilterRecoverySnapshot(7, 123, 0)
	if !ok || generation != 1 || covered != 0 || !first || !pending {
		t.Fatalf("marked recovery = generation %d covered %d first %v pending %v ok %v", generation, covered, first, pending, ok)
	}
	now := time.Unix(1000, 0)
	_, claimed := conn.ClaimDialogFilterRecoveryAttempt(7, 123, 0, now)
	if claimed {
		t.Fatal("recovery push started before the first authorized difference")
	}
	if !conn.AcknowledgeDialogFilterDifference(7, 123, true) {
		t.Fatal("first difference was not acknowledged")
	}
	claimID, claimed := conn.ClaimDialogFilterRecoveryAttempt(7, 123, 0, now)
	if !claimed {
		t.Fatal("first recovery attempt was not claimable")
	}
	conn.FinishDialogFilterRecoveryAttempt(7, 123, claimID)
	_, claimed = conn.ClaimDialogFilterRecoveryAttempt(7, 123, 0, now.Add(19*time.Second))
	if claimed {
		t.Fatal("second recovery attempt started before 20 seconds")
	}
	claimID, claimed = conn.ClaimDialogFilterRecoveryAttempt(7, 123, 0, now.Add(20*time.Second))
	if !claimed {
		t.Fatal("second recovery attempt was not claimable at 20 seconds")
	}
	conn.FinishDialogFilterRecoveryAttempt(7, 123, claimID)
	claimID, claimed = conn.ClaimDialogFilterRecoveryAttempt(7, 123, 0, now.Add(60*time.Second))
	if !claimed {
		t.Fatal("third recovery attempt was not claimable at 60 seconds")
	}
	conn.FinishDialogFilterRecoveryAttempt(7, 123, claimID)
	_, claimed = conn.ClaimDialogFilterRecoveryAttempt(7, 123, 0, now.Add(120*time.Second))
	if claimed {
		t.Fatal("recovery exceeded its three-attempt cap")
	}
	if _, _, first, pending, _ := conn.DialogFilterRecoverySnapshot(7, 123, 0); first || !pending {
		t.Fatalf("capped recovery = first %v, pending %v; fetch must remain pending", first, pending)
	}
	if !conn.AcknowledgeDialogFilterFetch(7, 123, 1, 0) {
		t.Fatal("authoritative fetch did not acknowledge captured generation")
	}
	generation, covered, first, pending, ok = conn.DialogFilterRecoverySnapshot(7, 123, 0)
	if !ok || generation != 1 || covered != 1 || first || pending {
		t.Fatalf("fetch coverage = generation %d covered %d first %v pending %v ok %v", generation, covered, first, pending, ok)
	}
}

func TestDialogFilterRecoveryFetchKeepsNewerInvalidationAndResetsBinding(t *testing.T) {
	conn := recoveryConn(7, 123)
	conn.EnsureDialogFilterRecoveryBinding(7, 123, 0)
	conn.MarkDialogFilterRecovery(7, 123, 1, 0)
	if !conn.AcknowledgeDialogFilterFetch(7, 123, 1, 0) {
		t.Fatal("initial fetch did not acknowledge its captured generation")
	}
	if !conn.AcknowledgeDialogFilterDifference(7, 123, true) {
		t.Fatal("first authorized difference did not consume its binding signal")
	}
	conn.MarkDialogFilterRecovery(7, 123, 2, 0)
	if !conn.AcknowledgeDialogFilterFetch(7, 123, 1, 0) {
		t.Fatal("older in-flight fetch was not accepted")
	}
	generation, covered, first, pending, ok := conn.DialogFilterRecoverySnapshot(7, 123, 0)
	if !ok || generation != 2 || covered != 1 || first || !pending {
		t.Fatalf("racing fetch snapshot = generation %d, covered %d, first %v, pending %v, ok %v", generation, covered, first, pending, ok)
	}
	if conn.MarkDialogFilterRecovery(8, 123, 3, 0) {
		t.Fatal("foreign owner invalidation changed this connection")
	}
	conn.setOwner(8)
	conn.EnsureDialogFilterRecoveryBinding(8, 123, 3)
	_, covered, first, pending, ok = conn.DialogFilterRecoverySnapshot(8, 123, 0)
	if !ok || covered != 3 || !first || pending {
		t.Fatalf("rebound recovery = covered %d, first %v, pending %v, ok %v", covered, first, pending, ok)
	}
	oldGeneration, oldCovered, oldFirst, oldPending, oldOK := conn.DialogFilterRecoverySnapshot(7, 123, 0)
	if oldOK {
		t.Fatalf("previous owner's recovery state survived rebinding: generation %d covered %d first %v pending %v", oldGeneration, oldCovered, oldFirst, oldPending)
	}
}

func TestDialogFilterRecoveryFetchEnablesPushBeforeFirstDifference(t *testing.T) {
	conn := recoveryConn(7, 123)
	conn.EnsureDialogFilterRecoveryBinding(7, 123, 0)
	if !conn.AcknowledgeDialogFilterFetch(7, 123, 0, 0) {
		t.Fatal("folder fetch was not acknowledged")
	}
	if !conn.MarkDialogFilterRecovery(7, 123, 1, 0) {
		t.Fatal("later owner invalidation was not marked")
	}

	generation, covered, first, pending, ok := conn.DialogFilterRecoverySnapshot(7, 123, 0)
	if !ok || generation != 1 || covered != 0 || !first || !pending {
		t.Fatalf("post-fetch invalidation = generation %d covered %d first %v pending %v ok %v", generation, covered, first, pending, ok)
	}
	_, claimed := conn.ClaimDialogFilterRecoveryAttempt(7, 123, 0, time.Unix(1000, 0))
	if !claimed {
		t.Fatal("authorized folder fetch did not enable recovery before the first difference")
	}
	if _, _, first, _, _ := conn.DialogFilterRecoverySnapshot(7, 123, 0); !first {
		t.Fatal("recovery push consumed the independent first-difference signal")
	}
}

type recoveryPushTestTransport struct {
	writes int
}

func (t *recoveryPushTestTransport) Send(context.Context, *bin.Buffer) error {
	t.writes++
	return nil
}

func (*recoveryPushTestTransport) Recv(context.Context, *bin.Buffer) error { return io.EOF }
func (*recoveryPushTestTransport) Close() error                            { return nil }

func TestDialogFilterRecoveryClaimCannotWriteAfterSameOwnerSessionRebind(t *testing.T) {
	transport := &recoveryPushTestTransport{}
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	conn := NewTestConn(transport, raw.WithID())
	conn.setOwner(7)
	conn.EnsureDialogFilterRecoveryBinding(7, 123, 0)
	if !conn.MarkDialogFilterRecovery(7, 123, 1, 0) {
		t.Fatal("owner invalidation was not marked")
	}
	if !conn.AcknowledgeDialogFilterDifference(7, 123, true) {
		t.Fatal("first difference was not acknowledged")
	}

	claimer, ok := any(conn).(interface {
		ClaimDialogFilterRecoveryAttempt(int64, int64, uint64, time.Time) (uint64, bool)
	})
	if !ok {
		t.Fatal("recovery claim does not carry a write-fencing token")
	}
	claimID, claimed := claimer.ClaimDialogFilterRecoveryAttempt(7, 123, 0, time.Unix(1000, 0))
	if !claimed {
		t.Fatal("pending recovery was not claimed")
	}

	conn.setSession(124)
	pusher, ok := any(conn).(interface {
		PushDialogFilterRecovery(context.Context, int64, int64, uint64, bin.Encoder) (bool, error)
	})
	if !ok {
		t.Fatal("recovery writes cannot verify the captured binding and claim")
	}
	pushed, err := pusher.PushDialogFilterRecovery(context.Background(), 7, 123, claimID, &tg.UpdateDialogFilters{})
	if err != nil {
		t.Fatalf("stale recovery write: %v", err)
	}
	if pushed || transport.writes != 0 {
		t.Fatalf("stale session recovery reached the socket: pushed %v, writes %d", pushed, transport.writes)
	}
}

func TestDialogFilterRecoveryReconnectEpochAndNewBinding(t *testing.T) {
	conn := recoveryConn(7, 123)
	conn.EnsureDialogFilterRecoveryBinding(7, 123, 0)
	generation, covered, first, pending, ok := conn.DialogFilterRecoverySnapshot(7, 123, 1)
	if !ok || !pending {
		t.Fatalf("listener epoch snapshot = generation %d covered %d first %v pending %v ok %v", generation, covered, first, pending, ok)
	}
	newConn := recoveryConn(7, 456)
	newConn.EnsureDialogFilterRecoveryBinding(7, 456, 1)
	_, covered, first, pending, _ = newConn.DialogFilterRecoverySnapshot(7, 456, 1)
	if covered != 1 || !first || pending {
		t.Fatalf("new binding inherited old epoch: covered %d, first %v, pending %v", covered, first, pending)
	}
}

func TestRPCDependencyOutcomesAreBoundedAndResetWithSession(t *testing.T) {
	conn := recoveryConn(7, 123)
	for id := int64(1); id <= 65; id++ {
		conn.recordRPCOutcome(id, 123, id%2 == 0)
	}
	if found, _ := conn.RPCDependencyOutcome(123, 1, 100); found {
		t.Fatal("dependency older than the 64-result ring was retained")
	}
	if found, success := conn.RPCDependencyOutcome(123, 2, 100); !found || !success {
		t.Fatalf("recent dependency result = found %v, success %v", found, success)
	}
	if found, _ := conn.RPCDependencyOutcome(124, 2, 100); found {
		t.Fatal("dependency result crossed into another session")
	}
	conn.setSession(124)
	if found, _ := conn.RPCDependencyOutcome(124, 2, 100); found {
		t.Fatal("prior-session outcomes survived a session rebind")
	}
}

func TestUnpackInvokeAfterMsgRequiresSuccessfulSameSessionDependency(t *testing.T) {
	const sessionID = 123
	conn := recoveryConn(7, sessionID)
	called := false
	var refusal InvokeAfterMsgRefusal
	var refusalVerdict UnimplementedVerdict
	handler := UnpackInvokeWithAfterMsg(HandlerFunc(func(_ *Conn, req *Request) error {
		called = true
		if req.MsgID != 104 {
			t.Errorf("inner request message id = %d, want outer id 104", req.MsgID)
		}
		id, err := req.Buf.PeekID()
		if err != nil || id != tg.HelpGetConfigRequestTypeID {
			t.Errorf("inner request id = %#x, err %v", id, err)
		}
		return nil
	}), func(_ *Conn, _ *Request, _ uint32, got InvokeAfterMsgRefusal, verdict UnimplementedVerdict) error {
		refusal, refusalVerdict = got, verdict
		return nil
	})
	invoke := func(dependency int64, query bin.Object) *Request {
		var body bin.Buffer
		if err := (&tg.InvokeAfterMsgRequest{MsgID: dependency, Query: query}).Encode(&body); err != nil {
			t.Fatalf("encode invokeAfterMsg: %v", err)
		}
		return &Request{SessionID: sessionID, MsgID: 104, Buf: &body}
	}

	conn.recordRPCOutcome(100, sessionID, true)
	if err := handler.OnMessage(conn, invoke(100, &tg.HelpGetConfigRequest{})); err != nil {
		t.Fatalf("successful dependency: %v", err)
	}
	if !called {
		t.Fatal("successful dependency did not execute its inner request")
	}

	called = false
	conn.recordRPCOutcome(101, sessionID, false)
	if err := handler.OnMessage(conn, invoke(101, &tg.HelpGetConfigRequest{})); err != nil {
		t.Fatalf("failed dependency refusal: %v", err)
	}
	if called || refusal != InvokeAfterMsgWaitFailed {
		t.Fatalf("failed dependency executed=%v, refusal=%v", called, refusal)
	}

	refusal = 0
	if err := handler.OnMessage(conn, invoke(99, &tg.HelpGetConfigRequest{})); err != nil {
		t.Fatalf("unknown dependency refusal: %v", err)
	}
	if called || refusal != InvokeAfterMsgWaitTimeout || refusalVerdict != UnimplementedAnswer {
		t.Fatalf("unknown dependency executed=%v, refusal=%v, verdict=%v", called, refusal, refusalVerdict)
	}

	refusal = 0
	if err := handler.OnMessage(conn, invoke(104, &tg.HelpGetConfigRequest{})); err != nil {
		t.Fatalf("self dependency refusal: %v", err)
	}
	if called || refusal != InvokeAfterMsgWaitTimeout {
		t.Fatalf("self dependency executed=%v, refusal=%v", called, refusal)
	}
	refusal = 0
	if err := handler.OnMessage(conn, invoke(105, &tg.HelpGetConfigRequest{})); err != nil {
		t.Fatalf("forward dependency refusal: %v", err)
	}
	if called || refusal != InvokeAfterMsgWaitTimeout {
		t.Fatalf("forward dependency executed=%v, refusal=%v", called, refusal)
	}
	refusal = 0
	crossSession := invoke(100, &tg.HelpGetConfigRequest{})
	crossSession.SessionID = sessionID + 1
	if err := handler.OnMessage(conn, crossSession); err != nil {
		t.Fatalf("cross-session dependency refusal: %v", err)
	}
	if called || refusal != InvokeAfterMsgWaitTimeout {
		t.Fatalf("cross-session dependency executed=%v, refusal=%v", called, refusal)
	}

	refusal = 0
	if err := handler.OnMessage(conn, invoke(100, &tg.InvokeWithLayerRequest{Layer: 1, Query: &tg.HelpGetConfigRequest{}})); err != nil {
		t.Fatalf("nested wrapper refusal: %v", err)
	}
	if called || refusal != InvokeAfterMsgWaitTimeout {
		t.Fatalf("nested wrapper executed=%v, refusal=%v", called, refusal)
	}
	refusal = 0
	if err := handler.OnMessage(conn, invoke(100, &tg.InvokeAfterMsgRequest{MsgID: 100, Query: &tg.HelpGetConfigRequest{}})); err != nil {
		t.Fatalf("nested invokeAfterMsg refusal: %v", err)
	}
	if called || refusal != InvokeAfterMsgWaitTimeout {
		t.Fatalf("nested invokeAfterMsg executed=%v, refusal=%v", called, refusal)
	}

	var afterMsgsBody bin.Buffer
	if err := (&tg.InvokeAfterMsgsRequest{MsgIDs: []int64{100}, Query: &tg.HelpGetConfigRequest{}}).Encode(&afterMsgsBody); err != nil {
		t.Fatalf("encode invokeAfterMsgs: %v", err)
	}
	refusal = 0
	if err := handler.OnMessage(conn, &Request{SessionID: sessionID, MsgID: 108, Buf: &afterMsgsBody}); err != nil {
		t.Fatalf("unsupported invokeAfterMsgs refusal: %v", err)
	}
	if called || refusal != InvokeAfterMsgWaitTimeout {
		t.Fatalf("invokeAfterMsgs executed=%v, refusal=%v", called, refusal)
	}
}
