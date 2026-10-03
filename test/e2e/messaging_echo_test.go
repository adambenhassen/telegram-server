package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/telegram/updates/hook"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/rsakey"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestMessagingSenderSessionEchoSuppression(t *testing.T) {
	t.Parallel()
	deadlineCtx, cancelDeadline := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancelDeadline)
	deadlineCtx = withRegistrySnapshotState(deadlineCtx)
	ctx, cancelFailure := context.WithCancelCause(deadlineCtx)
	t.Cleanup(func() { cancelFailure(nil) })
	failures := newClientFailureSignal(cancelFailure)

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	registry, stop := bootServerWithRegistry(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	type clientUpdates struct {
		manager *updates.Manager
		pushes  *updateCollector
	}
	newClient := func(updates clientUpdates, sess *session.StorageMemory) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:             dcID,
			DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:       dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: sess,
			UpdateHandler:  observedManagerHandler{observer: updates.pushes, manager: updates.manager},
			Middlewares: []telegram.Middleware{
				hook.UpdateHook(updates.manager.Handle),
				hook.AffectedHook(updates.manager),
			},
		})
	}
	flowFor := func(phone string) auth.Flow {
		return auth.NewFlow(
			auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
				func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
					return codes.wait(ctx, phone)
				})),
			auth.SendCodeOptions{},
		)
	}

	const phoneA, phoneB, phoneC = "+15551282501", "+15551282502", "+15551282503"
	seedPhoneUsers(t, ctx, st, phoneA, phoneB, phoneC)
	seedA, ok, err := st.UserByPhone(ctx, phoneA)
	if err != nil || !ok {
		t.Fatalf("look up A seed user: found=%v err=%v", ok, err)
	}
	seedMessage, seedPts, _, _, err := st.SendMessage(ctx, seedA.ID, seedA.ID, "A saved id seed", 82500, 0, 0)
	if err != nil {
		t.Fatalf("seed A saved message: %v", err)
	}
	if seedPts != 1 || seedMessage.LocalID != 1 {
		t.Fatalf("A seed message = id %d pts %d, want id 1 pts 1", seedMessage.LocalID, seedPts)
	}

	type runningClient struct {
		cmds      chan command
		ready     chan struct{}
		idCh      chan int64
		id        int64
		manager   *updates.Manager
		lifecycle *clientLifecycle
		options   clientRunOptions
		stop      sync.Once
	}
	var stopClient func(*runningClient)
	launchClient := func(label, phone string, pushes *updateCollector, manager *updates.Manager, sess *session.StorageMemory, forget bool, options ...clientRunOptions) *runningClient {
		run := &runningClient{cmds: make(chan command), ready: make(chan struct{}, 1), idCh: make(chan int64, 1), manager: manager}
		if len(options) > 0 {
			run.options = options[0]
		}
		client := newClient(clientUpdates{manager: manager, pushes: pushes}, sess)
		run.lifecycle = startClientLifecycle(ctx, label, failures, func(phase *clientPhaseState) error {
			return runClientWithOptions(phase, run.options, func() error {
				return runManagedInteractive(ctx, client, flowFor(phone), run.idCh, run.ready, run.cmds, manager, forget, phase)
			})
		})
		t.Cleanup(func() { stopClient(run) })
		return run
	}
	waitClient := func(run *runningClient) {
		loginStarted := time.Now()
		select {
		case run.id = <-run.idCh:
		case <-ctx.Done():
			t.Fatalf("%s", run.lifecycle.diagnostic("login", time.Since(loginStarted), contextFailureDescription(ctx)))
		case <-run.lifecycle.result.done:
			t.Fatalf("%s", run.lifecycle.diagnostic("login", time.Since(loginStarted), "cause="+safeErrorClass(run.lifecycle.result.error())))
		}
		managerStarted := time.Now()
		select {
		case <-run.ready:
		case <-ctx.Done():
			t.Fatalf("%s", run.lifecycle.diagnostic("manager readiness", time.Since(managerStarted), contextFailureDescription(ctx)))
		case <-run.lifecycle.result.done:
			t.Fatalf("%s", run.lifecycle.diagnostic("manager readiness", time.Since(managerStarted), "cause="+safeErrorClass(run.lifecycle.result.error())))
		}
	}
	exec := func(run *runningClient, fn func(context.Context, *tg.Client) error) error {
		started := time.Now()
		done := make(chan error, 1)
		select {
		case run.cmds <- command{fn: fn, done: done}:
		case <-ctx.Done():
			t.Fatalf("%s", run.lifecycle.diagnostic("command", time.Since(started), contextFailureDescription(ctx)))
		case <-run.lifecycle.result.done:
			return errors.New(run.lifecycle.diagnostic("command", time.Since(started), "cause="+safeErrorClass(run.lifecycle.result.error())))
		}
		return waitForClientCommand(ctx, run.lifecycle, done, started)
	}
	stopClient = func(run *runningClient) {
		run.stop.Do(func() {
			stopClientLifecycle(t, run.lifecycle, func() { close(run.cmds) })
			run.manager.Reset()
		})
	}

	sessA1, sessA2, sessB1, sessB2 := &session.StorageMemory{}, &session.StorageMemory{}, &session.StorageMemory{}, &session.StorageMemory{}
	collA1, collA2 := newUpdateCollector(), newUpdateCollector()
	collB1, collB2 := newUpdateCollector(), newUpdateCollector()
	pushA1, pushA2 := newUpdateCollector(), newUpdateCollector()
	pushB1, pushB2 := newUpdateCollector(), newUpdateCollector()
	managerA1, managerA2 := updates.New(updates.Config{Handler: collA1}), updates.New(updates.Config{Handler: collA2})
	managerB1, managerB2 := updates.New(updates.Config{Handler: collB1}), updates.New(updates.Config{Handler: collB2})
	a1 := launchClient("A1", phoneA, pushA1, managerA1, sessA1, true)
	waitClient(a1)
	a2 := launchClient("A2", phoneA, pushA2, managerA2, sessA2, true)
	waitClient(a2)
	b1 := launchClient("B1", phoneB, pushB1, managerB1, sessB1, true)
	waitClient(b1)
	b2 := launchClient("B2", phoneB, pushB2, managerB2, sessB2, true)
	waitClient(b2)
	collC, pushC := newUpdateCollector(), newUpdateCollector()
	managerC := updates.New(updates.Config{Handler: collC})
	c := launchClient("C", phoneC, pushC, managerC, &session.StorageMemory{}, true)
	waitClient(c)
	waitForDistinctAuthKeys(t, ctx, registry, b1.id, 2, "B1", b1.lifecycle)
	waitForDistinctAuthKeys(t, ctx, registry, a1.id, 2, "A1", a1.lifecycle)

	var warmup tg.UpdatesClass
	if err := exec(a1, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(a1.id, b1.id), Message: "A to B warmup", RandomID: 82501,
		})
		warmup = res
		return err
	}); err != nil {
		t.Fatalf("A warmup send: %v", err)
	}
	warmupMessage, warmupPts, ok := outgoingMessage(t, warmup, "A to B warmup")
	if !ok || countOutgoingMessages(warmup, "A to B warmup") != 1 {
		t.Fatal("A warmup RPC result omitted its outgoing message")
	}
	if warmupPts != 2 {
		t.Fatalf("A warmup RPC pts = %d, want 2 after its saved message", warmupPts)
	}
	assertObservedMessage(t, ctx, collA1, "A to B warmup", warmupMessage.ID, true, b1.id, warmupPts, "A1 RPC result", true)
	assertObservedMessage(t, ctx, collA2, "A to B warmup", warmupMessage.ID, true, b1.id, warmupPts, "A2 other session", true)
	assertObservedMessage(t, ctx, collB1, "A to B warmup", 1, false, a1.id, 1, "B1 incoming", true)
	assertObservedMessage(t, ctx, collB2, "A to B warmup", 1, false, a1.id, 1, "B2 incoming", true)
	assertObservedMessage(t, ctx, pushA1, "A saved id seed", int(seedMessage.LocalID), true, a1.id, seedPts, "A1 initial catch-up push")
	assertObservedMessage(t, ctx, pushA2, "A saved id seed", int(seedMessage.LocalID), true, a1.id, seedPts, "A2 initial catch-up push")
	assertObservedMessage(t, ctx, pushA2, "A to B warmup", warmupMessage.ID, true, b1.id, warmupPts, "A2 push")
	assertObservedMessage(t, ctx, pushB1, "A to B warmup", 1, false, a1.id, 1, "B1 push")
	assertObservedMessage(t, ctx, pushB2, "A to B warmup", 1, false, a1.id, 1, "B2 push")
	if warmupMessage.ID == 1 {
		t.Fatal("A and B warmup ids unexpectedly equal")
	}

	var sendResult tg.UpdatesClass
	if err := exec(b1, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(b1.id, a1.id), Message: "sender echo target", RandomID: 82502,
		})
		sendResult = res
		return err
	}); err != nil {
		t.Fatalf("B send to A: %v", err)
	}
	senderMessage, senderPts, ok := outgoingMessage(t, sendResult, "sender echo target")
	if !ok || countOutgoingMessages(sendResult, "sender echo target") != 1 {
		t.Fatal("sendMessage RPC result omitted its outgoing message")
	}
	if senderPts != 2 {
		t.Fatalf("sender RPC pts = %d, want 2 after A's message", senderPts)
	}
	assertObservedMessage(t, ctx, collB1, "sender echo target", senderMessage.ID, true, a1.id, senderPts, "B1 RPC result", true)
	assertObservedMessage(t, ctx, collB2, "sender echo target", senderMessage.ID, true, a1.id, senderPts, "B2 other session", true)
	assertObservedMessage(t, ctx, collA1, "sender echo target", 3, false, b1.id, 3, "A1 incoming", true)
	assertObservedMessage(t, ctx, collA2, "sender echo target", 3, false, b1.id, 3, "A2 incoming", true)
	assertObservedMessage(t, ctx, pushB2, "sender echo target", senderMessage.ID, true, a1.id, senderPts, "B2 push")
	assertObservedMessage(t, ctx, pushA1, "sender echo target", 3, false, b1.id, 3, "A1 push")
	assertObservedMessage(t, ctx, pushA2, "sender echo target", 3, false, b1.id, 3, "A2 push")
	if senderMessage.ID == 3 {
		t.Fatal("B sender id unexpectedly equals A recipient id")
	}
	recipientMessage := &tg.Message{ID: 3}

	var secondBResult tg.UpdatesClass
	if err := exec(b2, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(b2.id, a1.id), Message: "second B to A", RandomID: 82503,
		})
		secondBResult = res
		return err
	}); err != nil {
		t.Fatalf("B2 send to A: %v", err)
	}
	secondBMessage, secondBPts, ok := outgoingMessage(t, secondBResult, "second B to A")
	if !ok || countOutgoingMessages(secondBResult, "second B to A") != 1 {
		t.Fatal("second B send RPC result omitted its outgoing message")
	}
	if secondBPts != 3 {
		t.Fatalf("second B sender pts = %d, want 3", secondBPts)
	}
	assertObservedMessage(t, ctx, collB2, "second B to A", secondBMessage.ID, true, a1.id, secondBPts, "B2 RPC result", true)
	assertObservedMessage(t, ctx, collB1, "second B to A", secondBMessage.ID, true, a1.id, 3, "B1 other session", true)
	assertObservedMessage(t, ctx, collA1, "second B to A", 4, false, b1.id, 4, "A1 incoming second", true)
	assertObservedMessage(t, ctx, collA2, "second B to A", 4, false, b1.id, 4, "A2 incoming second", true)
	assertObservedMessage(t, ctx, pushB1, "second B to A", secondBMessage.ID, true, a1.id, 3, "B1 push")
	assertObservedMessage(t, ctx, pushA1, "second B to A", 4, false, b1.id, 4, "A1 push second")
	assertObservedMessage(t, ctx, pushA2, "second B to A", 4, false, b1.id, 4, "A2 push second")

	// The read uses A's owner-local id. B must receive the mirrored sender id,
	// and an oversized request must not move B's marker beyond that row.
	read := func(maxID int) error {
		return exec(a1, func(ctx context.Context, api *tg.Client) error {
			_, err := api.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
				Peer: peerUser(a1.id, b1.id), MaxID: maxID,
			})
			return err
		})
	}
	if err := read(recipientMessage.ID); err != nil {
		t.Fatalf("A readHistory through its local id: %v", err)
	}
	for _, session := range []struct {
		name string
		coll *updateCollector
		push *updateCollector
	}{
		{name: "B1", coll: collB1, push: pushB1},
		{name: "B2", coll: collB2, push: pushB2},
	} {
		assertObservedReadMarker(t, ctx, session.coll, senderMessage.ID, 4, session.name+" mirrored read")
		assertObservedReadMarker(t, ctx, session.push, senderMessage.ID, 4, session.name+" mirrored read push")
	}
	if err := read(int(1<<31 - 1)); err != nil {
		t.Fatalf("A oversized readHistory: %v", err)
	}
	for _, session := range []struct {
		name string
		coll *updateCollector
		push *updateCollector
	}{
		{name: "B1", coll: collB1, push: pushB1},
		{name: "B2", coll: collB2, push: pushB2},
	} {
		assertObservedReadMarker(t, ctx, session.coll, secondBMessage.ID, 5, session.name+" oversized read")
		assertObservedReadMarker(t, ctx, session.push, secondBMessage.ID, 5, session.name+" oversized read push")
	}

	checkHistory := func(run *runningClient, viewerID, peerID int64) {
		t.Helper()
		if err := exec(run, func(ctx context.Context, api *tg.Client) error {
			res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peerUser(viewerID, peerID), Limit: 10})
			if err != nil {
				return err
			}
			history, ok := res.(*tg.MessagesMessages)
			if !ok {
				return fmt.Errorf("history response = %T", res)
			}
			if len(history.Messages) != 3 {
				return fmt.Errorf("history messages = %d, want 3", len(history.Messages))
			}
			found := map[string]bool{}
			for _, class := range history.Messages {
				msg, ok := class.(*tg.Message)
				if !ok {
					return fmt.Errorf("history message = %+v", class)
				}
				if msg.Message == "A to B warmup" || msg.Message == "sender echo target" || msg.Message == "second B to A" {
					found[msg.Message] = true
					wantID := map[string]int{"A to B warmup": 1, "sender echo target": 2, "second B to A": 3}[msg.Message]
					if viewerID == a1.id {
						wantID = map[string]int{"A to B warmup": warmupMessage.ID, "sender echo target": 3, "second B to A": 4}[msg.Message]
					}
					if msg.ID != wantID {
						return fmt.Errorf("history %q id = %d for owner %d, want %d", msg.Message, msg.ID, viewerID, wantID)
					}
					msgPeer, ok := msg.PeerID.(*tg.PeerUser)
					if !ok || msgPeer.UserID != peerID {
						return fmt.Errorf("history %q peer_id = %+v, want %d", msg.Message, msg.PeerID, peerID)
					}
				}
				if viewerID == a1.id && msg.Message == "A to B warmup" && !msg.Out {
					return fmt.Errorf("A warmup history message is not outgoing: %+v", msg)
				}
				if viewerID == a1.id && msg.Message == "sender echo target" && msg.Out {
					return fmt.Errorf("A target history message is outgoing: %+v", msg)
				}
				if viewerID == a1.id && msg.Message == "second B to A" && msg.Out {
					return fmt.Errorf("A second target history message is outgoing: %+v", msg)
				}
				if viewerID == b1.id && msg.Message == "A to B warmup" && msg.Out {
					return fmt.Errorf("B warmup history message is outgoing: %+v", msg)
				}
				if viewerID == b1.id && msg.Message == "sender echo target" && !msg.Out {
					return fmt.Errorf("B target history message is incoming: %+v", msg)
				}
				if viewerID == b1.id && msg.Message == "second B to A" && !msg.Out {
					return fmt.Errorf("B second target history message is incoming: %+v", msg)
				}
			}
			if !found["A to B warmup"] || !found["sender echo target"] || !found["second B to A"] {
				return fmt.Errorf("history messages missing round trip: %v", found)
			}
			return nil
		}); err != nil {
			t.Fatalf("history for %d with peer %d: %v", viewerID, peerID, err)
		}
	}
	checkHistory(a1, a1.id, b1.id)
	checkHistory(a2, a2.id, b1.id)
	checkHistory(b1, b1.id, a1.id)
	checkHistory(b2, b1.id, a1.id)
	for _, peerID := range []int64{a1.id, b1.id} {
		if err := exec(c, func(ctx context.Context, api *tg.Client) error {
			res, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peerUser(c.id, peerID), Limit: 10})
			if err != nil {
				return err
			}
			history, ok := res.(*tg.MessagesMessages)
			if !ok {
				return fmt.Errorf("C history response = %T", res)
			}
			if len(history.Messages) != 0 {
				return fmt.Errorf("C history with %d exposed %d A/B messages", peerID, len(history.Messages))
			}
			return nil
		}); err != nil {
			t.Fatalf("C history with peer %d: %v", peerID, err)
		}
	}
	aStateBeforeCRead, err := st.State(ctx, a1.id)
	if err != nil {
		t.Fatalf("A state before C read: %v", err)
	}
	aDialogsBeforeCRead, err := st.Dialogs(ctx, a1.id, 0, 100)
	if err != nil {
		t.Fatalf("A dialogs before C read: %v", err)
	}
	if err := exec(c, func(ctx context.Context, api *tg.Client) error {
		_, err := api.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
			Peer: peerUser(c.id, a1.id), MaxID: int(1<<31 - 1),
		})
		return err
	}); err != nil {
		t.Fatalf("C readHistory without a dialog: %v", err)
	}
	aStateAfterCRead, err := st.State(ctx, a1.id)
	if err != nil {
		t.Fatalf("A state after C read: %v", err)
	}
	if aStateAfterCRead.Pts != aStateBeforeCRead.Pts {
		t.Fatalf("C no-dialog read changed A pts from %d to %d", aStateBeforeCRead.Pts, aStateAfterCRead.Pts)
	}
	aDialogsAfterCRead, err := st.Dialogs(ctx, a1.id, 0, 100)
	if err != nil {
		t.Fatalf("A dialogs after C read: %v", err)
	}
	if len(aDialogsAfterCRead) != len(aDialogsBeforeCRead) {
		t.Fatalf("C no-dialog read changed A dialog count from %d to %d", len(aDialogsBeforeCRead), len(aDialogsAfterCRead))
	}
	for i := range aDialogsBeforeCRead {
		before, after := aDialogsBeforeCRead[i], aDialogsAfterCRead[i]
		if before.PeerType != after.PeerType || before.PeerID != after.PeerID || before.ReadOutboxMaxID != after.ReadOutboxMaxID {
			t.Fatalf("C no-dialog read changed A outbox marker: before=%+v after=%+v", before, after)
		}
	}
	if err := managerA1.Handle(ctx, warmup); err != nil {
		t.Fatalf("replay duplicate RPC update through Manager: %v", err)
	}
	if got := takeMessage(t, collA1.newMsg); got != nil {
		t.Fatalf("Manager surfaced a duplicate pts update: %+v", got)
	}

	for _, session := range []struct {
		name string
		coll *updateCollector
	}{
		{name: "A1", coll: pushA1},
		{name: "B1", coll: pushB1},
		{name: "B2", coll: pushB2},
	} {
		if got := takeMessage(t, session.coll.newMsg); got != nil {
			t.Fatalf("originating session %s received its own send by push: %+v", session.name, got)
		}
	}
	if got := takeMessage(t, collC.newMsg); got != nil {
		t.Fatalf("C received an A/B message: %+v", got)
	}
	if got := takeReadOutbox(t, collC.readOutbox); got != nil {
		t.Fatalf("C received an A/B read marker: %d", *got)
	}
	if got := takeReadOutbox(t, pushC.readOutbox); got != nil {
		t.Fatalf("C received an A/B read marker push: %d", *got)
	}

	stopClient(b1)
	var recoveryResult tg.UpdatesClass
	if err := exec(a1, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(a1.id, b1.id), Message: "A to B offline recovery", RandomID: 82504,
		})
		recoveryResult = res
		return err
	}); err != nil {
		t.Fatalf("A recovery send while B1 is offline: %v", err)
	}
	recoveryMessage, recoveryPts, ok := outgoingMessage(t, recoveryResult, "A to B offline recovery")
	if !ok || countOutgoingMessages(recoveryResult, "A to B offline recovery") != 1 {
		t.Fatal("A recovery RPC result omitted its outgoing message")
	}
	assertObservedMessage(t, ctx, collA1, "A to B offline recovery", recoveryMessage.ID, true, b1.id, recoveryPts, "A1 recovery RPC result", true)
	assertDifferenceMessage(t, ctx, collA2, "A to B offline recovery", recoveryMessage.ID, true, b1.id, "A2 recovery other session")
	assertObservedMessage(t, ctx, collB2, "A to B offline recovery", 4, false, a1.id, 6, "B2 recovery incoming", true)
	assertObservedMessage(t, ctx, pushA2, "A to B offline recovery", recoveryMessage.ID, true, b1.id, recoveryPts, "A2 recovery push")
	assertObservedMessage(t, ctx, pushB2, "A to B offline recovery", 4, false, a1.id, 6, "B2 recovery push")
	if recoveryPts != 7 {
		t.Fatalf("A recovery pts = %d, want 7 after two reads", recoveryPts)
	}
	pushB1Reconnected := newUpdateCollector()
	b1Reconnected := launchClient("B1", phoneB, pushB1Reconnected, managerB1, sessB1, false)
	waitClient(b1Reconnected)
	assertDifferenceMessage(t, ctx, collB1, "A to B offline recovery", 4, false, a1.id, "B1 Manager recovery difference")

	var senderDifference tg.UpdatesDifferenceClass
	if err := exec(b1Reconnected, func(ctx context.Context, api *tg.Client) error {
		diff, err := api.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		senderDifference = diff
		return err
	}); err != nil {
		t.Fatalf("B getDifference after reconnect: %v", err)
	}
	full, ok := senderDifference.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("B difference = %T, want *tg.UpdatesDifference", senderDifference)
	}
	if len(full.NewMessages) != 4 {
		t.Fatalf("B difference new messages = %d, want four owner-local messages", len(full.NewMessages))
	}
	pts := []int{1, senderPts, secondBPts}
	foundDifferenceMessages := map[string]bool{}
	for _, class := range full.NewMessages {
		msg, ok := class.(*tg.Message)
		if !ok {
			t.Fatalf("B backfill message = %+v", class)
		}
		if msg.Message == "A to B warmup" && msg.Out {
			t.Fatalf("B backfill warmup unexpectedly outgoing: %+v", msg)
		}
		if msg.Message == "A to B warmup" && msg.ID != 1 {
			t.Fatalf("B backfill warmup id = %d, want B-local id 1", msg.ID)
		}
		if msg.Message == "sender echo target" && !msg.Out {
			t.Fatalf("B backfill target unexpectedly incoming: %+v", msg)
		}
		if msg.Message == "sender echo target" && msg.ID != senderMessage.ID {
			t.Fatalf("B backfill target id = %d, want B-local id %d", msg.ID, senderMessage.ID)
		}
		if msg.Message == "second B to A" && !msg.Out {
			t.Fatalf("B backfill second target unexpectedly incoming: %+v", msg)
		}
		if msg.Message == "second B to A" && msg.ID != secondBMessage.ID {
			t.Fatalf("B backfill second target id = %d, want B-local id %d", msg.ID, secondBMessage.ID)
		}
		if msg.Message == "A to B offline recovery" && (msg.Out || msg.ID != 4) {
			t.Fatalf("B backfill recovery message = %+v, want incoming local id 4", msg)
		}
		foundDifferenceMessages[msg.Message] = true
		if msg.Message == "sender echo target" && msg.ID != senderMessage.ID {
			t.Fatalf("B backfill id = %d, want %d", msg.ID, senderMessage.ID)
		}
	}
	if !foundDifferenceMessages["A to B warmup"] || !foundDifferenceMessages["sender echo target"] || !foundDifferenceMessages["second B to A"] || !foundDifferenceMessages["A to B offline recovery"] {
		t.Fatalf("B difference messages = %v", foundDifferenceMessages)
	}
	if full.State.Pts != 6 {
		t.Fatalf("B backfill state pts = %d, want contiguous head 6", full.State.Pts)
	}
	readMarkers := 0
	wantReadMarkers := []int{senderMessage.ID, secondBMessage.ID}
	for _, update := range full.OtherUpdates {
		if outbox, ok := update.(*tg.UpdateReadHistoryOutbox); ok {
			if readMarkers >= len(wantReadMarkers) || outbox.MaxID != wantReadMarkers[readMarkers] {
				t.Fatalf("B backfilled read marker = %d at index %d, want %v", outbox.MaxID, readMarkers, wantReadMarkers)
			}
			readMarkers++
			pts = append(pts, outbox.Pts)
		}
	}
	pts = append(pts, 6)
	if readMarkers != 2 {
		t.Fatalf("B read outbox updates = %d, want 2", readMarkers)
	}
	if len(pts) != 6 || pts[0] != 1 || pts[1] != 2 || pts[2] != 3 || pts[3] != 4 || pts[4] != 5 || pts[5] != 6 {
		t.Fatalf("B backfill pts = %v, want [1 2 3 4 5 6]", pts)
	}

	if err := exec(c, func(ctx context.Context, api *tg.Client) error {
		diff, err := api.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		if err != nil {
			return err
		}
		if full, ok := diff.(*tg.UpdatesDifference); ok && (len(full.NewMessages) != 0 || len(full.OtherUpdates) != 0) {
			return fmt.Errorf("C difference exposed A/B events: messages=%d updates=%d", len(full.NewMessages), len(full.OtherUpdates))
		}
		if _, ok := diff.(*tg.UpdatesDifferenceEmpty); !ok {
			return fmt.Errorf("C difference = %T, want empty", diff)
		}
		return nil
	}); err != nil {
		t.Fatalf("C isolation difference: %v", err)
	}

	const mediaFileID int64 = 82505
	const mediaRandomID int64 = 82506
	const mediaCaption = "A to B direct media"
	mediaPayload := []byte("direct media echo")
	if err := exec(a1, func(ctx context.Context, api *tg.Client) error {
		ok, err := api.UploadSaveFilePart(ctx, &tg.UploadSaveFilePartRequest{
			FileID: mediaFileID, FilePart: 0, Bytes: mediaPayload,
		})
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("upload.saveFilePart returned false")
		}
		return nil
	}); err != nil {
		t.Fatalf("A media upload: %v", err)
	}

	var mediaResult tg.UpdatesClass
	if err := exec(a1, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
			Peer: peerUser(a1.id, b1.id),
			Media: &tg.InputMediaUploadedDocument{
				File:     &tg.InputFile{ID: mediaFileID, Parts: 1, Name: "echo.txt"},
				MimeType: "text/plain",
			},
			Message:  mediaCaption,
			RandomID: mediaRandomID,
		})
		mediaResult = res
		return err
	}); err != nil {
		t.Fatalf("A direct media send: %v", err)
	}
	mediaMessage, mediaPts, ok := outgoingMessage(t, mediaResult, mediaCaption)
	if !ok || countOutgoingMessages(mediaResult, mediaCaption) != 1 {
		t.Fatal("A direct media RPC result omitted or duplicated its outgoing message")
	}
	if mediaPts != recoveryPts+1 {
		t.Fatalf("A direct media RPC pts = %d, want %d", mediaPts, recoveryPts+1)
	}
	mediaDocument := documentOf(t, mediaMessage)
	if int(mediaDocument.Size) != len(mediaPayload) {
		t.Fatalf("A direct media document size = %d, want %d", mediaDocument.Size, len(mediaPayload))
	}
	assertObservedMessage(t, ctx, collA1, mediaCaption, mediaMessage.ID, true, b1.id, mediaPts, "A1 media RPC result", true)
	assertObservedMessage(t, ctx, collA2, mediaCaption, mediaMessage.ID, true, b1.id, mediaPts, "A2 media other session", true)
	assertObservedMessage(t, ctx, pushA2, mediaCaption, mediaMessage.ID, true, b1.id, mediaPts, "A2 media push")
	assertObservedMessage(t, ctx, collB1, mediaCaption, 5, false, a1.id, 7, "B1 media incoming", true)
	assertObservedMessage(t, ctx, collB2, mediaCaption, 5, false, a1.id, 7, "B2 media incoming", true)
	drainPushBacklog(t, ctx, pushB1Reconnected)
	drainPushBacklog(t, ctx, pushB2)
	if got := takeMessage(t, pushA1.newMsg); got != nil {
		t.Fatalf("A1 media origin received its own send by push: %+v", got)
	}

	for _, session := range []struct {
		name string
		coll *updateCollector
	}{
		{name: "A1", coll: collA1},
		{name: "A2", coll: collA2},
		{name: "B1", coll: collB1},
		{name: "B2", coll: collB2},
		{name: "C", coll: collC},
		{name: "A1 raw push", coll: pushA1},
		{name: "A2 raw push", coll: pushA2},
		{name: "B1 raw push", coll: pushB1},
		{name: "B1 reconnect raw push", coll: pushB1Reconnected},
		{name: "B2 raw push", coll: pushB2},
		{name: "C raw push", coll: pushC},
	} {
		assertNoMessageFor(t, ctx, session.coll.newMsg, session.name)
		if marker := takeReadOutbox(t, session.coll.readOutbox); marker != nil {
			t.Fatalf("%s received an unexpected read outbox marker %d", session.name, *marker)
		}
	}

	stopClient(b1Reconnected)
	stopClient(b2)
	stopClient(a1)
	stopClient(a2)
	stopClient(c)
}

func waitForDistinctAuthKeys(t *testing.T, ctx context.Context, registry *mtproto.SessionRegistry, userID int64, want int, label string, lifecycle *clientLifecycle) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	started := time.Now()
	var last registrySnapshot
	lastValid := false
	for {
		if snapshot, ok := snapshotRegistry(ctx, registry, userID); ok {
			last, lastValid = snapshot, true
			lifecycle.rememberRegistrySnapshot(snapshot)
			if snapshot.connections == want && snapshot.zeroKeys == 0 && snapshot.distinctKeys == want {
				return
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("%s registry readiness failed after %s (%s; %s)", label, time.Since(started).Round(time.Millisecond), registrySnapshotDescription(last, lastValid), contextFailureDescription(ctx))
		}
	}
}

func drainPushBacklog(t *testing.T, ctx context.Context, coll *updateCollector) {
	t.Helper()
	for {
		select {
		case <-coll.newMsg:
			select {
			case <-coll.points:
			case <-ctx.Done():
				t.Fatalf("draining push points: %s", contextFailureDescription(ctx))
			}
		case <-coll.readOutbox:
			select {
			case <-coll.readOutboxPts:
			case <-ctx.Done():
				t.Fatalf("draining push read marker pts: %s", contextFailureDescription(ctx))
			}
		default:
			return
		}
	}
}

func outgoingMessage(t *testing.T, result tg.UpdatesClass, text string) (*tg.Message, int, bool) {
	t.Helper()
	updates, ok := result.(*tg.Updates)
	if !ok {
		return nil, 0, false
	}
	for _, update := range updates.Updates {
		message, ok := update.(*tg.UpdateNewMessage)
		if !ok {
			continue
		}
		msg, ok := message.Message.(*tg.Message)
		if ok && msg.Message == text && msg.Out {
			return msg, message.Pts, true
		}
	}
	return nil, 0, false
}

func countOutgoingMessages(result tg.UpdatesClass, text string) int {
	updates, ok := result.(*tg.Updates)
	if !ok {
		return 0
	}
	count := 0
	for _, update := range updates.Updates {
		newMessage, ok := update.(*tg.UpdateNewMessage)
		if !ok {
			continue
		}
		message, ok := newMessage.Message.(*tg.Message)
		if ok && message.Message == text && message.Out {
			count++
		}
	}
	return count
}

func assertObservedMessage(t *testing.T, ctx context.Context, coll *updateCollector, text string, id int, out bool, peerID int64, pts int, label string, allowUnspecifiedPts ...bool) {
	t.Helper()
	message := recvOrCtx(t, ctx, coll.newMsg, label+" message")
	if message.Message != text || message.ID != id || message.Out != out {
		t.Fatalf("%s message = {text:%q id:%d out:%v}, want {text:%q id:%d out:%v}", label, message.Message, message.ID, message.Out, text, id, out)
	}
	peer, ok := message.PeerID.(*tg.PeerUser)
	if !ok || peer.UserID != peerID {
		t.Fatalf("%s peer_id = %+v, want sender/peer user %d", label, message.PeerID, peerID)
	}
	gotPts := recvOrCtx(t, ctx, coll.points, label+" pts")
	// A gotd updates manager can surface a concurrent live update through a
	// difference response with an unspecified pts marker. The message identity
	// and direction remain authoritative in that path.
	unspecified := len(allowUnspecifiedPts) > 0 && allowUnspecifiedPts[0] && gotPts < 0
	if gotPts != pts && !unspecified {
		t.Fatalf("%s pts = %d, want %d", label, gotPts, pts)
	}
}

func assertDifferenceMessage(t *testing.T, ctx context.Context, coll *updateCollector, text string, id int, out bool, peerID int64, label string) {
	t.Helper()
	message := recvOrCtx(t, ctx, coll.newMsg, label+" message")
	if message.Message != text || message.ID != id || message.Out != out {
		t.Fatalf("%s message = {text:%q id:%d out:%v}, want {text:%q id:%d out:%v}", label, message.Message, message.ID, message.Out, text, id, out)
	}
	peer, ok := message.PeerID.(*tg.PeerUser)
	if !ok || peer.UserID != peerID {
		t.Fatalf("%s peer_id = %+v, want sender/peer user %d", label, message.PeerID, peerID)
	}
	if got := recvOrCtx(t, ctx, coll.points, label+" pts"); got >= 0 {
		t.Fatalf("%s pts = %d, want Manager difference output with unspecified pts", label, got)
	}
}

func assertNoMessageFor(t *testing.T, ctx context.Context, updates <-chan *tg.Message, label string) {
	t.Helper()
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case message := <-updates:
		t.Fatalf("%s received unexpected message during drain: %+v", label, message)
	case <-timer.C:
	case <-ctx.Done():
		t.Fatalf("waiting for %s drain: %s", label, contextFailureDescription(ctx))
	}
}

func assertObservedReadMarker(t *testing.T, ctx context.Context, coll *updateCollector, maxID, pts int, label string) {
	t.Helper()
	if got := recvOrCtx(t, ctx, coll.readOutbox, label+" max_id"); got != maxID {
		t.Fatalf("%s max_id = %d, want %d", label, got, maxID)
	}
	if got := recvOrCtx(t, ctx, coll.readOutboxPts, label+" pts"); got != pts {
		t.Fatalf("%s pts = %d, want %d", label, got, pts)
	}
}

func takeMessage(t *testing.T, updates <-chan *tg.Message) *tg.Message {
	t.Helper()
	select {
	case message := <-updates:
		return message
	default:
		return nil
	}
}

func takeReadOutbox(t *testing.T, updates <-chan int) *int {
	t.Helper()
	select {
	case marker := <-updates:
		return &marker
	default:
		return nil
	}
}

type observedManagerHandler struct {
	observer *updateCollector
	manager  *updates.Manager
}

func (h observedManagerHandler) Handle(ctx context.Context, upd tg.UpdatesClass) error {
	if err := h.observer.Handle(ctx, upd); err != nil {
		return err
	}
	return h.manager.Handle(ctx, upd)
}

func runManagedInteractive(ctx context.Context, client *telegram.Client, flow auth.Flow, selfOut chan<- int64, readyOut chan<- struct{}, cmds <-chan command, manager *updates.Manager, forget bool, phase *clientPhaseState) error {
	return client.Run(ctx, func(ctx context.Context) error {
		phase.set("login")
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return err
		}
		phase.set("self")
		self, err := client.Self(ctx)
		if err != nil {
			return err
		}
		selfOut <- self.ID

		managerCtx, cancelManager := context.WithCancel(ctx)
		var managerStopping atomic.Bool
		defer func() {
			managerStopping.Store(true)
			cancelManager()
		}()
		managerReady := make(chan struct{})
		managerResult := newTerminalResult()
		var onStart sync.Once
		go func() {
			managerErr := manager.Run(managerCtx, client.API(), self.ID, updates.AuthOptions{
				Forget: forget,
				OnStart: func(context.Context) {
					onStart.Do(func() { close(managerReady) })
				},
			})
			if managerErr == nil && !managerStopping.Load() {
				managerErr = errUnexpectedNilExit
			}
			managerResult.complete(managerErr)
		}()
		stopManager := func() error {
			managerStopping.Store(true)
			cancelManager()
			managerErr := managerResult.error()
			if managerErr != nil && !errors.Is(managerErr, context.Canceled) {
				return managerErr
			}
			return nil
		}
		phase.set("manager readiness")
		select {
		case <-managerReady:
			select {
			case readyOut <- struct{}{}:
			default:
			}
			phase.set("idle")
		case <-managerResult.done:
			phase.set("update manager")
			return managerResult.error()
		case <-ctx.Done():
			if err := stopManager(); err != nil {
				return err
			}
			return nil
		}
		return runManagedCommands(ctx, cmds, managerResult, phase, func(commandCtx context.Context, c command) error {
			return c.fn(commandCtx, client.API())
		}, stopManager)
	})
}
