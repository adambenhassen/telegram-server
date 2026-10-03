package api

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/blob"
	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

type recordingNotifyTransport struct {
	mu     sync.Mutex
	frames [][]byte
}

func (t *recordingNotifyTransport) Send(_ context.Context, b *bin.Buffer) error {
	t.mu.Lock()
	t.frames = append(t.frames, slices.Clone(b.Buf))
	t.mu.Unlock()
	return nil
}

func (*recordingNotifyTransport) Recv(context.Context, *bin.Buffer) error {
	return errors.New("unused")
}
func (*recordingNotifyTransport) Close() error { return nil }

func (t *recordingNotifyTransport) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.frames)
}

func (t *recordingNotifyTransport) framesFrom(start int) [][]byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	if start >= len(t.frames) {
		return nil
	}
	frames := make([][]byte, len(t.frames)-start)
	for i, frame := range t.frames[start:] {
		frames[i] = slices.Clone(frame)
	}
	return frames
}

type decodedServerFrame struct {
	rpc  *tg.Updates
	push *tg.Updates
}

func decodeServerFrames(t *testing.T, key crypto.AuthKey, frames [][]byte) []decodedServerFrame {
	t.Helper()
	cipher := crypto.NewClientCipher(crypto.DefaultRand())
	decoded := make([]decodedServerFrame, 0, len(frames))
	for _, frame := range frames {
		data, err := cipher.DecryptFromBuffer(key, &bin.Buffer{Buf: frame})
		if err != nil {
			t.Fatalf("decrypt server frame: %v", err)
		}
		body := data.Data()
		var result proto.Result
		if err := result.Decode(&bin.Buffer{Buf: body}); err == nil {
			updates := &tg.Updates{}
			if err := updates.Decode(&bin.Buffer{Buf: result.Result}); err != nil {
				t.Fatalf("decode RPC updates: %v", err)
			}
			decoded = append(decoded, decodedServerFrame{rpc: updates})
			continue
		}
		updates := &tg.Updates{}
		if err := updates.Decode(&bin.Buffer{Buf: body}); err != nil {
			t.Fatalf("decode pushed updates: %v", err)
		}
		decoded = append(decoded, decodedServerFrame{push: updates})
	}
	return decoded
}

func assertWireMessage(t *testing.T, got tg.MessageClass, want store.Message) {
	t.Helper()
	message, ok := got.(*tg.Message)
	if !ok {
		t.Fatalf("wire message = %T, want *tg.Message", got)
	}
	if message.ID != int(want.LocalID) {
		t.Fatalf("wire message id = %d, want %d", message.ID, want.LocalID)
	}
	if message.Out != want.Out {
		t.Fatalf("wire message out = %t, want %t", message.Out, want.Out)
	}
	if message.Message != want.Text {
		t.Fatalf("wire message text = %q, want %q", message.Message, want.Text)
	}
	from, ok := message.FromID.(*tg.PeerUser)
	if !ok || from.UserID != want.FromID {
		t.Fatalf("wire message from = %T/%d, want user %d", message.FromID, peerUserID(message.FromID), want.FromID)
	}
	peer, ok := message.PeerID.(*tg.PeerUser)
	if !ok || peer.UserID != want.PeerID {
		t.Fatalf("wire message peer = %T/%d, want user %d", message.PeerID, peerUserID(message.PeerID), want.PeerID)
	}
}

func peerUserID(peer tg.PeerClass) int64 {
	if user, ok := peer.(*tg.PeerUser); ok {
		return user.UserID
	}
	return 0
}

func assertNewMessageFrame(t *testing.T, frame decodedServerFrame, want store.Message, wantPts int, kind string) {
	t.Helper()
	if frame.rpc != nil || frame.push == nil {
		t.Fatalf("%s frame = %+v, want one push", kind, frame)
	}
	assertNewMessageUpdates(t, frame.push, want, wantPts, kind)
}

func assertForwardRPCFrame(t *testing.T, frame decodedServerFrame, want store.Message, wantPts int, randomID int64, kind string) {
	t.Helper()
	if frame.rpc == nil || frame.push != nil {
		t.Fatalf("%s frame = %+v, want one RPC result", kind, frame)
	}
	if len(frame.rpc.Updates) != 2 {
		t.Fatalf("%s updates = %d, want updateMessageID plus updateNewMessage", kind, len(frame.rpc.Updates))
	}
	messageID, ok := frame.rpc.Updates[0].(*tg.UpdateMessageID)
	if !ok {
		t.Fatalf("%s first update = %T, want *tg.UpdateMessageID", kind, frame.rpc.Updates[0])
	}
	if messageID.ID != int(want.LocalID) || messageID.RandomID != randomID {
		t.Fatalf("%s message id = %d/%d, want %d/%d", kind, messageID.ID, messageID.RandomID, want.LocalID, randomID)
	}
	update, ok := frame.rpc.Updates[1].(*tg.UpdateNewMessage)
	if !ok {
		t.Fatalf("%s second update = %T, want *tg.UpdateNewMessage", kind, frame.rpc.Updates[1])
	}
	if update.Pts != wantPts || update.PtsCount != 1 {
		t.Fatalf("%s pts = %d/%d, want %d/1", kind, update.Pts, update.PtsCount, wantPts)
	}
	assertWireMessage(t, update.Message, want)
}

func assertNewMessageUpdates(t *testing.T, updates *tg.Updates, want store.Message, wantPts int, kind string) {
	t.Helper()
	if len(updates.Updates) != 1 {
		t.Fatalf("%s updates = %d, want one", kind, len(updates.Updates))
	}
	update, ok := updates.Updates[0].(*tg.UpdateNewMessage)
	if !ok {
		t.Fatalf("%s update = %T, want *tg.UpdateNewMessage", kind, updates.Updates[0])
	}
	if update.Pts != wantPts || update.PtsCount != 1 {
		t.Fatalf("%s pts = %d/%d, want %d/1", kind, update.Pts, update.PtsCount, wantPts)
	}
	assertWireMessage(t, update.Message, want)
}

func assertEditMessageFrame(t *testing.T, frame decodedServerFrame, want store.Message, wantPts int, kind string) {
	t.Helper()
	if frame.rpc == nil && frame.push == nil {
		t.Fatalf("%s frame has no updates", kind)
	}
	if frame.rpc != nil {
		assertEditMessageUpdates(t, frame.rpc, want, wantPts, kind)
		return
	}
	assertEditMessageUpdates(t, frame.push, want, wantPts, kind)
}

func assertEditMessageUpdates(t *testing.T, updates *tg.Updates, want store.Message, wantPts int, kind string) {
	t.Helper()
	if len(updates.Updates) != 1 {
		t.Fatalf("%s updates = %d, want one", kind, len(updates.Updates))
	}
	update, ok := updates.Updates[0].(*tg.UpdateEditMessage)
	if !ok {
		t.Fatalf("%s update = %T, want *tg.UpdateEditMessage", kind, updates.Updates[0])
	}
	if update.Pts != wantPts || update.PtsCount != 1 {
		t.Fatalf("%s pts = %d/%d, want %d/1", kind, update.Pts, update.PtsCount, wantPts)
	}
	assertWireMessage(t, update.Message, want)
}

func getDifferenceForPts(t *testing.T, h *handlers, ctx context.Context, userID int64, pts int) *tg.UpdatesDifference {
	t.Helper()
	var body bin.Buffer
	if err := (&tg.UpdatesGetDifferenceRequest{Pts: pts}).Encode(&body); err != nil {
		t.Fatalf("encode getDifference: %v", err)
	}
	result, err := h.handleGetDifference(&mtproto.Request{Ctx: ctx, UserID: userID, Buf: &body})
	if err != nil {
		t.Fatalf("getDifference: %v", err)
	}
	difference, ok := result.(*tg.UpdatesDifference)
	if !ok {
		t.Fatalf("getDifference result = %T, want *tg.UpdatesDifference", result)
	}
	return difference
}

func assertDifferenceNewMessage(t *testing.T, difference *tg.UpdatesDifference, want store.Message, wantPts int, kind string) {
	t.Helper()
	if len(difference.NewMessages) != 1 || len(difference.OtherUpdates) != 0 {
		t.Fatalf("%s difference = %d new messages/%d other updates, want 1/0", kind, len(difference.NewMessages), len(difference.OtherUpdates))
	}
	if difference.State.Pts != wantPts {
		t.Fatalf("%s difference state pts = %d, want %d", kind, difference.State.Pts, wantPts)
	}
	message, ok := difference.NewMessages[0].(*tg.Message)
	if !ok {
		t.Fatalf("%s difference message = %T, want *tg.Message", kind, difference.NewMessages[0])
	}
	assertWireMessage(t, message, want)
}

func assertDifferenceEditMessage(t *testing.T, difference *tg.UpdatesDifference, want store.Message, wantPts int, kind string) {
	t.Helper()
	if len(difference.NewMessages) != 0 || len(difference.OtherUpdates) != 1 {
		t.Fatalf("%s difference = %d new messages/%d other updates, want 0/1", kind, len(difference.NewMessages), len(difference.OtherUpdates))
	}
	update, ok := difference.OtherUpdates[0].(*tg.UpdateEditMessage)
	if !ok {
		t.Fatalf("%s difference update = %T, want *tg.UpdateEditMessage", kind, difference.OtherUpdates[0])
	}
	if update.Pts != wantPts || update.PtsCount != 1 {
		t.Fatalf("%s difference pts = %d/%d, want %d/1", kind, update.Pts, update.PtsCount, wantPts)
	}
	if difference.State.Pts != wantPts {
		t.Fatalf("%s difference state pts = %d, want %d", kind, difference.State.Pts, wantPts)
	}
	assertWireMessage(t, update.Message, want)
}

func TestForwardAndEditOriginSessionOnlyGetsRPCResults(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := pgtest.DSN(t)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(blobs))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() }) //nolint:errcheck // teardown

	alice, err := s.CreateUser(ctx, "+15557002001")
	if err != nil {
		t.Fatalf("alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+15557002002")
	if err != nil {
		t.Fatalf("bob: %v", err)
	}
	carol, err := s.CreateUser(ctx, "+15557002003")
	if err != nil {
		t.Fatalf("carol: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, carol.ID, alice.ID, "source", 920001, 0, 0); err != nil {
		t.Fatalf("seed source message: %v", err)
	}
	received, ok, err := s.MessageByOwnerLocal(ctx, alice.ID, 1)
	if err != nil {
		t.Fatalf("load source message: %v", err)
	}
	if !ok {
		t.Fatal("source message missing from alice's inbox")
	}

	registry := mtproto.NewSessionRegistry()
	updater := NewUpdater(s, registry, nil, pgtest.PeerDeriver())
	originKey := retryTestKey(31)
	siblingKey := retryTestKey(32)
	recipientKey := retryTestKey(33)
	originTransport := &recordingNotifyTransport{}
	siblingTransport := &recordingNotifyTransport{}
	recipientTransport := &recordingNotifyTransport{}
	originConn := mtproto.NewTestConn(originTransport, originKey)
	originConn.SetOwner(alice.ID)
	siblingConn := mtproto.NewTestConn(siblingTransport, siblingKey)
	siblingConn.SetOwner(alice.ID)
	recipientConn := mtproto.NewTestConn(recipientTransport, recipientKey)
	recipientConn.SetOwner(bob.ID)
	if !registry.Add(alice.ID, originConn) || !registry.Add(alice.ID, siblingConn) || !registry.Add(bob.ID, recipientConn) {
		t.Fatal("register sessions")
	}
	t.Cleanup(func() {
		registry.Remove(alice.ID, originConn)
		registry.Remove(alice.ID, siblingConn)
		registry.Remove(bob.ID, recipientConn)
	})
	if !originConn.MarkRPCUpdate(alice.ID, originConn.AuthKeyID(), 1) || !siblingConn.MarkRPCUpdate(alice.ID, siblingConn.AuthKeyID(), 1) {
		t.Fatal("seed sender watermarks")
	}

	_, stop, err := store.StartListener(ctx, dsn,
		updater.Deliver,
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, int32) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	t.Cleanup(func() { _ = stop() }) //nolint:errcheck // teardown
	if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
		t.Fatalf("wait for listener: %v", err)
	}

	h := testHandlers(s)
	encode := func(req bin.Encoder, keyID int64) *mtproto.Request {
		var body bin.Buffer
		if err := req.Encode(&body); err != nil {
			t.Fatalf("encode request: %v", err)
		}
		return &mtproto.Request{Ctx: ctx, UserID: alice.ID, AuthKeyID: originKey.ID, MsgID: keyID, Buf: &body}
	}

	forwardReq := encode(&tg.MessagesForwardMessagesRequest{
		ToPeer:   InputPeerUser(alice.ID, bob.ID),
		FromPeer: InputPeerUser(alice.ID, carol.ID),
		ID:       []int{int(received.LocalID)},
		RandomID: []int64{920002},
	}, 1)
	forwardResult, forwardUpdate, forwardAfter, err := h.handleForwardMessagesAfterReplyOnConn(originConn, forwardReq)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if forwardResult == nil || forwardUpdate == nil || forwardAfter == nil {
		t.Fatal("forward did not return RPC ordering metadata")
	}
	waitTransportCount(t, recipientTransport, 1)
	if got := originTransport.count(); got != 0 {
		t.Fatalf("origin pushes before forward result = %d, want 0", got)
	}
	if got := siblingTransport.count(); got != 0 {
		t.Fatalf("sibling pushes before forward result = %d, want 0", got)
	}
	if err := originConn.SendResultAndMarkRPCUpdate(forwardReq, forwardResult, alice.ID, originConn.AuthKeyID(), forwardUpdate.pts); err != nil {
		t.Fatalf("forward result: %v", err)
	}
	forwardAfter()
	waitTransportCount(t, siblingTransport, 1)
	time.Sleep(100 * time.Millisecond)
	if got := originTransport.count(); got != 1 {
		t.Fatalf("origin sends after forward = %d, want one RPC result", got)
	}

	forwarded, ok, err := s.MessageByRandomID(ctx, alice.ID, 920002)
	if err != nil || !ok {
		t.Fatalf("load forwarded message: ok=%v err=%v", ok, err)
	}
	forwardedRecipient, ok, err := s.MessageByOwnerLocal(ctx, bob.ID, forwarded.PeerLocalID)
	if err != nil || !ok {
		t.Fatalf("load forwarded recipient message: ok=%v err=%v", ok, err)
	}
	forwardedRecipientPts, err := s.MessagePts(ctx, bob.ID, forwardedRecipient.LocalID)
	if err != nil {
		t.Fatalf("load forwarded recipient pts: %v", err)
	}
	forwardOriginFrames := decodeServerFrames(t, originKey, originTransport.framesFrom(0))
	forwardSiblingFrames := decodeServerFrames(t, siblingKey, siblingTransport.framesFrom(0))
	forwardRecipientFrames := decodeServerFrames(t, recipientKey, recipientTransport.framesFrom(0))
	if len(forwardOriginFrames) != 1 || forwardOriginFrames[0].rpc == nil || forwardOriginFrames[0].push != nil {
		t.Fatalf("forward origin frames = %+v, want one RPC result", forwardOriginFrames)
	}
	if len(forwardSiblingFrames) != 1 || forwardSiblingFrames[0].rpc != nil || forwardSiblingFrames[0].push == nil {
		t.Fatalf("forward sibling frames = %+v, want one push", forwardSiblingFrames)
	}
	if len(forwardRecipientFrames) != 1 || forwardRecipientFrames[0].rpc != nil || forwardRecipientFrames[0].push == nil {
		t.Fatalf("forward recipient frames = %+v, want one push", forwardRecipientFrames)
	}
	assertForwardRPCFrame(t, forwardOriginFrames[0], forwarded, forwardUpdate.pts, 920002, "forward A1 RPC")
	assertNewMessageFrame(t, forwardSiblingFrames[0], forwarded, forwardUpdate.pts, "forward A2 push")
	assertNewMessageFrame(t, forwardRecipientFrames[0], forwardedRecipient, forwardedRecipientPts, "forward B push")
	editReq := encode(&tg.MessagesEditMessageRequest{
		Peer:    InputPeerUser(alice.ID, bob.ID),
		ID:      int(forwarded.LocalID),
		Message: "edited forward",
	}, 2)
	editResult, editUpdate, editAfter, err := h.handleEditMessageAfterReplyOnConn(originConn, editReq)
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if editResult == nil || editUpdate == nil || editAfter == nil {
		t.Fatal("edit did not return RPC ordering metadata")
	}
	if _, ok := editResult.(*tg.Updates); !ok {
		t.Fatalf("edit result type = %T, want *tg.Updates", editResult)
	}
	waitTransportCount(t, recipientTransport, 2)
	if got := originTransport.count(); got != 1 {
		t.Fatalf("origin sends before edit result = %d, want one forward RPC result", got)
	}
	if got := siblingTransport.count(); got != 1 {
		t.Fatalf("sibling sends before edit result = %d, want one forward push", got)
	}
	if err := originConn.SendResultAndMarkRPCUpdate(editReq, editResult, alice.ID, originConn.AuthKeyID(), editUpdate.pts); err != nil {
		t.Fatalf("edit result: %v", err)
	}
	editAfter()
	waitTransportCount(t, siblingTransport, 2)
	time.Sleep(100 * time.Millisecond)
	if got := originTransport.count(); got != 2 {
		t.Fatalf("origin sends after edit = %d, want two RPC results", got)
	}
	editedSender, ok, err := s.MessageByOwnerLocal(ctx, alice.ID, forwarded.LocalID)
	if err != nil || !ok {
		t.Fatalf("load edited sender message: ok=%v err=%v", ok, err)
	}
	editedRecipient, ok, err := s.MessageByOwnerLocal(ctx, bob.ID, forwardedRecipient.LocalID)
	if err != nil || !ok {
		t.Fatalf("load edited recipient message: ok=%v err=%v", ok, err)
	}
	bobStateAfterEdit, err := s.State(ctx, bob.ID)
	if err != nil {
		t.Fatalf("load bob state after edit: %v", err)
	}
	editOriginFrames := decodeServerFrames(t, originKey, originTransport.framesFrom(1))
	editSiblingFrames := decodeServerFrames(t, siblingKey, siblingTransport.framesFrom(1))
	editRecipientFrames := decodeServerFrames(t, recipientKey, recipientTransport.framesFrom(1))
	if len(editOriginFrames) != 1 || editOriginFrames[0].rpc == nil || editOriginFrames[0].push != nil {
		t.Fatalf("edit origin frames = %+v, want one RPC result", editOriginFrames)
	}
	if len(editSiblingFrames) != 1 || editSiblingFrames[0].rpc != nil || editSiblingFrames[0].push == nil {
		t.Fatalf("edit sibling frames = %+v, want one push", editSiblingFrames)
	}
	if len(editRecipientFrames) != 1 || editRecipientFrames[0].rpc != nil || editRecipientFrames[0].push == nil {
		t.Fatalf("edit recipient frames = %+v, want one push", editRecipientFrames)
	}
	assertEditMessageFrame(t, editOriginFrames[0], editedSender, editUpdate.pts, "edit A1 RPC")
	assertEditMessageFrame(t, editSiblingFrames[0], editedSender, editUpdate.pts, "edit A2 push")
	assertEditMessageFrame(t, editRecipientFrames[0], editedRecipient, bobStateAfterEdit.Pts, "edit B push")

	// Catch the source notification up before staging the failed forward. That
	// leaves only the forward event behind the sender barrier, so the fallback
	// push can be asserted as exactly one durable update.
	if _, _, _, _, err := s.SendMessage(ctx, carol.ID, alice.ID, "failure source", 920004, 0, 0); err != nil {
		t.Fatalf("seed failure source message: %v", err)
	}
	h.notify(ctx, alice.ID)
	waitTransportCount(t, originTransport, 3)
	waitTransportCount(t, siblingTransport, 3)
	aliceBeforeFailedForward, err := s.State(ctx, alice.ID)
	if err != nil {
		t.Fatalf("load alice state before failed forward: %v", err)
	}
	bobBeforeFailedForward, err := s.State(ctx, bob.ID)
	if err != nil {
		t.Fatalf("load bob state before failed forward: %v", err)
	}
	forwardFailureReq := encode(&tg.MessagesForwardMessagesRequest{
		ToPeer:   InputPeerUser(alice.ID, bob.ID),
		FromPeer: InputPeerUser(alice.ID, carol.ID),
		ID:       []int{int(received.LocalID)},
		RandomID: []int64{920005},
	}, 3)
	forwardFailureResult, forwardFailureUpdate, _, err := h.handleForwardMessagesAfterReplyOnConn(originConn, forwardFailureReq)
	if err != nil {
		t.Fatalf("failed forward: %v", err)
	}
	if forwardFailureResult == nil || forwardFailureUpdate == nil || forwardFailureUpdate.onFailure == nil {
		t.Fatal("failed forward did not return result-failure metadata")
	}
	waitTransportCount(t, recipientTransport, 3)
	time.Sleep(100 * time.Millisecond)
	if got := originTransport.count(); got != 3 {
		t.Fatalf("origin pushes before failed forward result = %d, want 3", got)
	}
	if got := siblingTransport.count(); got != 3 {
		t.Fatalf("sibling pushes before failed forward result = %d, want 3", got)
	}
	forwardFailureUpdate.onFailure()
	waitTransportCount(t, originTransport, 4)
	waitTransportCount(t, siblingTransport, 4)
	failedForwardSender, ok, err := s.MessageByRandomID(ctx, alice.ID, 920005)
	if err != nil || !ok {
		t.Fatalf("load failed-forward sender message: ok=%v err=%v", ok, err)
	}
	failedForwardRecipient, ok, err := s.MessageByOwnerLocal(ctx, bob.ID, failedForwardSender.PeerLocalID)
	if err != nil || !ok {
		t.Fatalf("load failed-forward recipient message: ok=%v err=%v", ok, err)
	}
	failedForwardRecipientPts, err := s.MessagePts(ctx, bob.ID, failedForwardRecipient.LocalID)
	if err != nil {
		t.Fatalf("load failed-forward recipient pts: %v", err)
	}
	failedForwardOriginFrames := decodeServerFrames(t, originKey, originTransport.framesFrom(3))
	failedForwardSiblingFrames := decodeServerFrames(t, siblingKey, siblingTransport.framesFrom(3))
	failedForwardRecipientFrames := decodeServerFrames(t, recipientKey, recipientTransport.framesFrom(2))
	if len(failedForwardOriginFrames) != 1 || failedForwardOriginFrames[0].rpc != nil || failedForwardOriginFrames[0].push == nil {
		t.Fatalf("failed forward origin frames = %+v, want one fallback push", failedForwardOriginFrames)
	}
	if len(failedForwardSiblingFrames) != 1 || failedForwardSiblingFrames[0].rpc != nil || failedForwardSiblingFrames[0].push == nil {
		t.Fatalf("failed forward sibling frames = %+v, want one fallback push", failedForwardSiblingFrames)
	}
	if len(failedForwardRecipientFrames) != 1 || failedForwardRecipientFrames[0].rpc != nil || failedForwardRecipientFrames[0].push == nil {
		t.Fatalf("failed forward recipient frames = %+v, want one push", failedForwardRecipientFrames)
	}
	assertNewMessageFrame(t, failedForwardOriginFrames[0], failedForwardSender, forwardFailureUpdate.pts, "failed forward A1 fallback")
	assertNewMessageFrame(t, failedForwardSiblingFrames[0], failedForwardSender, forwardFailureUpdate.pts, "failed forward A2 fallback")
	assertNewMessageFrame(t, failedForwardRecipientFrames[0], failedForwardRecipient, failedForwardRecipientPts, "failed forward B push")
	assertDifferenceNewMessage(t, getDifferenceForPts(t, h, ctx, alice.ID, aliceBeforeFailedForward.Pts), failedForwardSender, forwardFailureUpdate.pts, "failed forward A1 recovery")
	assertDifferenceNewMessage(t, getDifferenceForPts(t, h, ctx, bob.ID, bobBeforeFailedForward.Pts), failedForwardRecipient, failedForwardRecipientPts, "failed forward B recovery")

	aliceBeforeFailedEdit, err := s.State(ctx, alice.ID)
	if err != nil {
		t.Fatalf("load alice state before failed edit: %v", err)
	}
	bobBeforeFailedEdit, err := s.State(ctx, bob.ID)
	if err != nil {
		t.Fatalf("load bob state before failed edit: %v", err)
	}
	editFailureReq := encode(&tg.MessagesEditMessageRequest{
		Peer:    InputPeerUser(alice.ID, bob.ID),
		ID:      int(failedForwardSender.LocalID),
		Message: "edited after failed forward",
	}, 4)
	editFailureResult, editFailureUpdate, _, err := h.handleEditMessageAfterReplyOnConn(originConn, editFailureReq)
	if err != nil {
		t.Fatalf("failed edit: %v", err)
	}
	if editFailureResult == nil || editFailureUpdate == nil || editFailureUpdate.onFailure == nil {
		t.Fatal("failed edit did not return result-failure metadata")
	}
	waitTransportCount(t, recipientTransport, 4)
	time.Sleep(100 * time.Millisecond)
	if got := originTransport.count(); got != 4 {
		t.Fatalf("origin pushes before failed edit result = %d, want 4", got)
	}
	if got := siblingTransport.count(); got != 4 {
		t.Fatalf("sibling pushes before failed edit result = %d, want 4", got)
	}
	editFailureUpdate.onFailure()
	waitTransportCount(t, originTransport, 5)
	waitTransportCount(t, siblingTransport, 5)
	failedEditSender, ok, err := s.MessageByOwnerLocal(ctx, alice.ID, failedForwardSender.LocalID)
	if err != nil || !ok {
		t.Fatalf("load failed-edit sender message: ok=%v err=%v", ok, err)
	}
	failedEditRecipient, ok, err := s.MessageByOwnerLocal(ctx, bob.ID, failedForwardRecipient.LocalID)
	if err != nil || !ok {
		t.Fatalf("load failed-edit recipient message: ok=%v err=%v", ok, err)
	}
	bobAfterFailedEdit, err := s.State(ctx, bob.ID)
	if err != nil {
		t.Fatalf("load bob state after failed edit: %v", err)
	}
	failedEditOriginFrames := decodeServerFrames(t, originKey, originTransport.framesFrom(4))
	failedEditSiblingFrames := decodeServerFrames(t, siblingKey, siblingTransport.framesFrom(4))
	failedEditRecipientFrames := decodeServerFrames(t, recipientKey, recipientTransport.framesFrom(3))
	if len(failedEditOriginFrames) != 1 || failedEditOriginFrames[0].rpc != nil || failedEditOriginFrames[0].push == nil {
		t.Fatalf("failed edit origin frames = %+v, want one fallback push", failedEditOriginFrames)
	}
	if len(failedEditSiblingFrames) != 1 || failedEditSiblingFrames[0].rpc != nil || failedEditSiblingFrames[0].push == nil {
		t.Fatalf("failed edit sibling frames = %+v, want one fallback push", failedEditSiblingFrames)
	}
	if len(failedEditRecipientFrames) != 1 || failedEditRecipientFrames[0].rpc != nil || failedEditRecipientFrames[0].push == nil {
		t.Fatalf("failed edit recipient frames = %+v, want one push", failedEditRecipientFrames)
	}
	assertEditMessageFrame(t, failedEditOriginFrames[0], failedEditSender, editFailureUpdate.pts, "failed edit A1 fallback")
	assertEditMessageFrame(t, failedEditSiblingFrames[0], failedEditSender, editFailureUpdate.pts, "failed edit A2 fallback")
	assertEditMessageFrame(t, failedEditRecipientFrames[0], failedEditRecipient, bobAfterFailedEdit.Pts, "failed edit B push")
	assertDifferenceEditMessage(t, getDifferenceForPts(t, h, ctx, alice.ID, aliceBeforeFailedEdit.Pts), failedEditSender, editFailureUpdate.pts, "failed edit A1 recovery")
	assertDifferenceEditMessage(t, getDifferenceForPts(t, h, ctx, bob.ID, bobBeforeFailedEdit.Pts), failedEditRecipient, bobAfterFailedEdit.Pts, "failed edit B recovery")

	if _, _, _, _, err := s.SendMessage(ctx, alice.ID, bob.ID, "after edit", 920003, 0, 0); err != nil {
		t.Fatalf("later send: %v", err)
	}
	h.notify(ctx, alice.ID)
	h.notify(ctx, bob.ID)
	waitTransportCount(t, originTransport, 6)
	waitTransportCount(t, siblingTransport, 6)
	waitTransportCount(t, recipientTransport, 5)
	laterSender, ok, err := s.MessageByRandomID(ctx, alice.ID, 920003)
	if err != nil || !ok {
		t.Fatalf("load later sender message: ok=%v err=%v", ok, err)
	}
	laterRecipient, ok, err := s.MessageByOwnerLocal(ctx, bob.ID, laterSender.PeerLocalID)
	if err != nil || !ok {
		t.Fatalf("load later recipient message: ok=%v err=%v", ok, err)
	}
	laterSenderPts, err := s.MessagePts(ctx, alice.ID, laterSender.LocalID)
	if err != nil {
		t.Fatalf("load later sender pts: %v", err)
	}
	laterRecipientPts, err := s.MessagePts(ctx, bob.ID, laterRecipient.LocalID)
	if err != nil {
		t.Fatalf("load later recipient pts: %v", err)
	}
	laterOriginFrames := decodeServerFrames(t, originKey, originTransport.framesFrom(5))
	laterSiblingFrames := decodeServerFrames(t, siblingKey, siblingTransport.framesFrom(5))
	laterRecipientFrames := decodeServerFrames(t, recipientKey, recipientTransport.framesFrom(4))
	if len(laterOriginFrames) != 1 || laterOriginFrames[0].rpc != nil || laterOriginFrames[0].push == nil {
		t.Fatalf("later origin frames = %+v, want one push", laterOriginFrames)
	}
	if len(laterSiblingFrames) != 1 || laterSiblingFrames[0].rpc != nil || laterSiblingFrames[0].push == nil {
		t.Fatalf("later sibling frames = %+v, want one push", laterSiblingFrames)
	}
	if len(laterRecipientFrames) != 1 || laterRecipientFrames[0].rpc != nil || laterRecipientFrames[0].push == nil {
		t.Fatalf("later recipient frames = %+v, want one push", laterRecipientFrames)
	}
	assertNewMessageFrame(t, laterOriginFrames[0], laterSender, laterSenderPts, "later A1 push")
	assertNewMessageFrame(t, laterSiblingFrames[0], laterSender, laterSenderPts, "later A2 push")
	assertNewMessageFrame(t, laterRecipientFrames[0], laterRecipient, laterRecipientPts, "later B push")
}

func waitTransportCount(t *testing.T, transport interface{ count() int }, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for transport.count() < want && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := transport.count(); got < want {
		t.Fatalf("transport sends = %d, want at least %d", got, want)
	}
}
