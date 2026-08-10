package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
)

// The pts a retry reports is an update-delivery contract, not a cosmetic field.
// updateNewMessage carries pts with pts_count 1, and a client applies it when
// its own pts plus that count equals the pts it received. Report the sender's
// *current* pts for an old message and a client sitting exactly one update
// behind applies that old message into the pts slot of a newer update it never
// saw, then counts itself caught up: the newer update is gone, and there is no
// gap left for getDifference to find.
//
// So every test below is the same shape. Send, advance the sender's pts with a
// second update, resend the first random_id, and require the reply to name the
// pts the stored message actually occupies.

// newMessagePts pulls the pts off the updateNewMessage in a send reply.
func newMessagePts(t *testing.T, enc bin.Encoder) int {
	t.Helper()
	ups := updatesOf(t, enc)
	for _, u := range ups.Updates {
		if nm, isNew := u.(*tg.UpdateNewMessage); isNew {
			if nm.PtsCount != 1 {
				t.Errorf("updateNewMessage pts_count = %d, want 1", nm.PtsCount)
			}
			return nm.Pts
		}
	}
	t.Fatalf("no updateNewMessage in %+v", ups.Updates)
	return 0
}

// assertNoSkip is the property every retry has to hold: a client whose pts is
// one short of the intervening update must not be told it is caught up. It is
// the reply's pts that decides that, so the reply must name the stored
// message's own pts and nothing later.
func assertNoSkip(t *testing.T, path string, replyPts, storedPts, currentPts int) {
	t.Helper()
	if storedPts >= currentPts {
		t.Fatalf("%s: stored pts %d is not behind current pts %d — the test set up no intervening update", path, storedPts, currentPts)
	}
	if replyPts != storedPts {
		t.Errorf("%s: retry reported pts %d, want the stored message's %d (current is %d)", path, replyPts, storedPts, currentPts)
	}
}

func TestSendMessageRetryReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551299101")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551299102")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	peer := api.InputPeerUser(a.ID, b.ID)

	first, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "one", RandomID: 101,
	})
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	storedPts := newMessagePts(t, first)

	// The intervening update the retry must not let a client skip.
	if _, err = api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "two", RandomID: 102,
	}); err != nil {
		t.Fatalf("second send: %v", err)
	}
	st, err := s.State(ctx, a.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}

	again, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "one", RandomID: 101,
	})
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	assertEncodes(t, again)
	if msgID(t, first) != msgID(t, again) {
		t.Errorf("resend id = %d, want %d", msgID(t, again), msgID(t, first))
	}
	assertNoSkip(t, "1:1 send", newMessagePts(t, again), storedPts, st.Pts)
}

func TestSendChatMessageRetryReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551299111", "+15551299112", "+15551299113")
	peer := &tg.InputPeerChat{ChatID: chat.ID}

	first, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "one", RandomID: 111,
	})
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	storedPts := newMessagePts(t, first)

	if _, err = api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "two", RandomID: 112,
	}); err != nil {
		t.Fatalf("second send: %v", err)
	}
	st, err := s.State(ctx, users[0].ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}

	again, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "one", RandomID: 111,
	})
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	assertEncodes(t, again)
	assertNoSkip(t, "chat send", newMessagePts(t, again), storedPts, st.Pts)
}

func TestSendChannelMessageRetryReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551299121")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}

	first, err := sendToChannel(t, s, creator.ID, ch.ID, "one", 121)
	if err != nil {
		t.Fatalf("first post: %v", err)
	}
	storedPts := newChannelMessage(t, first).Pts

	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "two", 122); err != nil {
		t.Fatalf("second post: %v", err)
	}
	current, err := s.ChannelState(ctx, ch.ID)
	if err != nil {
		t.Fatalf("channel state: %v", err)
	}

	again, err := sendToChannel(t, s, creator.ID, ch.ID, "one", 121)
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	assertEncodes(t, again)
	nm := newChannelMessage(t, again)
	if nm.PtsCount != 1 {
		t.Errorf("updateNewChannelMessage pts_count = %d, want 1", nm.PtsCount)
	}
	assertNoSkip(t, "channel post", nm.Pts, storedPts, current)
}

func TestSendMediaRetryReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551299131")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551299132")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	peer := api.InputPeerUser(a.ID, b.ID)
	saveParts(t, s, a.ID, 1310, []byte("one"))

	req := &tg.MessagesSendMediaRequest{
		Peer:     peer,
		Media:    uploadedDocument(1310, 1, "one.txt", "text/plain"),
		Message:  "look",
		RandomID: 131,
	}
	first, err := api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, req)
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	storedPts := newMessagePts(t, first)

	if _, err = api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: peer, Message: "two", RandomID: 132,
	}); err != nil {
		t.Fatalf("second send: %v", err)
	}
	st, err := s.State(ctx, a.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}

	// The parts are gone — assembly consumed them — so this reply can only come
	// from the dedupe path.
	again, err := api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, req)
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	assertEncodes(t, again)
	assertNoSkip(t, "media send", newMessagePts(t, again), storedPts, st.Pts)
}

func TestForwardRetryReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551299141")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551299142")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	c, err := s.CreateUser(ctx, "+15551299143")
	if err != nil {
		t.Fatalf("user c: %v", err)
	}
	peerB := api.InputPeerUser(a.ID, b.ID)
	peerC := api.InputPeerUser(a.ID, c.ID)

	if _, err = api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: peerB, Message: "original", RandomID: 141,
	}); err != nil {
		t.Fatalf("seed send: %v", err)
	}
	fwd := &tg.MessagesForwardMessagesRequest{
		ToPeer: peerC, FromPeer: peerB, ID: []int{1}, RandomID: []int64{142},
	}
	first, err := api.ForwardMessagesForTest(s, a.ID, fwd)
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	storedPts := newMessagePts(t, first)

	if _, err = api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: peerB, Message: "after", RandomID: 143,
	}); err != nil {
		t.Fatalf("second send: %v", err)
	}
	st, err := s.State(ctx, a.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}

	again, err := api.ForwardMessagesForTest(s, a.ID, fwd)
	if err != nil {
		t.Fatalf("forward retry: %v", err)
	}
	assertEncodes(t, again)
	assertNoSkip(t, "forward", newMessagePts(t, again), storedPts, st.Pts)
}

// TestRetryAgreesWithTheStoreDedupe holds the two halves together. The handler
// answers a retry from its own read, and the store answers one from the dedupe
// branch inside the send transaction when two resends race past that read. They
// are separate code paths reporting the same field, so a client must not be
// able to tell which one served it.
func TestRetryAgreesWithTheStoreDedupe(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551299151")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551299152")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	if _, err = api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(a.ID, b.ID), Message: "one", RandomID: 151,
	}); err != nil {
		t.Fatalf("first send: %v", err)
	}
	if _, err = api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(a.ID, b.ID), Message: "two", RandomID: 152,
	}); err != nil {
		t.Fatalf("second send: %v", err)
	}

	// The store's dedupe branch, reached directly.
	_, storePts, _, dup, err := s.SendMessage(ctx, a.ID, b.ID, "one", 151, 0, 0)
	if err != nil || !dup {
		t.Fatalf("store resend: dup=%v err=%v", dup, err)
	}
	again, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(a.ID, b.ID), Message: "one", RandomID: 151,
	})
	if err != nil {
		t.Fatalf("handler resend: %v", err)
	}
	if handlerPts := newMessagePts(t, again); handlerPts != storePts {
		t.Errorf("handler retry pts = %d, store dedupe pts = %d — they must agree", handlerPts, storePts)
	}
	st, err := s.State(ctx, a.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if storePts >= st.Pts {
		t.Errorf("store dedupe pts = %d, want the stored message's pts, behind the current %d", storePts, st.Pts)
	}
}

// TestOrdinarySendPtsIsUnchanged pins the non-retry contract the fix must not
// touch: consecutive sends report consecutive pts, one per update, and the
// reply's pts is the sender's new state.
func TestOrdinarySendPtsIsUnchanged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551299161")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551299162")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	peer := api.InputPeerUser(a.ID, b.ID)

	for i, want := range []int{1, 2, 3} {
		enc, serr := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
			Peer: peer, Message: "m", RandomID: int64(161 + i),
		})
		if serr != nil {
			t.Fatalf("send %d: %v", i, serr)
		}
		if got := newMessagePts(t, enc); got != want {
			t.Errorf("send %d reported pts %d, want %d", i, got, want)
		}
		st, serr := s.State(ctx, a.ID)
		if serr != nil {
			t.Fatalf("state %d: %v", i, serr)
		}
		if st.Pts != want {
			t.Errorf("sender pts after send %d = %d, want %d", i, st.Pts, want)
		}
	}

	// The recipient's side is its own pts space and advances the same way.
	st, err := s.State(ctx, b.ID)
	if err != nil {
		t.Fatalf("recipient state: %v", err)
	}
	if st.Pts != 3 {
		t.Errorf("recipient pts = %d, want 3", st.Pts)
	}
	if _, ok, err := s.MessageByRandomID(ctx, a.ID, 161); err != nil || !ok {
		t.Fatalf("stored message missing: ok=%v err=%v", ok, err)
	}
}
