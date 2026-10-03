package store_test

import (
	"context"
	"testing"

	"github.com/teagramhq/teagram-server/internal/store"
)

func TestSendMessageToSelfStoresOneMessageAndDeduplicates(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	user := mustUser(t, s, "+15551249001")

	first, senderPts, peerPts, dup, err := s.SendMessage(ctx, user.ID, user.ID, "saved one", 4901, 0, 0)
	if err != nil {
		t.Fatalf("send to self: %v", err)
	}
	if dup {
		t.Fatal("first self send flagged dup")
	}
	if senderPts != 1 || peerPts != 1 {
		t.Fatalf("pts = sender %d peer %d, want 1,1", senderPts, peerPts)
	}
	if first.LocalID != 1 || first.OwnerID != user.ID || first.PeerType != store.PeerTypeUser || first.PeerID != user.ID || first.FromID != user.ID || !first.Out {
		t.Fatalf("self row = %+v, want one outgoing user row owned by the account", first)
	}

	rows, err := s.History(ctx, user.ID, store.PeerTypeUser, user.ID, 0, 20)
	if err != nil {
		t.Fatalf("self history: %v", err)
	}
	if len(rows) != 1 || rows[0].LocalID != first.LocalID {
		t.Fatalf("self history rows = %+v, want exactly the first row", rows)
	}

	state, err := s.State(ctx, user.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if state.Pts != 1 {
		t.Fatalf("pts = %d, want 1", state.Pts)
	}
	events, err := s.EventsSince(ctx, user.ID, 0)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 1 || events[0].Type != store.EventNewMessage {
		t.Fatalf("events = %+v, want one new-message event", events)
	}

	dialog := dialogWith(t, s, user.ID, user.ID)
	if dialog.TopMessage != first.LocalID || dialog.UnreadCount != 0 {
		t.Fatalf("self dialog = %+v, want top_message %d and no unread messages", dialog, first.LocalID)
	}

	second, secondSenderPts, secondPeerPts, dup, err := s.SendMessage(ctx, user.ID, user.ID, "saved one", 4901, 0, 0)
	if err != nil {
		t.Fatalf("retry self send: %v", err)
	}
	if !dup || second.LocalID != first.LocalID || secondSenderPts != 1 || secondPeerPts != 1 {
		t.Fatalf("retry = row %+v pts %d,%d dup=%v, want original row and pts 1,1", second, secondSenderPts, secondPeerPts, dup)
	}
	rows, err = s.History(ctx, user.ID, store.PeerTypeUser, user.ID, 0, 20)
	if err != nil {
		t.Fatalf("self history after retry: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("self history after retry has %d rows, want 1", len(rows))
	}
	state, err = s.State(ctx, user.ID)
	if err != nil {
		t.Fatalf("state after retry: %v", err)
	}
	events, err = s.EventsSince(ctx, user.ID, 0)
	if err != nil {
		t.Fatalf("events after retry: %v", err)
	}
	if state.Pts != 1 || len(events) != 1 {
		t.Fatalf("retry changed pts/events: pts=%d events=%d, want 1/1", state.Pts, len(events))
	}
	dialogs, err := s.Dialogs(ctx, user.ID, 0, 20)
	if err != nil {
		t.Fatalf("dialogs after retry: %v", err)
	}
	if len(dialogs) != 1 || dialogs[0].PeerID != user.ID || dialogs[0].TopMessage != first.LocalID {
		t.Fatalf("dialogs after retry = %+v, want the original single self dialog", dialogs)
	}
}

func TestSelfMessageOperationsDoNotMirrorWithinOwner(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551249002")
	b := mustUser(t, s, "+15551249003")

	self := send(t, s, a, a, "saved", 4902)
	other := send(t, s, a, b, "other dialog", 4903)

	if peerID, pts, err := s.EditMessage(ctx, a.ID, self.LocalID, "edited saved"); err != nil {
		t.Fatalf("edit self message: %v", err)
	} else if peerID != a.ID || pts != 3 {
		t.Fatalf("edit self result = peer %d pts %d, want peer %d pts 3", peerID, pts, a.ID)
	}
	if got := msgAt(t, s, a.ID, other.LocalID); got.Text != "other dialog" {
		t.Fatalf("editing self row changed another dialog row: %+v", got)
	}

	targets, err := s.SendReaction(ctx, a.ID, self.LocalID, "❤")
	if err != nil {
		t.Fatalf("react to self message: %v", err)
	}
	if len(targets) != 1 || targets[0] != (store.ReactionTarget{OwnerID: a.ID, LocalID: self.LocalID}) {
		t.Fatalf("reaction targets = %+v, want one self row", targets)
	}
	if got := msgAt(t, s, a.ID, other.LocalID); got.Text != "other dialog" {
		t.Fatalf("reacting to self row changed another dialog row: %+v", got)
	}

	readPts, peerPts, err := s.ReadHistory(ctx, a.ID, a.ID, self.LocalID)
	if err != nil {
		t.Fatalf("read self history: %v", err)
	}
	if readPts != 4 || peerPts != 4 {
		t.Fatalf("read self pts = %d,%d, want 4,4", readPts, peerPts)
	}
	if got := dialogWith(t, s, a.ID, a.ID); got.UnreadCount != 0 || got.ReadInboxMaxID != self.LocalID || got.ReadOutboxMaxID != self.LocalID {
		t.Fatalf("self dialog after read = %+v, want no unread and both markers through %d", got, self.LocalID)
	}
	if got := msgAt(t, s, a.ID, other.LocalID); got.Text != "other dialog" {
		t.Fatalf("reading self row changed another dialog row: %+v", got)
	}

	if _, err := s.DeleteMessages(ctx, a.ID, []int64{self.LocalID}, true); err != nil {
		t.Fatalf("revoke self message: %v", err)
	}
	if got := msgAt(t, s, a.ID, other.LocalID); got.Text != "other dialog" || got.Deleted {
		t.Fatalf("revoking self row changed another dialog row: %+v", got)
	}
	rows, err := s.History(ctx, a.ID, store.PeerTypeUser, a.ID, 0, 20)
	if err != nil {
		t.Fatalf("self history after revoke: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("self history after revoke = %+v, want no visible self rows", rows)
	}
	state, err := s.State(ctx, a.ID)
	if err != nil {
		t.Fatalf("state after revoke: %v", err)
	}
	if state.Pts != 5 {
		t.Fatalf("pts after revoke = %d, want 5 (one bump per self operation)", state.Pts)
	}
}
