package store_test

import (
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func dialogWith(t *testing.T, s *store.Store, ownerID, peerID int64) store.Dialog {
	t.Helper()
	ds, err := s.Dialogs(context.Background(), ownerID)
	if err != nil {
		t.Fatalf("dialogs %d: %v", ownerID, err)
	}
	for _, d := range ds {
		if d.PeerID == peerID {
			return d
		}
	}
	t.Fatalf("no dialog owner=%d peer=%d in %+v", ownerID, peerID, ds)
	return store.Dialog{}
}

func TestDialogsAndReadState(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551260001")
	b := mustUser(t, s, "+15551260002")

	recvSide := send(t, s, a, b, "yo", 1) // sender copy; B's inbox copy local_id 1
	_ = recvSide

	bd := dialogWith(t, s, b.ID, a.ID)
	if bd.UnreadCount != 1 {
		t.Fatalf("B unread = %d, want 1", bd.UnreadCount)
	}
	if bd.TopMessage != 1 {
		t.Fatalf("B top_message = %d, want 1", bd.TopMessage)
	}

	readerPts, peerPts, err := s.ReadHistory(ctx, b.ID, a.ID, 1)
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	if readerPts != 2 || peerPts != 2 {
		t.Fatalf("read pts = reader %d peer %d, want 2,2", readerPts, peerPts)
	}

	bd = dialogWith(t, s, b.ID, a.ID)
	if bd.UnreadCount != 0 {
		t.Fatalf("B unread after read = %d, want 0", bd.UnreadCount)
	}
	if bd.ReadInboxMaxID != 1 {
		t.Fatalf("B read_inbox_max_id = %d, want 1", bd.ReadInboxMaxID)
	}
	ad := dialogWith(t, s, a.ID, b.ID)
	if ad.ReadOutboxMaxID != 1 {
		t.Fatalf("A read_outbox_max_id = %d, want 1", ad.ReadOutboxMaxID)
	}

	// Read events emitted for both owners.
	be, err := s.EventsSince(ctx, b.ID, 1)
	if err != nil {
		t.Fatalf("B events: %v", err)
	}
	if len(be) != 1 || be[0].Type != store.EventReadIn {
		t.Fatalf("B read event = %+v", be)
	}
	ae, err := s.EventsSince(ctx, a.ID, 1)
	if err != nil {
		t.Fatalf("A events: %v", err)
	}
	if len(ae) != 1 || ae[0].Type != store.EventReadOut {
		t.Fatalf("A read event = %+v", ae)
	}

	// Re-reading with a lower maxID must not regress the read marker.
	if _, _, err := s.ReadHistory(ctx, b.ID, a.ID, 0); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	bd = dialogWith(t, s, b.ID, a.ID)
	if bd.ReadInboxMaxID != 1 {
		t.Fatalf("B read_inbox_max_id regressed to %d, want 1", bd.ReadInboxMaxID)
	}
}
