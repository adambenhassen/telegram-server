package store_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func dialogWith(t *testing.T, s *store.Store, ownerID, peerID int64) store.Dialog {
	t.Helper()
	ds, err := s.Dialogs(context.Background(), ownerID, 0, 100)
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

// A page is bounded by limit, walks strictly older with no overlap and no gap,
// and runs off the end as a short page then an empty one. CountDialogs stays the
// whole-list total throughout, since that is what a truncated reply advertises.
func TestDialogsPaging(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	const total = 25
	owner := mustUser(t, s, "+15551263000")
	for i := range total {
		peer := mustUser(t, s, fmt.Sprintf("+1555126%04d", 3001+i))
		// The peer sends, so the owner gets an inbox copy and a dialog whose
		// top_message is the owner's own local_id — 1..total in this order.
		send(t, s, peer, owner, "hi", int64(i+1))
	}

	n, err := s.CountDialogs(ctx, owner.ID)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != total {
		t.Fatalf("count = %d, want %d", n, total)
	}

	const page = 10
	first, err := s.Dialogs(ctx, owner.ID, 0, page)
	if err != nil {
		t.Fatalf("first page: %v", err)
	}
	if len(first) != page {
		t.Fatalf("first page = %d dialogs, want %d", len(first), page)
	}
	if first[0].TopMessage != total {
		t.Fatalf("first page starts at top_message %d, want %d", first[0].TopMessage, total)
	}

	// Walk to the end, collecting every top_message in order.
	seen := []int64{}
	offset := int64(0)
	pages := 0
	for {
		got, perr := s.Dialogs(ctx, owner.ID, offset, page)
		if perr != nil {
			t.Fatalf("page at offset %d: %v", offset, perr)
		}
		pages++
		if len(got) == 0 {
			break
		}
		for i, d := range got {
			if i > 0 && d.TopMessage >= got[i-1].TopMessage {
				t.Fatalf("page at offset %d not newest-first: %+v", offset, got)
			}
			seen = append(seen, d.TopMessage)
		}
		if len(got) > page {
			t.Fatalf("page at offset %d = %d dialogs, over limit %d", offset, len(got), page)
		}
		offset = got[len(got)-1].TopMessage

		// The count never follows the offset: it is the size of the whole list.
		if c, cerr := s.CountDialogs(ctx, owner.ID); cerr != nil || c != total {
			t.Fatalf("count at offset %d = %d (err %v), want %d", offset, c, cerr, total)
		}
	}
	// 10 + 10 + 5 (short page, end of list) + 1 empty page.
	if pages != 4 {
		t.Fatalf("walked %d pages, want 4", pages)
	}
	if len(seen) != total {
		t.Fatalf("walk saw %d dialogs, want %d — overlap or gap", len(seen), total)
	}
	for i, top := range seen {
		if want := int64(total - i); top != want {
			t.Fatalf("walk position %d = top_message %d, want %d", i, top, want)
		}
	}

	// offsetID pages strictly older: the row at the offset is excluded.
	next, err := s.Dialogs(ctx, owner.ID, first[page-1].TopMessage, page)
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if next[0].TopMessage != first[page-1].TopMessage-1 {
		t.Fatalf("second page starts at %d, want %d", next[0].TopMessage, first[page-1].TopMessage-1)
	}
}
