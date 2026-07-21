package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func mustUser(t *testing.T, s *store.Store, phone string) store.User {
	t.Helper()
	u, err := s.CreateUser(context.Background(), phone)
	if err != nil {
		t.Fatalf("create user %s: %v", phone, err)
	}
	return u
}

func send(t *testing.T, s *store.Store, from, to store.User, text string, rid int64) store.Message {
	t.Helper()
	m, _, _, _, err := s.SendMessage(context.Background(), from.ID, to.ID, text, rid) //nolint:dogsled // only the stored message is needed here
	if err != nil {
		t.Fatalf("send %q: %v", text, err)
	}
	return m
}

func msgAt(t *testing.T, s *store.Store, ownerID, localID int64) store.Message {
	t.Helper()
	m, ok, err := s.MessageByOwnerLocal(context.Background(), ownerID, localID)
	if err != nil || !ok {
		t.Fatalf("message (%d,%d): ok=%v err=%v", ownerID, localID, ok, err)
	}
	return m
}

func TestSendMessageTwoSided(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551240001")
	b := mustUser(t, s, "+15551240002")

	sender, senderPts, recipientPts, dup, err := s.SendMessage(ctx, a.ID, b.ID, "hi", 111)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if dup {
		t.Fatal("first send flagged dup")
	}
	if senderPts != 1 || recipientPts != 1 {
		t.Fatalf("pts = sender %d recipient %d, want 1,1", senderPts, recipientPts)
	}
	if !sender.Out || sender.OwnerID != a.ID || sender.PeerID != b.ID || sender.FromID != a.ID {
		t.Fatalf("sender row wrong: %+v", sender)
	}
	if sender.LocalID != 1 {
		t.Fatalf("sender local_id = %d, want 1", sender.LocalID)
	}
	if sender.Text != "hi" {
		t.Fatalf("sender text = %q", sender.Text)
	}

	// Recipient's inbox copy has its own local_id space, also starting at 1.
	recv, ok, err := s.MessageByOwnerLocal(ctx, b.ID, 1)
	if err != nil || !ok {
		t.Fatalf("recipient row: ok=%v err=%v", ok, err)
	}
	if recv.Out || recv.OwnerID != b.ID || recv.PeerID != a.ID || recv.FromID != a.ID {
		t.Fatalf("recipient row wrong: %+v", recv)
	}
	if recv.Text != "hi" {
		t.Fatalf("recipient text = %q", recv.Text)
	}

	// One new-message event per owner.
	for _, u := range []store.User{a, b} {
		ev, err := s.EventsSince(ctx, u.ID, 0)
		if err != nil {
			t.Fatalf("events %d: %v", u.ID, err)
		}
		if len(ev) != 1 || ev[0].Type != store.EventNewMessage {
			t.Fatalf("owner %d events = %+v, want one new-message", u.ID, ev)
		}
	}
}

func TestSendMessageRandomIDDedup(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551240011")
	b := mustUser(t, s, "+15551240012")

	first, _, _, dup, err := s.SendMessage(ctx, a.ID, b.ID, "once", 999)
	if err != nil || dup {
		t.Fatalf("first send: dup=%v err=%v", dup, err)
	}
	again, sPts, rPts, dup, err := s.SendMessage(ctx, a.ID, b.ID, "once", 999)
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	if !dup {
		t.Fatal("resend with same random_id not flagged dup")
	}
	if again.LocalID != first.LocalID {
		t.Fatalf("dup returned local_id %d, want original %d", again.LocalID, first.LocalID)
	}
	// pts must not advance on a dedup'd resend.
	if sPts != 1 || rPts != 1 {
		t.Fatalf("dup pts = sender %d recipient %d, want unchanged 1,1", sPts, rPts)
	}
	st, err := s.State(ctx, a.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if st.Pts != 1 {
		t.Fatalf("sender pts after dup = %d, want 1", st.Pts)
	}
}

func TestHistoryPaging(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551250001")
	b := mustUser(t, s, "+15551250002")

	for i, txt := range []string{"m1", "m2", "m3"} {
		send(t, s, a, b, txt, int64(1000+i))
	}

	all, err := s.History(ctx, a.ID, b.ID, 0, 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("history len = %d, want 3", len(all))
	}
	if all[0].LocalID != 3 || all[2].LocalID != 1 {
		t.Fatalf("history not newest-first: %d..%d", all[0].LocalID, all[2].LocalID)
	}

	older, err := s.History(ctx, a.ID, b.ID, 2, 10) // strictly older than local_id 2
	if err != nil {
		t.Fatalf("history page: %v", err)
	}
	if len(older) != 1 || older[0].LocalID != 1 {
		t.Fatalf("paged history = %+v, want only local_id 1", older)
	}
}

func TestEditMessageBothSides(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551250011")
	b := mustUser(t, s, "+15551250012")

	sender := send(t, s, a, b, "orig", 1)

	peerID, newPts, err := s.EditMessage(ctx, a.ID, sender.LocalID, "edited")
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if peerID != b.ID {
		t.Fatalf("edit peer = %d, want %d", peerID, b.ID)
	}
	if newPts != 2 {
		t.Fatalf("editor pts = %d, want 2", newPts)
	}

	own := msgAt(t, s, a.ID, sender.LocalID)
	if own.Text != "edited" || own.EditDate == nil {
		t.Fatalf("owner row not edited: %+v", own)
	}
	mirror := msgAt(t, s, b.ID, sender.PeerLocalID)
	if mirror.Text != "edited" || mirror.EditDate == nil {
		t.Fatalf("mirror row not edited: %+v", mirror)
	}

	// Edit events on both owners.
	for _, u := range []int64{a.ID, b.ID} {
		ev, err := s.EventsSince(ctx, u, 1)
		if err != nil {
			t.Fatalf("events owner %d: %v", u, err)
		}
		if len(ev) != 1 || ev[0].Type != store.EventEdit {
			t.Fatalf("owner %d edit event = %+v", u, ev)
		}
	}

	// Non-owned / inbound edits fail closed.
	if _, _, err := s.EditMessage(ctx, a.ID, 999, "x"); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("edit absent: want ErrMessageInvalid, got %v", err)
	}
	if _, _, err := s.EditMessage(ctx, b.ID, sender.PeerLocalID, "x"); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("edit inbound: want ErrMessageInvalid, got %v", err)
	}
}

func TestDeleteMessagesBothSides(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551250021")
	b := mustUser(t, s, "+15551250022")

	sender := send(t, s, a, b, "bye", 1)

	perOwner, err := s.DeleteMessages(ctx, a.ID, []int64{sender.LocalID})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if perOwner[a.ID] != 2 || perOwner[b.ID] != 2 {
		t.Fatalf("delete pts = %+v, want a,b at 2", perOwner)
	}

	own := msgAt(t, s, a.ID, sender.LocalID)
	if !own.Deleted {
		t.Fatal("owner row not deleted")
	}
	mirror := msgAt(t, s, b.ID, sender.PeerLocalID)
	if !mirror.Deleted {
		t.Fatal("mirror row not deleted")
	}
	for _, u := range []int64{a.ID, b.ID} {
		ev, err := s.EventsSince(ctx, u, 1)
		if err != nil {
			t.Fatalf("events owner %d: %v", u, err)
		}
		if len(ev) != 1 || ev[0].Type != store.EventDelete {
			t.Fatalf("owner %d delete event = %+v", u, ev)
		}
	}

	if _, err := s.DeleteMessages(ctx, a.ID, []int64{999}); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("delete absent: want ErrMessageInvalid, got %v", err)
	}
}
