package store_test

import (
	"context"
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
