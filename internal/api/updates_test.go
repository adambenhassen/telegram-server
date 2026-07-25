package api_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestMain(m *testing.M) {
	if err := pgtest.Prewarm(); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest prewarm: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func TestBuildUpdatesNewMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	a, err := s.CreateUser(ctx, "+15551290001")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551290002")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "hi", 7); err != nil {
		t.Fatalf("send: %v", err)
	}

	ups, users, state, err := api.BuildUpdatesForTest(s, b.ID, 0)
	if err != nil {
		t.Fatalf("build updates: %v", err)
	}
	if state.Pts != 1 {
		t.Fatalf("state pts = %d, want 1", state.Pts)
	}
	if len(ups) != 1 {
		t.Fatalf("updates = %d, want 1", len(ups))
	}
	nm, ok := ups[0].(*tg.UpdateNewMessage)
	if !ok {
		t.Fatalf("update type = %T, want *tg.UpdateNewMessage", ups[0])
	}
	msg, ok := nm.Message.(*tg.Message)
	if !ok {
		t.Fatalf("message type = %T", nm.Message)
	}
	if msg.Message != "hi" || msg.Out {
		t.Fatalf("message = %q out=%v, want \"hi\" out=false", msg.Message, msg.Out)
	}
	peer, ok := msg.PeerID.(*tg.PeerUser)
	if !ok || peer.UserID != a.ID {
		t.Fatalf("peer = %+v, want PeerUser %d", msg.PeerID, a.ID)
	}
	if len(users) == 0 {
		t.Fatal("no users hydrated")
	}
	// The recipient's copy has from == peer == a, so only the sender is hydrated
	// here: a stranger's phone number must not come with them.
	for _, uc := range users {
		u, ok := uc.(*tg.User)
		if !ok {
			t.Fatalf("user type = %T, want *tg.User", uc)
		}
		if u.Self || u.ID != a.ID {
			t.Fatalf("hydrated user = id %d self=%v, want id %d self=false", u.ID, u.Self, a.ID)
		}
		if u.Phone != "" {
			t.Fatalf("peer phone = %q, want empty", u.Phone)
		}
	}

	// The sender's own copy hydrates both sides: a keeps its own phone as self,
	// b is a peer there and must not disclose one.
	_, senderUsers, _, err := api.BuildUpdatesForTest(s, a.ID, 0)
	if err != nil {
		t.Fatalf("build updates sender: %v", err)
	}
	var sawSelf, sawPeer bool
	for _, uc := range senderUsers {
		u, ok := uc.(*tg.User)
		if !ok {
			t.Fatalf("sender user type = %T, want *tg.User", uc)
		}
		if u.Self {
			sawSelf = true
			if u.ID != a.ID || u.Phone != a.Phone {
				t.Fatalf("self user = id %d phone %q, want id %d phone %q", u.ID, u.Phone, a.ID, a.Phone)
			}
			continue
		}
		sawPeer = true
		if u.ID != b.ID || u.Phone != "" {
			t.Fatalf("peer user = id %d phone %q, want id %d phone \"\"", u.ID, u.Phone, b.ID)
		}
	}
	if !sawSelf || !sawPeer {
		t.Fatalf("sender side hydrated self=%v peer=%v, want both", sawSelf, sawPeer)
	}

	enc, err := api.GetStateForTest(s, b.ID)
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	st, ok := enc.(*tg.UpdatesState)
	if !ok || st.Pts != 1 {
		t.Fatalf("getState = %#v, want pts 1", enc)
	}
}
