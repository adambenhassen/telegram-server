package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// mustUser creates a user on s, failing the test on error.
func mustUser(t *testing.T, s *store.Store, phone string) store.User {
	t.Helper()
	u, err := s.CreateUser(context.Background(), phone)
	if err != nil {
		t.Fatalf("create user %s: %v", phone, err)
	}
	return u
}

// loadUsersWire finds the wire object loadUsers emitted for id.
func loadUsersWire(t *testing.T, users []tg.UserClass, id int64) tg.UserClass {
	t.Helper()
	for _, uc := range users {
		if u, ok := uc.(*tg.User); ok && u.ID == id {
			return u
		}
		if e, ok := uc.(*tg.UserEmpty); ok && e.ID == id {
			return e
		}
	}
	t.Fatalf("no wire user for id %d in %d entries", id, len(users))
	return nil
}

// TestLoadUsersGatesEntitlement proves the loadUsers gate end to end: an id the
// viewer shares no live edge with degrades to userEmpty, while every live edge
// — self, a 1:1 dialog, a shared chat, a shared channel — keeps its full
// profile, and a banned channel member is not current.
func TestLoadUsersGatesEntitlement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	viewer := mustUser(t, s, "+15551960001")
	partner := mustUser(t, s, "+15551960002")  // 1:1 dialog
	chatmate := mustUser(t, s, "+15551960003") // shared chat
	chmate := mustUser(t, s, "+15551960004")   // shared channel
	stranger := mustUser(t, s, "+15551960005") // no live edge
	banned := mustUser(t, s, "+15551960006")   // banned from the shared channel

	// A 1:1 dialog with partner.
	if _, _, _, _, err := s.SendMessage(ctx, viewer.ID, partner.ID, "hi", 1, 0, 0); err != nil {
		t.Fatalf("send: %v", err)
	}
	// A chat the viewer and chatmate are both in.
	if _, err := s.CreateChat(ctx, viewer.ID, "Crew", []int64{chatmate.ID}); err != nil {
		t.Fatalf("create chat: %v", err)
	}
	// A channel the viewer, chmate and banned are in; banned is then banned.
	ch, err := s.CreateChannel(ctx, viewer.ID, "News", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err := s.CreateChannelInvite(ctx, ch.ID, ch.CreatorID); err != nil {
		t.Fatalf("invite: %v", err)
	}
	for _, id := range []int64{chmate.ID, banned.ID} {
		hash, err := s.CreateChannelInvite(ctx, ch.ID, ch.CreatorID)
		if err != nil {
			t.Fatalf("invite %d: %v", id, err)
		}
		if _, _, err := s.JoinChannelByInvite(ctx, hash, id); err != nil {
			t.Fatalf("join %d: %v", id, err)
		}
	}
	if err := s.SetChannelBan(ctx, ch.ID, viewer.ID, banned.ID, nil, true); err != nil {
		t.Fatalf("ban: %v", err)
	}

	users, err := api.LoadUsersForTest(s, []int64{
		viewer.ID, partner.ID, chatmate.ID, chmate.ID, stranger.ID, banned.ID,
	}, viewer.ID)
	if err != nil {
		t.Fatalf("load users: %v", err)
	}
	if len(users) != 6 {
		t.Fatalf("users = %d, want 6", len(users))
	}

	// The live edges keep their full profiles.
	for _, id := range []int64{viewer.ID, partner.ID, chatmate.ID, chmate.ID} {
		u, ok := loadUsersWire(t, users, id).(*tg.User)
		if !ok {
			t.Fatalf("id %d is not a full user", id)
		}
		if u.AccessHash == 0 {
			t.Errorf("id %d carries a zero access_hash", id)
		}
	}
	// The viewer is self.
	if self, ok := loadUsersWire(t, users, viewer.ID).(*tg.User); !ok || !self.Self {
		t.Fatalf("viewer is not self: %v", loadUsersWire(t, users, viewer.ID))
	}
	// No live edge: userEmpty, and nothing else.
	if e, ok := loadUsersWire(t, users, stranger.ID).(*tg.UserEmpty); !ok || e.ID != stranger.ID {
		t.Fatalf("stranger = %v, want userEmpty %d", loadUsersWire(t, users, stranger.ID), stranger.ID)
	}
	// Banned from the only shared channel: not current, userEmpty.
	if e, ok := loadUsersWire(t, users, banned.ID).(*tg.UserEmpty); !ok || e.ID != banned.ID {
		t.Fatalf("banned = %v, want userEmpty %d", loadUsersWire(t, users, banned.ID), banned.ID)
	}
}

// TestLoadUsersGatesRemovedMember proves the removed-member case the ticket
// names: after B removes C from the chat they share, C is entitled to B's
// profile only if a live edge remains. Here C keeps a 1:1 dialog with B, so B
// stays live for C; a third party D, who shares nothing with C, degrades.
func TestLoadUsersGatesRemovedMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	b := mustUser(t, s, "+15551960101")
	c := mustUser(t, s, "+15551960102")
	d := mustUser(t, s, "+15551960103")

	// B and C share a 1:1 dialog and a chat.
	if _, _, _, _, err := s.SendMessage(ctx, b.ID, c.ID, "hello", 2, 0, 0); err != nil {
		t.Fatalf("send: %v", err)
	}
	chat, err := s.CreateChat(ctx, b.ID, "Two", []int64{c.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	// Before removal: C sees B live.
	users, err := api.LoadUsersForTest(s, []int64{b.ID, c.ID, d.ID}, c.ID)
	if err != nil {
		t.Fatalf("load users: %v", err)
	}
	if _, ok := loadUsersWire(t, users, b.ID).(*tg.User); !ok {
		t.Fatalf("before removal, C sees B as %v, want full user", loadUsersWire(t, users, b.ID))
	}
	if _, ok := loadUsersWire(t, users, d.ID).(*tg.UserEmpty); !ok {
		t.Fatalf("before removal, C sees D as %v, want userEmpty", loadUsersWire(t, users, d.ID))
	}

	// B removes C from the chat. The 1:1 dialog remains, so B stays live for C.
	if _, _, _, err := s.RemoveChatUser(ctx, chat.ID, c.ID, b.ID); err != nil {
		t.Fatalf("remove: %v", err)
	}
	users, err = api.LoadUsersForTest(s, []int64{b.ID, c.ID, d.ID}, c.ID)
	if err != nil {
		t.Fatalf("load users: %v", err)
	}
	if _, ok := loadUsersWire(t, users, b.ID).(*tg.User); !ok {
		t.Fatalf("after removal with a live 1:1, C sees B as %v, want full user", loadUsersWire(t, users, b.ID))
	}

	// A viewer with no live edge at all degrades everyone else to userEmpty.
	users, err = api.LoadUsersForTest(s, []int64{b.ID, c.ID, d.ID}, d.ID)
	if err != nil {
		t.Fatalf("load users: %v", err)
	}
	if e, ok := loadUsersWire(t, users, b.ID).(*tg.UserEmpty); !ok || e.ID != b.ID {
		t.Fatalf("D sees B as %v, want userEmpty %d", loadUsersWire(t, users, b.ID), b.ID)
	}
	if e, ok := loadUsersWire(t, users, c.ID).(*tg.UserEmpty); !ok || e.ID != c.ID {
		t.Fatalf("D sees C as %v, want userEmpty %d", loadUsersWire(t, users, c.ID), c.ID)
	}
}

// TestLoadUsersEmptySet returns an empty slice for an empty id set without
// touching the store.
func TestLoadUsersEmptySet(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	users, err := api.LoadUsersForTest(s, nil, 0)
	if err != nil {
		t.Fatalf("load users: %v", err)
	}
	if len(users) != 0 {
		t.Fatalf("users = %d, want 0", len(users))
	}
}

// TestLoadUsersDegradedForNoSharedDialog reproduces the removed-member case the
// ticket names at the loadUsers level: after B removes C from their chat and C
// shares no 1:1 dialog with B, C's view of B degrades to userEmpty, and B's
// changed username is not served.
func TestLoadUsersDegradedForNoSharedDialog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	b := mustUser(t, s, "+15551960201")
	c := mustUser(t, s, "+15551960202")

	// B and C share only a chat.
	chat, err := s.CreateChat(ctx, b.ID, "Two", []int64{c.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	if _, err := s.CreateUsernameUser(ctx, "b_handle", "B", ""); err != nil {
		t.Fatalf("username user: %v", err)
	}
	// Claim a handle for B so a degraded view could leak it.
	if err := s.ClaimUsername(ctx, b.ID, "b_handle"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	// Before removal: C sees B live, with the handle.
	users, err := api.LoadUsersForTest(s, []int64{b.ID, c.ID}, c.ID)
	if err != nil {
		t.Fatalf("load users: %v", err)
	}
	if u, ok := loadUsersWire(t, users, b.ID).(*tg.User); !ok || u.Username != "b_handle" {
		t.Fatalf("before removal, C sees B as %v, want full user with b_handle", loadUsersWire(t, users, b.ID))
	}

	// B removes C from the chat. No 1:1 dialog, no channel: B degrades for C.
	if _, _, _, err := s.RemoveChatUser(ctx, chat.ID, c.ID, b.ID); err != nil {
		t.Fatalf("remove: %v", err)
	}
	users, err = api.LoadUsersForTest(s, []int64{b.ID, c.ID}, c.ID)
	if err != nil {
		t.Fatalf("load users: %v", err)
	}
	if e, ok := loadUsersWire(t, users, b.ID).(*tg.UserEmpty); !ok || e.ID != b.ID {
		t.Fatalf("after removal, C sees B as %v, want userEmpty %d", loadUsersWire(t, users, b.ID), b.ID)
	}
}
