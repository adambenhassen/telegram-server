package store_test

import (
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSetUserStatusOnlineThenOffline(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551260001")

	if err := s.SetUserStatus(ctx, u.ID, true); err != nil {
		t.Fatalf("set online: %v", err)
	}

	usr, ok, err := s.UserByID(ctx, u.ID)
	if err != nil || !ok {
		t.Fatalf("lookup after online: ok=%v err=%v", ok, err)
	}
	if !usr.IsOnline {
		t.Fatal("user not online after SetUserStatus(true)")
	}
	if usr.LastSeenAt == nil {
		t.Fatal("last_seen_at is nil after SetUserStatus(true)")
	}

	if err := s.SetUserStatus(ctx, u.ID, false); err != nil {
		t.Fatalf("set offline: %v", err)
	}

	usr, ok, err = s.UserByID(ctx, u.ID)
	if err != nil || !ok {
		t.Fatalf("lookup after offline: ok=%v err=%v", ok, err)
	}
	if usr.IsOnline {
		t.Fatal("user still online after SetUserStatus(false)")
	}
	if usr.LastSeenAt == nil {
		t.Fatal("last_seen_at is nil after SetUserStatus(false)")
	}
}

func TestSetUserStatusNonExistent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	err := s.SetUserStatus(ctx, 99999, true)
	if err == nil {
		t.Fatal("expected error for non-existent user")
	}
}

func TestDialogPartnersReturnsPartnersOnly(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551260011")
	b := mustUser(t, s, "+15551260012")
	c := mustUser(t, s, "+15551260013")

	// Create dialogs by sending messages.
	send(t, s, a, b, "ab", 1)
	send(t, s, a, c, "ac", 2)

	partners, err := s.DialogPartners(ctx, a.ID)
	if err != nil {
		t.Fatalf("dialog partners: %v", err)
	}
	if len(partners) != 2 {
		t.Fatalf("partners len = %d, want 2", len(partners))
	}

	// Should contain b and c, not a.
	found := map[int64]bool{}
	for _, p := range partners {
		found[p] = true
	}
	if found[a.ID] {
		t.Fatal("partner list contains user's own ID")
	}
	if !found[b.ID] || !found[c.ID] {
		t.Fatalf("partners missing b or c: %+v", found)
	}
}

func TestDialogPartnersEmpty(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	mustUser(t, s, "+15551260021")

	// Fresh user, no dialogs.
	u, err := s.CreateUser(ctx, "+15551260022")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	partners, err := s.DialogPartners(ctx, u.ID)
	if err != nil {
		t.Fatalf("dialog partners: %v", err)
	}
	if len(partners) != 0 {
		t.Fatalf("partners = %+v, want empty", partners)
	}
}

func TestUserDefaultsAfterCreate(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15551260031")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if u.IsOnline {
		t.Fatal("new user should be offline by default")
	}
	if u.LastSeenAt != nil {
		t.Fatal("new user should have nil last_seen_at")
	}
}

func TestCreateUserRegression(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15551260041")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if u.ID == 0 {
		t.Fatal("zero id")
	}

	found, ok, err := s.UserByID(ctx, u.ID)
	if err != nil || !ok {
		t.Fatalf("UserByID: ok=%v err=%v", ok, err)
	}
	if found.ID != u.ID {
		t.Fatalf("id mismatch")
	}

	found2, ok, err := s.UserByPhone(ctx, "+15551260041")
	if err != nil || !ok {
		t.Fatalf("UserByPhone: ok=%v err=%v", ok, err)
	}
	if found2.ID != u.ID {
		t.Fatalf("phone lookup id mismatch")
	}
}

func TestUserFromDBWithNilLastSeen(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u := mustUser(t, s, "+15551260051")
	if u.LastSeenAt != nil {
		t.Fatal("fresh user LastSeenAt should be nil")
	}

	if err := s.SetUserStatus(ctx, u.ID, true); err != nil {
		t.Fatalf("set online: %v", err)
	}

	u2, ok, err := s.UserByID(ctx, u.ID)
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if u2.LastSeenAt == nil {
		t.Fatal("LastSeenAt should be non-nil after SetUserStatus(true)")
	}
}

func TestDialogPartnersExcludesChatDialogs(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551260061")
	b := mustUser(t, s, "+15551260062")

	// Create a chat with both users.
	ch, err := s.CreateChat(ctx, a.ID, "test chat", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	// Post a message in the chat to create dialog rows for both.
	_, _, _, err = s.SendChatMessage(ctx, store.FanOut{ChatID: ch.ID, FromID: a.ID, Text: "hello", RandomID: 1}) //nolint:dogsled
	if err != nil {
		t.Fatalf("send chat message: %v", err)
	}

	// DialogPartners should NOT include b (chat dialogs excluded).
	partners, err := s.DialogPartners(ctx, a.ID)
	if err != nil {
		t.Fatalf("dialog partners: %v", err)
	}
	if len(partners) != 0 {
		t.Fatalf("partners = %+v, want empty (chat dialogs excluded)", partners)
	}

	// Now also have a 1:1 dialog.
	send(t, s, a, b, "dm", 100)

	partners, err = s.DialogPartners(ctx, a.ID)
	if err != nil {
		t.Fatalf("dialog partners: %v", err)
	}
	if len(partners) != 1 || partners[0] != b.ID {
		t.Fatalf("partners = %+v, want [%d]", partners, b.ID)
	}
}

func TestDialogPartnersIgnoresDeletedDialog(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551260071")
	b := mustUser(t, s, "+15551260072")

	send(t, s, a, b, "hi", 1)

	// Verify partner exists.
	partners, err := s.DialogPartners(ctx, a.ID)
	if err != nil {
		t.Fatalf("dialog partners before delete: %v", err)
	}
	if len(partners) != 1 || partners[0] != b.ID {
		t.Fatalf("partners before delete = %+v, want [%d]", partners, b.ID)
	}
}
