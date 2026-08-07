package store_test

import (
	"context"
	"errors"
	"sync"
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

// --- UpdateUsername store tests ---

func TestUpdateUsernameClaimAndClear(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551260081")

	// Claim a username.
	if err := s.UpdateUsername(ctx, u.ID, "alice123"); err != nil {
		t.Fatalf("claim: %v", err)
	}

	usr, ok, err := s.UserByID(ctx, u.ID)
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if usr.Username == nil || *usr.Username != "alice123" {
		t.Fatalf("username = %v, want alice123", usr.Username)
	}

	// Clear the username.
	if err := s.UpdateUsername(ctx, u.ID, ""); err != nil {
		t.Fatalf("clear: %v", err)
	}

	usr, ok, err = s.UserByID(ctx, u.ID)
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if usr.Username != nil {
		t.Fatalf("username = %v, want nil", usr.Username)
	}
}

func TestUpdateUsernameOccupied(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u1 := mustUser(t, s, "+15551260091")
	u2 := mustUser(t, s, "+15551260092")

	if err := s.UpdateUsername(ctx, u1.ID, "bob1234"); err != nil {
		t.Fatalf("u1 claim: %v", err)
	}

	err := s.UpdateUsername(ctx, u2.ID, "bob1234")
	if !errors.Is(err, store.ErrUsernameOccupied) {
		t.Fatalf("expected ErrUsernameOccupied, got %v", err)
	}
}

func TestUpdateUsernameCaseInsensitive(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u1 := mustUser(t, s, "+15551260101")
	u2 := mustUser(t, s, "+15551260102")

	if err := s.UpdateUsername(ctx, u1.ID, "alice123"); err != nil {
		t.Fatalf("u1 claim: %v", err)
	}

	err := s.UpdateUsername(ctx, u2.ID, "ALICE123")
	if !errors.Is(err, store.ErrUsernameOccupied) {
		t.Fatalf("expected ErrUsernameOccupied for case-insensitive clash, got %v", err)
	}
}

func TestUpdateUsernameFloodWait(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551260111")

	if err := s.UpdateUsername(ctx, u.ID, "test1234"); err != nil {
		t.Fatalf("change 1: %v", err)
	}
	if err := s.UpdateUsername(ctx, u.ID, ""); err != nil {
		t.Fatalf("change 2: %v", err)
	}
	if err := s.UpdateUsername(ctx, u.ID, "test5678"); err != nil {
		t.Fatalf("change 3: %v", err)
	}

	err := s.UpdateUsername(ctx, u.ID, "test9012")
	if !errors.Is(err, store.ErrUsernameFloodWait) {
		t.Fatalf("expected ErrUsernameFloodWait, got %v", err)
	}
}

func TestUpdateUsernameConcurrent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u1 := mustUser(t, s, "+15551260121")
	u2 := mustUser(t, s, "+15551260122")

	type result struct{ err error }
	results := make([]result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	ready := make(chan struct{})

	go func() {
		defer wg.Done()
		<-ready
		results[0].err = s.UpdateUsername(ctx, u1.ID, "race123")
	}()
	go func() {
		defer wg.Done()
		<-ready
		results[1].err = s.UpdateUsername(ctx, u2.ID, "race123")
	}()

	close(ready)
	wg.Wait()

	var success, occupied int
	for _, r := range results {
		switch {
		case r.err == nil:
			success++
		case errors.Is(r.err, store.ErrUsernameOccupied):
			occupied++
		default:
			t.Errorf("unexpected error: %v", r.err)
		}
	}

	if success != 1 {
		t.Errorf("successes = %d, want 1", success)
	}
	if occupied != 1 {
		t.Errorf("occupied = %d, want 1", occupied)
	}
}
