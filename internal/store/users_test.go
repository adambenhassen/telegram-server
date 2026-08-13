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

	err := s.UpdateUsername(ctx, u.ID, "test5678")
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

func TestUpdateUsernameRename(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u1 := mustUser(t, s, "+15551260131")
	u2 := mustUser(t, s, "+15551260132")

	// u1 claims "alice123".
	if err := s.UpdateUsername(ctx, u1.ID, "alice123"); err != nil {
		t.Fatalf("u1 claim alice123: %v", err)
	}

	// u1 renames to "bob12345" — old handle "alice123" should be released.
	if err := s.UpdateUsername(ctx, u1.ID, "bob12345"); err != nil {
		t.Fatalf("u1 rename: %v", err)
	}

	// u2 should now be able to claim "alice123" (old slot freed by rename).
	err := s.UpdateUsername(ctx, u2.ID, "alice123")
	if err != nil {
		t.Fatalf("u2 claim alice123 after rename: %v", err)
	}
}

func TestUpdateUsernameRollbackOnFloodWait(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u1 := mustUser(t, s, "+15551260141")
	u2 := mustUser(t, s, "+15551260142")

	// Change 1: set "alice123".
	if err := s.UpdateUsername(ctx, u1.ID, "alice123"); err != nil {
		t.Fatalf("set username: %v", err)
	}
	// Change 2: rename to "bob12345" (also verifies rename releases "alice123").
	if err := s.UpdateUsername(ctx, u1.ID, "bob12345"); err != nil {
		t.Fatalf("rename: %v", err)
	}
	// Change 3: attempt clear — should hit flood wait and not half-commit.
	err := s.UpdateUsername(ctx, u1.ID, "")
	if !errors.Is(err, store.ErrUsernameFloodWait) {
		t.Fatalf("expected ErrUsernameFloodWait, got %v", err)
	}

	// users.username should still be "bob12345" (clear did not commit).
	usr, ok, err := s.UserByID(ctx, u1.ID)
	if err != nil || !ok {
		t.Fatalf("user lookup: ok=%v err=%v", ok, err)
	}
	if usr.Username == nil || *usr.Username != "bob12345" {
		t.Fatalf("username = %v, want bob12345 (flood-wait clear should not have committed)", usr.Username)
	}

	// The usernames row for "bob12345" should still exist — u2 cannot claim it.
	err = s.UpdateUsername(ctx, u2.ID, "bob12345")
	if !errors.Is(err, store.ErrUsernameOccupied) {
		t.Fatalf("expected ErrUsernameOccupied, got %v (row was deleted despite flood-wait)", err)
	}
}

func TestUpdateUsernameRejectsLoginCredential(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUsernameUser(ctx, "creduser1", "Cred", "User")
	if err != nil {
		t.Fatalf("create username user: %v", err)
	}

	// Seed the initial username directly (bypassing the guard so the test can
	// start with a valid handle already claimed).
	if _, err := store.StorePool(s).Exec(ctx,
		"INSERT INTO usernames (handle, owner_type, owner_id) VALUES ($1, $2, $3)",
		"creduser1", "user", u.ID); err != nil {
		t.Fatalf("seed username: %v", err)
	}

	// Attempt to change the username — should be rejected.
	err = s.UpdateUsername(ctx, u.ID, "newhandle")
	if !errors.Is(err, store.ErrUsernameIsLoginCredential) {
		t.Fatalf("expected ErrUsernameIsLoginCredential, got %v", err)
	}

	// Attempt to clear the username — also rejected.
	err = s.UpdateUsername(ctx, u.ID, "")
	if !errors.Is(err, store.ErrUsernameIsLoginCredential) {
		t.Fatalf("expected ErrUsernameIsLoginCredential on clear, got %v", err)
	}

	// Username should still be "creduser1".
	usr, ok, err := s.UserByID(ctx, u.ID)
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if usr.Username == nil || *usr.Username != "creduser1" {
		t.Fatalf("username = %v, want creduser1", usr.Username)
	}
}

func TestUpdateUsernameAllowsPhoneMode(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u1 := mustUser(t, s, "+15551260161")
	u2 := mustUser(t, s, "+15551260162")

	// Phone-mode user can change username freely.
	if err := s.UpdateUsername(ctx, u1.ID, "phoneuser1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Use a second user to avoid rate limit.
	if err := s.UpdateUsername(ctx, u2.ID, "phoneuser2"); err != nil {
		t.Fatalf("rename: %v", err)
	}
}

// --- CreateUsernameUser tests ---

func TestCreateUsernameUserReturnsUsernameMode(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUsernameUser(ctx, "alice1", "Alice", "Smith")
	if err != nil {
		t.Fatalf("create username user: %v", err)
	}
	if u.ID == 0 {
		t.Fatal("zero id")
	}
	if u.Phone != "" {
		t.Errorf("phone = %q, want empty", u.Phone)
	}
	if u.FirstName != "Alice" {
		t.Errorf("first_name = %q, want Alice", u.FirstName)
	}
	if u.LastName != "Smith" {
		t.Errorf("last_name = %q, want Smith", u.LastName)
	}
	// Verify the row in DB has login_mode = 'username' via a direct read.
	var loginMode string
	err = store.StorePool(s).QueryRow(ctx,
		"SELECT login_mode FROM users WHERE id = $1", u.ID).Scan(&loginMode)
	if err != nil {
		t.Fatalf("read login_mode: %v", err)
	}
	if loginMode != "username" {
		t.Errorf("login_mode = %q, want username", loginMode)
	}
}

func TestCreateUsernameUserNoPhoneConflict(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	// Create two username-mode users — neither has a phone, so no conflict.
	u1, err := s.CreateUsernameUser(ctx, "alice2", "Alice", "Smith")
	if err != nil {
		t.Fatalf("create first: %v", err)
	}
	u2, err := s.CreateUsernameUser(ctx, "bob2", "Bob", "Jones")
	if err != nil {
		t.Fatalf("create second: %v", err)
	}
	if u1.ID == u2.ID {
		t.Error("two username users got same id")
	}
}

func TestCreateUsernameUserProvisionsUpdateState(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUsernameUser(ctx, "carol2", "Carol", "White")
	if err != nil {
		t.Fatalf("create username user: %v", err)
	}
	// Verify update_state row exists.
	var hasState bool
	err = store.StorePool(s).QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM update_state WHERE user_id = $1)", u.ID).Scan(&hasState)
	if err != nil {
		t.Fatalf("check update_state: %v", err)
	}
	if !hasState {
		t.Error("update_state row not provisioned for username user")
	}
}

func TestCreateUserPhoneModeUnchanged(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15551260151")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if u.Phone != "15551260151" {
		t.Errorf("phone = %q, want 15551260151", u.Phone)
	}
	// Verify the row in DB has login_mode = 'phone' (default).
	var loginMode string
	err = store.StorePool(s).QueryRow(ctx,
		"SELECT login_mode FROM users WHERE id = $1", u.ID).Scan(&loginMode)
	if err != nil {
		t.Fatalf("read login_mode: %v", err)
	}
	if loginMode != "phone" {
		t.Errorf("login_mode = %q, want phone", loginMode)
	}
}
