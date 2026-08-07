package api_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
)

// TestUpdateStatusRefusesUnauthenticated proves an unauthenticated caller
// (UserID 0) is rejected with AUTH_KEY_UNREGISTERED.
func TestUpdateStatusRefusesUnauthenticated(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	_, err := api.UpdateStatusForTest(s, 0, true)
	if err == nil {
		t.Fatal("expected error for unauthenticated caller")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("error = %v, want AUTH_KEY_UNREGISTERED", err)
	}
}

// TestUpdateStatusOffline proves Offline=true calls SetUserStatus(false) and
// leaves the user offline.
func TestUpdateStatusOffline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550000301")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if err := s.SetUserStatus(ctx, user.ID, true); err != nil {
		t.Fatalf("set online: %v", err)
	}

	res, err := api.UpdateStatusForTest(s, user.ID, true)
	if err != nil {
		t.Fatalf("update status offline: %v", err)
	}
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("result = %T, want *tg.BoolTrue", res)
	}

	u, ok, err := s.UserByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("user by id: %v", err)
	}
	if !ok {
		t.Fatal("user not found")
	}
	if u.IsOnline {
		t.Fatal("user still online after Offline=true")
	}
}

// TestUpdateStatusOnline proves Offline=false calls SetUserStatus(true) and
// leaves the user online.
func TestUpdateStatusOnline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550000302")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if err := s.SetUserStatus(ctx, user.ID, false); err != nil {
		t.Fatalf("set offline: %v", err)
	}

	res, err := api.UpdateStatusForTest(s, user.ID, false)
	if err != nil {
		t.Fatalf("update status online: %v", err)
	}
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("result = %T, want *tg.BoolTrue", res)
	}

	u, ok, err := s.UserByID(ctx, user.ID)
	if err != nil {
		t.Fatalf("user by id: %v", err)
	}
	if !ok {
		t.Fatal("user not found")
	}
	if !u.IsOnline {
		t.Fatal("user not online after Offline=false")
	}
}

// TestUpdateStatusNonExistentUser proves SetUserStatus failure for a missing
// user returns errInternal, not BoolTrue.
func TestUpdateStatusNonExistentUser(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	_, err := api.UpdateStatusForTest(s, 99999, false)
	if err == nil {
		t.Fatal("expected error for non-existent user")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "INTERNAL" {
		t.Fatalf("error = %v, want INTERNAL", err)
	}
}

// --- account.updateUsername tests ---

// TestUpdateUsernameUnauthenticated proves an unauthenticated caller (UserID 0)
// is rejected with AUTH_KEY_UNREGISTERED.
func TestUpdateUsernameUnauthenticated(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	_, err := api.UpdateUsernameForTest(s, 0, "alice")
	if err == nil {
		t.Fatal("expected error for unauthenticated caller")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("error = %v, want AUTH_KEY_UNREGISTERED", err)
	}
}

// TestUpdateUsernameSuccess proves a valid, available username is claimed and
// stored on both the usernames table and users.username column.
func TestUpdateUsernameSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001001")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.UpdateUsernameForTest(s, user.ID, "alice")
	if err != nil {
		t.Fatalf("update username: %v", err)
	}
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("result = %T, want *tg.BoolTrue", res)
	}

	u, ok, err := s.UserByID(ctx, user.ID)
	if err != nil || !ok {
		t.Fatalf("user lookup: ok=%v err=%v", ok, err)
	}
	if u.Username == nil || *u.Username != "alice" {
		t.Fatalf("username = %v, want alice", u.Username)
	}
}

// TestUpdateUsernameOccupied proves a second account cannot claim an already
// taken username.
func TestUpdateUsernameOccupied(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user1, err := s.CreateUser(ctx, "+15550001002")
	if err != nil {
		t.Fatal(err)
	}
	user2, err := s.CreateUser(ctx, "+15550001003")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user1.ID, "alice")
	if err != nil {
		t.Fatalf("user1 claim: %v", err)
	}

	_, err = api.UpdateUsernameForTest(s, user2.ID, "alice")
	if err == nil {
		t.Fatal("expected error for occupied username")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_OCCUPIED" {
		t.Fatalf("error = %v, want USERNAME_OCCUPIED", err)
	}
}

// TestUpdateUsernameCaseInsensitive proves that "ALICE" is rejected when
// "alice" is already taken (case-insensitive via normalization).
func TestUpdateUsernameCaseInsensitive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user1, err := s.CreateUser(ctx, "+15550001004")
	if err != nil {
		t.Fatal(err)
	}
	user2, err := s.CreateUser(ctx, "+15550001005")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user1.ID, "alice")
	if err != nil {
		t.Fatalf("user1 claim: %v", err)
	}

	_, err = api.UpdateUsernameForTest(s, user2.ID, "ALICE")
	if err == nil {
		t.Fatal("expected error for case-insensitive occupied username")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_OCCUPIED" {
		t.Fatalf("error = %v, want USERNAME_OCCUPIED", err)
	}
}

// TestUpdateUsernameClear proves calling with an empty string clears the
// username and releases the handle.
func TestUpdateUsernameClear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001006")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user.ID, "alice")
	if err != nil {
		t.Fatalf("set username: %v", err)
	}

	res, err := api.UpdateUsernameForTest(s, user.ID, "")
	if err != nil {
		t.Fatalf("clear username: %v", err)
	}
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("result = %T, want *tg.BoolTrue", res)
	}

	u, ok, err := s.UserByID(ctx, user.ID)
	if err != nil || !ok {
		t.Fatalf("user lookup: ok=%v err=%v", ok, err)
	}
	if u.Username != nil {
		t.Fatalf("username = %v, want nil", u.Username)
	}

	// Handle should be available for another user.
	user2, err := s.CreateUser(ctx, "+15550001007")
	if err != nil {
		t.Fatal(err)
	}
	_, err = api.UpdateUsernameForTest(s, user2.ID, "alice")
	if err != nil {
		t.Fatalf("reclaim username: %v", err)
	}
}

// TestUpdateUsernameClearIdempotent proves clearing a username when none is set
// returns True (idempotent).
func TestUpdateUsernameClearIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001008")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.UpdateUsernameForTest(s, user.ID, "")
	if err != nil {
		t.Fatalf("clear empty username: %v", err)
	}
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("result = %T, want *tg.BoolTrue", res)
	}
}

// TestUpdateUsernameTooShort proves a username shorter than 5 characters is
// rejected with USERNAME_INVALID.
func TestUpdateUsernameTooShort(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001009")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user.ID, "ab")
	if err == nil {
		t.Fatal("expected error for too-short username")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_INVALID" {
		t.Fatalf("error = %v, want USERNAME_INVALID", err)
	}
}

// TestUpdateUsernameDigitLeading proves a username starting with a digit is
// rejected with USERNAME_INVALID.
func TestUpdateUsernameDigitLeading(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001010")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user.ID, "1alice")
	if err == nil {
		t.Fatal("expected error for digit-leading username")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_INVALID" {
		t.Fatalf("error = %v, want USERNAME_INVALID", err)
	}
}

// TestUpdateUsernameUnderscoreLeading proves a username starting with an
// underscore is rejected with USERNAME_INVALID.
func TestUpdateUsernameUnderscoreLeading(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001011")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user.ID, "_alice")
	if err == nil {
		t.Fatal("expected error for underscore-leading username")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_INVALID" {
		t.Fatalf("error = %v, want USERNAME_INVALID", err)
	}
}

// TestUpdateUsernameInvalidChar proves a username with invalid characters is
// rejected with USERNAME_INVALID.
func TestUpdateUsernameInvalidChar(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001012")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user.ID, "alice!")
	if err == nil {
		t.Fatal("expected error for invalid character username")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_INVALID" {
		t.Fatalf("error = %v, want USERNAME_INVALID", err)
	}
}

// TestUpdateUsernameReserved proves a reserved handle like "admin" is rejected
// with USERNAME_INVALID.
func TestUpdateUsernameReserved(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001013")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user.ID, "admin")
	if err == nil {
		t.Fatal("expected error for reserved username")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_INVALID" {
		t.Fatalf("error = %v, want USERNAME_INVALID", err)
	}
}

// TestUpdateUsernameReservedCaseInsensitive proves reserved handles are checked
// case-insensitively.
func TestUpdateUsernameReservedCaseInsensitive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001014")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user.ID, "ADMIN")
	if err == nil {
		t.Fatal("expected error for reserved username (uppercase)")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_INVALID" {
		t.Fatalf("error = %v, want USERNAME_INVALID", err)
	}
}

// TestUpdateUsernameFloodWait proves a third change within 24 hours returns
// FLOOD_WAIT (limit is 2).
func TestUpdateUsernameFloodWait(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001015")
	if err != nil {
		t.Fatal(err)
	}

	// First change: set username.
	_, err = api.UpdateUsernameForTest(s, user.ID, "alice1")
	if err != nil {
		t.Fatalf("first change: %v", err)
	}
	// Second change: clear username.
	_, err = api.UpdateUsernameForTest(s, user.ID, "")
	if err != nil {
		t.Fatalf("second change: %v", err)
	}
	// Third change: should hit flood wait (limit is 2).
	_, err = api.UpdateUsernameForTest(s, user.ID, "alice2")
	if err == nil {
		t.Fatal("expected error for flood wait")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "FLOOD_WAIT" {
		t.Fatalf("error = %v, want FLOOD_WAIT", err)
	}
}

// TestUpdateUsernameConcurrentBoundary proves that two concurrent requests for
// the same username from different accounts result in exactly one success and
// one USERNAME_OCCUPIED, enforced by the usernames PRIMARY KEY.
func TestUpdateUsernameConcurrentBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user1, err := s.CreateUser(ctx, "+15550001016")
	if err != nil {
		t.Fatal(err)
	}
	user2, err := s.CreateUser(ctx, "+15550001017")
	if err != nil {
		t.Fatal(err)
	}

	type result struct{ err error }
	results := make([]result, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	ready := make(chan struct{})

	go func() {
		defer wg.Done()
		<-ready
		_, results[0].err = api.UpdateUsernameForTest(s, user1.ID, "racing")
	}()
	go func() {
		defer wg.Done()
		<-ready
		_, results[1].err = api.UpdateUsernameForTest(s, user2.ID, "racing")
	}()

	close(ready)
	wg.Wait()

	var success, occupied int
	for _, r := range results {
		switch {
		case r.err == nil:
			success++
		case tgerr.Is(r.err, "USERNAME_OCCUPIED"):
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

// TestUpdateUsernameClearCountsAsChange proves that clearing a username uses
// one change token, contributing to the rate limit.
func TestUpdateUsernameClearCountsAsChange(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001018")
	if err != nil {
		t.Fatal(err)
	}

	// Change 1: set username.
	_, err = api.UpdateUsernameForTest(s, user.ID, "alice")
	if err != nil {
		t.Fatalf("set: %v", err)
	}
	// Change 2: clear username (uses one token).
	_, err = api.UpdateUsernameForTest(s, user.ID, "")
	if err != nil {
		t.Fatalf("clear: %v", err)
	}
	// Third change should hit flood wait (limit is 2).
	_, err = api.UpdateUsernameForTest(s, user.ID, "bob1234")
	if err == nil {
		t.Fatal("expected flood wait")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "FLOOD_WAIT" {
		t.Fatalf("error = %v, want FLOOD_WAIT", err)
	}
}

// TestUpdateUsernameConcreteExample exercises the concrete example from the
// ticket: Account 1 sets "myname", Account 2 tries "Myname" (occupied),
// Account 1 clears, Account 2 now claims "myname".
func TestUpdateUsernameConcreteExample(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	acc1, err := s.CreateUser(ctx, "+15550001019")
	if err != nil {
		t.Fatal(err)
	}
	acc2, err := s.CreateUser(ctx, "+15550001020")
	if err != nil {
		t.Fatal(err)
	}

	// Account 1 calls account.updateUsername("myname") → success.
	_, err = api.UpdateUsernameForTest(s, acc1.ID, "myname")
	if err != nil {
		t.Fatalf("acc1 set myname: %v", err)
	}

	// Account 2 calls account.updateUsername("Myname") → USERNAME_OCCUPIED.
	_, err = api.UpdateUsernameForTest(s, acc2.ID, "Myname")
	if err == nil {
		t.Fatal("expected USERNAME_OCCUPIED")
	}
	if !tgerr.Is(err, "USERNAME_OCCUPIED") {
		t.Fatalf("error = %v, want USERNAME_OCCUPIED", err)
	}

	// Account 1 calls account.updateUsername("") → success (uses one change token).
	_, err = api.UpdateUsernameForTest(s, acc1.ID, "")
	if err != nil {
		t.Fatalf("acc1 clear: %v", err)
	}

	// Account 2 now calls account.updateUsername("myname") → success.
	_, err = api.UpdateUsernameForTest(s, acc2.ID, "myname")
	if err != nil {
		t.Fatalf("acc2 claim myname: %v", err)
	}
}

// TestUpdateUsernameTooLong proves a username over 32 characters is rejected.
func TestUpdateUsernameTooLong(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001021")
	if err != nil {
		t.Fatal(err)
	}

	_, err = api.UpdateUsernameForTest(s, user.ID, "abcdefghijklmnopqrstuvwxyz1234567")
	if err == nil {
		t.Fatal("expected error for too-long username")
	}
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "USERNAME_INVALID" {
		t.Fatalf("error = %v, want USERNAME_INVALID", err)
	}
}

// TestUpdateUsernameExactLength proves boundary lengths (5 and 32) are accepted.
func TestUpdateUsernameExactLength(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user1, err := s.CreateUser(ctx, "+15550001022")
	if err != nil {
		t.Fatal(err)
	}
	user2, err := s.CreateUser(ctx, "+15550001023")
	if err != nil {
		t.Fatal(err)
	}

	// 5 chars (minimum)
	_, err = api.UpdateUsernameForTest(s, user1.ID, "abcde")
	if err != nil {
		t.Fatalf("5-char username: %v", err)
	}

	// 32 chars (maximum) — use a second user to avoid rate limit
	_, err = api.UpdateUsernameForTest(s, user2.ID, "abcdefghijklmnopqrstuvwxyz123456")
	if err != nil {
		t.Fatalf("32-char username: %v", err)
	}
}

// TestUpdateUsernameReservedAll proves all reserved handles are rejected.
func TestUpdateUsernameReservedAll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	user, err := s.CreateUser(ctx, "+15550001023")
	if err != nil {
		t.Fatal(err)
	}

	reserved := []string{
		"admin", "support", "help", "me", "settings",
		"telegram", "channel", "channels", "bot", "bots",
		"login", "signup",
	}
	for _, r := range reserved {
		_, err := api.UpdateUsernameForTest(s, user.ID, r)
		if err == nil {
			t.Errorf("reserved handle %q was accepted", r)
		} else if !tgerr.Is(err, "USERNAME_INVALID") {
			t.Errorf("reserved handle %q: got %v, want USERNAME_INVALID", r, err)
		}
	}
}
