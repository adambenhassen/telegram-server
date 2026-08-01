package api_test

import (
	"context"
	"errors"
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
