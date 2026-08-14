package api_test

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestCheckPasswordRateLimitPerIP proves that N+1 failed checkPassword attempts
// from the same IP are denied with FLOOD_WAIT.
func TestCheckPasswordRateLimitPerIP(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296001")
	if err != nil {
		t.Fatal(err)
	}

	// Set a password for alice so the pending state is valid.
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   alice.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: make([]byte, 256),
	}); err != nil {
		t.Fatal(err)
	}

	// Register auth key and stage alice as pending.
	var authKeyID [8]byte
	authKeyID[7] = 1
	keyID := mtproto.AuthKeyIDInt64(authKeyID)
	if err := s.SaveAuthKey(ctx, keyID, []byte("key")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPendingUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("192.0.2.1")
	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}

	// 3 failed attempts should be allowed (they fail with SRP_ID_INVALID, not FLOOD_WAIT).
	for range 3 {
		_, err := api.CheckPasswordForTestWithLimits(s, authKeyID, addr, cfg, cfg, &tg.AuthCheckPasswordRequest{
			Password: &tg.InputCheckPasswordSRP{SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256)},
		})
		// Each attempt should fail with SRP_ID_INVALID (invalid SRP proof), not FLOOD_WAIT.
		if err == nil {
			t.Fatal("expected error from failed SRP proof")
		}
		if isFloodWait(err) {
			t.Fatal("got FLOOD_WAIT too early")
		}
	}

	// 4th attempt should be denied with FLOOD_WAIT.
	_, err = api.CheckPasswordForTestWithLimits(s, authKeyID, addr, cfg, cfg, &tg.AuthCheckPasswordRequest{
		Password: &tg.InputCheckPasswordSRP{SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256)},
	})
	if !isFloodWait(err) {
		t.Fatalf("4th attempt: expected FLOOD_WAIT, got %v", err)
	}
}

// TestGetPasswordIPRateLimit proves that N+1 unauthenticated getPassword calls
// from the same IP are denied with FLOOD_WAIT.
func TestGetPasswordIPRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296101")
	if err != nil {
		t.Fatal(err)
	}

	// Set a password for alice.
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   alice.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: make([]byte, 256),
	}); err != nil {
		t.Fatal(err)
	}

	// Register auth key and stage alice as pending.
	var authKeyID [8]byte
	authKeyID[7] = 2
	keyID := mtproto.AuthKeyIDInt64(authKeyID)
	if err := s.SaveAuthKey(ctx, keyID, []byte("key")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPendingUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("192.0.2.2")
	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}

	// 3 calls should pass.
	for range 3 {
		_, err := api.GetPasswordIPForTestWithLimits(s, authKeyID, addr, cfg, &tg.AccountGetPasswordRequest{})
		if err != nil {
			t.Fatalf("call: %v", err)
		}
	}

	// 4th call should be denied with FLOOD_WAIT.
	_, err = api.GetPasswordIPForTestWithLimits(s, authKeyID, addr, cfg, &tg.AccountGetPasswordRequest{})
	if !isFloodWait(err) {
		t.Fatalf("4th call: expected FLOOD_WAIT, got %v", err)
	}
}

// TestGetPasswordAuthenticatedExempt proves that a fully authenticated caller
// (r.UserID != 0) is not subject to the getPassword IP rate limit.
func TestGetPasswordAuthenticatedExempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a password (provisional).
	alice, err := s.CreateUsernameUser(ctx, "gpxempt", "Exempt", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, alice.ID, "gpxempt"); err != nil {
		t.Fatal(err)
	}

	// Register auth key and bind to alice (authenticated).
	keyID := int64(0x3)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("192.0.2.3")

	// Build a request with UserID set (authenticated).
	var buf bin.Buffer
	if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf); err != nil {
		t.Fatal(err)
	}

	// Many calls from an authenticated user should all pass -- the IP limit
	// does not apply when r.UserID != 0.
	for range 10 {
		var buf bin.Buffer
		if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf); err != nil {
			t.Fatal(err)
		}
		_, err := api.GetPasswordForTest(s, alice.ID, &mtproto.Request{
			Ctx:        context.Background(),
			UserID:     alice.ID,
			AuthKeyID:  [8]byte{3},
			ClientAddr: addr,
			Buf:        &buf,
		})
		if err != nil {
			t.Fatalf("authenticated call: %v", err)
		}
	}
}
