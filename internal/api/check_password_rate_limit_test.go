package api_test

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

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

// TestCheckPasswordFailedProofCharges proves that a failed SRP proof keeps
// the reserved token consumed (no refund on failure). The refund-on-success
// path is tested at the store level in TestReserveAndRefund.
func TestCheckPasswordFailedProofCharges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296301")
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
	authKeyID[7] = 4
	keyID := mtproto.AuthKeyIDInt64(authKeyID)
	if err := s.SaveAuthKey(ctx, keyID, []byte("key")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPendingUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("192.0.2.4")
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// One failed attempt consumes the only token.
	_, err = api.CheckPasswordForTestWithLimits(s, authKeyID, addr, cfg, cfg, &tg.AuthCheckPasswordRequest{
		Password: &tg.InputCheckPasswordSRP{SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256)},
	})
	if err == nil {
		t.Fatal("expected error from failed SRP proof")
	}

	// A second attempt should be denied with FLOOD_WAIT (token consumed, no refund on failure).
	_, err = api.CheckPasswordForTestWithLimits(s, authKeyID, addr, cfg, cfg, &tg.AuthCheckPasswordRequest{
		Password: &tg.InputCheckPasswordSRP{SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256)},
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
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

// TestCheckPasswordIPReserveErrorRefundsAccount proves that when the per-IP
// reserve returns an error (invalid address), the already-consumed account
// token is refunded. Without the refund the account budget is drained by
// errors that never reached SRP verification.
func TestCheckPasswordIPReserveErrorRefundsAccount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296401")
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
	authKeyID[7] = 5
	keyID := mtproto.AuthKeyIDInt64(authKeyID)
	if err := s.SaveAuthKey(ctx, keyID, []byte("key")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPendingUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Per-account limit of 1 so a leaked token is visible.
	perAccount := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	// Per-IP limit enabled so reserveRateLimitIP runs.
	perIP := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}

	// netip.Addr{} is invalid, so IPBucketKey returns ok=false and
	// reserveRateLimitIP returns an error (FLOOD_WAIT). This hits the error
	// branch between the account reserve and the IP reserve.
	_, err = api.CheckPasswordForTestWithLimits(s, authKeyID, netip.Addr{}, perAccount, perIP, &tg.AuthCheckPasswordRequest{
		Password: &tg.InputCheckPasswordSRP{A: make([]byte, 256), M1: make([]byte, 256)},
	})
	if err == nil {
		t.Fatal("expected error from invalid IP address")
	}

	// Budget should still be available — the account token was refunded.
	result, err := s.CheckRateLimitBudget(ctx, alice.ID, "check_password", perAccount)
	if err != nil {
		t.Fatalf("CheckRateLimitBudget: %v", err)
	}
	if result != nil {
		t.Fatal("account budget was consumed despite refund — token leaked on IP reserve error")
	}
}

// TestCheckPasswordIPReserveErrorReturnsOriginalError proves that the error
// returned to the client is the original error from the IP reserve, not the
// refund error.
func TestCheckPasswordIPReserveErrorReturnsOriginalError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551296402")
	if err != nil {
		t.Fatal(err)
	}

	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   alice.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: make([]byte, 256),
	}); err != nil {
		t.Fatal(err)
	}

	var authKeyID [8]byte
	authKeyID[7] = 6
	keyID := mtproto.AuthKeyIDInt64(authKeyID)
	if err := s.SaveAuthKey(ctx, keyID, []byte("key")); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPendingUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	perAccount := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	perIP := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}

	_, err = api.CheckPasswordForTestWithLimits(s, authKeyID, netip.Addr{}, perAccount, perIP, &tg.AuthCheckPasswordRequest{
		Password: &tg.InputCheckPasswordSRP{A: make([]byte, 256), M1: make([]byte, 256)},
	})
	if err == nil {
		t.Fatal("expected error")
	}

	// The error should be FLOOD_WAIT from the invalid IP, not an internal error.
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Code != 420 {
		t.Fatalf("expected FLOOD_WAIT (code 420), got %T: %v", err, err)
	}
}
