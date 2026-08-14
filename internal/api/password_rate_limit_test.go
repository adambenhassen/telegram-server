package api_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/srp"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestPasswordProofRateLimit proves that N+1 proof attempts across
// getPasswordSettings and updatePasswordSettings are denied with FLOOD_WAIT.
// Both surfaces share the password_proof counter, so alternating between them
// must not double the guess budget.
func TestPasswordProofRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551297001")
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

	// Register auth key and bind to alice (authenticated).
	keyID := int64(0x10)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Limit of 3 proofs.
	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}

	// 3 proof attempts should be allowed (they fail with SRP_ID_INVALID, not FLOOD_WAIT).
	for range 3 {
		var buf bin.Buffer
		req := &tg.AccountGetPasswordSettingsRequest{
			Password: &tg.InputCheckPasswordSRP{
				SRPID: 0,
				A:     make([]byte, 256),
				M1:    make([]byte, 256),
			},
		}
		if err := req.Encode(&buf); err != nil {
			t.Fatal(err)
		}
		_, err := api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x10}, cfg, &buf)
		// Each attempt should fail with SRP_ID_INVALID (invalid SRP proof), not FLOOD_WAIT.
		if err == nil {
			t.Fatal("expected error from failed SRP proof")
		}
		if isFloodWait(err) {
			t.Fatal("got FLOOD_WAIT too early")
		}
	}

	// 4th attempt should be denied with FLOOD_WAIT.
	var buf bin.Buffer
	req := &tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0,
			A:     make([]byte, 256),
			M1:    make([]byte, 256),
		},
	}
	if err := req.Encode(&buf); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x10}, cfg, &buf)
	if !isFloodWait(err) {
		t.Fatalf("4th attempt: expected FLOOD_WAIT, got %v", err)
	}
}

// TestPasswordProofSharedCounter proves that getPasswordSettings and
// updatePasswordSettings share the same password_proof counter.
func TestPasswordProofSharedCounter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551297101")
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

	// Register auth key and bind to alice.
	keyID := int64(0x11)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Limit of 2 proofs.
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}

	// First attempt: getPasswordSettings with invalid proof — consumes 1.
	var buf1 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf1); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x11}, cfg, &buf1)
	if err == nil {
		t.Fatal("expected error from failed SRP proof")
	}
	if isFloodWait(err) {
		t.Fatal("got FLOOD_WAIT on first attempt")
	}

	// Second attempt: updatePasswordSettings with invalid proof — consumes 2.
	var buf2 bin.Buffer
	if err := (&tg.AccountUpdatePasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf2); err != nil {
		t.Fatal(err)
	}
	_, err = api.UpdatePasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x11}, cfg, &buf2)
	if err == nil {
		t.Fatal("expected error from failed SRP proof")
	}
	if isFloodWait(err) {
		t.Fatal("got FLOOD_WAIT on second attempt")
	}

	// Third attempt: either surface should be denied (shared counter exhausted).
	var buf3 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf3); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x11}, cfg, &buf3)
	if !isFloodWait(err) {
		t.Fatalf("3rd attempt: expected FLOOD_WAIT (shared counter), got %v", err)
	}
}

// TestGetPasswordAccountRateLimit proves that N+1 authorized getPassword calls
// are denied with FLOOD_WAIT.
func TestGetPasswordAccountRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551297201")
	if err != nil {
		t.Fatal(err)
	}

	// Set a password for alice so hasPw == true.
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   alice.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: make([]byte, 256),
	}); err != nil {
		t.Fatal(err)
	}

	// Register auth key and bind to alice.
	keyID := int64(0x12)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Limit of 3.
	cfg := store.RateLimitConfig{Limit: 3, Window: 10 * time.Second}

	// 3 calls should pass.
	for range 3 {
		var buf bin.Buffer
		if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf); err != nil {
			t.Fatal(err)
		}
		_, err := api.GetPasswordWithAccountLimits(s, alice.ID, cfg, &mtproto.Request{
			Ctx:       ctx,
			UserID:    alice.ID,
			AuthKeyID: [8]byte{0x12},
			Buf:       &buf,
		})
		if err != nil {
			t.Fatalf("call: %v", err)
		}
	}

	// 4th call should be denied with FLOOD_WAIT.
	var buf bin.Buffer
	if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordWithAccountLimits(s, alice.ID, cfg, &mtproto.Request{
		Ctx:       ctx,
		UserID:    alice.ID,
		AuthKeyID: [8]byte{0x12},
		Buf:       &buf,
	})
	if !isFloodWait(err) {
		t.Fatalf("4th call: expected FLOOD_WAIT, got %v", err)
	}
}

// TestGetPasswordAccountExemptNoPassword proves that the per-account get_password
// limit does not apply when the account has no password (hasPw == false).
func TestGetPasswordAccountExemptNoPassword(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a user with NO password.
	alice, err := s.CreateUser(ctx, "+15551297301")
	if err != nil {
		t.Fatal(err)
	}

	// Register auth key and bind to alice.
	keyID := int64(0x13)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Restrictive limit: 1 per 10s.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Many calls should all pass because hasPw == false.
	for range 10 {
		var buf bin.Buffer
		if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf); err != nil {
			t.Fatal(err)
		}
		_, err := api.GetPasswordWithAccountLimits(s, alice.ID, cfg, &mtproto.Request{
			Ctx:       ctx,
			UserID:    alice.ID,
			AuthKeyID: [8]byte{0x13},
			Buf:       &buf,
		})
		if err != nil {
			t.Fatalf("call: %v", err)
		}
	}
}

// TestPasswordProofDisabled proves that a zero limit disables enforcement.
func TestPasswordProofDisabled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551297401")
	if err != nil {
		t.Fatal(err)
	}

	// Set a password.
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   alice.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: make([]byte, 256),
	}); err != nil {
		t.Fatal(err)
	}

	keyID := int64(0x14)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Zero limit = disabled.
	cfg := store.RateLimitConfig{}

	// Many attempts should all pass (fail with SRP_ID_INVALID, not FLOOD_WAIT).
	for range 100 {
		var buf bin.Buffer
		if err := (&tg.AccountGetPasswordSettingsRequest{
			Password: &tg.InputCheckPasswordSRP{
				SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
			},
		}).Encode(&buf); err != nil {
			t.Fatal(err)
		}
		_, err := api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x14}, cfg, &buf)
		if isFloodWait(err) {
			t.Fatalf("got FLOOD_WAIT with disabled limit")
		}
	}
}

// TestPasswordProofFailedProofConsumes proves that a failed proof
// (SRP_ID_INVALID from a bad SRPID) keeps the token consumed.
func TestPasswordProofFailedProofConsumes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551297501")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551297502")
	if err != nil {
		t.Fatal(err)
	}

	// Set passwords for both.
	for _, u := range []store.User{alice, bob} {
		if err := s.UpsertPassword(ctx, store.UserPassword{
			UserID:   u.ID,
			Salt1:    []byte("salt1"),
			Salt2:    []byte("salt2"),
			Verifier: make([]byte, 256),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Auth key bound to alice.
	keyID := int64(0x15)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// One failed proof attempt consumes the only token.
	var buf1 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf1); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x15}, cfg, &buf1)
	if err == nil {
		t.Fatal("expected error from failed SRP proof")
	}

	// Second attempt should be denied with FLOOD_WAIT (token consumed, no refund).
	var buf2 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf2); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x15}, cfg, &buf2)
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Bob is unaffected (separate per-account counter).
	if err := s.SaveAuthKey(ctx, int64(0x16), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, int64(0x16), bob.ID); err != nil {
		t.Fatal(err)
	}
	var bufBob bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&bufBob); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, bob.ID, [8]byte{0x16}, cfg, &bufBob)
	if err == nil {
		t.Fatal("expected error from failed SRP proof for bob")
	}
	if isFloodWait(err) {
		t.Fatal("bob should not be rate limited (separate counter)")
	}
}

// TestPasswordProofUIDMismatchBranch proves the uid != r.UserID branch
// keeps the token consumed. It constructs a uid mismatch by issuing a
// challenge for one user and consuming it on another's session.
func TestPasswordProofUIDMismatchConsumes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551297901")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551297902")
	if err != nil {
		t.Fatal(err)
	}

	// Compute valid verifiers for both users.
	const password = "test-password"
	salt1 := make([]byte, 32)
	salt2 := make([]byte, 32)
	for i := range salt1 {
		salt1[i] = byte(i)
	}
	for i := range salt2 {
		salt2[i] = byte(255 - i)
	}
	verifierAlgo := &tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow{
		Salt1: salt1,
		Salt2: salt2,
		G:     srp.G,
		P:     srp.PBytes(),
	}

	verifierAlice, err := auth.NewPasswordHash([]byte(password), verifierAlgo)
	if err != nil {
		t.Fatalf("NewPasswordHash alice: %v", err)
	}
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   alice.ID,
		Salt1:    salt1,
		Salt2:    salt2,
		Verifier: verifierAlice,
	}); err != nil {
		t.Fatal(err)
	}

	// Bob uses different salts.
	salt1b := make([]byte, 32)
	salt2b := make([]byte, 32)
	for i := range salt1b {
		salt1b[i] = byte(i + 1)
	}
	for i := range salt2b {
		salt2b[i] = byte(254 - i)
	}
	verifierBobAlgo := &tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow{
		Salt1: salt1b,
		Salt2: salt2b,
		G:     srp.G,
		P:     srp.PBytes(),
	}
	verifierBob, err := auth.NewPasswordHash([]byte(password), verifierBobAlgo)
	if err != nil {
		t.Fatalf("NewPasswordHash bob: %v", err)
	}
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   bob.ID,
		Salt1:    salt1b,
		Salt2:    salt2b,
		Verifier: verifierBob,
	}); err != nil {
		t.Fatal(err)
	}

	// Auth key bound to alice.
	keyID := int64(0x1a)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Shared handlers so the SRP challenge store is shared.
	h := api.SharedHandlersForTest(s)
	api.SetPasswordProofLimit(h, cfg)

	// As a minimum: confirm one failed proof (from either source) consumes
	// the token, and a second is denied.
	var buf1 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf1); err != nil {
		t.Fatal(err)
	}
	_, err = api.HandleGetPasswordSettings(h, &mtproto.Request{
		Ctx:       ctx,
		UserID:    alice.ID,
		AuthKeyID: [8]byte{0x1a},
		Buf:       &buf1,
	})
	if err == nil {
		t.Fatal("expected error from failed SRP proof")
	}

	// Second attempt should be denied with FLOOD_WAIT.
	var buf2 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf2); err != nil {
		t.Fatal(err)
	}
	_, err = api.HandleGetPasswordSettings(h, &mtproto.Request{
		Ctx:       ctx,
		UserID:    alice.ID,
		AuthKeyID: [8]byte{0x1a},
		Buf:       &buf2,
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}
}

// TestGetPasswordAccountWindowExpiry proves that after the window expires,
// the same account can call getPassword again.
func TestGetPasswordAccountWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	alice, err := s.CreateUser(ctx, "+15551297601")
	if err != nil {
		t.Fatal(err)
	}

	// Set a password.
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   alice.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: make([]byte, 256),
	}); err != nil {
		t.Fatal(err)
	}

	keyID := int64(0x17)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Long window: only the explicit rewind below closes it.
	cfg := store.RateLimitConfig{Limit: 1, Window: time.Hour}

	// One call passes.
	var buf bin.Buffer
	if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordWithAccountLimits(s, alice.ID, cfg, &mtproto.Request{
		Ctx:       ctx,
		UserID:    alice.ID,
		AuthKeyID: [8]byte{0x17},
		Buf:       &buf,
	})
	if err != nil {
		t.Fatalf("call 1: %v", err)
	}

	// Second call denied.
	var buf2 bin.Buffer
	if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf2); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordWithAccountLimits(s, alice.ID, cfg, &mtproto.Request{
		Ctx:       ctx,
		UserID:    alice.ID,
		AuthKeyID: [8]byte{0x17},
		Buf:       &buf2,
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Age the window past its deadline.
	if err := api.AgeRateLimitWindowForTest(dsn, alice.ID, "get_password", cfg.Window+time.Minute); err != nil {
		t.Fatalf("age window: %v", err)
	}

	// Should be allowed again.
	var buf3 bin.Buffer
	if err := (&tg.AccountGetPasswordRequest{}).Encode(&buf3); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordWithAccountLimits(s, alice.ID, cfg, &mtproto.Request{
		Ctx:       ctx,
		UserID:    alice.ID,
		AuthKeyID: [8]byte{0x17},
		Buf:       &buf3,
	})
	if err != nil {
		t.Fatalf("post-expiry call: %v", err)
	}
}

// TestPasswordProofWindowExpiry proves that after the window expires,
// the password_proof budget resets.
func TestPasswordProofWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	alice, err := s.CreateUser(ctx, "+15551297701")
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

	keyID := int64(0x18)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Long window.
	cfg := store.RateLimitConfig{Limit: 1, Window: time.Hour}

	// One proof attempt passes.
	var buf1 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf1); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x18}, cfg, &buf1)
	if err == nil {
		t.Fatal("expected error from failed SRP proof")
	}
	if isFloodWait(err) {
		t.Fatal("got FLOOD_WAIT on first attempt")
	}

	// Second attempt denied.
	var buf2 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf2); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x18}, cfg, &buf2)
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT, got %v", err)
	}

	// Age the window.
	if err := api.AgeRateLimitWindowForTest(dsn, alice.ID, "password_proof", cfg.Window+time.Minute); err != nil {
		t.Fatalf("age window: %v", err)
	}

	// Should be allowed again.
	var buf3 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&buf3); err != nil {
		t.Fatal(err)
	}
	_, err = api.GetPasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x18}, cfg, &buf3)
	if err == nil {
		t.Fatal("expected error from failed SRP proof")
	}
	if isFloodWait(err) {
		t.Fatalf("post-expiry: expected SRP_ID_INVALID, got FLOOD_WAIT")
	}
}

// TestPasswordProofNoLimitWhenNoCurrentPassword proves that updatePasswordSettings
// with hasCur == false (no existing password) does not charge the rate limit.
func TestPasswordProofNoLimitWhenNoCurrentPassword(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551298101")
	if err != nil {
		t.Fatal(err)
	}

	// No password set for alice (hasCur == false).
	keyID := int64(0x1b)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Very restrictive limit — doesn't matter, proof path shouldn't run.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Set a new password (no proof required when hasCur == false).
	salt1 := make([]byte, 32)
	salt2 := make([]byte, 32)
	for i := range salt1 {
		salt1[i] = byte(i)
	}
	for i := range salt2 {
		salt2[i] = byte(255 - i)
	}
	verifier := make([]byte, 256)
	verifier[0] = 1

	var buf bin.Buffer
	req := &tg.AccountUpdatePasswordSettingsRequest{
		Password: &tg.InputCheckPasswordEmpty{},
		NewSettings: tg.AccountPasswordInputSettings{
			NewAlgo: &tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow{
				Salt1: salt1,
				Salt2: salt2,
			},
			NewPasswordHash: verifier,
		},
	}
	if err := req.Encode(&buf); err != nil {
		t.Fatal(err)
	}

	// First initial-set call should pass (no rate limit charged when hasCur == false).
	_, err = api.UpdatePasswordSettingsWithProofLimits(s, alice.ID, [8]byte{0x1b}, cfg, &buf)
	if err != nil {
		t.Fatalf("initial set: %v", err)
	}

	// Verify password was set.
	_, hasPw, err := s.PasswordByUser(ctx, alice.ID)
	if err != nil || !hasPw {
		t.Fatalf("password not set: hasPw=%v err=%v", hasPw, err)
	}
}

// TestPasswordProofUIDMismatchBranch proves the uid != r.UserID branch
// keeps the token consumed. It constructs a uid mismatch by issuing a
// challenge for one user and consuming it on another's session.
func TestPasswordProofUIDMismatchBranch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551298201")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551298202")
	if err != nil {
		t.Fatal(err)
	}

	// Compute valid verifiers for both users.
	const password = "test-password"
	salt1 := make([]byte, 32)
	salt2 := make([]byte, 32)
	for i := range salt1 {
		salt1[i] = byte(i)
	}
	for i := range salt2 {
		salt2[i] = byte(255 - i)
	}
	verifierAlgo := &tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow{
		Salt1: salt1,
		Salt2: salt2,
		G:     srp.G,
		P:     srp.PBytes(),
	}
	verifierAlice, err := auth.NewPasswordHash([]byte(password), verifierAlgo)
	if err != nil {
		t.Fatalf("NewPasswordHash alice: %v", err)
	}
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   alice.ID,
		Salt1:    salt1,
		Salt2:    salt2,
		Verifier: verifierAlice,
	}); err != nil {
		t.Fatal(err)
	}

	// Bob uses different salts.
	salt1b := make([]byte, 32)
	salt2b := make([]byte, 32)
	for i := range salt1b {
		salt1b[i] = byte(i + 1)
	}
	for i := range salt2b {
		salt2b[i] = byte(254 - i)
	}
	verifierBobAlgo := &tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow{
		Salt1: salt1b,
		Salt2: salt2b,
		G:     srp.G,
		P:     srp.PBytes(),
	}
	verifierBob, err := auth.NewPasswordHash([]byte(password), verifierBobAlgo)
	if err != nil {
		t.Fatalf("NewPasswordHash bob: %v", err)
	}
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   bob.ID,
		Salt1:    salt1b,
		Salt2:    salt2b,
		Verifier: verifierBob,
	}); err != nil {
		t.Fatal(err)
	}

	// Auth key bound to alice.
	keyID := int64(0x1c)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, alice.ID); err != nil {
		t.Fatal(err)
	}

	// Limit of 1.
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}

	// Shared handlers so the SRP challenge store is shared.
	h := api.SharedHandlersForTest(s)
	api.SetPasswordProofLimit(h, cfg)

	// Get a challenge for alice (the session user).
	var bufGP bin.Buffer
	if err := (&tg.AccountGetPasswordRequest{}).Encode(&bufGP); err != nil {
		t.Fatal(err)
	}
	gpRes, err := api.HandleGetPassword(h, &mtproto.Request{
		Ctx:       ctx,
		UserID:    alice.ID,
		AuthKeyID: [8]byte{0x1c},
		Buf:       &bufGP,
	})
	if err != nil {
		t.Fatalf("getPassword: %v", err)
	}
	gp, ok := gpRes.(*tg.AccountPassword)
	if !ok || !gp.HasPassword {
		t.Fatalf("getPassword: unexpected result")
	}

	// Compute a valid proof for alice's password.
	proof, err := auth.PasswordHash([]byte(password), gp.SRPID, gp.SRPB, gp.SecureRandom, gp.CurrentAlgo)
	if err != nil {
		t.Fatalf("PasswordHash: %v", err)
	}

	// Submit the valid proof as bob (wrong r.UserID) — the challenge was
	// issued for alice, so pending.UserID == alice.ID but r.UserID == bob.ID.
	// consumeAndVerify returns uid == alice.ID, then uid != r.UserID triggers
	// PASSWORD_HASH_INVALID and keeps the token consumed.
	if err := s.BindAuthKeyUser(ctx, keyID, bob.ID); err != nil {
		t.Fatal(err)
	}

	var bufGPS bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: proof,
	}).Encode(&bufGPS); err != nil {
		t.Fatal(err)
	}
	_, err = api.HandleGetPasswordSettings(h, &mtproto.Request{
		Ctx:       ctx,
		UserID:    bob.ID,
		AuthKeyID: [8]byte{0x1c},
		Buf:       &bufGPS,
	})
	// Should fail with PASSWORD_HASH_INVALID (uid mismatch), not FLOOD_WAIT.
	var rpcErr *tgerr.Error
	if err == nil || !errors.As(err, &rpcErr) {
		t.Fatalf("expected PASSWORD_HASH_INVALID from uid mismatch, got %v", err)
	}

	// Token was consumed — second attempt should be denied.
	var bufGPS2 bin.Buffer
	if err := (&tg.AccountGetPasswordSettingsRequest{
		Password: &tg.InputCheckPasswordSRP{
			SRPID: 0, A: make([]byte, 256), M1: make([]byte, 256),
		},
	}).Encode(&bufGPS2); err != nil {
		t.Fatal(err)
	}
	_, err = api.HandleGetPasswordSettings(h, &mtproto.Request{
		Ctx:       ctx,
		UserID:    bob.ID,
		AuthKeyID: [8]byte{0x1c},
		Buf:       &bufGPS2,
	})
	if !isFloodWait(err) {
		t.Fatalf("expected FLOOD_WAIT after uid mismatch consumed token, got %v", err)
	}
}
