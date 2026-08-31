package api_test

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// countUsers returns the number of rows in the users table.
func countUsers(t *testing.T, dsn string) int64 {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	var count int64
	err = pool.QueryRow(context.Background(), "SELECT count(*) FROM users").Scan(&count)
	if err != nil {
		t.Fatalf("count users: %v", err)
	}
	return count
}

// isSessionPasswordNeeded reports whether err is a 401 SESSION_PASSWORD_NEEDED error.
func isSessionPasswordNeeded(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 401 && rpc.Message == "SESSION_PASSWORD_NEEDED"
}

// isAuthSignUpRequired reports whether the result is an AuthAuthorizationSignUpRequired.
func isAuthSignUpRequired(res bin.Encoder) bool {
	_, ok := res.(*tg.AuthAuthorizationSignUpRequired)
	return ok
}

func TestSignInUsernameUnknownReturnsSignUpRequired(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Count users before — the test asserts no new row is created.
	before := countUsers(t, dsn)

	// Issue a code for the username (so the hash is valid).
	hash, _, err := s.IssueCodeForUsername(ctx, "unknown")
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.1")
	cfg := store.RateLimitConfig{} // disabled

	res, err := api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   "unknown",
		PhoneCodeHash: hash,
		PhoneCode:     "", // ignored in username mode
	})
	if err != nil {
		t.Fatalf("signIn with unknown username: expected no error, got %v", err)
	}
	if !isAuthSignUpRequired(res) {
		t.Fatalf("result = %T, want *tg.AuthAuthorizationSignUpRequired", res)
	}

	// Database-level assertion: no row was written to users.
	after := countUsers(t, dsn)
	if after != before {
		t.Errorf("users table grew from %d to %d — signIn must not create a user for unknown username", before, after)
	}
}

func TestSignInUsernameUnknownCodeIsBounded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	hash, _, err := s.IssueCodeForUsername(ctx, "bounded")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.SignInForTestWithLimits(s, [8]byte{1}, netip.MustParseAddr("10.0.0.11"), store.RateLimitConfig{}, &tg.AuthSignInRequest{
		PhoneNumber:   "bounded",
		PhoneCodeHash: hash,
		PhoneCode:     strings.Repeat("x", 512),
	})
	if err != nil {
		t.Fatalf("signIn with unknown username: %v", err)
	}
	if !isAuthSignUpRequired(res) {
		t.Fatalf("result = %T, want *tg.AuthAuthorizationSignUpRequired", res)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var code string
	if err := pool.QueryRow(ctx, `SELECT code FROM phone_codes WHERE code_hash = $1`, hash).Scan(&code); err != nil {
		t.Fatal(err)
	}
	if len([]byte(code)) > 256 {
		t.Fatalf("stored code length = %d bytes, want at most 256", len([]byte(code)))
	}
}

func TestSignInUsernameUnknownCodeWriteIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	hash, _, err := s.IssueCodeForUsername(ctx, "idempotent")
	if err != nil {
		t.Fatal(err)
	}

	for i, code := range []string{"first-secret", "second-secret"} {
		res, err := api.SignInForTestWithLimits(s, [8]byte{byte(i + 1)}, netip.MustParseAddr("10.0.0.12"), store.RateLimitConfig{}, &tg.AuthSignInRequest{
			PhoneNumber:   "idempotent",
			PhoneCodeHash: hash,
			PhoneCode:     code,
		})
		if err != nil {
			t.Fatalf("signIn %d: %v", i, err)
		}
		if !isAuthSignUpRequired(res) {
			t.Fatalf("signIn %d result = %T, want *tg.AuthAuthorizationSignUpRequired", i, res)
		}
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var code string
	if err := pool.QueryRow(ctx, `SELECT code FROM phone_codes WHERE code_hash = $1`, hash).Scan(&code); err != nil {
		t.Fatal(err)
	}
	if code != "first-secret" {
		t.Fatalf("stored code = %q, want first-secret", code)
	}
}

func TestSignInUsernameKnownWithVerifierReturnsPasswordNeeded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user. CreateUsernameUser does NOT claim the
	// username in the usernames table — the handler is responsible for that.
	user, err := s.CreateUsernameUser(ctx, "testuser", "Test", "User")
	if err != nil {
		t.Fatal(err)
	}

	// Manually claim the username because UpdateUsername rejects changes for
	// login_mode='username' accounts (the handle is the credential).
	if err := api.ClaimUsernameForTest(s, user.ID, "testuser"); err != nil {
		t.Fatal(err)
	}

	// Set a cloud password (verifier).
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   user.ID,
		Salt1:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Salt2:    []byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		Verifier: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	}); err != nil {
		t.Fatal(err)
	}

	// Issue a code for the username.
	hash, _, err := s.IssueCodeForUsername(ctx, "testuser")
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key so SetPendingUser succeeds.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.2")
	cfg := store.RateLimitConfig{} // disabled

	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   "testuser",
		PhoneCodeHash: hash,
		PhoneCode:     "", // ignored in username mode
	})
	if !isSessionPasswordNeeded(err) {
		t.Fatalf("signIn with known user + verifier: expected SESSION_PASSWORD_NEEDED, got %v", err)
	}
}

func TestSignInUsernameChannelTreatedAsUnknown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Count users before.
	before := countUsers(t, dsn)

	// Create a user first (required as channel creator).
	creator, err := s.CreateUser(ctx, "+15551297001")
	if err != nil {
		t.Fatal(err)
	}

	// Create a channel and claim a username for it.
	ch, err := s.CreateChannel(ctx, creator.ID, "Channel", "About", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimChannelUsername(ctx, ch.ID, "channelx"); err != nil {
		t.Fatal(err)
	}

	// Issue a code for the username.
	hash, _, err := s.IssueCodeForUsername(ctx, "channelx")
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.3")
	cfg := store.RateLimitConfig{}

	res, err := api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   "channelx",
		PhoneCodeHash: hash,
		PhoneCode:     "",
	})
	if err != nil {
		t.Fatalf("signIn with channel username: expected no error, got %v", err)
	}
	if !isAuthSignUpRequired(res) {
		t.Fatalf("result = %T, want *tg.AuthAuthorizationSignUpRequired", res)
	}

	// No user row created for the signIn attempt (creator was already counted).
	after := countUsers(t, dsn)
	if after != before+1 {
		t.Errorf("users table grew from %d to %d — channel username must not create a new user row", before, after)
	}
}

func TestSignInUsernameInvalidHash(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	addr := netip.MustParseAddr("10.0.0.4")
	cfg := store.RateLimitConfig{}

	_, err := api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: "invalid-hash",
		PhoneCode:     "",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("signIn with invalid hash: expected PHONE_CODE_INVALID, got %v", err)
	}
}

func TestSignInUsernameExpiredHash(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Issue a code for the username.
	hash, _, err := s.IssueCodeForUsername(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}

	// Age the code past expiry.
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	_, err = pool.Exec(ctx, "UPDATE phone_codes SET expires_at = '2020-01-01' WHERE code_hash = $1", hash)
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.5")
	cfg := store.RateLimitConfig{}

	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: hash,
		PhoneCode:     "",
	})
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("signIn with expired hash: expected RPC error, got %v", err)
	}
	if rpc.Code != 400 {
		t.Errorf("got code %d, want 400", rpc.Code)
	}
	if rpc.Message != "PHONE_CODE_INVALID" {
		t.Errorf("got message %q, want PHONE_CODE_INVALID", rpc.Message)
	}
}

func TestSignInUsernamePhonePathUnchanged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	phone := "+15551296901"
	if _, err := s.CreateUser(ctx, phone); err != nil {
		t.Fatal(err)
	}
	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key so BindAuthKeyUser succeeds.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.6")
	cfg := store.RateLimitConfig{}

	res, err := api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: hash,
		PhoneCode:     code,
	})
	if err != nil {
		t.Fatalf("phone-mode signIn: expected success, got %v", err)
	}
	auth, ok := res.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("result = %T, want *tg.AuthAuthorization", res)
	}
	if auth.User == nil {
		t.Error("authorization has no user")
	}
}

func TestSignInUsernameCaseInsensitive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user.
	user, err := s.CreateUsernameUser(ctx, "caseuser", "Test", "User")
	if err != nil {
		t.Fatal(err)
	}
	// Claim the username directly (UpdateUsername rejects for login_mode='username').
	if err := s.ClaimUsername(ctx, user.ID, "caseuser"); err != nil {
		t.Fatal(err)
	}

	// Set a cloud password.
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   user.ID,
		Salt1:    []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Salt2:    []byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1},
		Verifier: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
	}); err != nil {
		t.Fatal(err)
	}

	// Issue a code for the username (lowercase).
	hash, _, err := s.IssueCodeForUsername(ctx, "caseuser")
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.7")
	cfg := store.RateLimitConfig{}

	// signIn with mixed case — should resolve the same user.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   "CaseUser",
		PhoneCodeHash: hash,
		PhoneCode:     "",
	})
	if !isSessionPasswordNeeded(err) {
		t.Fatalf("signIn with mixed-case username: expected SESSION_PASSWORD_NEEDED, got %v", err)
	}
}

func TestSignInUsernameProvisionalNoVerifier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Create a username-mode user without a cloud password.
	user, err := s.CreateUsernameUser(ctx, "provis", "Provisional", "User")
	if err != nil {
		t.Fatal(err)
	}
	// Claim the username directly (UpdateUsername rejects for login_mode='username').
	if err := s.ClaimUsername(ctx, user.ID, "provis"); err != nil {
		t.Fatal(err)
	}

	// Save the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	// Issue a code for the username.
	hash, _, err := s.IssueCodeForUsername(ctx, "provis")
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.8")
	cfg := store.RateLimitConfig{}

	// signIn with a provisional user (no verifier) should return an error.
	_, err = api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   "provis",
		PhoneCodeHash: hash,
		PhoneCode:     "",
	})
	if err == nil {
		t.Fatal("signIn with provisional user: expected error, got nil")
	}
}

// TestSignInUsernameUnknownNoUserCreated is the M7 test requirement: the
// authorizationSignUpRequired path must not silently create a user row.
func TestSignInUsernameUnknownNoUserCreated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Count users before.
	before := countUsers(t, dsn)

	addr := netip.MustParseAddr("10.0.0.9")
	cfg := store.RateLimitConfig{}

	// signIn with an unknown username and a valid hash (issued for the same
	// identifier) — the handler must return authorizationSignUpRequired without
	// calling CreateUser.
	hash, _, err := s.IssueCodeForUsername(ctx, "nonexist")
	if err != nil {
		t.Fatal(err)
	}

	res, err := api.SignInForTestWithLimits(s, [8]byte{1}, addr, cfg, &tg.AuthSignInRequest{
		PhoneNumber:   "nonexist",
		PhoneCodeHash: hash,
		PhoneCode:     "",
	})
	if err != nil {
		t.Fatalf("signIn with unknown username: expected no error, got %v", err)
	}
	if !isAuthSignUpRequired(res) {
		t.Fatalf("result = %T, want *tg.AuthAuthorizationSignUpRequired", res)
	}

	// Database-level assertion: no row was written to users.
	after := countUsers(t, dsn)
	if after != before {
		t.Errorf("users table grew from %d to %d — signIn must not create a user for unknown username", before, after)
	}
}
