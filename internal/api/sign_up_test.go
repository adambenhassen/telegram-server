package api_test

import (
	"context"
	"errors"
	"net/netip"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// isInputRequestInvalid reports whether err is a 400 INPUT_REQUEST_INVALID error.
func isInputRequestInvalid(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 400 && rpc.Message == "INPUT_REQUEST_INVALID"
}

// isUsernameOccupied reports whether err is a 400 USERNAME_OCCUPIED error.
func isUsernameOccupied(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 400 && rpc.Message == "USERNAME_OCCUPIED"
}

func TestSignUpRegistrationClosed(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	addr := netip.MustParseAddr("10.0.0.1")
	cfg := store.RateLimitConfig{}

	_, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationClosed, &tg.AuthSignUpRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: "any-hash",
		FirstName:     "Alice",
	})
	if !isInputRequestInvalid(err) {
		t.Fatalf("signUp with closed registration: expected INPUT_REQUEST_INVALID, got %v", err)
	}
}

func isPhoneNumberInvalid(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 400 && rpc.Message == "PHONE_NUMBER_INVALID"
}

func TestSignUpInvalidUsername(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	addr := netip.MustParseAddr("10.0.0.2")
	cfg := store.RateLimitConfig{}

	// Phone number format should be rejected — auth.signUp is not the phone path.
	_, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "+15550001111",
		PhoneCodeHash: "any-hash",
		FirstName:     "Alice",
	})
	if !isPhoneNumberInvalid(err) {
		t.Fatalf("signUp with phone number: expected PHONE_NUMBER_INVALID, got %v", err)
	}

	// Too short username (must be at least 5 chars total per the username pattern).
	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "ab",
		PhoneCodeHash: "any-hash",
		FirstName:     "Alice",
	})
	if !isPhoneNumberInvalid(err) {
		t.Fatalf("signUp with short username: expected PHONE_NUMBER_INVALID, got %v", err)
	}

	// Starts with digit.
	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "1alice",
		PhoneCodeHash: "any-hash",
		FirstName:     "Alice",
	})
	if !isPhoneNumberInvalid(err) {
		t.Fatalf("signUp with digit-leading username: expected PHONE_NUMBER_INVALID, got %v", err)
	}
}

func TestSignUpInvalidHash(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	addr := netip.MustParseAddr("10.0.0.3")
	cfg := store.RateLimitConfig{}

	_, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: "invalid-hash",
		FirstName:     "Alice",
	})
	if !isPhoneCodeInvalid(err) {
		t.Fatalf("signUp with invalid hash: expected PHONE_CODE_INVALID, got %v", err)
	}
}

func TestSignUpSuccess(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Issue a code for the username so the hash is valid.
	hash, _, err := s.IssueCodeForUsername(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key so BindAuthKeyUser succeeds.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	// Count users before.
	before := countUsers(t, dsn)

	addr := netip.MustParseAddr("10.0.0.4")
	cfg := store.RateLimitConfig{}

	res, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: hash,
		FirstName:     "Alice",
		LastName:      "",
	})
	if err != nil {
		t.Fatalf("signUp: expected no error, got %v", err)
	}

	auth, ok := res.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("result = %T, want *tg.AuthAuthorization", res)
	}
	user, ok := auth.User.(*tg.User)
	if !ok {
		t.Fatalf("user = %T, want *tg.User", auth.User)
	}
	if user.ID <= 0 {
		t.Errorf("user id = %d, want positive", user.ID)
	}
	if user.FirstName != "Alice" {
		t.Errorf("first name = %q, want Alice", user.FirstName)
	}

	// Database-level assertion: exactly one user was created.
	after := countUsers(t, dsn)
	if after != before+1 {
		t.Errorf("users table grew from %d to %d, want +1", before, after)
	}

	// Assert the auth key is bound and provisional.
	key, ok, err := s.AuthKeyByID(ctx, int64(0x1))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("auth key not found")
	}
	if key.UserID != user.ID {
		t.Errorf("key user_id = %d, want %d", key.UserID, user.ID)
	}
	if !key.Provisional {
		t.Error("key.Provisional = false, want true")
	}
}

func TestSignUpUsernameAlreadyOccupied(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Create a user and claim the username.
	user, err := s.CreateUsernameUser(ctx, "bobbb", "Bob", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, user.ID, "bobbb"); err != nil {
		t.Fatal(err)
	}

	// Count users before.
	before := countUsers(t, dsn)

	// Issue a code for the username.
	hash, _, err := s.IssueCodeForUsername(ctx, "bobbb")
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.5")
	cfg := store.RateLimitConfig{}

	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "bobbb",
		PhoneCodeHash: hash,
		FirstName:     "Bob Jr",
	})
	if !isUsernameOccupied(err) {
		t.Fatalf("signUp with occupied username: expected USERNAME_OCCUPIED, got %v", err)
	}

	// No new user created.
	after := countUsers(t, dsn)
	if after != before {
		t.Errorf("users table grew from %d to %d, want no change", before, after)
	}
}

func TestSignUpCaseInsensitive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Issue a code with lowercase username.
	hash, _, err := s.IssueCodeForUsername(ctx, "casey")
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	before := countUsers(t, dsn)

	addr := netip.MustParseAddr("10.0.0.6")
	cfg := store.RateLimitConfig{}

	// signUp with mixed case — should normalise to lowercase.
	res, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "Casey",
		PhoneCodeHash: hash,
		FirstName:     "Casey",
	})
	if err != nil {
		t.Fatalf("signUp with mixed-case username: expected no error, got %v", err)
	}

	auth, ok := res.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("result = %T, want *tg.AuthAuthorization", res)
	}
	if _, ok := auth.User.(*tg.User); !ok {
		t.Fatalf("user = %T, want *tg.User", auth.User)
	}

	// Only one user created.
	after := countUsers(t, dsn)
	if after != before+1 {
		t.Errorf("users table grew from %d to %d, want +1", before, after)
	}
}

func TestSignUpDoesNotMutateAuthKeyForClosedRegistration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Save the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.7")
	cfg := store.RateLimitConfig{}

	_, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationClosed, &tg.AuthSignUpRequest{
		PhoneNumber:   "alice",
		PhoneCodeHash: "any-hash",
		FirstName:     "Alice",
	})
	if !isInputRequestInvalid(err) {
		t.Fatalf("expected INPUT_REQUEST_INVALID, got %v", err)
	}

	// Auth key should not be bound to any user.
	key, ok, err := s.AuthKeyByID(ctx, int64(0x1))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("auth key was deleted")
	}
	if key.UserID != 0 {
		t.Errorf("auth key user_id = %d, want 0 (unbound)", key.UserID)
	}
}

func TestSignUpNoUserCreatedOnOccupiedUsername(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Pre-create a user with the target username.
	existing, err := s.CreateUsernameUser(ctx, "takent", "Taken", "User")
	if err != nil {
		t.Fatal(err)
	}
	if err := api.ClaimUsernameForTest(s, existing.ID, "takent"); err != nil {
		t.Fatal(err)
	}

	// Issue a code for the username.
	hash, _, err := s.IssueCodeForUsername(ctx, "takent")
	if err != nil {
		t.Fatal(err)
	}

	before := countUsers(t, dsn)

	// Save the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.8")
	cfg := store.RateLimitConfig{}

	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "takent",
		PhoneCodeHash: hash,
		FirstName:     "New",
	})
	if !isUsernameOccupied(err) {
		t.Fatalf("expected USERNAME_OCCUPIED, got %v", err)
	}

	// No new user row — the atomic transaction rolled back.
	after := countUsers(t, dsn)
	if after != before {
		t.Errorf("users table grew from %d to %d, want no change", before, after)
	}
}

func TestSignUpWithLastName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Issue a code.
	hash, _, err := s.IssueCodeForUsername(ctx, "davee")
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.9")
	cfg := store.RateLimitConfig{}

	res, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "davee",
		PhoneCodeHash: hash,
		FirstName:     "Dave",
		LastName:      "Smith",
	})
	if err != nil {
		t.Fatalf("signUp with last name: expected no error, got %v", err)
	}

	auth, ok := res.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("result = %T, want *tg.AuthAuthorization", res)
	}
	user, ok := auth.User.(*tg.User)
	if !ok {
		t.Fatalf("user = %T, want *tg.User", auth.User)
	}
	// The userTL mapper does not include last_name in the returned User TL,
	// but the user was created with it — verify through the store.
	u, ok, err := s.UserByID(ctx, user.ID)
	if err != nil || !ok {
		t.Fatalf("lookup created user: ok=%v err=%v", ok, err)
	}
	if u.LastName != "Smith" {
		t.Errorf("last name = %q, want Smith", u.LastName)
	}
}

func TestSignUpReservedHandle(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	addr := netip.MustParseAddr("10.0.0.10")
	cfg := store.RateLimitConfig{}

	// Reserved handles like "telegram" must be rejected.
	_, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "telegram",
		PhoneCodeHash: "any-hash",
		FirstName:     "Admin",
	})
	if !isPhoneNumberInvalid(err) {
		t.Fatalf("signUp with reserved handle: expected PHONE_NUMBER_INVALID, got %v", err)
	}

	// "admin" is also reserved.
	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "admin",
		PhoneCodeHash: "any-hash",
		FirstName:     "Admin",
	})
	if !isPhoneNumberInvalid(err) {
		t.Fatalf("signUp with reserved handle 'admin': expected PHONE_NUMBER_INVALID, got %v", err)
	}
}

func TestSignUpRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	// Issue a code for the username.
	hash, _, err := s.IssueCodeForUsername(ctx, "ratelt")
	if err != nil {
		t.Fatal(err)
	}

	// Save the auth key.
	if err := s.SaveAuthKey(ctx, int64(0x1), make([]byte, 256)); err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.11")
	// Limit: 1 call per hour.
	cfg := store.RateLimitConfig{Limit: 1, Window: time.Hour}

	// First call should succeed.
	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "ratelt",
		PhoneCodeHash: hash,
		FirstName:     "Rate",
	})
	if err != nil {
		t.Fatalf("first signUp: expected success, got %v", err)
	}

	// Second call should be rate-limited.
	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "ratelt2",
		PhoneCodeHash: hash,
		FirstName:     "Rate",
	})
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Code != 420 {
		t.Fatalf("second signUp: expected FLOOD_WAIT, got %v", err)
	}
}

func TestSignUpInvalidDisplayName(t *testing.T) {
	t.Parallel()
	s := openStore(t)

	addr := netip.MustParseAddr("10.0.0.12")
	cfg := store.RateLimitConfig{}

	// Empty first name should be rejected.
	_, err := api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "validn",
		PhoneCodeHash: "any-hash",
		FirstName:     "",
	})
	if !isFirstNameInvalid(err) {
		t.Fatalf("signUp with empty first name: expected FIRSTNAME_INVALID, got %v", err)
	}

	// NUL byte in first name should be rejected.
	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "validn",
		PhoneCodeHash: "any-hash",
		FirstName:     "A\x00lice",
	})
	if !isFirstNameInvalid(err) {
		t.Fatalf("signUp with NUL in first name: expected FIRSTNAME_INVALID, got %v", err)
	}

	// NUL byte in last name should be rejected.
	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "validn",
		PhoneCodeHash: "any-hash",
		FirstName:     "Alice",
		LastName:      "S\x00mith",
	})
	if !isFirstNameInvalid(err) {
		t.Fatalf("signUp with NUL in last name: expected FIRSTNAME_INVALID, got %v", err)
	}
}

// isFirstNameInvalid reports whether err is a 400 FIRSTNAME_INVALID error.
func isFirstNameInvalid(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 400 && rpc.Message == "FIRSTNAME_INVALID"
}

func TestSignUpBoundKeyRollsBack(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	// Create a user and bind an auth key to them.
	existing, err := s.CreateUser(ctx, "+15550009999")
	if err != nil {
		t.Fatal(err)
	}
	keyID := int64(0x1)
	if err := s.SaveAuthKey(ctx, keyID, make([]byte, 256)); err != nil {
		t.Fatal(err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, existing.ID); err != nil {
		t.Fatal(err)
	}

	// Record the original binding.
	keyBefore, _, err := s.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}

	// Count users before.
	before := countUsers(t, dsn)

	// Issue a code for the username.
	hash, _, err := s.IssueCodeForUsername(ctx, "rollbk")
	if err != nil {
		t.Fatal(err)
	}

	addr := netip.MustParseAddr("10.0.0.13")
	cfg := store.RateLimitConfig{}

	// Try to sign up with a key that is already bound.
	_, err = api.SignUpForTest(s, [8]byte{1}, addr, cfg, config.RegistrationOpen, &tg.AuthSignUpRequest{
		PhoneNumber:   "rollbk",
		PhoneCodeHash: hash,
		FirstName:     "Rollback",
	})
	if !isAuthKeyUnreg(err) {
		t.Fatalf("signUp with bound key: expected AUTH_KEY_UNREGISTERED, got %v", err)
	}

	// No new user created — the entire transaction rolled back.
	after := countUsers(t, dsn)
	if after != before {
		t.Errorf("users table grew from %d to %d, want no change", before, after)
	}

	// Original key binding intact.
	keyAfter, ok, err := s.AuthKeyByID(ctx, keyID)
	if err != nil || !ok {
		t.Fatalf("auth key lookup: ok=%v err=%v", ok, err)
	}
	if keyAfter.UserID != keyBefore.UserID {
		t.Errorf("key user_id = %d, want %d (original binding intact)", keyAfter.UserID, keyBefore.UserID)
	}
}

func isAuthKeyUnreg(err error) bool {
	var rpc *tgerr.Error
	return errors.As(err, &rpc) && rpc.Code == 401 && rpc.Message == "AUTH_KEY_UNREGISTERED"
}
