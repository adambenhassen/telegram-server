package store_test

import (
	"context"
	"crypto/rand"
	"errors"
	"math/big"
	"testing"

	"github.com/gotd/td/crypto/srp"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestBootstrapAccount_CreatesNewAccount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	params := bootstrapParams(t)
	result, err := s.BootstrapAccount(ctx, params)
	if err != nil {
		t.Fatalf("BootstrapAccount: %v", err)
	}
	if !result.Created {
		t.Error("expected Created=true for new account")
	}
	if result.UserID == 0 {
		t.Error("expected non-zero UserID")
	}

	// Verify the user exists.
	_, found, err := s.UserByID(ctx, result.UserID)
	if err != nil || !found {
		t.Fatalf("UserByID: found=%v err=%v", found, err)
	}

	// Verify the user can be resolved by username.
	resolved, ok, err := s.UserByUsernameWithLoginMode(ctx, params.Handle)
	if err != nil {
		t.Fatalf("UserByUsernameWithLoginMode: %v", err)
	}
	if !ok {
		t.Fatal("expected user to be found by username")
	}
	if resolved.ID != result.UserID {
		t.Errorf("resolved user ID %d != expected %d", resolved.ID, result.UserID)
	}
	if resolved.LoginMode != "username" {
		t.Errorf("login_mode = %q, want username", resolved.LoginMode)
	}

	// Verify the password row exists.
	pw, found, err := s.PasswordByUser(ctx, result.UserID)
	if err != nil || !found {
		t.Fatalf("PasswordByUser: found=%v err=%v", found, err)
	}
	if len(pw.Verifier) == 0 {
		t.Error("expected non-empty verifier")
	}
}

func TestBootstrapAccount_Idempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	// Same salt and password means same verifier → same computed hash
	// However, NewHash appends random bytes to salt1 internally, so the
	// verifier will differ. We need to compute the verifier ourselves.
	password := []byte("test-password")
	salt1 := make([]byte, 32)
	rand.Read(salt1)
	salt2 := make([]byte, 32)
	rand.Read(salt2)

	verifier := computeVerifier(t, password, salt1, salt2)

	params := store.BootstrapParams{
		Handle:    "operator",
		FirstName: "Operator",
		LastName:  "",
		Salt1:     salt1,
		Salt2:     salt2,
		Verifier:  verifier,
	}

	// First call creates.
	result1, err := s.BootstrapAccount(ctx, params)
	if err != nil {
		t.Fatalf("first BootstrapAccount: %v", err)
	}
	if !result1.Created {
		t.Error("expected Created=true for first call")
	}

	// Second call with same params is idempotent.
	result2, err := s.BootstrapAccount(ctx, params)
	if err != nil {
		t.Fatalf("second BootstrapAccount: %v", err)
	}
	if result2.Created {
		t.Error("expected Created=false for idempotent call")
	}
	if result2.UserID != result1.UserID {
		t.Errorf("UserID mismatch: first=%d second=%d", result1.UserID, result2.UserID)
	}
}

func TestBootstrapAccount_SquattedByChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	// Create a user to act as channel creator.
	creator, err := s.CreateUser(ctx, "+15551234567")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// Seed a channel with the bootstrap username.
	ch, err := s.CreateChannel(ctx, creator.ID, "Test Channel", "", false)
	if err != nil {
		t.Fatalf("CreateChannel: %v", err)
	}
	if err := s.ClaimChannelUsername(ctx, ch.ID, "operator"); err != nil {
		t.Fatalf("ClaimChannelUsername: %v", err)
	}

	params := bootstrapParams(t)
	_, err = s.BootstrapAccount(ctx, params)
	if !errors.Is(err, store.ErrBootstrapSquatted) {
		t.Fatalf("expected ErrBootstrapSquatted, got: %v", err)
	}
}

func TestBootstrapAccount_SquattedByPhoneModeUser(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	// Create a phone-mode user and claim the handle.
	u, err := s.CreateUser(ctx, "+15551234567")
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := s.ClaimUsername(ctx, u.ID, "operator"); err != nil {
		t.Fatalf("ClaimUsername: %v", err)
	}

	params := bootstrapParams(t)
	_, err = s.BootstrapAccount(ctx, params)
	if !errors.Is(err, store.ErrBootstrapSquatted) {
		t.Fatalf("expected ErrBootstrapSquatted, got: %v", err)
	}
}

func TestBootstrapAccount_SquattedWrongVerifier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	// Create bootstrap account with one password.
	params := bootstrapParams(t)
	_, err := s.BootstrapAccount(ctx, params)
	if err != nil {
		t.Fatalf("first BootstrapAccount: %v", err)
	}

	// Try bootstrap with different password → different verifier.
	diffSalt1 := make([]byte, 32)
	rand.Read(diffSalt1)
	diffSalt2 := make([]byte, 32)
	rand.Read(diffSalt2)
	diffVerifier := computeVerifier(t, []byte("different-password"), diffSalt1, diffSalt2)

	diffParams := store.BootstrapParams{
		Handle:    params.Handle,
		FirstName: params.FirstName,
		LastName:  params.LastName,
		Salt1:     diffSalt1,
		Salt2:     diffSalt2,
		Verifier:  diffVerifier,
	}

	_, err = s.BootstrapAccount(ctx, diffParams)
	if !errors.Is(err, store.ErrBootstrapSquatted) {
		t.Fatalf("expected ErrBootstrapSquatted, got: %v", err)
	}
}

func TestBootstrapAccount_NoVerifier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	// Create a username-mode user without a password.
	u, err := s.CreateUsernameUser(ctx, "operator", "Operator", "")
	if err != nil {
		t.Fatalf("CreateUsernameUser: %v", err)
	}
	if err := s.ClaimUsername(ctx, u.ID, "operator"); err != nil {
		t.Fatalf("ClaimUsername: %v", err)
	}

	params := bootstrapParams(t)
	_, err = s.BootstrapAccount(ctx, params)
	if !errors.Is(err, store.ErrBootstrapSquatted) {
		t.Fatalf("expected ErrBootstrapSquatted, got: %v", err)
	}
}

func TestValidateBootstrapUsername_Valid(t *testing.T) {
	t.Parallel()
	valid := []string{"abcde", "abcde12345", "a_b_c_d_e", "abcdefghijklmnopqrstuvwxy"}
	for _, name := range valid {
		if err := store.ValidateBootstrapUsername(name); err != nil {
			t.Errorf("ValidateBootstrapUsername(%q): unexpected error: %v", name, err)
		}
	}
}

func TestValidateBootstrapUsername_Invalid(t *testing.T) {
	t.Parallel()
	invalid := []string{
		"",       // empty
		"abc",    // too short
		"1abcde", // starts with digit
		"_abcde", // starts with underscore
		"a-b-c",  // contains hyphen
		"a b c",  // contains space
	}
	for _, name := range invalid {
		if err := store.ValidateBootstrapUsername(name); !errors.Is(err, store.ErrBootstrapInvalid) {
			t.Errorf("ValidateBootstrapUsername(%q): expected ErrBootstrapInvalid, got: %v", name, err)
		}
	}
}

func TestValidateBootstrapUsername_Reserved(t *testing.T) {
	t.Parallel()
	// Only test reserved names that pass format validation (5+ chars, letter start).
	// Names like "me", "bot", "bots", "help", "login" are too short and fail
	// format validation first — which is correct: they are rejected either way.
	reserved := []string{"admin", "support", "settings", "telegram", "channel", "channels", "signup"}
	for _, name := range reserved {
		if err := store.ValidateBootstrapUsername(name); !errors.Is(err, store.ErrBootstrapReserved) {
			t.Errorf("ValidateBootstrapUsername(%q): expected ErrBootstrapReserved, got: %v", name, err)
		}
	}
}

// computeVerifier computes the SRP verifier v = g^x mod p for the given password and salts.
func computeVerifier(tb testing.TB, password, salt1, salt2 []byte) []byte {
	tb.Helper()
	p := srpPBytes()
	alg := srp.Input{
		Salt1: salt1,
		Salt2: salt2,
		G:     3,
		P:     p,
	}
	srpClient := srp.NewSRP(rand.Reader)
	verifier, _, err := srpClient.NewHash(password, alg)
	if err != nil {
		tb.Fatalf("NewHash: %v", err)
	}
	return verifier
}

// bootstrapParams returns a BootstrapParams with a random password (so each
// test gets an independent credential).
func bootstrapParams(tb testing.TB) store.BootstrapParams {
	tb.Helper()
	password := []byte("test-password")
	salt1 := make([]byte, 32)
	rand.Read(salt1)
	salt2 := make([]byte, 32)
	rand.Read(salt2)
	verifier := computeVerifier(tb, password, salt1, salt2)
	return store.BootstrapParams{
		Handle:    "operator",
		FirstName: "Operator",
		LastName:  "",
		Salt1:     salt1,
		Salt2:     salt2,
		Verifier:  verifier,
	}
}

// srpPBytes returns the canonical 256-byte padded Telegram SRP prime.
func srpPBytes() []byte {
	hex := "" +
		"C71CAEB9C6B1C9048E6C522F70F13F73980D40238E3E21C14934D037563D930F" +
		"48198A0AA7C14058229493D22530F4DBFA336F6E0AC925139543AED44CCE7C37" +
		"20FD51F69458705AC68CD4FE6B6B13ABDC9746512969328454F18FAF8C595F64" +
		"2477FE96BB2A941D5BCD1D4AC8CC49880708FA9B378E3C4F3A9060BEE67CF9A4" +
		"A4A695811051907E162753B56B0F6B410DBA74D8A84B2A14B3144E0EF1284754" +
		"FD17ED950D5965B4B9DD46582DB1178D169C6BC465B0D6FF9CA3928FEF5B9AE4" +
		"E418FC15E83EBEA0F87FA9FF5EED70050DED2849F47BF959D956850CE929851F" +
		"0D8115F635B105EE2E4E15D04B2454BF6F4FADF034B10403119CD8E3B92FCC5B"
	b, _ := new(big.Int).SetString(hex, 16)
	out := make([]byte, 256)
	copy(out[256-len(b.Bytes()):], b.Bytes())
	return out
}

func openStore(tb testing.TB, dsn string) *store.Store {
	tb.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, dsn, pgtest.EncKey())
	if err != nil {
		tb.Fatalf("store.Open: %v", err)
	}
	tb.Cleanup(func() { _ = s.Close() }) //nolint:errcheck // best-effort close
	return s
}
