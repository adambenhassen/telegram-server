package store_test

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"testing"

	"github.com/gotd/td/crypto/srp"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	tsrp "github.com/adambenhassen/telegram-server/internal/srp"
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

	password := []byte("test-password")
	params := store.BootstrapParams{
		Handle:    "operator",
		FirstName: "Operator",
		LastName:  "",
		Password:  password,
	}

	// First call creates.
	result1, err := s.BootstrapAccount(ctx, params)
	if err != nil {
		t.Fatalf("first BootstrapAccount: %v", err)
	}
	if !result1.Created {
		t.Error("expected Created=true for first call")
	}

	// Second call with same password is idempotent.
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

func TestBootstrapAccount_SquattedWrongPassword(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	// Create bootstrap account with one password.
	params := bootstrapParams(t)
	_, err := s.BootstrapAccount(ctx, params)
	if err != nil {
		t.Fatalf("first BootstrapAccount: %v", err)
	}

	// Try bootstrap with different password → verifier mismatch.
	diffParams := store.BootstrapParams{
		Handle:    params.Handle,
		FirstName: params.FirstName,
		LastName:  params.LastName,
		Password:  []byte("different-password"),
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

// TestBootstrapAccount_SRPRoundTrip verifies a full SRP authentication
// round-trip against a bootstrap-seeded account: tsrp.Challenge (server) →
// srp.Hash (client) → tsrp.Verify (server). This is the same path a real
// username login takes.
func TestBootstrapAccount_SRPRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	password := []byte("test-password")
	params := store.BootstrapParams{
		Handle:    "operator",
		FirstName: "Operator",
		LastName:  "",
		Password:  password,
	}

	// Create the bootstrap account.
	result, err := s.BootstrapAccount(ctx, params)
	if err != nil {
		t.Fatalf("BootstrapAccount: %v", err)
	}
	if !result.Created {
		t.Fatal("expected Created=true")
	}

	// Read the stored password row (PasswordByUser returns the decrypted verifier).
	pw, found, err := s.PasswordByUser(ctx, result.UserID)
	if err != nil || !found {
		t.Fatalf("PasswordByUser: found=%v err=%v", found, err)
	}

	// Server side: generate challenge from stored verifier.
	bPub, bSecret, err := tsrp.Challenge(pw.Verifier)
	if err != nil {
		t.Fatalf("srp challenge: %v", err)
	}

	// Client side: compute SRP answer.
	random := make([]byte, 256)
	if _, err := rand.Read(random); err != nil {
		t.Fatalf("srp random: %v", err)
	}
	srpClient := srp.NewSRP(rand.Reader)
	algo := srp.Input{
		Salt1: pw.Salt1,
		Salt2: pw.Salt2,
		G:     3,
		P:     tsrp.PBytes(),
	}
	answer, err := srpClient.Hash(password, bPub, random, algo)
	if err != nil {
		t.Fatalf("srp hash: %v", err)
	}

	// Server side: verify.
	if !tsrp.Verify(pw.Verifier, pw.Salt1, pw.Salt2, answer.A, answer.M1, bPub, bSecret) {
		t.Fatal("SRP verify failed for correct password")
	}

	// Verify wrong password is rejected.
	wrongAnswer, err := srpClient.Hash([]byte("wrong-password"), bPub, random, algo)
	if err != nil {
		t.Fatalf("srp hash (wrong): %v", err)
	}
	if tsrp.Verify(pw.Verifier, pw.Salt1, pw.Salt2, wrongAnswer.A, wrongAnswer.M1, bPub, bSecret) {
		t.Fatal("SRP verify should have rejected wrong password")
	}
}

// TestBootstrapAccount_ConcurrentCreate fires two goroutines at the same
// handle simultaneously and asserts exactly one create and one idempotent
// no-op, with zero errors — covering the 23505 race recovery path.
func TestBootstrapAccount_ConcurrentCreate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t, pgtest.DSN(t))

	password := []byte("test-password")
	params := store.BootstrapParams{
		Handle:    "operator",
		FirstName: "Operator",
		LastName:  "",
		Password:  password,
	}

	var wg sync.WaitGroup
	barrier := make(chan struct{})

	type result struct {
		created bool
		userID  int64
		err     error
	}
	results := make([]result, 2)

	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-barrier
			res, err := s.BootstrapAccount(ctx, params)
			results[idx] = result{created: res.Created, userID: res.UserID, err: err}
		}(i)
	}

	close(barrier) // release both goroutines simultaneously
	wg.Wait()

	// Exactly one error-free result expected from each goroutine.
	for i, r := range results {
		if r.err != nil {
			t.Errorf("goroutine %d: unexpected error: %v", i, r.err)
		}
	}

	// Exactly one created, one idempotent no-op.
	createdCount := 0
	var userID int64
	for i, r := range results {
		if r.err != nil {
			continue
		}
		if r.created {
			createdCount++
			userID = r.userID
		}
		if r.userID == 0 {
			t.Errorf("goroutine %d: expected non-zero UserID", i)
		}
	}
	if createdCount != 1 {
		t.Errorf("expected exactly 1 Created=true, got %d", createdCount)
	}
	if userID == 0 {
		t.Fatal("no successful result")
	}

	// Both goroutines should have returned the same user ID.
	for i, r := range results {
		if r.err != nil {
			continue
		}
		if r.userID != userID {
			t.Errorf("goroutine %d: userID %d != expected %d", i, r.userID, userID)
		}
	}
}

// bootstrapParams returns a BootstrapParams with a test password.
func bootstrapParams(tb testing.TB) store.BootstrapParams {
	tb.Helper()
	return store.BootstrapParams{
		Handle:    "operator",
		FirstName: "Operator",
		LastName:  "",
		Password:  []byte("test-password"),
	}
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
