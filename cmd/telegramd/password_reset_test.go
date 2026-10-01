package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	gotdsrp "github.com/gotd/td/crypto/srp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	tsrp "github.com/adambenhassen/telegram-server/internal/srp"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestRunCommandAdminSetPasswordRejectsPasswordFlagsBeforeOpeningStore(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "not a postgres connection string")
	t.Setenv("TG_AUTHKEY_ENC_KEY", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")

	for _, args := range [][]string{
		{"admin", "set-password", "--username", "tester1", "--password", "stdin-secret-marker"},
		{"admin", "set-password", "--username", "tester1", "--password=stdin-secret-marker"},
	} {
		var stdout, stderr bytes.Buffer
		err := runCommand(args, slog.New(slog.DiscardHandler), &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "usage:") {
			t.Fatalf("runCommand(%v) error = %v, want usage error", args, err)
		}
		if strings.Contains(err.Error()+stdout.String()+stderr.String(), "stdin-secret-marker") {
			t.Fatalf("password flag value appeared in diagnostics: %q", err)
		}
	}
}

func TestRunCommandAdminSetPasswordResetsExistingUsernamePassword(t *testing.T) {
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	key := pgtest.EncKey()
	t.Setenv("TG_POSTGRES_DSN", dsn)
	t.Setenv("TG_AUTHKEY_ENC_KEY", hex.EncodeToString(key))
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	t.Setenv("TG_REGISTRATION", "open")

	st, err := store.Open(ctx, dsn, key, store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open fixture store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close fixture store: %v", err)
		}
	})

	user, err := st.CreateUsernameUser(ctx, "tester1", "Tester", "")
	if err != nil {
		t.Fatalf("create username account: %v", err)
	}
	if err := st.ClaimUsername(ctx, user.ID, "tester1"); err != nil {
		t.Fatalf("claim username: %v", err)
	}
	oldVerifier, oldSalt1, oldSalt2 := passwordResetTestVerifier(t, "old-password")
	if err := st.UpsertPassword(ctx, store.UserPassword{
		UserID: user.ID, Salt1: oldSalt1, Salt2: oldSalt2, Verifier: oldVerifier,
		Hint: "old hint", RecoveryEmail: "qa@example.invalid", HasRecovery: true,
	}); err != nil {
		t.Fatalf("seed password: %v", err)
	}

	previousVerifier, previousSalt1, previousSalt2 := oldVerifier, oldSalt1, oldSalt2
	for _, tc := range []struct {
		handle   string
		password string
		input    string
	}{
		{handle: "Tester1", password: "no-newline-secret", input: "no-newline-secret"}, //nolint:gosec // G101: synthetic test password.
		{handle: "tester1", password: "lf-secret-marker", input: "lf-secret-marker\n"},
		{handle: "@Tester1", password: "crlf-secret-marker", input: "crlf-secret-marker\r\n"},
	} {
		var stdout, stderr bytes.Buffer
		err = runCommandWithStdin(t, []string{"admin", "set-password", "--username", tc.handle}, tc.input, &stdout, &stderr)
		if err != nil {
			t.Fatalf("set-password for %q: %v", tc.handle, err)
		}
		if stdout.Len() != 0 {
			t.Fatalf("stdout = %q, want empty", stdout.String())
		}
		wantConfirmation := fmt.Sprintf("Password reset: tester1 (user id: %d)\n", user.ID)
		if stderr.String() != wantConfirmation {
			t.Fatalf("stderr = %q, want %q", stderr.String(), wantConfirmation)
		}
		if strings.Contains(stdout.String()+stderr.String(), tc.password) {
			t.Fatalf("successful output for %q contains the password", tc.handle)
		}

		got, ok, err := st.PasswordByUser(ctx, user.ID)
		if err != nil || !ok {
			t.Fatalf("read reset password for %q: ok=%v err=%v", tc.handle, ok, err)
		}
		if bytes.Equal(got.Verifier, previousVerifier) || bytes.Equal(got.Salt1, previousSalt1) || bytes.Equal(got.Salt2, previousSalt2) {
			t.Fatalf("reset for %q reused an old verifier or salt", tc.handle)
		}
		if got.Hint != "" || got.RecoveryEmail != "qa@example.invalid" || !got.HasRecovery {
			t.Fatalf("reset password metadata = {hint:%q email:%q recovery:%t}", got.Hint, got.RecoveryEmail, got.HasRecovery)
		}
		previousVerifier, previousSalt1, previousSalt2 = got.Verifier, got.Salt1, got.Salt2
	}
}

func TestRunCommandAdminSetPasswordKeyFileLogsOnlyConfirmation(t *testing.T) {
	ctx := context.Background()
	dsn, key, st := openPasswordResetTestStore(t)
	user := seedPasswordResetUsername(t, ctx, st, "tester1", "old-password", "old hint", nil, false)
	keyPath := filepath.Join(t.TempDir(), "auth.key")
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(key)), 0o600); err != nil {
		t.Fatalf("write test master key: %v", err)
	}
	t.Setenv("TG_POSTGRES_DSN", dsn)
	t.Setenv("TG_AUTHKEY_ENC_KEY", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", keyPath)

	var stdout, stderr bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&stderr, nil))
	err := runCommandWithStdinAndLogger(t, []string{"admin", "set-password", "--username", "tester1"}, "replacement-secret-marker\n", logger, &stdout, &stderr)
	if err != nil {
		t.Fatalf("set-password using key file: %v", err)
	}
	want := fmt.Sprintf("Password reset: tester1 (user id: %d)\n", user.ID)
	if stdout.Len() != 0 || stderr.String() != want {
		t.Fatalf("stdout/stderr = %q/%q, want empty stdout and %q", stdout.String(), stderr.String(), want)
	}
	if strings.Contains(stdout.String()+stderr.String(), "replacement-secret-marker") {
		t.Fatal("success output contained the password")
	}
}

func TestRunCommandAdminSetPasswordRejectsInvalidUsernameBeforeDatabase(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "not a postgres connection string")
	t.Setenv("TG_AUTHKEY_ENC_KEY", "")
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")

	for _, handle := range []string{"", "1tester", "tést", "tester-name", "@@Tester1"} {
		var stdout, stderr bytes.Buffer
		err := runCommandWithStdin(t, []string{"admin", "set-password", "--username", handle}, "stdin-secret-marker\n", &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "username") {
			t.Fatalf("handle %q error = %v, want username validation error", handle, err)
		}
		if strings.Contains(err.Error()+stdout.String()+stderr.String(), "stdin-secret-marker") {
			t.Fatalf("password appeared in diagnostics for handle %q", handle)
		}
	}
}

func TestRunCommandAdminSetPasswordRejectsUnsupportedAccountsWithoutWrites(t *testing.T) {
	ctx := context.Background()
	dsn, key, st := openPasswordResetTestStore(t)
	tester := seedPasswordResetUsername(t, ctx, st, "tester1", "old-password", "old hint", nil, false)

	phone, err := st.CreateUser(ctx, "+15551250100")
	if err != nil {
		t.Fatalf("create phone-mode account: %v", err)
	}
	if err := st.ClaimUsername(ctx, phone.ID, "phoneaccount"); err != nil {
		t.Fatalf("claim phone-mode username: %v", err)
	}
	phoneVerifier, phoneSalt1, phoneSalt2 := passwordResetTestVerifier(t, "phone-password")
	if err := st.UpsertPassword(ctx, store.UserPassword{UserID: phone.ID, Salt1: phoneSalt1, Salt2: phoneSalt2, Verifier: phoneVerifier}); err != nil {
		t.Fatalf("seed phone-mode password: %v", err)
	}
	provisional, err := st.CreateUsernameUser(ctx, "provisional", "Provisional", "")
	if err != nil {
		t.Fatalf("create provisional account: %v", err)
	}
	if err := st.ClaimUsername(ctx, provisional.ID, "provisional"); err != nil {
		t.Fatalf("claim provisional username: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture database: %v", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO usernames (handle, owner_type, owner_id) VALUES ('channelowner', 'channel', 9223372036854775807)`); err != nil {
		if closeErr := conn.Close(ctx); closeErr != nil {
			t.Errorf("close fixture connection: %v", closeErr)
		}
		t.Fatalf("seed channel-owned handle: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close fixture connection: %v", err)
	}

	before := passwordResetSnapshot(t, ctx, dsn)
	for _, tc := range []struct {
		handle string
		want   string
	}{
		{handle: "missinguser", want: "username account not found"},
		{handle: "channelowner", want: "channel-owned handle"},
		{handle: "phoneaccount", want: "phone-mode account"},
		{handle: "provisional", want: "no password row"},
	} {
		var stdout, stderr bytes.Buffer
		err := runCommandWithStdin(t, []string{"admin", "set-password", "--username", tc.handle}, "replacement-secret-marker\n", &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("handle %q error = %v, want message containing %q", tc.handle, err, tc.want)
		}
		if strings.Contains(errText(err, stdout.String(), stderr.String()), "replacement-secret-marker") {
			t.Errorf("password appeared in diagnostics for %q", tc.handle)
		}
		if after := passwordResetSnapshot(t, ctx, dsn); !reflect.DeepEqual(after, before) {
			t.Errorf("rejected reset for %q changed password rows", tc.handle)
		}
	}
	if tester.ID == 0 || len(before) != 2 {
		t.Fatalf("test fixtures were not persisted as expected: tester=%+v rows=%d", tester, len(before))
	}
	_ = key
}

func TestRunCommandAdminSetPasswordInputAndDatabaseFailuresLeaveRowsUntouched(t *testing.T) {
	ctx := context.Background()
	dsn, _, st := openPasswordResetTestStore(t)
	t.Setenv("TG_PASSWORD", "environment-secret-marker")
	user := seedPasswordResetUsername(t, ctx, st, "tester1", "old-password", "old hint", nil, false)
	before := passwordResetSnapshot(t, ctx, dsn)

	for _, input := range []string{"", "\n", "\r\n", "one\ntwo\n"} {
		var stdout, stderr bytes.Buffer
		err := runCommandWithStdin(t, []string{"admin", "set-password", "--username", "tester1"}, input, &stdout, &stderr)
		if err == nil || !strings.Contains(err.Error(), "password input") {
			t.Errorf("input %q error = %v, want password input error", input, err)
		}
		if strings.Contains(errText(err, stdout.String(), stderr.String()), input) && input != "" {
			t.Errorf("input appeared in diagnostics for input %q", input)
		}
		if strings.Contains(errText(err, stdout.String(), stderr.String()), "environment-secret-marker") {
			t.Errorf("environment password appeared in diagnostics for input %q", input)
		}
		if after := passwordResetSnapshot(t, ctx, dsn); !reflect.DeepEqual(after, before) {
			t.Errorf("invalid input %q changed password rows", input)
		}
	}
	var stdout, stderr bytes.Buffer
	err := runCommandWithUnreadableStdin(t, []string{"admin", "set-password", "--username", "tester1"}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "password input") {
		t.Errorf("unreadable stdin error = %v, want password input error", err)
	}
	if after := passwordResetSnapshot(t, ctx, dsn); !reflect.DeepEqual(after, before) {
		t.Fatal("unreadable stdin changed password rows")
	}

	t.Setenv("TG_POSTGRES_DSN", "postgres://telegram:telegram@127.0.0.1:1/telegram?sslmode=disable")
	stdout.Reset()
	stderr.Reset()
	err = runCommandWithStdin(t, []string{"admin", "set-password", "--username", "tester1"}, "replacement-secret-marker\n", &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "open store") {
		t.Fatalf("database failure = %v, want store open error", err)
	}
	if strings.Contains(errText(err, stdout.String(), stderr.String()), "replacement-secret-marker") {
		t.Fatal("password appeared in database failure diagnostics")
	}
	if strings.Contains(errText(err, stdout.String(), stderr.String()), "environment-secret-marker") {
		t.Fatal("environment password appeared in database failure diagnostics")
	}
	t.Setenv("TG_POSTGRES_DSN", dsn)
	if after := passwordResetSnapshot(t, ctx, dsn); !reflect.DeepEqual(after, before) {
		t.Fatal("database failure changed password rows")
	}
	if user.ID == 0 {
		t.Fatal("test account was not persisted")
	}
}

func TestRunCommandAdminSetPasswordRefusesInteractiveInput(t *testing.T) {
	t.Setenv("TG_POSTGRES_DSN", "not a postgres connection string")
	interactive, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatalf("open null device: %v", err)
	}
	previous := os.Stdin
	os.Stdin = interactive
	var stdout, stderr bytes.Buffer
	err = runCommand([]string{"admin", "set-password", "--username", "tester1"}, slog.New(slog.DiscardHandler), &stdout, &stderr)
	os.Stdin = previous
	if closeErr := interactive.Close(); closeErr != nil {
		t.Errorf("close null device: %v", closeErr)
	}
	if err == nil || !strings.Contains(err.Error(), "interactive") {
		t.Fatalf("interactive input error = %v, want interactive-input refusal", err)
	}
}

func TestRunCommandAdminSetPasswordDatabaseWriteFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	dsn, _, st := openPasswordResetTestStore(t)
	seedPasswordResetUsername(t, ctx, st, "tester1", "old-password", "old hint", nil, false)
	before := passwordResetSnapshot(t, ctx, dsn)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect trigger fixture: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close trigger fixture: %v", err)
		}
	})
	if _, err := conn.Exec(ctx, `CREATE FUNCTION reject_password_reset_test() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected password reset write failure'; END $$`); err != nil {
		t.Fatalf("create password reset failure trigger function: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TRIGGER reject_password_reset_test BEFORE UPDATE ON user_passwords FOR EACH ROW EXECUTE FUNCTION reject_password_reset_test()`); err != nil {
		t.Fatalf("create password reset failure trigger: %v", err)
	}

	var stdout, stderr bytes.Buffer
	err = runCommandWithStdin(t, []string{"admin", "set-password", "--username", "tester1"}, "replacement-secret-marker\n", &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "write replacement password verifier") {
		t.Fatalf("database write failure = %v, want password verifier write error", err)
	}
	if strings.Contains(errText(err, stdout.String(), stderr.String()), "replacement-secret-marker") {
		t.Fatal("password appeared in database write failure diagnostics")
	}
	if after := passwordResetSnapshot(t, ctx, dsn); !reflect.DeepEqual(after, before) {
		t.Fatalf("database write failure changed password rows: before=%+v after=%+v", before, after)
	}
}

func TestRunCommandAdminSetPasswordVerifierGenerationFailureLeavesRowsUntouched(t *testing.T) {
	ctx := context.Background()
	dsn, _, st := openPasswordResetTestStore(t)
	seedPasswordResetUsername(t, ctx, st, "tester1", "old-password", "old hint", nil, false)
	before := passwordResetSnapshot(t, ctx, dsn)

	previousReader := rand.Reader
	rand.Reader = passwordResetFailReader{}
	t.Cleanup(func() { rand.Reader = previousReader })
	var stdout, stderr bytes.Buffer
	err := runCommandWithStdin(t, []string{"admin", "set-password", "--username", "tester1"}, "replacement-secret-marker\n", &stdout, &stderr)
	rand.Reader = previousReader
	if err == nil || !strings.Contains(err.Error(), "generate") {
		t.Fatalf("verifier generation failure = %v, want generation error", err)
	}
	if strings.Contains(errText(err, stdout.String(), stderr.String()), "replacement-secret-marker") {
		t.Fatal("password appeared in verifier generation diagnostics")
	}
	if after := passwordResetSnapshot(t, ctx, dsn); !reflect.DeepEqual(after, before) {
		t.Fatal("verifier generation failure changed password rows")
	}
}

func TestRunCommandAdminSetPasswordWrongKeyLeavesVerifiableRowUntouched(t *testing.T) {
	ctx := context.Background()
	dsn, key, st := openPasswordResetTestStore(t)
	user := seedPasswordResetUsername(t, ctx, st, "tester1", "old-password", "old hint", nil, false)
	before := passwordResetSnapshot(t, ctx, dsn)
	wrongKey := bytes.Repeat([]byte{0xa5}, len(key))
	t.Setenv("TG_AUTHKEY_ENC_KEY", hex.EncodeToString(wrongKey))

	var stdout, stderr bytes.Buffer
	err := runCommandWithStdin(t, []string{"admin", "set-password", "--username", "tester1"}, "replacement-secret-marker\n", &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "decrypt") {
		t.Fatalf("wrong-key reset error = %v, want decrypt error", err)
	}
	if strings.Contains(errText(err, stdout.String(), stderr.String()), "replacement-secret-marker") {
		t.Fatal("password appeared in wrong-key diagnostics")
	}
	t.Setenv("TG_AUTHKEY_ENC_KEY", hex.EncodeToString(key))
	if after := passwordResetSnapshot(t, ctx, dsn); !reflect.DeepEqual(after, before) {
		t.Fatal("wrong-key reset changed password rows")
	}
	if got, ok, err := st.PasswordByUser(ctx, user.ID); err != nil || !ok || string(got.Verifier) == "" {
		t.Fatalf("original key could not decrypt untouched password row: ok=%v err=%v", ok, err)
	}
}

func TestRunCommandAdminSetPasswordPreservesNullableRecoveryStorage(t *testing.T) {
	ctx := context.Background()
	dsn, key, st := openPasswordResetTestStore(t)
	user := seedPasswordResetUsername(t, ctx, st, "nullableuser", "old-password", "old hint", nil, true)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture database: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE user_passwords SET recovery_email = NULL WHERE user_id = $1`, user.ID); err != nil {
		if closeErr := conn.Close(ctx); closeErr != nil {
			t.Errorf("close fixture connection: %v", closeErr)
		}
		t.Fatalf("seed nullable recovery email: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close fixture connection: %v", err)
	}
	before := passwordResetSnapshot(t, ctx, dsn)
	var stdout, stderr bytes.Buffer
	err = runCommandWithStdin(t, []string{"admin", "set-password", "--username", "nullableuser"}, "replacement-password\n", &stdout, &stderr)
	if err != nil {
		t.Fatalf("set-password with nullable recovery email: %v", err)
	}
	after := passwordResetSnapshot(t, ctx, dsn)
	if len(before) != 1 || len(after) != 1 || before[0].RecoveryEmail.Valid || after[0].RecoveryEmail.Valid {
		t.Fatalf("nullable recovery email changed: before=%+v after=%+v", before, after)
	}
	if before[0].HasRecovery != after[0].HasRecovery {
		t.Fatalf("recovery flag changed: before=%t after=%t", before[0].HasRecovery, after[0].HasRecovery)
	}
	if got, ok, err := st.PasswordByUser(ctx, user.ID); err != nil || !ok || got.RecoveryEmail != "" || !got.HasRecovery || got.Hint != "" {
		t.Fatalf("password metadata after reset = %+v ok=%v err=%v", got, ok, err)
	}
	_ = key
}

func TestRunAdminCommandSerializesConcurrentAccountPasswordChange(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	dsn, _, st := openPasswordResetTestStore(t)
	target := seedPasswordResetUsername(t, ctx, st, "tester1", "old-password", "old hint", nil, false)
	source := seedPasswordResetUsername(t, ctx, st, "passwordsource", "account-password", "", nil, false)
	sourcePassword, ok, err := st.PasswordByUser(ctx, source.ID)
	if err != nil || !ok {
		t.Fatalf("read account-change fixture: ok=%v err=%v", ok, err)
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect fixture database: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close fixture database connection: %v", err)
		}
	})
	const lockClass, lockObject = 52110, 1101
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1, $2)`, lockClass, lockObject); err != nil {
		t.Fatalf("hold password reset test lock: %v", err)
	}
	lockHeld := true
	unlock := func() error {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer unlockCancel()
		if _, err := conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1, $2)`, lockClass, lockObject); err != nil {
			return err
		}
		lockHeld = false
		return nil
	}
	defer func() {
		if lockHeld {
			if err := unlock(); err != nil {
				t.Errorf("release password reset test lock: %v", err)
			}
		}
	}()
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION password_reset_test_pause() RETURNS trigger
		LANGUAGE plpgsql AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock(%d, %d);
			RETURN NEW;
		END;
		$$`, lockClass, lockObject)); err != nil {
		t.Fatalf("create reset pause trigger function: %v", err)
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf(`
		CREATE TRIGGER password_reset_test_pause
		BEFORE UPDATE OF salt1, salt2, verifier ON user_passwords
		FOR EACH ROW WHEN (OLD.user_id = %d)
		EXECUTE FUNCTION password_reset_test_pause()`, target.ID)); err != nil {
		t.Fatalf("create reset pause trigger: %v", err)
	}
	t.Cleanup(func() {
		if _, err := conn.Exec(context.Background(), `DROP TRIGGER IF EXISTS password_reset_test_pause ON user_passwords`); err != nil {
			t.Errorf("drop reset pause trigger: %v", err)
		}
		if _, err := conn.Exec(context.Background(), `DROP FUNCTION IF EXISTS password_reset_test_pause()`); err != nil {
			t.Errorf("drop reset pause trigger function: %v", err)
		}
	})

	var stderr bytes.Buffer
	resetDone := make(chan error, 1)
	go func() {
		resetDone <- runAdminCommand([]string{"set-password", "--username", "tester1"}, strings.NewReader("reset-wins-first\n"), slog.New(slog.DiscardHandler), &stderr)
	}()
	if err := waitForPasswordResetActivity(ctx, conn, "%UPDATE user_passwords%", "Lock"); err != nil {
		if unlockErr := unlock(); unlockErr != nil {
			t.Errorf("release test lock after reset wait failure: %v", unlockErr)
		}
		if resetErr := <-resetDone; resetErr != nil {
			t.Errorf("reset command after wait failure: %v", resetErr)
		}
		t.Fatalf("reset did not reach its locked update: %v", err)
	}

	accountChange := store.UserPassword{
		UserID: target.ID, Salt1: sourcePassword.Salt1, Salt2: sourcePassword.Salt2,
		Verifier: sourcePassword.Verifier, Hint: "account-change-committed-last",
	}
	changeDone := make(chan error, 1)
	go func() { changeDone <- st.UpsertPassword(ctx, accountChange) }()
	if err := waitForPasswordResetActivity(ctx, conn, "%INSERT INTO user_passwords%", "Lock"); err != nil {
		if unlockErr := unlock(); unlockErr != nil {
			t.Errorf("release test lock after account update wait failure: %v", unlockErr)
		}
		if resetErr := <-resetDone; resetErr != nil {
			t.Errorf("reset command after account update wait failure: %v", resetErr)
		}
		if changeErr := <-changeDone; changeErr != nil {
			t.Errorf("account password change after wait failure: %v", changeErr)
		}
		t.Fatalf("account password change did not wait for reset: %v", err)
	}
	if err := unlock(); err != nil {
		t.Fatalf("release reset test lock: %v", err)
	}
	if err := <-resetDone; err != nil {
		t.Fatalf("reset command: %v", err)
	}
	if err := <-changeDone; err != nil {
		t.Fatalf("concurrent account password change: %v", err)
	}

	got, ok, err := st.PasswordByUser(ctx, target.ID)
	if err != nil || !ok {
		t.Fatalf("read final password: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got.Salt1, accountChange.Salt1) || !bytes.Equal(got.Salt2, accountChange.Salt2) || !bytes.Equal(got.Verifier, accountChange.Verifier) || got.Hint != accountChange.Hint {
		t.Fatalf("final password = %+v, want last committed account update", got)
	}
}

func waitForPasswordResetActivity(ctx context.Context, conn *pgx.Conn, queryPattern, waitType string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var waiting bool
		if err := conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND query LIKE $1
				  AND wait_event_type = $2
			)`, queryPattern, waitType).Scan(&waiting); err != nil {
			return fmt.Errorf("inspect PostgreSQL activity: %w", err)
		}
		if waiting {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func runCommandWithStdin(t *testing.T, args []string, input string, stdout, stderr io.Writer) error {
	t.Helper()
	return runCommandWithStdinAndLogger(t, args, input, slog.New(slog.DiscardHandler), stdout, stderr)
}

func runCommandWithStdinAndLogger(t *testing.T, args []string, input string, logger *slog.Logger, stdout, stderr io.Writer) error {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		return err
	}
	if _, err := io.WriteString(write, input); err != nil {
		return errors.Join(err, read.Close(), write.Close())
	}
	if err := write.Close(); err != nil {
		return errors.Join(err, read.Close())
	}
	previous := os.Stdin
	os.Stdin = read
	defer func() {
		os.Stdin = previous
		if err := read.Close(); err != nil {
			t.Errorf("close test stdin: %v", err)
		}
	}()
	return runCommand(args, logger, stdout, stderr)
}

func runCommandWithUnreadableStdin(t *testing.T, args []string, stdout, stderr io.Writer) error {
	t.Helper()
	read, write, err := os.Pipe()
	if err != nil {
		return err
	}
	if err := read.Close(); err != nil {
		return errors.Join(err, write.Close())
	}
	if err := write.Close(); err != nil {
		return err
	}
	previous := os.Stdin
	os.Stdin = read
	defer func() { os.Stdin = previous }()
	return runCommand(args, slog.New(slog.DiscardHandler), stdout, stderr)
}

type passwordResetFailReader struct{}

func (passwordResetFailReader) Read([]byte) (int, error) {
	return 0, errors.New("password reset test random source failed")
}

type passwordResetRawRow struct {
	UserID        int64
	Salt1         []byte
	Salt2         []byte
	Verifier      []byte
	Hint          string
	RecoveryEmail pgtype.Text
	HasRecovery   bool
	CreatedAt     string
	UpdatedAt     string
}

func openPasswordResetTestStore(t *testing.T) (string, []byte, *store.Store) {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	key := pgtest.EncKey()
	t.Setenv("TG_POSTGRES_DSN", dsn)
	t.Setenv("TG_AUTHKEY_ENC_KEY", hex.EncodeToString(key))
	t.Setenv("TG_AUTHKEY_ENC_KEY_FILE", "")
	t.Setenv("TG_REGISTRATION", "open")
	st, err := store.Open(ctx, dsn, key, store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open fixture store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close fixture store: %v", err)
		}
	})
	return dsn, key, st
}

func seedPasswordResetUsername(t *testing.T, ctx context.Context, st *store.Store, handle, password, hint string, recoveryEmail *string, hasRecovery bool) store.User {
	t.Helper()
	user, err := st.CreateUsernameUser(ctx, handle, "Password", "Reset")
	if err != nil {
		t.Fatalf("create username account %q: %v", handle, err)
	}
	if err := st.ClaimUsername(ctx, user.ID, handle); err != nil {
		t.Fatalf("claim username %q: %v", handle, err)
	}
	verifier, salt1, salt2 := passwordResetTestVerifier(t, password)
	var email string
	if recoveryEmail != nil {
		email = *recoveryEmail
	}
	if err := st.UpsertPassword(ctx, store.UserPassword{
		UserID: user.ID, Salt1: salt1, Salt2: salt2, Verifier: verifier,
		Hint: hint, RecoveryEmail: email, HasRecovery: hasRecovery,
	}); err != nil {
		t.Fatalf("seed password for %q: %v", handle, err)
	}
	return user
}

func passwordResetSnapshot(t *testing.T, ctx context.Context, dsn string) []passwordResetRawRow {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for password snapshot: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close password snapshot connection: %v", err)
		}
	}()
	rows, err := conn.Query(ctx, `
		SELECT user_id, salt1, salt2, verifier, hint, recovery_email, has_recovery,
		       created_at::text, updated_at::text
		FROM user_passwords ORDER BY user_id`)
	if err != nil {
		t.Fatalf("query password snapshot: %v", err)
	}
	defer rows.Close()
	var snapshot []passwordResetRawRow
	for rows.Next() {
		var row passwordResetRawRow
		if err := rows.Scan(&row.UserID, &row.Salt1, &row.Salt2, &row.Verifier, &row.Hint,
			&row.RecoveryEmail, &row.HasRecovery, &row.CreatedAt, &row.UpdatedAt); err != nil {
			t.Fatalf("scan password snapshot: %v", err)
		}
		snapshot = append(snapshot, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read password snapshot: %v", err)
	}
	return snapshot
}

func errText(err error, output ...string) string {
	var b strings.Builder
	if err != nil {
		b.WriteString(err.Error())
	}
	for _, value := range output {
		b.WriteString(value)
	}
	return b.String()
}

func passwordResetTestVerifier(t *testing.T, password string) (verifier, salt1, salt2 []byte) {
	t.Helper()
	salt1 = make([]byte, 32)
	salt2 = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt1); err != nil {
		t.Fatalf("generate salt1: %v", err)
	}
	if _, err := io.ReadFull(rand.Reader, salt2); err != nil {
		t.Fatalf("generate salt2: %v", err)
	}
	verifier, augmentedSalt1, err := gotdsrp.NewSRP(rand.Reader).NewHash([]byte(password), gotdsrp.Input{
		Salt1: salt1,
		Salt2: salt2,
		G:     3,
		P:     tsrp.PBytes(),
	})
	if err != nil {
		t.Fatalf("generate fixture verifier: %v", err)
	}
	return verifier, augmentedSalt1, salt2
}
