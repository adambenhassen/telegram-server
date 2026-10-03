package e2e_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	osexec "os/exec"
	"reflect"
	"strings"
	"testing"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func testSmokeUsernamePasswordReset(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const username = "smokereset1"
	const oldPassword = "smoke-old-password"
	const newPassword = "smoke-new-password"
	userID := seedUsernameUser(t, f.ctx, f.store, username, "Smoke", oldPassword)
	initialPassword, ok, err := f.store.PasswordByUser(f.ctx, userID)
	if err != nil || !ok {
		t.Fatalf("load initial password: ok=%v err=%v", ok, err)
	}
	if err := f.store.UpsertPassword(f.ctx, store.UserPassword{
		UserID: userID, Salt1: initialPassword.Salt1, Salt2: initialPassword.Salt2,
		Verifier: initialPassword.Verifier, Hint: "retired hint",
		RecoveryEmail: "qa@example.invalid", HasRecovery: true,
	}); err != nil {
		t.Fatalf("seed password metadata: %v", err)
	}

	contact, err := f.store.CreateUser(f.ctx, "+15551049991")
	if err != nil {
		t.Fatalf("create contact account: %v", err)
	}
	if _, err := f.store.AddContact(f.ctx, userID, contact.ID); err != nil {
		t.Fatalf("seed contact: %v", err)
	}

	boundSession := &session.StorageMemory{}
	boundClient := newUsernameClient(f.port, f.key, f.dcID, boundSession)
	var boundAuthKeyID int64
	const savedMessage = "password-reset-smoke-message"
	if err := boundClient.Run(f.ctx, func(ctx context.Context) error {
		api := boundClient.API()
		if err := loginUsernamePassword(ctx, f, api, username, oldPassword); err != nil {
			return fmt.Errorf("initial sign-in: %w", err)
		}
		result, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerSelf{}, Message: savedMessage, RandomID: 1101001,
		})
		if err != nil {
			return fmt.Errorf("send saved message: %w", err)
		}
		if _, ok := result.(*tg.Updates); !ok {
			return fmt.Errorf("send saved message response = %T, want *tg.Updates", result)
		}
		boundAuthKeyID, err = passwordResetAuthKeyID(ctx, boundSession)
		return err
	}); err != nil {
		t.Fatalf("prepare bound session: %v", err)
	}
	stateBefore, err := f.store.State(f.ctx, userID)
	if err != nil {
		t.Fatalf("read update state before reset: %v", err)
	}
	contactsBefore, _, err := f.store.Contacts(f.ctx, userID)
	if err != nil {
		t.Fatalf("read contacts before reset: %v", err)
	}

	staleOld, err := startPasswordResetPendingSession(t, f, username)
	if err != nil {
		t.Fatalf("prepare stale-old challenge: %v", err)
	}
	staleNew, err := startPasswordResetPendingSession(t, f, username)
	if err != nil {
		t.Fatalf("prepare stale-new challenge: %v", err)
	}
	for _, pending := range []*passwordResetPendingSession{staleOld, staleNew} {
		key, ok, err := f.store.AuthKeyByID(f.ctx, pending.authKeyID)
		if err != nil || !ok || key.UserID != 0 || key.PendingUserID != userID {
			t.Fatalf("pending auth key %d before reset = %+v ok=%v err=%v", pending.authKeyID, key, ok, err)
		}
	}
	boundKey, ok, err := f.store.AuthKeyByID(f.ctx, boundAuthKeyID)
	if err != nil || !ok || boundKey.UserID != userID || boundKey.PendingUserID != 0 {
		t.Fatalf("bound auth key before reset = %+v ok=%v err=%v", boundKey, ok, err)
	}

	stdout, stderr, err := runPasswordResetCLI(f.ctx, f.dsn, username, newPassword)
	if err != nil {
		t.Fatalf("password reset CLI: %v; stdout=%q stderr=%q", err, stdout, stderr)
	}
	if stdout != "" {
		t.Fatalf("password reset stdout = %q, want empty", stdout)
	}
	wantConfirmation := fmt.Sprintf("Password reset: %s (user id: %d)\n", username, userID)
	if stderr != wantConfirmation {
		t.Fatalf("password reset stderr = %q, want %q", stderr, wantConfirmation)
	}
	if strings.Contains(stdout+stderr, newPassword) {
		t.Fatal("password reset output contains the password")
	}

	if err := staleOld.check(oldPassword, false); !isRPCMessage(err, "PASSWORD_HASH_INVALID") {
		t.Fatalf("old password on pre-reset challenge = %v, want PASSWORD_HASH_INVALID", err)
	}
	if err := staleNew.check(newPassword, false); !isRPCMessage(err, "PASSWORD_HASH_INVALID") {
		t.Fatalf("new password on pre-reset challenge = %v, want PASSWORD_HASH_INVALID", err)
	}
	for _, pending := range []*passwordResetPendingSession{staleOld, staleNew} {
		key, ok, err := f.store.AuthKeyByID(f.ctx, pending.authKeyID)
		if err != nil || !ok || key.UserID != 0 || key.PendingUserID != userID {
			t.Fatalf("pending auth key %d after stale proof = %+v ok=%v err=%v", pending.authKeyID, key, ok, err)
		}
	}

	if err := staleNew.check(oldPassword, true); !isRPCMessage(err, "PASSWORD_HASH_INVALID") {
		t.Fatalf("old password on fresh post-reset challenge = %v, want PASSWORD_HASH_INVALID", err)
	}
	if err := staleOld.check(newPassword, true); err != nil {
		t.Fatalf("new password on fresh post-reset challenge: %v", err)
	}
	newBoundKey, ok, err := f.store.AuthKeyByID(f.ctx, staleOld.authKeyID)
	if err != nil || !ok || newBoundKey.UserID != userID || newBoundKey.PendingUserID != 0 {
		t.Fatalf("newly authenticated key = %+v ok=%v err=%v", newBoundKey, ok, err)
	}
	stillPending, ok, err := f.store.AuthKeyByID(f.ctx, staleNew.authKeyID)
	if err != nil || !ok || stillPending.UserID != 0 || stillPending.PendingUserID != userID {
		t.Fatalf("uncompleted pending key = %+v ok=%v err=%v", stillPending, ok, err)
	}

	stored, ok, err := f.store.PasswordByUser(f.ctx, userID)
	if err != nil || !ok {
		t.Fatalf("read reset password: ok=%v err=%v", ok, err)
	}
	if stored.Hint != "" || stored.RecoveryEmail != "qa@example.invalid" || !stored.HasRecovery {
		t.Fatalf("password metadata after reset = {hint:%q recovery_email:%q has_recovery:%t}", stored.Hint, stored.RecoveryEmail, stored.HasRecovery)
	}

	stateAfter, err := f.store.State(f.ctx, userID)
	if err != nil || stateAfter != stateBefore {
		t.Fatalf("update state after reset = %+v, want %+v (err=%v)", stateAfter, stateBefore, err)
	}
	contactsAfter, _, err := f.store.Contacts(f.ctx, userID)
	if err != nil || !reflect.DeepEqual(contactsAfter, contactsBefore) {
		t.Fatalf("contacts after reset = %v, want %v (err=%v)", contactsAfter, contactsBefore, err)
	}

	reopenedBoundClient := newUsernameClient(f.port, f.key, f.dcID, boundSession)
	if err := reopenedBoundClient.Run(f.ctx, func(ctx context.Context) error {
		api := reopenedBoundClient.API()
		passwordState, err := api.AccountGetPassword(ctx)
		if err != nil {
			return fmt.Errorf("authenticated account.getPassword after reset: %w", err)
		}
		if !passwordState.HasPassword || passwordState.Hint != "" || !passwordState.HasRecovery {
			return fmt.Errorf("authenticated account.getPassword returned unexpected state: %+v", passwordState)
		}
		history, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: &tg.InputPeerSelf{}, Limit: 10})
		if err != nil {
			return fmt.Errorf("read existing message after reset: %w", err)
		}
		messages, ok := history.(*tg.MessagesMessages)
		if !ok {
			return fmt.Errorf("history response = %T, want *tg.MessagesMessages", history)
		}
		for _, class := range messages.Messages {
			if message, ok := class.(*tg.Message); ok && message.Message == savedMessage {
				return nil
			}
		}
		return fmt.Errorf("saved message %q missing after reset", savedMessage)
	}); err != nil {
		t.Fatalf("existing bound session after reset: %v", err)
	}
}

func loginUsernamePassword(ctx context.Context, f *smokeFixture, api *tg.Client, username, password string) error {
	codeHash, err := sendCodeUsername(ctx, api, username)
	if err != nil {
		return fmt.Errorf("sendCode: %w", err)
	}
	code, err := f.codes.wait(ctx, username)
	if err != nil {
		return fmt.Errorf("wait for login code: %w", err)
	}
	response, err := signInUsername(ctx, api, username, codeHash, code)
	if !isSessionPasswordNeeded(err) {
		if err != nil {
			return fmt.Errorf("signIn: %w", err)
		}
		return fmt.Errorf("signIn response = %T, want password challenge", response)
	}
	return checkPasswordUsername(ctx, api, password)
}

type passwordResetPendingRequest struct {
	password string
	refresh  bool
	result   chan error
}

type passwordResetPendingSession struct {
	authKeyID int64
	requests  chan passwordResetPendingRequest
	done      chan error
	ready     chan error
}

func startPasswordResetPendingSession(t *testing.T, f *smokeFixture, username string) (*passwordResetPendingSession, error) {
	t.Helper()
	sess := &session.StorageMemory{}
	client := newUsernameClient(f.port, f.key, f.dcID, sess)
	pending := &passwordResetPendingSession{
		requests: make(chan passwordResetPendingRequest),
		done:     make(chan error, 1),
		ready:    make(chan error, 1),
	}
	go func() {
		pending.done <- client.Run(f.ctx, func(ctx context.Context) error {
			api := client.API()
			codeHash, err := sendCodeUsername(ctx, api, username)
			if err != nil {
				pending.ready <- fmt.Errorf("sendCode: %w", err)
				return err
			}
			code, err := f.codes.wait(ctx, username)
			if err != nil {
				pending.ready <- fmt.Errorf("wait for login code: %w", err)
				return err
			}
			response, err := signInUsername(ctx, api, username, codeHash, code)
			if !isSessionPasswordNeeded(err) {
				if err == nil {
					err = fmt.Errorf("signIn response = %T, want password challenge", response)
				}
				pending.ready <- err
				return err
			}
			initialChallenge, err := api.AccountGetPassword(ctx)
			if err != nil {
				pending.ready <- fmt.Errorf("get initial SRP challenge: %w", err)
				return err
			}
			pending.authKeyID, err = passwordResetAuthKeyID(ctx, sess)
			if err != nil {
				pending.ready <- err
				return err
			}
			pending.ready <- nil
			for request := range pending.requests {
				challenge := initialChallenge
				var err error
				if request.refresh {
					challenge, err = api.AccountGetPassword(ctx)
				}
				if err == nil {
					var proof *tg.InputCheckPasswordSRP
					proof, err = auth.PasswordHash([]byte(request.password), challenge.SRPID, challenge.SRPB, challenge.SecureRandom, challenge.CurrentAlgo)
					if err == nil {
						response, checkErr := api.AuthCheckPassword(ctx, proof)
						err = checkErr
						if err == nil {
							if _, ok := response.(*tg.AuthAuthorization); !ok {
								err = fmt.Errorf("checkPassword response = %T, want *tg.AuthAuthorization", response)
							}
						}
					}
				}
				request.result <- err
			}
			return nil
		})
	}()
	select {
	case err := <-pending.ready:
		if err != nil {
			return nil, err
		}
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
	t.Cleanup(func() {
		close(pending.requests)
		select {
		case err := <-pending.done:
			if err != nil {
				t.Errorf("pending username client: %v", err)
			}
		case <-f.ctx.Done():
			t.Errorf("pending username client did not stop before fixture context ended")
		}
	})
	return pending, nil
}

func (p *passwordResetPendingSession) check(password string, refresh bool) error {
	result := make(chan error, 1)
	p.requests <- passwordResetPendingRequest{password: password, refresh: refresh, result: result}
	return <-result
}

func passwordResetAuthKeyID(ctx context.Context, sess *session.StorageMemory) (int64, error) {
	data, err := (&session.Loader{Storage: sess}).Load(ctx)
	if err != nil {
		return 0, fmt.Errorf("load session: %w", err)
	}
	if len(data.AuthKeyID) != 8 {
		return 0, fmt.Errorf("auth key id length = %d, want 8", len(data.AuthKeyID))
	}
	var authKeyID [8]byte
	copy(authKeyID[:], data.AuthKeyID)
	return mtproto.AuthKeyIDInt64(authKeyID), nil
}

func runPasswordResetCLI(ctx context.Context, dsn, username, password string) (string, string, error) {
	cmd := osexec.CommandContext(ctx, "go", "run", "../../cmd/telegramd", "admin", "set-password", "--username", username) // #nosec G204 -- fixed Go test invocation against local fixture; username is test data.
	cmd.Env = passwordResetCommandEnvironment(dsn)
	cmd.Stdin = strings.NewReader(password + "\n")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stdout.String(), stderr.String(), err
}

func passwordResetCommandEnvironment(dsn string) []string {
	settings := map[string]string{
		"TG_POSTGRES_DSN":         dsn,
		"TG_AUTHKEY_ENC_KEY":      hex.EncodeToString(pgtest.EncKey()),
		"TG_AUTHKEY_ENC_KEY_FILE": "",
		"TG_REGISTRATION":         "open",
	}
	env := make([]string, 0, len(os.Environ())+len(settings))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, replace := settings[name]; !replace {
			env = append(env, entry)
		}
	}
	for name, value := range settings {
		env = append(env, name+"="+value)
	}
	return env
}
