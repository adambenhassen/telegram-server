package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestCloudPassword2FA is the Milestone 3 compatibility gate. A real gotd client
// drives the full SRP cloud-password lifecycle against the hand-rolled server
// SRP, which only passes if the server math is byte-exact with the client:
//
//  1. Register/login (no password), then enable 2FA.
//  2. A fresh login is challenged: SESSION_PASSWORD_NEEDED, then the SRP proof
//     authorizes; a wrong password is rejected.
//  3. Change the password; the new one logs in, the old one is rejected.
//  4. Remove the password; a fresh login no longer prompts.
func TestCloudPassword2FA(t *testing.T) {
	t.Parallel()
	// ponytail: 3 min budget; SRP package (~60s CPU) runs concurrently and starves this test under 90s
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	port := addr.Port
	stop := bootServer(t, ctx, key, dcID, st, codes.Logger(), ln)
	t.Cleanup(stop)

	newClient := func(sess telegram.SessionStorage) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:             dcID,
			DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
			PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:       dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: sess,
		})
	}
	codeAuth := auth.CodeAuthenticatorFunc(func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
		return codes.wait(ctx)
	})

	const phone = "+15551260000"

	// flowLogin runs a fresh-session login with the given password and returns
	// whether the client ended up authorized. A fresh session forces a real
	// handshake + login rather than reusing a persisted key.
	flowLogin := func(t *testing.T, password string) (bool, error) {
		t.Helper()
		client := newClient(&session.StorageMemory{})
		flow := auth.NewFlow(auth.Constant(phone, password, codeAuth), auth.SendCodeOptions{})
		var authorized bool
		runErr := client.Run(ctx, func(ctx context.Context) error {
			if err := client.Auth().IfNecessary(ctx, flow); err != nil {
				return err
			}
			s, err := client.Auth().Status(ctx)
			if err != nil {
				return err
			}
			authorized = s.Authorized
			return nil
		})
		return authorized, runErr
	}

	// withPrimary runs fn on an authorized connection for the primary account.
	// The shared session storage means the first call performs the login flow and
	// later calls reconnect with the persisted (already-authorized) key.
	primary := &session.StorageMemory{}
	primaryFlow := auth.NewFlow(auth.Constant(phone, "", codeAuth), auth.SendCodeOptions{})
	withPrimary := func(t *testing.T, fn func(ctx context.Context, c *telegram.Client) error) {
		t.Helper()
		client := newClient(primary)
		if err := client.Run(ctx, func(ctx context.Context) error {
			if err := client.Auth().IfNecessary(ctx, primaryFlow); err != nil {
				return err
			}
			return fn(ctx, client)
		}); err != nil {
			t.Fatalf("primary run: %v", err)
		}
	}

	// --- Phase 1: initial login (no password) + enable 2FA. ---
	withPrimary(t, func(ctx context.Context, c *telegram.Client) error {
		return c.Auth().UpdatePassword(ctx, "pw1", auth.UpdatePasswordOptions{Hint: "hint1"})
	})
	if _, ok, err := st.PasswordByUser(ctx, mustUser(t, ctx, st, phone).ID); err != nil || !ok {
		t.Fatalf("2FA not enabled: ok=%v err=%v", ok, err)
	}

	// --- Phase 2: a fresh login is now challenged for the password. ---
	// Code-only (no password provider): the server must return
	// SESSION_PASSWORD_NEEDED, which gotd surfaces as ErrPasswordNotProvided.
	noPwClient := newClient(&session.StorageMemory{})
	noPwFlow := auth.NewFlow(auth.CodeOnly(phone, codeAuth), auth.SendCodeOptions{})
	noPwErr := noPwClient.Run(ctx, func(ctx context.Context) error {
		return noPwClient.Auth().IfNecessary(ctx, noPwFlow)
	})
	if !errors.Is(noPwErr, auth.ErrPasswordNotProvided) {
		t.Fatalf("expected SESSION_PASSWORD_NEEDED (ErrPasswordNotProvided), got %v", noPwErr)
	}
	// Correct password authorizes.
	if authed, err := flowLogin(t, "pw1"); err != nil || !authed {
		t.Fatalf("login with correct password: authed=%v err=%v", authed, err)
	}
	// Wrong password is rejected.
	if authed, err := flowLogin(t, "wrongpw"); err == nil || authed {
		t.Fatalf("login with wrong password should fail: authed=%v err=%v", authed, err)
	}

	// --- Phase 3: change the password. ---
	withPrimary(t, func(ctx context.Context, c *telegram.Client) error {
		return c.Auth().UpdatePassword(ctx, "pw2", auth.UpdatePasswordOptions{
			Hint:     "hint2",
			Password: func(context.Context) (string, error) { return "pw1", nil },
		})
	})
	if authed, err := flowLogin(t, "pw2"); err != nil || !authed {
		t.Fatalf("login with new password: authed=%v err=%v", authed, err)
	}
	if authed, err := flowLogin(t, "pw1"); err == nil || authed {
		t.Fatalf("login with old password should fail: authed=%v err=%v", authed, err)
	}

	// --- Phase 3b: an email-only update must NOT disable 2FA. ---
	withPrimary(t, func(ctx context.Context, c *telegram.Client) error {
		return emailOnlyUpdate(ctx, c.API(), "pw2")
	})
	if _, ok, err := st.PasswordByUser(ctx, mustUser(t, ctx, st, phone).ID); err != nil || !ok {
		t.Fatalf("email-only update wiped 2FA: ok=%v err=%v", ok, err)
	}
	if authed, err := flowLogin(t, "pw2"); err != nil || !authed {
		t.Fatalf("login after email-only update: authed=%v err=%v", authed, err)
	}

	// --- Phase 4: remove the password; fresh login no longer prompts. ---
	withPrimary(t, func(ctx context.Context, c *telegram.Client) error {
		return removePassword(ctx, c.API(), "pw2")
	})
	if _, ok, err := st.PasswordByUser(ctx, mustUser(t, ctx, st, phone).ID); err != nil || ok {
		t.Fatalf("password not removed: ok=%v err=%v", ok, err)
	}
	if authed, err := flowLogin(t, ""); err != nil || !authed {
		t.Fatalf("login after removal should not prompt: authed=%v err=%v", authed, err)
	}
}

// mustUser fetches the user by phone, failing the test if absent.
func mustUser(t *testing.T, ctx context.Context, st *store.Store, phone string) store.User {
	t.Helper()
	u, ok, err := st.UserByPhone(ctx, phone)
	if err != nil || !ok {
		t.Fatalf("user %s not found: ok=%v err=%v", phone, ok, err)
	}
	return u
}

// emailOnlyUpdate sends an updatePasswordSettings carrying only a recovery
// email (no new_algo/new_password_hash), gated by an SRP proof of the current
// password. The server must treat this as a passthrough no-op, never a removal.
func emailOnlyUpdate(ctx context.Context, api *tg.Client, currentPassword string) error {
	p, err := api.AccountGetPassword(ctx)
	if err != nil {
		return err
	}
	proof, err := auth.PasswordHash([]byte(currentPassword), p.SRPID, p.SRPB, p.SecureRandom, p.CurrentAlgo)
	if err != nil {
		return err
	}
	_, err = api.AccountUpdatePasswordSettings(ctx, &tg.AccountUpdatePasswordSettingsRequest{
		Password:    proof,
		NewSettings: tg.AccountPasswordInputSettings{Email: "recover@example.com"},
	})
	return err
}

// removePassword clears the current 2FA password via the raw API: it proves the
// current password with an SRP proof and sends an empty new_password_hash, which
// the server treats as removal. gotd has no high-level remove helper.
func removePassword(ctx context.Context, api *tg.Client, currentPassword string) error {
	p, err := api.AccountGetPassword(ctx)
	if err != nil {
		return err
	}
	proof, err := auth.PasswordHash([]byte(currentPassword), p.SRPID, p.SRPB, p.SecureRandom, p.CurrentAlgo)
	if err != nil {
		return err
	}
	_, err = api.AccountUpdatePasswordSettings(ctx, &tg.AccountUpdatePasswordSettingsRequest{
		Password:    proof,
		NewSettings: tg.AccountPasswordInputSettings{NewAlgo: &tg.PasswordKdfAlgoUnknown{}},
	})
	return err
}
