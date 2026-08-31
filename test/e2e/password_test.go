package e2e_test

import (
	"context"
	"errors"
	"fmt"
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
	// Budget derived by measurement, not chosen round. This test is eight
	// sequential fresh-session logins (RSA+DH handshake, then SRP) plus three
	// reconnects on the persisted key, all under -race: the time goes on
	// cryptography the test has to do, not on a wait that could be tightened,
	// so wall time scales with how little of the host it gets. Measured on the
	// 4-vCPU host, busy loops standing in for foreign suites (one e2e suite
	// saturates roughly four threads; 8-12 loops is the two-to-three foreign
	// suites this has actually been seen failing under):
	//
	//	quiet host      68s, 79s, 81s
	//	 4 busy loops   176s
	//	 8 busy loops   147s, 160s, 209s, 210s, 216s, 278s
	//	12 busy loops   220s
	//	16 busy loops   337s
	//
	// 8m is 1.4x the slowest measured and 1.7x the slowest inside that
	// two-to-three suite band, and stays under the package's 15m -timeout so a
	// real hang still fails here, naming the step it hung in, rather than as a
	// suite-wide goroutine dump. The previous 180s sat inside the band and
	// failed the test for contention the rest of the suite survives (MAIN-240).
	const budget = 8 * time.Minute
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	// fail reports a failed step by name, and says when the budget rather than
	// the server is what ended it. Every wait below bottoms out in client.Run
	// returning a bare "context deadline exceeded" that names neither the step
	// it was in nor the budget as the cause, and under contention that is the
	// error every one of these call sites reports.
	fail := func(t *testing.T, step, detail string) {
		t.Helper()
		if ctx.Err() != nil {
			t.Fatalf("%s: %s budget exhausted after %s: %v: %s", step, budget, time.Since(started).Round(time.Second), ctx.Err(), detail)
		}
		t.Fatalf("%s: %s", step, detail)
	}

	// rejected reports whether err is the server refusing the SRP proof
	// (PASSWORD_HASH_INVALID, which gotd converts to ErrPasswordInvalid) rather
	// than the budget expiring mid-login. The two rejection checks below are the
	// only ones satisfied by an error instead of by its absence, so without this
	// an expiry passes them silently and is then reported against a later step
	// than the one that actually ran out.
	rejected := func(err error) bool { return errors.Is(err, auth.ErrPasswordInvalid) }

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
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
	seedPhoneUsers(t, ctx, st, phone)

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
	withPrimary := func(t *testing.T, step string, fn func(ctx context.Context, c *telegram.Client) error) {
		t.Helper()
		client := newClient(primary)
		if err := client.Run(ctx, func(ctx context.Context) error {
			if err := client.Auth().IfNecessary(ctx, primaryFlow); err != nil {
				return err
			}
			return fn(ctx, client)
		}); err != nil {
			fail(t, step, err.Error())
		}
	}

	// --- Phase 1: initial login (no password) + enable 2FA. ---
	withPrimary(t, "phase 1: primary login and enable 2FA", func(ctx context.Context, c *telegram.Client) error {
		return c.Auth().UpdatePassword(ctx, "pw1", auth.UpdatePasswordOptions{Hint: "hint1"})
	})
	if _, ok, err := st.PasswordByUser(ctx, mustUser(t, ctx, st, phone).ID); err != nil || !ok {
		fail(t, "phase 1: read stored password", fmt.Sprintf("2FA not enabled: ok=%v err=%v", ok, err))
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
		fail(t, "phase 2: code-only login", fmt.Sprintf("expected SESSION_PASSWORD_NEEDED (ErrPasswordNotProvided), got %v", noPwErr))
	}
	// Correct password authorizes.
	if authed, err := flowLogin(t, "pw1"); err != nil || !authed {
		fail(t, "phase 2: login with correct password", fmt.Sprintf("authed=%v err=%v", authed, err))
	}
	// Wrong password is rejected.
	if authed, err := flowLogin(t, "wrongpw"); authed || !rejected(err) {
		fail(t, "phase 2: login with wrong password", fmt.Sprintf("want ErrPasswordInvalid: authed=%v err=%v", authed, err))
	}

	// --- Phase 3: change the password. ---
	withPrimary(t, "phase 3: change password to pw2", func(ctx context.Context, c *telegram.Client) error {
		return c.Auth().UpdatePassword(ctx, "pw2", auth.UpdatePasswordOptions{
			Hint:     "hint2",
			Password: func(context.Context) (string, error) { return "pw1", nil },
		})
	})
	if authed, err := flowLogin(t, "pw2"); err != nil || !authed {
		fail(t, "phase 3: login with new password", fmt.Sprintf("authed=%v err=%v", authed, err))
	}
	if authed, err := flowLogin(t, "pw1"); authed || !rejected(err) {
		fail(t, "phase 3: login with old password", fmt.Sprintf("want ErrPasswordInvalid: authed=%v err=%v", authed, err))
	}

	// --- Phase 3b: an email-only update must NOT disable 2FA. ---
	withPrimary(t, "phase 3b: email-only update", func(ctx context.Context, c *telegram.Client) error {
		return emailOnlyUpdate(ctx, c.API(), "pw2")
	})
	if _, ok, err := st.PasswordByUser(ctx, mustUser(t, ctx, st, phone).ID); err != nil || !ok {
		fail(t, "phase 3b: read stored password", fmt.Sprintf("email-only update wiped 2FA: ok=%v err=%v", ok, err))
	}
	if authed, err := flowLogin(t, "pw2"); err != nil || !authed {
		fail(t, "phase 3b: login after email-only update", fmt.Sprintf("authed=%v err=%v", authed, err))
	}

	// --- Phase 4: remove the password; fresh login no longer prompts. ---
	withPrimary(t, "phase 4: remove password", func(ctx context.Context, c *telegram.Client) error {
		return removePassword(ctx, c.API(), "pw2")
	})
	if _, ok, err := st.PasswordByUser(ctx, mustUser(t, ctx, st, phone).ID); err != nil || ok {
		fail(t, "phase 4: read stored password", fmt.Sprintf("password not removed: ok=%v err=%v", ok, err))
	}
	if authed, err := flowLogin(t, ""); err != nil || !authed {
		fail(t, "phase 4: login after removal", fmt.Sprintf("should not prompt: authed=%v err=%v", authed, err))
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
