package e2e_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	gotdsrp "github.com/gotd/td/crypto/srp"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	tsrp "github.com/adambenhassen/telegram-server/internal/srp"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// --- helpers ---

// bootServerWithRegMode boots a server with the given registration mode and
// returns a stop function.
func bootServerWithRegMode(t *testing.T, ctx context.Context, key *rsa.PrivateKey, dcID int, st *store.Store, log *slog.Logger, ln net.Listener, regMode config.RegistrationMode) func() {
	t.Helper()
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	handler := api.New(st, dcID, tgcfg, log, true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{}, regMode)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, log)

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, ln) }()

	var once bool
	return func() {
		if once {
			return
		}
		once = true
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
	}
}

// bootServerWithRegAndLimits boots a server with the given registration mode
// and rate-limit config, and returns a stop function.
func bootServerWithRegAndLimits(t *testing.T, ctx context.Context, key *rsa.PrivateKey, dcID int, st *store.Store, log *slog.Logger, ln net.Listener, regMode config.RegistrationMode, rateLimits config.RateLimitsConfig) func() {
	t.Helper()
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	handler := api.New(st, dcID, tgcfg, log, true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), rateLimits, regMode)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, log)

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, ln) }()

	var once bool
	return func() {
		if once {
			return
		}
		once = true
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
	}
}

// newUsernameClient creates a gotd client pointed at the server on the given
// port, using the given RSA key. SessionStorage defaults to a fresh memory
// session if nil.
func newUsernameClient(port int, key *rsa.PrivateKey, dcID int, sess telegram.SessionStorage) *telegram.Client {
	if sess == nil {
		sess = &session.StorageMemory{}
	}
	return telegram.NewClient(1, "hash", telegram.Options{
		DC:             dcID,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
		PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
		Resolver:       dcs.Plain(dcs.PlainOptions{}),
		SessionStorage: sess,
	})
}

// sendCodeUsername calls auth.sendCode with a username and returns the
// phoneCodeHash. It is used in username-mode auth flows where the identifier
// is a username, not a phone number.
func sendCodeUsername(ctx context.Context, api *tg.Client, username string) (string, error) {
	sent, err := api.AuthSendCode(ctx, &tg.AuthSendCodeRequest{
		PhoneNumber: username,
		APIID:       1,
		APIHash:     "hash",
	})
	if err != nil {
		return "", err
	}
	if sc, ok := sent.(*tg.AuthSentCode); ok {
		return sc.PhoneCodeHash, nil
	}
	return "", fmt.Errorf("unexpected sendCode response type: %T", sent)
}

// signInUsername calls auth.signIn with a username and code hash. It returns
// the raw response so the caller can inspect whether the server returned
// SESSION_PASSWORD_NEEDED or authorizationSignUpRequired.
func signInUsername(ctx context.Context, api *tg.Client, username, codeHash, code string) (bin.Encoder, error) {
	return api.AuthSignIn(ctx, &tg.AuthSignInRequest{
		PhoneNumber:   username,
		PhoneCodeHash: codeHash,
		PhoneCode:     code,
	})
}

// signUpUsername calls auth.signUp with a username (as PhoneNumber), code hash,
// and display name. Returns the raw response.
func signUpUsername(ctx context.Context, api *tg.Client, username, codeHash, firstName, lastName string) (bin.Encoder, error) {
	return api.AuthSignUp(ctx, &tg.AuthSignUpRequest{
		PhoneNumber:   username,
		PhoneCodeHash: codeHash,
		FirstName:     firstName,
		LastName:      lastName,
	})
}

// checkPasswordUsername performs the full SRP password check: fetches password
// settings, computes the SRP proof, and calls auth.checkPassword. Asserts the
// returned value is *tg.AuthAuthorization.
func checkPasswordUsername(ctx context.Context, api *tg.Client, password string) error {
	pwd, err := api.AccountGetPassword(ctx)
	if err != nil {
		return err
	}
	proof, err := auth.PasswordHash([]byte(password), pwd.SRPID, pwd.SRPB, pwd.SecureRandom, pwd.CurrentAlgo)
	if err != nil {
		return err
	}
	resp, err := api.AuthCheckPassword(ctx, proof)
	if err != nil {
		return err
	}
	if _, ok := resp.(*tg.AuthAuthorization); !ok {
		return fmt.Errorf("checkPassword: unexpected response type %T, want *tg.AuthAuthorization", resp)
	}
	return nil
}

// isSignUpRequired checks if the error is auth.authorizationSignUpRequired.
func isSignUpRequired(err error) bool {
	if err == nil {
		return false
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		return false
	}
	return tgErr.Message == "SIGN_UP_REQUIRED"
}

// isSessionPasswordNeeded checks if the error is SESSION_PASSWORD_NEEDED.
func isSessionPasswordNeeded(err error) bool {
	if err == nil {
		return false
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		return false
	}
	return tgErr.Message == "SESSION_PASSWORD_NEEDED"
}

// isAuthKeyUnregistered checks if the error is AUTH_KEY_UNREGISTERED.
func isAuthKeyUnregistered(err error) bool {
	if err == nil {
		return false
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		return false
	}
	return tgErr.Message == "AUTH_KEY_UNREGISTERED"
}

// isInputRequestInvalid checks if the error is INPUT_REQUEST_INVALID.
func isInputRequestInvalid(err error) bool {
	if err == nil {
		return false
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		return false
	}
	return tgErr.Message == "INPUT_REQUEST_INVALID"
}

// isUsernameNotModified checks if the error is USERNAME_NOT_MODIFIED.
func isUsernameNotModified(err error) bool {
	if err == nil {
		return false
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		return false
	}
	return tgErr.Message == "USERNAME_NOT_MODIFIED"
}

// isFloodWait checks if the error is a FLOOD_WAIT error.
func isFloodWait(err error) bool {
	if err == nil {
		return false
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		return false
	}
	return strings.HasPrefix(tgErr.Message, "FLOOD_WAIT")
}

// seedUsernameUser creates a username-mode user with the given username,
// display name, and password (SRP verifier). Returns the user ID.
func seedUsernameUser(t *testing.T, ctx context.Context, st *store.Store, username, firstName, password string) int64 {
	t.Helper()
	user, err := st.CreateUsernameUser(ctx, username, firstName, "")
	if err != nil {
		t.Fatalf("create username user %s: %v", username, err)
	}
	if err := st.ClaimUsername(ctx, user.ID, username); err != nil {
		t.Fatalf("claim username %s: %v", username, err)
	}
	// Generate SRP verifier from the password.
	verifier, salt1, salt2, err := testComputeSRPVerifier([]byte(password))
	if err != nil {
		t.Fatalf("compute SRP verifier for %s: %v", username, err)
	}
	if err := st.UpsertPassword(ctx, store.UserPassword{
		UserID:   user.ID,
		Salt1:    salt1,
		Salt2:    salt2,
		Verifier: verifier,
	}); err != nil {
		t.Fatalf("upsert password for %s: %v", username, err)
	}
	return user.ID
}

// testComputeSRPVerifier generates fresh salts and computes the SRP verifier
// for the given password, mirroring the logic in store.computeSRPVerifier.
func testComputeSRPVerifier(password []byte) (verifier, salt1, salt2 []byte, err error) {
	salt1 = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt1); err != nil {
		return nil, nil, nil, fmt.Errorf("generate salt1: %w", err)
	}
	salt2 = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt2); err != nil {
		return nil, nil, nil, fmt.Errorf("generate salt2: %w", err)
	}

	srpClient := gotdsrp.NewSRP(rand.Reader)
	algo := gotdsrp.Input{
		Salt1: salt1,
		Salt2: salt2,
		G:     3,
		P:     tsrp.PBytes(),
	}
	verifier, augmentedSalt1, err := srpClient.NewHash(password, algo)
	if err != nil {
		return nil, nil, nil, err
	}
	return verifier, augmentedSalt1, salt2, nil
}

// --- tests ---

// TestUsernamePasswordSignIn proves the full username/password sign-in flow
// against a real gotd client: sendCode with username, signIn to get
// SESSION_PASSWORD_NEEDED, then account.getPassword + checkPassword to complete.
func TestUsernamePasswordSignIn(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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
	stop := bootServerWithRegMode(t, ctx, key, dcID, st, codes.Logger(), ln, config.RegistrationInvite)
	t.Cleanup(stop)

	const username = "signintest1"
	const password = "testpassword"
	const firstName = "Test"

	// Seed a username-mode user with a password verifier.
	seedUsernameUser(t, ctx, st, username, firstName, password)

	client := newUsernameClient(port, key, dcID, nil)
	if err := client.Run(ctx, func(ctx context.Context) error {
		api := client.API()

		// Step 1: auth.sendCode with username.
		hash, err := sendCodeUsername(ctx, api, username)
		if err != nil {
			return fmt.Errorf("sendCode: %w", err)
		}

		// Step 2: auth.signIn with username + hash → SESSION_PASSWORD_NEEDED.
		_, err = signInUsername(ctx, api, username, hash, "")
		if !isSessionPasswordNeeded(err) {
			return fmt.Errorf("signIn: expected SESSION_PASSWORD_NEEDED, got %w", err)
		}

		// Step 3: account.getPassword + auth.checkPassword.
		if err := checkPasswordUsername(ctx, api, password); err != nil {
			return fmt.Errorf("checkPassword: %w", err)
		}

		// Step 4: verify authorized.
		s, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !s.Authorized {
			return errors.New("not authorized after checkPassword")
		}

		return nil
	}); err != nil {
		t.Fatalf("login flow: %v", err)
	}
}

// TestUsernamePasswordSignUp proves the invite-backed username registration
// flow against a real gotd client and leaves the new account provisional.
func TestUsernamePasswordSignUp(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

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
	stop := bootServerWithRegMode(t, ctx, key, dcID, st, codes.Logger(), ln, config.RegistrationInvite)
	t.Cleanup(stop)

	const username = "signup_test_user"
	const firstName = "SignUp"
	_, inviteSecret, err := st.IssueInvite(ctx, username)
	if err != nil {
		t.Fatalf("issue invite: %v", err)
	}

	client := newUsernameClient(port, key, dcID, nil)
	if err := client.Run(ctx, func(ctx context.Context) error {
		api := client.API()

		// Step 1: auth.sendCode with unknown username.
		hash, err := sendCodeUsername(ctx, api, username)
		if err != nil {
			return fmt.Errorf("sendCode: %w", err)
		}

		// Step 2: auth.signIn → authorizationSignUpRequired.
		resp, err := signInUsername(ctx, api, username, hash, inviteSecret)
		if err != nil {
			// gotd returns SIGN_UP_REQUIRED as a tg error.
			if !isSignUpRequired(err) {
				return fmt.Errorf("signIn: expected SIGN_UP_REQUIRED, got %w", err)
			}
		} else {
			// If gotd returns the struct directly.
			if _, ok := resp.(*tg.AuthAuthorizationSignUpRequired); !ok {
				return fmt.Errorf("signIn: unexpected response type %T, want AuthAuthorizationSignUpRequired", resp)
			}
		}

		// Step 3: auth.signUp → auth.Authorization with the provisional user.
		resp, err = signUpUsername(ctx, api, username, hash, firstName, "")
		if err != nil {
			return fmt.Errorf("signUp: %w", err)
		}
		authz, ok := resp.(*tg.AuthAuthorization)
		if !ok {
			return fmt.Errorf("signUp: unexpected response type %T, want AuthAuthorization", resp)
		}
		if user, ok := authz.User.(*tg.User); !ok || user.ID == 0 {
			return fmt.Errorf("signUp: authorization user = %T, want non-zero tg.User", authz.User)
		}

		return nil
	}); err != nil {
		t.Fatalf("invite registration flow: %v", err)
	}
}

// TestRegistrationClosed proves that when the server is started with
// registration closed (the default), auth.signUp returns INPUT_REQUEST_INVALID.
func TestRegistrationClosed(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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
	// Explicitly set registration mode to closed.
	stop := bootServerWithRegMode(t, ctx, key, dcID, st, codes.Logger(), ln, config.RegistrationClosed)
	t.Cleanup(stop)

	const username = "closed_reg_user"
	const firstName = "Closed"

	client := newUsernameClient(port, key, dcID, nil)
	if err := client.Run(ctx, func(ctx context.Context) error {
		api := client.API()

		// sendCode with unknown username.
		hash, err := sendCodeUsername(ctx, api, username)
		if err != nil {
			return fmt.Errorf("sendCode: %w", err)
		}

		// signIn → signUpRequired
		resp, err := signInUsername(ctx, api, username, hash, "")
		if err == nil {
			if _, ok := resp.(*tg.AuthAuthorizationSignUpRequired); !ok {
				return fmt.Errorf("signIn: unexpected response type %T", resp)
			}
		} else if !isSignUpRequired(err) {
			return fmt.Errorf("signIn: expected SIGN_UP_REQUIRED, got %w", err)
		}

		// signUp → should be rejected with INPUT_REQUEST_INVALID.
		_, err = signUpUsername(ctx, api, username, hash, firstName, "")
		if err == nil {
			return errors.New("signUp: expected INPUT_REQUEST_INVALID, got success")
		}
		if !isInputRequestInvalid(err) {
			return fmt.Errorf("signUp: expected INPUT_REQUEST_INVALID, got %w", err)
		}

		return nil
	}); err != nil {
		t.Fatalf("registration-closed flow: %v", err)
	}
}

// TestUsernameModeLock proves that an existing username-mode account keeps its
// immutable login handle after sign-in.
func TestUsernameModeLock(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

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
	stop := bootServerWithRegMode(t, ctx, key, dcID, st, codes.Logger(), ln, config.RegistrationInvite)
	t.Cleanup(stop)

	const username = "locktest_user"
	const password = "lockpassword"
	const firstName = "Lock"

	// Seed the account directly. Registration is intentionally unavailable, so
	// this test must not depend on auth.signUp to create its fixture.
	seedUsernameUser(t, ctx, st, username, firstName, password)

	client := newUsernameClient(port, key, dcID, nil)
	if err := client.Run(ctx, func(ctx context.Context) error {
		api := client.API()

		// Full sign-in flow.
		hash, err := sendCodeUsername(ctx, api, username)
		if err != nil {
			return fmt.Errorf("sendCode: %w", err)
		}

		_, err = signInUsername(ctx, api, username, hash, "")
		if !isSessionPasswordNeeded(err) {
			return fmt.Errorf("signIn: expected SESSION_PASSWORD_NEEDED, got %w", err)
		}

		if err := checkPasswordUsername(ctx, api, password); err != nil {
			return fmt.Errorf("checkPassword: %w", err)
		}

		// Now try to change the username.
		_, err = api.AccountUpdateUsername(ctx, "newusername")
		if err == nil {
			return errors.New("account.updateUsername: expected USERNAME_NOT_MODIFIED, got success")
		}
		if !isUsernameNotModified(err) {
			return fmt.Errorf("account.updateUsername: expected USERNAME_NOT_MODIFIED, got %w", err)
		}

		// Also try clearing the username.
		_, err = api.AccountUpdateUsername(ctx, "")
		if err == nil {
			return errors.New("account.updateUsername(\"\"): expected USERNAME_NOT_MODIFIED, got success")
		}
		if !isUsernameNotModified(err) {
			return fmt.Errorf("account.updateUsername(\"\"): expected USERNAME_NOT_MODIFIED, got %w", err)
		}

		return nil
	}); err != nil {
		t.Fatalf("username-lock phase: %v", err)
	}
}

// TestCheckPasswordBruteForceAccount proves that repeated failed
// auth.checkPassword calls against one account eventually return FLOOD_WAIT_<n>,
// while a different account in the same window is unaffected.
func TestCheckPasswordBruteForceAccount(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

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

	// Use a small per-account checkPassword limit: 3 failures in 10 minutes.
	rateLimits := config.RateLimitsConfig{
		CheckPassword: store.RateLimitConfig{Limit: 3, Window: 10 * time.Minute},
	}
	stop := bootServerWithRegAndLimits(t, ctx, key, dcID, st, codes.Logger(), ln, config.RegistrationInvite, rateLimits)
	t.Cleanup(stop)

	const usernameA = "brute_force_user_a"
	const usernameB = "brute_force_user_b"
	const passwordA = "passwordA"
	const passwordB = "passwordB"
	const firstName = "Brute"

	// Seed both users with password verifiers.
	seedUsernameUser(t, ctx, st, usernameA, firstName, passwordA)
	seedUsernameUser(t, ctx, st, usernameB, firstName, passwordB)

	// signInAndStay logs in a username-mode account to the SESSION_PASSWORD_NEEDED
	// state and keeps the client alive, so subsequent checkPassword calls can be
	// made on the same connection.
	signInAndStay := func(t *testing.T, ctx context.Context, username string) (*tg.Client, context.CancelFunc) {
		t.Helper()
		sess := &session.StorageMemory{}
		client := newUsernameClient(port, key, dcID, sess)
		runCtx, runCancel := context.WithCancel(ctx)
		ready := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			done <- client.Run(runCtx, func(ctx context.Context) error {
				api := client.API()
				hash, err := sendCodeUsername(ctx, api, username)
				if err != nil {
					return fmt.Errorf("sendCode: %w", err)
				}
				_, err = signInUsername(ctx, api, username, hash, "")
				if !isSessionPasswordNeeded(err) {
					return fmt.Errorf("signIn: expected SESSION_PASSWORD_NEEDED, got %w", err)
				}
				// Assert pending key is refused authorized RPCs.
				_, err = api.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUser{}})
				if err == nil {
					return errors.New("users.getUsers: expected AUTH_KEY_UNREGISTERED from pending key, got success")
				}
				if !isAuthKeyUnregistered(err) {
					return fmt.Errorf("users.getUsers: expected AUTH_KEY_UNREGISTERED, got %w", err)
				}
				close(ready)
				// Block until context is cancelled.
				<-ctx.Done()
				return nil
			})
		}()
		// Wait for the login to complete.
		select {
		case <-ready:
			// Login complete, client is alive and ready.
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("signInAndStay %s: %v", username, err)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("signInAndStay %s: timed out waiting for login", username)
		}
		return client.API(), runCancel
	}

	// Helper: attempt checkPassword with a wrong password.
	wrongPassword := func(ctx context.Context, api *tg.Client) error {
		pwd, err := api.AccountGetPassword(ctx)
		if err != nil {
			return err
		}
		proof, err := auth.PasswordHash([]byte("wrongpassword"), pwd.SRPID, pwd.SRPB, pwd.SecureRandom, pwd.CurrentAlgo)
		if err != nil {
			return err
		}
		_, err = api.AuthCheckPassword(ctx, proof)
		return err
	}

	// User A: 3 failed attempts should be allowed, 4th should be FLOOD_WAIT.
	apiA, cancelA := signInAndStay(t, ctx, usernameA)
	defer cancelA()
	for i := range 3 {
		err := wrongPassword(ctx, apiA)
		if err == nil {
			t.Fatalf("A attempt %d: expected PASSWORD_HASH_INVALID, got success", i+1)
		}
		var rpcErr *tgerr.Error
		if !errors.As(err, &rpcErr) || rpcErr.Message != "PASSWORD_HASH_INVALID" {
			t.Fatalf("A attempt %d: expected PASSWORD_HASH_INVALID, got %v", i+1, err)
		}
	}

	// 4th attempt should be FLOOD_WAIT.
	err = wrongPassword(ctx, apiA)
	if err == nil {
		t.Fatal("A attempt 4: expected FLOOD_WAIT, got success")
	}
	if !isFloodWait(err) {
		t.Fatalf("A attempt 4: expected FLOOD_WAIT, got %v", err)
	}

	// User B: should be unaffected by A's failures.
	apiB, cancelB := signInAndStay(t, ctx, usernameB)
	defer cancelB()
	// B's first attempt should succeed (correct password).
	if err := checkPasswordUsername(ctx, apiB, passwordB); err != nil {
		t.Fatalf("B correct password: %v", err)
	}
}

// TestSignUpUsernameOccupied proves that registration is rejected before
// username occupancy can be disclosed.
func TestSignUpUsernameOccupied(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

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
	stop := bootServerWithRegMode(t, ctx, key, dcID, st, codes.Logger(), ln, config.RegistrationInvite)
	t.Cleanup(stop)

	const username = "occupied_user"
	const firstName = "Occupied"

	// Pre-register the username.
	seedUsernameUser(t, ctx, st, username, firstName, "somepassword")

	// Attempt to sign up with the same username.
	client := newUsernameClient(port, key, dcID, nil)
	if err := client.Run(ctx, func(ctx context.Context) error {
		api := client.API()

		// sendCode with the occupied username.
		hash, err := sendCodeUsername(ctx, api, username)
		if err != nil {
			return fmt.Errorf("sendCode: %w", err)
		}

		// signIn → should return SESSION_PASSWORD_NEEDED (user exists with password).
		_, err = signInUsername(ctx, api, username, hash, "")
		if !isSessionPasswordNeeded(err) {
			return fmt.Errorf("signIn: expected SESSION_PASSWORD_NEEDED, got %w", err)
		}

		// signUp → should be rejected at the registration boundary.
		_, err = signUpUsername(ctx, api, username, hash, firstName, "")
		if err == nil {
			return errors.New("signUp: expected INPUT_REQUEST_INVALID, got success")
		}
		if !isInputRequestInvalid(err) {
			return fmt.Errorf("signUp: expected INPUT_REQUEST_INVALID, got %w", err)
		}

		return nil
	}); err != nil {
		t.Fatalf("occupied username flow: %v", err)
	}
}
