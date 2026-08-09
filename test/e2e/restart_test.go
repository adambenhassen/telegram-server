package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestRestartPersistence is the Milestone 2 gate: a client that logs in must
// survive a full server restart against the same database and stay authorized
// WITHOUT a new handshake or a new login.
//
// Session-reuse mechanism: the gotd client is given a single
// *session.StorageMemory (telegram.Options.SessionStorage) and is reused across
// both server generations. After login the client holds its MTProto auth key in
// that storage; on the second run it reconnects with the SAME auth key ID rather
// than performing key exchange. Server #2, backed by the same Postgres DB, loads
// that key from auth_keys and accepts the encrypted frames. If persistence were
// broken, server #2 would answer the unknown auth key ID with AuthKeyNotFound,
// the client would be forced to re-handshake into an unbound key, and
// Auth().Status would report Authorized=false, failing this test.
func TestRestartPersistence(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Same server RSA key for both generations.
	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}

	// One database for the whole test.
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

	// Boot server #1 on an ephemeral port.
	ln1 := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln1.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln1.Addr())
	}
	port := addr.Port
	stop1 := bootServer(t, ctx, key, dcID, st, codes.Logger(), ln1)

	// A single session storage is shared across both client generations. gotd's
	// Client.Run is one-shot, so run #2 uses a fresh client that loads the auth
	// key from this same storage rather than performing a new key exchange.
	sess := &session.StorageMemory{}
	newClient := func(port int) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:             dcID,
			DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
			PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:       dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: sess,
		})
	}
	client := newClient(port)

	phone := "+15551239999"
	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return codes.wait(ctx)
			})),
		auth.SendCodeOptions{},
	)

	// Run #1: log in against server #1.
	if err := client.Run(ctx, func(ctx context.Context) error {
		return client.Auth().IfNecessary(ctx, flow)
	}); err != nil {
		t.Fatalf("login flow: %v", err)
	}

	u, ok, err := st.UserByPhone(ctx, phone)
	if err != nil || !ok {
		t.Fatalf("user not persisted: ok=%v err=%v", ok, err)
	}
	keysBefore, err := st.AuthKeysByUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("auth keys by user: %v", err)
	}
	if len(keysBefore) != 1 {
		t.Fatalf("want 1 bound auth key before restart, got %d", len(keysBefore))
	}

	// Restart: stop server #1, then boot server #2 on the SAME port, DB, and key.
	stop1()
	ln2 := mustListen(t, ctx, fmt.Sprintf("127.0.0.1:%d", port))
	stop2 := bootServer(t, ctx, key, dcID, st, codes.Logger(), ln2)
	t.Cleanup(stop2)

	// Run #2: fresh client, SAME session storage. No auth flow is provided, so an
	// authorized status can only come from the persisted auth key.
	client2 := newClient(port)
	var status *auth.Status
	if err := client2.Run(ctx, func(ctx context.Context) error {
		s, serr := client2.Auth().Status(ctx)
		if serr != nil {
			return serr
		}
		status = s
		return nil
	}); err != nil {
		t.Fatalf("post-restart run: %v", err)
	}

	if !status.Authorized {
		t.Fatal("client not authorized after restart: persisted auth key was rejected")
	}
	if status.User == nil || status.User.ID != u.ID {
		t.Fatalf("post-restart self user = %+v, want id %d", status.User, u.ID)
	}

	// The auth_keys row survived the restart and is still bound to the user.
	keysAfter, err := st.AuthKeysByUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("auth keys by user after restart: %v", err)
	}
	if len(keysAfter) != 1 || keysAfter[0].ID != keysBefore[0].ID {
		t.Fatalf("auth key not persisted across restart: before=%v after=%v", keysBefore, keysAfter)
	}
	if keysAfter[0].UserID != u.ID {
		t.Fatalf("auth key %d bound to user %d, want %d", keysAfter[0].ID, keysAfter[0].UserID, u.ID)
	}
}

// mustListen binds a TCP listener on addr, failing the test on error.
func mustListen(t *testing.T, ctx context.Context, addr string) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("listen %s: %v", addr, err)
	}
	return ln
}

// bootServer starts an mtproto server on ln backed by st and returns a stop
// function that cancels it and waits for the serve loop to exit.
func bootServer(t *testing.T, ctx context.Context, key *rsa.PrivateKey, dcID int, st *store.Store, log *slog.Logger, ln net.Listener) func() {
	t.Helper()
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	// Sign-in here reads the code off the log, so the gated line must be on.
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	handler := api.New(st, dcID, tgcfg, log, true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{})
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
