package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestProvisionalGateProbe drives a real client whose auth key is bound to a
// provisional (username-mode, no verifier) account and verifies the gate blocks
// non-allow-listed RPCs through both the register and registerRevoke paths.
func TestProvisionalGateProbe(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	keyPath := t.TempDir() + "/key.pem"
	key, err := rsakey.LoadOrGenerate(keyPath)
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

	codes := newCodeSink()
	const dcID = 2
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	handler := api.New(st, dcID, tgcfg, codes.Logger(), true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{}, config.RegistrationClosed)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, codes.Logger())

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, ln) }()
	t.Cleanup(func() {
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
	})

	sess := &session.StorageMemory{}
	newClient := func() *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:             dcID,
			DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:       dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: sess,
		})
	}

	// Login with a phone user to create an auth key.
	phone := "+15551239911"
	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return codes.wait(ctx)
			})),
		auth.SendCodeOptions{},
	)

	c := newClient()
	if err := c.Run(ctx, func(ctx context.Context) error {
		return c.Auth().IfNecessary(ctx, flow)
	}); err != nil {
		t.Fatalf("login: %v", err)
	}

	phoneUser, ok, err := st.UserByPhone(ctx, phone)
	if err != nil || !ok {
		t.Fatalf("user by phone: ok=%v err=%v", ok, err)
	}
	keys, err := st.AuthKeysByUser(ctx, phoneUser.ID)
	if err != nil || len(keys) != 1 {
		t.Fatalf("auth keys by user: n=%d err=%v", len(keys), err)
	}
	keyID := keys[0].ID

	// Rebind the client's key to a username-mode account with no verifier:
	// the exact state auth.signUp will produce once it ships.
	prov, err := st.CreateUsernameUser(ctx, "provprobe", "Prov", "Probe")
	if err != nil {
		t.Fatal(err)
	}
	if err := st.ClaimUsername(ctx, prov.ID, "provprobe"); err != nil {
		t.Fatal(err)
	}
	if err := st.BindAuthKeyUser(ctx, keyID, prov.ID); err != nil {
		t.Fatal(err)
	}

	// Verify the session is now provisional.
	row, ok, err := st.AuthKeyByID(ctx, keyID)
	if err != nil || !ok || !row.Provisional {
		t.Fatalf("expected provisional key: ok=%v provisional=%v err=%v", ok, row.Provisional, err)
	}

	// Drive a new client connection with the saved session.
	c2 := newClient()
	if err := c2.Run(ctx, func(ctx context.Context) error {
		raw := tg.NewClient(c2)

		// help.getConfig should succeed (allow-listed).
		if _, err := raw.HelpGetConfig(ctx); err != nil {
			t.Errorf("help.getConfig: expected success, got %v", err)
		} else {
			t.Log("help.getConfig succeeded as expected")
		}

		// updates.getState — registered with register — should be blocked.
		if _, err := raw.UpdatesGetState(ctx); err == nil {
			t.Error("updates.getState: expected gate to reject, got success")
		} else if !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
			t.Errorf("updates.getState: err = %v, want AUTH_KEY_UNREGISTERED", err)
		} else {
			t.Log("updates.getState blocked as expected")
		}

		// account.resetAuthorization — registered with registerRevoke — should be blocked.
		res, err := raw.AccountResetAuthorization(ctx, keyID)
		t.Logf("account.resetAuthorization: res=%v err=%v", res, err)
		switch {
		case err == nil:
			t.Error("account.resetAuthorization: reached the handler from a provisional session")
		case !tgerr.Is(err, "AUTH_KEY_UNREGISTERED"):
			t.Errorf("account.resetAuthorization: err = %v, want AUTH_KEY_UNREGISTERED", err)
		default:
			t.Log("account.resetAuthorization blocked as expected")
		}

		// help.getNearestDc — not registered on the server (handled by fallback).
		// The handleUnknownGated fallback must still apply the gate and return
		// AUTH_KEY_UNREGISTERED rather than INPUT_METHOD_INVALID.
		if _, err := raw.HelpGetNearestDC(ctx); err == nil {
			t.Error("help.getNearestDc: expected gate to reject, got success")
		} else if !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
			t.Errorf("help.getNearestDc: err = %v, want AUTH_KEY_UNREGISTERED", err)
		} else {
			t.Log("help.getNearestDc blocked as expected")
		}
		return nil
	}); err != nil {
		t.Fatalf("run: %v", err)
	}

	// If the handler ran, the key it named is gone.
	_, stillThere, err := st.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("auth key still present after resetAuthorization: %v", stillThere)
	if !stillThere {
		t.Error("provisional session deleted an auth key through account.resetAuthorization")
	}
}
