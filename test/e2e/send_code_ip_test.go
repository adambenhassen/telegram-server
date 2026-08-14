package e2e_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
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

// TestClientLoginUnderDefaultRateLimits proves the shipped per-IP defaults do
// not stand in the way of a real client logging in. The limits are read out of
// the environment loader rather than written here, so a default tightened to a
// value that breaks the first login fails this test rather than production.
//
// Not parallel: it reads the real defaults through config.Load, and t.Setenv
// cannot be used from a parallel test.
func TestClientLoginUnderDefaultRateLimits(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Load needs a DSN and a master key to return at all; neither is used here,
	// the store below opens its own.
	t.Setenv("TG_POSTGRES_DSN", "postgres://unused/tg")
	t.Setenv("TG_AUTHKEY_ENC_KEY", strings.Repeat("0a", 32))
	cfg, err := config.Load(slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !cfg.RateLimits.SendCodeIP.Enabled() {
		t.Fatal("the per-IP sendCode limits are off by default: this test would prove nothing")
	}

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
	codes := newMultiCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}

	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	handler := api.New(st, dcID, tgcfg, codes.Logger(), true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), cfg.RateLimits, config.RegistrationClosed)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, codes.Logger())

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, ln) }()
	t.Cleanup(func() {
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
	})

	// Two accounts from the one loopback address: a shared-NAT population is
	// meant to be delayed at worst, never locked out at the first user.
	for _, phone := range []string{"+15551270001", "+15551270002"} {
		client := telegram.NewClient(1, "hash", telegram.Options{
			DC:         dcID,
			DCList:     dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys: []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:   dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: &telegram.FileSessionStorage{
				Path: t.TempDir() + "/session.json",
			},
		})
		flow := auth.NewFlow(
			auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
				func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
					return codes.wait(ctx, phone)
				})),
			auth.SendCodeOptions{},
		)
		if err := client.Run(ctx, func(ctx context.Context) error {
			return client.Auth().IfNecessary(ctx, flow)
		}); err != nil {
			t.Fatalf("login flow for %s under the default per-IP limits: %v", phone, err)
		}
		if _, ok, err := st.UserByPhone(ctx, phone); err != nil || !ok {
			t.Fatalf("user %s not persisted: ok=%v err=%v", phone, ok, err)
		}
	}
}
