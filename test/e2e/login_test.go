package e2e_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/config"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestClientLogin(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Server key.
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
	// The code sink scrapes the issued code out of the log, so the e2e suite
	// needs the gated line on.
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

	// Client pointed at our server. The Plain resolver dials the DC address
	// taken from DCList, so no separate address needs to be supplied.
	client := telegram.NewClient(1, "hash", telegram.Options{
		DC:         dcID,
		DCList:     dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
		PublicKeys: []telegram.PublicKey{{RSA: &key.PublicKey}},
		Resolver:   dcs.Plain(dcs.PlainOptions{}),
	})

	phone := "+15551230000"
	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return codes.wait(ctx)
			})),
		auth.SendCodeOptions{},
	)

	if err := client.Run(ctx, func(ctx context.Context) error {
		return client.Auth().IfNecessary(ctx, flow)
	}); err != nil {
		t.Fatalf("login flow: %v", err)
	}

	// Assert user persisted.
	u, ok, err := st.UserByPhone(ctx, phone)
	if err != nil || !ok {
		t.Fatalf("user not persisted: ok=%v err=%v", ok, err)
	}
	if u.Phone != store.NormalizePhone(phone) {
		t.Errorf("phone = %q, want %q", u.Phone, store.NormalizePhone(phone))
	}
}

type codeSink struct {
	mu sync.Mutex
	ch chan string
}

func newCodeSink() *codeSink { return &codeSink{ch: make(chan string, 1)} }

func (c *codeSink) wait(ctx context.Context) (string, error) {
	select {
	case code := <-c.ch:
		return code, nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (c *codeSink) Logger() *slog.Logger { return slog.New(c) }

// Enabled reports whether the handler handles records at the given level.
func (c *codeSink) Enabled(context.Context, slog.Level) bool { return true }

// WithAttrs returns the handler unchanged; attributes are not accumulated.
func (c *codeSink) WithAttrs([]slog.Attr) slog.Handler { return c }

// WithGroup returns the handler unchanged; groups are not tracked.
func (c *codeSink) WithGroup(string) slog.Handler { return c }

// Handle captures the "code" attribute from a log record and forwards it.
func (c *codeSink) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "code" {
			select {
			case c.ch <- a.Value.String():
			default:
			}
			return false
		}
		return true
	})
	return nil
}
