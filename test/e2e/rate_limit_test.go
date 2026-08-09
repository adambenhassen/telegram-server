package e2e_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestRateLimitE2E proves the rate limiter works end-to-end against a real
// gotd client: with a small configured limit, the over-limit send fails with
// a flood-wait error the client surfaces, a different account is unaffected,
// and the limited account succeeds after the wait.
func TestRateLimitE2E(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey())
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

	// Boot server with a small rate limit: 3 sends per 10s.
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	rateLimits := config.RateLimitsConfig{
		MessageSend: store.RateLimitConfig{Limit: 3, Window: 10 * time.Second},
	}
	handler := api.New(st, dcID, tgcfg, codes.Logger(), true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), rateLimits)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, codes.Logger())

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, transport.Listen(ln)) }()
	t.Cleanup(func() {
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
	})

	newClient := func() *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:         dcID,
			DCList:     dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
			PublicKeys: []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:   dcs.Plain(dcs.PlainOptions{}),
		})
	}
	flowFor := func(phone string) auth.Flow {
		return auth.NewFlow(
			auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
				func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
					return codes.wait(ctx, phone)
				})),
			auth.SendCodeOptions{},
		)
	}

	const phoneA, phoneB = "+15551295001", "+15551295002"

	// Start A in interactive mode.
	aClient := newClient()
	aCmds := make(chan command)
	aErr := make(chan error, 1)
	aID := make(chan int64, 1)
	go func() { aErr <- runInteractive(ctx, aClient, flowFor(phoneA), aID, aCmds) }()

	var aUserID int64
	select {
	case aUserID = <-aID:
	case <-ctx.Done():
		t.Fatalf("A login timeout: %v", ctx.Err())
	case err := <-aErr:
		t.Fatalf("A interactive: %v", err)
	}

	// Start B in interactive mode.
	bClient := newClient()
	bCmds := make(chan command)
	bErr := make(chan error, 1)
	bID := make(chan int64, 1)
	go func() { bErr <- runInteractive(ctx, bClient, flowFor(phoneB), bID, bCmds) }()

	var bUserID int64
	select {
	case bUserID = <-bID:
	case <-ctx.Done():
		t.Fatalf("B login timeout: %v", ctx.Err())
	case err := <-bErr:
		t.Fatalf("B interactive: %v", err)
	}

	execA := func(fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case aCmds <- command{fn: fn, done: d}:
		case <-ctx.Done():
			t.Fatalf("A command enqueue timeout: %v", ctx.Err())
		}
		return <-d
	}

	execB := func(fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case bCmds <- command{fn: fn, done: d}:
		case <-ctx.Done():
			t.Fatalf("B command enqueue timeout: %v", ctx.Err())
		}
		return <-d
	}

	// A sends 3 messages (the limit).
	for i := range 3 {
		if err := execA(func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer:     peerUser(aUserID, bUserID),
				Message:  "msg",
				RandomID: int64(i + 1),
			})
			return err
		}); err != nil {
			t.Fatalf("A send %d: %v", i+1, err)
		}
	}

	// A's 4th send should be denied with FLOOD_WAIT.
	err = execA(func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "blocked",
			RandomID: 4,
		})
		return err
	})
	if err == nil {
		t.Fatal("A send 4: expected FLOOD_WAIT error, got nil")
	}
	var rpcErr *tgerr.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != 420 {
		t.Fatalf("A send 4: expected 420 FLOOD_WAIT, got %v", err)
	}

	// B is unaffected — can still send.
	if err := execB(func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(bUserID, aUserID),
			Message:  "b sends",
			RandomID: 100,
		})
		return err
	}); err != nil {
		t.Fatalf("B send: %v", err)
	}

	// Wait for the window to expire.
	time.Sleep(11 * time.Second)

	// A should be allowed to send again.
	if err := execA(func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerUser(aUserID, bUserID),
			Message:  "after wait",
			RandomID: 5,
		})
		return err
	}); err != nil {
		t.Fatalf("A post-wait send: %v", err)
	}

	close(aCmds)
	close(bCmds)
	if err := <-aErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("A run: %v", err)
	}
	if err := <-bErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("B run: %v", err)
	}
}
