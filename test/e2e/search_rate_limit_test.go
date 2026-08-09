package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
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
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestSearchRateLimitE2E proves that a real gotd client with a small
// configured limit gets a flood-wait on the over-limit search and gets
// results again after the window.
func TestSearchRateLimitE2E(t *testing.T) {
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

	// Boot server with small search rate limits. The two surfaces get
	// deliberately different limits and windows: identical settings would let a
	// transposed assignment in api.New pass, and each surface is driven below.
	stop := bootServerWithSearchLimits(t, ctx, key, dcID, st, dsn, codes.Logger(), ln,
		store.RateLimitConfig{Limit: 3, Window: 2 * time.Second},
		store.RateLimitConfig{Limit: 2, Window: 30 * time.Second},
	)
	t.Cleanup(stop)

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

	const phoneA = "+15551297001"
	const phoneB = "+15551297002"

	clientA, clientB := newClient(), newClient()
	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	for i, ch := range []chan int64{aID, bID} {
		select {
		case id := <-ch:
			switch i {
			case 0:
				aUserID = id
			case 1:
				bUserID = id
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("client login timeout (user %d)", i)
		}
	}

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}

	peerB := peerUser(aUserID, bUserID)

	// A sends a message to B so there is something to search.
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerB,
			Message:  "hello world",
			RandomID: 1,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}

	// A searches 3 times — should all succeed (limit is 3).
	for i := range 3 {
		var msgs []*tg.Message
		if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
				Peer:     peerB,
				Q:        "hello",
				Filter:   &tg.InputMessagesFilterEmpty{},
				OffsetID: 0,
				Limit:    10,
			})
			if err != nil {
				return err
			}
			m, ok := res.(*tg.MessagesMessages)
			if !ok {
				return errors.New("unexpected result type")
			}
			msgs = make([]*tg.Message, 0, len(m.Messages))
			for _, msg := range m.Messages {
				if mm, ok := msg.(*tg.Message); ok {
					msgs = append(msgs, mm)
				}
			}
			return nil
		}); err != nil {
			t.Fatalf("A search %d: %v", i+1, err)
		}
		if len(msgs) != 1 {
			t.Fatalf("A search %d: got %d messages, want 1", i+1, len(msgs))
		}
	}

	// A's 4th search should be denied with FLOOD_WAIT.
	var searchErr error
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		_, searchErr = c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:     peerB,
			Q:        "hello",
			Filter:   &tg.InputMessagesFilterEmpty{},
			OffsetID: 0,
			Limit:    10,
		})
		return nil
	}); err != nil {
		t.Fatalf("A search 4 run: %v", err)
	}
	if searchErr == nil {
		t.Fatal("A search 4: expected FLOOD_WAIT, got nil")
	}
	var rpcErr *tgerr.Error
	if !errors.As(searchErr, &rpcErr) {
		t.Fatalf("A search 4: expected RPC error, got %T: %v", searchErr, searchErr)
	}
	if rpcErr.Code != 420 {
		t.Fatalf("A search 4: code = %d, want 420", rpcErr.Code)
	}

	// Wait for the window to expire.
	time.Sleep(2500 * time.Millisecond)

	// A should be able to search again after the window.
	var msgsAfter []*tg.Message
	if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:     peerB,
			Q:        "hello",
			Filter:   &tg.InputMessagesFilterEmpty{},
			OffsetID: 0,
			Limit:    10,
		})
		if err != nil {
			return err
		}
		m, ok := res.(*tg.MessagesMessages)
		if !ok {
			return errors.New("unexpected result type")
		}
		msgsAfter = make([]*tg.Message, 0, len(m.Messages))
		for _, msg := range m.Messages {
			if mm, ok := msg.(*tg.Message); ok {
				msgsAfter = append(msgsAfter, mm)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("A search after window: %v", err)
	}
	if len(msgsAfter) != 1 {
		t.Fatalf("A search after window: got %d messages, want 1", len(msgsAfter))
	}

	// B's searches should be unaffected (independent quota).
	var msgsB []*tg.Message
	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSearch(ctx, &tg.MessagesSearchRequest{
			Peer:     peerUser(bUserID, aUserID),
			Q:        "hello",
			Filter:   &tg.InputMessagesFilterEmpty{},
			OffsetID: 0,
			Limit:    10,
		})
		if err != nil {
			return err
		}
		m, ok := res.(*tg.MessagesMessages)
		if !ok {
			return errors.New("unexpected result type")
		}
		msgsB = make([]*tg.Message, 0, len(m.Messages))
		for _, msg := range m.Messages {
			if mm, ok := msg.(*tg.Message); ok {
				msgsB = append(msgsB, mm)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("B search: %v", err)
	}
	if len(msgsB) != 1 {
		t.Fatalf("B search: got %d messages, want 1", len(msgsB))
	}

	// contacts.search carries its own quota, 2 per 30s. There is no RPC to set
	// a display name, so these queries match nothing — which is the point: the
	// quota is charged on a miss exactly as on a hit, so it cannot be read as
	// an existence oracle.
	contactsSearch := func() error {
		return exec(aCmds, func(ctx context.Context, c *tg.Client) error {
			found, cerr := c.ContactsSearch(ctx, &tg.ContactsSearchRequest{Q: "bravo", Limit: 10})
			if cerr != nil {
				return cerr
			}
			if len(found.MyResults) != 0 {
				return errors.New("unexpected contacts match")
			}
			return nil
		})
	}
	for i := range 2 {
		if err := contactsSearch(); err != nil {
			t.Fatalf("A contacts search %d: %v", i+1, err)
		}
	}

	// A's 3rd contacts search should be denied, while the messages quota it
	// already refilled stays untouched.
	err = contactsSearch()
	if err == nil {
		t.Fatal("A contacts search 3: expected FLOOD_WAIT, got nil")
	}
	if !errors.As(err, &rpcErr) {
		t.Fatalf("A contacts search 3: expected RPC error, got %T: %v", err, err)
	}
	if rpcErr.Code != 420 {
		t.Fatalf("A contacts search 3: code = %d, want 420", rpcErr.Code)
	}

	// B's contacts quota is independent and still spends.
	if err := exec(bCmds, func(ctx context.Context, c *tg.Client) error {
		_, cerr := c.ContactsSearch(ctx, &tg.ContactsSearchRequest{Q: "bravo", Limit: 10})
		return cerr
	}); err != nil {
		t.Fatalf("B contacts search: %v", err)
	}

	close(aCmds)
	close(bCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}

// bootServerWithSearchLimits is like bootServerWithDelivery but with custom
// rate limits for messages.search and contacts.search.
func bootServerWithSearchLimits(t *testing.T, ctx context.Context, key *rsa.PrivateKey, dcID int, st *store.Store, dsn string, log *slog.Logger, ln net.Listener, searchMessages, searchContacts store.RateLimitConfig) func() {
	t.Helper()
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	handler := api.New(st, dcID, tgcfg, log, true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{
		SearchMessages: searchMessages,
		SearchContacts: searchContacts,
	})
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, log)

	updater := api.NewUpdater(st, server.Registry(), log, pgtest.PeerDeriver())
	_, stopListener, err := store.StartListener(ctx, dsn, updater.Deliver, updater.DeliverTyping, updater.Evict, updater.DeliverChannelPost, updater.DeliverEncryption, updater.DeliverStatus, updater.DeliverEncryptedMsg, updater.DeliverReactions, updater.DeliverPinned, log)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}

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
		if lerr := stopListener(); lerr != nil {
			t.Errorf("listener stop: %v", lerr)
		}
	}
}
