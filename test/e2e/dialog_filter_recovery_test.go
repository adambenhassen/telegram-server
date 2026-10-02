package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestDialogFilterRecoveryAcrossReplicasAfterListenerReconnect(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatalf("load server key: %v", err)
	}
	dsn := pgtest.DSN(t)
	storeA, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open replica A store: %v", err)
	}
	t.Cleanup(func() {
		if err := storeA.Close(); err != nil {
			t.Errorf("close replica A store: %v", err)
		}
	})
	storeB, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open replica B store: %v", err)
	}
	t.Cleanup(func() {
		if err := storeB.Close(); err != nil {
			t.Errorf("close replica B store: %v", err)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	listenerA := mustListen(t, ctx, "127.0.0.1:0")
	listenerB := mustListen(t, ctx, "127.0.0.1:0")
	replicaA := startDialogFilterReplica(t, ctx, key, dcID, storeA, dsn, listenerA, codes.Logger())
	replicaB := startDialogFilterReplica(t, ctx, key, dcID, storeB, dsn, listenerB, codes.Logger())
	if err := store.WaitForNotificationListener(ctx, storeA, 2); err != nil {
		t.Fatalf("wait for both replica listeners: %v", err)
	}

	const phone = "+15551289901"
	seedPhoneUsers(t, ctx, storeA, phone)
	collectorA, collectorB := newUpdateCollector(), newUpdateCollector()
	startClient := func(port int, collector *updateCollector, label string) (int64, chan command) {
		t.Helper()
		client := telegram.NewClient(1, "hash", telegram.Options{
			DC: dcID,
			DCList: dcs.List{Options: []tg.DCOption{{
				ID: dcID, IPAddress: "127.0.0.1", Port: port,
			}}},
			PublicKeys:    []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:      dcs.Plain(dcs.PlainOptions{}),
			UpdateHandler: collector,
		})
		cmds := make(chan command)
		ids := make(chan int64, 1)
		done := make(chan error, 1)
		flow := auth.NewFlow(auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return codes.wait(ctx, phone)
			})), auth.SendCodeOptions{})
		go func() { done <- runInteractive(ctx, client, flow, ids, cmds) }()
		t.Cleanup(func() {
			close(cmds)
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("%s client: %v", label, err)
				}
			case <-time.After(5 * time.Second):
				t.Errorf("%s client did not stop", label)
			}
		})
		return recvOrCtx(t, ctx, ids, label+" login"), cmds
	}
	ownerA, cmdsA := startClient(tcpPort(t, listenerA), collectorA, "replica A")
	ownerB, cmdsB := startClient(tcpPort(t, listenerB), collectorB, "replica B")
	if ownerB != ownerA {
		t.Fatalf("second authorized session owner = %d, want %d", ownerB, ownerA)
	}
	if got := len(replicaA.registry.Conns(ownerA)); got != 1 {
		t.Fatalf("replica A registered %d owner connections after login, want one", got)
	}

	exec := func(cmds chan command, name string, fn func(context.Context, *tg.Client) error) {
		t.Helper()
		done := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: done}:
		case <-ctx.Done():
			t.Fatalf("%s enqueue: %v", name, ctx.Err())
		}
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-ctx.Done():
			t.Fatalf("%s: %v", name, ctx.Err())
		}
	}

	var initial *tg.MessagesDialogFilters
	exec(cmdsA, "initial authorized folder fetch", func(ctx context.Context, client *tg.Client) error {
		var err error
		initial, err = client.MessagesGetDialogFilters(ctx)
		return err
	})
	if initial.TagsEnabled || len(initial.Filters) != 2 {
		t.Fatalf("initial folder fetch = %d filters, tags enabled %v; want All chats and Unread", len(initial.Filters), initial.TagsEnabled)
	}
	if _, ok := initial.Filters[0].(*tg.DialogFilterDefault); !ok {
		t.Fatalf("initial folder = %T, want All chats", initial.Filters[0])
	}
	if unread, ok := initial.Filters[1].(*tg.DialogFilter); !ok || unread.ID != 2 || unread.Title.Text != "Unread" {
		t.Fatalf("initial second folder = %#v, want Unread", initial.Filters[1])
	}
	seedUpdate := recvOrCtx(t, ctx, collectorB.dialogFilter, "replica B seeded folder update")
	seedFilter, ok := seedUpdate.GetFilter()
	if folder, folderOK := seedFilter.(*tg.DialogFilter); !ok || !folderOK || seedUpdate.ID != 2 || folder.Title.Text != "Unread" || !folder.ExcludeRead {
		t.Fatalf("replica B seed update = %#v, want updateDialogFilter for Unread", seedUpdate)
	}
	select {
	case unexpected := <-collectorA.dialogFilters:
		t.Fatalf("unexpected folder invalidation before replica B's commit: %#v", unexpected)
	default:
	}

	terminateListenBackends(t, dsn)
	filter := &tg.DialogFilter{ID: 2, Title: tg.TextWithEntities{Text: "Recovered"}, Groups: true}
	filter.SetFlags()
	upsert := &tg.MessagesUpdateDialogFilterRequest{ID: 2, Filter: filter}
	upsert.SetFlags()
	exec(cmdsB, "replica B folder commit", func(ctx context.Context, client *tg.Client) error {
		ok, err := client.MessagesUpdateDialogFilter(ctx, upsert)
		if err == nil && !ok {
			return errors.New("folder commit returned false")
		}
		return err
	})
	recvOrCtx(t, ctx, replicaA.reconnected, "replica A listener reconnect")
	recvOrCtx(t, ctx, replicaB.reconnected, "replica B listener reconnect")
	if err := store.WaitForNotificationListener(ctx, storeA, 2); err != nil {
		t.Fatalf("wait for reconnected replica listeners: %v", err)
	}
	recvOrCtx(t, ctx, collectorA.dialogFilters, "client-visible folder recovery update")

	var recovered *tg.MessagesDialogFilters
	exec(cmdsA, "authorized folder fetch after recovery", func(ctx context.Context, client *tg.Client) error {
		var err error
		recovered, err = client.MessagesGetDialogFilters(ctx)
		return err
	})
	if recovered.TagsEnabled || len(recovered.Filters) != 2 {
		t.Fatalf("recovered folder fetch = %d filters, tags enabled %v; want All chats and ID 2", len(recovered.Filters), recovered.TagsEnabled)
	}
	folder, ok := recovered.Filters[1].(*tg.DialogFilter)
	if !ok || folder.ID != 2 || folder.Title.Text != "Recovered" || !folder.Groups {
		t.Fatalf("client fetched folder = %#v, want persisted ID 2 Recovered with Groups", recovered.Filters[1])
	}
}

type dialogFilterReplica struct {
	registry     *mtproto.SessionRegistry
	reconnected  chan struct{}
	serverCancel context.CancelFunc
	serveErr     chan error
	stopListener func() error
	stopRecovery func()
	stopOnce     sync.Once
}

func startDialogFilterReplica(
	t *testing.T,
	ctx context.Context,
	key *rsa.PrivateKey,
	dcID int,
	st *store.Store,
	dsn string,
	ln net.Listener,
	log *slog.Logger,
) *dialogFilterReplica {
	t.Helper()
	dialogFilterSync := api.NewDialogFilterSync()
	handler := api.NewWithDialogFilterSync(
		st, dcID, fixtureConfigForListener(t, dcID, ln), log, true,
		100<<20, testBlobs(t), 2<<30, pgtest.PeerDeriver(),
		config.RateLimitsConfig{}, config.RegistrationClosed, dialogFilterSync,
	)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, log)
	updater := api.NewUpdaterWithDialogFilterSync(st, server.Registry(), log, pgtest.PeerDeriver(), dialogFilterSync)
	replica := &dialogFilterReplica{
		registry:    server.Registry(),
		reconnected: make(chan struct{}, 8),
		serveErr:    make(chan error, 1),
	}
	_, stopListener, err := store.StartListenerWithDialogFilters(
		ctx, dsn, updater.Deliver, updater.DeliverTyping, updater.Evict,
		updater.DeliverChannelPost, updater.DeliverEncryption, updater.DeliverStatus,
		updater.DeliverEncryptedMsg, updater.DeliverReactions, updater.DeliverPinned,
		updater.MarkDialogFilters,
		updater.DeliverSeededDialogFilter,
		func() {
			updater.DialogFilterListenerReconnected()
			select {
			case replica.reconnected <- struct{}{}:
			default:
			}
		},
		log,
	)
	if err != nil {
		t.Fatalf("start dialog-filter listener: %v", err)
	}
	replica.stopListener = stopListener
	replica.stopRecovery = updater.StartDialogFilterRecovery(ctx)
	serverCtx, serverCancel := context.WithCancel(ctx)
	replica.serverCancel = serverCancel
	go func() { replica.serveErr <- server.Serve(serverCtx, ln) }()
	t.Cleanup(func() { replica.stop(t) })
	return replica
}

func (r *dialogFilterReplica) stop(t *testing.T) {
	t.Helper()
	r.stopOnce.Do(func() {
		r.serverCancel()
		if err := <-r.serveErr; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("replica server: %v", err)
		}
		if err := r.stopListener(); err != nil {
			t.Errorf("replica listener: %v", err)
		}
		r.stopRecovery()
	})
}
