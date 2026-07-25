package e2e_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// waitNoConn waits for replica's registry to hold no connection for userID.
func waitNoConn(t *testing.T, reg *mtproto.SessionRegistry, userID int64, within time.Duration, what string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if len(reg.Conns(userID)) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: replica still holds %d conn(s) for user %d after %s",
				what, len(reg.Conns(userID)), userID, within)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestEvictRevokedSessionAcrossReplicas proves session revocation crosses
// processes without waiting on the revoked client. A logs in on replica 1 and
// then sends nothing at all, so nothing on that socket ever re-reads its auth
// key binding. A second session of the same user resets A's authorization on
// replica 2, which deletes the auth_keys row on the shared database and emits
// the evict NOTIFY. Replica 1 must close A's socket on that signal alone, and B's
// message must not reach it.
//
// The 3s bound is what makes this fail without the evict path: A's socket would
// otherwise keep the deleted key — and keep receiving message bodies — until it
// sent a frame or the 30s read timeout expired.
func TestEvictRevokedSessionAcrossReplicas(t *testing.T) {
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
	ln1 := mustListen(t, ctx, "127.0.0.1:0")
	ln2 := mustListen(t, ctx, "127.0.0.1:0")
	port1 := tcpPort(t, ln1)
	port2 := tcpPort(t, ln2)
	reg1, stop1 := bootServerWithRegistry(t, ctx, key, dcID, st, dsn, codes.Logger(), ln1)
	t.Cleanup(stop1)
	t.Cleanup(bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln2))

	newClient := func(port int, collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
			PublicKeys:    []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:      dcs.Plain(dcs.PlainOptions{}),
			UpdateHandler: collector,
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

	// A logs in on replica 1 and then goes silent: its command channel is never
	// fed, so the socket sends no further frame and never re-reads its binding.
	collA := newUpdateCollector()
	aCmds := make(chan command)
	aID := make(chan int64, 1)
	errA := make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, newClient(port1, collA), flowFor(phoneA), aID, aCmds)
	}()
	var aUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("client A login timeout")
	}

	// Baseline: replica 1 really holds A's socket, so a later empty bucket is the
	// evict and not a socket that was never registered.
	registered := false
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if len(reg1.Conns(aUserID)) > 0 {
			registered = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !registered {
		t.Fatal("replica 1 never registered A's connection")
	}

	keys, err := st.AuthKeysByUser(ctx, aUserID)
	if err != nil {
		t.Fatalf("auth keys for A: %v", err)
	}
	if len(keys) != 1 {
		t.Fatalf("want 1 auth key for A before the second session, got %d", len(keys))
	}
	aKeyID := keys[0].ID

	// A second session of the same user, on replica 2, revokes A's key.
	revoker := newClient(port2, newUpdateCollector())
	if err := revoker.Run(ctx, func(ctx context.Context) error {
		if err := revoker.Auth().IfNecessary(ctx, flowFor(phoneA)); err != nil {
			return err
		}
		ok, err := revoker.API().AccountResetAuthorization(ctx, aKeyID)
		if err != nil {
			return err
		}
		if !ok {
			t.Error("resetAuthorization returned false")
		}
		return nil
	}); err != nil {
		t.Fatalf("second session reset: %v", err)
	}

	// The evict crosses to replica 1 on the NOTIFY alone: A sent nothing.
	waitNoConn(t, reg1, aUserID, 3*time.Second, "after the cross-replica reset")

	// B's message must not reach the revoked socket.
	bClient := newClient(port2, newUpdateCollector())
	if err := bClient.Run(ctx, func(ctx context.Context) error {
		if err := bClient.Auth().IfNecessary(ctx, flowFor(phoneB)); err != nil {
			return err
		}
		_, err := bClient.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     &tg.InputPeerUser{UserID: aUserID, AccessHash: aUserID},
			Message:  "after revocation",
			RandomID: 30001,
		})
		return err
	}); err != nil {
		t.Fatalf("B login+send: %v", err)
	}

	select {
	case got := <-collA.newMsg:
		t.Fatalf("revoked session received %q", got.Message)
	case <-time.After(3 * time.Second):
	}
	// A reconnect after the evict re-handshakes into an unbound key, so the
	// bucket must stay empty rather than filling again.
	if conns := reg1.Conns(aUserID); len(conns) != 0 {
		t.Fatalf("replica 1 holds %d conn(s) for A again after the evict", len(conns))
	}

	close(aCmds)
	// A's transport was closed under it on purpose, so its run may end in any
	// transport error; the assertions above are what this test proves.
	if err := <-errA; err != nil {
		t.Logf("client A run ended with: %v", err)
	}
}

// TestLogOutEvictsOnlyBoundKeys covers both halves of the auth.logOut emitter
// against a raw LISTEN connection: an authorized logOut announces its own key so
// every replica drops the sockets holding it, while a logOut on an unbound key
// still succeeds and announces nothing — that key is registered under nobody, and
// an evict naming user 0 would be a signal no replica can act on.
func TestLogOutEvictsOnlyBoundKeys(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	port := tcpPort(t, ln)
	t.Cleanup(bootServer(t, ctx, key, dcID, st, codes.Logger(), ln))

	// A raw listener observes the channel directly, so the assertions do not
	// depend on any server-side eviction having run.
	lconn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("listener connect: %v", err)
	}
	t.Cleanup(func() { _ = lconn.Close(context.Background()) }) //nolint:errcheck // best-effort close
	if _, err := lconn.Exec(ctx, "LISTEN "+store.ChannelEvict); err != nil {
		t.Fatalf("listen: %v", err)
	}

	newClient := func() *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:         dcID,
			DCList:     dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
			PublicKeys: []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:   dcs.Plain(dcs.PlainOptions{}),
		})
	}

	// An unauthorized client handshakes into an unbound key and logs out.
	unbound := newClient()
	if err := unbound.Run(ctx, func(ctx context.Context) error {
		_, err := unbound.API().AuthLogOut(ctx)
		return err
	}); err != nil {
		t.Fatalf("logout on an unbound key: %v", err)
	}
	quietCtx, quietCancel := context.WithTimeout(ctx, 2*time.Second)
	defer quietCancel()
	if n, err := lconn.WaitForNotification(quietCtx); err == nil {
		t.Fatalf("unbound logout emitted an evict: %q", n.Payload)
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait for quiet: %v", err)
	}

	// An authorized client's logOut announces its own key.
	const phone = "+15551295003"
	authed := newClient()
	var keyID, userID int64
	if err := authed.Run(ctx, func(ctx context.Context) error {
		flow := auth.NewFlow(
			auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
				func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
					return codes.wait(ctx, phone)
				})),
			auth.SendCodeOptions{},
		)
		if err := authed.Auth().IfNecessary(ctx, flow); err != nil {
			return err
		}
		self, err := authed.Self(ctx)
		if err != nil {
			return err
		}
		userID = self.ID
		keys, err := st.AuthKeysByUser(ctx, userID)
		if err != nil {
			return err
		}
		if len(keys) != 1 {
			t.Errorf("want 1 auth key before logout, got %d", len(keys))
		}
		keyID = keys[0].ID
		_, err = authed.API().AuthLogOut(ctx)
		return err
	}); err != nil {
		t.Fatalf("login+logout: %v", err)
	}

	evictCtx, evictCancel := context.WithTimeout(ctx, 5*time.Second)
	defer evictCancel()
	n, err := lconn.WaitForNotification(evictCtx)
	if err != nil {
		t.Fatalf("authorized logout emitted no evict: %v", err)
	}
	if want := store.EvictPayload(userID, keyID); n.Payload != want {
		t.Fatalf("evict payload = %q, want %q", n.Payload, want)
	}
}
