package e2e_test

import (
	"context"
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

// TestSessionManagement is the Phase E gate: it proves account.getAuthorizations
// lists a user's sessions and account.resetAuthorization revokes one, scoped to
// the caller. The same user logs in on two independent auth keys (two clients,
// two session storages). Client1 sees both sessions with exactly one flagged
// Current (its own), resets client2's session by hash, and the reset removes
// client2's auth_keys row so client2 can no longer make authorized calls while
// client1 keeps working.
func TestSessionManagement(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}

	st, err := store.Open(ctx, pgtest.DSN(t))
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

	// Each client keeps its own session storage, so each performs its own key
	// exchange and ends up with a distinct auth key bound to the same user.
	sess1 := &session.StorageMemory{}
	sess2 := &session.StorageMemory{}
	newClient := func(sess *session.StorageMemory) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:             dcID,
			DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
			PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:       dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: sess,
		})
	}

	phone := "+15551235555"
	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return codes.wait(ctx)
			})),
		auth.SendCodeOptions{},
	)

	login := func(sess *session.StorageMemory) {
		c := newClient(sess)
		if err := c.Run(ctx, func(ctx context.Context) error {
			return c.Auth().IfNecessary(ctx, flow)
		}); err != nil {
			t.Fatalf("login: %v", err)
		}
	}

	// Client1 logs in; its auth key is the only one bound so far.
	login(sess1)
	u, ok, err := st.UserByPhone(ctx, phone)
	if err != nil || !ok {
		t.Fatalf("user not persisted: ok=%v err=%v", ok, err)
	}
	keys1, err := st.AuthKeysByUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("auth keys after login1: %v", err)
	}
	if len(keys1) != 1 {
		t.Fatalf("want 1 auth key after first login, got %d", len(keys1))
	}
	client1Key := keys1[0].ID

	// Client2 logs in as the same user on a second, independent auth key.
	login(sess2)
	keys2, err := st.AuthKeysByUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("auth keys after login2: %v", err)
	}
	if len(keys2) != 2 {
		t.Fatalf("want 2 auth keys after second login, got %d", len(keys2))
	}
	var client2Key int64
	for _, k := range keys2 {
		if k.ID != client1Key {
			client2Key = k.ID
		}
	}
	if client2Key == 0 {
		t.Fatal("could not identify client2's auth key")
	}

	// From client1: list authorizations (both) and reset client2's session.
	client1 := newClient(sess1)
	if err := client1.Run(ctx, func(ctx context.Context) error {
		auths, err := client1.API().AccountGetAuthorizations(ctx)
		if err != nil {
			return err
		}
		if len(auths.Authorizations) != 2 {
			t.Fatalf("getAuthorizations returned %d sessions, want 2", len(auths.Authorizations))
		}
		var currents int
		for _, a := range auths.Authorizations {
			if a.Current {
				currents++
				if a.Hash != client1Key {
					t.Fatalf("Current session hash = %d, want client1 key %d", a.Hash, client1Key)
				}
			}
		}
		if currents != 1 {
			t.Fatalf("want exactly 1 Current session, got %d", currents)
		}

		// Reset client2's session by its hash.
		okReset, err := client1.API().AccountResetAuthorization(ctx, client2Key)
		if err != nil {
			return err
		}
		if !okReset {
			t.Fatal("resetAuthorization returned false")
		}
		return nil
	}); err != nil {
		t.Fatalf("client1 session ops: %v", err)
	}

	// Client2's auth_keys row is gone.
	if _, ok, err := st.AuthKeyByID(ctx, client2Key); err != nil {
		t.Fatalf("auth key by id: %v", err)
	} else if ok {
		t.Fatal("client2 auth key still present after reset")
	}

	// Client2 can no longer make authorized calls: reconnecting with its now
	// deleted key forces a re-handshake into an unbound key, so it is not
	// authorized (no auth flow is supplied here).
	client2 := newClient(sess2)
	if err := client2.Run(ctx, func(ctx context.Context) error {
		s, serr := client2.Auth().Status(ctx)
		if serr != nil {
			return serr
		}
		if s.Authorized {
			t.Fatal("client2 still authorized after its session was reset")
		}
		return nil
	}); err != nil {
		t.Fatalf("client2 post-reset run: %v", err)
	}

	// Client1 still works: its session survives and remains authorized.
	client1b := newClient(sess1)
	if err := client1b.Run(ctx, func(ctx context.Context) error {
		auths, err := client1b.API().AccountGetAuthorizations(ctx)
		if err != nil {
			return err
		}
		if len(auths.Authorizations) != 1 {
			t.Fatalf("after reset client1 sees %d sessions, want 1", len(auths.Authorizations))
		}
		if auths.Authorizations[0].Hash != client1Key {
			t.Fatalf("remaining session hash = %d, want client1 key %d", auths.Authorizations[0].Hash, client1Key)
		}
		return nil
	}); err != nil {
		t.Fatalf("client1 still-works run: %v", err)
	}
}
