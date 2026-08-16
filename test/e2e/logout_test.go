package e2e_test

import (
	"context"
	"fmt"
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

// TestClientLogOut proves auth.logOut de-authorizes the connection's auth key:
// (a) the auth_keys row for that key is deleted from the store, and (b) a fresh
// client reusing the same session (holding the now-deleted key) is no longer
// authorized, because the server rejects the unknown key and forces a
// re-handshake into an unbound key. Without the delete, the key would survive
// and the reconnecting client would still report Authorized (see restart test).
func TestClientLogOut(t *testing.T) {
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
	stop := bootServer(t, ctx, key, dcID, st, codes.Logger(), ln)
	t.Cleanup(stop)

	// A single session storage is shared across both client generations so the
	// second client reconnects with the same auth key rather than a new one.
	sess := &session.StorageMemory{}
	newClient := func() *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:             dcID,
			DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
			PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:       dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: sess,
		})
	}

	phone := "+15551237777"
	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return codes.wait(ctx)
			})),
		auth.SendCodeOptions{},
	)

	// Run #1: log in, capture the bound auth key id, then log out.
	client := newClient()
	var keyID int64
	if err := client.Run(ctx, func(ctx context.Context) error {
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return err
		}
		u, ok, err := st.UserByPhone(ctx, phone)
		if err != nil {
			return fmt.Errorf("user by phone: %w", err)
		}
		if !ok {
			return fmt.Errorf("user %s not persisted", phone)
		}
		keys, err := st.AuthKeysByUser(ctx, u.ID)
		if err != nil {
			return fmt.Errorf("auth keys by user: %w", err)
		}
		if len(keys) != 1 {
			return fmt.Errorf("want 1 bound auth key before logout, got %d", len(keys))
		}
		keyID = keys[0].ID
		if _, err := client.API().AuthLogOut(ctx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("login+logout: %v", err)
	}

	// (a) The auth_keys row for that key is gone.
	if _, ok, err := st.AuthKeyByID(ctx, keyID); err != nil {
		t.Fatalf("auth key by id: %v", err)
	} else if ok {
		t.Fatal("auth key still present after logout: DeleteAuthKey did not run")
	}

	// (b) A fresh client reusing the same (now-deleted) session key is no longer
	// authorized: the server rejects the unknown key, forcing a re-handshake into
	// an unbound key with no auth flow supplied.
	client2 := newClient()
	var status *auth.Status
	if err := client2.Run(ctx, func(ctx context.Context) error {
		s, serr := client2.Auth().Status(ctx)
		if serr != nil {
			return serr
		}
		status = s
		return nil
	}); err != nil {
		t.Fatalf("post-logout run: %v", err)
	}
	if status.Authorized {
		t.Fatal("client still authorized after logout: auth key was not de-authorized")
	}
}
