package mtproto_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// revokedKeyStore reports the key as bound to userID on the first lookup and
// absent on every lookup after it, standing in for auth.logOut deleting the row
// (or account.resetAuthorization) while the connection is still live.
type revokedKeyStore struct {
	key    crypto.AuthKey
	userID int64

	mu   sync.Mutex
	gets int
}

func (s *revokedKeyStore) Save(context.Context, crypto.AuthKey) error { return nil }
func (s *revokedKeyStore) Touch(context.Context, [8]byte) error       { return nil }

func (s *revokedKeyStore) Get(context.Context, [8]byte) (crypto.AuthKey, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if s.gets > 1 {
		return crypto.AuthKey{}, 0, false, nil
	}
	return s.key, s.userID, true, nil
}

func (s *revokedKeyStore) lookups() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

// TestHandshakeFrameDropsRegistration covers the one path that never re-reads
// the key's binding: a zero-auth-key-id frame goes straight to key exchange, so
// neither the resync nor the revoked-key check runs. A client that registers
// once and then sends nothing but handshakes would otherwise hold its place in
// the previous user's bucket for as long as it keeps renegotiating — outliving
// a rebind and even a logOut that deletes the key, and bounded by nothing. The
// conn must therefore leave the registry when it renegotiates and stay out.
func TestHandshakeFrameDropsRegistration(t *testing.T) {
	t.Parallel()

	rsaKey, err := rsa.GenerateKey(rand.Reader, crypto.RSAKeyBits)
	if err != nil {
		t.Fatalf("rsa key: %v", err)
	}
	priv := exchange.PrivateKey{RSA: rsaKey}

	key := rebindTestKey()
	store := &revokedKeyStore{key: key, userID: 7}
	srv := mtproto.New(priv, 2, store, nil, nil)

	client, server := transport.Intermediate.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	served := make(chan error, 1)
	go func() { served <- srv.ServeConn(ctx, server) }()

	// One encrypted frame under the live key registers the conn for user 7.
	frame := clientFrame(t, key, 42, int64(1)<<32, &mt.PingRequest{PingID: 1})
	b := &bin.Buffer{Buf: slices.Clone(frame)}
	if err := client.Send(ctx, b); err != nil {
		t.Fatalf("send ping: %v", err)
	}
	// The pipe is synchronous, so drain both server writes (new_session_created
	// and the pong) or the serve loop blocks before it reaches the resync.
	for i := range 2 {
		if err := client.Recv(ctx, b); err != nil {
			t.Fatalf("recv server frame %d: %v", i, err)
		}
	}
	select {
	case err := <-served:
		t.Fatalf("serveConn exited early: %v", err)
	default:
	}
	// The Add happens just after the pong write, so poll for it.
	var registered *mtproto.Conn
	for range 200 {
		if conns := srv.Registry().Conns(7); len(conns) == 1 {
			registered = conns[0]
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if registered == nil {
		t.Fatalf("after the ping: registered users = %v, want [7]", srv.Registry().Users())
	}

	// The key is now revoked in the store. Instead of another encrypted frame,
	// the client runs full key exchanges on the same socket: each one is a
	// zero-auth-key-id frame, so the serve loop never calls keys.Get again.
	for i := range 3 {
		if _, err := exchange.NewExchanger(client, 2).
			Client([]exchange.PublicKey{priv.Public()}).
			Run(ctx); err != nil {
			t.Fatalf("client exchange %d: %v", i, err)
		}
		if users := srv.Registry().Users(); len(users) != 0 {
			t.Fatalf("after handshake %d: registered users = %v, want none", i, users)
		}
	}

	if got := store.lookups(); got != 1 {
		t.Fatalf("keys.Get called %d times, want 1: the handshake path does not re-validate the key, which is why the conn must be dropped rather than re-checked", got)
	}
	// Leaving the bucket is not enough on its own: a delivery holding the conn
	// from an earlier snapshot must find it disowned too.
	pushed, err := registered.PushTo(ctx, 7, &mt.Pong{PingID: 2}, 0)
	if err != nil {
		t.Fatalf("push to the renegotiated conn: %v", err)
	}
	if pushed {
		t.Fatal("a renegotiated conn still accepts pushes for the user it was registered under")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	<-served
}
