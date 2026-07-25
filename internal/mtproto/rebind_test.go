package mtproto_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// bindingKeyStore is an AuthKeyStore whose key/user binding changes from one
// lookup to the next, standing in for an auth.signIn that rebinds the key (or a
// 2FA sign-in that unbinds it) while the connection that established it is
// still live. The key itself always exists, so the revoked-key path is not
// involved. Lookups happen only on the single serve goroutine, so no locking.
type bindingKeyStore struct {
	key crypto.AuthKey
	// users is the bound user each successive lookup reports; 0 means the key
	// exists but is bound to nobody. The last entry repeats if lookups outrun it.
	users []int64
	n     int
}

func (s *bindingKeyStore) Save(context.Context, crypto.AuthKey) error { return nil }
func (s *bindingKeyStore) Touch(context.Context, [8]byte) error       { return nil }

func (s *bindingKeyStore) Get(context.Context, [8]byte) (crypto.AuthKey, int64, bool, error) {
	userID := s.users[min(s.n, len(s.users)-1)]
	s.n++
	return s.key, userID, true, nil
}

// frameConn replays pre-built client frames, calling before with the number of
// frames already served so a test can inspect the registry between frames, and
// reporting io.EOF once the script runs out.
type frameConn struct {
	frames [][]byte
	i      int
	before func(served int)
}

func (c *frameConn) Recv(_ context.Context, b *bin.Buffer) error {
	c.before(c.i)
	if c.i >= len(c.frames) {
		return io.EOF
	}
	b.ResetTo(slices.Clone(c.frames[c.i]))
	c.i++
	return nil
}

func (c *frameConn) Send(context.Context, *bin.Buffer) error { return nil }
func (c *frameConn) Close() error                            { return nil }

// clientFrame encrypts body the way a real client puts it on the wire, so the
// server's own cipher accepts it.
func clientFrame(t *testing.T, key crypto.AuthKey, sessionID, msgID int64, body bin.Encoder) []byte {
	t.Helper()
	var b bin.Buffer
	if err := body.Encode(&b); err != nil {
		t.Fatalf("encode body: %v", err)
	}
	data := crypto.EncryptedMessageData{
		SessionID:              sessionID,
		MessageID:              msgID,
		MessageDataLen:         int32(b.Len()), //nolint:gosec // ping request, far below MaxInt32
		MessageDataWithPadding: b.Copy(),
	}
	if err := crypto.NewClientCipher(crypto.DefaultRand()).Encrypt(key, data, &b); err != nil {
		t.Fatalf("encrypt frame: %v", err)
	}
	return b.Copy()
}

func rebindTestKey() crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i)
	}
	return raw.WithID()
}

// TestServeConnResyncsRegistryWithKeyBinding proves the serve loop keeps a
// connection's registry bucket in step with its auth key's current binding: a
// rebind moves the connection to the new user and an unbind takes it out
// entirely, both on the connection's next frame. Registering only once would
// leave the socket in the first user's bucket, still receiving that user's
// updates under a key someone else now holds.
func TestServeConnResyncsRegistryWithKeyBinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		// users is the key's bound user at each successive lookup, one per frame.
		users []int64
		// want[i] lists the users whose bucket must hold this connection once i
		// frames have been served.
		want [][]int64
	}{
		{
			name:  "first login registers once the key binds",
			users: []int64{0, 7},
			want:  [][]int64{{}, {}, {7}},
		},
		{
			name:  "rebind moves the conn to the new user",
			users: []int64{7, 9},
			want:  [][]int64{{}, {7}, {9}},
		},
		{
			name:  "unbind leaves the conn in no bucket",
			users: []int64{7, 0},
			want:  [][]int64{{}, {7}, {}},
		},
		{
			name:  "an unchanged binding does not register twice",
			users: []int64{7, 7, 7},
			want:  [][]int64{{}, {7}, {7}, {7}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			key := rebindTestKey()
			srv := mtproto.New(exchange.PrivateKey{}, 2, &bindingKeyStore{key: key, users: tt.users}, nil, nil)

			frames := make([][]byte, len(tt.users))
			for i := range frames {
				frames[i] = clientFrame(t, key, 42, int64(i+1)<<32, &mt.PingRequest{PingID: int64(i + 1)})
			}

			// Only one connection is ever served, so every populated bucket must
			// hold exactly that connection.
			var seen *mtproto.Conn
			check := func(served int) {
				if served >= len(tt.want) {
					t.Fatalf("served %d frames, want at most %d", served, len(tt.want)-1)
				}
				users := srv.Registry().Users()
				slices.Sort(users)
				if !slices.Equal(users, tt.want[served]) {
					t.Fatalf("after %d frames: registered users = %v, want %v", served, users, tt.want[served])
				}
				for _, u := range tt.want[served] {
					conns := srv.Registry().Conns(u)
					if len(conns) != 1 {
						t.Fatalf("after %d frames: user %d holds %d conns, want 1", served, u, len(conns))
					}
					if seen == nil {
						seen = conns[0]
					}
					if conns[0] != seen {
						t.Fatalf("after %d frames: user %d holds a different conn", served, u)
					}
					// The bucket alone does not gate delivery: the conn must also
					// answer to that user and refuse anyone else, which is what
					// stops a delivery holding a stale snapshot from writing.
					pushed, err := conns[0].PushTo(context.Background(), u, &mt.Pong{PingID: 1}, 0)
					if err != nil {
						t.Fatalf("after %d frames: push to user %d: %v", served, u, err)
					}
					if !pushed {
						t.Fatalf("after %d frames: conn in user %d's bucket refuses that user's push", served, u)
					}
					stranger, err := conns[0].PushTo(context.Background(), u+1000, &mt.Pong{PingID: 1}, 0)
					if err != nil {
						t.Fatalf("after %d frames: push to a stranger: %v", served, err)
					}
					if stranger {
						t.Fatalf("after %d frames: conn accepted a push for a user it does not belong to", served)
					}
				}
			}

			err := srv.ServeConn(context.Background(), &frameConn{frames: frames, before: check})
			if !errors.Is(err, io.EOF) {
				t.Fatalf("ServeConn = %v, want io.EOF once the script is exhausted", err)
			}
			// The deferred deregistration on loop exit still empties the registry.
			if users := srv.Registry().Users(); len(users) != 0 {
				t.Fatalf("after the conn ended, registered users = %v, want none", users)
			}
		})
	}
}
