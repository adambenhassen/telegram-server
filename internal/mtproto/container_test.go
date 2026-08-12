package mtproto_test

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// containerFrame encrypts a MessageContainer with N ping requests, the way a
// real client puts it on the wire.
func containerFrame(t *testing.T, key crypto.AuthKey, sessionID, msgID int64, n int) []byte {
	t.Helper()
	msgs := make([]proto.Message, n)
	for i := range msgs {
		var b bin.Buffer
		if err := (&mt.PingRequest{PingID: int64(i + 1)}).Encode(&b); err != nil {
			t.Fatalf("encode ping %d: %v", i, err)
		}
		msgs[i] = proto.Message{
			ID:    msgID + int64(i),
			SeqNo: i + 1,
			Bytes: b.Len(),
			Body:  b.Copy(),
		}
	}
	container := proto.MessageContainer{Messages: msgs}
	var b bin.Buffer
	if err := container.Encode(&b); err != nil {
		t.Fatalf("encode container: %v", err)
	}
	data := crypto.EncryptedMessageData{
		SessionID:              sessionID,
		MessageID:              msgID,
		MessageDataLen:         int32(b.Len()), //nolint:gosec // test data, far below MaxInt32
		MessageDataWithPadding: b.Copy(),
	}
	if err := crypto.NewClientCipher(crypto.DefaultRand()).Encrypt(key, data, &b); err != nil {
		t.Fatalf("encrypt frame: %v", err)
	}
	return b.Copy()
}

// scriptedConn reads scripted frames and delegates sends to an injectable sendFn,
// so a test can control whether writes succeed or fail.
type scriptedConn struct {
	frames [][]byte
	i      int
	sendFn func() error
}

func (c *scriptedConn) Recv(_ context.Context, b *bin.Buffer) error {
	if c.i >= len(c.frames) {
		return io.EOF
	}
	b.ResetTo(c.frames[c.i])
	c.i++
	return nil
}

func (c *scriptedConn) Send(_ context.Context, _ *bin.Buffer) error {
	return c.sendFn()
}

func (c *scriptedConn) Close() error { return nil }

// TestContainerAbortsOnWriteError verifies that a container frame whose first
// message fails to write aborts the container loop instead of continuing with
// the remaining messages. Without this fix, a single frame can hold a socket
// for N write deadlines when the peer has stopped reading.
func TestContainerAbortsOnWriteError(t *testing.T) {
	t.Parallel()

	key := rebindTestKey()
	store := mtproto.NewMemoryAuthKeyStore()
	if err := store.Save(context.Background(), key); err != nil {
		t.Fatalf("save key: %v", err)
	}
	srv := mtproto.New(exchange.PrivateKey{}, 2, store, nil, nil)

	// Three pings in one container.
	const n = 3
	frame := containerFrame(t, key, 42, int64(1)<<32, n)

	// The first send (new_session_created) succeeds so the container dispatch
	// actually runs; every subsequent send fails to simulate a peer that has
	// stopped reading.
	var callCount atomic.Int64
	conn := &scriptedConn{
		frames: [][]byte{frame},
		sendFn: func() error {
			n := callCount.Add(1)
			if n == 1 {
				return nil // new_session_created
			}
			return io.ErrClosedPipe
		},
	}

	err := srv.ServeConn(context.Background(), conn)
	if err == nil {
		t.Fatal("ServeConn returned nil, want error")
	}

	// With the fix: the container loop aborts after the first pong write fails.
	// Sends: 1 (new_session_created) + 1 (first pong, fails) = 2 total.
	// Without the fix: the loop would continue and try N pong writes = N+1 total.
	calls := callCount.Load()
	if calls == int64(n+1) {
		t.Fatalf("Send called %d times (new_session_created + %d pongs): the container loop did not abort on write error", calls, n)
	}
	if calls != 2 {
		t.Fatalf("Send called %d times, want 2 (new_session_created + first pong)", calls)
	}
}

// TestContainerAllWritesSucceed verifies that a container where every message
// dispatches successfully still processes all messages and writes responses for
// each. This ensures the fix does not regress the happy path.
func TestContainerAllWritesSucceed(t *testing.T) {
	t.Parallel()

	key := rebindTestKey()
	store := mtproto.NewMemoryAuthKeyStore()
	if err := store.Save(context.Background(), key); err != nil {
		t.Fatalf("save key: %v", err)
	}
	srv := mtproto.New(exchange.PrivateKey{}, 2, store, nil, nil)

	const n = 3
	frame := containerFrame(t, key, 42, int64(1)<<32, n)

	var callCount atomic.Int64
	conn := &scriptedConn{
		frames: [][]byte{frame},
		sendFn: func() error {
			callCount.Add(1)
			return nil
		},
	}

	err := srv.ServeConn(context.Background(), conn)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn = %v, want io.EOF", err)
	}

	// One new_session_created + N pongs = N+1 sends.
	calls := callCount.Load()
	if calls != int64(n+1) {
		t.Fatalf("Send called %d times, want %d (new_session_created + %d pongs)", calls, n+1, n)
	}
}
