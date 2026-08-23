package mtproto_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// recordingFrameConn serves scripted frames and records every encrypted
// message the server writes back, so a test can decrypt them and assert on the
// actual replies that reached the wire.
type recordingFrameConn struct {
	frames [][]byte
	i      int

	mu   sync.Mutex
	sent [][]byte
}

func (c *recordingFrameConn) Recv(_ context.Context, b *bin.Buffer) error {
	if c.i >= len(c.frames) {
		return io.EOF
	}
	b.ResetTo(slices.Clone(c.frames[c.i]))
	c.i++
	return nil
}

func (c *recordingFrameConn) Send(_ context.Context, b *bin.Buffer) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, slices.Clone(b.Buf))
	return nil
}

func (c *recordingFrameConn) Close() error { return nil }

// replies decrypts every recorded frame under key and returns the plaintext
// message bodies in wire order.
func (c *recordingFrameConn) replies(t *testing.T, key crypto.AuthKey) [][]byte {
	t.Helper()
	c.mu.Lock()
	frames := slices.Clone(c.sent)
	c.mu.Unlock()

	// A client-side cipher, not the server's one: the two sides of an MTProto
	// session encrypt toward each other, so what the server wrote is what a
	// client decrypts.
	cipher := crypto.NewClientCipher(crypto.DefaultRand())
	out := make([][]byte, 0, len(frames))
	for _, f := range frames {
		m := &crypto.EncryptedMessage{}
		if err := m.DecodeWithoutCopy(&bin.Buffer{Buf: f}); err != nil {
			t.Fatalf("decode server frame: %v", err)
		}
		msg, err := cipher.Decrypt(key, m)
		if err != nil {
			t.Fatalf("decrypt server frame: %v", err)
		}
		out = append(out, slices.Clone(msg.Data()))
	}
	return out
}

// findResult scans decrypted bodies for the RPC result addressed to msgID and
// returns its raw payload.
func findResult(t *testing.T, bodies [][]byte, msgID int64) []byte {
	t.Helper()
	for _, body := range bodies {
		var res proto.Result
		if err := res.Decode(&bin.Buffer{Buf: body}); err != nil {
			continue
		}
		if res.RequestMessageID != msgID {
			continue
		}
		return res.Result
	}
	t.Fatalf("no RPC result for msg id %d in %d replies", msgID, len(bodies))
	return nil
}

// TestRPCDeadlineAbandonsWithoutTearingDownConnection proves all three halves
// of the per-request ceiling at once: a handler still running when its request
// deadline fires gets its context cancelled, the client receives the same
// generic INTERNAL any transient failure produces, and the connection itself
// survives — the next frame is served normally under a full, fresh budget, so
// a chunked transfer is bounded per chunk and never per logical transfer.
func TestRPCDeadlineAbandonsWithoutTearingDownConnection(t *testing.T) {
	t.Parallel()
	key := rebindTestKey()
	ks := &statusKeyStore{key: key, users: []int64{0}}

	const deadline = 150 * time.Millisecond
	var mu sync.Mutex
	var budgets []time.Duration
	h := mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
		if dl, ok := req.Ctx.Deadline(); ok {
			mu.Lock()
			budgets = append(budgets, time.Until(dl))
			mu.Unlock()
		} else {
			t.Error("request context carries no deadline")
		}
		// Hold the RPC until the ceiling cancels it, the way a wedged store
		// call would hold one.
		<-req.Ctx.Done()
		return req.Ctx.Err()
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, ks, h, nil)
	if err := srv.SetRPCDeadline(deadline); err != nil {
		t.Fatalf("set rpc deadline: %v", err)
	}

	conn := &recordingFrameConn{
		frames: [][]byte{
			statusClientFrame(t, key, 42, 1<<32, &tg.AccountRegisterDeviceRequest{}),
			statusClientFrame(t, key, 42, 2<<32, &tg.AccountRegisterDeviceRequest{}),
		},
	}
	// The serve loop must run both frames to EOF: the deadline abandons the
	// request, never the connection.
	if err := srv.ServeConn(context.Background(), conn); !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn = %v, want EOF (the connection survived)", err)
	}

	replies := conn.replies(t, key)
	decodeErr := func(msgID int64) *mt.RPCError {
		e := &mt.RPCError{}
		if err := e.Decode(&bin.Buffer{Buf: findResult(t, replies, msgID)}); err != nil {
			t.Fatalf("decode rpc error for msg id %d: %v", msgID, err)
		}
		return e
	}
	firstErr, secondErr := decodeErr(1<<32), decodeErr(2<<32)
	for i, e := range []*mt.RPCError{firstErr, secondErr} {
		if e.ErrorCode != 500 || e.ErrorMessage != "INTERNAL" {
			t.Fatalf("request %d: rpc error = %d %q, want generic INTERNAL",
				i+1, e.ErrorCode, e.ErrorMessage)
		}
	}

	// The second request started with a full budget of its own: per-chunk
	// bounding, not a shared pool that a long transfer drains.
	mu.Lock()
	defer mu.Unlock()
	if len(budgets) != 2 {
		t.Fatalf("handler saw %d requests, want 2", len(budgets))
	}
	if budgets[0] <= 0 || budgets[1] <= 0 {
		t.Fatalf("budgets = %v, want both positive", budgets)
	}
	if budgets[1] < deadline/2 {
		t.Fatalf("second request budget = %s, want a fresh full budget (>=%s)",
			budgets[1], deadline/2)
	}
}
