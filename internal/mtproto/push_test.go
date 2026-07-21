package mtproto_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// fakeConn is a transport.Conn that records writes and can force a send error.
type fakeConn struct {
	mu      sync.Mutex
	count   int
	sendErr error
}

func (f *fakeConn) Send(_ context.Context, _ *bin.Buffer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	return f.sendErr
}

func (f *fakeConn) Recv(_ context.Context, _ *bin.Buffer) error { return errors.New("unused") }
func (f *fakeConn) Close() error                                { return nil }

func testKey(t *testing.T) crypto.AuthKey {
	t.Helper()
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i)
	}
	return raw.WithID()
}

func TestPushErrorsOnWriteFailure(t *testing.T) {
	t.Parallel()
	fc := &fakeConn{sendErr: errors.New("boom")}
	c := mtproto.NewTestConn(fc, testKey(t))

	if err := c.Push(context.Background(), &mt.Pong{PingID: 1}); err == nil {
		t.Fatal("Push must surface the transport write error")
	}
}

// TestPushConcurrentWithResult drives Push and SendResult on one conn from two
// goroutines; -race proves the write mutex serializes them.
func TestPushConcurrentWithResult(t *testing.T) {
	t.Parallel()
	fc := &fakeConn{}
	c := mtproto.NewTestConn(fc, testKey(t))
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			if err := c.Push(ctx, &mt.Pong{PingID: 1}); err != nil {
				t.Errorf("push: %v", err)
				return
			}
		}
	})
	wg.Go(func() {
		for range 100 {
			if err := c.SendResult(&mtproto.Request{Ctx: ctx, MsgID: 2}, &mt.Pong{PingID: 2}); err != nil {
				t.Errorf("send result: %v", err)
				return
			}
		}
	})
	wg.Wait()

	if fc.count != 200 {
		t.Fatalf("writes = %d, want 200", fc.count)
	}
}
