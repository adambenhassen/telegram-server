package mtproto_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// fakeConn is a transport.Conn that records writes and can force a send error.
// When block is non-nil each Send announces itself on entered and waits for
// block to be closed, so a test can park a push inside the conn's write lock.
type fakeConn struct {
	mu      sync.Mutex
	count   int
	closes  int
	sendErr error

	entered chan struct{}
	block   chan struct{}
}

func (f *fakeConn) Send(_ context.Context, _ *bin.Buffer) error {
	if f.block != nil {
		f.entered <- struct{}{}
		<-f.block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.count++
	return f.sendErr
}

func (f *fakeConn) Recv(_ context.Context, _ *bin.Buffer) error { return errors.New("unused") }

func (f *fakeConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return nil
}

func (f *fakeConn) writes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.count
}

func (f *fakeConn) closed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes > 0
}

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
	c.SetOwner(7)

	if _, err := c.PushTo(context.Background(), 7, &mt.Pong{PingID: 1}, 0); err == nil {
		t.Fatal("PushTo must surface the transport write error")
	}
}

// TestPushConcurrentWithResult drives PushTo and SendResult on one conn from two
// goroutines; -race proves the write mutex serializes them.
func TestPushConcurrentWithResult(t *testing.T) {
	t.Parallel()
	fc := &fakeConn{}
	c := mtproto.NewTestConn(fc, testKey(t))
	c.SetOwner(7)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			pushed, err := c.PushTo(ctx, 7, &mt.Pong{PingID: 1}, 0)
			if err != nil {
				t.Errorf("push: %v", err)
				return
			}
			if !pushed {
				t.Error("push refused by an owner that never changed")
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

// TestPushToDropsUpdateForPreviousOwner covers the delivery race the registry
// resync alone cannot close: Conns hands out a snapshot and the update batch is
// built from the database before anything is written, so the key can rebind in
// between. The push must then be dropped rather than written to a socket the
// new user controls, and it must not restore the old owner's watermark over the
// reset — that would silently suppress every push to the new user.
func TestPushToDropsUpdateForPreviousOwner(t *testing.T) {
	t.Parallel()
	fc := &fakeConn{}
	c := mtproto.NewTestConn(fc, testKey(t))
	ctx := context.Background()

	c.SetOwner(7)
	pushed, err := c.PushTo(ctx, 7, &mt.Pong{PingID: 1}, 5)
	if err != nil {
		t.Fatalf("PushTo: %v", err)
	}
	if !pushed {
		t.Fatal("PushTo refused a push for the current owner")
	}
	if got := c.LastPushedPts(); got != 5 {
		t.Fatalf("watermark = %d, want 5", got)
	}

	// A transient push carries no pts and must leave the watermark alone;
	// zeroing it here would replay the whole backlog on the next delivery.
	pushed, err = c.PushTo(ctx, 7, &mt.Pong{PingID: 3}, 0)
	if err != nil || !pushed {
		t.Fatalf("transient PushTo = %v, %v; want true, nil", pushed, err)
	}
	if got := c.LastPushedPts(); got != 5 {
		t.Fatalf("watermark after a transient push = %d, want 5", got)
	}

	c.SetOwner(9)
	if got := c.LastPushedPts(); got != 0 {
		t.Fatalf("watermark after the conn changed hands = %d, want 0", got)
	}

	before := fc.writes()
	pushed, err = c.PushTo(ctx, 7, &mt.Pong{PingID: 2}, 9)
	if err != nil {
		t.Fatalf("PushTo after rebind: %v", err)
	}
	if pushed {
		t.Fatal("a push addressed to the previous owner was accepted")
	}
	if got := fc.writes(); got != before {
		t.Fatalf("writes = %d, want %d: nothing may reach the socket", got, before)
	}
	if got := c.LastPushedPts(); got != 0 {
		t.Fatalf("watermark = %d, want 0: a dropped push must not restore it", got)
	}
}

// TestOwnerHandoffWaitsForInFlightPush pins the ownership check, the socket
// write and the watermark advance into one critical section. A delivery already
// on the wire holds the write lock, so the hand-off cannot complete underneath
// it, and every push addressed to the previous owner afterwards is refused. If
// the ownership check moved outside writeMu, a delivery holding a stale
// registry snapshot could still write after the rebind.
func TestOwnerHandoffWaitsForInFlightPush(t *testing.T) {
	t.Parallel()
	fc := &fakeConn{entered: make(chan struct{}, 4), block: make(chan struct{})}
	c := mtproto.NewTestConn(fc, testKey(t))
	ctx := context.Background()
	c.SetOwner(7)

	pushDone := make(chan error, 1)
	go func() {
		_, err := c.PushTo(ctx, 7, &mt.Pong{PingID: 1}, 5)
		pushDone <- err
	}()
	<-fc.entered // the push now holds the write lock

	rebound := make(chan struct{})
	go func() {
		c.SetOwner(9)
		close(rebound)
	}()
	select {
	case <-rebound:
		t.Fatal("the conn changed hands while a push was still on the wire")
	case <-time.After(50 * time.Millisecond):
	}

	close(fc.block)
	if err := <-pushDone; err != nil {
		t.Fatalf("in-flight push: %v", err)
	}
	select {
	case <-rebound:
	case <-time.After(2 * time.Second):
		t.Fatal("hand-off did not complete after the push was released")
	}

	// The in-flight push advanced the watermark to 5 while it held the lock, so
	// that advance lands after the hand-off was already waiting. The moved conn
	// must still not carry the previous owner's watermark: keeping it would
	// silently drop every push to the new owner until their pts passed it.
	if got := c.LastPushedPts(); got != 0 {
		t.Fatalf("watermark = %d, want 0: an advance from the previous owner survived the hand-off", got)
	}

	pushed, err := c.PushTo(ctx, 7, &mt.Pong{PingID: 2}, 9)
	if err != nil {
		t.Fatalf("PushTo after rebind: %v", err)
	}
	if pushed {
		t.Fatal("a push for the previous owner was accepted after the hand-off")
	}
}
