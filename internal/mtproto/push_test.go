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

func TestMarkRPCUpdateRequiresCurrentOwnerAndKey(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)

	if !c.MarkRPCUpdate(7, keyID, 5) {
		t.Fatal("MarkRPCUpdate refused the current owner and key")
	}
	if got := c.LastPushedPts(); got != 5 {
		t.Fatalf("watermark = %d, want RPC result pts 5", got)
	}
	if c.MarkRPCUpdate(8, keyID, 8) {
		t.Fatal("MarkRPCUpdate accepted a different owner")
	}
	if c.MarkRPCUpdate(7, keyID+1, 8) {
		t.Fatal("MarkRPCUpdate accepted a different auth key")
	}
	if !c.MarkRPCUpdate(7, keyID, 3) {
		t.Fatal("MarkRPCUpdate refused a current key with an older pts")
	}
	if got := c.LastPushedPts(); got != 5 {
		t.Fatalf("watermark regressed to %d, want 5", got)
	}

	c.SetOwner(9)
	if c.MarkRPCUpdate(7, keyID, 9) {
		t.Fatal("MarkRPCUpdate accepted the previous owner after rebind")
	}
	if got := c.LastPushedPts(); got != 0 {
		t.Fatalf("watermark after rebind = %d, want 0", got)
	}
}

func TestPendingRPCUpdateBarrierTracksCommitAndClearsWithResult(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)
	if !c.MarkRPCUpdate(7, keyID, 4) {
		t.Fatal("seed sender watermark")
	}

	if !c.BeginRPCUpdate(7, keyID, 0) {
		t.Fatal("BeginRPCUpdate refused the current owner and key")
	}
	if pts, pending := c.PendingRPCUpdate(7); !pending || pts != 0 {
		t.Fatalf("pending barrier = (%d, %t), want (0, true)", pts, pending)
	}
	if !c.SetRPCUpdatePts(7, keyID, 5) {
		t.Fatal("SetRPCUpdatePts refused the in-flight barrier")
	}
	if pts, pending := c.PendingRPCUpdate(7); !pending || pts != 5 {
		t.Fatalf("committed barrier = (%d, %t), want (5, true)", pts, pending)
	}
	if err := c.SendResultAndMarkRPCUpdate(
		&mtproto.Request{Ctx: context.Background(), MsgID: 1},
		&mt.Pong{PingID: 1}, 7, keyID, 5,
	); err != nil {
		t.Fatalf("send result: %v", err)
	}
	if _, pending := c.PendingRPCUpdate(7); pending {
		t.Fatal("successful result left the pending barrier active")
	}
}

func TestPendingRPCUpdateBarrierSurvivesNoncontiguousResult(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)

	if !c.BeginRPCUpdate(7, keyID, 0) {
		t.Fatal("BeginRPCUpdate refused the current owner and key")
	}
	if !c.SetRPCUpdatePts(7, keyID, 5) {
		t.Fatal("SetRPCUpdatePts refused the in-flight barrier")
	}
	if err := c.SendResultAndMarkRPCUpdate(
		&mtproto.Request{Ctx: context.Background(), MsgID: 3},
		&mt.Pong{PingID: 3}, 7, keyID, 5,
	); err != nil {
		t.Fatalf("send result: %v", err)
	}
	if got := c.LastPushedPts(); got != 0 {
		t.Fatalf("watermark = %d, want 0 while the prefix is missing", got)
	}
	if pts, pending := c.PendingRPCUpdate(7); !pending || pts != 5 {
		t.Fatalf("pending barrier = (%d, %t), want (5, true) until keyed delivery", pts, pending)
	}
	if !c.MarkRPCUpdate(7, keyID, 5) {
		t.Fatal("MarkRPCUpdate refused the keyed sender event")
	}
	if _, pending := c.PendingRPCUpdate(7); pending {
		t.Fatal("keyed sender delivery left the pending barrier active")
	}
	if got := c.LastPushedPts(); got != 5 {
		t.Fatalf("watermark after keyed delivery = %d, want 5", got)
	}
}

func TestPendingRPCUpdateBarrierPreservesBackToBackResults(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)

	for _, tc := range []struct {
		msgID int64
		pts   int
	}{{msgID: 5, pts: 5}, {msgID: 6, pts: 6}} {
		msgID, pts := tc.msgID, tc.pts
		if !c.BeginRPCUpdate(7, keyID, 0) || !c.SetRPCUpdatePts(7, keyID, pts) {
			t.Fatalf("stage sender result pts %d", pts)
		}
		if err := c.SendResultAndMarkRPCUpdate(
			&mtproto.Request{Ctx: context.Background(), MsgID: msgID},
			&mt.Pong{PingID: msgID}, 7, keyID, pts,
		); err != nil {
			t.Fatalf("send result pts %d: %v", pts, err)
		}
	}
	if pts, pending := c.PendingRPCUpdate(7); !pending || pts != 5 {
		t.Fatalf("first pending barrier = (%d, %t), want (5, true)", pts, pending)
	}
	if !c.MarkRPCUpdate(7, keyID, 5) {
		t.Fatal("MarkRPCUpdate refused the first keyed sender event")
	}
	if pts, pending := c.PendingRPCUpdate(7); !pending || pts != 6 {
		t.Fatalf("second pending barrier = (%d, %t), want (6, true)", pts, pending)
	}
	if got := c.LastPushedPts(); got != 5 {
		t.Fatalf("watermark after first keyed event = %d, want 5", got)
	}
	if !c.MarkRPCUpdate(7, keyID, 6) {
		t.Fatal("MarkRPCUpdate refused the second keyed sender event")
	}
	if _, pending := c.PendingRPCUpdate(7); pending {
		t.Fatal("back-to-back keyed delivery left a pending barrier active")
	}
	if got := c.LastPushedPts(); got != 6 {
		t.Fatalf("watermark after back-to-back keyed events = %d, want 6", got)
	}
}

func TestPendingRPCUpdateBarrierDeduplicatesKnownResult(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)

	first := c.BeginRPCUpdate(7, keyID, 5)
	retry := c.BeginRPCUpdate(7, keyID, 5)
	if !first || !retry {
		t.Fatal("BeginRPCUpdate refused a known sender result or its retry")
	}
	if !c.MarkRPCUpdate(7, keyID, 5) {
		t.Fatal("MarkRPCUpdate refused the known sender result")
	}
	if _, pending := c.PendingRPCUpdate(7); pending {
		t.Fatal("deduplicated sender retry left a duplicate barrier")
	}
}

func TestPendingRPCUpdateBarrierCapsQueuedResults(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)

	for pts := 1; pts <= 64; pts++ {
		if !c.BeginRPCUpdate(7, keyID, pts) {
			t.Fatalf("BeginRPCUpdate refused barrier %d before the queue cap", pts)
		}
	}
	if c.BeginRPCUpdate(7, keyID, 65) {
		t.Fatal("BeginRPCUpdate accepted a barrier beyond the queue cap")
	}
	if pts, pending := c.PendingRPCUpdate(7); !pending || pts != 1 {
		t.Fatalf("first queued barrier = (%d, %t), want (1, true)", pts, pending)
	}
}

func TestPendingRPCUpdateReservationClearIsAttemptScoped(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)

	first, ok := c.BeginRPCUpdateAttempt(7, keyID, 5)
	if !ok {
		t.Fatal("first sender reservation refused")
	}
	retry, ok := c.BeginRPCUpdateAttempt(7, keyID, 5)
	if !ok {
		t.Fatal("deduplicated sender reservation refused")
	}
	if c.ClearRPCUpdateAttempt(retry) {
		t.Fatal("deduplicated retry cleared the original barrier")
	}
	if pts, pending := c.PendingRPCUpdate(7); !pending || pts != 5 {
		t.Fatalf("barrier after deduplicated retry failure = (%d, %t), want (5, true)", pts, pending)
	}
	if !c.ClearRPCUpdateAttempt(first) {
		t.Fatal("original sender reservation did not clear its barrier")
	}
	if _, pending := c.PendingRPCUpdate(7); pending {
		t.Fatal("original sender reservation left a barrier")
	}
}

func TestPendingRPCUpdateReservationRefusalCannotClearQueue(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)

	for pts := 1; pts <= 64; pts++ {
		if _, ok := c.BeginRPCUpdateAttempt(7, keyID, pts); !ok {
			t.Fatalf("sender reservation %d refused before the queue cap", pts)
		}
	}
	refused, ok := c.BeginRPCUpdateAttempt(7, keyID, 65)
	if ok {
		t.Fatal("sender reservation beyond the queue cap succeeded")
	}
	if c.ClearRPCUpdateAttempt(refused) {
		t.Fatal("capacity-refused sender attempt cleared a queued barrier")
	}
	if pts, pending := c.PendingRPCUpdate(7); !pending || pts != 1 {
		t.Fatalf("first queued barrier after capacity refusal = (%d, %t), want (1, true)", pts, pending)
	}
}

func TestSendResultAndMarkRPCUpdate(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	c := mtproto.NewTestConn(&fakeConn{}, key)
	c.SetOwner(7)
	if !c.MarkRPCUpdate(7, mtproto.AuthKeyIDInt64(key.ID), 4) {
		t.Fatal("seed sender watermark")
	}

	err := c.SendResultAndMarkRPCUpdate(
		&mtproto.Request{Ctx: context.Background(), MsgID: 2},
		&mt.Pong{PingID: 2},
		7,
		mtproto.AuthKeyIDInt64(key.ID),
		5,
	)
	if err != nil {
		t.Fatalf("send result and mark: %v", err)
	}
	if got := c.LastPushedPts(); got != 5 {
		t.Fatalf("watermark = %d, want 5 after successful result", got)
	}
}

func TestPushToAtWatermarkRejectsStaleBatch(t *testing.T) {
	t.Parallel()
	key := testKey(t)
	fc := &fakeConn{}
	c := mtproto.NewTestConn(fc, key)
	c.SetOwner(7)
	if !c.MarkRPCUpdate(7, mtproto.AuthKeyIDInt64(key.ID), 4) {
		t.Fatal("seed sender watermark")
	}

	pushed, stale, err := c.PushToAtWatermark(context.Background(), 7, 3, &mt.Pong{PingID: 1}, 5)
	if err != nil {
		t.Fatalf("stale push: %v", err)
	}
	if pushed || !stale {
		t.Fatalf("stale push result = pushed:%v stale:%v, want false:true", pushed, stale)
	}
	if got := fc.writes(); got != 0 {
		t.Fatalf("stale push wrote %d frames", got)
	}
	if got := c.LastPushedPts(); got != 4 {
		t.Fatalf("stale push watermark = %d, want 4", got)
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
