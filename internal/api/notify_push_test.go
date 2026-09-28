package api_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type blockedPushTransport struct {
	mu        sync.Mutex
	attempts  int
	entered   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func (t *blockedPushTransport) Send(ctx context.Context, _ *bin.Buffer) error {
	t.mu.Lock()
	t.attempts++
	first := t.attempts == 1
	t.mu.Unlock()
	if first {
		close(t.entered)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (*blockedPushTransport) Recv(context.Context, *bin.Buffer) error { return errors.New("unused") }

func (t *blockedPushTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func (t *blockedPushTransport) pushAttempts() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.attempts
}

var _ transport.Conn = (*blockedPushTransport)(nil)

func testPushKey(seed byte) crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = seed + byte(i)
	}
	return raw.WithID()
}

func TestChannelUpdatesProductionDeliverRecordsOnePushSample(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	start := time.Unix(1_700_000_000, 0)
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return start })
	reg := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, reg, nil, pgtest.PeerDeriver(), metrics)

	alice, err := s.CreateUser(ctx, "+15554127301")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+15554127302")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, alice.ID, bob.ID, "hello", 1, 0, 0); err != nil {
		t.Fatalf("persist message: %v", err)
	}

	transport := &fakeTransport{}
	conn := mtproto.NewTestConn(transport, testKey())
	conn.SetOwner(bob.ID)
	if !reg.Add(bob.ID, conn) {
		t.Fatal("registry rejected bob connection")
	}
	t.Cleanup(func() { reg.Remove(bob.ID, conn) })
	typingDone := make(chan struct{}, 1)
	statusDone := make(chan struct{}, 1)

	_, stop, err := store.StartListener(ctx, dsn,
		updater.Deliver,
		func(ctx context.Context, peerID, fromID int64) {
			updater.DeliverTyping(ctx, peerID, fromID)
			typingDone <- struct{}{}
		},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(ctx context.Context, userID int64, online bool) {
			updater.DeliverStatus(ctx, userID, online)
			statusDone <- struct{}{}
		},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, int32) {},
		nil,
		metrics,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	t.Cleanup(func() { _ = stop() }) //nolint:errcheck // teardown

	if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
		t.Fatalf("wait for listener: %v", err)
	}
	if err := s.Notify(ctx, store.ChannelUpdates, strconv.FormatInt(bob.ID, 10)); err != nil {
		t.Fatalf("notify updates: %v", err)
	}
	if !waitSent(transport) {
		t.Fatal("production Deliver did not write the pending update")
	}

	deadline := time.Now().Add(5 * time.Second)
	var snapshot store.NotificationMetricsSnapshot
	for {
		snapshot = metrics.Snapshot()
		if snapshot.Push.SampleCount == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("push snapshot = %+v, want one sample", snapshot.Push)
		}
		time.Sleep(10 * time.Millisecond)
	}
	beforeTransient := snapshot.Push
	if err := s.Notify(ctx, store.ChannelTyping, store.TypingPayload(bob.ID, alice.ID)); err != nil {
		t.Fatalf("notify typing: %v", err)
	}
	select {
	case <-typingDone:
	case <-time.After(5 * time.Second):
		t.Fatal("typing callback did not run")
	}
	if err := s.Notify(ctx, store.ChannelStatus, store.StatusPayload(alice.ID, true)); err != nil {
		t.Fatalf("notify status: %v", err)
	}
	select {
	case <-statusDone:
	case <-time.After(5 * time.Second):
		t.Fatal("status callback did not run")
	}
	afterTransient := metrics.Snapshot()
	if afterTransient.Push != beforeTransient {
		t.Fatalf("transient push telemetry changed from %+v to %+v", beforeTransient, afterTransient.Push)
	}
	if afterTransient.Channels.Updates != 1 {
		t.Fatalf("valid tg_updates count = %d, want one", afterTransient.Channels.Updates)
	}
	if afterTransient.Channels.Typing != 1 || afterTransient.Channels.Status != 1 {
		t.Fatalf("transient channel counts = %+v, want one typing and one status", afterTransient.Channels)
	}
	if afterTransient.Push.Outcomes != (store.PushOutcomeCounts{Success: 1}) {
		t.Fatalf("push outcomes = %+v, want exactly one success", afterTransient.Push.Outcomes)
	}
	var bucketSamples int64
	for _, count := range afterTransient.Push.LatencyBucketCounts {
		bucketSamples += count
	}
	if bucketSamples != 1 {
		t.Fatalf("push latency buckets = %v, want exactly one sample", afterTransient.Push.LatencyBucketCounts)
	}
}

func TestMultiSocketSlowPushCannotStallAnotherAccount(t *testing.T) {
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	registry := mtproto.NewSessionRegistry()
	updater := api.NewUpdater(s, registry, nil, pgtest.PeerDeriver())

	slowUser, err := s.CreateUser(ctx, "+15554127311")
	if err != nil {
		t.Fatalf("create slow user: %v", err)
	}
	slowSender, err := s.CreateUser(ctx, "+15554127312")
	if err != nil {
		t.Fatalf("create slow sender: %v", err)
	}
	healthyUser, err := s.CreateUser(ctx, "+15554127313")
	if err != nil {
		t.Fatalf("create healthy user: %v", err)
	}
	healthySender, err := s.CreateUser(ctx, "+15554127314")
	if err != nil {
		t.Fatalf("create healthy sender: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, slowSender.ID, slowUser.ID, "slow", 1, 0, 0); err != nil {
		t.Fatalf("persist slow-user event: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, healthySender.ID, healthyUser.ID, "healthy", 2, 0, 0); err != nil {
		t.Fatalf("persist healthy-user event: %v", err)
	}

	slowTransports := make([]*blockedPushTransport, 3)
	slowConns := make([]*mtproto.Conn, len(slowTransports))
	for i := range slowTransports {
		slowTransports[i] = &blockedPushTransport{entered: make(chan struct{}), closed: make(chan struct{})}
		keySeed := byte(i + 1)
		if keySeed >= 3 {
			keySeed++ // keep the sender's key out of the blocked sibling set
		}
		slowConns[i] = mtproto.NewTestConn(slowTransports[i], testPushKey(keySeed))
		slowConns[i].SetOwner(slowUser.ID)
		if !registry.Add(slowUser.ID, slowConns[i]) {
			t.Fatalf("register slow connection %d", i)
		}
		conn := slowConns[i]
		t.Cleanup(func() { registry.Remove(slowUser.ID, conn) })
	}

	originKey := testPushKey(3)
	originTransport := &fakeTransport{}
	originConn := mtproto.NewTestConn(originTransport, originKey)
	originConn.SetOwner(slowUser.ID)
	if !registry.Add(slowUser.ID, originConn) {
		t.Fatal("register origin connection")
	}
	t.Cleanup(func() { registry.Remove(slowUser.ID, originConn) })

	siblingTransport := &fakeTransport{}
	siblingConn := mtproto.NewTestConn(siblingTransport, testPushKey(4))
	siblingConn.SetOwner(slowUser.ID)
	if !registry.Add(slowUser.ID, siblingConn) {
		t.Fatal("register healthy sibling connection")
	}
	t.Cleanup(func() { registry.Remove(slowUser.ID, siblingConn) })

	healthyTransport := &fakeTransport{}
	healthyConn := mtproto.NewTestConn(healthyTransport, testPushKey(2))
	healthyConn.SetOwner(healthyUser.ID)
	if !registry.Add(healthyUser.ID, healthyConn) {
		t.Fatal("register healthy connection")
	}
	t.Cleanup(func() { registry.Remove(healthyUser.ID, healthyConn) })

	_, stop, err := store.StartListener(ctx, dsn,
		updater.Deliver,
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, int32) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	t.Cleanup(func() { _ = stop() }) //nolint:errcheck // teardown
	if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
		t.Fatalf("wait for listener: %v", err)
	}
	slowPayload := strconv.FormatInt(slowUser.ID, 10) + "|" + strconv.FormatInt(mtproto.AuthKeyIDInt64(originKey.ID), 10) + "|1"
	if err := s.Notify(ctx, store.ChannelUpdates, slowPayload); err != nil {
		t.Fatalf("notify slow user: %v", err)
	}
	for i, transport := range slowTransports {
		select {
		case <-transport.entered:
		case <-time.After(500 * time.Millisecond):
			t.Fatalf("slow push %d did not reach the transport with sibling sockets in parallel", i)
		}
	}
	for range 8 {
		if err := s.Notify(ctx, store.ChannelUpdates, slowPayload); err != nil {
			t.Fatalf("repeat slow-user notify: %v", err)
		}
	}

	started := time.Now()
	if err := s.Notify(ctx, store.ChannelUpdates, strconv.FormatInt(healthyUser.ID, 10)); err != nil {
		t.Fatalf("notify healthy user: %v", err)
	}
	if !waitSentWithin(healthyTransport, 500*time.Millisecond) {
		t.Fatalf("healthy push waited behind slow socket for %s", time.Since(started))
	}
	if !siblingTransport.wasSent() {
		t.Fatal("healthy sibling did not receive the update")
	}
	for i, transport := range slowTransports {
		select {
		case <-transport.closed:
		case <-time.After(2 * time.Second):
			t.Fatalf("failed slow socket %d was not closed", i)
		}
	}
	// Repeated notifications must not get another write attempt after the first
	// deadline closes the connection.
	time.Sleep(100 * time.Millisecond)
	for i, transport := range slowTransports {
		if got := transport.pushAttempts(); got != 1 {
			t.Fatalf("slow push %d attempts = %d, want one before removal", i, got)
		}
	}
	if originTransport.wasSent() {
		t.Fatal("sender session received its own keyed update echo")
	}
}

func waitSentWithin(ft *fakeTransport, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ft.wasSent() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return ft.wasSent()
}
