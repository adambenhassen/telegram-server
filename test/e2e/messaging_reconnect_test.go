package e2e_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
	"github.com/gotd/td/tg"
)

func TestMessagingReconnectPushGap(t *testing.T) {
	t.Parallel()
	f := newSmokeFixture(t)

	const phoneA, phoneB, phoneC = "+15551046101", "+15551046102", "+15551046103"
	seedPhoneUsers(t, f.ctx, f.store, phoneA, phoneB, phoneC)

	a1 := newSmokeClient(t, f, "A1", phoneA)
	a1Connections := f.listener.activeConnections()
	if len(a1Connections) == 0 {
		t.Fatal("A1 has no accepted fixture connection")
	}
	var advertised *tg.Config
	if err := a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		cfg, err := api.HelpGetConfig(ctx)
		advertised = cfg
		return err
	}); err != nil {
		t.Fatalf("A1 config request failed (cause=%s)", safeErrorClass(err))
	}
	advertisementMatches := fixtureConfigMatchesListener(advertised, f.dcID, f.listener)
	keysBeforeFirstReconnect, err := reconnectAuthKeyIDs(f.ctx, f.store, a1.id)
	if err != nil {
		t.Fatalf("A auth-key snapshot failed (type=%T)", err)
	}
	acceptsBeforeFirstReconnect := f.listener.acceptCount()
	releaseFirstAccept, firstAccepted := f.listener.pauseAccept()
	defer releaseFirstAccept()
	if err := f.listener.closeConnections(a1Connections); err != nil {
		t.Fatalf("A1 socket close failed (type=%T)", err)
	}
	firstReconnectCtx, cancelFirstReconnect := context.WithTimeout(f.ctx, 20*time.Second)
	defer cancelFirstReconnect()
	select {
	case <-firstAccepted:
	case <-firstReconnectCtx.Done():
		t.Fatalf("A1 reconnect did not reach the fixture listener (accepts before=%d after=%d; cause=%s)", acceptsBeforeFirstReconnect, f.listener.acceptCount(), contextFailureDescription(firstReconnectCtx))
	}
	if f.listener.acceptCount() <= acceptsBeforeFirstReconnect {
		t.Fatalf("A1 reconnect did not produce a fresh accept (before=%d after=%d)", acceptsBeforeFirstReconnect, f.listener.acceptCount())
	}
	if !advertisementMatches {
		t.Fatal("A1 config did not advertise its bound fixture listener")
	}
	releaseFirstAccept()
	waitForDistinctAuthKeys(t, firstReconnectCtx, f.registry, a1.id, 1, "A1", a1.lifecycle)
	if err := a1.call(firstReconnectCtx, func(ctx context.Context, api *tg.Client) error {
		_, err := api.UpdatesGetState(ctx)
		return err
	}); err != nil {
		t.Fatalf("A1 resumed request failed (cause=%s)", safeErrorClass(err))
	}
	keysAfterFirstReconnect, err := reconnectAuthKeyIDs(f.ctx, f.store, a1.id)
	cancelFirstReconnect()
	if err != nil {
		t.Fatalf("A auth-key verification failed (type=%T)", err)
	}
	if !sameReconnectAuthKeyIDs(keysBeforeFirstReconnect, keysAfterFirstReconnect) {
		t.Fatal("A authenticated key set changed across reconnect")
	}
	a1Connections = f.listener.activeConnections()

	a2 := newSmokeClient(t, f, "A2", phoneA)
	b1 := newSmokeClient(t, f, "B1", phoneB)
	b2 := newSmokeClient(t, f, "B2", phoneB)
	c := newSmokeClient(t, f, "C", phoneC)
	waitForDistinctAuthKeys(t, f.ctx, f.registry, a1.id, 2, "A1", a1.lifecycle)
	waitForDistinctAuthKeys(t, f.ctx, f.registry, b1.id, 2, "B1", b1.lifecycle)

	keysBefore, err := reconnectAuthKeyIDs(f.ctx, f.store, a1.id)
	if err != nil {
		t.Fatalf("A auth-key snapshot failed (type=%T)", err)
	}
	acceptsBefore := f.listener.acceptCount()
	releaseAccept, accepted := f.listener.pauseAccept()
	defer releaseAccept()
	if err := f.listener.closeConnections(a1Connections); err != nil {
		t.Fatalf("A1 socket close failed (type=%T)", err)
	}
	acceptCtx, cancelAccept := context.WithTimeout(f.ctx, 30*time.Second)
	defer cancelAccept()
	select {
	case <-accepted:
	case <-acceptCtx.Done():
		t.Fatalf("A1 reconnect did not reach the fixture listener (accepts before=%d after=%d; cause=%s)", acceptsBefore, f.listener.acceptCount(), contextFailureDescription(acceptCtx))
	}
	cancelAccept()
	if f.listener.acceptCount() <= acceptsBefore {
		t.Fatalf("A1 reconnect did not produce a fresh accept (before=%d after=%d)", acceptsBefore, f.listener.acceptCount())
	}
	if !advertisementMatches {
		t.Fatal("A1 config did not advertise its bound fixture listener")
	}

	const gapText = "A2 sent during A1 reconnect gap"
	var gapResult tg.UpdatesClass
	if err := a2.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(a2.id, b1.id), Message: gapText, RandomID: 1046101,
		})
		gapResult = res
		return err
	}); err != nil {
		t.Fatalf("A2 gap send failed (cause=%s)", safeErrorClass(err))
	}
	gapSenderMessage, gapSenderPts, ok := outgoingMessage(t, gapResult, gapText)
	if !ok || countOutgoingMessages(gapResult, gapText) != 1 {
		t.Fatal("A2 gap send omitted exactly one outgoing message")
	}
	assertReconnectMessage(t, f.ctx, a2.seen, gapText, gapSenderMessage.ID, true, b1.id, gapSenderPts, "A2 gap result")
	assertReconnectMessage(t, f.ctx, b1.push, gapText, reconnectHistoryMessageID(t, f, b1, a1.id, gapText), false, a1.id, reconnectStatePts(t, f, b1.id), "B1 gap push")
	assertReconnectMessage(t, f.ctx, b2.push, gapText, reconnectHistoryMessageID(t, f, b2, a1.id, gapText), false, a1.id, reconnectStatePts(t, f, b1.id), "B2 gap push")
	assertReconnectNoMessage(t, f.ctx, c.seen, "C during gap")
	assertReconnectNoMessage(t, f.ctx, c.push, "C push during gap")
	assertReconnectNoMessage(t, f.ctx, a2.push, "A2 origin push during gap")

	releaseAccept()
	resumeCtx, cancelResume := context.WithTimeout(f.ctx, 45*time.Second)
	defer cancelResume()
	waitForDistinctAuthKeys(t, resumeCtx, f.registry, a1.id, 2, "A1", a1.lifecycle)
	var resumed *tg.UpdatesState
	if err := a1.call(resumeCtx, func(ctx context.Context, api *tg.Client) error {
		state, err := api.UpdatesGetState(ctx)
		resumed = state
		return err
	}); err != nil {
		t.Fatalf("A1 resumed request failed (cause=%s)", safeErrorClass(err))
	}
	var gapDifference tg.UpdatesDifferenceClass
	if err := a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		diff, err := api.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		gapDifference = diff
		return err
	}); err != nil {
		t.Fatalf("A1 gap difference request failed (cause=%s)", safeReconnectCallFailure(a1, err))
	}
	fullDifference, ok := gapDifference.(*tg.UpdatesDifference)
	if !ok || fullDifference == nil {
		t.Fatalf("A1 gap difference response type = %T", gapDifference)
	}
	if !reconnectDifferenceHasMessage(fullDifference, gapText, gapSenderMessage.ID, b1.id) || fullDifference.State.Pts != gapSenderPts {
		t.Fatalf("A1 gap difference mismatch (message count=%d pts=%d)", len(fullDifference.NewMessages), fullDifference.State.Pts)
	}
	keysAfter, err := reconnectAuthKeyIDs(f.ctx, f.store, a1.id)
	if err != nil {
		t.Fatalf("A auth-key verification failed (type=%T)", err)
	}
	if !sameReconnectAuthKeyIDs(keysBefore, keysAfter) {
		t.Fatal("A authenticated key set changed across reconnect")
	}
	accountState, err := f.store.State(f.ctx, a1.id)
	if err != nil {
		t.Fatalf("A state verification failed (type=%T)", err)
	}
	if resumed == nil || resumed.Pts != accountState.Pts {
		t.Fatalf("A1 resumed state pts mismatch (got=%d want=%d)", statePts(resumed), accountState.Pts)
	}
	const liveText = "A1 sent after reconnect"
	var liveResult tg.UpdatesClass
	if err := a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		res, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(a1.id, b1.id), Message: liveText, RandomID: 1046102,
		})
		liveResult = res
		return err
	}); err != nil {
		t.Fatalf("A1 live send failed (cause=%s)", safeErrorClass(err))
	}
	liveSenderMessage, liveSenderPts, ok := outgoingMessage(t, liveResult, liveText)
	if !ok || countOutgoingMessages(liveResult, liveText) != 1 {
		t.Fatal("A1 live send omitted exactly one outgoing message")
	}
	assertReconnectMessage(t, f.ctx, a2.push, liveText, liveSenderMessage.ID, true, b1.id, liveSenderPts, "A2 live push")
	assertReconnectMessage(t, f.ctx, b1.push, liveText, reconnectHistoryMessageID(t, f, b1, a1.id, liveText), false, a1.id, reconnectStatePts(t, f, b1.id), "B1 live push")
	assertReconnectMessage(t, f.ctx, b2.push, liveText, reconnectHistoryMessageID(t, f, b2, a1.id, liveText), false, a1.id, reconnectStatePts(t, f, b1.id), "B2 live push")
	assertReconnectOriginSuppressed(t, f.ctx, a1.push, liveText, liveSenderMessage.ID, gapText, gapSenderMessage.ID, gapSenderPts, b1.id, "A1")
	assertReconnectNoMessage(t, f.ctx, c.seen, "C after reconnect")
	assertReconnectNoMessage(t, f.ctx, c.push, "C push after reconnect")
	if err := c.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		diff, err := api.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 0, Qts: 0})
		if err != nil {
			return err
		}
		if empty, ok := diff.(*tg.UpdatesDifferenceEmpty); ok && empty != nil {
			return nil
		}
		if full, ok := diff.(*tg.UpdatesDifference); ok && len(full.NewMessages) == 0 && len(full.OtherUpdates) == 0 {
			return nil
		}
		return errors.New("C difference exposed A/B updates")
	}); err != nil {
		t.Fatalf("C isolation difference failed (cause=%s)", safeErrorClass(err))
	}
}

type acceptCountingListener struct {
	net.Listener

	mu       sync.Mutex
	accepted uint64
	active   map[*acceptCountingConn]struct{}
	gate     *acceptGate
}

type acceptCountingConn struct {
	net.Conn

	once     sync.Once
	closeErr error
	owner    *acceptCountingListener
}

type acceptGate struct {
	done        chan struct{}
	arrived     chan struct{}
	arrivedOnce sync.Once
	releaseOnce sync.Once
}

func newAcceptCountingListener(ln net.Listener) *acceptCountingListener {
	return &acceptCountingListener{
		Listener: ln,
		active:   make(map[*acceptCountingConn]struct{}),
	}
}

func (l *acceptCountingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tracked := &acceptCountingConn{Conn: conn, owner: l}
	l.mu.Lock()
	l.accepted++
	l.active[tracked] = struct{}{}
	gate := l.gate
	l.mu.Unlock()
	if gate != nil {
		gate.arrivedOnce.Do(func() { close(gate.arrived) })
		<-gate.done
	}
	return tracked, nil
}

func (l *acceptCountingListener) Close() error {
	l.mu.Lock()
	gate := l.gate
	l.gate = nil
	l.mu.Unlock()
	if gate != nil {
		gate.release()
	}
	return l.Listener.Close()
}

func (l *acceptCountingListener) acceptCount() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.accepted
}

func (l *acceptCountingListener) activeConnections() []net.Conn {
	l.mu.Lock()
	defer l.mu.Unlock()
	connections := make([]net.Conn, 0, len(l.active))
	for conn := range l.active {
		connections = append(connections, conn)
	}
	return connections
}

func (l *acceptCountingListener) closeConnections(connections []net.Conn) error {
	for _, conn := range connections {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
	}
	return nil
}

func (l *acceptCountingListener) pauseAccept() (func(), <-chan struct{}) {
	gate := &acceptGate{done: make(chan struct{}), arrived: make(chan struct{})}
	l.mu.Lock()
	l.gate = gate
	l.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			l.mu.Lock()
			if l.gate == gate {
				l.gate = nil
			}
			l.mu.Unlock()
			gate.release()
		})
	}
	return release, gate.arrived
}

func (g *acceptGate) release() {
	g.releaseOnce.Do(func() { close(g.done) })
}

func (c *acceptCountingConn) Close() error {
	c.once.Do(func() {
		c.closeErr = c.Conn.Close()
		c.owner.mu.Lock()
		delete(c.owner.active, c)
		c.owner.mu.Unlock()
	})
	return c.closeErr
}

func fixtureConfigMatchesListener(cfg *tg.Config, dcID int, listener net.Listener) bool {
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || cfg == nil || cfg.ThisDC != dcID || len(cfg.DCOptions) != 1 {
		return false
	}
	option := cfg.DCOptions[0]
	return option.ID == dcID && option.IPAddress == addr.IP.String() && option.Port == addr.Port
}

func reconnectAuthKeyIDs(ctx context.Context, st *store.Store, userID int64) (map[int64]struct{}, error) {
	keys, err := st.AuthKeysByUser(ctx, userID)
	if err != nil {
		return nil, err
	}
	ids := make(map[int64]struct{}, len(keys))
	for _, key := range keys {
		ids[key.ID] = struct{}{}
	}
	return ids, nil
}

func sameReconnectAuthKeyIDs(left, right map[int64]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for id := range left {
		if _, ok := right[id]; !ok {
			return false
		}
	}
	return true
}

func reconnectHistoryMessageID(t *testing.T, f *smokeFixture, client *smokeClient, peerID int64, text string) int {
	t.Helper()
	var id int
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		result, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer: peerUser(client.id, peerID), Limit: 20,
		})
		if err != nil {
			return err
		}
		history, ok := result.(*tg.MessagesMessages)
		if !ok {
			return errors.New("unexpected history response")
		}
		for _, class := range history.Messages {
			if message, ok := class.(*tg.Message); ok && message.Message == text {
				if id != 0 {
					return errors.New("duplicate history message")
				}
				id = message.ID
			}
		}
		if id == 0 {
			return errors.New("history message missing")
		}
		return nil
	}); err != nil {
		t.Fatalf("%s history check failed (cause=%s)", client.label, safeErrorClass(err))
	}
	return id
}

func reconnectStatePts(t *testing.T, f *smokeFixture, userID int64) int {
	t.Helper()
	state, err := f.store.State(f.ctx, userID)
	if err != nil {
		t.Fatalf("update state lookup failed (type=%T)", err)
	}
	return state.Pts
}

func reconnectDifferenceHasMessage(diff *tg.UpdatesDifference, text string, wantID int, peerID int64) bool {
	count := 0
	for _, class := range diff.NewMessages {
		message, ok := class.(*tg.Message)
		if !ok || message.Message != text {
			continue
		}
		peer, ok := message.PeerID.(*tg.PeerUser)
		if !ok || peer.UserID != peerID || message.ID != wantID || !message.Out {
			return false
		}
		count++
	}
	return count == 1
}

func assertReconnectMessage(t *testing.T, ctx context.Context, collector *updateCollector, text string, wantID int, wantOut bool, peerID int64, wantPts int, label string) {
	t.Helper()
	message := recvOrCtx(t, ctx, collector.newMsg, label+" message")
	pts := recvOrCtx(t, ctx, collector.points, label+" pts")
	peer, ok := message.PeerID.(*tg.PeerUser)
	if message.Message != text || message.ID != wantID || message.Out != wantOut || !ok || peer.UserID != peerID {
		t.Fatalf("%s message metadata mismatch (id=%d out=%t)", label, message.ID, message.Out)
	}
	if pts != wantPts {
		t.Fatalf("%s pts = %d, want %d", label, pts, wantPts)
	}
}

func assertReconnectNoMessage(t *testing.T, ctx context.Context, collector *updateCollector, label string) {
	t.Helper()
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-collector.newMsg:
		t.Fatalf("%s received an unexpected message", label)
	case <-timer.C:
	case <-ctx.Done():
		t.Fatalf("waiting for %s isolation check: %s", label, contextFailureDescription(ctx))
	}
}

func assertReconnectOriginSuppressed(t *testing.T, ctx context.Context, collector *updateCollector, originText string, originID int, allowedText string, allowedID, allowedPts int, allowedPeer int64, label string) {
	t.Helper()
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case message := <-collector.newMsg:
			pts := recvOrCtx(t, ctx, collector.points, label+" push pts")
			if message.Message == originText && message.ID == originID {
				t.Fatalf("%s received its own origin echo (id=%d)", label, message.ID)
			}
			peer, ok := message.PeerID.(*tg.PeerUser)
			if message.Message != allowedText || message.ID != allowedID || !message.Out || !ok || peer.UserID != allowedPeer || pts != allowedPts {
				t.Fatalf("%s received unexpected push metadata (id=%d pts=%d)", label, message.ID, pts)
			}
		case <-timer.C:
			return
		case <-ctx.Done():
			t.Fatalf("waiting for %s origin-echo check: %s", label, contextFailureDescription(ctx))
		}
	}
}

func statePts(state *tg.UpdatesState) int {
	if state == nil {
		return -1
	}
	return state.Pts
}

func safeReconnectCallFailure(client *smokeClient, err error) string {
	if client.lifecycle.result.finished() {
		phase, started := client.lifecycle.phase.snapshot()
		if phase == "" {
			phase, started = "client run", time.Now()
		}
		return client.lifecycle.diagnostic(phase, time.Since(started), "cause="+safeErrorClass(client.lifecycle.result.error()))
	}
	return "cause=" + safeErrorClass(err)
}
