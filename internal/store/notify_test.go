package store_test

import (
	"context"
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func openDSN(t *testing.T, dsn string) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}

func TestNotifyReachesRawListener(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDSN(t, dsn)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err := conn.Exec(ctx, "LISTEN "+store.ChannelUpdates); err != nil {
		t.Fatalf("listen: %v", err)
	}

	if err := s.Notify(ctx, store.ChannelUpdates, "42"); err != nil {
		t.Fatalf("notify: %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n, err := conn.WaitForNotification(waitCtx)
	if err != nil {
		t.Fatalf("wait notification: %v", err)
	}
	if n.Payload != "42" {
		t.Fatalf("payload = %q, want 42", n.Payload)
	}
}

func TestStartListenerDispatches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDSN(t, dsn)

	delivered := make(chan int64, 1)
	typed := make(chan [2]int64, 1)
	evicted := make(chan [2]int64, 1)
	encrypted := make(chan [2]int64, 1)
	_, stop, err := store.StartListener(ctx, dsn,
		func(_ context.Context, userID int64) { delivered <- userID },
		func(_ context.Context, peerID, fromID int64) { typed <- [2]int64{peerID, fromID} },
		func(_ context.Context, userID, authKeyID int64) { evicted <- [2]int64{userID, authKeyID} },
		func(_ context.Context, _ int64) {},
		func(_ context.Context, userID, chatID int64) { encrypted <- [2]int64{userID, chatID} },
		func(context.Context, int64, bool) {},
		func(_ context.Context, _ int64, _ int) {},
		func(context.Context, int64, int64, int64) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	if err := s.Notify(ctx, store.ChannelUpdates, "7"); err != nil {
		t.Fatalf("notify updates: %v", err)
	}
	select {
	case got := <-delivered:
		if got != 7 {
			t.Fatalf("delivered userID = %d, want 7", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliver callback not invoked")
	}

	if err := s.Notify(ctx, store.ChannelTyping, store.TypingPayload(3, 9)); err != nil {
		t.Fatalf("notify typing: %v", err)
	}
	select {
	case got := <-typed:
		if got != [2]int64{3, 9} {
			t.Fatalf("typing = %v, want [3 9]", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("typing callback not invoked")
	}

	// A malformed evict payload must be dropped, not widened into a callback with
	// a partially parsed id: the callback closes live sockets.
	if err := s.Notify(ctx, store.ChannelEvict, "not-a-pair"); err != nil {
		t.Fatalf("notify malformed evict: %v", err)
	}
	if err := s.Notify(ctx, store.ChannelEvict, store.EvictPayload(11, -4242)); err != nil {
		t.Fatalf("notify evict: %v", err)
	}
	select {
	case got := <-evicted:
		if got != [2]int64{11, -4242} {
			t.Fatalf("evict = %v, want [11 -4242]", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("evict callback not invoked")
	}

	// A malformed encryption payload is dropped rather than half-parsed: the
	// callback discloses g_a to whoever it names.
	if err := s.Notify(ctx, store.ChannelEncryption, "not-a-pair"); err != nil {
		t.Fatalf("notify malformed encryption: %v", err)
	}
	if err := s.Notify(ctx, store.ChannelEncryption, store.EncryptionPayload(21, 5)); err != nil {
		t.Fatalf("notify encryption: %v", err)
	}
	select {
	case got := <-encrypted:
		if got != [2]int64{21, 5} {
			t.Fatalf("encryption = %v, want [21 5]", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("encryption callback not invoked")
	}
}

// TestNextBackoff pins the reconnect pacing rule on the two cases a
// notification-counting rule got wrong: a database that flaps must keep backing
// off even while it delivers, and a connection that stayed up while idle must
// not carry a stale penalty into its next failure.
func TestNextBackoff(t *testing.T) {
	t.Parallel()

	// Flapping: every connection dies well before it counts as stable, so the
	// delay grows to the cap however much traffic each one carried.
	got := store.ListenerBackoffMin
	for range 20 {
		got = store.NextBackoff(got, 500*time.Millisecond)
		if got > store.ListenerBackoffMax {
			t.Fatalf("backoff = %v, want at most %v", got, store.ListenerBackoffMax)
		}
	}
	if got != store.ListenerBackoffMax {
		t.Fatalf("backoff after flapping = %v, want the %v cap", got, store.ListenerBackoffMax)
	}

	// Idle recovery: a silent connection that stayed up long enough is healthy,
	// so its failure starts over at the floor rather than at the cap.
	if got := store.NextBackoff(store.ListenerBackoffMax, 2*time.Hour); got != store.ListenerBackoffMin {
		t.Fatalf("backoff after a long idle connection = %v, want %v", got, store.ListenerBackoffMin)
	}
	if got := store.NextBackoff(store.ListenerBackoffMax, store.ListenerStableFor); got != store.ListenerBackoffMin {
		t.Fatalf("backoff at the stability threshold = %v, want %v", got, store.ListenerBackoffMin)
	}

	// A connect attempt that never came up has no uptime to credit.
	if got := store.NextBackoff(store.ListenerBackoffMin, 0); got != 2*store.ListenerBackoffMin {
		t.Fatalf("backoff after a failed dial = %v, want %v", got, 2*store.ListenerBackoffMin)
	}
}

// TestStartListenerReconnectsAfterBackendTermination kills the listener's
// Postgres backend server-side and proves delivery resumes on the same process:
// a dropped connection must not end real-time push until a restart.
func TestStartListenerReconnectsAfterBackendTermination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDSN(t, dsn)

	delivered := make(chan int64, 8)
	evicted := make(chan [2]int64, 8)
	_, stop, err := store.StartListener(ctx, dsn,
		func(_ context.Context, userID int64) { delivered <- userID },
		func(_ context.Context, _, _ int64) {},
		func(_ context.Context, userID, authKeyID int64) { evicted <- [2]int64{userID, authKeyID} },
		func(_ context.Context, _ int64) {},
		func(_ context.Context, _, _ int64) {},
		func(context.Context, int64, bool) {},
		func(_ context.Context, _ int64, _ int) {},
		func(context.Context, int64, int64, int64) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	if err := s.Notify(ctx, store.ChannelUpdates, "7"); err != nil {
		t.Fatalf("notify updates: %v", err)
	}
	select {
	case got := <-delivered:
		if got != 7 {
			t.Fatalf("delivered userID = %d, want 7", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliver callback not invoked before termination")
	}

	terminateListenBackends(t, dsn)

	// Notifications emitted while the listener is reconnecting are lost by
	// design (push is an optimisation over a correct pull), so retry until one
	// lands rather than asserting on a single send.
	deadline := time.Now().Add(30 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("no delivery after the listener backend was terminated")
		}
		if err := s.Notify(ctx, store.ChannelUpdates, "9"); err != nil {
			t.Fatalf("notify after termination: %v", err)
		}
		select {
		case got := <-delivered:
			if got != 9 {
				continue // a queued pre-termination delivery, keep waiting
			}
		case <-time.After(250 * time.Millisecond):
			continue
		}
		break
	}

	// The reconnect re-subscribes every channel, not just the update one: a
	// connection carrying a subset silently drops a whole delivery class.
	if err := s.Notify(ctx, store.ChannelEvict, store.EvictPayload(11, -4242)); err != nil {
		t.Fatalf("notify evict after reconnect: %v", err)
	}
	select {
	case got := <-evicted:
		if got != [2]int64{11, -4242} {
			t.Fatalf("evict = %v, want [11 -4242]", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("evict callback not invoked after reconnect")
	}
}

// TestStartListenerBacksOffWhileDatabaseIsDown proves the two bounds a
// reconnect loop has to hold: a database that stays down is retried on a
// backoff rather than in a tight loop, and stop still returns promptly while
// the loop is waiting out that backoff.
func TestStartListenerBacksOffWhileDatabaseIsDown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	proxy, proxyDSN := startPGProxy(t, pgtest.DSN(t))

	_, stop, err := store.StartListener(ctx, proxyDSN,
		func(_ context.Context, _ int64) {},
		func(_ context.Context, _, _ int64) {},
		func(_ context.Context, _, _ int64) {},
		func(_ context.Context, _ int64) {},
		func(_ context.Context, _, _ int64) {},
		func(context.Context, int64, bool) {},
		func(_ context.Context, _ int64, _ int) {},
		func(context.Context, int64, int64, int64) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}

	proxy.cut()
	time.Sleep(3 * time.Second)
	// 3s of backoff starting at 100ms and doubling is ~5 attempts; a tight
	// reconnect loop would be in the thousands.
	if attempts := proxy.attempts(); attempts > 20 {
		t.Fatalf("reconnect attempts in 3s = %d, want a backed-off handful", attempts)
	}

	stopped := make(chan error, 1)
	go func() { stopped <- stop() }()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatalf("stop while reconnecting: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stop did not return while the listener was in backoff")
	}
}

// TestStartListenerDeliversChannelPost proves the channel-post callback fires
// with the right channelID and that a malformed payload is dropped without
// disturbing delivery on another channel.
func TestStartListenerDeliversChannelPost(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDSN(t, dsn)

	delivered := make(chan int64, 1)
	posted := make(chan int64, 1)
	_, stop, err := store.StartListener(ctx, dsn,
		func(_ context.Context, userID int64) { delivered <- userID },
		func(_ context.Context, _, _ int64) {},
		func(_ context.Context, _, _ int64) {},
		func(_ context.Context, channelID int64) { posted <- channelID },
		func(_ context.Context, _, _ int64) {},
		func(context.Context, int64, bool) {},
		func(_ context.Context, _ int64, _ int) {},
		func(context.Context, int64, int64, int64) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	// Malformed payload must be dropped; the channel-updates callback must still
	// fire on the next valid notification.
	if err := s.Notify(ctx, store.ChannelPost, "not-an-int"); err != nil {
		t.Fatalf("notify malformed channel post: %v", err)
	}
	if err := s.Notify(ctx, store.ChannelUpdates, "5"); err != nil {
		t.Fatalf("notify updates: %v", err)
	}
	select {
	case got := <-delivered:
		if got != 5 {
			t.Fatalf("delivered userID = %d, want 5", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deliver callback not invoked after malformed channel-post payload")
	}

	// Valid channel-post payload must reach the callback.
	if err := s.Notify(ctx, store.ChannelPost, store.ChannelPostPayload(42)); err != nil {
		t.Fatalf("notify channel post: %v", err)
	}
	select {
	case got := <-posted:
		if got != 42 {
			t.Fatalf("channelPost channelID = %d, want 42", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("channelPost callback not invoked")
	}
}

// TestStartListenerDeliversStatus proves the status callback fires with the
// right userID and online value, and that a malformed payload is dropped.
func TestStartListenerDeliversStatus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openDSN(t, dsn)

	statused := make(chan [2]bool, 2) // [userID==7, online]
	_, stop, err := store.StartListener(ctx, dsn,
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(_ context.Context, userID int64, online bool) { statused <- [2]bool{userID == 7, online} },
		func(_ context.Context, _ int64, _ int) {},
		func(context.Context, int64, int64, int64) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	defer func() {
		if err := stop(); err != nil {
			t.Errorf("stop: %v", err)
		}
	}()

	// Malformed payload must be dropped.
	if err := s.Notify(ctx, store.ChannelStatus, "not-a-pair"); err != nil {
		t.Fatalf("notify malformed status: %v", err)
	}

	// Valid online payload.
	if err := s.Notify(ctx, store.ChannelStatus, store.StatusPayload(7, true)); err != nil {
		t.Fatalf("notify status online: %v", err)
	}
	select {
	case got := <-statused:
		if !got[0] || !got[1] {
			t.Fatalf("status = user7=%v, online=%v, want true, true", got[0], got[1])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("status callback not invoked for online")
	}

	// Valid offline payload.
	if err := s.Notify(ctx, store.ChannelStatus, store.StatusPayload(7, false)); err != nil {
		t.Fatalf("notify status offline: %v", err)
	}
	select {
	case got := <-statused:
		if !got[0] || got[1] {
			t.Fatalf("status = user7=%v, online=%v, want true, false", got[0], got[1])
		}
	case <-time.After(5 * time.Second):
		t.Fatal("status callback not invoked for offline")
	}
}

// TestStatusPayloadFormat pins the payload shape: "<userID>|<1 or 0>".
func TestStatusPayloadFormat(t *testing.T) {
	t.Parallel()
	if got := store.StatusPayload(42, true); got != "42|1" {
		t.Fatalf("StatusPayload(42, true) = %q, want 42|1", got)
	}
	if got := store.StatusPayload(42, false); got != "42|0" {
		t.Fatalf("StatusPayload(42, false) = %q, want 42|0", got)
	}
}

// terminateListenBackends kills every Postgres backend holding a LISTEN on this
// test's database, the server-side equivalent of the connection dropping.
func terminateListenBackends(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	rows, err := conn.Query(ctx, `SELECT pid FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE 'LISTEN %'`)
	if err != nil {
		t.Fatalf("query listen backends: %v", err)
	}
	pids, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		t.Fatalf("collect listen backends: %v", err)
	}
	if len(pids) == 0 {
		t.Fatal("no LISTEN backend found to terminate")
	}
	for _, pid := range pids {
		if _, err := conn.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
			t.Fatalf("terminate backend %d: %v", pid, err)
		}
	}
}

// pgProxy is a TCP passthrough in front of Postgres that a test can cut. After
// the cut every connection is closed on accept, which is what a listener facing
// a down database sees, and attempts counts the dials so a test can tell a
// backed-off retry from a spin.
type pgProxy struct {
	target string
	ln     net.Listener

	mu      sync.Mutex
	down    bool
	dials   int
	live    []net.Conn
	closers sync.WaitGroup
}

// startPGProxy listens on a loopback port forwarding to the Postgres behind dsn
// and returns the proxy plus a dsn pointing at it.
func startPGProxy(t *testing.T, dsn string) (*pgProxy, string) {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &pgProxy{target: u.Host, ln: ln}
	t.Cleanup(p.close)
	go p.serve()

	u.Host = ln.Addr().String()
	return p, u.String()
}

func (p *pgProxy) serve() {
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.dials++
		down := p.down
		p.mu.Unlock()
		if down {
			closeQuiet(conn)
			continue
		}
		p.closers.Add(1)
		go p.pipe(conn)
	}
}

func (p *pgProxy) pipe(client net.Conn) {
	defer p.closers.Done()
	var d net.Dialer
	server, err := d.DialContext(context.Background(), "tcp", p.target)
	if err != nil {
		closeQuiet(client)
		return
	}
	p.mu.Lock()
	if p.down { // cut landed between the accept and this dial
		p.mu.Unlock()
		closeQuiet(client)
		closeQuiet(server)
		return
	}
	p.live = append(p.live, client, server)
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Go(func() { copyQuiet(server, client) })
	wg.Go(func() { copyQuiet(client, server) })
	wg.Wait()
}

// cut takes the database away: live connections are dropped and every later one
// is closed on accept, so reconnect attempts still count as dials.
func (p *pgProxy) cut() {
	p.mu.Lock()
	p.down = true
	live := p.live
	p.live = nil
	p.mu.Unlock()

	for _, c := range live {
		closeQuiet(c)
	}
}

// close cuts the proxy, stops accepting, and drains the forwarding goroutines.
func (p *pgProxy) close() {
	p.cut()
	_ = p.ln.Close() //nolint:errcheck // best-effort teardown
	p.closers.Wait()
}

func (p *pgProxy) attempts() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dials
}

func closeQuiet(c net.Conn) {
	_ = c.Close() //nolint:errcheck // proxy teardown, nothing to recover
}

func copyQuiet(dst, src net.Conn) {
	_, _ = io.Copy(dst, src) //nolint:errcheck // proxy plumbing, ends when either side closes
	closeQuiet(dst)
}
