package mtproto_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// A key exchange used to buy a hold that nothing counted. The pre-auth bounds
// end at the first frame that decrypts under a key this server issued, and the
// per-user connection cap counts only signed-in sessions, so a key that never
// signs in sat between the two: one exchange, ever, and then as many concurrent
// connections from as many addresses as the peer cared to open. These tests are
// that gap, and the two counterweights that must survive closing it — the
// client waiting on a human to read a login code, and the session that has
// since signed in.

// TestUnboundKeyCapBoundsWhatOneKeyHolds drives the reuse pattern itself: many
// sockets, one key that never signs in, each socket sending frames that decrypt
// under it. Past the cap the connections are closed, the ones inside it are
// left alone, and a slot comes back when its connection ends.
func TestUnboundKeyCapBoundsWhatOneKeyHolds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const (
		maxHeld = 3
		opened  = 6
	)
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	keys := newSignInStore()
	seen := serveKeys(t, ctx, nl, keys, maxHeld)
	addr := nl.Addr().String()

	raws := make([]net.Conn, opened)
	conns := make([]transport.Conn, opened)
	for i := range raws {
		raws[i], conns[i] = keyClient(t, ctx, addr, keys.key, int64(i+1))
	}
	// Every frame is dispatched before the cap decides anything, so waiting for
	// all of them makes what follows a statement about the bound rather than
	// about how far the server had got.
	wantRequests(t, seen, opened)

	var live []int
	for i, raw := range raws {
		if !closedByServer(t, raw, 2*time.Second) {
			live = append(live, i)
		}
	}
	if len(live) != maxHeld {
		t.Fatalf("%d of %d connections under one never-signed-in key are still held, want %d", len(live), opened, maxHeld)
	}

	// The survivors are ordinary connections, and they keep being served: this
	// bounds the population under a key without taking the sockets inside it
	// away. The frames are the keepalives the hold was built out of, well inside
	// the read deadline.
	for _, i := range live {
		sendFrame(t, ctx, conns[i], keys.key, int64(i+1), int64(2)<<32)
	}
	wantRequests(t, seen, len(live))

	// A slot comes back when its connection ends, or the cap would be a budget
	// spent once per key rather than a bound on what is held at once.
	if err := raws[live[0]].Close(); err != nil {
		t.Fatalf("close held connection: %v", err)
	}
	waitKeyAdmitted(t, ctx, addr, keys.key)
}

// TestUnboundKeyCapLeavesAClientWaitingToSignInAlone is the first
// counterweight. The legitimate holder of a key with no user on it is a client
// between key exchange and a human reading a login code, and it holds one
// connection. At the smallest cap that can be configured it is served, stays
// served while it waits, and is never closed.
func TestUnboundKeyCapLeavesAClientWaitingToSignInAlone(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	keys := newSignInStore()
	seen := serveKeys(t, ctx, nl, keys, 1)

	const session = int64(5)
	raw, conn := keyClient(t, ctx, nl.Addr().String(), keys.key, session)
	wantRequests(t, seen, 1)

	// It waits, signing in to nothing, and every frame it sends meanwhile is
	// answered: a connection is charged once, not once per frame.
	for i := range 3 {
		sendFrame(t, ctx, conn, keys.key, session, int64(i+2)<<32)
		wantRequests(t, seen, 1)
	}
	if closedByServer(t, raw, time.Second) {
		t.Fatal("a client that completed key exchange and then waited without signing in was closed at the unbound-key cap")
	}
}

// TestUnboundKeyCapReleasesASignedInConnectionForGood is the second, and it is
// the one-way property the pre-auth bounds already have: what a signed-in
// client may hold is the per-user connection cap's to decide, so a connection
// whose key has resolved to a user must never be refused or closed by this
// bound again — not when its key is unbound afterwards, which the 2FA path
// does, and not when the slot it gave back has since gone to somebody else.
func TestUnboundKeyCapReleasesASignedInConnectionForGood(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	keys := newSignInStore()
	seen := serveKeys(t, ctx, nl, keys, 1)
	addr := nl.Addr().String()

	// The first connection takes the key's only slot while nobody has signed in
	// on it.
	const session = int64(9)
	signedIn, conn := keyClient(t, ctx, addr, keys.key, session)
	wantRequests(t, seen, 1)

	// auth.signIn lands: the key now names a user, and this connection's next
	// frame takes it out from under the bound and gives the slot back.
	keys.bind(7)
	sendFrame(t, ctx, conn, keys.key, session, int64(2)<<32)
	wantRequests(t, seen, 1)

	// The key is unbound again, and another connection takes the freed slot —
	// so the key is at its cap once more, with the signed-in connection outside
	// it.
	keys.bind(0)
	other, _ := keyClient(t, ctx, addr, keys.key, session+1)
	wantRequests(t, seen, 1)
	if closedByServer(t, other, time.Second) {
		t.Fatal("the slot a signed-in connection released was never given back")
	}

	sendFrame(t, ctx, conn, keys.key, session, int64(3)<<32)
	wantRequests(t, seen, 1)
	if closedByServer(t, signedIn, time.Second) {
		t.Fatal("a connection whose key had signed in was later closed at the unbound-key cap")
	}
}

// TestUnboundKeyCapCountsTwoKeysIndependently proves the cap is per key,
// not per process: two different keys, each at the cap, hold twice the cap
// in total.
func TestUnboundKeyCapCountsTwoKeysIndependently(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const maxHeld = 2
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	keys := newTwoKeyStore()
	seen := serveKeys(t, ctx, nl, keys, maxHeld)
	addr := nl.Addr().String()

	// Open maxHeld connections on key A and maxHeld on key B.
	rawsA := make([]net.Conn, maxHeld)
	connsA := make([]transport.Conn, maxHeld)
	for i := range rawsA {
		rawsA[i], connsA[i] = keyClient(t, ctx, addr, keys.keyA, int64(1+i))
	}
	wantRequests(t, seen, maxHeld)

	rawsB := make([]net.Conn, maxHeld)
	connsB := make([]transport.Conn, maxHeld)
	for i := range rawsB {
		rawsB[i], connsB[i] = keyClient(t, ctx, addr, keys.keyB, int64(10+i))
	}
	wantRequests(t, seen, maxHeld)

	// Both keys hold maxHeld connections — 2×maxHeld in total.
	liveA := 0
	for _, raw := range rawsA {
		if !closedByServer(t, raw, 2*time.Second) {
			liveA++
		}
	}
	liveB := 0
	for _, raw := range rawsB {
		if !closedByServer(t, raw, 2*time.Second) {
			liveB++
		}
	}
	if liveA != maxHeld {
		t.Fatalf("key A: %d connections held, want %d", liveA, maxHeld)
	}
	if liveB != maxHeld {
		t.Fatalf("key B: %d connections held, want %d", liveB, maxHeld)
	}

	// A third connection on key A is refused; one on key B is also refused.
	// (They are each at cap.)
	refusedA, _ := keyClient(t, ctx, addr, keys.keyA, 100)
	wantRequests(t, seen, 1)
	if !closedByServer(t, refusedA, 2*time.Second) {
		t.Fatal("key A: cap did not refuse the N+1th connection")
	}
	refusedB, _ := keyClient(t, ctx, addr, keys.keyB, 110)
	wantRequests(t, seen, 1)
	if !closedByServer(t, refusedB, 2*time.Second) {
		t.Fatal("key B: cap did not refuse the N+1th connection")
	}

	// Survivors keep being served.
	for _, c := range connsA[:liveA] {
		sendFrame(t, ctx, c, keys.keyA, 1, 999<<32)
	}
	for _, c := range connsB[:liveB] {
		sendFrame(t, ctx, c, keys.keyB, 10, 999<<32)
	}
	wantRequests(t, seen, liveA+liveB)
}

// TestUnboundKeyCapZeroDisablesIt proves zero is "cap off": connections on the
// same never-signed-in key are not refused, matching the pre-auth bounds where
// zero disables the bound.
func TestUnboundKeyCapZeroDisablesIt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const opened = 8
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	keys := newSignInStore()
	seen := serveKeys(t, ctx, nl, keys, 0)
	addr := nl.Addr().String()

	raws := make([]net.Conn, opened)
	conns := make([]transport.Conn, opened)
	for i := range raws {
		raws[i], conns[i] = keyClient(t, ctx, addr, keys.key, int64(1+i))
	}
	wantRequests(t, seen, opened)

	// With cap=0, nothing is refused: send a second frame on each connection
	// and confirm the handler receives them all.
	for i := range conns {
		sendFrame(t, ctx, conns[i], keys.key, int64(1+i), int64(2)<<32)
	}
	wantRequests(t, seen, opened)

	// Then verify each one is still open.
	for i, raw := range raws {
		if closedByServer(t, raw, 2*time.Second) {
			t.Fatalf("connection %d closed at cap=0 (cap should be disabled)", i)
		}
	}
}

// TestSetMaxConnsPerUnboundKeyRefusesANegativeCap covers the difference between
// the two ways of writing "no bound", the same way the pre-auth bounds do: zero
// is a decision and is honoured, negative is a typo or a unit mistake and is
// refused, because read as equal it would silently disable the bound it was
// meant to set.
func TestSetMaxConnsPerUnboundKeyRefusesANegativeCap(t *testing.T) {
	t.Parallel()

	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	if err := srv.SetMaxConnsPerUnboundKey(-1); err == nil {
		t.Error("SetMaxConnsPerUnboundKey(-1) was accepted, which turns the cap off")
	}
	if err := srv.SetMaxConnsPerUnboundKey(0); err != nil {
		t.Errorf("SetMaxConnsPerUnboundKey(0) refused the cap being turned off: %v", err)
	}
}

// signInStore is an AuthKeyStore holding one key whose bound user a test moves
// while connections under it are live: bind(7) is an auth.signIn landing,
// bind(0) the 2FA path putting the key back to pending. Every connection's
// serve goroutine looks the binding up, so it is guarded.
type signInStore struct {
	key crypto.AuthKey

	mu     sync.Mutex
	userID int64
}

func newSignInStore() *signInStore { return &signInStore{key: rebindTestKey()} }

func (s *signInStore) bind(userID int64) {
	s.mu.Lock()
	s.userID = userID
	s.mu.Unlock()
}

func (s *signInStore) Save(context.Context, crypto.AuthKey) error { return nil }
func (s *signInStore) Touch(context.Context, [8]byte) error       { return nil }

func (s *signInStore) Get(context.Context, [8]byte) (crypto.AuthKey, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.key, s.userID, true, nil
}

// twoKeyStore is an AuthKeyStore holding two distinct keys, neither bound to
// a user. It resolves by auth key ID so each key is looked up independently.
type twoKeyStore struct {
	keyA crypto.AuthKey
	keyB crypto.AuthKey
}

func newTwoKeyStore() *twoKeyStore {
	return &twoKeyStore{keyA: rebindTestKey(), keyB: makeTestKey(0xAA)}
}

func (s *twoKeyStore) Save(_ context.Context, _ crypto.AuthKey) error { return nil }
func (s *twoKeyStore) Touch(_ context.Context, _ [8]byte) error       { return nil }

func (s *twoKeyStore) Get(_ context.Context, id [8]byte) (crypto.AuthKey, int64, bool, error) {
	if id == s.keyA.ID {
		return s.keyA, 0, true, nil
	}
	if id == s.keyB.ID {
		return s.keyB, 0, true, nil
	}
	return crypto.AuthKey{}, 0, false, nil
}

// makeTestKey returns a deterministic auth key with all bytes set to b,
// distinct from rebindTestKey.
func makeTestKey(b byte) crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = b
	}
	return raw.WithID()
}

// serveKeys runs a server on ln whose auth keys come from keys and whose
// unbound-key cap is max, signalling every request that reaches the handler. It
// is serveHandler with the store left to the caller: this bound turns on the
// user a key is bound to, which the memory store never has one of.
func serveKeys(t *testing.T, ctx context.Context, ln net.Listener, keys mtproto.AuthKeyStore, maxConns int) <-chan struct{} {
	t.Helper()
	seen := make(chan struct{}, 64)
	handler := mtproto.HandlerFunc(func(_ *mtproto.Conn, _ *mtproto.Request) error {
		select {
		case seen <- struct{}{}:
		default:
		}
		return nil
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, keys, handler, nil)
	if err := srv.SetMaxConnsPerUnboundKey(maxConns); err != nil {
		t.Fatalf("set unbound-key cap: %v", err)
	}
	srvCtx, stop := context.WithCancel(ctx)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(srvCtx, ln) }()
	t.Cleanup(func() {
		stop()
		if err := <-served; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return seen
}

// keyClient opens a connection and uses a key the server already holds, which
// is what a client does with the key its exchange produced. The frame is the
// one that proves the key, and so the one this cap counts.
func keyClient(t *testing.T, ctx context.Context, addr string, key crypto.AuthKey, session int64) (net.Conn, transport.Conn) {
	t.Helper()
	raw := dialClient(t, ctx, addr)
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		t.Fatalf("transport handshake: %v", err)
	}
	sendFrame(t, ctx, conn, key, session, int64(1)<<32)
	return raw, conn
}

// wantRequests waits for n requests to reach the handler, bounded so a
// connection the server dropped fails the test rather than running it out.
func wantRequests(t *testing.T, seen <-chan struct{}, n int) {
	t.Helper()
	for i := range n {
		select {
		case <-seen:
		case <-time.After(15 * time.Second):
			t.Fatalf("only %d of %d frames reached the handler", i, n)
		}
	}
}

// closedByServer reports whether the server dropped conn within grace,
// discarding whatever it wrote first: a connection past this cap has its frame
// answered before it is closed, so bytes arriving are not the connection
// surviving.
func closedByServer(t *testing.T, conn net.Conn, grace time.Duration) bool {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(grace)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 512)
	for {
		switch _, err := conn.Read(buf); {
		case err == nil:
		case isTimeout(err):
			// Left readable for whatever the test does with it next.
			if err := conn.SetReadDeadline(time.Time{}); err != nil {
				t.Fatalf("clear read deadline: %v", err)
			}
			return false
		default:
			return true
		}
	}
}

// waitKeyAdmitted waits until a fresh connection under key is served and kept.
// A slot comes back when the connection holding it ends, which the server
// notices on its own schedule — the one genuinely asynchronous thing here, and
// so the one waited for rather than asserted outright.
func waitKeyAdmitted(t *testing.T, ctx context.Context, addr string, key crypto.AuthKey) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		raw, _ := keyClient(t, ctx, addr, key, int64(1000+attempt))
		if !closedByServer(t, raw, time.Second) {
			return
		}
	}
	t.Fatal("no connection was admitted under the key after one of its slots was released")
}
