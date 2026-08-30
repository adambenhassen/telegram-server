package mtproto_test

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// What an unauthenticated peer may hold is bounded by three numbers, and each
// one is asserted here against the hold it exists to end: the global cap against
// a peer opening sockets faster than they are negotiated, the per-address cap
// against one client doing it behind a balancer, and the lifetime ceiling
// against the connection that stays inside every deadline by dripping frames.
//
// The counterweight is asserted too. A bound that also ends established sessions
// would pass every test above and be unshippable.

// TestPreAuthGlobalCapRefusesBeforeNegotiating covers the first bound and the
// cost rule that comes with it: past the cap a socket is closed on the accept
// loop, without a goroutine, a deadline or a single read spent on it. A refusal
// that cost as much as an acceptance would be a way of applying load rather than
// shedding it.
func TestPreAuthGlobalCapRefusesBeforeNegotiating(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	tap := &tapListener{Listener: nl}
	// Two slots, no other bound: the cap alone decides every outcome below.
	serveClients(t, ctx, tap, nil, nil, withPreAuthLimits(t, mtproto.PreAuthLimits{MaxConns: 2}))
	addr := nl.Addr().String()

	holds := make([]net.Conn, 2)
	for i := range holds {
		holds[i] = holdPreAuth(t, ctx, addr, nil)
	}

	refused := dialClient(t, ctx, addr)
	assertClosed(t, refused)
	if n := tap.reads(t, 2); n != 0 {
		t.Errorf("the refused socket was read from %d times, want 0: the refusal must land before negotiation is paid for", n)
	}

	// A slot comes back when its connection ends, or the cap would be a
	// one-shot budget rather than a bound on what is held at once.
	if err := holds[0].Close(); err != nil {
		t.Fatalf("close held connection: %v", err)
	}
	waitServed(t, ctx, addr, nil)
}

// TestPreAuthPerAddrCapKeysOnTheBalancerAddress covers the second bound, and the
// only thing about it that can be got wrong quietly. Behind an L4 balancer every
// socket's peer is the balancer, so a cap keyed on the socket is a global cap
// wearing a per-client name: the first flooder to fill it locks out every other
// client behind the same balancer. Keying on the address the balancer reports is
// what makes the bound mean what it says.
func TestPreAuthPerAddrCapKeysOnTheBalancerAddress(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	// One slot per client address and no global bound, so nothing but the
	// per-address cap can refuse anything here.
	serveClients(t, ctx, nl, loopback, nil, withPreAuthLimits(t, mtproto.PreAuthLimits{MaxConnsPerNet: 1}))
	addr := nl.Addr().String()

	flooder := netip.MustParseAddr("203.0.113.9")
	holdPreAuth(t, ctx, addr, proxyV2Header(proxyCmdProxy, flooder))

	// A second connection reporting the same client is refused.
	served, err := probeServed(ctx, addr, proxyV2Header(proxyCmdProxy, flooder))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if served {
		t.Errorf("a second connection from %s was served past a per-address cap of 1", flooder)
	}

	// The same balancer, the same loopback peer address, a different client:
	// served. A cap keyed on the socket would have refused this one too.
	other := netip.MustParseAddr("198.51.100.7")
	served, err = probeServed(ctx, addr, proxyV2Header(proxyCmdProxy, other))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !served {
		t.Errorf("a second client behind the same balancer was refused: the cap is keyed on the socket peer, not on %s", other)
	}
}

// TestPreAuthPerNetCapBucketsIPv6ByPrefix covers what "one client" means for
// IPv6, and it is not one address. A host on a routed allocation mints fresh
// addresses inside its own /64 for free, so a cap keyed on the address does not
// bind for it at all: it would open one connection from each of 1024 addresses
// and fill the global cap alone, after which every other client is refused on
// the accept loop. The per-IP rate limits already key on the /64 for this
// reason, and this cap has to count the same peer as the same peer.
func TestPreAuthPerNetCapBucketsIPv6ByPrefix(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	serveClients(t, ctx, nl, loopback, nil, withPreAuthLimits(t, mtproto.PreAuthLimits{MaxConnsPerNet: 1}))
	addr := nl.Addr().String()

	holdPreAuth(t, ctx, addr, proxyV2Header(proxyCmdProxy, netip.MustParseAddr("2001:db8:1:1::1")))

	// A different address, the same /64: the same host, and refused.
	sameNet := netip.MustParseAddr("2001:db8:1:1::2")
	served, err := probeServed(ctx, addr, proxyV2Header(proxyCmdProxy, sameNet))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if served {
		t.Errorf("%s was served past a cap of 1 already held inside its own /64: the cap is keyed on the address, which an IPv6 host mints for free", sameNet)
	}

	// A different /64 is a different peer, and is served.
	otherNet := netip.MustParseAddr("2001:db8:1:2::1")
	served, err = probeServed(ctx, addr, proxyV2Header(proxyCmdProxy, otherNet))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !served {
		t.Errorf("%s was refused although no connection is held in its /64", otherNet)
	}
}

// TestPreAuthNoClientAddressIsChargedToNoNetwork pins the exemption in the
// per-network cap, because an exemption nobody has written down is a hole
// somebody finds. A header that names no client — the LOCAL command a balancer's
// health check sends, or AF_UNSPEC — is charged to no bucket at all, so those
// connections are bounded by the global cap and the ceiling alone.
//
// It is the right call for health checks and the wrong list to be generous with:
// everything inside the allowlisted CIDRs can send such a header, so the
// allowlist should name the balancers rather than a whole network. That is what
// the docs entry says, and this is the behaviour it describes.
func TestPreAuthNoClientAddressIsChargedToNoNetwork(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	serveClients(t, ctx, nl, loopback, nil, withPreAuthLimits(t, mtproto.PreAuthLimits{MaxConnsPerNet: 1}))
	addr := nl.Addr().String()

	// One held connection per header that names no client, both well past a cap
	// of one, and neither refused.
	local := proxyV2Header(proxyCmdLocal, netip.MustParseAddr("203.0.113.9"))
	unspec := proxyV2Header(proxyCmdProxy, netip.Addr{})
	holdPreAuth(t, ctx, addr, local)
	for _, prelude := range [][]byte{local, unspec} {
		served, err := probeServed(ctx, addr, prelude)
		if err != nil {
			t.Fatalf("probe: %v", err)
		}
		if !served {
			t.Error("a connection carrying no client address was refused at the per-network cap: it is charged to no bucket, so it cannot fill one")
		}
	}

	// The exemption is the address, not the cap: a header that does name a
	// client is still counted.
	client := netip.MustParseAddr("198.51.100.7")
	holdPreAuth(t, ctx, addr, proxyV2Header(proxyCmdProxy, client))
	served, err := probeServed(ctx, addr, proxyV2Header(proxyCmdProxy, client))
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if served {
		t.Errorf("a second connection from %s was served past a cap of 1", client)
	}
}

// TestSetPreAuthLimitsRefusesNegativeBounds covers the difference between the
// two ways of writing "no bound": zero is a decision and is honoured, negative
// is a typo or a unit mistake and is refused. Read as equal, a negative would
// disable the bound it was meant to set, which is silent and is the failure
// every layer of this surface is written to avoid.
func TestSetPreAuthLimitsRefusesNegativeBounds(t *testing.T) {
	t.Parallel()

	for name, limits := range map[string]mtproto.PreAuthLimits{
		"negative global cap":  {MaxConns: -1},
		"negative network cap": {MaxConnsPerNet: -1},
		"negative lifetime":    {Lifetime: -time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
			if err := srv.SetPreAuthLimits(limits); err == nil {
				t.Errorf("SetPreAuthLimits(%+v) accepted a negative bound, which turns it off", limits)
			}
		})
	}

	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	if err := srv.SetPreAuthLimits(mtproto.PreAuthLimits{}); err != nil {
		t.Errorf("SetPreAuthLimits refused every bound turned off: %v", err)
	}
}

// TestPreAuthLifetimeClosesAFrameDripHold covers the third bound against the
// hold that no deadline catches. Every read the server does re-derives its own
// deadline, so a peer that sends one small frame just inside each of them keeps
// a socket, a goroutine and a negotiated codec for as long as it likes, at the
// cost of a few bytes a minute. Only a ceiling measured from accept ends it.
func TestPreAuthLifetimeClosesAFrameDripHold(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const lifetime = 750 * time.Millisecond
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	serveClients(t, ctx, nl, nil, nil, withPreAuthLimits(t, mtproto.PreAuthLimits{Lifetime: lifetime}))

	raw := dialClient(t, ctx, nl.Addr().String())
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		t.Fatalf("transport handshake: %v", err)
	}

	// Well past the ceiling, so a connection that survives the loop survived it
	// on merit rather than on the loop running out first.
	deadline := time.Now().Add(20 * lifetime)
	for time.Now().Before(deadline) {
		frameCtx, cancelFrame := context.WithTimeout(ctx, 5*time.Second)
		err := dripFrame(frameCtx, conn)
		cancelFrame()
		if err != nil {
			// The server closed the connection: the ceiling fired.
			return
		}
		time.Sleep(lifetime / 5)
	}
	t.Fatal("a connection dripping frames without ever authenticating was never closed")
}

// TestPreAuthAuthenticatedConnectionOutlivesTheCeiling is the counterweight to
// all three bounds: they are pre-auth bounds, and a connection that has proved
// possession of a key this server issued is out from under every one of them.
// It keeps being served past the ceiling, and the slot it held while it was
// still anonymous is back — asserted at a cap of one, where a slot never
// returned would refuse everybody after the first client.
func TestPreAuthAuthenticatedConnectionOutlivesTheCeiling(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	// Long enough that a loaded machine cannot expire it between the accept and
	// the first frame, short enough that the sleep below is well past it.
	const lifetime = 2 * time.Second
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	seen, key := serveClients(t, ctx, nl, nil, nil, withPreAuthLimits(t, mtproto.PreAuthLimits{
		MaxConns:       1,
		MaxConnsPerNet: 1,
		Lifetime:       lifetime,
	}))

	raw := dialClient(t, ctx, nl.Addr().String())
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		t.Fatalf("transport handshake: %v", err)
	}
	const session = int64(7)
	sendFrame(t, ctx, conn, key, session, int64(1)<<32)
	wantClientAddr(t, ctx, seen)

	time.Sleep(3 * lifetime)
	sendFrame(t, ctx, conn, key, session, int64(2)<<32)
	select {
	case <-seen:
	case <-ctx.Done():
		t.Fatal("an established session stopped being served at the pre-auth lifetime ceiling")
	}

	waitServed(t, ctx, nl.Addr().String(), nil)
}

// TestPreAuthCeilingEndsAtTheProvenKeyNotAtTheHandler pins where the pre-auth
// state ends inside a frame: at the MAC verifying, not at the dispatch it leads
// to. A handler slower than what is left of the ceiling would otherwise have its
// connection closed underneath it — a socket that had already proved a
// server-issued key, killed by a bound on the sockets that have not.
func TestPreAuthCeilingEndsAtTheProvenKeyNotAtTheHandler(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	const lifetime = 500 * time.Millisecond
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	// The handler outlasts the ceiling several times over, so a build that
	// cleared the slot after the dispatch instead of before it loses this
	// connection while the handler is still in it.
	seen, key := serveBlocking(t, ctx, nl, 4*lifetime, withPreAuthLimits(t, mtproto.PreAuthLimits{Lifetime: lifetime}))

	raw := dialClient(t, ctx, nl.Addr().String())
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		t.Fatalf("transport handshake: %v", err)
	}
	const session = int64(11)
	sendFrame(t, ctx, conn, key, session, int64(1)<<32)
	wantHandled(t, seen, "the slow handler's own frame")

	// The connection outlived its own handler: a second frame is still served.
	sendFrame(t, ctx, conn, key, session, int64(2)<<32)
	wantHandled(t, seen, "a frame after the slow handler returned")
}

// serveBlocking runs a server whose handler sleeps for block before recording
// the request, so a test can hold a connection inside a dispatch for longer than
// a bound allows.
func serveBlocking(t *testing.T, ctx context.Context, ln net.Listener, block time.Duration, opts ...func(*mtproto.Server)) (<-chan netip.Addr, crypto.AuthKey) {
	t.Helper()
	seen := make(chan netip.Addr, 8)
	handler := mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
		time.Sleep(block)
		select {
		case seen <- req.ClientAddr:
		default:
		}
		return nil
	})
	return seen, serveHandler(t, ctx, ln, nil, nil, handler, opts...)
}

// wantHandled waits for one request to reach the handler, bounded so a
// connection the server dropped fails the test rather than running it out.
func wantHandled(t *testing.T, seen <-chan netip.Addr, what string) {
	t.Helper()
	select {
	case <-seen:
	case <-time.After(15 * time.Second):
		t.Fatalf("%s never reached the handler", what)
	}
}

// holdPreAuth opens a connection that holds its pre-auth slot and keeps holding
// it: it negotiates a codec and takes one protocol error back, which is the
// server saying it has established this connection's client address, charged it
// to the bounds, and gone on to wait for a frame that will never authenticate.
//
// Waiting for that answer is what makes the assertions after it exact rather
// than timed. A slot is taken in the connection's own goroutine, so a test that
// merely wrote bytes and moved on would be asserting against a bound that had
// not been applied yet.
func holdPreAuth(t *testing.T, ctx context.Context, addr string, prelude []byte) net.Conn {
	t.Helper()
	raw := dialClient(t, ctx, addr)
	mustWrite(t, raw, prelude)
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		t.Fatalf("transport handshake: %v", err)
	}
	frameCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := dripFrame(frameCtx, conn); err != nil {
		t.Fatalf("the connection meant to hold a pre-auth slot was not served: %v", err)
	}
	return raw
}

// withPreAuthLimits configures a test server's pre-auth bounds. Every field is
// set explicitly by the caller, defaults included, so a test states the whole
// policy it is asserting against.
func withPreAuthLimits(t *testing.T, l mtproto.PreAuthLimits) func(*mtproto.Server) {
	t.Helper()
	return func(s *mtproto.Server) {
		if err := s.SetPreAuthLimits(l); err != nil {
			t.Fatalf("set pre-auth limits: %v", err)
		}
	}
}

// sendFrame writes one encrypted request under key, which is what an
// authenticated client's traffic looks like from the server's side.
func sendFrame(t *testing.T, ctx context.Context, conn transport.Conn, key crypto.AuthKey, session, msgID int64) {
	t.Helper()
	frame := clientFrame(t, key, session, msgID, &tg.HelpGetConfigRequest{})
	if err := conn.Send(ctx, &bin.Buffer{Buf: slices.Clone(frame)}); err != nil {
		t.Fatalf("send frame: %v", err)
	}
}

// dripFrame sends one frame bearing an auth key id the server does not know and
// waits for the protocol error it answers with. It reports an error once the
// connection is gone, which is the only outcome that is not the server keeping
// the hold alive.
func dripFrame(ctx context.Context, conn transport.Conn) error {
	var out bin.Buffer
	out.Put(make([]byte, 16))
	out.Buf[0] = 1 // Not a key exchange, and not a key the server holds.
	if err := conn.Send(ctx, &out); err != nil {
		return err
	}
	var in bin.Buffer
	err := conn.Recv(ctx, &in)
	if hasProtocolError(err) {
		return nil
	}
	if err == nil {
		return nil
	}
	return err
}

func hasProtocolError(err error) bool {
	protoErr, ok := errors.AsType[*codec.ProtocolErr](err)
	return ok && protoErr != nil
}

// probeServed opens a connection, negotiates a codec and sends a frame the
// server answers with a protocol error, reporting whether the answer came. It is
// how a refusal is told from an acceptance at the client end: a connection
// refused at a pre-auth bound is closed without ever answering.
//
// The probe holds a pre-auth slot while it is open and gives it back on the way
// out, so it observes the bounds without moving them.
func probeServed(ctx context.Context, addr string, prelude []byte) (bool, error) {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false, err
	}
	defer raw.Close() //nolint:errcheck // the probe's own socket, closed either end.

	if len(prelude) > 0 {
		if _, err := raw.Write(prelude); err != nil {
			return false, nil
		}
	}
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return dripFrame(ctx, conn) == nil, nil
}

// waitServed waits until a connection is admitted. A slot comes back when the
// connection holding it ends, which the server notices on its own schedule —
// the one thing here that is genuinely asynchronous, and so the one thing
// waited for rather than asserted outright.
func waitServed(t *testing.T, ctx context.Context, addr string, prelude []byte) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		served, err := probeServed(ctx, addr, prelude)
		if err != nil {
			t.Fatalf("probe: %v", err)
		}
		if served {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no connection was admitted after a pre-auth slot was released")
}

// tapListener counts the reads the server performs on every socket it hands
// over, so a test can tell a connection refused on the accept loop — closed with
// nothing read from it — from one the server negotiated with.
type tapListener struct {
	net.Listener

	mu    sync.Mutex
	conns []*tapConn
}

func (l *tapListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	tapped := &tapConn{Conn: conn}
	l.mu.Lock()
	l.conns = append(l.conns, tapped)
	l.mu.Unlock()
	return tapped, nil
}

// reads reports how many times the server read from the i-th socket it
// accepted. Accepts are sequential, so the index is the connection's order.
func (l *tapListener) reads(t *testing.T, i int) int64 {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	if i >= len(l.conns) {
		t.Fatalf("connection %d was never accepted; %d were", i, len(l.conns))
	}
	return l.conns[i].reads.Load()
}

type tapConn struct {
	net.Conn

	reads atomic.Int64
}

func (c *tapConn) Read(b []byte) (int, error) {
	c.reads.Add(1)
	return c.Conn.Read(b)
}
