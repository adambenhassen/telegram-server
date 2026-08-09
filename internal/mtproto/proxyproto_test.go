package mtproto_test

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"net/netip"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// loopback is the allowlist used by the tests that stand in for a balancer: the
// header they write arrives from 127.0.0.1 or ::1, so both have to be trusted.
var loopback = []netip.Prefix{
	netip.MustParsePrefix("127.0.0.0/8"),
	netip.MustParsePrefix("::1/128"),
}

// TestProxyV2UsesTheHeaderAddress is the point of the mode: behind a balancer
// every socket says the balancer, and what a handler must see instead is the
// address the balancer reported for that connection.
//
// Asserted through the whole path — accept, header, codec negotiation, serve
// loop — because the header is consumed from the same stream the codec sniffer
// reads next, and a byte left behind there does not corrupt the address, it
// corrupts the connection.
func TestProxyV2UsesTheHeaderAddress(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		listen string
		client netip.Addr
	}{
		{name: "ipv4 client", listen: "127.0.0.1:0", client: netip.MustParseAddr("203.0.113.9")},
		{name: "ipv6 client", listen: "127.0.0.1:0", client: netip.MustParseAddr("2001:db8::5")},
		{name: "ipv6 balancer", listen: "[::1]:0", client: netip.MustParseAddr("2001:db8::7")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			nl := mustListenTCP(t, ctx, tt.listen)
			key := rebindTestKey()
			seen := serveRequests(t, ctx, mtproto.ListenProxyV2(nl, loopback, nil), key)

			raw := dial(t, ctx, nl.Addr().String())
			mustWrite(t, raw, proxyV2Header(proxyCmdProxy, tt.client))
			client, err := transport.Intermediate.Handshake(raw)
			if err != nil {
				t.Fatalf("transport handshake: %v", err)
			}
			// help.getConfig rather than a ping: service messages are answered
			// inside the serve loop and never reach a Handler.
			frame := clientFrame(t, key, 42, int64(1)<<32, &tg.HelpGetConfigRequest{})
			if err := client.Send(ctx, &bin.Buffer{Buf: slices.Clone(frame)}); err != nil {
				t.Fatalf("send frame: %v", err)
			}

			select {
			case got := <-seen:
				if got != tt.client {
					t.Errorf("handler saw client address %s, want the address the balancer reported, %s", got, tt.client)
				}
			case <-ctx.Done():
				t.Fatal("no request reached the handler")
			}
		})
	}
}

// TestProxyV2RefusesAllowlistedSenderWithoutAHeader is the fail-closed half of
// the mode. A connection from the balancer that carries no usable header cannot
// be served on its socket address — that address is the balancer's, and one
// bucket for every client is exactly what this mode exists to avoid — so it is
// dropped, and the listener carries on serving the next one.
func TestProxyV2RefusesAllowlistedSenderWithoutAHeader(t *testing.T) {
	t.Parallel()

	client := netip.MustParseAddr("198.51.100.4")
	for _, tt := range []struct {
		name    string
		prelude []byte
	}{
		// A client speaking MTProto straight at the port: codec header first,
		// then frame bytes. Nothing in it is a PROXY header.
		{name: "no header", prelude: append([]byte{0xee, 0xee, 0xee, 0xee}, make([]byte, 32)...)},
		{name: "truncated header", prelude: proxyV2Header(proxyCmdProxy, client)[:8]},
		{name: "truncated address block", prelude: proxyV2Header(proxyCmdProxy, client)[:20]},
		// PROXY v1 is the text form. It is refused rather than parsed: one
		// header format is the whole of what this mode has to get right.
		{name: "v1 text header", prelude: []byte("PROXY TCP4 198.51.100.4 10.0.0.1 51000 2443\r\n")},
		{name: "v1 version nibble", prelude: withVersion(proxyV2Header(proxyCmdProxy, client), 1)},
		{name: "unknown command", prelude: proxyV2Header(0x0f, client)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			nl := mustListenTCP(t, ctx, "127.0.0.1:0")
			accepted := acceptLoop(t, mtproto.ListenProxyV2(nl, loopback, nil))

			refused := dial(t, ctx, nl.Addr().String())
			mustWrite(t, refused, tt.prelude)
			// Half-closed so a header this short is short for good: a balancer
			// writes its header in one go, and waiting out the header timeout
			// would prove the same refusal ten seconds later.
			closeWrite(t, refused)
			assertClosed(t, refused)

			// The next connection is served: one refusal drops its own socket,
			// not the listener. A crash here would mean any peer can take the
			// server off the air by writing four bytes.
			good := dial(t, ctx, nl.Addr().String())
			mustWrite(t, good, proxyV2Header(proxyCmdProxy, client))
			if _, err := transport.Intermediate.Handshake(good); err != nil {
				t.Fatalf("transport handshake: %v", err)
			}
			select {
			case got, ok := <-accepted:
				if !ok {
					t.Fatal("the listener stopped accepting after a refused connection")
				}
				if got.addr != client {
					t.Errorf("accepted address %s, want %s", got.addr, client)
				}
			case <-ctx.Done():
				t.Fatal("the connection after a refused one was never accepted")
			}
		})
	}
}

// TestProxyV2RefusesUnallowlistedSender is the other direction of fail-closed,
// and the one an attacker probes. A PROXY header is client-supplied bytes; the
// allowlist is the only reason any of them can be believed. A sender outside it
// that writes a header is refused outright — the address it named is never used,
// and the bytes never reach the codec sniffer either.
//
// Refusing is the choice here rather than falling back to the sender's own
// socket address: in this mode every legitimate connection arrives through a
// balancer, so one that did not is either a misconfiguration or a client trying
// to reach around the limiter, and neither should be served.
func TestProxyV2RefusesUnallowlistedSender(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// An allowlist that loopback is not in, so the test's own connection is the
	// untrusted sender.
	allow := []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	accepted := acceptLoop(t, mtproto.ListenProxyV2(nl, allow, nil))

	raw := dial(t, ctx, nl.Addr().String())
	mustWrite(t, raw, proxyV2Header(proxyCmdProxy, netip.MustParseAddr("203.0.113.9")))
	assertClosed(t, raw)

	select {
	case got, ok := <-accepted:
		if ok {
			t.Fatalf("a sender outside the allowlist was accepted, keyed on %s", got.addr)
		}
	case <-time.After(500 * time.Millisecond):
	}
}

// TestProxyV2RefusesUnsupportedFamilyOrTransport is the listener half of the
// parser matrix, and it is written so that crediting the address would fail it.
//
// Each connection is complete — header then codec tag — so a build that read the
// family without the transport would accept it and hand the handler the address
// the header named. Nothing is accepted here, in either direction: not the
// address, not the connection.
func TestProxyV2RefusesUnsupportedFamilyOrTransport(t *testing.T) {
	t.Parallel()

	client := netip.MustParseAddr("198.51.100.9")
	for _, tt := range []struct {
		name     string
		famProto byte
	}{
		{name: "inet over datagram", famProto: 0x12},
		{name: "inet over unspec transport", famProto: 0x10},
		{name: "inet6 over datagram", famProto: 0x22},
		// AF_UNIX names a socket path rather than a network peer, and used to
		// land in the no-address carve-out instead of being refused.
		{name: "unix family", famProto: 0x31},
		{name: "unknown family", famProto: 0x41},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			nl := mustListenTCP(t, ctx, "127.0.0.1:0")
			accepted := acceptLoop(t, mtproto.ListenProxyV2(nl, loopback, nil))

			raw := dial(t, ctx, nl.Addr().String())
			// Header plus the abridged codec tag: everything a served
			// connection needs, so only the family and transport decide it.
			mustWrite(t, raw, append(withFamilyProto(proxyV2Header(proxyCmdProxy, client), tt.famProto), 0xef))
			assertClosed(t, raw)

			select {
			case got, ok := <-accepted:
				if ok {
					t.Fatalf("family/transport %#x was served, keyed on %s", tt.famProto, got.addr)
				}
			case <-time.After(500 * time.Millisecond):
			}
		})
	}
}

// TestProxyV2HeaderWithoutAClientAddress covers the headers that are well formed
// and name no client: LOCAL, which is what a balancer's own health check sends,
// and the address families this server cannot key on.
//
// The connection is served, and it carries no address at all rather than the
// balancer's. Nothing keyed on an address can then be charged to it — the
// sendCode limiter refuses a request it cannot attribute — so this stays closed
// without taking a load balancer's health checks off the air.
func TestProxyV2HeaderWithoutAClientAddress(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		header []byte
	}{
		{name: "local command", header: proxyV2Header(proxyCmdLocal, netip.MustParseAddr("203.0.113.9"))},
		{name: "unspecified family", header: proxyV2Header(proxyCmdProxy, netip.Addr{})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			nl := mustListenTCP(t, ctx, "127.0.0.1:0")
			accepted := acceptLoop(t, mtproto.ListenProxyV2(nl, loopback, nil))

			raw := dial(t, ctx, nl.Addr().String())
			mustWrite(t, raw, tt.header)
			if _, err := transport.Intermediate.Handshake(raw); err != nil {
				t.Fatalf("transport handshake: %v", err)
			}

			select {
			case got, ok := <-accepted:
				if !ok {
					t.Fatal("the listener stopped accepting")
				}
				if got.addr.IsValid() {
					t.Errorf("connection carries address %s, want none: the header named no client", got.addr)
				}
			case <-ctx.Done():
				t.Fatal("the connection was never accepted")
			}
		})
	}
}

// TestListenerAcceptsEveryCodec pins the ordering trap this ticket turns on. The
// server picks a transport codec by sniffing a connection's first bytes, so a
// PROXY header still in the stream at that point is read as a codec tag — and
// the unrecognised tag lands on Full, whose frames then decode as garbage.
//
// Every codec the server accepts has to keep working in both modes, so each one
// negotiates and round-trips a frame here.
func TestListenerAcceptsEveryCodec(t *testing.T) {
	t.Parallel()

	client := netip.MustParseAddr("203.0.113.22")
	for _, mode := range []struct {
		name   string
		listen func(net.Listener) mtproto.Listener
		// prelude is written before the codec header, as a balancer would.
		prelude []byte
		want    func(net.Conn) netip.Addr
	}{
		{
			name:   "socket",
			listen: mtproto.Listen,
			want: func(c net.Conn) netip.Addr {
				return netip.MustParseAddrPort(c.LocalAddr().String()).Addr().Unmap()
			},
		},
		{
			name:    "proxy-v2",
			listen:  func(ln net.Listener) mtproto.Listener { return mtproto.ListenProxyV2(ln, loopback, nil) },
			prelude: proxyV2Header(proxyCmdProxy, client),
			want:    func(net.Conn) netip.Addr { return client },
		},
	} {
		for _, codec := range []struct {
			name  string
			proto transport.Protocol
		}{
			{name: "abridged", proto: transport.Abridged},
			{name: "intermediate", proto: transport.Intermediate},
			{name: "padded intermediate", proto: transport.PaddedIntermediate},
			{name: "full", proto: transport.Full},
		} {
			t.Run(mode.name+"/"+codec.name, func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
				defer cancel()

				nl := mustListenTCP(t, ctx, "127.0.0.1:0")
				accepted := acceptLoop(t, mode.listen(nl))

				raw := dial(t, ctx, nl.Addr().String())
				mustWrite(t, raw, mode.prelude)
				client, err := codec.proto.Handshake(raw)
				if err != nil {
					t.Fatalf("transport handshake: %v", err)
				}
				// Longer than four bytes and word-aligned: a four-byte frame is
				// read as a protocol error code, and two codecs refuse to write
				// anything that is not a multiple of four.
				payload := []byte("proxy-protocol-v2-payload-000000")
				if err := client.Send(ctx, &bin.Buffer{Buf: slices.Clone(payload)}); err != nil {
					t.Fatalf("send frame: %v", err)
				}

				var got acceptedConn
				select {
				case c, ok := <-accepted:
					if !ok {
						t.Fatal("the listener stopped accepting")
					}
					got = c
				case <-ctx.Done():
					t.Fatal("the connection was never accepted")
				}
				if want := mode.want(raw); got.addr != want {
					t.Errorf("client address %s, want %s", got.addr, want)
				}

				var b bin.Buffer
				if err := got.conn.Recv(ctx, &b); err != nil {
					t.Fatalf("recv frame: %v", err)
				}
				if string(b.Buf) != string(payload) {
					t.Errorf("frame decoded as %q, want %q: the codec was mis-detected", b.Buf, payload)
				}
			})
		}
	}
}

// PROXY protocol v2 commands, as the header's low nibble spells them.
const (
	proxyCmdLocal = 0x00
	proxyCmdProxy = 0x01
)

// proxyV2Header builds the header a balancer writes ahead of a proxied
// connection. An invalid client address produces an AF_UNSPEC header with no
// address block, which is what a balancer sends for a connection whose original
// endpoints it cannot report.
func proxyV2Header(cmd byte, client netip.Addr) []byte {
	var h [16]byte
	copy(h[:], []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A})
	h[12] = 0x20 | cmd // version 2, command
	var body []byte
	switch {
	case !client.IsValid():
		h[13] = 0x00 // AF_UNSPEC, UNSPEC: no address block at all
	case client.Is4():
		h[13] = 0x11 // AF_INET, STREAM
		src, dst := client.As4(), [4]byte{10, 0, 0, 1}
		body = append(append(body, src[:]...), dst[:]...)
		binary.BigEndian.PutUint16(h[14:16], 4+4+2+2)
	default:
		h[13] = 0x21 // AF_INET6, STREAM
		src, dst := client.As16(), netip.MustParseAddr("2001:db8::1").As16()
		body = append(append(body, src[:]...), dst[:]...)
		binary.BigEndian.PutUint16(h[14:16], 16+16+2+2)
	}
	if len(body) > 0 {
		body = binary.BigEndian.AppendUint16(body, 51000) // source port
		body = binary.BigEndian.AppendUint16(body, 2443)  // destination port
	}
	return append(h[:], body...)
}

// withFamilyProto rewrites a header's family-and-transport byte, to build the
// combinations a correct balancer never emits.
func withFamilyProto(header []byte, famProto byte) []byte {
	out := slices.Clone(header)
	out[13] = famProto
	return out
}

// withVersion rewrites a header's version nibble, to build the versions this
// mode does not accept without hand-assembling a second header.
func withVersion(header []byte, version byte) []byte {
	out := slices.Clone(header)
	out[12] = version<<4 | out[12]&0x0f
	return out
}

// acceptedConn is one connection the listener handed back.
type acceptedConn struct {
	conn transport.Conn
	addr netip.Addr
}

// acceptLoop drains l in the background so a refused connection shows up as the
// absence of a result rather than a blocked test, and so a later connection can
// prove the listener kept going. The channel closes when the listener stops.
func acceptLoop(t *testing.T, l mtproto.Listener) <-chan acceptedConn {
	t.Helper()
	out := make(chan acceptedConn, 4)
	go func() {
		defer close(out)
		for {
			conn, addr, err := l.Accept()
			if err != nil {
				return
			}
			out <- acceptedConn{conn: conn, addr: addr}
		}
	}()
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Errorf("close listener: %v", err)
		}
	})
	return out
}

// serveRequests runs a server on l and reports the client address of every
// request that reaches a handler.
func serveRequests(t *testing.T, ctx context.Context, l mtproto.Listener, key crypto.AuthKey) <-chan netip.Addr {
	t.Helper()
	keys := mtproto.NewMemoryAuthKeyStore()
	if err := keys.Save(ctx, key); err != nil {
		t.Fatalf("save key: %v", err)
	}
	seen := make(chan netip.Addr, 1)
	handler := mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
		select {
		case seen <- req.ClientAddr:
		default:
		}
		return nil
	})
	// No key exchange runs here: the client sends an encrypted frame under a key
	// the store already holds, so the server needs no private key of its own.
	srv := mtproto.New(exchange.PrivateKey{}, 2, keys, handler, nil)
	srvCtx, stop := context.WithCancel(ctx)
	served := make(chan error, 1)
	go func() { served <- srv.Serve(srvCtx, l) }()
	t.Cleanup(func() {
		stop()
		if err := <-served; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return seen
}

func mustListenTCP(t *testing.T, ctx context.Context, addr string) net.Listener {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		t.Skipf("listen on %s: %v", addr, err)
	}
	return ln
}

// dial opens a client connection that is closed before the server it is talking
// to, so the serve loop sees the disconnect and returns.
func dial(t *testing.T, ctx context.Context, addr string) net.Conn {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() {
		// A connection the server already refused is closed at both ends.
		if err := conn.Close(); err != nil && !isClosedConn(err) {
			t.Errorf("close client: %v", err)
		}
	})
	return conn
}

func mustWrite(t *testing.T, conn net.Conn, b []byte) {
	t.Helper()
	if len(b) == 0 {
		return
	}
	if _, err := conn.Write(b); err != nil {
		t.Fatalf("write %d bytes: %v", len(b), err)
	}
}

// assertClosed waits for the server to drop the connection. It is how a refusal
// is visible from the client side: no reply, no bytes, just the close.
func assertClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var b [1]byte
	switch n, err := conn.Read(b[:]); {
	case err == nil:
		t.Fatalf("the connection was served: read %d bytes back", n)
	case isTimeout(err):
		t.Fatal("the connection was neither served nor closed")
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// closeWrite ends the client's half of the stream, so a read waiting for more
// header bytes sees the end of them rather than a stall.
func closeWrite(t *testing.T, conn net.Conn) {
	t.Helper()
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("client conn type = %T, want *net.TCPConn", conn)
	}
	// The server may already have refused this connection and closed it — a
	// header it can reject outright needs no end-of-stream to do it — and that
	// is the outcome the caller is about to assert, not a failure to half-close.
	if err := tcp.CloseWrite(); err != nil && !isClosedConn(err) && !errors.Is(err, syscall.ENOTCONN) {
		t.Fatalf("close write half: %v", err)
	}
}

// isClosedConn reports whether err is the close of a connection the server
// already dropped, which is not a failure to close it.
func isClosedConn(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
