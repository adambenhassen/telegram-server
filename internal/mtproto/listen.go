package mtproto

import (
	"errors"
	"log/slog"
	"net"
	"net/netip"

	"github.com/gotd/td/transport"
)

// Listener accepts MTProto transport connections and reports, alongside each
// one, the address of the client that opened it.
//
// gotd's transport.Listener hands back a connection with no way to ask which
// socket it came from, and MTProto carries no client-supplied address a server
// could trust instead. So the address is decided here, before a single client
// byte has been interpreted as protocol.
type Listener interface {
	// Accept returns the next connection and its client address. The address is
	// the zero value when the connection carries none the server may key on,
	// which callers must handle rather than treat as an address.
	Accept() (transport.Conn, netip.Addr, error)
	// Close closes the listener, unblocking a pending Accept.
	Close() error
}

// Listen accepts MTProto connections on ln, attributing each to the peer
// address of its own socket.
//
// This is the default and the only source nothing a client sends can influence.
// It assumes the peer is the client, which stops being true behind an L4 load
// balancer, where every peer address is the balancer's: ListenProxyV2 is the
// mode for that deployment.
//
// Codec detection is unchanged: every transport gotd's own listener negotiates
// still connects, because that listener is what still performs the negotiation
// here — one connection at a time, on the socket whose address was just read.
func Listen(ln net.Listener) Listener {
	return &listener{ln: ln, src: socketSource{}, log: slog.New(slog.DiscardHandler)}
}

// clientAddrSource decides the address one accepted connection is attributed
// to. It is the seam the configured trust mode selects between, and the only
// place a client address is ever established.
type clientAddrSource interface {
	// clientAddr returns the address to attribute conn to, consuming from conn
	// only what its own framing requires and leaving the stream positioned at
	// the first byte of the transport codec header.
	//
	// An error refuses the connection. It never means "fall back to the socket
	// address": a mode that cannot establish the address it was configured to
	// use has no second answer that is still true.
	clientAddr(conn net.Conn) (netip.Addr, error)
}

// listener is the accept loop every address source shares: take a socket,
// establish its client address, then let gotd detect the transport codec on it.
type listener struct {
	ln  net.Listener
	src clientAddrSource
	log *slog.Logger
}

// Accept returns the next connection that could be set up, skipping the ones
// that could not.
//
// A connection that is refused, or whose codec never resolves, is that
// connection's failure and not the listener's. Returning it here would end the
// accept loop in Serve, so any peer that opened a socket and wrote a few bytes
// could take the server off the air; the socket is closed and the loop moves on.
func (l *listener) Accept() (transport.Conn, netip.Addr, error) {
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			return nil, netip.Addr{}, err
		}
		// Read before setup, which closes the socket when it refuses it.
		peer := conn.RemoteAddr()
		tconn, addr, err := l.setup(conn)
		if err != nil {
			l.log.Info("refused connection", "peer", peer, "err", err)
			continue
		}
		return tconn, addr, nil
	}
}

// setup establishes one connection's client address and then its transport
// codec. The order is the whole of it: the codec is chosen by sniffing the first
// bytes of the stream, so whatever the address source has to read must already
// be consumed, or it is read as a codec tag and every later frame is garbage.
func (l *listener) setup(conn net.Conn) (transport.Conn, netip.Addr, error) {
	addr, err := l.src.clientAddr(conn)
	if err != nil {
		return nil, netip.Addr{}, errors.Join(err, conn.Close())
	}
	// Hand gotd this one socket to negotiate: its Accept owns the failure path
	// and closes the connection if detection fails, exactly as it did when it
	// held the listener itself.
	tconn, err := transport.Listen(&singleConn{conn: conn}).Accept()
	if err != nil {
		return nil, netip.Addr{}, err
	}
	return tconn, addr, nil
}

// Close closes the underlying socket listener.
func (l *listener) Close() error {
	return l.ln.Close()
}

// socketSource attributes a request to the address its own connection came
// from. It reads nothing from the stream, so nothing a client sends can reach
// it.
type socketSource struct{}

// clientAddr reports the peer address of the accepted socket. It never refuses:
// an address the transport cannot parse yields the zero Addr, which downstream
// treats as unattributable rather than as a key.
func (socketSource) clientAddr(conn net.Conn) (netip.Addr, error) {
	return peerAddr(conn.RemoteAddr()), nil
}

// peerAddr parses the transport peer address of an accepted socket. An address
// that is absent, or from a network that has no IP, yields the zero Addr: this
// server only ever listens on TCP, so that is a transport-layer fault rather
// than a client-reachable state, and it must not silently become a usable key.
func peerAddr(a net.Addr) netip.Addr {
	ap, ok := a.(interface{ AddrPort() netip.AddrPort })
	if !ok {
		return netip.Addr{}
	}
	// TCPAddr reports an IPv4 peer as a 4-in-6 address; unmapping keeps one
	// host from being two different addresses depending on the socket family.
	return ap.AddrPort().Addr().Unmap()
}

// singleConn presents one already-accepted socket as a net.Listener, so gotd's
// codec detection can run against it without owning the accept loop.
type singleConn struct {
	conn net.Conn
}

// Accept yields the connection once. A second call means the caller looped over
// a listener that was never meant to serve more than one socket.
func (l *singleConn) Accept() (net.Conn, error) {
	conn := l.conn
	if conn == nil {
		return nil, net.ErrClosed
	}
	l.conn = nil
	return conn, nil
}

// Close is a no-op: the connection's lifetime belongs to whoever accepted it,
// and gotd's listener never closes the listener it was given.
func (l *singleConn) Close() error { return nil }

// Addr reports the connection's local address, which is the address it was
// accepted on.
func (l *singleConn) Addr() net.Addr {
	if l.conn == nil {
		return nil
	}
	return l.conn.LocalAddr()
}
