package mtproto

import (
	"net"
	"net/netip"

	"github.com/gotd/td/transport"
)

// Listener accepts MTProto transport connections and reports, alongside each
// one, the address of the peer that opened it.
//
// gotd's transport.Listener hands back a connection with no way to ask which
// socket it came from, and MTProto carries no client-supplied address a server
// could trust instead. So the peer address is read here, off the accepted
// socket, before a single client byte has been interpreted — nothing a client
// sends can reach it.
type Listener interface {
	// Accept returns the next connection and its peer address. The address is
	// the zero value when the socket has none the server can parse, which
	// callers must handle rather than treat as an address.
	Accept() (transport.Conn, netip.Addr, error)
	// Close closes the listener, unblocking a pending Accept.
	Close() error
}

// Listen accepts MTProto connections on ln, capturing each peer address.
//
// Codec detection is unchanged: every transport gotd's own listener negotiates
// still connects, because that listener is what still performs the negotiation
// here — one connection at a time, on the socket whose address was just read.
func Listen(ln net.Listener) Listener {
	return &socketListener{ln: ln}
}

// socketListener is the socket-address implementation of Listener: the address
// a request is attributed to is the one its own connection came from.
//
// This is the only address source the server supports today, and the trust mode
// naming it is the seam a load-balancer-supplied address would arrive through:
// such a source would be another Listener, leaving everything downstream of
// Accept unchanged.
type socketListener struct {
	ln net.Listener
}

// Accept takes the next socket, records its peer address, and then lets gotd
// detect the transport codec on it.
func (l *socketListener) Accept() (transport.Conn, netip.Addr, error) {
	conn, err := l.ln.Accept()
	if err != nil {
		return nil, netip.Addr{}, err
	}
	// Read before the codec detection below consumes the first client bytes,
	// so the address cannot be a function of anything the client wrote.
	addr := peerAddr(conn.RemoteAddr())

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
func (l *socketListener) Close() error {
	return l.ln.Close()
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
