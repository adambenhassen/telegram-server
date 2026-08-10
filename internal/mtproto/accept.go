package mtproto

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"syscall"
	"time"

	"github.com/gotd/td/transport"
)

const (
	// Accept backoff bounds for a transient listener fault: long enough for
	// descriptors to be returned, short enough that recovery is not noticeable.
	minAcceptBackoff = 5 * time.Millisecond
	maxAcceptBackoff = time.Second
)

// Negotiating an accepted socket is two steps — establishing the client address
// and detecting the transport codec — and they are two functions because a bound
// keyed on the address belongs between them. Both read client bytes, and both
// run in the connection's own goroutine rather than in the accept loop, so that
// one peer cannot decide when — or whether — anybody else gets connected, and so
// that its read error is never reported as a listener failure.
//
// Shutdown is neither one's business: the caller closes the socket on
// cancellation, which ends whichever read it has got to.

// clientAddr establishes the address of an accepted socket, from the socket
// itself or from a PROXY v2 header, and sets the deadline that bounds the whole
// negotiation.
//
// The address comes first and, in socket mode, before a single byte has been
// read, so what every per-IP limit keys on cannot be a function of anything the
// client wrote. In proxy-v2 mode the allowlist decides before any read too:
// bytes from a sender that is not a configured balancer are never interpreted,
// as a header or as anything else.
//
// One deadline covers the header read and the codec sniff that follows, and is
// left set on the way out: gotd resets it from the context before every frame it
// reads, so nothing here has to clear it.
func (s *Server) clientAddr(sock net.Conn) (netip.Addr, error) {
	addr := peerAddr(sock.RemoteAddr())
	if s.proxyV2 != nil && !s.proxyV2.allowed(addr) {
		return netip.Addr{}, errors.Join(
			fmt.Errorf("no PROXY header is accepted from %s: not an allowlisted balancer", addr), sock.Close())
	}
	if err := sock.SetReadDeadline(time.Now().Add(s.handshakeTimeout)); err != nil {
		return netip.Addr{}, errors.Join(errors.New("set handshake deadline"), err, sock.Close())
	}
	if s.proxyV2 == nil {
		return addr, nil
	}

	// Before the codec sniff, never after: the sniff picks a codec from the
	// first bytes of the stream, so a header still in it would be read as a
	// codec tag and every later frame would decode as garbage.
	client, err := readProxyV2(sock)
	if err != nil {
		return netip.Addr{}, errors.Join(errors.New("read PROXY v2 header"), err, sock.Close())
	}
	return client, nil
}

// detectCodec negotiates the transport of a socket whose address has already
// been established.
func (s *Server) detectCodec(sock net.Conn) (transport.Conn, error) {
	// Hand gotd this one socket to negotiate: its Accept owns the failure path
	// and closes the socket if detection fails, exactly as it did when it held
	// the listener itself.
	conn, err := transport.Listen(&singleConn{conn: sock}).Accept()
	if err != nil {
		return nil, errors.Join(errors.New("detect codec"), err)
	}
	return conn, nil
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

// isTransientAccept reports whether an accept failure is a passing condition of
// the process, or of one pending connection, rather than a fault of the
// listener. Running out of file descriptors is the one a burst of connection
// opens produces: it belongs to no connection, it clears once open sockets are
// returned, and treating it as fatal would let that burst stop the server. A
// peer that resets or aborts between the SYN and the accept fails the same way
// and is likewise nobody else's problem — Go's own net package counts both as
// temporary.
func isTransientAccept(err error) bool {
	if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	var nerr net.Error
	return errors.As(err, &nerr) && nerr.Timeout()
}

// nextAcceptBackoff doubles the wait between accept retries, from
// minAcceptBackoff up to maxAcceptBackoff, so a listener that keeps failing is
// not retried in a tight loop.
func nextAcceptBackoff(d time.Duration) time.Duration {
	if d == 0 {
		return minAcceptBackoff
	}
	if d *= 2; d > maxAcceptBackoff {
		return maxAcceptBackoff
	}
	return d
}
