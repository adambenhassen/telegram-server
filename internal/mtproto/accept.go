package mtproto

import (
	"errors"
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

// negotiate detects the transport codec of an accepted socket, bounded by the
// handshake timeout.
//
// This runs per connection rather than in the accept loop on purpose: codec
// detection reads client bytes, so performing it where the next socket is
// accepted would let one peer decide when — or whether — anybody else gets
// connected, and would report that peer's read error as a listener failure.
func (s *Server) negotiate(sock net.Conn) (transport.Conn, error) {
	if err := sock.SetReadDeadline(time.Now().Add(s.handshakeTimeout)); err != nil {
		return nil, errors.Join(errors.New("set handshake deadline"), err, sock.Close())
	}

	// Hand gotd this one socket to negotiate: its Accept owns the failure path
	// and closes the socket if detection fails, exactly as it did when it held
	// the listener itself.
	conn, err := transport.Listen(&singleConn{conn: sock}).Accept()
	if err != nil {
		return nil, errors.Join(errors.New("detect codec"), err)
	}

	// Frame reads carry their own deadline, derived from the read timeout, so
	// the handshake bound must not outlive the handshake.
	if err := sock.SetReadDeadline(time.Time{}); err != nil {
		return nil, errors.Join(errors.New("clear handshake deadline"), err, conn.Close())
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
// the process rather than a fault of the listener. Running out of file
// descriptors is the one a burst of connection opens produces: it belongs to no
// connection, it clears once open sockets are returned, and treating it as
// fatal would let that burst stop the server.
func isTransientAccept(err error) bool {
	if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) || errors.Is(err, syscall.ECONNABORTED) {
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
