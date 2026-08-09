package mtproto

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

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
	return newListener(ln, socketSource{}, nil)
}

// newListener builds the accept path shared by every address source. A nil
// logger discards.
func newListener(ln net.Listener, src clientAddrSource, log *slog.Logger) *listener {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &listener{
		ln:    ln,
		src:   src,
		log:   log,
		ready: make(chan readyConn),
		dead:  make(chan struct{}),
	}
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

const (
	// setupTimeout bounds everything the accept path reads off a new socket: the
	// PROXY header where there is one, and the codec tag in every mode. Without
	// a bound a peer that says nothing holds a goroutine and a file descriptor
	// for as long as it likes.
	//
	// It is defaultReadTimeout rather than something tighter, even though a real
	// client sends these bytes the instant it connects. The deadline covers the
	// server being slow to read as well as the peer being slow to write, and a
	// value under the one an established connection already gets would drop
	// clients this server holds on to everywhere else.
	setupTimeout = defaultReadTimeout
	// refusalLogInterval bounds how often a refused connection is logged. The
	// refusals are driven by whoever can reach the port, unauthenticated, so an
	// unsampled line per refusal is a log an attacker writes; each line that
	// does come out carries how many were suppressed behind it.
	refusalLogInterval = 10 * time.Second
)

// listener accepts sockets and sets each one up — client address first, then
// transport codec — concurrently.
//
// Concurrently is the point, and it is a correctness property rather than a
// throughput one. Setting a connection up means reading from it, and a peer that
// connects and stays silent never sends the bytes being read. Doing that work on
// the accept path means that one peer holds up every connection behind it: one
// socket and no payload, unauthenticated, and the server accepts nobody. So the
// accept path here does nothing but accept, and each socket is set up in its own
// goroutine under setupTimeout, which is also what bounds how many of those
// goroutines a peer can pile up.
//
// Connections are therefore handed out in the order their setup finished, not
// the order they were accepted. Nothing downstream depends on accept order.
type listener struct {
	ln  net.Listener
	src clientAddrSource
	log *slog.Logger

	// start launches the accept pump on the first Accept, so a listener that is
	// closed before it is ever served never starts a goroutine.
	start sync.Once
	// ready carries connections that finished setup. Unbuffered: a completed
	// connection waits in its own goroutine until Accept takes it.
	ready chan readyConn
	// dead is closed when the accept pump stops, which is the only way Accept
	// reports a listener-level failure. acceptErr is written before that close
	// and read only after it, so the close is what publishes it.
	dead      chan struct{}
	acceptErr error

	refusals logSampler
}

// readyConn is a connection whose codec and client address are established.
type readyConn struct {
	conn transport.Conn
	addr netip.Addr
}

// Accept returns the next connection that finished setup.
//
// Connections that could not be set up never appear here. A refusal, or a codec
// that never resolves, is that connection's failure and not the listener's:
// surfacing it would end the accept loop in Serve, so any peer that opened a
// socket and wrote a few bytes could take the server off the air.
func (l *listener) Accept() (transport.Conn, netip.Addr, error) {
	l.start.Do(func() { go l.pump() })
	select {
	case c := <-l.ready:
		return c.conn, c.addr, nil
	case <-l.dead:
		// Connections that finished setup before the listener stopped are still
		// handed out; only once none is left is the accept failure reported.
		select {
		case c := <-l.ready:
			return c.conn, c.addr, nil
		default:
			return nil, netip.Addr{}, l.acceptErr
		}
	}
}

// pump owns the accept path and does nothing on it but accept.
func (l *listener) pump() {
	defer close(l.dead)
	for {
		conn, err := l.ln.Accept()
		if err != nil {
			l.acceptErr = err
			return
		}
		go l.setupOne(conn)
	}
}

// setupOne sets one accepted socket up and offers it to Accept, off the accept
// path. A socket that cannot be set up is closed by setup and goes no further.
func (l *listener) setupOne(conn net.Conn) {
	// Read before setup, which closes the socket when it refuses it.
	peer := conn.RemoteAddr()
	tconn, addr, err := l.setup(conn)
	if err != nil {
		if dropped, ok := l.refusals.allow(time.Now(), refusalLogInterval); ok {
			l.log.Info("refused connection", "peer", peer, "err", err, "suppressed", dropped)
		}
		return
	}
	select {
	case l.ready <- readyConn{conn: tconn, addr: addr}:
	case <-l.dead:
		// The listener stopped while this one was still being set up.
		if cerr := tconn.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			l.log.Info("close connection accepted during shutdown", "peer", peer, "err", cerr)
		}
	}
}

// setup establishes one connection's client address and then its transport
// codec. The order is the whole of it: the codec is chosen by sniffing the first
// bytes of the stream, so whatever the address source has to read must already
// be consumed, or it is read as a codec tag and every later frame is garbage.
//
// One deadline covers both reads. It is armed here rather than inside a source
// because the codec sniff reads too, and gotd sets no deadline of its own.
func (l *listener) setup(conn net.Conn) (transport.Conn, netip.Addr, error) {
	if err := conn.SetReadDeadline(time.Now().Add(setupTimeout)); err != nil {
		return nil, netip.Addr{}, errors.Join(fmt.Errorf("set setup deadline: %w", err), conn.Close())
	}
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
	// Cleared before the connection is handed on, so the rest of its life keeps
	// the deadlines the serve loop sets rather than this one.
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		return nil, netip.Addr{}, errors.Join(fmt.Errorf("clear setup deadline: %w", err), tconn.Close())
	}
	return tconn, addr, nil
}

// Close closes the underlying socket listener, which is what stops the pump.
func (l *listener) Close() error {
	return l.ln.Close()
}

// logSampler thins a log line that anyone who can reach the port can provoke.
// It holds no lock — the compare-and-swap is the whole of it, and a caller that
// loses the swap is one of the callers being dropped anyway.
type logSampler struct {
	// last is the unix-nanosecond time of the line that was emitted, 0 before
	// the first one. dropped counts the lines suppressed since then.
	last    atomic.Int64
	dropped atomic.Int64
}

// allow reports whether a line may be emitted now and, when it may, how many
// were dropped since the previous one.
func (s *logSampler) allow(now time.Time, interval time.Duration) (int64, bool) {
	n := now.UnixNano()
	last := s.last.Load()
	if n-last < int64(interval) || !s.last.CompareAndSwap(last, n) {
		s.dropped.Add(1)
		return 0, false
	}
	return s.dropped.Swap(0), true
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
