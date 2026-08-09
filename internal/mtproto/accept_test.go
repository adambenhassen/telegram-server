package mtproto_test

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// TestServeSurvivesEarlyClose covers the accept path's exposure to a single
// unauthenticated socket: a connection closed before it sends a byte fails
// transport negotiation, and that failure belongs to that connection alone. The
// server must keep serving everyone else.
func TestServeSurvivesEarlyClose(t *testing.T) {
	t.Parallel()

	addr, serveDone := serveTest(t, 0)

	// One throwaway connection: opened, closed without writing anything.
	dead, err := dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := dead.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// The server still answers a fresh connection.
	assertServes(t, addr, transport.Abridged)

	select {
	case err := <-serveDone:
		t.Fatalf("Serve returned while the listener was still open: %v", err)
	default:
	}
}

// TestServeClosesSilentConnection covers the second shape of the same hole: a
// socket that opens and then sends nothing must not hold the accept path, and
// must itself be closed after a bounded time rather than kept forever.
func TestServeClosesSilentConnection(t *testing.T) {
	t.Parallel()

	const handshake = 200 * time.Millisecond
	addr, _ := serveTest(t, handshake)

	silent, err := dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer silent.Close() //nolint:errcheck // the server closes it; this is a safety net.

	// Another client connects and is served while the silent one is still open.
	assertServes(t, addr, transport.Abridged)

	// The silent socket is closed by the server once the handshake bound
	// elapses: the read ends instead of blocking forever.
	if err := silent.SetReadDeadline(time.Now().Add(10 * handshake)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var buf [1]byte
	_, err = silent.Read(buf[:])
	if err == nil {
		t.Fatal("read from silent connection succeeded, want close")
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		t.Fatalf("silent connection still open after %v", 10*handshake)
	}
}

// TestServeAcceptErrorClassification covers the listener-level half of the
// invariant, in both directions. A transient failure belongs to no connection
// and clears on its own, so serving must survive it; a permanent one is the
// listener itself failing, and swallowing that would leave a process that
// accepts nobody looking alive. Both halves are asserted here because a
// predicate widened until it swallowed real faults would pass a suite that only
// tested the first.
func TestServeAcceptErrorClassification(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		err  error
		// fatal says Serve must return the error rather than retry past it.
		fatal bool
	}{
		{name: "process out of descriptors", err: syscall.EMFILE},
		{name: "system out of descriptors", err: syscall.ENFILE},
		{name: "peer aborted before accept", err: syscall.ECONNABORTED},
		{name: "peer reset before accept", err: syscall.ECONNRESET},
		{name: "accept deadline elapsed", err: os.ErrDeadlineExceeded},
		{name: "listener rejects the call", err: syscall.EINVAL, fatal: true},
		{name: "listener fault with no errno", err: errors.New("listener broken"), fatal: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nl, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatalf("listen: %v", err)
			}
			// One fault is enough for the permanent case; the transient one is
			// repeated so a retry that only survived once would still fail.
			fl := &faultyListener{Listener: nl, err: tt.err, faults: 3}

			srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
			done := make(chan error, 1)
			go func() { done <- srv.Serve(context.Background(), fl); close(done) }()
			t.Cleanup(func() {
				if cerr := fl.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
					t.Errorf("listener close: %v", cerr)
				}
				<-done
			})

			if tt.fatal {
				select {
				case err := <-done:
					if !errors.Is(err, tt.err) {
						t.Fatalf("Serve returned %v, want it to carry %v", err, tt.err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("Serve kept running after a permanent listener fault")
				}
				return
			}

			assertServes(t, nl.Addr().String(), transport.Abridged)

			select {
			case err := <-done:
				t.Fatalf("Serve returned after a transient accept fault: %v", err)
			default:
			}
		})
	}
}

// TestServeEscalatesWhenAcceptStaysBroken covers the other half of surviving a
// transient fault: a server that retries forever at Info is a server that
// accepts nobody while reading as healthy. Once the retry interval has
// saturated, the fault has outlived any blip and must be reported as such.
func TestServeEscalatesWhenAcceptStaysBroken(t *testing.T) {
	t.Parallel()

	nl, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	// Enough consecutive faults to walk the backoff to its ceiling, then the
	// listener recovers so the serve loop can be shut down normally.
	fl := &faultyListener{Listener: nl, err: syscall.EMFILE, faults: 12}

	sink := &warnSink{warned: make(chan struct{}, 1)}
	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, slog.New(sink))
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), fl); close(done) }()
	t.Cleanup(func() {
		if cerr := fl.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			t.Errorf("listener close: %v", cerr)
		}
		<-done
	})

	select {
	case <-sink.warned:
	case <-time.After(30 * time.Second):
		t.Fatal("accept stuck at maximum backoff was never reported above Info")
	}
}

// TestServeShutdownDoesNotAwaitNegotiation covers what cancellation reaches: a
// socket sitting in transport negotiation is blocked on a read that takes no
// context, so without the deadline being expired for it, stopping the server
// would wait out the whole handshake bound — a deploy restart stalling on
// whatever silent sockets happen to be mid-negotiation.
func TestServeShutdownDoesNotAwaitNegotiation(t *testing.T) {
	t.Parallel()

	nl, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	accepted := make(chan struct{})
	al := &acceptedListener{Listener: nl, accepted: accepted}

	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	// The production bound, deliberately: the assertion below is that shutdown
	// does not wait for it.
	srv.SetHandshakeTimeout(30 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, al); close(done) }()

	silent, err := dial(nl.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	defer silent.Close() //nolint:errcheck // the server owns this socket's close.

	// Cancel only once the socket is in the server's hands and negotiating, so
	// the wait below is the real thing rather than a shutdown with nothing
	// pending.
	select {
	case <-accepted:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("server never accepted the connection")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve waited on a connection still negotiating instead of shutting down")
	}
}

// TestServeShutdownDoesNotAwaitFirstFrame covers the far side of the same
// handoff. Expiring a deadline is a one-shot signal, and the read on the other
// side of negotiation re-derives its own deadline from the context before it
// blocks — a context whose cancellation it never consults. So cancellation that
// lands once the transport marker has been consumed must still end the
// connection, or shutdown waits out the frame read instead of the handshake.
func TestServeShutdownDoesNotAwaitFirstFrame(t *testing.T) {
	t.Parallel()

	nl, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	reading := make(chan struct{})
	fl := &frameReadListener{Listener: nl, reading: reading}

	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	// Production-sized, both of them: the assertion is that neither bound is
	// what shutdown waits for.
	srv.SetHandshakeTimeout(30 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, fl); close(done) }()

	c, err := dial(nl.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	defer c.Close() //nolint:errcheck // the server owns this socket's close.

	// The abridged marker and nothing more: codec detection completes, and the
	// server goes on to wait for a frame that never arrives.
	if _, err := c.Write([]byte{codec.AbridgedClientStart[0]}); err != nil {
		cancel()
		t.Fatalf("write transport marker: %v", err)
	}

	select {
	case <-reading:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("server never reached the frame read")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve waited out the frame read instead of shutting down")
	}
}

// TestServeNegotiatesEveryCodec pins the transports the server accepts: moving
// codec detection off the accept loop must not narrow them.
func TestServeNegotiatesEveryCodec(t *testing.T) {
	t.Parallel()

	addr, _ := serveTest(t, 0)

	for name, protocol := range map[string]transport.Protocol{
		"abridged":            transport.Abridged,
		"intermediate":        transport.Intermediate,
		"padded intermediate": transport.PaddedIntermediate,
		"full":                transport.Full,
	} {
		t.Run(name, func(t *testing.T) {
			assertServes(t, addr, protocol)
		})
	}
}

// serveTest starts a server on a loopback listener and returns its address and
// the channel Serve's result lands on. A zero handshake timeout keeps the
// production bound.
func serveTest(t *testing.T, handshake time.Duration) (string, <-chan error) {
	t.Helper()

	nl, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	if handshake > 0 {
		srv.SetHandshakeTimeout(handshake)
	}

	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), nl); close(done) }()
	t.Cleanup(func() {
		if cerr := nl.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			t.Errorf("listener close: %v", cerr)
		}
		if serr := <-done; serr != nil {
			t.Errorf("serve: %v", serr)
		}
	})
	return nl.Addr().String(), done
}

// assertServes opens a connection, negotiates protocol and sends a frame
// bearing an auth key id the server does not know, expecting the protocol error
// back. It fails the test if the server does not answer.
func assertServes(t *testing.T, addr string, protocol transport.Protocol) {
	t.Helper()

	c, err := dial(addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close() //nolint:errcheck // read-only probe, the server owns the close.

	conn, err := protocol.Handshake(c)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var out bin.Buffer
	out.Put(make([]byte, 16))
	out.Buf[0] = 1 // A non-zero auth key id: not a key exchange, not a known key.
	if err := conn.Send(ctx, &out); err != nil {
		t.Fatalf("send: %v", err)
	}

	// The answer is a bare protocol error, which the client codec surfaces as
	// one rather than as a frame.
	var in bin.Buffer
	err = conn.Recv(ctx, &in)
	var protoErr *codec.ProtocolErr
	if !errors.As(err, &protoErr) {
		t.Fatalf("recv = %v, want protocol error", err)
	}
	if protoErr.Code != codec.CodeAuthKeyNotFound {
		t.Fatalf("protocol error code = %d, want %d", protoErr.Code, codec.CodeAuthKeyNotFound)
	}
}

// faultyListener fails the first faults accepts with err, wrapped the way the
// net package delivers an accept failure, before behaving normally.
type faultyListener struct {
	net.Listener

	err    error
	faults int
}

// warnSink signals the first record logged above Info, so a test can assert an
// escalation without matching on message text.
type warnSink struct {
	once   sync.Once
	warned chan struct{}
}

func (s *warnSink) Enabled(context.Context, slog.Level) bool { return true }
func (s *warnSink) WithAttrs([]slog.Attr) slog.Handler       { return s }
func (s *warnSink) WithGroup(string) slog.Handler            { return s }

func (s *warnSink) Handle(_ context.Context, r slog.Record) error {
	if r.Level > slog.LevelInfo {
		s.once.Do(func() { close(s.warned) })
	}
	return nil
}

// frameReadListener hands over connections that report the read after the one
// consuming the transport marker — that is, the first frame read, which is the
// side of the negotiation handoff a cancelled deadline no longer covers.
type frameReadListener struct {
	net.Listener

	reading chan struct{}
}

func (l *frameReadListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &frameReadConn{Conn: conn, reading: l.reading}, nil
}

// frameReadConn signals before the second read, at which point gotd has already
// applied the frame deadline and is about to block on the socket. Only the
// connection's own goroutine reads, so the counter needs no locking.
type frameReadConn struct {
	net.Conn

	reads   int
	once    sync.Once
	reading chan struct{}
}

func (c *frameReadConn) Read(b []byte) (int, error) {
	c.reads++
	if c.reads > 1 {
		c.once.Do(func() { close(c.reading) })
	}
	return c.Conn.Read(b)
}

// acceptedListener reports the first socket it hands over, so a test can act at
// the point where the server has one connection but has not finished
// negotiating it.
type acceptedListener struct {
	net.Listener

	once     sync.Once
	accepted chan struct{}
}

func (l *acceptedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.once.Do(func() { close(l.accepted) })
	}
	return conn, err
}

// dial opens a plain TCP connection to addr.
func dial(addr string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(context.Background(), "tcp", addr)
}

func (l *faultyListener) Accept() (net.Conn, error) {
	if l.faults > 0 {
		l.faults--
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: l.err}
	}
	return l.Listener.Accept()
}
