package mtproto_test

import (
	"context"
	"errors"
	"net"
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

// TestServeSurvivesTemporaryAcceptError covers the listener-level half of the
// invariant: an accept failure that is transient (the process is momentarily
// out of file descriptors) belongs to no connection and clears on its own, so
// it must not end serving.
func TestServeSurvivesTemporaryAcceptError(t *testing.T) {
	t.Parallel()

	nl, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	fl := &faultyListener{Listener: nl, faults: 3}

	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), fl); close(done) }()
	t.Cleanup(func() {
		if cerr := fl.Close(); cerr != nil && !errors.Is(cerr, net.ErrClosed) {
			t.Errorf("listener close: %v", cerr)
		}
		<-done
	})

	assertServes(t, nl.Addr().String(), transport.Abridged)

	select {
	case err := <-done:
		t.Fatalf("Serve returned after a transient accept fault: %v", err)
	default:
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

// faultyListener fails the first faults accepts with a transient
// out-of-descriptors error before behaving normally, standing in for a burst of
// connection opens exhausting the process's file descriptors.
type faultyListener struct {
	net.Listener

	faults int
}

// dial opens a plain TCP connection to addr.
func dial(addr string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(context.Background(), "tcp", addr)
}

func (l *faultyListener) Accept() (net.Conn, error) {
	if l.faults > 0 {
		l.faults--
		return nil, &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
	}
	return l.Listener.Accept()
}
