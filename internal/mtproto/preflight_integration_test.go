package mtproto_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"

	"github.com/adambenhassen/telegram-server/internal/discovery"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestLocalPreflightReturnsConfiguredPublicIdentityAndDC(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	keys := &recordingAuthKeyStore{}
	var handlerCalls atomic.Int64
	handler := mtproto.HandlerFunc(func(*mtproto.Conn, *mtproto.Request) error {
		handlerCalls.Add(1)
		return nil
	})
	server := mtproto.New(exchange.PrivateKey{RSA: key}, 7, keys, handler, nil)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})

	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck // server closes the preflight socket.
	nonce := make([]byte, discovery.NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	request, err := discovery.BuildPreflightRequest(nonce)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	closeWrite(t, conn)
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	response, err := io.ReadAll(conn)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	gotDC, gotKey, err := discovery.ParsePreflightResponse(response, nonce)
	if err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if gotDC != 7 {
		t.Fatalf("response DC = %d, want 7", gotDC)
	}
	if gotKey.N.Cmp(key.N) != 0 || gotKey.E != key.E {
		t.Fatal("response key differs from the key used by the server")
	}
	if got := keys.saveCalls.Load(); got != 0 {
		t.Fatalf("auth-key saves = %d, want 0", got)
	}
	if got := keys.getCalls.Load(); got != 0 {
		t.Fatalf("auth-key gets = %d, want 0", got)
	}
	if got := keys.touchCalls.Load(); got != 0 {
		t.Fatalf("auth-key touches = %d, want 0", got)
	}
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("RPC handler calls = %d, want 0", got)
	}
	if got := server.Registry().TotalConns(); got != 0 {
		t.Fatalf("live sessions = %d, want 0", got)
	}
	if got := server.Registry().TotalSessions(); got != 0 {
		t.Fatalf("session owners = %d, want 0", got)
	}
}

func TestPartialPreflightNeverProducesDiscoveryResponse(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	server.SetHandshakeTimeout(100 * time.Millisecond)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-serveDone:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})

	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck // server owns the partial socket.
	if _, err := conn.Write([]byte(discovery.RequestMagic[:8])); err != nil {
		t.Fatalf("write partial request: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var response [1]byte
	_, err = conn.Read(response[:])
	if err == nil {
		t.Fatal("partial preflight received a response")
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatal("partial preflight socket remained open after the deadline")
	}
}

func TestOverlongPreflightNeverProducesDiscoveryResponse(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-serveDone:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})

	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck // server owns the overlong socket.
	nonce := make([]byte, discovery.NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	request, err := discovery.BuildPreflightRequest(nonce)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	request = append(request, 0xff)
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("write overlong request: %v", err)
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	response, err := io.ReadAll(conn)
	if err != nil && len(response) > 0 {
		t.Fatalf("read response (%d bytes): %v", len(response), err)
	}
	if len(response) != 0 {
		t.Fatalf("overlong request received %d response bytes", len(response))
	}
}

func TestDelayedTrailingPreflightByteNeverProducesDiscoveryResponse(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	server.SetHandshakeTimeout(250 * time.Millisecond)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-serveDone:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})

	conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck // server owns the malformed socket.
	nonce := make([]byte, discovery.NonceSize)
	if _, err := rand.Read(nonce); err != nil {
		t.Fatalf("nonce: %v", err)
	}
	request, err := discovery.BuildPreflightRequest(nonce)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	if _, err := conn.Write(request); err != nil {
		t.Fatalf("write request: %v", err)
	}
	// The old discriminator answered after a short probe. Delay the trailing
	// byte long enough that such an answer is already on the wire.
	time.Sleep(20 * time.Millisecond)
	if _, err := conn.Write([]byte{0xff}); err != nil {
		t.Fatalf("write delayed trailing byte: %v", err)
	}
	closeWrite(t, conn)
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	response, err := io.ReadAll(conn)
	if err != nil && len(response) > 0 {
		t.Fatalf("read response (%d bytes): %v", len(response), err)
	}
	if len(response) != 0 {
		t.Fatalf("delayed trailing byte received %d response bytes", len(response))
	}
}

func TestPreflightBlockedWriteHonorsHandshakeDeadline(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	request, err := discovery.BuildPreflightRequest(make([]byte, discovery.NonceSize))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	conn := newDeadlinePreflightConn(request)
	listener := newPreflightListener(conn)
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	server.SetHandshakeTimeout(100 * time.Millisecond)
	server.SetWriteTimeout(time.Second)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-serveDone:
			if err != nil {
				t.Errorf("serve: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})

	var started time.Time
	select {
	case started = <-conn.writeStarted:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("preflight response write did not start")
	}
	select {
	case finished := <-conn.writeFinished:
		if elapsed := finished.Sub(started); elapsed > 500*time.Millisecond {
			t.Fatalf("blocked preflight write lasted %s, want <= 500ms", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("blocked preflight write outlived the handshake deadline")
	}
	cancel()
}

func TestPreflightRunsAfterProxyHeaderAndChargesReportedNetwork(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	server.TrustProxyV2Headers([]netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")})
	if err := server.SetDiscoveryLimits(mtproto.DiscoveryLimits{
		MaxRequestsPerNet: 1,
		PerNetWindow:      time.Minute,
	}); err != nil {
		t.Fatalf("set discovery limits: %v", err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-serveDone:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})

	request := func(clientAddr netip.Addr) []byte {
		nonce := make([]byte, discovery.NonceSize)
		if _, err := rand.Read(nonce); err != nil {
			t.Fatalf("nonce: %v", err)
		}
		body, err := discovery.BuildPreflightRequest(nonce)
		if err != nil {
			t.Fatalf("build request: %v", err)
		}
		return append(proxyV2Header(proxyCmdProxy, clientAddr), body...)
	}
	readResult := func(input []byte) int {
		conn, err := (&net.Dialer{}).DialContext(context.Background(), "tcp", listener.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close() //nolint:errcheck // server owns the preflight socket.
		if _, err := conn.Write(input); err != nil {
			t.Fatalf("write preflight: %v", err)
		}
		closeWrite(t, conn)
		if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
			t.Fatalf("set read deadline: %v", err)
		}
		response, readErr := io.ReadAll(conn)
		if readErr != nil && len(response) > 0 {
			t.Fatalf("read preflight response (%d bytes): %v", len(response), readErr)
		}
		return len(response)
	}

	if got := readResult(request(netip.MustParseAddr("198.51.100.1"))); got == 0 {
		t.Fatal("first proxied preflight received no response")
	}
	if got := readResult(request(netip.MustParseAddr("198.51.100.1"))); got != 0 {
		t.Fatalf("same reported network received %d response bytes", got)
	}
	if got := readResult(request(netip.MustParseAddr("203.0.113.1"))); got == 0 {
		t.Fatal("distinct reported network was refused")
	}
}

type recordingAuthKeyStore struct {
	saveCalls  atomic.Int64
	getCalls   atomic.Int64
	touchCalls atomic.Int64
}

func (s *recordingAuthKeyStore) Save(context.Context, crypto.AuthKey) error {
	s.saveCalls.Add(1)
	return nil
}

func (s *recordingAuthKeyStore) Get(context.Context, [8]byte) (crypto.AuthKey, int64, bool, bool, error) {
	s.getCalls.Add(1)
	return crypto.AuthKey{}, 0, false, false, nil
}

func (s *recordingAuthKeyStore) Touch(context.Context, [8]byte) error {
	s.touchCalls.Add(1)
	return nil
}

type preflightListener struct {
	conn      net.Conn
	addr      net.Addr
	closed    chan struct{}
	closeOnce sync.Once
	accepted  bool
}

func newPreflightListener(conn net.Conn) *preflightListener {
	return &preflightListener{conn: conn, addr: dummyPreflightAddr("listener"), closed: make(chan struct{})}
}

func (l *preflightListener) Accept() (net.Conn, error) {
	if !l.accepted {
		l.accepted = true
		return l.conn, nil
	}
	<-l.closed
	return nil, net.ErrClosed
}

func (l *preflightListener) Close() error {
	l.closeOnce.Do(func() { close(l.closed) })
	return nil
}

func (l *preflightListener) Addr() net.Addr { return l.addr }

type deadlinePreflightConn struct {
	reader        *bytes.Reader
	closed        chan struct{}
	closeOnce     sync.Once
	writeDeadline time.Time
	writeMu       sync.Mutex
	writeStarted  chan time.Time
	writeFinished chan time.Time
}

func newDeadlinePreflightConn(request []byte) *deadlinePreflightConn {
	return &deadlinePreflightConn{
		reader:        bytes.NewReader(request),
		closed:        make(chan struct{}),
		writeStarted:  make(chan time.Time, 1),
		writeFinished: make(chan time.Time, 1),
	}
}

func (c *deadlinePreflightConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c *deadlinePreflightConn) Write([]byte) (int, error) {
	started := time.Now()
	c.writeStarted <- started
	c.writeMu.Lock()
	deadline := c.writeDeadline
	c.writeMu.Unlock()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-timer.C:
		c.writeFinished <- time.Now()
		return 0, preflightTimeoutError{}
	case <-c.closed:
		c.writeFinished <- time.Now()
		return 0, net.ErrClosed
	}
}

func (c *deadlinePreflightConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

func (*deadlinePreflightConn) LocalAddr() net.Addr  { return dummyPreflightAddr("local") }
func (*deadlinePreflightConn) RemoteAddr() net.Addr { return dummyPreflightAddr("remote") }

func (c *deadlinePreflightConn) SetDeadline(deadline time.Time) error {
	return errors.Join(c.SetReadDeadline(deadline), c.SetWriteDeadline(deadline))
}

func (*deadlinePreflightConn) SetReadDeadline(time.Time) error { return nil }

func (c *deadlinePreflightConn) SetWriteDeadline(deadline time.Time) error {
	c.writeMu.Lock()
	c.writeDeadline = deadline
	c.writeMu.Unlock()
	return nil
}

type preflightTimeoutError struct{}

func (preflightTimeoutError) Error() string   { return "preflight write timeout" }
func (preflightTimeoutError) Timeout() bool   { return true }
func (preflightTimeoutError) Temporary() bool { return true }

type dummyPreflightAddr string

func (dummyPreflightAddr) Network() string  { return "tcp" }
func (a dummyPreflightAddr) String() string { return string(a) }
