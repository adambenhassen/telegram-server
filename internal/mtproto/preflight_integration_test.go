package mtproto_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io"
	"net"
	"net/netip"
	"testing"
	"time"

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
	server := mtproto.New(exchange.PrivateKey{RSA: key}, 7, mtproto.NewMemoryAuthKeyStore(), nil, nil)
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
