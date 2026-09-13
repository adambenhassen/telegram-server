//nolint:testpackage // These tests exercise the unexported discriminator and limiter.
package mtproto

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/exchange"

	"github.com/adambenhassen/telegram-server/internal/discovery"
)

func TestProbePreflightReplaysEveryNonMatchingByte(t *testing.T) {
	t.Parallel()

	server, client := net.Pipe()
	defer client.Close() //nolint:errcheck // the test owns the peer.
	input := append([]byte("telegramd-key-v2"), 1, 2, 3, 4, 5)
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := client.Write(input)
		writeDone <- errors.Join(writeErr, client.Close())
	}()

	probe, err := probePreflight(server, time.Time{})
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if probe.matched {
		t.Fatal("wrong-version request matched discovery")
	}
	if err := server.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set replay deadline: %v", err)
	}
	got, err := io.ReadAll(probe.stream)
	if err != nil {
		t.Fatalf("read replay: %v", err)
	}
	if !bytes.Equal(got, input) {
		t.Fatalf("replayed bytes = %x, want %x", got, input)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write input: %v", err)
	}
}

func TestProbePreflightRejectsPartialNonce(t *testing.T) {
	t.Parallel()

	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck // the test owns the peer.
	input := append([]byte(discovery.RequestMagic), 1, 2, 3)
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := client.Write(input)
		writeDone <- errors.Join(writeErr, client.Close())
	}()

	probe, err := probePreflight(server, time.Time{})
	if err == nil {
		t.Fatal("partial nonce was accepted")
	}
	if !probe.malformed || probe.matched {
		t.Fatalf("partial nonce probe = %+v, want malformed", probe)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write input: %v", err)
	}
}

func TestProbePreflightRejectsExpiredDeadline(t *testing.T) {
	t.Parallel()

	server, client := net.Pipe()
	defer server.Close() //nolint:errcheck // the test owns the peer.
	request, err := discovery.BuildPreflightRequest(make([]byte, discovery.NonceSize))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := client.Write(request)
		writeDone <- errors.Join(writeErr, client.Close())
	}()

	probe, err := probePreflight(server, time.Now().Add(-time.Millisecond))
	if err == nil {
		t.Fatal("expired preflight deadline was accepted")
	}
	if !probe.malformed || probe.matched {
		t.Fatalf("expired deadline probe = %+v, want malformed", probe)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write input: %v", err)
	}
}

func TestDiscoveryLimiterBoundsGlobalAndNetworkRequests(t *testing.T) {
	t.Parallel()

	l := newDiscoveryLimiter(DiscoveryLimits{
		MaxRequests:       2,
		MaxRequestsPerNet: 1,
		Window:            time.Minute,
		PerNetWindow:      time.Minute,
	})
	first := mustAddr("192.0.2.1")
	second := mustAddr("192.0.2.2")
	now := time.Unix(100, 0)
	if !l.allow(first, now) {
		t.Fatal("first request refused")
	}
	if l.allow(first, now) {
		t.Fatal("second request from one network was accepted")
	}
	if !l.allow(second, now) {
		t.Fatal("request from a distinct network refused")
	}
	if l.allow(mustAddr("192.0.2.3"), now) {
		t.Fatal("global discovery limit was not enforced")
	}
	if !l.allow(first, now.Add(time.Minute)) {
		t.Fatal("discovery limit did not recover after its window")
	}
}

func TestServePreflightReturnsWriteFailure(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	server := New(exchange.PrivateKey{RSA: key}, 2, NewMemoryAuthKeyStore(), nil, nil)
	conn := &failingPreflightConn{}
	var nonce [32]byte
	if err := server.servePreflight(context.Background(), conn, nonce); err == nil || !strings.Contains(err.Error(), "write discovery response") {
		t.Fatalf("servePreflight error = %v, want write failure", err)
	}
	if conn.writes != 1 {
		t.Fatalf("write attempts = %d, want one response operation", conn.writes)
	}
}

func TestServePreflightCancellationClosesBlockedWrite(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	server := New(exchange.PrivateKey{RSA: key}, 2, NewMemoryAuthKeyStore(), nil, nil)
	left, right := net.Pipe()
	defer right.Close() //nolint:errcheck // the test owns the peer.
	var nonce [32]byte
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.servePreflight(ctx, left, nonce) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("servePreflight succeeded after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close a blocked response write")
	}
}

type failingPreflightConn struct {
	writes int
}

func (*failingPreflightConn) Read([]byte) (int, error) { return 0, io.EOF }

func (c *failingPreflightConn) Write([]byte) (int, error) {
	c.writes++
	return 0, errors.New("write failed")
}

func (*failingPreflightConn) Close() error { return nil }

func (*failingPreflightConn) LocalAddr() net.Addr { return dummyAddr("local") }

func (*failingPreflightConn) RemoteAddr() net.Addr { return dummyAddr("remote") }

func (*failingPreflightConn) SetDeadline(time.Time) error { return nil }

func (*failingPreflightConn) SetReadDeadline(time.Time) error { return nil }

func (*failingPreflightConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return "tcp" }

func (a dummyAddr) String() string { return string(a) }

func mustAddr(raw string) netip.Addr {
	return netip.MustParseAddr(raw)
}
