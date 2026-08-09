package mtproto_test

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// The proxy-v2 half of "one silent peer must not hold up anyone else". Socket
// mode is covered by the accept suite (TestServeClosesSilentConnection and the
// e2e TestLoginWhileConnectionSilent); what is particular here is that the
// sender gets past the allowlist and the header before it goes quiet, so it is
// inside the same handshake bound the codec sniff runs under.

// TestProxyV2SilentAfterHeaderDoesNotStallAccept holds a connection that wrote a
// valid header and then stopped, and requires an ordinary client behind it to be
// served anyway.
func TestProxyV2SilentAfterHeaderDoesNotStallAccept(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	seen, key := serveClients(t, ctx, nl, loopback, nil)

	stalled := netip.MustParseAddr("203.0.113.9")
	stall := dialClient(t, ctx, nl.Addr().String())
	mustWrite(t, stall, proxyV2Header(proxyCmdProxy, stalled))
	// Given a moment to be picked up, so the client below is genuinely behind
	// it rather than racing it.
	time.Sleep(200 * time.Millisecond)

	client := netip.MustParseAddr("198.51.100.7")
	speak(t, ctx, nl.Addr().String(), proxyV2Header(proxyCmdProxy, client), transport.Abridged, key)

	if got := wantClientAddr(t, ctx, seen); got != client {
		t.Errorf("served address %s, want the client behind the silent peer, %s", got, client)
	}
}

// TestRefusedConnectionsDoNotFloodTheLog is the same concern one layer over: a
// refusal is driven by whoever can reach the port, unauthenticated, so a line
// per refusal is a log an attacker writes as fast as it can open sockets. The
// line that does come out has to say how many it stands for, or the bound turns
// a flood into silence.
func TestRefusedConnectionsDoNotFloodTheLog(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var log syncBuffer
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	// An allowlist loopback is not in, so every connection below is refused on
	// the first check and nothing depends on timing out.
	allow := []netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}
	serveClients(t, ctx, nl, allow, slog.New(slog.NewTextHandler(&log, nil)))

	const refusals = 25
	for range refusals {
		conn := dialClient(t, ctx, nl.Addr().String())
		mustWrite(t, conn, proxyV2Header(proxyCmdProxy, netip.MustParseAddr("203.0.113.9")))
		// Waited out one at a time, so all of them have been refused and logged
		// by the time the buffer is read.
		assertClosed(t, conn)
	}

	lines := strings.Count(log.String(), "transport negotiation error")
	if lines != 1 {
		t.Errorf("%d refusals wrote %d log lines, want 1 inside the sampling window:\n%s", refusals, lines, log.String())
	}
	if !strings.Contains(log.String(), "suppressed") {
		t.Errorf("the emitted line does not say how many refusals it stands for:\n%s", log.String())
	}
}

// syncBuffer collects log output written from the per-connection goroutines,
// which slog does not serialise for a plain bytes.Buffer.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestServeShutdownDoesNotAwaitProxyHeader is the proxy-v2 half of the
// cancellation property the accept suite establishes for codec detection.
//
// This mode adds a second read ahead of that one, on the same socket and under
// the same handshake bound, so it adds a second place a stopping process could
// be made to wait out that bound. It must not: cancellation is delivered by
// closing the socket, which ends whichever of the two reads the connection is
// in. The peer here is allowlisted and has written half a header, so the server
// is blocked inside the header read rather than the codec sniff.
func TestServeShutdownDoesNotAwaitProxyHeader(t *testing.T) {
	t.Parallel()

	nl, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	accepted := make(chan struct{})
	al := &acceptedListener{Listener: nl, accepted: accepted}

	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	srv.TrustProxyV2Headers(loopback)
	// The production bound, deliberately: the assertion below is that shutdown
	// does not wait for it.
	srv.SetHandshakeTimeout(30 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, al); close(done) }()

	half, err := dial(nl.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("dial: %v", err)
	}
	defer half.Close() //nolint:errcheck // the server owns this socket's close.
	// Half a header: past the allowlist, into the read, and never completing.
	if _, err := half.Write(proxyV2Header(proxyCmdProxy, netip.MustParseAddr("203.0.113.9"))[:8]); err != nil {
		cancel()
		t.Fatalf("write partial header: %v", err)
	}

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
		t.Fatal("Serve waited out the handshake bound on a connection still reading its PROXY header")
	}
}
