package mtproto_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/transport"
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
