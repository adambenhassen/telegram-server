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

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// These two cover the same scenario Security's repro did, with the assertion
// the other way up: a peer that opens a socket and then says nothing must not
// hold up the connections behind it.
//
// Setting a connection up means reading from it — the codec tag in every mode,
// the PROXY header in proxy-v2 — and a silent peer never sends those bytes. Done
// on the accept path that is one unauthenticated socket, no payload, and the
// server accepts nobody: not a slow path but a stopped one.

// TestOneSilentPeerDoesNotStallAccept holds a silent socket open in socket mode
// and requires an ordinary client behind it to be accepted anyway.
func TestOneSilentPeerDoesNotStallAccept(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	accepted := acceptLoop(t, mtproto.Listen(nl))

	// The attacker: connect, write nothing, hold it open.
	dial(t, ctx, nl.Addr().String())
	// Given a moment to be picked up, so the victim below is genuinely behind
	// it rather than racing it.
	time.Sleep(200 * time.Millisecond)

	// The victim: an ordinary abridged client.
	good := dial(t, ctx, nl.Addr().String())
	mustWrite(t, good, []byte{0xef})

	want := netip.MustParseAddrPort(good.LocalAddr().String()).Addr().Unmap()
	select {
	case got, ok := <-accepted:
		if !ok {
			t.Fatal("the listener stopped accepting")
		}
		if got.addr != want {
			t.Errorf("accepted address %s, want the victim's %s", got.addr, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing accepted in 5s: one silent peer holds the accept path")
	}
}

// TestProxyV2SilentAfterHeaderDoesNotStallAccept is the proxy-v2 half. The
// sender is inside the allowlist and writes a valid header, so it is past the
// address check and into the codec sniff when it goes quiet — the read that used
// to run with the header deadline already cleared.
func TestProxyV2SilentAfterHeaderDoesNotStallAccept(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	accepted := acceptLoop(t, mtproto.ListenProxyV2(nl, loopback, nil))

	stalled := netip.MustParseAddr("203.0.113.9")
	stall := dial(t, ctx, nl.Addr().String())
	mustWrite(t, stall, proxyV2Header(proxyCmdProxy, stalled))
	time.Sleep(200 * time.Millisecond)

	client := netip.MustParseAddr("198.51.100.7")
	good := dial(t, ctx, nl.Addr().String())
	mustWrite(t, good, append(proxyV2Header(proxyCmdProxy, client), 0xef))

	select {
	case got, ok := <-accepted:
		if !ok {
			t.Fatal("the listener stopped accepting")
		}
		if got.addr != client {
			t.Errorf("accepted address %s, want the victim's %s", got.addr, client)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("nothing accepted in 5s: a peer silent after its header holds the accept path")
	}
}

// TestSilentPeerIsDroppedAtTheSetupDeadline is the other half of not stalling.
// Moving setup off the accept path stops a silent peer blocking anyone, but on
// its own it would let one hold a goroutine and a file descriptor for as long as
// it liked. The deadline is what bounds that, and the bound is the whole reason
// a peer cannot pile them up.
func TestSilentPeerIsDroppedAtTheSetupDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	acceptLoop(t, mtproto.Listen(nl))

	stall := dial(t, ctx, nl.Addr().String())
	// Past the 30s setup deadline. Slow by unit-test standards, parallel so the
	// wall time hides behind the rest of the package, and deliberately not
	// mocked: the deadline is the mitigation, and a test that stubbed the clock
	// would not show the socket actually being dropped.
	if err := stall.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var b [1]byte
	switch _, err := stall.Read(b[:]); {
	case err == nil:
		t.Fatal("the server wrote to a connection it never set up")
	case isTimeout(err):
		t.Fatal("a silent connection was still held 60s after connecting, past the setup deadline")
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
	acceptLoop(t, mtproto.ListenProxyV2(nl, allow, slog.New(slog.NewTextHandler(&log, nil))))

	const refusals = 25
	for range refusals {
		conn := dial(t, ctx, nl.Addr().String())
		mustWrite(t, conn, proxyV2Header(proxyCmdProxy, netip.MustParseAddr("203.0.113.9")))
		// Waited out one at a time, so all of them have been refused and logged
		// by the time the buffer is read.
		assertClosed(t, conn)
	}

	lines := strings.Count(log.String(), "refused connection")
	if lines != 1 {
		t.Errorf("%d refusals wrote %d log lines, want 1 inside the sampling window:\n%s", refusals, lines, log.String())
	}
	if !strings.Contains(log.String(), "suppressed") {
		t.Errorf("the emitted line does not say how many refusals it stands for:\n%s", log.String())
	}
}

// syncBuffer collects log output written from the per-connection setup
// goroutines, which slog does not serialise for a plain bytes.Buffer.
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
