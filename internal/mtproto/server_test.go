package mtproto_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// TestServeReturnsOnListenerClose verifies Serve unblocks promptly when the
// listener is closed while the context is still live (P1 regression).
func TestServeReturnsOnListenerClose(t *testing.T) {
	nl, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	l := transport.Listen(nl)

	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)

	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background(), l) }()

	// Give Serve a moment to start, then close the listener with ctx still live.
	time.Sleep(20 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return within 2s after listener close")
	}
}
