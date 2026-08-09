package mtproto_test

import (
	"context"
	"net"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// TestRequestCarriesTheSocketPeerAddress is the whole basis of every per-IP
// limit: the address a handler is handed must be the one the connection
// actually came from, over a real socket, for both address families.
//
// It is asserted end to end rather than on the parsing helper because the value
// travels through the accept path, the codec negotiation and the serve loop
// before a handler sees it, and each of those is a place it could be lost or
// replaced. Nothing the client sends contributes to it: the frame below carries
// no address, because the protocol has no field for one.
func TestRequestCarriesTheSocketPeerAddress(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		listen string
	}{
		{name: "ipv4", listen: "127.0.0.1:0"},
		{name: "ipv6", listen: "[::1]:0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			key := rebindTestKey()
			keys := mtproto.NewMemoryAuthKeyStore()
			if err := keys.Save(ctx, key); err != nil {
				t.Fatalf("save key: %v", err)
			}

			seen := make(chan netip.Addr, 1)
			handler := mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
				select {
				case seen <- req.ClientAddr:
				default:
				}
				return nil
			})
			// No key exchange runs here: the client sends an encrypted frame
			// under a key the store already holds, so the server needs no
			// private key of its own.
			srv := mtproto.New(exchange.PrivateKey{}, 2, keys, handler, nil)

			nl, err := (&net.ListenConfig{}).Listen(ctx, "tcp", tt.listen)
			if err != nil {
				t.Skipf("listen on %s: %v", tt.listen, err)
			}
			srvCtx, stopServer := context.WithCancel(ctx)
			served := make(chan error, 1)
			go func() { served <- srv.Serve(srvCtx, mtproto.Listen(nl)) }()
			t.Cleanup(func() {
				stopServer()
				if err := <-served; err != nil {
					t.Errorf("serve: %v", err)
				}
			})

			raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", nl.Addr().String())
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			// Closed before the server is stopped (cleanups run last-registered
			// first), so the serve loop sees the disconnect and returns.
			t.Cleanup(func() {
				if err := raw.Close(); err != nil {
					t.Errorf("close client: %v", err)
				}
			})
			client, err := transport.Intermediate.Handshake(raw)
			if err != nil {
				t.Fatalf("transport handshake: %v", err)
			}

			// help.getConfig rather than a ping: service messages are answered
			// inside the serve loop and never reach a Handler.
			frame := clientFrame(t, key, 42, int64(1)<<32, &tg.HelpGetConfigRequest{})
			if err := client.Send(ctx, &bin.Buffer{Buf: slices.Clone(frame)}); err != nil {
				t.Fatalf("send frame: %v", err)
			}

			local, ok := raw.LocalAddr().(*net.TCPAddr)
			if !ok {
				t.Fatalf("client local addr type = %T", raw.LocalAddr())
			}
			want := local.AddrPort().Addr().Unmap()

			select {
			case got := <-seen:
				if got != want {
					t.Errorf("handler saw client address %s, want the connecting socket's %s", got, want)
				}
			case <-ctx.Done():
				t.Fatal("no request reached the handler")
			}
		})
	}
}
