package mtproto_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/mtproxy"
	"github.com/gotd/td/mtproxy/obfuscator"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestServeWebSocketHandlesMessageBoundaries(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		packed bool
		want   int
	}{
		{name: "split packet", want: 1},
		{name: "two packets in one message", packed: true, want: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()

			rsaKey, err := rsa.GenerateKey(rand.Reader, crypto.RSAKeyBits)
			if err != nil {
				t.Fatalf("rsa key: %v", err)
			}
			seen := make(chan netip.Addr, tt.want)
			handler := mtproto.HandlerFunc(func(c *mtproto.Conn, req *mtproto.Request) error {
				seen <- req.ClientAddr
				return c.SendResult(req, &tg.BoolTrue{})
			})
			srv := mtproto.New(
				exchange.PrivateKey{RSA: rsaKey},
				2,
				mtproto.NewMemoryAuthKeyStore(),
				handler,
				nil,
			)

			ln := mustListenTCP(t, ctx, "127.0.0.1:0")
			srvCtx, stopServer := context.WithCancel(ctx)
			served := make(chan error, 1)
			go func() { served <- srv.ServeWebSocket(srvCtx, ln) }()
			t.Cleanup(func() {
				stopServer()
				if err := <-served; err != nil {
					t.Errorf("serve websocket: %v", err)
				}
			})

			ws, err := dialWebSocket(ctx, ln.Addr().String())
			if err != nil {
				t.Fatalf("dial websocket: %v", err)
			}
			t.Cleanup(func() { closeWebSocket(t, ws) })

			stream := websocket.NetConn(ctx, ws, websocket.MessageBinary)
			boundary := &webSocketBoundaryConn{Conn: stream, splitHeader: true}
			obfs := obfuscator.Obfuscated2(rand.Reader, boundary)
			if err := obfs.Handshake(codec.Abridged{}.ObfuscatedTag(), 2, mtproxy.Secret{}); err != nil {
				t.Fatalf("obfuscation handshake: %v", err)
			}
			client, err := transport.NewProtocol(func() transport.Codec {
				return codec.NoHeader{Codec: codec.Abridged{}}
			}).Handshake(obfs)
			if err != nil {
				t.Fatalf("transport handshake: %v", err)
			}
			result, err := exchange.NewExchanger(client, 2).
				Client([]exchange.PublicKey{exchange.PrivateKey{RSA: rsaKey}.Public()}).
				Run(ctx)
			if err != nil {
				t.Fatalf("auth-key exchange: %v", err)
			}

			if tt.packed {
				boundary.bufferWrites = true
			}
			for i := range tt.want {
				frame := clientFrame(t, result.AuthKey, 42, int64(i+1)<<32, &tg.HelpGetConfigRequest{})
				if err := client.Send(ctx, &bin.Buffer{Buf: slices.Clone(frame)}); err != nil {
					t.Fatalf("send frame %d: %v", i, err)
				}
			}
			if tt.packed {
				if err := boundary.Flush(); err != nil {
					t.Fatalf("flush packed frames: %v", err)
				}
			}

			gotResults := 0
			clientCipher := crypto.NewClientCipher(crypto.DefaultRand())
			for gotResults < tt.want {
				var in bin.Buffer
				if err := client.Recv(ctx, &in); err != nil {
					t.Fatalf("recv response: %v", err)
				}
				message, err := clientCipher.DecryptFromBuffer(result.AuthKey, &in)
				if err != nil {
					t.Fatalf("decrypt response: %v", err)
				}
				body := &bin.Buffer{Buf: message.MessageDataWithPadding[:message.MessageDataLen]}
				id, err := body.PeekID()
				if err != nil {
					t.Fatalf("peek response id: %v", err)
				}
				if id == mt.NewSessionCreatedTypeID {
					continue
				}
				var result proto.Result
				if err := result.Decode(body); err != nil {
					t.Fatalf("decode rpc result: %v", err)
				}
				var value tg.BoolTrue
				if err := value.Decode(&bin.Buffer{Buf: result.Result}); err != nil {
					t.Fatalf("decode rpc value: %v", err)
				}
				gotResults++
			}

			for range tt.want {
				select {
				case addr := <-seen:
					if !addr.IsValid() {
						t.Error("handler saw an invalid WebSocket peer address")
					}
				case <-ctx.Done():
					t.Fatal("request did not reach handler")
				}
			}
		})
	}
}

func TestServeWebSocketUsesProxyAddress(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	base := mustListenTCP(t, ctx, "127.0.0.1:0")
	ln := &webSocketPreludeListener{
		Listener: base,
		header:   proxyV2Header(proxyCmdProxy, netip.MustParseAddr("203.0.113.9")),
	}
	key := rebindTestKey()
	seen := make(chan netip.Addr, 1)
	keys := mtproto.NewMemoryAuthKeyStore()
	if err := keys.Save(ctx, key); err != nil {
		t.Fatalf("save auth key: %v", err)
	}
	srv := mtproto.New(
		exchange.PrivateKey{},
		2,
		keys,
		mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
			seen <- req.ClientAddr
			return nil
		}),
		nil,
	)
	srv.TrustProxyV2Headers(loopback)
	served := make(chan error, 1)
	go func() { served <- srv.ServeWebSocket(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve websocket: %v", err)
		}
	})

	ws, err := dialWebSocket(ctx, base.Addr().String())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() { closeWebSocket(t, ws) })
	client, err := transport.Intermediate.Handshake(websocket.NetConn(ctx, ws, websocket.MessageBinary))
	if err != nil {
		t.Fatalf("transport handshake: %v", err)
	}
	frame := clientFrame(t, key, 42, int64(1)<<32, &tg.HelpGetConfigRequest{})
	if err := client.Send(ctx, &bin.Buffer{Buf: slices.Clone(frame)}); err != nil {
		t.Fatalf("send frame: %v", err)
	}
	select {
	case got := <-seen:
		if got != netip.MustParseAddr("203.0.113.9") {
			t.Fatalf("handler saw %s, want PROXY address 203.0.113.9", got)
		}
	case <-ctx.Done():
		t.Fatal("request did not reach handler")
	}
}

func TestServeWebSocketShutdownClosesHijackedConnection(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ln := mustListenTCP(t, ctx, "127.0.0.1:0")
	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	served := make(chan error, 1)
	go func() { served <- srv.ServeWebSocket(ctx, ln) }()

	ws, err := dialWebSocket(ctx, ln.Addr().String())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() { closeWebSocket(t, ws) })

	stream := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	obfs := obfuscator.Obfuscated2(rand.Reader, stream)
	if err := obfs.Handshake(codec.Abridged{}.ObfuscatedTag(), 2, mtproxy.Secret{}); err != nil {
		t.Fatalf("obfuscation handshake: %v", err)
	}
	cancel()

	closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if _, _, err := ws.Read(closeCtx); err == nil {
		t.Fatal("WebSocket stayed open after server shutdown")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("WebSocket stayed open until the client-side deadline after server shutdown")
	}
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve websocket: %v", err)
		}
	case <-closeCtx.Done():
		t.Fatal("ServeWebSocket did not stop after cancellation")
	}
}

func TestServeWebSocketHandshakeTimeout(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ln := mustListenTCP(t, ctx, "127.0.0.1:0")
	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	srv.SetHandshakeTimeout(100 * time.Millisecond)
	served := make(chan error, 1)
	go func() { served <- srv.ServeWebSocket(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve websocket: %v", err)
		}
	})

	ws, err := dialWebSocket(ctx, ln.Addr().String())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() { closeWebSocket(t, ws) })
	if err := ws.Write(ctx, websocket.MessageBinary, []byte{0}); err != nil {
		t.Fatalf("write partial framing: %v", err)
	}
	closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if _, _, err := ws.Read(closeCtx); err == nil {
		t.Fatal("partial transport framing was not closed at the handshake timeout")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("partial transport framing stayed open past the handshake timeout")
	}
}

func TestServeWebSocketRejectsOversizedMessage(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ln := mustListenTCP(t, ctx, "127.0.0.1:0")
	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	served := make(chan error, 1)
	go func() { served <- srv.ServeWebSocket(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve websocket: %v", err)
		}
	})

	ws, err := dialWebSocket(ctx, ln.Addr().String())
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() { closeWebSocket(t, ws) })

	const bodyLen = 16<<20 + 128
	frame := make([]byte, 1+4+bodyLen)
	frame[0] = 0xef
	frame[1] = 0x7f
	binary.LittleEndian.PutUint32(frame[2:6], bodyLen/4)
	if err := ws.Write(ctx, websocket.MessageBinary, frame); err != nil {
		t.Fatalf("write oversized message: %v", err)
	}

	closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if _, _, err := ws.Read(closeCtx); err == nil {
		t.Fatal("oversized WebSocket message was accepted")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("oversized WebSocket message did not close the connection")
	} else if status := websocket.CloseStatus(err); status != websocket.StatusMessageTooBig {
		t.Fatalf("close status = %v, want %v: %v", status, websocket.StatusMessageTooBig, err)
	}
}

func TestServeWebSocketReleasesSlotAfterMalformedMessage(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ln := mustListenTCP(t, ctx, "127.0.0.1:0")
	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	if err := srv.SetPreAuthLimits(mtproto.PreAuthLimits{MaxConns: 1}); err != nil {
		t.Fatalf("set pre-auth limits: %v", err)
	}
	served := make(chan error, 1)
	go func() { served <- srv.ServeWebSocket(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve websocket: %v", err)
		}
	})

	bad, err := dialWebSocket(ctx, ln.Addr().String())
	if err != nil {
		t.Fatalf("dial malformed peer: %v", err)
	}
	if err := bad.Write(ctx, websocket.MessageBinary, []byte{0xef, 0}); err != nil {
		t.Fatalf("write malformed message: %v", err)
	}
	closeCtx, stop := context.WithTimeout(context.Background(), 2*time.Second)
	defer stop()
	if _, _, err := bad.Read(closeCtx); err == nil {
		t.Fatal("malformed WebSocket message was accepted")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("malformed WebSocket message did not close the connection")
	}
	var good *websocket.Conn
	dialCtx, stopDial := context.WithTimeout(ctx, 2*time.Second)
	defer stopDial()
	for {
		good, err = dialWebSocket(dialCtx, ln.Addr().String())
		if err == nil {
			break
		}
		select {
		case <-dialCtx.Done():
			t.Fatalf("dial after malformed peer: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	closeWebSocket(t, good)
}

func closeWebSocket(t *testing.T, ws *websocket.Conn) {
	t.Helper()
	if err := ws.CloseNow(); err != nil {
		t.Logf("close WebSocket: %v", err)
	}
}

func dialWebSocket(ctx context.Context, addr string) (*websocket.Conn, error) {
	ws, resp, err := websocket.Dial(ctx, "ws://"+addr+"/apiws", &websocket.DialOptions{
		HTTPHeader:   http.Header{"Origin": []string{"https://web.telegram.org"}},
		Subprotocols: []string{"binary"},
	})
	if resp != nil && resp.Body != nil {
		if closeErr := resp.Body.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}
	return ws, err
}

type webSocketPreludeListener struct {
	net.Listener

	header []byte
}

func (l *webSocketPreludeListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &webSocketPreludeConn{Conn: conn, header: slices.Clone(l.header)}, nil
}

type webSocketPreludeConn struct {
	net.Conn

	header []byte
}

func (c *webSocketPreludeConn) Read(p []byte) (int, error) {
	if len(c.header) == 0 {
		return c.Conn.Read(p)
	}
	n := copy(p, c.header)
	c.header = c.header[n:]
	return n, nil
}

// webSocketBoundaryConn makes the transport stream cross WebSocket message
// boundaries that do not match MTProto packets. The first write is split in
// half to model a browser delivering the obfuscation init in two messages. In
// packed mode later writes are held until Flush, putting two complete packets
// in one message.
type webSocketBoundaryConn struct {
	net.Conn

	splitHeader  bool
	bufferWrites bool
	pending      []byte
}

func (c *webSocketBoundaryConn) Write(p []byte) (int, error) {
	if c.splitHeader {
		c.splitHeader = false
		if len(p) != 64 {
			return 0, errors.New("obfuscation header was not one write")
		}
		if _, err := c.Conn.Write(p[:32]); err != nil {
			return 0, err
		}
		if _, err := c.Conn.Write(p[32:]); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if c.bufferWrites {
		c.pending = append(c.pending, p...)
		return len(p), nil
	}
	return c.Conn.Write(p)
}

func (c *webSocketBoundaryConn) Flush() error {
	if len(c.pending) == 0 {
		return nil
	}
	pending := c.pending
	c.pending = nil
	_, err := c.Conn.Write(pending)
	return err
}
