package mtproto_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
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
			} else {
				boundary.splitPacket = true
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
			} else if boundary.splitPacket {
				t.Fatal("abridged length prefix and body were not separate writes")
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

func TestServeWebSocketProxyAddressDoesNotBlockNextAccept(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	base := mustListenTCP(t, ctx, "127.0.0.1:0")
	accepted := make(chan struct{}, 1)
	ln := &webSocketPreludeListener{
		Listener:        base,
		header:          proxyV2Header(proxyCmdProxy, netip.MustParseAddr("203.0.113.9")),
		accepted:        accepted,
		skipFirstHeader: true,
	}
	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	srv.TrustProxyV2Headers(loopback)
	srv.SetHandshakeTimeout(time.Second)
	served := make(chan error, 1)
	go func() { served <- srv.ServeWebSocket(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve websocket: %v", err)
		}
	})

	silent, err := (&net.Dialer{}).DialContext(ctx, "tcp", base.Addr().String())
	if err != nil {
		t.Fatalf("dial silent peer: %v", err)
	}
	t.Cleanup(func() {
		if err := silent.Close(); err != nil {
			t.Logf("close silent peer: %v", err)
		}
	})
	select {
	case <-accepted:
	case <-ctx.Done():
		t.Fatal("server never accepted the silent peer")
	}

	healthyCtx, stopHealthy := context.WithTimeout(ctx, 500*time.Millisecond)
	defer stopHealthy()
	healthy, err := dialWebSocket(healthyCtx, base.Addr().String())
	if err != nil {
		t.Fatalf("healthy peer handshake blocked behind silent peer: %v", err)
	}
	closeWebSocket(t, healthy)
}

func TestServeWebSocketNegotiationUsesOneBudget(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	base := mustListenTCP(t, ctx, "127.0.0.1:0")
	accepted := make(chan time.Time, 1)
	ln := &timestampWebSocketListener{Listener: base, accepted: accepted}
	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
	srv.SetHandshakeTimeout(300 * time.Millisecond)
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

	first, err := (&net.Dialer{}).DialContext(ctx, "tcp", base.Addr().String())
	if err != nil {
		t.Fatalf("dial slow peer: %v", err)
	}
	t.Cleanup(func() {
		if err := first.Close(); err != nil {
			t.Logf("close slow peer: %v", err)
		}
	})
	var acceptedAt time.Time
	select {
	case acceptedAt = <-accepted:
	case <-ctx.Done():
		t.Fatal("server never accepted the slow peer")
	}
	request := rawWebSocketHandshake()
	if _, err := first.Write(request[:len(request)-2]); err != nil {
		t.Fatalf("write partial raw WebSocket handshake: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if _, err := first.Write(request[len(request)-2:]); err != nil {
		t.Fatalf("finish raw WebSocket handshake: %v", err)
	}
	if err := readRawWebSocketHandshake(first); err != nil {
		t.Fatalf("read slow peer handshake: %v", err)
	}
	if _, err := first.Write([]byte{0x82, 0x81, 0, 0, 0, 0, 0}); err != nil {
		t.Fatalf("write partial transport frame: %v", err)
	}

	dialCtx, stopDial := context.WithDeadline(ctx, acceptedAt.Add(450*time.Millisecond))
	defer stopDial()
	var healthy *websocket.Conn
	for {
		healthy, err = dialWebSocket(dialCtx, base.Addr().String())
		if err == nil {
			break
		}
		select {
		case <-dialCtx.Done():
			t.Fatalf("slot was not released within one negotiation budget: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer closeWebSocket(t, healthy)
	if elapsed := time.Since(acceptedAt); elapsed > 450*time.Millisecond {
		t.Fatalf("slot was released after %s, want one negotiation budget", elapsed)
	}
}

func TestServeWebSocketOriginPolicy(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name           string
		origin         string
		originPatterns []string
		sameHost       bool
		wantError      bool
	}{
		{name: "absent", wantError: false},
		{name: "allowed", origin: "https://web.telegram.org", originPatterns: []string{"https://web.telegram.org"}, wantError: false},
		{name: "unlisted", origin: "https://evil.example", wantError: true},
		{name: "same-host-unset", sameHost: true, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			ln := mustListenTCP(t, ctx, "127.0.0.1:0")
			srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, nil)
			srv.SetWebSocketOriginPatterns(tt.originPatterns)
			served := make(chan error, 1)
			go func() { served <- srv.ServeWebSocket(ctx, ln) }()
			t.Cleanup(func() {
				cancel()
				if err := <-served; err != nil {
					t.Errorf("serve websocket: %v", err)
				}
			})

			origin := tt.origin
			if tt.sameHost {
				origin = "http://" + ln.Addr().String()
			}
			ws, err := dialWebSocketOrigin(ctx, ln.Addr().String(), origin)
			if tt.wantError {
				if err == nil {
					closeWebSocket(t, ws)
					t.Fatal("unlisted WebSocket origin was accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("headerless WebSocket handshake: %v", err)
			}
			closeWebSocket(t, ws)
		})
	}
}

func TestServeWebSocketReportsPersistentAcceptFailures(t *testing.T) {
	t.Parallel()

	base := mustListenTCP(t, context.Background(), "127.0.0.1:0")
	fl := &faultyListener{Listener: base, err: syscall.EMFILE, faults: 12}
	sink := &webSocketAcceptLogSink{
		info: make(chan struct{}, 1),
		warn: make(chan struct{}, 1),
	}
	srv := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, slog.New(sink))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ServeWebSocket(ctx, fl) }()
	t.Cleanup(func() {
		cancel()
		if err := fl.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("listener close: %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("serve websocket: %v", err)
		}
	})

	select {
	case <-sink.info:
	case <-time.After(5 * time.Second):
		t.Fatal("persistent WebSocket accept failure was not reported at Info")
	}
	select {
	case <-sink.warn:
	case <-time.After(30 * time.Second):
		t.Fatal("persistent WebSocket accept failure was not escalated at Warn")
	}
}

func TestServeWebSocketWriteTimeoutDoesNotBlockOtherPeer(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	ln := mustListenTCP(t, ctx, "127.0.0.1:0")
	key := rebindTestKey()
	keys := &websocketAuthKeyStore{key: key}
	var blackhole atomic.Pointer[mtproto.Conn]
	blackholeSelected := make(chan struct{})
	writeFailed := make(chan error, 1)
	handler := mtproto.HandlerFunc(func(c *mtproto.Conn, req *mtproto.Request) error {
		if blackhole.CompareAndSwap(nil, c) {
			close(blackholeSelected)
		}
		if blackhole.Load() == c {
			err := c.SendResult(req, websocketLargeResult{payload: make([]byte, 8<<20)})
			if err != nil {
				select {
				case writeFailed <- err:
				default:
				}
			}
			return err
		}
		return c.SendResult(req, &tg.BoolTrue{})
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, keys, handler, nil)
	srv.SetWriteTimeout(100 * time.Millisecond)
	served := make(chan error, 1)
	go func() { served <- srv.ServeWebSocket(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve websocket: %v", err)
		}
	})

	blackCtx, cancelBlack := context.WithCancel(ctx)
	defer cancelBlack()
	blackWS, err := dialWebSocket(blackCtx, ln.Addr().String())
	if err != nil {
		t.Fatalf("dial blackhole peer: %v", err)
	}
	t.Cleanup(func() { closeWebSocket(t, blackWS) })
	blackClient, err := transport.Intermediate.Handshake(websocket.NetConn(blackCtx, blackWS, websocket.MessageBinary))
	if err != nil {
		t.Fatalf("blackhole transport handshake: %v", err)
	}
	first := clientFrame(t, key, 42, int64(1)<<32, &tg.HelpGetConfigRequest{})
	if err := blackClient.Send(ctx, &bin.Buffer{Buf: slices.Clone(first)}); err != nil {
		t.Fatalf("send blackhole request: %v", err)
	}
	select {
	case <-blackholeSelected:
	case <-ctx.Done():
		t.Fatal("blackhole request did not authenticate")
	}

	flood := make([][]byte, 15)
	for i := range flood {
		flood[i] = clientFrame(t, key, 42, int64(i+2)<<32, &tg.HelpGetConfigRequest{})
	}
	floodDone := make(chan struct{})
	go func() {
		defer close(floodDone)
		for _, frame := range flood {
			if err := blackClient.Send(blackCtx, &bin.Buffer{Buf: slices.Clone(frame)}); err != nil {
				return
			}
		}
	}()

	healthy, err := dialWebSocket(ctx, ln.Addr().String())
	if err != nil {
		t.Fatalf("dial healthy peer: %v", err)
	}
	t.Cleanup(func() { closeWebSocket(t, healthy) })
	healthyClient, err := transport.Intermediate.Handshake(websocket.NetConn(ctx, healthy, websocket.MessageBinary))
	if err != nil {
		t.Fatalf("healthy transport handshake: %v", err)
	}
	for i := range 4 {
		frame := clientFrame(t, key, 84, int64(i+1)<<32, &tg.HelpGetConfigRequest{})
		if err := healthyClient.Send(ctx, &bin.Buffer{Buf: slices.Clone(frame)}); err != nil {
			t.Fatalf("send healthy request %d: %v", i, err)
		}
		receiveBoolResult(t, ctx, healthyClient, key)
	}

	writeStarted := time.Now()
	select {
	case err := <-writeFailed:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("blackhole write failed without the write bound: %v", err)
		}
		if elapsed := time.Since(writeStarted); elapsed > time.Second {
			t.Fatalf("blackhole write took %s to hit the bound", elapsed)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blackhole peer did not hit the write bound")
	}
	cancelBlack()
	select {
	case <-floodDone:
	case <-ctx.Done():
		t.Fatal("blackhole flood did not stop")
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

	if _, _, err := ws.Read(ctx); err == nil {
		t.Fatal("oversized WebSocket message was accepted")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("oversized WebSocket message did not close the connection before the test deadline")
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

func receiveBoolResult(t *testing.T, ctx context.Context, client transport.Conn, key crypto.AuthKey) {
	t.Helper()
	cipher := crypto.NewClientCipher(crypto.DefaultRand())
	for {
		var in bin.Buffer
		if err := client.Recv(ctx, &in); err != nil {
			t.Fatalf("recv response: %v", err)
		}
		message, err := cipher.DecryptFromBuffer(key, &in)
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
		return
	}
}

func dialWebSocket(ctx context.Context, addr string) (*websocket.Conn, error) {
	return dialWebSocketOrigin(ctx, addr, "")
}

func dialWebSocketOrigin(ctx context.Context, addr, origin string) (*websocket.Conn, error) {
	var headers http.Header
	if origin != "" {
		headers = http.Header{"Origin": []string{origin}}
	}
	ws, resp, err := websocket.Dial(ctx, "ws://"+addr+"/apiws", &websocket.DialOptions{
		HTTPHeader:   headers,
		Subprotocols: []string{"binary"},
	})
	if resp != nil && resp.Body != nil {
		if closeErr := resp.Body.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}
	return ws, err
}

func rawWebSocketHandshake() []byte {
	return []byte("GET /apiws HTTP/1.1\r\n" +
		"Host: localhost\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n")
}

func readRawWebSocketHandshake(conn net.Conn) error {
	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if line == "\r\n" {
			return nil
		}
	}
}

type webSocketPreludeListener struct {
	net.Listener

	header          []byte
	accepted        chan struct{}
	skipFirstHeader bool
	acceptCount     int
}

type timestampWebSocketListener struct {
	net.Listener

	accepted chan time.Time
}

type webSocketAcceptLogSink struct {
	info     chan struct{}
	warn     chan struct{}
	infoOnce sync.Once
	warnOnce sync.Once
}

func (s *webSocketAcceptLogSink) Enabled(context.Context, slog.Level) bool { return true }
func (s *webSocketAcceptLogSink) WithAttrs([]slog.Attr) slog.Handler       { return s }
func (s *webSocketAcceptLogSink) WithGroup(string) slog.Handler            { return s }

func (s *webSocketAcceptLogSink) Handle(_ context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		s.warnOnce.Do(func() { close(s.warn) })
	} else if r.Level >= slog.LevelInfo {
		s.infoOnce.Do(func() { close(s.info) })
	}
	return nil
}

func (l *timestampWebSocketListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	acceptedAt := time.Now()
	select {
	case l.accepted <- acceptedAt:
	default:
	}
	return conn, nil
}

type websocketAuthKeyStore struct {
	key crypto.AuthKey
}

func (s *websocketAuthKeyStore) Save(context.Context, crypto.AuthKey) error { return nil }
func (s *websocketAuthKeyStore) Touch(context.Context, [8]byte) error       { return nil }
func (s *websocketAuthKeyStore) Get(context.Context, [8]byte) (crypto.AuthKey, int64, bool, bool, error) {
	return s.key, 7, false, true, nil
}

type websocketLargeResult struct {
	payload []byte
}

func (r websocketLargeResult) Encode(b *bin.Buffer) error {
	b.Put(r.payload)
	return nil
}

func (l *webSocketPreludeListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if l.accepted != nil {
		select {
		case l.accepted <- struct{}{}:
		default:
		}
	}
	l.acceptCount++
	header := l.header
	if l.skipFirstHeader && l.acceptCount == 1 {
		header = nil
	}
	return &webSocketPreludeConn{Conn: conn, header: slices.Clone(header)}, nil
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
	splitPacket  bool
	packetPrefix int
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
	if c.splitPacket {
		if c.packetPrefix == 0 {
			if len(p) != 1 && len(p) != 4 {
				return 0, errors.New("abridged length prefix was not one write")
			}
			c.packetPrefix = len(p)
		} else {
			if len(p) == 0 {
				return 0, errors.New("abridged packet body was empty")
			}
			c.splitPacket = false
			c.packetPrefix = 0
		}
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
