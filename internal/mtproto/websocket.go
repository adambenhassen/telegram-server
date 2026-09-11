package mtproto

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"time"

	"github.com/coder/websocket"
)

const (
	// websocketPath is the endpoint used by Telegram Web's MTProto transport.
	websocketPath = "/apiws"
	// The MTProto codecs reject payloads larger than 16 MiB. The extra bytes
	// cover the largest transport and obfuscation headers that can share a
	// WebSocket message with one payload.
	maxWebSocketMessageSize = 16<<20 + 64
)

type webSocketConnKey struct{}

type webSocketConnState struct {
	socket *webSocketAcceptedConn
	slot   *preAuthSlot
	addr   netip.Addr
	err    error
}

// webSocketAcceptedConn carries the pre-auth slot from the HTTP server's
// accepted socket into the WebSocket handler. Closing the socket releases the
// slot even when the request never upgrades.
type webSocketAcceptedConn struct {
	net.Conn

	slot *preAuthSlot
}

func (c *webSocketAcceptedConn) Close() error {
	c.slot.clear()
	return c.Conn.Close()
}

// webSocketListener admits sockets before net/http reads their HTTP request.
// The HTTP server still owns request parsing, while this wrapper keeps the
// process-wide pre-auth cap on the same side of the accept boundary as TCP.
type webSocketListener struct {
	net.Listener

	server *Server
}

func (l *webSocketListener) Accept() (net.Conn, error) {
	for {
		sock, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		slot, ok := l.server.preAuth.admit()
		if !ok {
			l.server.dropRefused(sock)
			if dropped, allow := l.server.globalCapLog.allow(time.Now(), preAuthLogInterval); allow {
				l.server.log.Info("WebSocket connection refused at the pre-auth cap",
					"cap", l.server.preAuth.limits.MaxConns, "suppressed", dropped)
			}
			continue
		}
		return &webSocketAcceptedConn{Conn: sock, slot: slot}, nil
	}
}

// ServeWebSocket serves MTProto over WebSocket on l. It is deliberately a
// separate listener: the raw TCP listener keeps its existing transport
// detection, while this endpoint performs the HTTP upgrade and then enters the
// same per-connection MTProto path.
func (s *Server) ServeWebSocket(ctx context.Context, l net.Listener) error {
	server := &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.handleWebSocket(ctx, w, r) }),
		ReadHeaderTimeout: s.handshakeTimeout,
		MaxHeaderBytes:    8192,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ConnContext:       s.webSocketConnContext,
	}
	server.SetKeepAlivesEnabled(false)
	stop := context.AfterFunc(ctx, func() {
		if err := server.Close(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Info("close WebSocket server", "err", err)
		}
	})
	defer stop()

	err := server.Serve(&webSocketListener{Listener: l, server: s})
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// webSocketConnContext establishes the client address before net/http reads
// the request. In PROXY-v2 mode this consumes and validates that header here,
// before any HTTP or MTProto bytes can be interpreted, and the resulting
// address is the one used by every later per-IP bound.
func (s *Server) webSocketConnContext(ctx context.Context, conn net.Conn) context.Context {
	accepted, ok := conn.(*webSocketAcceptedConn)
	if !ok {
		return ctx
	}

	state := &webSocketConnState{socket: accepted, slot: accepted.slot}
	accepted.slot.armLifetime(func() {
		if err := accepted.Close(); err != nil && !isDisconnect(err) {
			s.log.Info("close WebSocket connection at the pre-auth ceiling", "err", err)
		}
		if dropped, ok := s.ceilingLog.allow(time.Now(), preAuthLogInterval); ok {
			s.log.Info("WebSocket connection closed at the pre-auth lifetime ceiling",
				"lifetime", s.preAuth.limits.Lifetime, "suppressed", dropped)
		}
	})

	addr, err := s.clientAddr(accepted)
	if err != nil {
		state.err = err
		return context.WithValue(ctx, webSocketConnKey{}, state)
	}
	if !accepted.slot.keyAddr(addr) {
		state.err = fmt.Errorf("WebSocket connection refused at the per-network pre-auth cap for %s", addr)
		s.dropRefused(accepted)
		if dropped, ok := s.netCapLog.allow(time.Now(), preAuthLogInterval); ok {
			s.log.Info("WebSocket connection refused at the per-network pre-auth cap",
				"client_addr", addr, "cap", s.preAuth.limits.MaxConnsPerNet, "suppressed", dropped)
		}
		return context.WithValue(ctx, webSocketConnKey{}, state)
	}
	state.addr = addr
	return context.WithValue(ctx, webSocketConnKey{}, state)
}

func (s *Server) handleWebSocket(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	state, ok := r.Context().Value(webSocketConnKey{}).(*webSocketConnState)
	if !ok {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	// Keep the slot tied to the request even when address establishment or the
	// HTTP upgrade fails. Neither path enters the hijacked-connection cleanup.
	defer state.slot.clear()
	if state.err != nil {
		s.logNegotiation(state.err)
		return
	}
	if r.URL.Path != websocketPath {
		w.Header().Set("Connection", "close")
		http.NotFound(w, r)
		return
	}

	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols:    []string{"binary"},
		CompressionMode: websocket.CompressionDisabled,
		// MTProto authentication lives in the encrypted stream, not HTTP
		// cookies. Web K is served from web.telegram.org while its WebSocket
		// endpoint is on a different host, so origin verification here would
		// reject the browser before MTProto can authenticate it.
		InsecureSkipVerify: true,
	})
	if err != nil {
		s.logNegotiation(err)
		return
	}
	defer func() {
		if err := ws.CloseNow(); err != nil && !isDisconnect(err) {
			s.log.Info("close WebSocket connection", "err", err)
		}
	}()
	// Hijacking clears net/http's header deadline. Keep the same handshake
	// timeout for transport detection; serveConn replaces it with the normal
	// per-frame deadline after detection succeeds.
	if err := state.socket.SetReadDeadline(time.Now().Add(s.handshakeTimeout)); err != nil {
		s.logNegotiation(errors.Join(errors.New("set WebSocket handshake deadline"), err))
		return
	}
	stream := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	// NetConn disables the library's default message limit for generic tunnels;
	// restore a finite bound after creating that stream wrapper.
	ws.SetReadLimit(maxWebSocketMessageSize)
	stop := context.AfterFunc(ctx, func() {
		if err := ws.CloseNow(); err != nil && !isDisconnect(err) {
			s.log.Info("close WebSocket connection at shutdown", "err", err)
		}
	})
	defer stop()

	conn, err := s.detectCodec(stream)
	if err != nil {
		s.logNegotiation(err)
		return
	}
	if err := state.socket.SetReadDeadline(time.Time{}); err != nil {
		s.logNegotiation(errors.Join(errors.New("clear WebSocket handshake deadline"), err))
		return
	}
	if err := s.serveConn(ctx, conn, state.addr, state.slot); err != nil && !isDisconnect(err) {
		s.log.Info("WebSocket connection handler error", "err", err)
	}
}
