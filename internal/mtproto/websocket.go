package mtproto

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/netip"
	"sync"
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
}

// webSocketAcceptedConn carries the pre-auth slot from the HTTP server's
// accepted socket into the WebSocket handler. Closing the socket releases the
// slot even when the request never upgrades.
type webSocketAcceptedConn struct {
	net.Conn

	slot   *preAuthSlot
	state  sync.Mutex
	closed bool
	addr   netip.Addr
}

func (c *webSocketAcceptedConn) Close() error {
	c.state.Lock()
	if c.closed {
		c.state.Unlock()
		return nil
	}
	c.closed = true
	c.state.Unlock()
	c.slot.clear()
	return c.Conn.Close()
}

// webSocketListener admits sockets before net/http reads their HTTP request.
// The raw accept loop only admits and dispatches sockets; every client-byte
// read, including PROXY-v2 address establishment, happens in a worker before
// that socket is made visible to net/http. This keeps a silent peer from
// holding the goroutine that accepts the next HTTP connection.
type webSocketListener struct {
	net.Listener

	server    *Server
	ready     chan *webSocketAcceptedConn
	acceptErr chan error
	done      chan struct{}
	closeOnce sync.Once

	// pendingMu is a leaf bookkeeping lock. It is never held while calling
	// pre-auth, socket or server methods, so it cannot order against any other
	// lock in the connection path.
	pendingMu sync.Mutex
	pending   map[*webSocketAcceptedConn]struct{}
}

func newWebSocketListener(listener net.Listener, server *Server) *webSocketListener {
	return &webSocketListener{
		Listener:  listener,
		server:    server,
		ready:     make(chan *webSocketAcceptedConn, 1),
		acceptErr: make(chan error, 1),
		done:      make(chan struct{}),
		pending:   make(map[*webSocketAcceptedConn]struct{}),
	}
}

func (l *webSocketListener) start() {
	go l.acceptLoop()
}

func (l *webSocketListener) Accept() (net.Conn, error) {
	select {
	case err := <-l.acceptErr:
		return nil, err
	default:
	}
	select {
	case accepted := <-l.ready:
		l.untrack(accepted)
		return accepted, nil
	case err := <-l.acceptErr:
		return nil, err
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *webSocketListener) acceptLoop() {
	var backoff time.Duration
	for {
		sock, err := l.Listener.Accept()
		if err != nil {
			select {
			case <-l.done:
				return
			default:
			}
			if isTransientAccept(err) {
				backoff = nextAcceptBackoff(backoff)
				select {
				case <-l.done:
					return
				case <-time.After(backoff):
				}
				continue
			}
			select {
			case l.acceptErr <- errors.Join(errors.New("accept WebSocket connection"), err):
			default:
			}
			return
		}
		backoff = 0

		slot, ok := l.server.preAuth.admit()
		if !ok {
			l.server.dropRefused(sock)
			if dropped, allow := l.server.globalCapLog.allow(time.Now(), preAuthLogInterval); allow {
				l.server.log.Info("WebSocket connection refused at the pre-auth cap",
					"cap", l.server.preAuth.limits.MaxConns, "suppressed", dropped)
			}
			continue
		}
		accepted := &webSocketAcceptedConn{Conn: sock, slot: slot}
		l.armLifetime(accepted)
		if !l.track(accepted) {
			l.closeAccepted(accepted)
			return
		}
		go l.prepare(accepted)
	}
}

func (l *webSocketListener) armLifetime(accepted *webSocketAcceptedConn) {
	accepted.slot.armLifetime(func() {
		if err := accepted.Close(); err != nil && !isDisconnect(err) {
			l.server.log.Info("close WebSocket connection at the pre-auth ceiling", "err", err)
		}
		if dropped, ok := l.server.ceilingLog.allow(time.Now(), preAuthLogInterval); ok {
			l.server.log.Info("WebSocket connection closed at the pre-auth lifetime ceiling",
				"lifetime", l.server.preAuth.limits.Lifetime, "suppressed", dropped)
		}
	})
}

func (l *webSocketListener) prepare(accepted *webSocketAcceptedConn) {
	addr, err := l.server.clientAddr(accepted)
	if err != nil {
		l.server.logNegotiation(err)
		l.closeAccepted(accepted)
		l.untrack(accepted)
		return
	}
	accepted.state.Lock()
	if accepted.closed {
		accepted.state.Unlock()
		l.untrack(accepted)
		return
	}
	if !accepted.slot.keyAddr(addr) {
		accepted.state.Unlock()
		l.server.dropRefused(accepted)
		accepted.slot.clear()
		l.untrack(accepted)
		if dropped, ok := l.server.netCapLog.allow(time.Now(), preAuthLogInterval); ok {
			l.server.log.Info("WebSocket connection refused at the per-network pre-auth cap",
				"client_addr", addr, "cap", l.server.preAuth.limits.MaxConnsPerNet, "suppressed", dropped)
		}
		return
	}
	accepted.addr = addr
	delivered := false
	select {
	case l.ready <- accepted:
		delivered = true
	case <-l.done:
	}
	accepted.state.Unlock()
	if !delivered {
		l.closeAccepted(accepted)
		l.untrack(accepted)
	}
}

func (l *webSocketListener) closeAccepted(accepted *webSocketAcceptedConn) {
	if err := accepted.Close(); err != nil && !isDisconnect(err) {
		l.server.log.Info("close WebSocket connection", "err", err)
	}
}

func (l *webSocketListener) track(accepted *webSocketAcceptedConn) bool {
	l.pendingMu.Lock()
	defer l.pendingMu.Unlock()
	select {
	case <-l.done:
		return false
	default:
	}
	l.pending[accepted] = struct{}{}
	return true
}

func (l *webSocketListener) untrack(accepted *webSocketAcceptedConn) {
	l.pendingMu.Lock()
	delete(l.pending, accepted)
	l.pendingMu.Unlock()
}

func (l *webSocketListener) Close() error {
	var closeErr error
	l.closeOnce.Do(func() {
		close(l.done)
		closeErr = l.Listener.Close()
		l.pendingMu.Lock()
		pending := make([]*webSocketAcceptedConn, 0, len(l.pending))
		for accepted := range l.pending {
			pending = append(pending, accepted)
		}
		l.pending = make(map[*webSocketAcceptedConn]struct{})
		l.pendingMu.Unlock()
		for _, accepted := range pending {
			l.closeAccepted(accepted)
		}
	})
	return closeErr
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

	listener := newWebSocketListener(l, s)
	listener.start()
	defer func() {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			s.log.Info("close WebSocket listener", "err", err)
		}
	}()
	err := server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// webSocketConnContext copies the address established by the listener worker.
// It must not read from conn: net/http invokes ConnContext on its accept path,
// and a PROXY-v2 read there would let one silent peer stall every later accept.
func (s *Server) webSocketConnContext(ctx context.Context, conn net.Conn) context.Context {
	accepted, ok := conn.(*webSocketAcceptedConn)
	if !ok {
		return ctx
	}

	state := &webSocketConnState{
		socket: accepted,
		slot:   accepted.slot,
		addr:   accepted.addr,
	}
	return context.WithValue(ctx, webSocketConnKey{}, state)
}

func (s *Server) handleWebSocket(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	state, ok := r.Context().Value(webSocketConnKey{}).(*webSocketConnState)
	if !ok {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	// Keep the slot tied to the request when the HTTP upgrade fails. The listener
	// worker closes address-establishment failures before they reach net/http.
	defer state.slot.clear()
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
