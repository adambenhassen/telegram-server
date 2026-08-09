// Package mtproto implements a minimal MTProto server loop built directly on
// gotd's exported packages (transport, exchange, crypto, proto, mt). It owns the
// accept loop, key exchange, session bookkeeping and message dispatch so the
// application no longer depends on gotd's internal tgtest server.
package mtproto

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"slices"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mtproto"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/tdsync"
	"github.com/gotd/td/transport"
)

const (
	defaultReadTimeout  = 30 * time.Second
	defaultWriteTimeout = 30 * time.Second
	// defaultHandshakeTimeout bounds how long an accepted socket may take to
	// declare its transport. A client that sends nothing is closed after it,
	// which is what keeps a silent connection from costing the server a slot
	// for as long as the peer cares to hold it open.
	defaultHandshakeTimeout = 30 * time.Second
	// touchInterval throttles per-connection last-seen updates so an active
	// session writes its activity time at most once per interval, not per frame.
	touchInterval = 60 * time.Second
)

// Server is an MTProto server: it accepts transport connections, performs key
// exchange for new clients, and dispatches decrypted RPC requests to a Handler.
type Server struct {
	dcID     int
	key      exchange.PrivateKey
	keys     AuthKeyStore
	handler  Handler
	registry *SessionRegistry

	cipher crypto.Cipher
	clock  clock.Clock
	msgID  mtproto.MessageIDSource

	readTimeout  time.Duration
	writeTimeout time.Duration
	// handshakeTimeout bounds transport negotiation on a freshly accepted
	// socket, before any frame exists to apply readTimeout to.
	handshakeTimeout time.Duration

	// proxyV2 is the balancer allowlist when client addresses come from PROXY
	// protocol v2 headers, and nil when they come from the socket itself.
	// Written once before Serve and only read after, so the accept path needs
	// no synchronisation to see it.
	proxyV2 *proxyV2Source
	// negotiationLog thins the per-connection negotiation failure line, which
	// anyone who can reach the port can provoke.
	negotiationLog logSampler

	// onStatusChange fires when a user's connection count transitions between
	// zero and non-zero. Called after the registry has been updated, so a
	// callback that reads the registry does not race the bind. userID is the
	// user whose status changed; online is true when binding, false when the
	// last connection dropped.
	onStatusChange func(ctx context.Context, userID int64, online bool)

	log *slog.Logger
}

// OnStatusChange sets a callback that fires when a user's connection count
// transitions between zero and non-zero.
func (s *Server) OnStatusChange(fn func(ctx context.Context, userID int64, online bool)) {
	s.onStatusChange = fn
}

// TrustProxyV2Headers makes the server take each client address from a PROXY
// protocol v2 header instead of the socket it arrived on, honoured only from a
// source in allow. Call it before Serve.
//
// This is the mode for running behind an L4 load balancer, where every socket's
// peer address is the balancer's and socket keying puts every client on earth in
// one bucket. See proxyV2Source for what the allowlist is load-bearing for.
func (s *Server) TrustProxyV2Headers(allow []netip.Prefix) {
	// Cloned: the allowlist is the trust decision, and it must not change under
	// the server because a caller reused the slice it passed in.
	s.proxyV2 = &proxyV2Source{allow: slices.Clone(allow)}
}

// New creates a Server that answers on dcID using key for the handshake, keys to
// persist auth keys, and handler for RPC requests.
func New(key exchange.PrivateKey, dcID int, keys AuthKeyStore, handler Handler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c := clock.System
	return &Server{
		dcID:             dcID,
		key:              key,
		keys:             keys,
		handler:          handler,
		registry:         NewSessionRegistry(),
		cipher:           crypto.NewServerCipher(crypto.DefaultRand()),
		clock:            c,
		msgID:            proto.NewMessageIDGen(c.Now),
		readTimeout:      defaultReadTimeout,
		writeTimeout:     defaultWriteTimeout,
		handshakeTimeout: defaultHandshakeTimeout,
		log:              log,
	}
}

// Registry returns the connected-session registry the server populates as auth
// keys resolve to users. The update-delivery listener reads it to push updates.
func (s *Server) Registry() *SessionRegistry {
	return s.registry
}

// Key returns the public key clients use to reach this server.
func (s *Server) Key() exchange.PublicKey {
	return s.key.Public()
}

// Serve accepts connections on l until ctx is cancelled or l is closed.
//
// Serving belongs to every client at once, so nothing a single connection does
// may end it: the accept loop only takes sockets off the listener, and
// everything that reads from one — transport negotiation included — happens in
// that connection's own goroutine. Serve returns when the listener is closed or
// fails permanently, and for no other reason.
//
// l is a plain net.Listener because the socket, not a negotiated connection, is
// what the accept path hands over: the peer address is read from it and the
// codec detected from it, both per connection. An address source other than the
// socket — a load balancer's, say — arrives as a listener whose connections
// report it as their RemoteAddr, leaving everything here unchanged.
func (s *Server) Serve(ctx context.Context, l net.Listener) error {
	grp := tdsync.NewCancellableGroup(ctx)
	grp.Go(func(ctx context.Context) error {
		// Unblock the sibling shutdown goroutine when the accept loop exits
		// (e.g. listener closed while ctx is still live).
		defer grp.Cancel()
		var backoff time.Duration
		for {
			sock, err := l.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return nil
				}
				if isTransientAccept(err) {
					// Wait for the condition to pass instead of spinning on it,
					// and keep the listener open: it is still good.
					backoff = nextAcceptBackoff(backoff)
					// A fault still there once the retry interval has saturated
					// is no longer a blip: the process is accepting nobody, and
					// at Info a server that serves no one reads as healthy.
					if backoff >= maxAcceptBackoff {
						s.log.Warn("accept still failing at maximum backoff", "err", err)
					} else {
						s.log.Info("accept failed, retrying", "err", err)
					}
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(backoff):
					}
					continue
				}
				return errors.Join(errors.New("accept"), err)
			}
			backoff = 0
			grp.Go(func(ctx context.Context) error {
				s.serveSocket(ctx, sock)
				return nil
			})
		}
	})
	grp.Go(func(ctx context.Context) error {
		<-ctx.Done()
		if err := l.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			return err
		}
		return nil
	})
	return grp.Wait()
}

// serveSocket negotiates the transport of an accepted socket and then serves
// it. Every failure here is one connection's own — a peer that closed early,
// sent a transport nobody speaks, or sent nothing at all — so it ends that
// connection and is reported no further.
func (s *Server) serveSocket(ctx context.Context, sock net.Conn) {
	// Cancellation has to reach this socket wherever it happens to be, and the
	// reads it blocks in take no context: gotd re-derives a read deadline from
	// one per frame, so a deadline expired at cancel is undone by the very next
	// read. Closing is the one signal that handoff cannot reset, and it covers
	// every wait the connection has — negotiating, or idle between frames — so
	// a stopping process never waits out a timeout on a peer that has gone
	// quiet. Both sides then close: the loser gets ErrClosed, which is a
	// disconnect like any other.
	stop := context.AfterFunc(ctx, func() {
		if err := sock.Close(); err != nil && !isDisconnect(err) {
			s.log.Info("close connection at shutdown", "err", err)
		}
	})
	defer stop()

	conn, addr, err := s.negotiate(sock)
	if err != nil {
		// Sampled: a refused or malformed connection is driven by whoever can
		// reach the port, unauthenticated, so a line each is a log an attacker
		// writes as fast as it can open sockets. The line that does come out
		// says how many it stands for, or the bound would turn a flood into
		// silence.
		if !isDisconnect(err) {
			if dropped, ok := s.negotiationLog.allow(time.Now(), negotiationLogInterval); ok {
				s.log.Info("transport negotiation error", "err", err, "suppressed", dropped)
			}
		}
		return
	}
	if err := s.serveConn(ctx, conn, addr); err != nil && !isDisconnect(err) {
		s.log.Info("connection handler error", "err", err)
	}
}

// isDisconnect reports whether err is an expected client disconnect rather than
// a server fault worth logging as an error.
func isDisconnect(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && (opErr.Op == "read" || opErr.Op == "write") {
		return true
	}
	return false
}

// serveConn reads frames from conn: zero auth key ID starts key exchange, a
// known ID drives the RPC path, and an unknown non-zero ID gets an
// AuthKeyNotFound protocol error.
//
// clientAddr is the peer address of this socket, captured at accept. It travels
// with every request the connection produces and is never re-read from
// anything the client sends.
func (s *Server) serveConn(ctx context.Context, tconn transport.Conn, clientAddr netip.Addr) (rErr error) {
	defer func() {
		if err := tconn.Close(); err != nil && rErr == nil && !isDisconnect(err) {
			rErr = err
		}
	}()

	conn := newConn(tconn, s.cipher, s.msgID, s.clock, s.writeTimeout, s.log)
	b := new(bin.Buffer)
	var lastTouch time.Time
	// registeredUser is the userID this conn is currently registered under
	// (0 = none). bind moves the conn to userID and is the only thing that
	// writes it, so every path that changes or drops the binding — rebind,
	// unbind, renegotiation, loop exit — goes through one definition.
	//
	// Ownership is handed over before the registry buckets change: Conns gives
	// delivery a snapshot and the update batch is built from the database before
	// anything is written, so a delivery for the previous user can already be in
	// flight. setOwner waits for that write to finish and makes every later one
	// fail its ownership check; moving the conn between buckets alone would not
	// stop it. setOwner also clears the push watermark, since the previous
	// owner's pts means nothing in the new owner's space.
	// It reports whether the conn is bound: only a registration refused by the
	// per-user connection cap fails, and unbinding always succeeds.
	var registeredUser int64
	bind := func(userID int64) bool {
		if userID == registeredUser {
			return true
		}
		conn.setOwner(userID)
		if registeredUser != 0 {
			s.registry.Remove(registeredUser, conn)
			if len(s.registry.Conns(registeredUser)) == 0 {
				if s.onStatusChange != nil {
					s.onStatusChange(ctx, registeredUser, false)
				}
			}
			registeredUser = 0
		}
		// Register once the auth key resolves to a bound user (post-login,
		// reconnect or rebind); the conn's session is established so pushes can
		// write.
		if userID != 0 {
			if !s.registry.Add(userID, conn) {
				// The user already holds the maximum number of sockets here.
				// Disowned so nothing is written to it, and reported so the
				// caller closes it rather than serving a socket the user's
				// updates will never reach.
				conn.setOwner(0)
				return false
			}
			if s.onStatusChange != nil {
				s.onStatusChange(ctx, userID, true)
			}
			registeredUser = userID
		}
		return true
	}
	// The registry only holds live sockets, and a delivery already holding this
	// conn in a snapshot writes nothing to it on the way out.
	defer bind(0)
	for {
		if err := s.read(ctx, tconn, b); err != nil {
			return err
		}

		var authKeyID [8]byte
		if err := b.PeekN(authKeyID[:], len(authKeyID)); err != nil {
			return errors.Join(errors.New("peek id"), err)
		}

		if authKeyID == ([8]byte{}) {
			// Key exchange never reaches s.keys.Get, so neither the resync below
			// nor the revoked-key check can run on a socket that sends only
			// handshakes. Left registered, such a socket would keep receiving the
			// user's updates for as long as it kept renegotiating, outliving both
			// a rebind and a logOut that deletes the key, and bounded by nothing.
			// A renegotiating conn holds no established session anyway; it
			// re-registers on its first encrypted frame under the new key.
			//
			// Before runExchange, not after: gotd applies its own 60s
			// DefaultTimeout per handshake read, wider than the frame deadline.
			bind(0)
			if err := s.runExchange(ctx, tconn, b); err != nil {
				return err
			}
			continue
		}

		key, userID, ok, err := s.keys.Get(ctx, authKeyID)
		if err != nil {
			return errors.Join(errors.New("get auth key"), err)
		}
		if !ok {
			// A key that previously resolved to a user and is now gone means the
			// session was revoked (logOut/resetAuthorization). Drop it from the
			// registry and close the socket so a revoked client can no longer
			// receive pushes under its cached key.
			//
			// This stays the guarantee. The tg_evict NOTIFY closes a revoked
			// socket that sends nothing, but it is best-effort — not persisted,
			// and lost while a replica's listener is down — so a socket that
			// missed the signal is still caught here on its next frame, and
			// failing that by the read timeout.
			if registeredUser != 0 {
				// The deferred cleanup deregisters and disowns the conn.
				return nil
			}
			if err := s.sendProtoError(ctx, tconn, codec.CodeAuthKeyNotFound); err != nil {
				return err
			}
			continue
		}

		conn.setKey(key)
		if err := s.rpcHandle(ctx, conn, b, userID, clientAddr); err != nil {
			return err
		}

		// Keep the conn in step with the key's current binding, which s.keys.Get
		// re-reads every frame: an auth.signIn on this key rebinds it to whoever
		// signed in, and the 2FA path unbinds it back to pending. Registering
		// once would leave the socket in the first user's bucket, still
		// receiving that user's updates under a key someone else now holds, with
		// no revocation path to close it.
		//
		// ponytail: resyncs on the conn's next frame, since the rebind lands
		// inside the rpcHandle above, so a socket keeps the previous user until
		// its next frame of any kind or the read timeout, whichever comes first.
		if !bind(userID) {
			// Past the per-user connection cap. The frame just handled was
			// answered normally; the socket then closes on the way out, so a
			// client that keeps opening sockets is refused the new one instead
			// of costing the user a working one.
			s.log.Info("connection refused at user cap", "user_id", userID)
			return nil
		}

		// Advance last-seen only after rpcHandle has decrypted and dispatched the
		// frame, so activity reflects MAC-authenticated traffic. A garbage frame
		// bearing a valid (cleartext) key id fails decryption in rpcHandle and
		// never reaches here, so it cannot spoof DateActive.
		s.touch(ctx, authKeyID, &lastTouch)
	}
}

// touch advances the auth key's last-seen time at most once per touchInterval
// per connection. It is best-effort: a failed update is logged, never fatal, so
// activity tracking cannot break an otherwise-healthy session.
func (s *Server) touch(ctx context.Context, id [8]byte, last *time.Time) {
	now := s.clock.Now()
	if now.Sub(*last) < touchInterval {
		return
	}
	*last = now
	if err := s.keys.Touch(ctx, id); err != nil {
		s.log.Info("touch auth key", "err", err)
	}
}

// runExchange performs key exchange, replaying the already-read first frame, and
// persists the resulting auth key. A ServerExchangeError is reported to the
// client as a protocol error and ends the connection.
func (s *Server) runExchange(ctx context.Context, tconn transport.Conn, first *bin.Buffer) error {
	bc := newBufferedConn(tconn)
	bc.Push(first)

	key, err := s.exchange(ctx, exchangeConn{Conn: bc})
	if err != nil {
		var exErr *exchange.ServerExchangeError
		if errors.As(err, &exErr) {
			// Report the failure to the client and close quietly, matching
			// gotd tgtest: a bad handshake is not a server-side error.
			if sendErr := s.sendProtoError(ctx, bc, exErr.Code); sendErr != nil {
				return sendErr
			}
			return nil
		}
		return errors.Join(errors.New("key exchange"), err)
	}

	if err := s.keys.Save(ctx, key); err != nil {
		return errors.Join(errors.New("save auth key"), err)
	}
	return nil
}

// read resets b and reads one frame from conn under the read timeout.
func (s *Server) read(ctx context.Context, conn transport.Conn, b *bin.Buffer) error {
	b.Reset()
	ctx, cancel := context.WithTimeout(ctx, s.readTimeout)
	defer cancel()
	return conn.Recv(ctx, b)
}

// sendProtoError writes a bare MTProto protocol error (negative int32 code).
func (s *Server) sendProtoError(ctx context.Context, conn transport.Conn, code int32) error {
	var buf bin.Buffer
	buf.PutInt32(-code)

	ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
	defer cancel()
	if err := conn.Send(ctx, &buf); err != nil {
		return errors.Join(errors.New("send proto error"), err)
	}
	return nil
}
