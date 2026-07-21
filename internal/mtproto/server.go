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

	log *slog.Logger
}

// New creates a Server that answers on dcID using key for the handshake, keys to
// persist auth keys, and handler for RPC requests.
func New(key exchange.PrivateKey, dcID int, keys AuthKeyStore, handler Handler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	c := clock.System
	return &Server{
		dcID:         dcID,
		key:          key,
		keys:         keys,
		handler:      handler,
		registry:     NewSessionRegistry(),
		cipher:       crypto.NewServerCipher(crypto.DefaultRand()),
		clock:        c,
		msgID:        proto.NewMessageIDGen(c.Now),
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
		log:          log,
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
func (s *Server) Serve(ctx context.Context, l transport.Listener) error {
	grp := tdsync.NewCancellableGroup(ctx)
	grp.Go(func(ctx context.Context) error {
		// Unblock the sibling shutdown goroutine when the accept loop exits
		// (e.g. listener closed while ctx is still live).
		defer grp.Cancel()
		for {
			conn, err := l.Accept()
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return nil
				}
				return errors.Join(errors.New("accept"), err)
			}
			grp.Go(func(ctx context.Context) error {
				if err := s.serveConn(ctx, conn); err != nil && !isDisconnect(err) {
					s.log.Info("connection handler error", "err", err)
				}
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
func (s *Server) serveConn(ctx context.Context, tconn transport.Conn) (rErr error) {
	defer func() {
		if err := tconn.Close(); err != nil && rErr == nil && !isDisconnect(err) {
			rErr = err
		}
	}()

	conn := newConn(tconn, s.cipher, s.msgID, s.clock, s.writeTimeout, s.log)
	b := new(bin.Buffer)
	var lastTouch time.Time
	// registeredUser is the userID this conn was registered under (0 = not yet).
	// It deregisters on loop exit so the registry only holds live sockets.
	var registeredUser int64
	defer func() {
		if registeredUser != 0 {
			s.registry.Remove(registeredUser, conn)
		}
	}()
	for {
		if err := s.read(ctx, tconn, b); err != nil {
			return err
		}

		var authKeyID [8]byte
		if err := b.PeekN(authKeyID[:], len(authKeyID)); err != nil {
			return errors.Join(errors.New("peek id"), err)
		}

		if authKeyID == ([8]byte{}) {
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
			// ponytail: only closes on the client's next frame; a silent socket
			// keeps its cached key until it sends again. Full revocation needs a
			// cross-replica evict signal — deferred to a sessions-hardening pass.
			if registeredUser != 0 {
				s.registry.Remove(registeredUser, conn)
				registeredUser = 0
				return nil
			}
			if err := s.sendProtoError(ctx, tconn, codec.CodeAuthKeyNotFound); err != nil {
				return err
			}
			continue
		}

		conn.setKey(key)
		if err := s.rpcHandle(ctx, conn, b, userID); err != nil {
			return err
		}

		// Register once the auth key resolves to a bound user (post-login or
		// reconnect); the conn's session is now established so pushes can write.
		if userID != 0 && registeredUser == 0 {
			registeredUser = userID
			s.registry.Add(userID, conn)
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
