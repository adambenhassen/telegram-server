package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestLoginAfterEarlyClose covers the unauthenticated denial of service: one
// throwaway TCP connection, closed before it sends a byte, must not stop the
// server. A real client logs in afterwards against the same running server.
func TestLoginAfterEarlyClose(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv := bootAcceptServer(t, ctx, 0)

	dead, err := (&net.Dialer{}).DialContext(ctx, "tcp", srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := dead.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	srv.login(t, ctx, "+15551239100")
}

// TestLoginWhileConnectionSilent covers the other shape of the same hole: a
// connection that opens and sends nothing must neither delay another client
// from connecting nor be held by the server forever.
func TestLoginWhileConnectionSilent(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Short enough to observe the silent socket being dropped inside the test's
	// budget; the production bound is the same mechanism, further out.
	const handshake = 2 * time.Second
	srv := bootAcceptServer(t, ctx, handshake)

	silent, err := (&net.Dialer{}).DialContext(ctx, "tcp", srv.addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer silent.Close() //nolint:errcheck // the server closes it; this is a safety net.

	// The silent connection is still open and has sent nothing.
	srv.login(t, ctx, "+15551239101")

	// And it is dropped once the handshake bound elapses, rather than costing
	// the server a connection for as long as the peer keeps it open.
	if err := silent.SetReadDeadline(time.Now().Add(3 * handshake)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	var buf [1]byte
	_, err = silent.Read(buf[:])
	if err == nil {
		t.Fatal("read from silent connection succeeded, want close")
	}
	var nerr net.Error
	if errors.As(err, &nerr) && nerr.Timeout() {
		t.Fatalf("silent connection still open after %v", 3*handshake)
	}
}

// acceptServer is a running server plus what a client needs to reach it.
type acceptServer struct {
	addr  string
	port  int
	dcID  int
	key   *rsa.PrivateKey
	codes *codeSink
}

// bootAcceptServer starts a server on a loopback port and stops it with the
// test. A non-zero handshake bounds transport negotiation more tightly than
// production, so a silent socket can be observed being closed.
func bootAcceptServer(t *testing.T, ctx context.Context, handshake time.Duration) *acceptServer {
	t.Helper()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatalf("server key: %v", err)
	}

	st, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatalf("store open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	codes := newCodeSink()
	const dcID = 2
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	// The code sink scrapes the issued code out of the log, so the gated line
	// needs to be on.
	handler := api.New(st, dcID, api.DefaultConfig(dcID, "127.0.0.1", 0), codes.Logger(), true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{}, config.RegistrationClosed)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, codes.Logger())
	if handshake > 0 {
		server.SetHandshakeTimeout(handshake)
	}

	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, ln) }()
	t.Cleanup(func() {
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
	})

	return &acceptServer{addr: addr.String(), port: addr.Port, dcID: dcID, key: key, codes: codes}
}

// login runs a full client login against the server, failing the test if any
// step of it does not complete.
func (s *acceptServer) login(t *testing.T, ctx context.Context, phone string) {
	t.Helper()

	client := telegram.NewClient(1, "hash", telegram.Options{
		DC:         s.dcID,
		DCList:     dcs.List{Options: []tg.DCOption{{ID: s.dcID, IPAddress: "127.0.0.1", Port: s.port}}},
		PublicKeys: []telegram.PublicKey{{RSA: &s.key.PublicKey}},
		Resolver:   dcs.Plain(dcs.PlainOptions{}),
	})

	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return s.codes.wait(ctx)
			})),
		auth.SendCodeOptions{},
	)
	if err := client.Run(ctx, func(ctx context.Context) error {
		return client.Auth().IfNecessary(ctx, flow)
	}); err != nil {
		t.Fatalf("login flow: %v", err)
	}
}
