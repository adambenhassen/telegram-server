package e2e_test

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestSendCodeBehindBalancerKeysOnRealClients is the deployment this mode
// exists for: replicas behind an L4 load balancer, where every socket the server
// accepts has the balancer's address on it.
//
// With socket keying that is one bucket for the whole internet — the first
// client to spend the per-IP budget locks out every other login, which is worse
// than having no per-IP limit at all. Here three clients reach one server
// through one balancer, and the test holds only if each is charged against the
// address the balancer reported: the first client is refused a second call while
// the other two, arriving on the same balancer socket, are served.
//
// The call limit is set to one so a shared bucket is visible on the very next
// call rather than after ten.
func TestSendCodeBehindBalancerKeysOnRealClients(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	limits := config.RateLimitsConfig{
		SendCodeIP: store.SendCodeIPLimits{
			Calls:  store.RateLimitConfig{Limit: 1, Window: time.Hour},
			Phones: store.RateLimitConfig{Limit: 20, Window: 24 * time.Hour},
		},
	}
	codes := newMultiCodeSink()
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	tgcfg := api.DefaultConfig(dcID, "127.0.0.1", 0)
	handler := api.New(st, dcID, tgcfg, codes.Logger(), true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), limits)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, codes.Logger())

	ln := mustListen(t, ctx, "127.0.0.1:0")
	// The balancers below all connect from loopback, which is what the
	// allowlist has to name for their headers to be believed.
	proxies := []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}
	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(srvCtx, mtproto.ListenProxyV2(ln, proxies, codes.Logger()))
	}()
	t.Cleanup(func() {
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
	})

	// One balancer per client address. Every one of them dials the server from
	// the same loopback address, so anything keyed on the socket puts all three
	// in one bucket.
	first := startBalancer(t, ctx, ln.Addr().String(), netip.MustParseAddr("203.0.113.10"))
	second := startBalancer(t, ctx, ln.Addr().String(), netip.MustParseAddr("203.0.113.11"))
	// An IPv6 client, which the limiter keys on its /64 rather than its address.
	third := startBalancer(t, ctx, ln.Addr().String(), netip.MustParseAddr("2001:db8::1"))

	sendCode := func(t *testing.T, via *net.TCPAddr, phone string) error {
		t.Helper()
		client := telegram.NewClient(1, "hash", telegram.Options{
			DC:         dcID,
			DCList:     dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: via.Port}}},
			PublicKeys: []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:   dcs.Plain(dcs.PlainOptions{}),
			SessionStorage: &telegram.FileSessionStorage{
				Path: t.TempDir() + "/session.json",
			},
		})
		return client.Run(ctx, func(ctx context.Context) error {
			_, err := client.API().AuthSendCode(ctx, &tg.AuthSendCodeRequest{
				PhoneNumber: phone,
				APIID:       1,
				APIHash:     "hash",
				Settings:    tg.CodeSettings{},
			})
			return err
		})
	}

	if err := sendCode(t, first, "+15551280001"); err != nil {
		t.Fatalf("first client's send code: %v", err)
	}
	// The same client again, over a new connection through the same balancer:
	// its own bucket is spent, and it is the only one that should be.
	err = sendCode(t, first, "+15551280002")
	var rpcErr *tgerr.Error
	if !errors.As(err, &rpcErr) || rpcErr.Code != 420 {
		t.Fatalf("first client's second send code: %v, want 420 FLOOD_WAIT", err)
	}
	if err := sendCode(t, second, "+15551280003"); err != nil {
		t.Fatalf("second client behind the same balancer: %v — its bucket is the first client's", err)
	}
	if err := sendCode(t, third, "+15551280004"); err != nil {
		t.Fatalf("IPv6 client behind the same balancer: %v", err)
	}
}

// startBalancer runs an L4 load balancer in front of target: it accepts a
// connection, opens one to the server, announces client in a PROXY protocol v2
// header, and then copies bytes in both directions without looking at them.
//
// It is deliberately the whole of what a real balancer contributes — one header,
// written once, ahead of a stream it does not otherwise touch.
func startBalancer(t *testing.T, ctx context.Context, target string, client netip.Addr) *net.TCPAddr {
	t.Helper()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("balancer addr type = %T", ln.Addr())
	}

	var wg sync.WaitGroup
	go func() {
		for {
			down, err := ln.Accept()
			if err != nil {
				return
			}
			wg.Go(func() { forward(ctx, down, target, client) })
		}
	}()
	t.Cleanup(func() {
		if err := ln.Close(); err != nil {
			t.Errorf("close balancer: %v", err)
		}
		wg.Wait()
	})
	return addr
}

// forward proxies one accepted connection to the server, prefixed with the
// header naming the client it came from. Errors end this connection and nothing
// else: a balancer that cannot reach one backend is not a test failure, it is
// the connection failing, which the caller sees as its RPC failing.
func forward(ctx context.Context, down net.Conn, target string, client netip.Addr) {
	defer func() { _ = down.Close() }() //nolint:errcheck // the client side sees the close either way
	up, err := (&net.Dialer{}).DialContext(ctx, "tcp", target)
	if err != nil {
		return
	}
	defer func() { _ = up.Close() }() //nolint:errcheck // same
	if _, err := up.Write(proxyV2ClientHeader(client)); err != nil {
		return
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.Copy(up, down) //nolint:errcheck // a copy that stops ends the connection
	}()
	_, _ = io.Copy(down, up) //nolint:errcheck // same
	<-done
}

// proxyV2ClientHeader builds the PROXY protocol v2 header announcing client as
// the source of the connection it precedes.
func proxyV2ClientHeader(client netip.Addr) []byte {
	var h [16]byte
	copy(h[:], []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A})
	h[12] = 0x21 // version 2, PROXY command
	var body []byte
	if client.Is4() {
		h[13] = 0x11 // AF_INET, STREAM
		src, dst := client.As4(), [4]byte{10, 0, 0, 1}
		body = append(append(body, src[:]...), dst[:]...)
		binary.BigEndian.PutUint16(h[14:16], 4+4+2+2)
	} else {
		h[13] = 0x21 // AF_INET6, STREAM
		src, dst := client.As16(), netip.MustParseAddr("2001:db8::ffff").As16()
		body = append(append(body, src[:]...), dst[:]...)
		binary.BigEndian.PutUint16(h[14:16], 16+16+2+2)
	}
	body = binary.BigEndian.AppendUint16(body, 51000) // source port
	body = binary.BigEndian.AppendUint16(body, 2443)  // destination port
	return append(h[:], body...)
}
