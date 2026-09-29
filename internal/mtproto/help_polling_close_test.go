package mtproto_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

type helpPollingBudgetConn struct {
	frames [][]byte
	next   int
	sent   int
	closed bool
}

func (c *helpPollingBudgetConn) Recv(_ context.Context, b *bin.Buffer) error {
	if c.next == len(c.frames) {
		return io.EOF
	}
	b.ResetTo(c.frames[c.next])
	c.next++
	return nil
}

func (c *helpPollingBudgetConn) Send(context.Context, *bin.Buffer) error {
	c.sent++
	return nil
}

func (c *helpPollingBudgetConn) Close() error {
	c.closed = true
	return nil
}

// TestServeConnClosesAtHelpPromoBudgetCeiling connects the newly registered
// method's rejected path to the real serve loop: it answers through the shared
// budget, then drops the transport on the 256th call without an RPC reply.
func TestServeConnClosesAtHelpPromoBudgetCeiling(t *testing.T) {
	t.Parallel()
	key := rebindTestKey()
	keys := &statusKeyStore{key: key, users: []int64{0}}
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	handler := api.New(nil, 2, api.DefaultConfig(2, "127.0.0.1", 0), slog.New(slog.DiscardHandler), false, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{}, config.RegistrationClosed)
	server := mtproto.New(exchange.PrivateKey{}, 2, keys, handler, nil)

	const frameCount = 300
	frames := make([][]byte, frameCount)
	for i := range frames {
		var call bin.Encoder = &tg.HelpGetPromoDataRequest{}
		if i%2 == 1 {
			call = &tg.AccountRegisterDeviceRequest{}
		}
		frames[i] = statusClientFrame(t, key, 42, int64(i+1)<<32, call)
	}
	conn := &helpPollingBudgetConn{frames: frames}
	serveErr := server.ServeConn(context.Background(), conn)
	if serveErr == nil || errors.Is(serveErr, io.EOF) {
		t.Fatalf("ServeConn = %v, want the non-RPC rejection ceiling", serveErr)
	}
	if conn.next != 256 {
		t.Fatalf("server read %d calls, want close on call 256", conn.next)
	}
	if conn.sent != 256 {
		t.Fatalf("server sent %d frames before close, want the session-created frame and 255 RPC replies", conn.sent)
	}
	if !conn.closed {
		t.Fatal("server returned at the rejection ceiling without closing the transport")
	}
}
