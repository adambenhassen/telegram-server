package mtproto_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// errCeiling stands for whatever a handler returns once a connection has spent
// the whole of its unimplemented-method budget. Any non-RPC error ends the
// connection; this test is about what the serve loop then does with it.
var errCeiling = errors.New("unimplemented-method ceiling")

// TestServeConnClosesAtUnimplementedCeiling proves the last of the three bands
// reaches the socket. The budget lives on the conn and the verdict is returned
// to a handler, so nothing here is worth anything unless the connection
// actually ends: the frames past the ceiling must never be read, which is what
// makes the burst cost the peer a fresh TCP connect, transport negotiation and
// key exchange to continue.
func TestServeConnClosesAtUnimplementedCeiling(t *testing.T) {
	t.Parallel()
	key := rebindTestKey()
	ks := &statusKeyStore{key: key, users: []int64{0}}

	var verdicts []mtproto.UnimplementedVerdict
	h := mtproto.HandlerFunc(func(c *mtproto.Conn, _ *mtproto.Request) error {
		v := c.ChargeUnimplemented()
		verdicts = append(verdicts, v)
		if v == mtproto.UnimplementedClose {
			return errCeiling
		}
		return nil
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, ks, h, nil)

	const frameCount = 300
	frames := make([][]byte, frameCount)
	for i := range frames {
		frames[i] = statusClientFrame(t, key, 42, int64(i+1)<<32, &tg.AccountRegisterDeviceRequest{})
	}
	conn := &statusFrameConn{frames: frames}

	if err := srv.ServeConn(context.Background(), conn); !errors.Is(err, errCeiling) {
		t.Fatalf("ServeConn = %v, want %v", err, errCeiling)
	}
	if len(verdicts) != 256 {
		t.Fatalf("dispatched %d calls, want the connection to end on the 256th", len(verdicts))
	}
	if got := verdicts[len(verdicts)-1]; got != mtproto.UnimplementedClose {
		t.Errorf("last verdict = %v, want %v", got, mtproto.UnimplementedClose)
	}
}
