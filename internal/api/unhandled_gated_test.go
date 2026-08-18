package api_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// provisionalBody drives the gated fallback through a body positioned at the
// constructor id of a method this server does not implement.
func provisionalBody(t *testing.T) *mtproto.Request {
	t.Helper()
	return &mtproto.Request{
		Ctx:         context.Background(),
		UserID:      1,
		Provisional: true,
		Buf:         registerDeviceBody(t),
	}
}

// gatedConn builds a connection for the gated fallback tests. Its transport
// records every write, so a test can assert whether the provisional answer
// actually reached the wire.
func gatedConn() (*mtproto.Conn, *fakeTransport) {
	ft := &fakeTransport{}
	return mtproto.NewTestConn(ft, testKey()), ft
}

// TestGatedFallbackInsideBudgetKeepsProvisionalAnswer pins the answer a
// provisional session gets for an unimplemented call inside the budget: the
// in-band answer must stay exactly what the client needs to re-authenticate
// rather than give up, so the bound must not converge the two paths onto one
// error string.
func TestGatedFallbackInsideBudgetKeepsProvisionalAnswer(t *testing.T) {
	t.Parallel()
	log := slog.New(&captureHandler{})
	conn, ft := gatedConn()
	for i := 1; i <= 64; i++ {
		if err := api.GatedUnhandledForTest(log, conn, provisionalBody(t)); err != nil {
			t.Fatalf("call %d: %v, want the answer written to the wire", i, err)
		}
	}
	if !ft.wasSent() {
		t.Fatal("the provisional answer never reached the wire")
	}
}

// TestGatedFallbackCrossesAllThreeThresholds is the ticket's case: a
// provisional connection driving unimplemented calls is bounded identically to
// a non-provisional one. Inside the budget it still answers as a provisional
// answer, past the budget it is on the non-provisional back-off schedule, and
// the ceiling call ends the connection.
func TestGatedFallbackCrossesAllThreeThresholds(t *testing.T) {
	t.Parallel()
	log := slog.New(&captureHandler{})
	conn, _ := gatedConn()

	for i := 1; i <= 300; i++ {
		err := api.GatedUnhandledForTest(log, conn, provisionalBody(t))
		switch {
		case i <= 64:
			if err != nil {
				t.Fatalf("call %d: %v, want the answer written to the wire", i, err)
			}
		case i < 256:
			rpc := mustRPCError(t, err)
			if rpc.Code != 420 || rpc.Message != "FLOOD_WAIT_30" {
				t.Fatalf("call %d: %d %s, want 420 FLOOD_WAIT_30", i, rpc.Code, rpc.Message)
			}
		default:
			if err == nil {
				t.Fatalf("call %d: answered, want the connection ended", i)
			}
			if asRPC(err) {
				t.Fatalf("call %d: answered with an RPC error, want a non-RPC error that ends the connection", i)
			}
		}
	}
}

// TestGatedFallbackSharesTheBudgetWithNonProvisional proves the budget is one
// counter per connection, not two: a connection that alternates provisional
// and non-provisional calls spends the same allowance, so it cannot get a
// fresh one by switching which of the two paths it drives.
func TestGatedFallbackSharesTheBudgetWithNonProvisional(t *testing.T) {
	t.Parallel()
	log := slog.New(&captureHandler{})
	conn, _ := gatedConn()
	prov := provisionalBody(t)
	plain := registerDeviceBody(t)

	for i := 1; i <= 300; i++ {
		var err error
		if i%2 == 1 {
			err = api.GatedUnhandledForTest(log, conn, prov)
		} else {
			err = api.UnhandledForTest(log, conn, plain)
		}
		switch {
		case i <= 64:
			if i%2 == 1 {
				if err != nil {
					t.Fatalf("call %d: %v, want the answer written to the wire", i, err)
				}
			} else {
				rpc := mustRPCError(t, err)
				if rpc.Code != 400 || rpc.Message != "INPUT_METHOD_INVALID" {
					t.Fatalf("call %d: %d %s, want 400 INPUT_METHOD_INVALID", i, rpc.Code, rpc.Message)
				}
			}
		case i < 256:
			rpc := mustRPCError(t, err)
			if rpc.Code != 420 || rpc.Message != "FLOOD_WAIT_30" {
				t.Fatalf("call %d: %d %s, want 420 FLOOD_WAIT_30", i, rpc.Code, rpc.Message)
			}
		default:
			if err == nil {
				t.Fatalf("call %d: answered, want the connection ended", i)
			}
			if asRPC(err) {
				t.Fatalf("call %d: answered with an RPC error, want a non-RPC error that ends the connection", i)
			}
		}
	}
}

// TestGatedFallbackSamplesTheLog covers the second cost of the burst on the
// provisional path: the not-implemented line is sampled there too, and the
// line that is emitted carries the count of lines it stands for.
func TestGatedFallbackSamplesTheLog(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	cl := &testClock{now: time.Now()}
	conn, _ := gatedConn()
	conn.SetClock(cl)
	// The flush writes to the conn's own logger, not the handlers' — point it
	// at the same capture so the drop's line lands where the test reads it.
	conn.SetLog(log)

	const calls = 300
	for i := range calls {
		if err := api.GatedUnhandledForTest(log, conn, provisionalBody(t)); err == nil && i >= 64 {
			t.Fatalf("call %d: answered with no error", i+1)
		}
	}
	if n := len(h.records); n != 1 {
		t.Fatalf("emitted %d lines inside the interval, want the 1 the sampled path emits", n)
	}
	if got := attrs(h.records[0])["suppressed"]; got != "0" {
		t.Errorf("first line suppressed = %q, want %q", got, "0")
	}

	// The burst ends the connection at the 256th call. The provisional path
	// logs on Answer and Close verdicts but not on FloodWait, so the sampler
	// sees: call 1 emits (suppressed=0), calls 2-64 drop (63), calls 65-255
	// are silent (FloodWait, no log call), call 256 drops (64), calls 257-300
	// drop (44 more, total 108). Advance the clock past the interval and flush
	// the way the serve loop does when it drops the conn: the line it writes
	// must carry the calls it stands for.
	cl.Advance(11 * time.Second)
	conn.FlushUnimplementedLog()

	if len(h.records) != 2 {
		t.Fatalf("captured %d records, want the in-interval line and the flush", len(h.records))
	}
	if got := attrs(h.records[1])["suppressed"]; got != "108" {
		t.Errorf("flush suppressed = %q, want %q", got, "108")
	}
}

// TestGatedFallbackUnauthenticatedIsUnchanged pins the other side of the gate:
// a request with no bound user takes the plain fallback path, answered as an
// unimplemented call and charged against the same budget.
func TestGatedFallbackUnauthenticatedIsUnchanged(t *testing.T) {
	t.Parallel()
	log := slog.New(&captureHandler{})
	conn, _ := gatedConn()
	req := &mtproto.Request{Ctx: context.Background(), Buf: registerDeviceBody(t)}

	rpc := mustRPCError(t, api.GatedUnhandledForTest(log, conn, req))
	if rpc.Code != 400 || rpc.Message != "INPUT_METHOD_INVALID" {
		t.Fatalf("unauthenticated: %d %s, want 400 INPUT_METHOD_INVALID", rpc.Code, rpc.Message)
	}
}

// asRPC reports whether err is an RPC error a client would receive.
func asRPC(err error) bool {
	var rpc *tgerr.Error
	return err != nil && errors.As(err, &rpc)
}
