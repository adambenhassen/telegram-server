package mtproto_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/gotd/td/exchange"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// flushLineRecorder captures the one line the serve loop's drop writes when it
// flushes a conn's not-implemented count.
type flushLineRecorder struct {
	records []slog.Record
}

func (r *flushLineRecorder) Enabled(context.Context, slog.Level) bool { return true }
func (r *flushLineRecorder) Handle(_ context.Context, rec slog.Record) error {
	r.records = append(r.records, rec)
	return nil
}
func (r *flushLineRecorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *flushLineRecorder) WithGroup(string) slog.Handler      { return r }

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

// TestServeConnClosesAtUnimplementedCeilingOnProvisionalSession pins the
// ticket's case at the serve-loop level: a provisional session driving
// unimplemented calls is bounded identically to a non-provisional one, so the
// ceiling call ends the connection the same way. The handler here charges the
// shared budget the way the real gated fallback does, so the count the serve
// loop sees is the real one.
func TestServeConnClosesAtUnimplementedCeilingOnProvisionalSession(t *testing.T) {
	t.Parallel()
	key := rebindTestKey()
	ks := &statusKeyStore{key: key, users: []int64{7}}

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

	if err := srv.ServeConn(context.Background(), &statusFrameConn{frames: frames}); !errors.Is(err, errCeiling) {
		t.Fatalf("ServeConn = %v, want %v", err, errCeiling)
	}
	if len(verdicts) != 256 {
		t.Fatalf("dispatched %d calls, want the connection to end on the 256th", len(verdicts))
	}
	if got := verdicts[len(verdicts)-1]; got != mtproto.UnimplementedClose {
		t.Errorf("last verdict = %v, want %v", got, mtproto.UnimplementedClose)
	}
}

// TestServeConnFlushesUnimplementedCountOnDrop proves the serve loop's
// deferred flush carries the sampler's pending count when a burst ends the
// connection. The drop is the only line a 255-call burst ever gets, and it has
// to say how many it stands for — the operator's way of telling one stray call
// from a flood. The handler charges and samples the way the real fallback does,
// so the count the drop owes is the real one; the flush runs on the serve
// loop's drop, before the socket closes and never under any counter lock.
func TestServeConnFlushesUnimplementedCountOnDrop(t *testing.T) {
	t.Parallel()
	key := rebindTestKey()
	ks := &statusKeyStore{key: key, users: []int64{0}}

	recorder := &flushLineRecorder{}
	h := mtproto.HandlerFunc(func(c *mtproto.Conn, _ *mtproto.Request) error {
		if c.ChargeUnimplemented() == mtproto.UnimplementedClose {
			return errCeiling
		}
		c.LogUnimplemented()
		return nil
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, ks, h, slog.New(recorder))

	const frameCount = 300
	frames := make([][]byte, frameCount)
	for i := range frames {
		frames[i] = statusClientFrame(t, key, 42, int64(i+1)<<32, &tg.AccountRegisterDeviceRequest{})
	}

	if err := srv.ServeConn(context.Background(), &statusFrameConn{frames: frames}); !errors.Is(err, errCeiling) {
		t.Fatalf("ServeConn = %v, want %v", err, errCeiling)
	}

	// The drop wrote the conn's not-implemented flush line carrying the count.
	var flush *slog.Record
	for i := range recorder.records {
		if recorder.records[i].Message == "method not implemented" {
			flush = &recorder.records[i]
		}
	}
	if flush == nil {
		t.Fatalf("drop wrote no not-implemented line: %v", recorder.records)
	}
	suppressed := int64(-1)
	flush.Attrs(func(a slog.Attr) bool {
		if a.Key == "suppressed" {
			if v, ok := a.Value.Any().(int64); ok {
				suppressed = v
			}
		}
		return true
	})
	// 254 calls were suppressed behind the drop: the 63 answer-band calls and
	// the 191 flood-band calls the handler sampled, all past the first line.
	// The ceiling call itself returns before sampling in this stub, so it is
	// not in the count the drop owes.
	if suppressed != 254 {
		t.Errorf("drop suppressed = %d, want %d", suppressed, int64(254))
	}
}

// TestServeConnDropWritesNoFlushLineWhenNothingSuppressed pins the other side
// of the drop: a conn that made no unimplemented call, or whose single call the
// sampled path already logged, owes the drop no line. A zero-count flush would
// be a line the conn never owed, and the drop must not write one.
func TestServeConnDropWritesNoFlushLineWhenNothingSuppressed(t *testing.T) {
	t.Parallel()
	key := rebindTestKey()
	ks := &statusKeyStore{key: key, users: []int64{0}}

	recorder := &flushLineRecorder{}
	h := mtproto.HandlerFunc(func(_ *mtproto.Conn, _ *mtproto.Request) error {
		return nil
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, ks, h, slog.New(recorder))

	// The scripted conn runs out of frames and the read returns EOF, which the
	// serve loop treats as a clean disconnect; the drop still runs its defers.
	frame := statusClientFrame(t, key, 42, 1<<32, &tg.AccountRegisterDeviceRequest{})
	if err := srv.ServeConn(context.Background(), &statusFrameConn{frames: [][]byte{frame}}); !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn = %v, want EOF", err)
	}
	for i := range recorder.records {
		if recorder.records[i].Message == "method not implemented" {
			t.Errorf("drop wrote a not-implemented line: %v", recorder.records[i])
		}
	}
}
