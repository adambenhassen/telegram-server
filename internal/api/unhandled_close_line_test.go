package api_test

import (
	"log/slog"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// wantCloseLine asserts that a connection-ending line is a complete record of
// the call that ended it: it names the method the way the other verdicts'
// lines do, and it reports the error the caller received. On a close the
// caller receives no RPC answer at all — the connection ends — so any in-band
// RPC error named in the line is one the caller never saw.
func wantCloseLine(t *testing.T, got map[string]string) {
	t.Helper()
	if got["type_id"] != "0xec86017a" {
		t.Errorf("type_id = %q, want the call's constructor id %q", got["type_id"], "0xec86017a")
	}
	if got["method"] != "account.registerDevice#ec86017a" {
		t.Errorf("method = %q, want the resolved name %q", got["method"], "account.registerDevice#ec86017a")
	}
	if got["error"] != "unimplemented-method ceiling" {
		t.Errorf("error = %q, want the non-RPC error that ended the connection %q",
			got["error"], "unimplemented-method ceiling")
	}
	if code := got["error_code"]; code != "" {
		t.Errorf("error_code = %q, want none: the caller received no RPC error", code)
	}
}

// driveToClose drives conn through unimplemented calls until the
// connection-ending verdict and returns the lines emitted. The close line is
// exempt from the interval gate, so it is the last line and the one this
// helper asserts on.
func driveToClose(t *testing.T, h *captureHandler, conn *mtproto.Conn,
	call func() error,
) (map[string]string, int) {
	t.Helper()
	for i := 1; ; i++ {
		err := call()
		if err == nil {
			t.Fatalf("call %d: answered with no error", i)
		}
		if i == 256 {
			break
		}
	}
	n := len(h.records)
	if n < 2 {
		t.Fatalf("emitted %d lines, want the sampled line and the connection-ending one", n)
	}
	return attrs(h.records[n-1]), n
}

// TestUnhandledCloseLineNamesTheCall pins the record of the call that ends a
// non-provisional connection. The close verdict is exempt from the interval
// gate: in a burst the sampled path has already spent the interval on the
// first call, so without the exemption the one line describing the call that
// killed the connection would never be written at all. With it, the burst
// ends with exactly one line naming the call, and it says what actually
// happened.
func TestUnhandledCloseLineNamesTheCall(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	conn := unhandledConn()
	body := registerDeviceBody(t)

	got, n := driveToClose(t, h, conn, func() error {
		return api.UnhandledForTest(log, conn, body)
	})
	// The sampled path emits the first call's line and nothing else; the
	// close verdict adds exactly one more.
	if n != 2 {
		t.Fatalf("emitted %d lines, want the first and the connection-ending one", n)
	}
	wantCloseLine(t, got)
}

// TestGatedCloseLineNamesTheCall is the same pin on the provisional path: a
// provisional burst ends the connection the way a non-provisional one does, so
// the line it records must stand for the call the way the non-provisional
// path's line does.
func TestGatedCloseLineNamesTheCall(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	conn, _ := gatedConn()

	got, n := driveToClose(t, h, conn, func() error {
		return api.GatedUnhandledForTest(log, conn, provisionalBody(t))
	})
	if n != 2 {
		t.Fatalf("emitted %d lines, want the first and the connection-ending one", n)
	}
	wantCloseLine(t, got)
}

// TestUnhandledFloodLineReportsTheFloodWait pins the error a past-budget line
// reports. The caller of a past-budget call receives the flood answer, so the
// line for it must say so; naming any other answer is one the caller never
// received. The clock is advanced past the interval so the past-budget call's
// line is the one the sampler emits.
func TestUnhandledFloodLineReportsTheFloodWait(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	cl := &testClock{now: time.Now()}
	conn := unhandledConn()
	conn.SetClock(cl)
	body := registerDeviceBody(t)

	// 64 calls fill the answer band; the first line goes with the first of
	// them. Advance past the interval so the 65th call — the first past-budget
	// call — is the next line the sampler emits.
	for i := 1; i <= 64; i++ {
		if err := api.UnhandledForTest(log, conn, body); err == nil {
			t.Fatalf("call %d: answered with no error", i)
		}
	}
	cl.Advance(11 * time.Second)
	if err := api.UnhandledForTest(log, conn, body); err == nil {
		t.Fatal("call 65: answered with no error")
	}
	if n := len(h.records); n != 2 {
		t.Fatalf("emitted %d lines, want the first and the past-budget one", n)
	}
	got := attrs(h.records[1])
	if got["error"] != "FLOOD_WAIT_30" {
		t.Errorf("error = %q, want the flood answer the caller received %q", got["error"], "FLOOD_WAIT_30")
	}
	if got["error_code"] != "420" {
		t.Errorf("error_code = %q, want %q", got["error_code"], "420")
	}
}

// TestGatedFloodLineReportsTheFloodWait is the same pin on the provisional
// path: past the budget the provisional session is on the non-provisional
// back-off schedule, so its past-budget line reports the same answer.
func TestGatedFloodLineReportsTheFloodWait(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	cl := &testClock{now: time.Now()}
	conn, _ := gatedConn()
	conn.SetClock(cl)

	for i := 1; i <= 64; i++ {
		if err := api.GatedUnhandledForTest(log, conn, provisionalBody(t)); err == nil {
			t.Fatalf("call %d: answered with no error", i)
		}
	}
	cl.Advance(11 * time.Second)
	if err := api.GatedUnhandledForTest(log, conn, provisionalBody(t)); err == nil {
		t.Fatal("call 65: answered with no error")
	}
	if n := len(h.records); n != 2 {
		t.Fatalf("emitted %d lines, want the first and the past-budget one", n)
	}
	got := attrs(h.records[1])
	if got["error"] != "FLOOD_WAIT_30" {
		t.Errorf("error = %q, want the flood answer the caller received %q", got["error"], "FLOOD_WAIT_30")
	}
	if got["error_code"] != "420" {
		t.Errorf("error_code = %q, want %q", got["error_code"], "420")
	}
}

// TestUnhandledCloseLineCarriesThePendingCount pins the accounting of the
// close line: it stands for every call the sampler suppressed behind the
// first line, and the drop's flush then has nothing left to write. Total
// lines over the burst are unchanged from today — the first line and the
// close line, no more — and the count rides on a line that names the call
// instead of on a line that names nothing.
func TestUnhandledCloseLineCarriesThePendingCount(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	cl := &testClock{now: time.Now()}
	conn := unhandledConn()
	conn.SetClock(cl)
	// The flush writes to the conn's own logger, not the handlers' — point it
	// at the same capture so the drop's line lands where the test reads it.
	conn.SetLog(log)
	body := registerDeviceBody(t)

	for i := 1; i <= 256; i++ {
		if err := api.UnhandledForTest(log, conn, body); err == nil {
			t.Fatalf("call %d: answered with no error", i)
		}
	}
	// The first line stands for the first call; the close line stands for the
	// 254 calls the sampler suppressed behind it (calls 2-255; the close call
	// itself is the one the close line describes, so it is not a drop).
	if n := len(h.records); n != 2 {
		t.Fatalf("emitted %d lines, want the first and the connection-ending one", n)
	}
	if got := attrs(h.records[0])["suppressed"]; got != "0" {
		t.Errorf("first line suppressed = %q, want %q", got, "0")
	}
	if got := attrs(h.records[1])["suppressed"]; got != "254" {
		t.Errorf("close line suppressed = %q, want %q", got, "254")
	}

	// The drop writes its owed line the way the serve loop does when it drops
	// the conn. The close line already consumed the pending count, so the
	// flush owes nothing: no third line.
	cl.Advance(11 * time.Second)
	conn.FlushUnimplementedLog()
	if n := len(h.records); n != 2 {
		t.Fatalf("captured %d records after the drop, want the two lines and nothing more", n)
	}
}

// TestGatedCloseLineCarriesThePendingCount is the same pin on the provisional
// path: a provisional burst owes the close line the same count a
// non-provisional one does, and the drop's flush then has nothing left to
// write.
func TestGatedCloseLineCarriesThePendingCount(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	cl := &testClock{now: time.Now()}
	conn, _ := gatedConn()
	conn.SetClock(cl)
	conn.SetLog(log)

	for i := 1; i <= 256; i++ {
		if err := api.GatedUnhandledForTest(log, conn, provisionalBody(t)); err == nil {
			t.Fatalf("call %d: answered with no error", i)
		}
	}
	if n := len(h.records); n != 2 {
		t.Fatalf("emitted %d lines, want the first and the connection-ending one", n)
	}
	if got := attrs(h.records[0])["suppressed"]; got != "0" {
		t.Errorf("first line suppressed = %q, want %q", got, "0")
	}
	if got := attrs(h.records[1])["suppressed"]; got != "254" {
		t.Errorf("close line suppressed = %q, want %q", got, "254")
	}

	cl.Advance(11 * time.Second)
	conn.FlushUnimplementedLog()
	if n := len(h.records); n != 2 {
		t.Fatalf("captured %d records after the drop, want the two lines and nothing more", n)
	}
}
