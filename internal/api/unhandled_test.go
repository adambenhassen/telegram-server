package api_test

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// attrs flattens a record's attributes into a key/value map for assertion.
func attrs(r slog.Record) map[string]string {
	got := map[string]string{}
	r.Attrs(func(a slog.Attr) bool {
		got[a.Key] = a.Value.String()
		return true
	})
	return got
}

// unhandledConn builds a connection to charge the fallback's per-connection
// budget against. It never writes: the fallback answers by returning the error,
// which the RPC layer above it turns into a reply.
func unhandledConn() *mtproto.Conn {
	return mtproto.NewTestConn(&fakeTransport{}, testKey())
}

// registerDeviceBody is a request body positioned at the constructor id of a
// method this server does not implement.
func registerDeviceBody(t *testing.T) *bin.Buffer {
	t.Helper()
	var buf bin.Buffer
	if err := (&tg.AccountRegisterDeviceRequest{}).Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return &buf
}

// mustRPCError extracts the RPC error a caller would receive, failing when the
// fallback returned something that never reaches a client.
func mustRPCError(t *testing.T, err error) *tgerr.Error {
	t.Helper()
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("err = %v, want an rpc error", err)
	}
	return rpc
}

// TestUnhandledLogsResolvedMethodName covers the one record a client's
// unsupported call leaves behind. The name matters more than the id: a capture
// against a real client is a list of method names, and a log line carrying only
// a constructor id has to be resolved by hand against the schema before it says
// anything.
func TestUnhandledLogsResolvedMethodName(t *testing.T) {
	h := &captureHandler{}
	err := api.UnhandledForTest(slog.New(h), unhandledConn(), registerDeviceBody(t))

	rpc := mustRPCError(t, err)
	if rpc.Code != 400 || rpc.Message != "INPUT_METHOD_INVALID" {
		t.Errorf("returned %d %s, want 400 INPUT_METHOD_INVALID", rpc.Code, rpc.Message)
	}
	if len(h.records) != 1 {
		t.Fatalf("captured %d records, want 1", len(h.records))
	}
	r := h.records[0]
	if r.Level != slog.LevelWarn {
		t.Errorf("level = %v, want %v", r.Level, slog.LevelWarn)
	}
	if r.Message != "method not implemented" {
		t.Errorf("message = %q, want %q", r.Message, "method not implemented")
	}
	want := map[string]string{
		"type_id":    "0xec86017a",
		"method":     "account.registerDevice#ec86017a",
		"error_code": "400",
		"error":      "INPUT_METHOD_INVALID",
	}
	got := attrs(r)
	for k, v := range want {
		if got[k] != v {
			t.Errorf("attr %s = %q, want %q", k, got[k], v)
		}
	}
}

// TestUnhandledLogsUnresolvableID keeps the record useful for a constructor the
// pinned schema has no name for. That is the case the capture cannot afford to
// lose: an id gotd does not know is either a layer mismatch or a method added
// after the pin, and both are findings.
func TestUnhandledLogsUnresolvableID(t *testing.T) {
	var buf bin.Buffer
	buf.PutID(0xdeadbeef)

	h := &captureHandler{}
	if err := api.UnhandledForTest(slog.New(h), unhandledConn(), &buf); err == nil {
		t.Fatal("unknown constructor accepted")
	}
	if len(h.records) != 1 {
		t.Fatalf("captured %d records, want 1", len(h.records))
	}
	got := attrs(h.records[0])
	if got["type_id"] != "0xdeadbeef" {
		t.Errorf("type_id = %q, want %q", got["type_id"], "0xdeadbeef")
	}
	if got["method"] != "unknown" {
		t.Errorf("method = %q, want %q", got["method"], "unknown")
	}
}

// TestUnhandledRejectsUnreadableBody guards the branch where the body is too
// short to carry a constructor id at all: it must still refuse the call rather
// than fall through as a success.
func TestUnhandledRejectsUnreadableBody(t *testing.T) {
	h := &captureHandler{}
	if err := api.UnhandledForTest(slog.New(h), unhandledConn(), &bin.Buffer{}); err == nil {
		t.Fatal("empty body accepted")
	}
	if len(h.records) != 1 {
		t.Fatalf("captured %d records, want 1", len(h.records))
	}
	if h.records[0].Message != "method not implemented: peek id failed" {
		t.Errorf("message = %q", h.records[0].Message)
	}
}

// TestUnhandledBudgetBandsOnOneConnection is the ticket's case end to end
// through the fallback: a connection that sends 300 calls to a method this
// server does not implement is answered as it always was for the first 64,
// told to wait for the next 191, and cut on the 256th. The first band is the
// one that must not move — it is what a real client's cold start spends — and
// the error it gets must stay INPUT_METHOD_INVALID rather than anything a
// client treats as a reason to throw its keys away.
func TestUnhandledBudgetBandsOnOneConnection(t *testing.T) {
	t.Parallel()
	log := slog.New(&captureHandler{})
	conn := unhandledConn()
	body := registerDeviceBody(t)

	for i := 1; i <= 300; i++ {
		err := api.UnhandledForTest(log, conn, body)
		switch {
		case i <= 64:
			rpc := mustRPCError(t, err)
			if rpc.Code != 400 || rpc.Message != "INPUT_METHOD_INVALID" {
				t.Fatalf("call %d: %d %s, want 400 INPUT_METHOD_INVALID", i, rpc.Code, rpc.Message)
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
			var rpc *tgerr.Error
			if errors.As(err, &rpc) {
				t.Fatalf("call %d: %d %s, want a non-RPC error that ends the connection", i, rpc.Code, rpc.Message)
			}
		}
	}
}

// TestUnhandledBudgetSamplesTheLog covers the second cost of the burst. 300
// calls must not be 300 lines, and the one line that is emitted has to say how
// many it stands for or an operator reading it cannot tell a stray call from a
// flood.
func TestUnhandledBudgetSamplesTheLog(t *testing.T) {
	t.Parallel()
	h := &captureHandler{}
	log := slog.New(h)
	conn := unhandledConn()
	body := registerDeviceBody(t)

	for i := range 300 {
		if err := api.UnhandledForTest(log, conn, body); err == nil {
			t.Fatalf("call %d: answered with no error", i+1)
		}
	}

	if len(h.records) != 1 {
		t.Fatalf("captured %d records, want 1", len(h.records))
	}
	if got := attrs(h.records[0])["suppressed"]; got != "0" {
		t.Errorf("suppressed = %q, want %q", got, "0")
	}
}

// TestUnhandledBudgetIsPerConnection pins where the counter lives. A budget
// shared between connections would let one peer's loop refuse everybody else's
// first such call, which is the flood turned into an outage.
func TestUnhandledBudgetIsPerConnection(t *testing.T) {
	t.Parallel()
	log := slog.New(&captureHandler{})
	body := registerDeviceBody(t)

	spent := unhandledConn()
	for i := range 256 {
		if err := api.UnhandledForTest(log, spent, body); err == nil {
			t.Fatalf("call %d: answered with no error", i+1)
		}
	}
	if err := api.UnhandledForTest(log, spent, body); err == nil {
		t.Fatal("spent connection: answered, want the connection ended")
	}

	rpc := mustRPCError(t, api.UnhandledForTest(log, unhandledConn(), body))
	if rpc.Code != 400 || rpc.Message != "INPUT_METHOD_INVALID" {
		t.Errorf("fresh connection: %d %s, want 400 INPUT_METHOD_INVALID", rpc.Code, rpc.Message)
	}
}
