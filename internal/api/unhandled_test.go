package api_test

import (
	"errors"
	"log/slog"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
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

// TestUnhandledLogsResolvedMethodName covers the one record a client's
// unsupported call leaves behind. The name matters more than the id: a capture
// against a real client is a list of method names, and a log line carrying only
// a constructor id has to be resolved by hand against the schema before it says
// anything.
func TestUnhandledLogsResolvedMethodName(t *testing.T) {
	var buf bin.Buffer
	if err := (&tg.AccountRegisterDeviceRequest{}).Encode(&buf); err != nil {
		t.Fatalf("encode: %v", err)
	}

	h := &captureHandler{}
	err := api.UnhandledForTest(slog.New(h), &buf)

	var rpc *tgerr.Error
	if !errors.As(err, &rpc) {
		t.Fatalf("err = %v, want an rpc error", err)
	}
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
	if err := api.UnhandledForTest(slog.New(h), &buf); err == nil {
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
	if err := api.UnhandledForTest(slog.New(h), &bin.Buffer{}); err == nil {
		t.Fatal("empty body accepted")
	}
	if len(h.records) != 1 {
		t.Fatalf("captured %d records, want 1", len(h.records))
	}
	if h.records[0].Message != "method not implemented: peek id failed" {
		t.Errorf("message = %q", h.records[0].Message)
	}
}
