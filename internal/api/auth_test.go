package api_test

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestValidatePhone(t *testing.T) {
	if err := api.ValidatePhone("+123456789"); err != nil {
		t.Errorf("valid phone rejected: %v", err)
	}
	if err := api.ValidatePhone("abc"); err == nil {
		t.Error("letters accepted as phone")
	}
	if err := api.ValidatePhone(""); err == nil {
		t.Error("empty phone accepted")
	}
}

// TestSelfRevocationOnlyForTheRequestsOwnKey guards which revocations may defer
// their evict notification past the reply. Only the request's own key qualifies:
// deferring is what lets an update NOTIFY overtake the evict, so widening this
// predicate would silently reintroduce that window for sessions the caller is
// not connected on, while narrowing it would let a self-revocation close the
// socket before its own reply is written.
func TestSelfRevocationOnlyForTheRequestsOwnKey(t *testing.T) {
	own := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	req := &mtproto.Request{AuthKeyID: own}
	ownID := mtproto.AuthKeyIDInt64(own)

	if !api.SelfRevocation(req, ownID) {
		t.Error("revoking the key the request arrived on must count as a self-revocation")
	}
	for _, other := range []int64{ownID + 1, ownID - 1, 0, -ownID} {
		if api.SelfRevocation(req, other) {
			t.Errorf("key id %d counted as the request's own key %d", other, ownID)
		}
	}
}

func TestSignInWrongCodeMapsToRPCError(t *testing.T) {
	err := api.VerifyToRPC(store.ErrCodeInvalid)
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Code != 400 || rpc.Message != "PHONE_CODE_INVALID" {
		t.Errorf("got %v, want PHONE_CODE_INVALID", err)
	}
	if got := api.VerifyToRPC(store.ErrCodeExpired); got.Message != "PHONE_CODE_EXPIRED" {
		t.Errorf("expired mapped to %v", got)
	}
	if api.VerifyToRPC(nil) != nil {
		t.Error("nil should map to nil")
	}
}

// captureHandler records every log record it is handed. Enabled always reports
// true so a record suppressed by the gate cannot be confused with one dropped
// by a level filter — absence is what the flag-off case asserts.
type captureHandler struct{ records []slog.Record }

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func TestLogIssuedCodeGate(t *testing.T) {
	const (
		phone = "+15550001111"
		code  = "12345"
	)
	tests := map[string]struct {
		logLoginCodes bool
		wantRecords   int
	}{
		"flag off": {logLoginCodes: false, wantRecords: 0},
		"flag on":  {logLoginCodes: true, wantRecords: 1},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			h := &captureHandler{}
			api.LogIssuedCodeForTest(slog.New(h), tc.logLoginCodes, phone, code)
			if len(h.records) != tc.wantRecords {
				t.Fatalf("captured %d records, want %d", len(h.records), tc.wantRecords)
			}
			if tc.wantRecords == 0 {
				return
			}
			r := h.records[0]
			if r.Level != slog.LevelInfo {
				t.Errorf("level = %v, want %v", r.Level, slog.LevelInfo)
			}
			if r.Message != "login code issued" {
				t.Errorf("message = %q, want %q", r.Message, "login code issued")
			}
			var got []string
			r.Attrs(func(a slog.Attr) bool {
				got = append(got, a.Key+"="+a.Value.String())
				return true
			})
			want := []string{"phone=" + phone, "code=" + code}
			if !slices.Equal(got, want) {
				t.Errorf("attrs = %v, want %v", got, want)
			}
		})
	}
}

func TestSentCodeShape(t *testing.T) {
	sc := api.NewSentCode("deadbeef")
	if sc.PhoneCodeHash != "deadbeef" {
		t.Fatal("hash not set")
	}
	sms, ok := sc.Type.(*tg.AuthSentCodeTypeSMS)
	if !ok || sms.Length != 5 {
		t.Errorf("type = %#v, want SMS length 5", sc.Type)
	}
}
