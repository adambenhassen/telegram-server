package api

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/config"
	"github.com/teagramhq/teagram-server/internal/mtproto"
)

type signUpRejectionCapture struct {
	records []slog.Record
}

func (h *signUpRejectionCapture) Enabled(context.Context, slog.Level) bool { return true }

func (h *signUpRejectionCapture) Handle(_ context.Context, record slog.Record) error {
	h.records = append(h.records, record.Clone())
	return nil
}

func (h *signUpRejectionCapture) WithAttrs([]slog.Attr) slog.Handler { return h }

func (h *signUpRejectionCapture) WithGroup(string) slog.Handler { return h }

func TestHandleSignUpLogsMalformedRequest(t *testing.T) {
	t.Parallel()
	logs := &signUpRejectionCapture{}
	h := &handlers{
		log:              slog.New(logs),
		registrationMode: config.RegistrationOpen,
		now:              func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
	_, err := h.handleSignUp(&mtproto.Request{Ctx: context.Background(), Buf: &bin.Buffer{}})
	if err == nil {
		t.Fatal("malformed request accepted")
	}
	assertSignUpRejectionRecord(t, logs.records, "malformed_request", "open")
}

func TestHandleSignUpLogsClosedMode(t *testing.T) {
	t.Parallel()
	logs := &signUpRejectionCapture{}
	h := &handlers{
		log:              slog.New(logs),
		registrationMode: config.RegistrationClosed,
		now:              func() time.Time { return time.Unix(1_700_000_000, 0) },
	}
	var body bin.Buffer
	if err := (&tg.AuthSignUpRequest{PhoneNumber: "closedhandle"}).Encode(&body); err != nil {
		t.Fatalf("encode sign-up request: %v", err)
	}
	_, err := h.handleSignUp(&mtproto.Request{Ctx: context.Background(), Buf: &body})
	if err == nil {
		t.Fatal("closed registration mode accepted sign-up")
	}
	assertSignUpRejectionRecord(t, logs.records, "registration_mode_unavailable", "closed")
}

func assertSignUpRejectionRecord(t *testing.T, records []slog.Record, reason, mode string) {
	t.Helper()
	if len(records) != 1 {
		t.Fatalf("captured %d records, want 1", len(records))
	}
	record := records[0]
	if record.Message != "auth.signUp rejected" {
		t.Fatalf("message = %q, want auth.signUp rejected", record.Message)
	}
	fields := map[string]string{}
	record.Attrs(func(attr slog.Attr) bool {
		fields[attr.Key] = attr.Value.String()
		return true
	})
	if fields["reason"] != reason {
		t.Errorf("reason = %q, want %q", fields["reason"], reason)
	}
	if fields["registration_mode"] != mode {
		t.Errorf("registration_mode = %q, want %q", fields["registration_mode"], mode)
	}
	if fields["suppressed"] != "0" {
		t.Errorf("suppressed = %q, want 0", fields["suppressed"])
	}
}
