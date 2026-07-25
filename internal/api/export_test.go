package api

import (
	"context"
	"log/slog"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// Test-only aliases exposing unexported helpers to the external api_test package.
var (
	ValidatePhone  = validatePhone
	VerifyToRPC    = verifyToRPC
	NewSentCode    = newSentCode
	SelfRevocation = selfRevocation
)

// LogIssuedCodeForTest drives the gated login-code log line for the external
// api_test package, without needing a store or a database.
func LogIssuedCodeForTest(log *slog.Logger, logLoginCodes bool, phone, code string) {
	h := &handlers{log: log, logLoginCodes: logLoginCodes}
	h.logIssuedCode(phone, code)
}

func testHandlers(s *store.Store) *handlers {
	return &handlers{store: s, log: slog.New(slog.DiscardHandler)}
}

// BuildUpdatesForTest exposes buildUpdates for the external api_test package.
func BuildUpdatesForTest(s *store.Store, userID int64, fromPts int) ([]tg.UpdateClass, []tg.UserClass, store.State, error) {
	ups, users, state, _, err := testHandlers(s).buildUpdates(context.Background(), userID, fromPts)
	return ups, users, state, err
}

// GetStateForTest exposes handleGetState for the external api_test package.
func GetStateForTest(s *store.Store, userID int64) (bin.Encoder, error) {
	return testHandlers(s).handleGetState(&mtproto.Request{Ctx: context.Background(), UserID: userID})
}

// PeerUserID exposes the peer-resolution guard for the external api_test package.
var PeerUserID = peerUserID

// SendMessageForTest encodes req and invokes handleSendMessage for the caller.
func SendMessageForTest(s *store.Store, userID int64, req *tg.MessagesSendMessageRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSendMessage(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}
