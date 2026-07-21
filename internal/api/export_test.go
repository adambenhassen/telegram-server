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
	ValidatePhone = validatePhone
	VerifyToRPC   = verifyToRPC
	NewSentCode   = newSentCode
)

func testHandlers(s *store.Store) *handlers {
	return &handlers{store: s, log: slog.New(slog.DiscardHandler)}
}

// BuildUpdatesForTest exposes buildUpdates for the external api_test package.
func BuildUpdatesForTest(s *store.Store, userID int64, fromPts int) ([]tg.UpdateClass, []tg.UserClass, store.State, error) {
	return testHandlers(s).buildUpdates(context.Background(), userID, fromPts)
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
