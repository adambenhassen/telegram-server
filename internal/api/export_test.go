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
	b, err := testHandlers(s).buildUpdates(context.Background(), userID, fromPts)
	return b.ups, b.users, b.state, err
}

// GetStateForTest exposes handleGetState for the external api_test package.
func GetStateForTest(s *store.Store, userID int64) (bin.Encoder, error) {
	return testHandlers(s).handleGetState(&mtproto.Request{Ctx: context.Background(), UserID: userID})
}

// PeerUserID exposes the peer-resolution guard for the external api_test package.
var PeerUserID = peerUserID

// Peer-resolution and wire-mapping helpers, exposed for the external api_test
// package. All four are pure and need no store.
var (
	InputPeer   = inputPeer
	InputUserID = inputUserID
	PeerToTL    = peerToTL
	MessageToTL = messageToTL
	ChatToTL    = chatToTL
)

// BuildUpdatesChatsForTest exposes the user and chat lists a batch carries
// alongside its updates, which BuildUpdatesForTest partly omits.
func BuildUpdatesChatsForTest(s *store.Store, userID int64, fromPts int) ([]tg.UpdateClass, []tg.UserClass, []tg.ChatClass, error) {
	b, err := testHandlers(s).buildUpdates(context.Background(), userID, fromPts)
	return b.ups, b.users, b.chats, err
}

// LoadChatsForTest exposes loadChats for the external api_test package.
func LoadChatsForTest(s *store.Store, ids []int64, viewerID int64) ([]tg.ChatClass, error) {
	set := make(map[int64]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return testHandlers(s).loadChats(context.Background(), set, viewerID)
}

// GetHistoryForTest encodes req and invokes handleGetHistory for the caller.
func GetHistoryForTest(s *store.Store, userID int64, req *tg.MessagesGetHistoryRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetHistory(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// GetDialogsForTest invokes handleGetDialogs for the caller.
func GetDialogsForTest(s *store.Store, userID int64) (bin.Encoder, error) {
	var buf bin.Buffer
	req := &tg.MessagesGetDialogsRequest{OffsetPeer: &tg.InputPeerEmpty{}}
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetDialogs(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// ReadHistoryForTest encodes req and invokes handleReadHistory for the caller.
func ReadHistoryForTest(s *store.Store, userID int64, req *tg.MessagesReadHistoryRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleReadHistory(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// SetTypingForTest encodes req and invokes handleSetTyping for the caller.
func SetTypingForTest(s *store.Store, userID int64, req *tg.MessagesSetTypingRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSetTyping(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// LogOutForTest invokes handleLogOut for a request arriving on authKeyID,
// handing back the evict announcement unrun so a test can assert whether it was
// already published by the time the handler returned.
func LogOutForTest(s *store.Store, authKeyID [8]byte) (bin.Encoder, func(), error) {
	var buf bin.Buffer
	if err := (&tg.AuthLogOutRequest{}).Encode(&buf); err != nil {
		return nil, nil, err
	}
	return testHandlers(s).handleLogOut(&mtproto.Request{
		Ctx: context.Background(), AuthKeyID: authKeyID, Buf: &buf,
	})
}

// ResetAuthorizationForTest invokes handleResetAuthorization for userID against
// hash, on a request arriving on authKeyID, with the same unrun announcement.
func ResetAuthorizationForTest(s *store.Store, userID int64, authKeyID [8]byte, hash int64) (bin.Encoder, func(), error) {
	var buf bin.Buffer
	if err := (&tg.AccountResetAuthorizationRequest{Hash: hash}).Encode(&buf); err != nil {
		return nil, nil, err
	}
	return testHandlers(s).handleResetAuthorization(&mtproto.Request{
		Ctx: context.Background(), UserID: userID, AuthKeyID: authKeyID, Buf: &buf,
	})
}

// SendMessageForTest encodes req and invokes handleSendMessage for the caller.
func SendMessageForTest(s *store.Store, userID int64, req *tg.MessagesSendMessageRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSendMessage(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// EditMessageForTest encodes req and invokes handleEditMessage for the caller.
func EditMessageForTest(s *store.Store, userID int64, req *tg.MessagesEditMessageRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleEditMessage(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}
