package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// CreateChatForTest encodes req and invokes handleCreateChat for the caller.
func CreateChatForTest(s *store.Store, userID int64, req *tg.MessagesCreateChatRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleCreateChat(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// EditChatTitleForTest encodes req and invokes handleEditChatTitle for the caller.
func EditChatTitleForTest(s *store.Store, userID int64, req *tg.MessagesEditChatTitleRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleEditChatTitle(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// ChatTitle exposes the title guard for the external api_test package.
var ChatTitle = chatTitle
