package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// AddChatUserForTest encodes req and invokes handleAddChatUser for the caller.
func AddChatUserForTest(s *store.Store, userID int64, req *tg.MessagesAddChatUserRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleAddChatUser(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// DeleteChatUserForTest encodes req and invokes handleDeleteChatUser.
func DeleteChatUserForTest(s *store.Store, userID int64, req *tg.MessagesDeleteChatUserRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleDeleteChatUser(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}
