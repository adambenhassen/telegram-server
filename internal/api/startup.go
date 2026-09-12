package api

import (
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func (h *handlers) handleGetSavedReactionTags(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetSavedReactionTagsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.MessagesSavedReactionTags{Tags: []tg.SavedReactionTag{}}, nil
}

func (h *handlers) handleGetAttachMenuBots(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetAttachMenuBotsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.AttachMenuBots{
		Bots:  []tg.AttachMenuBot{},
		Users: []tg.UserClass{},
	}, nil
}

func (h *handlers) handleGetStickerSet(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetStickerSetRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.MessagesStickerSetNotModified{}, nil
}

func (h *handlers) handleGetAllDrafts(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetAllDraftsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{},
		Date:    int(time.Now().Unix()),
	}, nil
}

func (h *handlers) handleReceivedMessages(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesReceivedMessagesRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.ReceivedNotifyMessageVector{Elems: []tg.ReceivedNotifyMessage{}}, nil
}

func (h *handlers) handleGetJoinedCommunities(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.CommunitiesGetJoinedCommunitiesRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.MessagesChats{Chats: []tg.ChatClass{}}, nil
}
