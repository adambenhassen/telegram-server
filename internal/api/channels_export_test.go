package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// CreateChannelForTest encodes req and invokes handleCreateChannel for the caller.
func CreateChannelForTest(s *store.Store, userID int64, req *tg.ChannelsCreateChannelRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleCreateChannel(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// GetChannelsForTest encodes req and invokes handleGetChannels for the caller.
func GetChannelsForTest(s *store.Store, userID int64, req *tg.ChannelsGetChannelsRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetChannels(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// LeaveChannelForTest encodes req and invokes handleLeaveChannel for the caller.
func LeaveChannelForTest(s *store.Store, userID int64, req *tg.ChannelsLeaveChannelRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleLeaveChannel(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// ExportChatInviteForTest encodes req and invokes handleExportChatInvite for the caller.
func ExportChatInviteForTest(s *store.Store, userID int64, req *tg.MessagesExportChatInviteRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleExportChatInvite(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// CheckChatInviteForTest encodes req and invokes handleCheckChatInvite for the caller.
func CheckChatInviteForTest(s *store.Store, userID int64, req *tg.MessagesCheckChatInviteRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleCheckChatInvite(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// ImportChatInviteForTest encodes req and invokes handleImportChatInvite for the caller.
func ImportChatInviteForTest(s *store.Store, userID int64, req *tg.MessagesImportChatInviteRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleImportChatInvite(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// EditAdminForTest encodes req and invokes handleEditAdmin for the caller.
func EditAdminForTest(s *store.Store, userID int64, req *tg.ChannelsEditAdminRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleEditAdmin(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// EditBannedForTest encodes req and invokes handleEditBanned for the caller.
func EditBannedForTest(s *store.Store, userID int64, req *tg.ChannelsEditBannedRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleEditBanned(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// GetChannelDifferenceForTest encodes req and invokes handleGetChannelDifference
// for the caller.
func GetChannelDifferenceForTest(s *store.Store, userID int64, req *tg.UpdatesGetChannelDifferenceRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetChannelDifference(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// RevokeExportedChatInviteForTest invokes handleRevokeExportedChatInvite for the caller.
func RevokeExportedChatInviteForTest(s *store.Store, userID, channelID int64, hash string) (bin.Encoder, error) {
	var buf bin.Buffer
	buf.PutID(revokeExportedChatInviteTypeID)
	peer := &tg.InputPeerChannel{ChannelID: channelID, AccessHash: DeriveChannelHash(userID, channelID)}
	if err := peer.Encode(&buf); err != nil {
		return nil, err
	}
	buf.PutString(hash)
	return testHandlers(s).handleRevokeExportedChatInvite(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// MaxGetChannels exposes the getChannels input cap to the api_test package.
const MaxGetChannels = maxGetChannels
