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

// MaxGetChannels exposes the getChannels input cap to the api_test package.
const MaxGetChannels = maxGetChannels
