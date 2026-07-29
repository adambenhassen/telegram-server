package api

import (
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	// maxChannelAbout bounds the description a client may set. channels.about is
	// an unbounded TEXT column and the store does not validate it, so the bound
	// belongs here: without one a single request can park most of a 16 MB frame
	// in one row, once per channel the account is allowed to create.
	maxChannelAbout = 255
	// maxGetChannels caps channels.getChannels' input vector. Each id costs two
	// store round trips inside loadChannels, and a Vector<InputChannel> carries
	// ~650k ids inside gotd's 16 MB frame, so an uncapped vector is free
	// amplification. 100 is Telegram's own documented limit for the method.
	maxGetChannels = 100
)

// channelAbout validates a client-supplied channel description and returns the
// trimmed form that gets stored. An empty about is legal — unlike a title, a
// channel without a description is a normal channel.
//
// Text the server cannot store and an over-length description share
// errChatTitleInvalid with the title guard rather than getting an error of their
// own: both are "the metadata you sent is not storable", the client's fix is the
// same either way, and a new error string here would be one more thing for a
// client to special-case for no gain.
func channelAbout(raw string) (string, error) {
	about := strings.TrimSpace(raw)
	if !validText(about) || utf8.RuneCountInString(about) > maxChannelAbout {
		return "", errChatTitleInvalid
	}
	return about, nil
}

// inputChannelID resolves a client-supplied InputChannel to a channel id through
// the same validation inputPeer applies, so the placeholder access-hash rule is
// written once. InputChannelEmpty and InputChannelFromMessage are
// PEER_ID_INVALID: neither names a channel this server can resolve.
func inputChannelID(c tg.InputChannelClass) (int64, error) {
	v, ok := c.(*tg.InputChannel)
	if !ok {
		return 0, errPeerIDInvalid
	}
	_, id, err := inputPeer(&tg.InputPeerChannel{ChannelID: v.ChannelID, AccessHash: v.AccessHash})
	if err != nil {
		return 0, err
	}
	return id, nil
}

// handleCreateChannel serves channels.createChannel.
//
// The reply carries no service message and does not move the channel's pts. M7
// writes no channel service messages at all, and a create that bumped the pts
// before any client can have read the channel would only make every future
// getChannelDifference start one step behind.
func (h *handlers) handleCreateChannel(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsCreateChannelRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	title, err := chatTitle(req.Title)
	if err != nil {
		return nil, err
	}
	about, err := channelAbout(req.About)
	if err != nil {
		return nil, err
	}
	// Exactly one of the two flags decides the kind. Neither set leaves the kind
	// unstated and both set states two, and guessing either way writes a channel
	// the client did not ask for — a broadcast and a megagroup differ in who may
	// post, so the wrong guess is a permissions decision made for the caller.
	if req.Megagroup == req.Broadcast {
		return nil, errPeerIDInvalid
	}

	ch, err := h.store.CreateChannel(r.Ctx, r.UserID, title, about, req.Megagroup)
	// The per-account channel cap reuses USERS_TOO_MUCH, the same wire error the
	// chat path returns for its participant cap (internal/api/chats.go:150). It
	// is the closest existing sentinel, and a distinct one would tell a caller
	// exactly when an account is at its limit — a fact about that account which
	// no legitimate client behaviour depends on.
	if errors.Is(err, store.ErrTooManyChannels) {
		return nil, errUsersTooMuch
	}
	if err != nil {
		h.log.Error("create channel", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	chats, err := h.loadChannels(r.Ctx, map[int64]bool{ch.ID: true}, r.UserID)
	if err != nil {
		h.log.Error("create channel render", "channel_id", ch.ID, "err", err)
		return nil, errInternal
	}
	users, err := h.loadUsers(r.Ctx, map[int64]bool{r.UserID: true}, r.UserID)
	if err != nil {
		h.log.Error("create channel users", "channel_id", ch.ID, "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: ch.ID}},
		Chats:   chats,
		Users:   users,
		Date:    int(ch.Date.Unix()),
	}, nil
}

// handleGetChannels serves channels.getChannels. Rendering goes through
// loadChannels with the caller as viewer, which is the whole authorization
// boundary: a non-member or banned caller gets tg.ChannelForbidden and never a
// title, and an id with no channel row is dropped rather than distinguished.
func (h *handlers) handleGetChannels(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsGetChannelsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if len(req.ID) > maxGetChannels {
		return nil, errUsersTooMuch
	}
	ids := make(map[int64]bool, len(req.ID))
	for _, c := range req.ID {
		id, err := inputChannelID(c)
		if err != nil {
			return nil, err
		}
		ids[id] = true
	}

	chats, err := h.loadChannels(r.Ctx, ids, r.UserID)
	if err != nil {
		h.log.Error("get channels", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.MessagesChats{Chats: chats}, nil
}

// handleLeaveChannel serves channels.leaveChannel.
//
// The creator may leave, which the store already permits. M7 has no ownership
// transfer, so a channel whose creator left keeps its remaining admins and all
// of its content and simply has no creator — a deliberate acceptance, not an
// oversight: refusing would strand the creator in every channel they ever made.
func (h *handlers) handleLeaveChannel(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsLeaveChannelRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	id, err := inputChannelID(req.Channel)
	if err != nil {
		return nil, err
	}

	left, err := h.store.LeaveChannel(r.Ctx, id, r.UserID)
	if err != nil {
		h.log.Error("leave channel", "channel_id", id, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	// No participant row and no channel at all are one error on purpose, the way
	// the chat path collapses them (internal/api/chatusers.go:35): channel ids are
	// a dense BIGSERIAL space, so a distinguishable not-found would make every
	// channel on the server enumerable by a caller who is in none of them.
	if !left {
		return nil, errPeerIDInvalid
	}

	// Rendered after the delete, so the caller is no longer a member and this is
	// tg.ChannelForbidden — the client's signal to drop the channel's metadata.
	chats, err := h.loadChannels(r.Ctx, map[int64]bool{id: true}, r.UserID)
	if err != nil {
		h.log.Error("leave channel render", "channel_id", id, "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: id}},
		Chats:   chats,
	}, nil
}
