package api

import (
	"context"
	"errors"
	"strings"
	"time"
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

	// A banned member may not leave. LeaveChannel deletes the participant row and
	// JoinChannelByInvite (internal/store/channels.go:281) admits any account that
	// has none, so without this check leave-then-rejoin is a ban reset for anyone
	// still holding the invite hash. It is the same errPeerIDInvalid a non-member
	// gets, so it tells a banned caller nothing new: loadChannels already renders
	// them channelForbidden, which is what a stranger sees too.
	//
	// The check and the delete are two statements, so a ban landing between them
	// still slips through. Closing that means the store's delete carrying the ban
	// predicate in its WHERE clause, which is a store change and belongs with the
	// editBanned ticket that introduces the only writer of banned_until — M7 has
	// no RPC that sets a ban, so today nothing races this.
	member, found, err := h.store.ChannelMemberOf(r.Ctx, id, r.UserID)
	if err != nil {
		h.log.Error("leave channel membership", "channel_id", id, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !found || member.Banned(time.Now()) {
		return nil, errPeerIDInvalid
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

// maxChannelMessagesPerCall caps the ids one channels.getMessages may name, so
// a single call cannot ask the server to hydrate an unbounded batch.
const maxChannelMessagesPerCall = 100

// requireChannelMember is the read gate for a client-supplied channel id: the
// caller must hold a participant row that is not banned as of now. An unknown
// channel, a channel the caller never joined and a banned member all report
// errPeerIDInvalid, so a caller cannot probe which channel ids exist over a
// dense id space, and a ban is not merely cosmetic.
//
// Unlike the post path, whose authoritative check lives inside the store's
// write transaction under the channel_state row lock, a read has no write to
// order against: this check and the read that follows are the whole of it.
func (h *handlers) requireChannelMember(ctx context.Context, channelID, userID int64) error {
	member, found, err := h.store.ChannelMemberOf(ctx, channelID, userID)
	if err != nil {
		h.log.Error("channel membership", "channel_id", channelID, "user_id", userID, "err", err)
		return errInternal
	}
	if !found || member.Banned(time.Now()) {
		return errPeerIDInvalid
	}
	return nil
}

// sendChannelMessage posts to a channel and returns the poster-side Updates.
//
// There is no membership or post-rights check here on purpose, and it is the
// same reasoning handleEditChatTitle records for chats: the store re-checks
// both inside its own transaction under the channel_state row lock, and that is
// the authorization boundary. A check here would run in a different transaction
// and the gap between the two is exactly what a member banned mid-call would
// ride. store.ErrNotMember covers "not a member", "banned", "may not post in a
// broadcast" and "no such channel" alike, and all four must stay one wire error
// so channel ids are not enumerable.
func (h *handlers) sendChannelMessage(r *mtproto.Request, channelID int64, req *tg.MessagesSendMessageRequest) (bin.Encoder, error) {
	// PostChannelMessageAs, never PostChannelMessage: the latter is the
	// unchecked primitive and trusts its caller to have decided post rights.
	msg, pts, _, err := h.store.PostChannelMessageAs(r.Ctx, channelID, r.UserID, req.Message, req.RandomID, nil)
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("send channel message", "user_id", r.UserID, "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	// No notify is emitted here. Delivery of a channel post lands in MAIN-96,
	// which owns the channel notify payload and the member fan-out; nothing is
	// emitted rather than inventing a payload this handler would then have to
	// keep. When it does land it is ONE notify per post, never one per member
	// (threat model G4): a 10 000-member channel would otherwise land 10 000
	// notifications on the single Listener goroutine and stall live delivery for
	// every unrelated account on the replica. A dup — a resend of an
	// already-stored post, which moved no pts and wrote no event — announces
	// nothing either way, the same rule the 1:1 and chat send paths follow, so
	// the dup flag is not read here.

	channels, err := h.loadChannels(r.Ctx, map[int64]bool{channelID: true}, r.UserID)
	if err != nil {
		h.log.Error("send channel message channels", "err", err)
		return nil, errInternal
	}
	users, err := h.loadUsers(r.Ctx, map[int64]bool{r.UserID: true}, r.UserID)
	if err != nil {
		h.log.Error("send channel message users", "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: int(msg.LocalID), RandomID: req.RandomID},
			// sendMessage never carries media, so the post has no file to hydrate.
			&tg.UpdateNewChannelMessage{Message: channelMessageToTL(msg, r.UserID, nil), Pts: pts, PtsCount: 1},
		},
		Chats: channels,
		Users: users,
		Date:  int(msg.Date.Unix()),
	}, nil
}

// handleGetChannelMessages serves channels.getMessages: the named posts of one
// channel, for a caller who is currently an unbanned member of it.
func (h *handlers) handleGetChannelMessages(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsGetMessagesRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	channelID, err := inputChannelID(req.Channel)
	if err != nil {
		return nil, err
	}
	if err = h.requireChannelMember(r.Ctx, channelID, r.UserID); err != nil {
		return nil, err
	}

	ids := req.ID
	if len(ids) > maxChannelMessagesPerCall {
		ids = ids[:maxChannelMessagesPerCall]
	}
	localIDs := make([]int64, 0, len(ids))
	for _, id := range ids {
		v, ok := id.(*tg.InputMessageID)
		if !ok {
			// The other input-message forms name a message by reply or by pinned
			// position, neither of which M7 resolves.
			return nil, errMessageIDInvalid
		}
		localIDs = append(localIDs, int64(v.ID))
	}

	found, err := h.store.ChannelMessages(r.Ctx, channelID, localIDs)
	if err != nil {
		h.log.Error("channel messages", "user_id", r.UserID, "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	// Preserve the caller's order and drop ids with no row, the way the wire
	// expects: a client asks for what it is missing and gets back what exists.
	msgs := make([]store.ChannelMessage, 0, len(localIDs))
	for _, id := range localIDs {
		if m, ok := found[id]; ok {
			msgs = append(msgs, m)
		}
	}
	return h.channelMessages(r, channelID, msgs)
}

// channelHistory renders one page of a channel's history for the caller, whom
// requireChannelMember has already established is an unbanned member.
//
// A member reads the channel's whole history, including posts from before they
// joined. That is deliberate: join_pts bounds how far back the difference path
// will replay, a cost control, and it is NOT a confidentiality control (threat
// model G5). Nothing may later be built on it as one.
func (h *handlers) channelHistory(r *mtproto.Request, channelID int64, req *tg.MessagesGetHistoryRequest, limit int) (bin.Encoder, error) {
	msgs, err := h.store.ChannelHistory(r.Ctx, channelID, int64(req.OffsetID), limit)
	if err != nil {
		h.log.Error("channel history", "user_id", r.UserID, "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	return h.channelMessages(r, channelID, msgs)
}

// channelMessages renders a batch of one channel's posts into the wire reply
// both channel read paths return. It is messages.channelMessages and not
// messages.messagesSlice: a client needs the channel's pts to know where the
// batch sits in that channel's own update stream.
func (h *handlers) channelMessages(r *mtproto.Request, channelID int64, msgs []store.ChannelMessage) (bin.Encoder, error) {
	files, err := h.loadChannelFiles(r.Ctx, msgs)
	if err != nil {
		h.log.Error("channel messages files", "user_id", r.UserID, "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	// A page has as many authors as the channel has posters, so the user list is
	// collected off the batch itself.
	authors := map[int64]bool{r.UserID: true}
	tlMsgs := make([]tg.MessageClass, len(msgs))
	for i, m := range msgs {
		tlMsgs[i] = channelMessageToTL(m, r.UserID, files)
		authors[m.FromID] = true
	}

	pts, err := h.store.ChannelState(r.Ctx, channelID)
	if err != nil {
		h.log.Error("channel messages pts", "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	users, err := h.loadUsers(r.Ctx, authors, r.UserID)
	if err != nil {
		h.log.Error("channel messages users", "err", err)
		return nil, errInternal
	}
	channels, err := h.loadChannels(r.Ctx, map[int64]bool{channelID: true}, r.UserID)
	if err != nil {
		h.log.Error("channel messages channels", "err", err)
		return nil, errInternal
	}
	return &tg.MessagesChannelMessages{
		Pts:      pts,
		Count:    len(tlMsgs),
		Messages: tlMsgs,
		Chats:    channels,
		Users:    users,
	}, nil
}
