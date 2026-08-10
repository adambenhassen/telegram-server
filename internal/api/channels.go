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
	"github.com/adambenhassen/telegram-server/internal/peerhash"
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

// The coarse participant roles M7 stores, mirroring internal/store's own
// unexported constants. There is no bitfield: a member either holds admin
// rights or does not.
const (
	channelRoleMember  = 0
	channelRoleAdmin   = 1
	channelRoleCreator = 2
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

// inputChannelID resolves a client-supplied InputChannel to a channel id.
// Channel access_hash must be derived for (viewerID, channelID). M1 placeholder
// (access_hash == id) is rejected. InputChannelEmpty and InputChannelFromMessage
// are PEER_ID_INVALID.
func (h *handlers) inputChannelID(c tg.InputChannelClass, viewerID int64) (int64, error) {
	v, ok := c.(*tg.InputChannel)
	if !ok {
		return 0, errPeerIDInvalid
	}
	if v.ChannelID == 0 {
		return 0, errPeerIDInvalid
	}
	wantHash := h.peers.Derive(viewerID, peerhash.KindChannel, v.ChannelID)
	if v.AccessHash != wantHash {
		return 0, errPeerIDInvalid
	}
	return v.ChannelID, nil
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
	// Rate limit before any write.
	if err := h.checkRateLimit(r, "create_channel", h.rateLimitCreateChannel); err != nil {
		return nil, err
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
		id, err := h.inputChannelID(c, r.UserID)
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
	id, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}
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
	// the chat path collapses them (internal/api/chatusers.go:35): a
	// distinguishable not-found answers "does this channel exist" for every id a
	// caller can name, and the id is never what admits anyone.
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
// errPeerIDInvalid, so the error cannot be read as an answer to "does this
// channel exist", and a ban is not merely cosmetic.
//
// Unlike the post path, whose authoritative check lives inside the store's
// write transaction under the channel_state row lock, a read has no write to
// order against: this check and the read that follows are the whole of it.
func (h *handlers) requireChannelMember(ctx context.Context, channelID, userID int64) (store.ChannelMember, error) {
	member, found, err := h.store.ChannelMemberOf(ctx, channelID, userID)
	if err != nil {
		h.log.Error("channel membership", "channel_id", channelID, "user_id", userID, "err", err)
		return store.ChannelMember{}, errInternal
	}
	if !found || member.Banned(time.Now()) {
		return store.ChannelMember{}, errPeerIDInvalid
	}
	return member, nil
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
	// Check for a transport retry before the rate limit.
	if req.RandomID != 0 {
		if existing, ok, err := h.store.ChannelMessageByRandomID(r.Ctx, channelID, req.RandomID); err == nil && ok {
			// Retry: return the stored post, at the pts it occupies, without
			// rate-limiting. Never the channel's current pts — see
			// store.ErrPtsUnknown for what a subscriber loses to that.
			pts, err := h.store.ChannelPostPts(r.Ctx, channelID, existing.LocalID)
			if err != nil {
				h.log.Error("read stored post pts on retry", "channel_id", channelID, "err", err)
				return nil, errInternal
			}
			channels, err := h.loadChannels(r.Ctx, map[int64]bool{channelID: true}, r.UserID)
			if err != nil {
				h.log.Error("load channels on retry", "err", err)
				return nil, errInternal
			}
			users, err := h.loadUsers(r.Ctx, map[int64]bool{r.UserID: true}, r.UserID)
			if err != nil {
				h.log.Error("load users on retry", "err", err)
				return nil, errInternal
			}
			return &tg.Updates{
				Updates: []tg.UpdateClass{
					&tg.UpdateMessageID{ID: int(existing.LocalID), RandomID: req.RandomID},
					&tg.UpdateNewChannelMessage{Message: channelMessageToTL(existing, r.UserID, nil), Pts: pts, PtsCount: 1},
				},
				Users: users,
				Chats: channels,
				Date:  int(existing.Date.Unix()),
			}, nil
		} else if err != nil {
			h.log.Error("random_id lookup", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
	}

	// Rate limit: new message, consume a token.
	if err := h.checkRateLimit(r, "message_send", h.rateLimitMessageSend); err != nil {
		return nil, err
	}

	// PostChannelMessageAs, never PostChannelMessage: the latter is the
	// unchecked primitive and trusts its caller to have decided post rights.
	msg, pts, dup, err := h.store.PostChannelMessageAs(r.Ctx, channelID, r.UserID, req.Message, req.RandomID, nil)
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("send channel message", "user_id", r.UserID, "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	// Only notify when the post is new. A duplicate means another caller
	// already committed the same random_id and fired the notify.
	if !dup {
		h.notifyChannelPost(r.Ctx, channelID)
	}

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

// handleGetChannelDifference serves updates.getChannelDifference.
//
// ponytail: channelDifferenceTooLong is not implemented. Nothing trims
// channel_events, so no pts can fall off the log yet and the too-long path
// would be unreachable code. It rides with the event-log GC decision
// (ROADMAP.md:258).
func (h *handlers) handleGetChannelDifference(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.UpdatesGetChannelDifferenceRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	channelID, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}
	member, err := h.requireChannelMember(r.Ctx, channelID, r.UserID)
	if err != nil {
		return nil, err
	}

	// Clamp pts to join_pts (G5 cost control). A client-supplied pts below the
	// floor is clamped rather than trusted, so a fresh joiner cannot make the
	// server replay a decade of events. This is NOT a confidentiality control:
	// getHistory still serves the full history to a current member, deliberately,
	// and that asymmetry needs this comment or someone will later "fix" one of
	// the two to match the other.
	fromPts := max(req.Pts, member.JoinPts)

	// Clamp req.Limit into [1, maxDiffEvents].
	limit := max(1, min(req.Limit, maxDiffEvents))

	// Read state first to clamp ahead-of-server.
	currentPts, err := h.store.ChannelState(r.Ctx, channelID)
	if err != nil {
		h.log.Error("channel difference state", "channel_id", channelID, "err", err)
		return nil, errInternal
	}

	// Client pts above server's current pts: clamp to empty (same as
	// handleGetDifference for the per-account stream).
	if fromPts >= currentPts {
		return &tg.UpdatesChannelDifferenceEmpty{Pts: currentPts, Final: true}, nil
	}

	b, err := h.buildChannelUpdates(r.Ctx, channelID, r.UserID, fromPts, limit, currentPts)
	if err != nil {
		h.log.Error("channel difference build", "channel_id", channelID, "err", err)
		return nil, errInternal
	}

	if len(b.ups) == 0 {
		return &tg.UpdatesChannelDifferenceEmpty{Pts: b.currentPts, Final: true}, nil
	}

	// Extract messages from updates for NewMessages.
	var newMessages []tg.MessageClass
	for _, u := range b.ups {
		if nuc, ok := u.(*tg.UpdateNewChannelMessage); ok {
			newMessages = append(newMessages, nuc.Message)
		}
	}

	if b.more {
		return &tg.UpdatesChannelDifference{
			Final:        false,
			Pts:          b.currentPts,
			NewMessages:  newMessages,
			OtherUpdates: nil,
			Chats:        b.chats,
			Users:        b.users,
		}, nil
	}
	return &tg.UpdatesChannelDifference{
		Final:        true,
		Pts:          b.currentPts,
		NewMessages:  newMessages,
		OtherUpdates: nil,
		Chats:        b.chats,
		Users:        b.users,
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
	channelID, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}
	if _, err = h.requireChannelMember(r.Ctx, channelID, r.UserID); err != nil {
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

// channelSearch renders one page of a channel's keyword search for the caller,
// whom requireChannelMember has already established is an unbanned member.
//
// The whole history is searchable, posts from before the caller joined
// included, for the same reason channelHistory serves them: join_pts bounds the
// difference path's replay as a cost control and is never a confidentiality one
// (threat model G5). Search reaches no further than getHistory already does.
//
// Membership is re-established by the caller on every page rather than carried
// in offset_id, so a ban landing between two pages stops the next one.
func (h *handlers) channelSearch(r *mtproto.Request, channelID int64, query string, offsetID int64, limit int) (bin.Encoder, error) {
	msgs, err := h.store.SearchChannelPosts(r.Ctx, channelID, query, offsetID, limit)
	if err != nil {
		h.log.Error("channel search", "user_id", r.UserID, "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	return h.channelMessages(r, channelID, msgs)
}

// channelMessages renders a batch of one channel's posts into the wire reply
// every channel read path returns. It is messages.channelMessages and not
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

// inviteLinkPrefix is what a hash is rendered behind. The link is the whole
// credential, so nothing but the hash may appear after it — an id in the link
// hands a real channel id to everyone the link travels through, and the hash
// alone is what admits.
const inviteLinkPrefix = "https://t.me/+"

// revokeExportedChatInviteTypeID is the constructor id of
// messages.revokeExportedChatInvite (0x13db322c). gotd v0.161.0 does not
// generate this type, so the handler decodes the two fields (peer, hash)
// directly from the buffer.
const revokeExportedChatInviteTypeID = 0x13db322c

// revokeExportedChatInviteRequest carries the decoded request for
// messages.revokeExportedChatInvite.
type revokeExportedChatInviteRequest struct {
	Peer tg.InputPeerClass
	Hash string
}

func (r *revokeExportedChatInviteRequest) Decode(b *bin.Buffer) error {
	if err := b.ConsumeID(revokeExportedChatInviteTypeID); err != nil {
		return err
	}
	peer, err := tg.DecodeInputPeer(b)
	if err != nil {
		return err
	}
	r.Peer = peer
	h, err := b.String()
	if err != nil {
		return err
	}
	r.Hash = h
	return nil
}

// handleExportChatInvite serves messages.exportChatInvite for a channel.
//
// Every rejection returns errPeerIDInvalid: no channel row, no participant row,
// role 0, or a live ban. They are ONE error deliberately: a distinguishable
// "not a member" would confirm to any account that a channel with that id
// exists, and the invite hash is the only thing standing between an account and
// admission.
//
// Role 1 is the floor because an invite is a bearer credential for the whole
// channel: a role-0 member able to mint one would put every member in charge of
// the channel's admission boundary.
//
// Every call mints a NEW hash and no call retires an old one: M7 has no
// revocation, so an admin exporting twice has permanently issued two independent
// bearer credentials to the channel and can withdraw neither. That fan-out is
// unbounded and is not closed by returning the existing invite instead —
// revocation is what closes it, and it is out of M7 scope. Until then, every
// hash an admin has ever exported admits forever.
//
// req.ExpireDate, req.UsageLimit, req.RequestNeeded, req.LegacyRevokePermanent
// and req.Title are accepted and IGNORED. M7 stores none of them, so the invite
// this returns never expires, has no usage limit and needs no approval — the
// reply says so with Permanent. Accepting and ignoring is the posture M6 took
// for fwd_limit; it is stated here because a silently-dropped expiry is the kind
// of thing a reader assumes works.
func (h *handlers) handleExportChatInvite(r *mtproto.Request) (bin.Encoder, error) {
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	var req tg.MessagesExportChatInviteRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	peerType, channelID, err := h.inputPeer(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}
	// Chats keep having no invites in M7, and a user peer never had any.
	if peerType != store.PeerTypeChannel {
		return nil, errPeerIDInvalid
	}

	member, found, err := h.store.ChannelMemberOf(r.Ctx, channelID, r.UserID)
	if err != nil {
		h.log.Error("export chat invite membership", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !found || member.Role < 1 || member.Banned(time.Now()) {
		return nil, errPeerIDInvalid
	}

	hash, err := h.store.CreateChannelInvite(r.Ctx, channelID, r.UserID)
	if err != nil {
		h.log.Error("export chat invite create", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.ChatInviteExported{
		Link:      inviteLinkPrefix + hash,
		AdminID:   r.UserID,
		Date:      int(time.Now().Unix()),
		Permanent: true,
	}, nil
}

// handleRevokeExportedChatInvite serves messages.revokeExportedChatInvite.
// gotd v0.161.0 does not generate this request type, so the handler decodes
// it manually (peer, hash) and registers the constructor id directly.
//
// Every rejection returns errPeerIDInvalid: no channel row, no participant row,
// role 0, a live ban, or a hash that the channel never minted. They are ONE
// error deliberately — a distinguishable "hash not found" would let an account
// walk the invite space.
//
// Role 1 is the floor for the same reason export requires it: a role-0 member
// able to revoke would be able to confirm which hashes belong to the channel.
//
// Revoke is idempotent: a second revoke of the same hash is a no-op. The store
// writes nothing new and returns success.
func (h *handlers) handleRevokeExportedChatInvite(r *mtproto.Request) (bin.Encoder, error) {
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	var req revokeExportedChatInviteRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	peerType, channelID, err := h.inputPeer(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}
	if peerType != store.PeerTypeChannel {
		return nil, errPeerIDInvalid
	}

	// Strip link prefix if client sent full link instead of bare hash.
	hash, _ := strings.CutPrefix(req.Hash, inviteLinkPrefix)

	member, found, err := h.store.ChannelMemberOf(r.Ctx, channelID, r.UserID)
	if err != nil {
		h.log.Error("revoke exported chat invite membership", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !found || member.Role < 1 || member.Banned(time.Now()) {
		return nil, errPeerIDInvalid
	}

	if err = h.store.RevokeChannelInvite(r.Ctx, hash, channelID); err != nil {
		h.log.Error("revoke exported chat invite", "channel_id", channelID, "hash", hash, "err", err)
		return nil, errInternal
	}

	return &tg.BoolTrue{}, nil
}

// channelMemberUpdate is the reply both membership-editing RPCs return: the
// channel as the CALLER may now see it, plus the two accounts involved. No
// service message and no pts move — M7 writes no channel service messages, and
// a role or ban change is not a post.
func (h *handlers) channelMemberUpdate(r *mtproto.Request, channelID, targetID int64) (bin.Encoder, error) {
	chats, err := h.loadChannels(r.Ctx, map[int64]bool{channelID: true}, r.UserID)
	if err != nil {
		h.log.Error("channel member update channels", "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	users, err := h.loadUsers(r.Ctx, map[int64]bool{r.UserID: true, targetID: true}, r.UserID)
	if err != nil {
		h.log.Error("channel member update users", "channel_id", channelID, "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: channelID}},
		Chats:   chats,
		Users:   users,
		Date:    int(time.Now().Unix()),
	}, nil
}

// channelRoleUnchanged reports whether the store.ErrNotMember SetChannelRole
// just returned was only "the target already holds this role".
//
// SetChannelRole is deliberately not idempotent: G3 lists two transitions and a
// no-op is not one of them, so it fails closed with the same error a rights
// rejection returns. MTProto clients retry, and a retried promotion that has
// already committed must not come back as PEER_ID_INVALID. This re-read is the
// ONE thing the handler is allowed to decide about the rule set, and it decides
// nothing the caller could not already see: it converts a failure into a
// success only when the caller is an unbanned creator — the one role the store
// would have let through — and the target already sits at the requested role.
// A caller who is not that gets the identical error either way.
func (h *handlers) channelRoleUnchanged(ctx context.Context, channelID, callerID, targetID int64, role int) (bool, error) {
	caller, found, err := h.store.ChannelMemberOf(ctx, channelID, callerID)
	if err != nil {
		return false, err
	}
	if !found || caller.Role != channelRoleCreator || caller.Banned(time.Now()) {
		return false, nil
	}
	target, found, err := h.store.ChannelMemberOf(ctx, channelID, targetID)
	if err != nil {
		return false, err
	}
	return found && target.Role == role, nil
}

// handleEditAdmin serves channels.editAdmin.
//
// There is no rights check here on purpose, the same reasoning
// handleEditChatTitle records for chats: the store re-checks caller and target
// inside its own transaction under the channels row lock, and that is the
// authorization boundary. A copy of the rule set here would run in a different
// transaction, and the gap between the two is exactly what a concurrent
// demotion would ride.
//
// req.AdminRights is a bitfield and M7 stores no bitfield, so it collapses to
// the coarse role: any right set at all promotes to role 1, the zero value
// demotes to role 0. The individual flags are accepted and IGNORED — the M6
// posture for fwd_limit and for ChatAdminRights (ROADMAP.md:271). A client that
// sets only BanUsers gets a FULL admin, which is the consequence of a coarse
// role and is stated here rather than left as a silent surprise. req.Rank is
// accepted and ignored too: M7 stores no custom rank.
//
// store.ErrNotMember is the only mapping, and every rejection it covers — not a
// member, not the creator, target is the creator, target has no row, self as
// target, no such channel — returns the identical errPeerIDInvalid. A
// distinguishable "you may not do that" would confirm both that the channel
// exists and what the target's role is.
func (h *handlers) handleEditAdmin(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsEditAdminRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	channelID, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}
	targetID, err := h.inputUserID(req.UserID, r.UserID)
	if err != nil {
		return nil, err
	}

	role := channelRoleMember
	if !req.AdminRights.Zero() {
		role = channelRoleAdmin
	}

	err = h.store.SetChannelRole(r.Ctx, channelID, r.UserID, targetID, role)
	if errors.Is(err, store.ErrNotMember) {
		unchanged, rerr := h.channelRoleUnchanged(r.Ctx, channelID, r.UserID, targetID, role)
		if rerr != nil {
			h.log.Error("edit admin recheck", "channel_id", channelID, "user_id", r.UserID, "err", rerr)
			return nil, errInternal
		}
		if !unchanged {
			return nil, errPeerIDInvalid
		}
	} else if err != nil {
		h.log.Error("edit admin", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return h.channelMemberUpdate(r, channelID, targetID)
}

// handleEditBanned serves channels.editBanned. It is the same thin shape as
// handleEditAdmin and for the same reason: the store owns the rule set.
//
// ViewMessages is the only flag M7 reads. Set, it is a ban; an until_date of 0
// means forever and anything else is that instant. The ZERO value — and only
// the zero value — is the unban.
//
// A rights struct that revokes something other than ViewMessages is rejected
// rather than ignored. M7 has no partial restriction to store, so ignoring the
// flags would land such a request on the unban path: a caller tightening a
// restriction on a currently-banned member would clear that member's ban and be
// told the edit applied, which is the write moving opposite to the request.
//
// An until_date already in the past is rejected instead of written. It would
// otherwise commit a ban that ChannelMember.Banned reports as already lapsed,
// so the caller would be told a member was banned who is not.
func (h *handlers) handleEditBanned(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsEditBannedRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	channelID, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}
	// Only a user may be banned. A chat or channel peer here names no
	// participant row.
	targetID, err := h.peerUserID(req.Participant, r.UserID)
	if err != nil {
		return nil, err
	}

	// Decided on the client's own rights struct, before any channel or
	// participant row is read — the same ordering errUntilDateInvalid keeps
	// below. It is reachable without any channel existing, so it is not a
	// distinguishable outcome an attacker can play off errPeerIDInvalid, and the
	// post-read collapse to that one error is untouched.
	if !req.BannedRights.ViewMessages && !req.BannedRights.Zero() {
		return nil, errBannedRightsInvalid
	}

	var (
		until   *time.Time
		forever bool
	)
	if req.BannedRights.ViewMessages {
		if req.BannedRights.UntilDate == 0 {
			forever = true
		} else {
			ts := time.Unix(int64(req.BannedRights.UntilDate), 0)
			if !ts.After(time.Now()) {
				return nil, errUntilDateInvalid
			}
			until = &ts
		}
	}

	// A repeated ban and an unban of an unbanned member both succeed here, so
	// unlike editAdmin this needs no retry re-read.
	err = h.store.SetChannelBan(r.Ctx, channelID, r.UserID, targetID, until, forever)
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("edit banned", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return h.channelMemberUpdate(r, channelID, targetID)
}

// handleCheckChatInvite serves messages.checkChatInvite. It writes nothing: the
// hash is looked up, the caller's membership is read, and that is all.
//
// The hash is the only input. Every failure — unknown hash, an invite whose
// channel is gone, a malformed hash — returns errPeerIDInvalid, the same value
// export and import return. 128 bits of entropy only buys an unwalkable invite
// space if failures inside it are indistinguishable from one another.
func (h *handlers) handleCheckChatInvite(r *mtproto.Request) (bin.Encoder, error) {
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	var req tg.MessagesCheckChatInviteRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}

	ch, err := h.store.ChannelByInvite(r.Ctx, req.Hash)
	switch {
	case errors.Is(err, store.ErrInviteInvalid):
		return nil, errPeerIDInvalid
	case err != nil:
		h.log.Error("check chat invite lookup", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	member, found, err := h.store.ChannelMemberOf(r.Ctx, ch.ID, r.UserID)
	if err != nil {
		h.log.Error("check chat invite membership", "channel_id", ch.ID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if found {
		// A banned member holds a participant row, so it is still "already" — but
		// the channel it gets back is the forbidden form, the same answer
		// loadChannels gives. A ban that still served the live title would be
		// cosmetic.
		return &tg.ChatInviteAlready{Chat: h.channelToTL(ch, member, !member.Banned(time.Now()), r.UserID)}, nil
	}
	// No participants and no photo: M7 stores neither, and a preview naming who
	// is inside a private channel would give away more than the title does.
	return &tg.ChatInvite{
		Title:     ch.Title,
		About:     ch.About,
		Photo:     &tg.PhotoEmpty{},
		Channel:   true,
		Broadcast: !ch.Megagroup,
		Megagroup: ch.Megagroup,
	}, nil
}

// handleImportChatInvite serves messages.importChatInvite: the only way an
// account acquires a channel participant row.
//
// store.ErrInviteInvalid maps to errPeerIDInvalid, byte-identical to what export
// and check return, so the invite space stays unprobeable. The two cap errors
// may be distinct from it and from each other — reaching either needs a hash the
// caller already holds, so neither says anything about whether an invite exists.
//
// No NOTIFY is emitted and no pts moves: joining is not a channel event, and the
// joiner's join_pts was set inside the store's transaction under the channel's
// state row lock.
func (h *handlers) handleImportChatInvite(r *mtproto.Request) (bin.Encoder, error) {
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	var req tg.MessagesImportChatInviteRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}

	ch, member, err := h.store.JoinChannelByInvite(r.Ctx, req.Hash, r.UserID)
	switch {
	case errors.Is(err, store.ErrInviteInvalid):
		return nil, errPeerIDInvalid
	case errors.Is(err, store.ErrChannelFull):
		return nil, errUsersTooMuch
	case errors.Is(err, store.ErrTooManyChannels):
		return nil, errChannelsTooMuch
	case err != nil:
		h.log.Error("import chat invite join", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	users, err := h.loadUsers(r.Ctx, map[int64]bool{r.UserID: true}, r.UserID)
	if err != nil {
		h.log.Error("import chat invite render users", "channel_id", ch.ID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	// gotd v0.161.0 maps messages.importChatInvite to MessagesChatInviteJoinResultClass
	// (type id 0x445663a7), NOT bare Updates (0x74ae4240). Returning bare Updates would
	// cause the client to fail decoding with an unexpected type id error.
	return &tg.MessagesChatInviteJoinResultOk{
		Updates: &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: ch.ID}},
			Users:   users,
			// A re-join by a banned member returns that member's untouched row, and
			// it must not hand back metadata the ban revoked.
			Chats: []tg.ChatClass{h.channelToTL(ch, member, !member.Banned(time.Now()), r.UserID)},
			Date:  int(time.Now().Unix()),
		},
	}, nil
}

// handleJoinChannel serves channels.joinChannel: join a public channel by
// username. The caller first resolves the channel via contacts.resolveUsername,
// which returns the channel's access_hash. They then call channels.joinChannel
// with that peer.
//
// The access_hash is an addressing guard, not an admission credential. The
// admission decision is the re-read of channels.username IS NOT NULL inside
// the store's transaction. A wrong access_hash is caught by inputChannelID
// before the store is reached.
//
// Every rejection — private channel, unknown channel, banned caller — returns
// errPeerIDInvalid. A distinguishable rejection turns the refusal into an
// existence oracle for every id an attacker can name.
func (h *handlers) handleJoinChannel(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsJoinChannelRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	channelID, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}

	ch, member, err := h.store.JoinChannelByUsername(r.Ctx, channelID, r.UserID)
	switch {
	case errors.Is(err, store.ErrNotMember):
		return nil, errPeerIDInvalid
	case errors.Is(err, store.ErrChannelFull):
		return nil, errUsersTooMuch
	case errors.Is(err, store.ErrTooManyChannels):
		return nil, errChannelsTooMuch
	case err != nil:
		h.log.Error("join channel", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	users, err := h.loadUsers(r.Ctx, map[int64]bool{r.UserID: true}, r.UserID)
	if err != nil {
		h.log.Error("join channel render users", "channel_id", ch.ID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	// A re-join by a banned member returns that member's untouched row, and
	// it must not hand back metadata the ban revoked.
	//
	// Wrap in MessagesChatInviteJoinResultOk so the gotd client can decode the
	// response: ChannelsJoinChannel expects MessagesChatInviteJoinResultClass,
	// not bare Updates.
	return &tg.MessagesChatInviteJoinResultOk{
		Updates: &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateChannel{ChannelID: ch.ID}},
			Users:   users,
			Chats:   []tg.ChatClass{h.channelToTL(ch, member, !member.Banned(time.Now()), r.UserID)},
			Date:    int(time.Now().Unix()),
		},
	}, nil
}

// handleEditChannelUsername serves channels.updateUsername. An authenticated
// channel admin sets or clears the channel's public username. An empty string
// clears the current username. A non-empty string must pass validation
// (length, character set, first char, blocklist) before the store is consulted.
//
// Non-members get errPeerIDInvalid (same as every other channel read/write).
// Members who are not admins get errChatAdminRequired. The store re-checks both
// inside its own transaction under the channels row lock, so the handler-level
// check is an optimisation — it keeps a clearly-unauthorized caller from taking
// a row lock and holding it while waiting.
//
// Validation errors (bad format, reserved handle) are errUsernameInvalid.
// Username already taken is errUsernameOccupied.
// Rate limit exceeded is errUsernameFloodWait.
func (h *handlers) handleEditChannelUsername(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsUpdateUsernameRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	channelID, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}

	username := req.Username
	// Non-empty usernames must pass validation before any DB access.
	if username != "" {
		if !usernameRe.MatchString(username) {
			return nil, errUsernameInvalid
		}
		normalized := strings.ToLower(username)
		if reservedUsernames[normalized] {
			return nil, errUsernameInvalid
		}
	}

	// Check membership and admin rights before the store transaction.
	member, err := h.requireChannelMember(r.Ctx, channelID, r.UserID)
	if err != nil {
		return nil, err
	}
	if member.Role < 1 {
		return nil, errChatAdminRequired
	}

	if err := h.store.EditChannelUsername(r.Ctx, channelID, r.UserID, username); err != nil {
		switch {
		case errors.Is(err, store.ErrNotMember):
			// Store re-check failed (demotion/ban between handler check and
			// transaction). Same wire error as the handler-level check.
			return nil, errPeerIDInvalid
		case errors.Is(err, store.ErrUsernameOccupied):
			return nil, errUsernameOccupied
		case errors.Is(err, store.ErrUsernameFloodWait):
			return nil, errUsernameFloodWait
		default:
			h.log.Error("edit channel username", "channel_id", channelID, "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
	}
	return &tg.BoolTrue{}, nil
}
