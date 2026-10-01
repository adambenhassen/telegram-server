package api

import (
	"context"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	// maxChatTitle bounds the title a client may set. chats.title is an unbounded
	// TEXT column and every rename copies the title into one messages row per
	// member, so an unbounded title is a 200x write amplifier. The store
	// deliberately does not validate, so the bound belongs here.
	maxChatTitle = 255
	// maxChatUsers caps the invite vector before the store sees it. The store
	// bounds its own allocation and returns ErrChatFull regardless, so this is
	// defence in depth — but a Vector<InputUser> can carry ~1.3M ids inside
	// gotd's 16 MB frame, and rejecting here spares even the per-id lookups.
	maxChatUsers = 200
	// maxGetChatIDs bounds getChats' member-selected participant-count read to
	// 100 chats per request.
	maxGetChatIDs = 100
)

// chatTitle validates a client-supplied chat title and returns the trimmed form
// that gets stored. Empty, whitespace-only, over-length and text the server
// cannot store are one error: the client is not owed a distinction it cannot act
// on differently.
func chatTitle(raw string) (string, error) {
	title := strings.TrimSpace(raw)
	if title == "" || !validText(title) || utf8.RuneCountInString(title) > maxChatTitle {
		return "", errChatTitleInvalid
	}
	return title, nil
}

// resolveInvitees maps the request's input users to ids, splitting them into
// members to create the chat with and invitees that do not exist. A user id with
// no users row is reported as missing rather than failing the call — Telegram's
// own behaviour — but a malformed input peer fails the whole call, since it is a
// client bug rather than a fact about an account.
func (h *handlers) resolveInvitees(ctx context.Context, users []tg.InputUserClass, selfID int64) (members []int64, missing []tg.MissingInvitee, err error) {
	for _, u := range users {
		id, err := h.inputUserID(u, selfID)
		if err != nil {
			return nil, nil, err
		}
		_, ok, err := h.store.UserByID(ctx, id)
		if err != nil {
			return nil, nil, err
		}
		if !ok {
			missing = append(missing, tg.MissingInvitee{UserID: id})
			continue
		}
		members = append(members, id)
	}
	return members, missing, nil
}

// addMissingCreatedChatInvitees reports valid invitees that the store filtered
// before inserting the chat. The participant read is after CreateChat commits,
// so the response reflects the durable membership set and keeps blocked users
// out of both the chat and its success vector. The existing MissingInvitee
// shape intentionally remains: a creator with a previously resolved invitee
// can infer a block from this result, which is an accepted create-time residual.
func (h *handlers) addMissingCreatedChatInvitees(ctx context.Context, chatID int64, memberIDs []int64, missing []tg.MissingInvitee) ([]tg.MissingInvitee, error) {
	participants, err := h.store.Participants(ctx, chatID)
	if err != nil {
		return nil, err
	}
	present := make(map[int64]bool, len(participants))
	for _, participant := range participants {
		present[participant.UserID] = true
	}
	reported := make(map[int64]bool, len(missing))
	for _, invitee := range missing {
		reported[invitee.UserID] = true
	}
	for _, id := range memberIDs {
		if !present[id] && !reported[id] {
			missing = append(missing, tg.MissingInvitee{UserID: id})
			reported[id] = true
		}
	}
	return missing, nil
}

// chatUpdate builds the envelope a chat mutation returns to its caller: the
// caller's own copy of the service message, at the caller's own new pts, plus
// the users and the chat the client needs to render it.
//
// perOwner carries every member's new pts and is server-side notification state
// only — never echoed to the caller. Only perOwner[callerID] leaves this
// function, and its key set is used as the member set.
func (h *handlers) chatUpdate(ctx context.Context, callerID int64, chat store.Chat, sender store.Message, perOwner map[int64]int, createUsers []int64) (*tg.Updates, error) {
	ids := make(map[int64]bool, len(perOwner))
	for uid := range perOwner {
		ids[uid] = true
	}
	users, err := h.loadUsers(ctx, ids, callerID)
	if err != nil {
		return nil, err
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				// A service message never carries media.
				Message:  messageToTL(sender, createUsers, nil, nil, nil),
				Pts:      perOwner[callerID],
				PtsCount: 1,
			},
		},
		Users: users,
		Chats: []tg.ChatClass{chatToTL(chat, len(perOwner), callerID)},
		Date:  int(sender.Date.Unix()),
	}, nil
}

// memberIDs returns the fan-out's owners in ascending order, matching the order
// store.Participants reads them so the create action's user list is identical
// whether a client sees it here or replays it through getDifference.
func memberIDs(perOwner map[int64]int) []int64 {
	ids := make([]int64, 0, len(perOwner))
	for uid := range perOwner {
		ids = append(ids, uid)
	}
	slices.Sort(ids)
	return ids
}

// handleCreateChat serves messages.createChat: it creates the chat, announces it
// to every member with a service message, and returns the caller's own update.
//
// The chat and its announcement are two transactions, which is deliberate and is
// the one place a chat mutation is allowed to split them. A half-completed add,
// removal or rename is harmful because the member set and what members were told
// disagree; a chat whose create announcement failed is in nobody's dialog list,
// creator included, so it fails safe. Nothing unwinds the chat row.
func (h *handlers) handleCreateChat(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesCreateChatRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	// Rate limit before any write.
	if err := h.checkRateLimit(r, "create_chat", h.rateLimitCreateChat); err != nil {
		return nil, err
	}
	title, err := chatTitle(req.Title)
	if err != nil {
		return nil, err
	}
	if len(req.Users) > maxChatUsers {
		return nil, errUsersTooMuch
	}
	members, missing, err := h.resolveInvitees(r.Ctx, req.Users, r.UserID)
	if errors.Is(err, errPeerIDInvalid) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("create chat: resolve invitees", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	chat, err := h.store.CreateChat(r.Ctx, r.UserID, title, members)
	if errors.Is(err, store.ErrChatFull) {
		return nil, errUsersTooMuch
	}
	if err != nil {
		h.log.Error("create chat", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	missing, err = h.addMissingCreatedChatInvitees(r.Ctx, chat.ID, members, missing)
	if err != nil {
		h.log.Error("create chat: inspect participants", "chat_id", chat.ID, "err", err)
		return nil, errInternal
	}

	sender, perOwner, _, err := h.store.SendChatMessage(r.Ctx, store.FanOut{
		ChatID: chat.ID, FromID: r.UserID, Text: title, Action: store.ChatActionCreate,
	})
	if err != nil {
		h.log.Error("create chat announce", "chat_id", chat.ID, "err", err)
		return nil, errInternal
	}
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}

	ups, err := h.chatUpdate(r.Ctx, r.UserID, chat, sender, perOwner, memberIDs(perOwner))
	if err != nil {
		h.log.Error("create chat updates", "chat_id", chat.ID, "err", err)
		return nil, errInternal
	}
	// TTLPeriod is accepted and ignored: M6 stores no per-chat message TTL.
	return &tg.MessagesInvitedUsers{Updates: ups, MissingInvitees: missing}, nil
}

// handleEditChatTitle serves messages.editChatTitle: one store call renames the
// chat and announces the rename atomically.
//
// There is no authorization check here on purpose. The store re-checks the
// caller's membership and creator authority inside its own transaction under the
// chats row lock, and that is the authorization boundary; a check here would run
// in a different transaction and the gap between the two is exactly what an
// attacker removed from the chat mid-call would ride. store.ErrNotMember covers
// both "not a member" and "no such chat", as well as denied creator authority,
// and all must stay one wire error so chat ids are not enumerable.
func (h *handlers) handleEditChatTitle(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesEditChatTitleRequest
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

	chat, sender, perOwner, err := h.store.SetChatTitle(r.Ctx, req.ChatID, r.UserID, title)
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("edit chat title", "chat_id", req.ChatID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}

	ups, err := h.chatUpdate(r.Ctx, r.UserID, chat, sender, perOwner, nil)
	if err != nil {
		h.log.Error("edit chat title updates", "chat_id", chat.ID, "err", err)
		return nil, errInternal
	}
	return ups, nil
}

// handleEditChatDefaultBannedRights saves basic-group or megagroup default
// restrictions. The store re-checks current membership and authority under the
// peer row lock before it compares values or writes. Rights updates have no
// message event or owner pts, so the reply carries Telegram's peer-version
// update alone.
func (h *handlers) handleEditChatDefaultBannedRights(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesEditChatDefaultBannedRightsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	var (
		chatID      int64
		channelPeer *tg.InputPeerChannel
	)
	switch peer := req.Peer.(type) {
	case *tg.InputPeerChat:
		if peer.ChatID <= 0 {
			return nil, errPeerIDInvalid
		}
		chatID = peer.ChatID
	case *tg.InputPeerChannel:
		if peer.ChannelID <= 0 {
			return nil, errPeerIDInvalid
		}
		channelPeer = peer
	default:
		return nil, errPeerIDInvalid
	}
	rights, err := chatDefaultBannedRightsFromTL(req.BannedRights)
	if err != nil {
		return nil, err
	}

	if channelPeer != nil {
		peerType, channelID, err := h.inputPeer(channelPeer, r.UserID)
		if err != nil {
			return nil, err
		}
		if peerType != store.PeerTypeChannel {
			return nil, errPeerIDInvalid
		}
		channel, changed, err := h.store.SetChannelDefaultBannedRights(r.Ctx, channelID, r.UserID, rights)
		if errors.Is(err, store.ErrNotMember) {
			return nil, errPeerIDInvalid
		}
		if err != nil {
			h.log.Error("edit channel default banned rights", "channel_id", channelID, "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		if !changed {
			return nil, errChatNotModified
		}
		return &tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateChatDefaultBannedRights{
				Peer:                &tg.PeerChannel{ChannelID: channel.ID},
				DefaultBannedRights: chatDefaultBannedRightsToTL(channel.DefaultBannedRights),
				Version:             channel.Version,
			}},
			Date: int(h.now().Unix()),
		}, nil
	}

	chat, changed, err := h.store.SetChatDefaultBannedRights(r.Ctx, chatID, r.UserID, rights)
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("edit chat default banned rights", "chat_id", chatID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !changed {
		return nil, errChatNotModified
	}

	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateChatDefaultBannedRights{
			Peer:                &tg.PeerChat{ChatID: chat.ID},
			DefaultBannedRights: chatDefaultBannedRightsToTL(chat.DefaultBannedRights),
			Version:             chat.Version,
		}},
		Date: int(h.now().Unix()),
	}, nil
}

func (h *handlers) handleGetFullChat(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetFullChatRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if req.ChatID <= 0 {
		return nil, errPeerIDInvalid
	}

	snapshot, err := h.store.ChatInfoForMemberSnapshot(r.Ctx, r.UserID, []int64{req.ChatID})
	if err != nil {
		h.log.Error("get full chat", "chat_id", req.ChatID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	chat, ok := snapshot.Chats[req.ChatID]
	if !ok {
		return nil, errPeerIDInvalid
	}
	participants := snapshot.Participants[req.ChatID]
	wireFull := &tg.ChatFull{
		ID:             chat.ID,
		About:          "",
		Participants:   chatParticipantsToTL(chat, participants),
		NotifySettings: tg.PeerNotifySettings{},
	}
	wireFull.SetChatPhoto(&tg.PhotoEmpty{})
	pinnedMessageID, hasPinnedMessage, err := h.store.ChatPinnedMessageForOwner(r.Ctx, chat.ID, r.UserID)
	if err != nil {
		h.log.Error("get full chat pin", "chat_id", chat.ID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if hasPinnedMessage {
		wireFull.SetPinnedMsgID(int(pinnedMessageID))
	}
	users, err := h.chatInfoUsers(r.Ctx, snapshot, r.UserID)
	if err != nil {
		h.log.Error("get full chat users", "chat_id", req.ChatID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	return &tg.MessagesChatFull{
		FullChat: wireFull,
		Chats:    []tg.ChatClass{chatToTL(chat, len(participants), r.UserID)},
		Users:    users,
	}, nil
}

func (h *handlers) handleGetChats(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetChatsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if len(req.ID) > maxGetChatIDs {
		return nil, errLimitInvalid
	}

	chatIDs := make([]int64, 0, len(req.ID))
	seen := make(map[int64]bool, len(req.ID))
	for _, id := range req.ID {
		if id <= 0 || seen[id] {
			continue
		}
		seen[id] = true
		chatIDs = append(chatIDs, id)
	}

	snapshot, err := h.store.ChatListInfoForMemberSnapshot(r.Ctx, r.UserID, chatIDs)
	if err != nil {
		h.log.Error("get chats", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	chats := make([]tg.ChatClass, 0, len(chatIDs))
	for _, id := range chatIDs {
		chat, ok := snapshot.Chats[id]
		if !ok {
			chats = append(chats, &tg.ChatForbidden{ID: id, Title: ""})
			continue
		}
		chats = append(chats, chatToTL(chat, int(snapshot.ParticipantCounts[id]), r.UserID))
	}
	return &tg.MessagesChats{Chats: chats}, nil
}

func chatParticipantsToTL(chat store.Chat, participants []store.Participant) *tg.ChatParticipants {
	wireParticipants := make([]tg.ChatParticipantClass, 0, len(participants))
	for _, participant := range participants {
		if participant.UserID == chat.CreatorID {
			wireParticipants = append(wireParticipants, &tg.ChatParticipantCreator{UserID: participant.UserID})
			continue
		}
		wireParticipants = append(wireParticipants, &tg.ChatParticipant{
			UserID:    participant.UserID,
			InviterID: participant.InviterID,
			Date:      int(participant.Date.Unix()),
		})
	}
	return &tg.ChatParticipants{ChatID: chat.ID, Participants: wireParticipants, Version: chat.Version}
}

func (h *handlers) chatInfoUsers(ctx context.Context, snapshot store.ChatInfoSnapshot, viewerID int64) ([]tg.UserClass, error) {
	ids := make([]int64, 0, len(snapshot.Users))
	for id := range snapshot.Users {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	contactIDs := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id == viewerID || snapshot.EntitledUsers[id] {
			contactIDs = append(contactIDs, id)
		}
	}
	contactStates, err := h.contactStatesForUsers(ctx, viewerID, contactIDs)
	if err != nil {
		return nil, err
	}
	users := make([]tg.UserClass, 0, len(ids))
	for _, id := range ids {
		user := snapshot.Users[id]
		if id != viewerID && !snapshot.EntitledUsers[id] {
			users = append(users, &tg.UserEmpty{ID: id})
			continue
		}
		users = append(users, h.userToTL(user, viewerID, id == viewerID, contactStates[id]))
	}
	return users, nil
}
