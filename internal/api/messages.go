package api

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	defaultHistoryLimit = 20
	maxHistoryLimit     = 100

	defaultDialogsLimit = 20
	maxDialogsLimit     = 100
)

// notify emits the cross-replica update nudge for userID (best-effort).
func (h *handlers) notify(ctx context.Context, userID int64) {
	if err := h.store.Notify(ctx, store.ChannelUpdates, strconv.FormatInt(userID, 10)); err != nil {
		h.log.Error("notify updates", "user_id", userID, "err", err)
	}
}

// notifyTyping emits the transient typing nudge to peerID from fromID.
func (h *handlers) notifyTyping(ctx context.Context, peerID, fromID int64) {
	if err := h.store.Notify(ctx, store.ChannelTyping, store.TypingPayload(peerID, fromID)); err != nil {
		h.log.Error("notify typing", "peer_id", peerID, "err", err)
	}
}

// notifyEvict announces that authKeyID, bound to userID, has been revoked, so
// every replica closes the sockets still holding it. Emitted only after the
// delete has committed: a client whose socket is closed first reconnects with
// the same cached key, finds the row still there, re-registers, and the evict is
// spent.
func (h *handlers) notifyEvict(ctx context.Context, userID, authKeyID int64) {
	if err := h.store.Notify(ctx, store.ChannelEvict, store.EvictPayload(userID, authKeyID)); err != nil {
		h.log.Error("notify evict", "user_id", userID, "err", err)
	}
}

// notifyChannelPost emits the cross-replica nudge for a new post in channelID.
func (h *handlers) notifyChannelPost(ctx context.Context, channelID int64) {
	if err := h.store.Notify(ctx, store.ChannelPost, store.ChannelPostPayload(channelID)); err != nil {
		h.log.Error("notify channel post", "channel_id", channelID, "err", err)
	}
}

// notifyEncryptedMsg emits the cross-replica nudge for a new encrypted message
// for recipientID at qts. Emitted after commit.
func (h *handlers) notifyEncryptedMsg(ctx context.Context, recipientID int64, qts int) {
	if err := h.store.Notify(ctx, store.ChannelEncryptedMsg, store.EncryptedMsgPayload(recipientID, qts)); err != nil {
		h.log.Error("notify encrypted msg", "recipient_id", recipientID, "qts", qts, "err", err)
	}
}

// twoUsers hydrates the caller and the peer into the update user list.
func (h *handlers) twoUsers(ctx context.Context, selfID, peerID int64) ([]tg.UserClass, error) {
	return h.loadUsers(ctx, map[int64]bool{selfID: true, peerID: true}, selfID)
}

// loadFiles hydrates the files referenced by a batch of message rows into wire
// documents. Rows with no media, and files whose bytes were never stored, are
// simply absent from the map — messageToTL renders those as plain messages.
//
// The id list is derived from the caller's own rows and never from anything
// client-supplied: store.FilesByIDs checks no entitlement (the download gate
// lives in FileForDownload alone), so this list is the boundary on this path.
//
// A batch with no media skips the query and returns an empty map, so no call
// site needs a nil check or a branch of its own.
func (h *handlers) loadFiles(ctx context.Context, msgs []store.Message) (map[int64]*tg.Document, error) {
	var ids []int64
	for _, m := range msgs {
		if m.FileID != 0 {
			ids = append(ids, m.FileID)
		}
	}
	return h.fileDocs(ctx, ids)
}

// loadChannelFiles is loadFiles for channel posts. It is a separate collector
// for the sentinel alone: channel_messages.file_id is a nullable column, where
// messages.file_id uses 0 for "no media".
func (h *handlers) loadChannelFiles(ctx context.Context, msgs []store.ChannelMessage) (map[int64]*tg.Document, error) {
	var ids []int64
	for _, m := range msgs {
		if m.FileID != nil {
			ids = append(ids, *m.FileID)
		}
	}
	return h.fileDocs(ctx, ids)
}

// fileDocs hydrates file ids into wire documents. See loadFiles for why the id
// list may only ever be derived from the caller's own rows.
func (h *handlers) fileDocs(ctx context.Context, ids []int64) (map[int64]*tg.Document, error) {
	if len(ids) == 0 {
		return map[int64]*tg.Document{}, nil
	}
	files, err := h.store.FilesByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	docs := make(map[int64]*tg.Document, len(files))
	for id, f := range files {
		docs[id] = h.documentToTL(f)
	}
	return docs, nil
}

// validText rejects client text Postgres cannot store: a NUL byte or an invalid
// UTF-8 sequence. Both reach the driver intact and fail the INSERT, turning a
// client bug into a 500 and a log line.
func validText(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// handleSendMessage serves messages.sendMessage: it persists both sides, nudges
// both users' sessions, and returns the sender-side Updates (updateMessageID +
// updateNewMessage).
func (h *handlers) handleSendMessage(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesSendMessageRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	// Before the peer split, so the chat fan-out is guarded by the same check.
	if !validText(req.Message) {
		return nil, errMessageEmpty
	}
	peerType, toID, err := h.inputPeer(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}
	if peerType == store.PeerTypeChannel {
		return h.sendChannelMessage(r, toID, &req)
	}
	replyToMsgID := int64(0)
	if replyTo, ok := req.GetReplyTo(); ok {
		if rep, ok := replyTo.(*tg.InputReplyToMessage); ok && rep.ReplyToMsgID > 0 {
			replyToMsgID = int64(rep.ReplyToMsgID)
		}
	}

	if peerType == store.PeerTypeChat {
		return h.sendChatMessage(r, toID, &req, replyToMsgID)
	}

	sender, senderPts, _, dup, err := h.store.SendMessage(r.Ctx, r.UserID, toID, req.Message, req.RandomID, 0, replyToMsgID)
	if err != nil {
		h.log.Error("send message", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !dup {
		h.notify(r.Ctx, r.UserID)
		h.notify(r.Ctx, toID)
	}

	users, err := h.twoUsers(r.Ctx, r.UserID, toID)
	if err != nil {
		h.log.Error("send message users", "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: int(sender.LocalID), RandomID: req.RandomID},
			// sendMessage never carries media; the media send path builds its own reply.
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, nil, nil), Pts: senderPts, PtsCount: 1},
		},
		Users: users,
		Date:  int(sender.Date.Unix()),
	}, nil
}

// requireMember is the authorization boundary for a client-supplied chat id.
// An unknown chat and a chat the caller is not in report the identical error, so
// a caller cannot probe which ids exist over a dense BIGSERIAL id space. It takes
// no lock and writes nothing on the rejecting path.
//
// It is an early error, not the boundary that holds: it runs in a different
// transaction from the write that follows, so the store re-checks membership
// under the chats row lock.
func (h *handlers) requireMember(ctx context.Context, chatID, userID int64) error {
	member, err := h.store.IsMember(ctx, chatID, userID)
	if err != nil {
		h.log.Error("chat membership", "chat_id", chatID, "user_id", userID, "err", err)
		return errInternal
	}
	if !member {
		return errPeerIDInvalid
	}
	return nil
}

// sendChatMessage fans one message out to every member of chatID and returns the
// sender-side Updates. The reply is the 1:1 shape plus the chat itself.
func (h *handlers) sendChatMessage(r *mtproto.Request, chatID int64, req *tg.MessagesSendMessageRequest, replyToMsgID int64) (bin.Encoder, error) {
	if err := h.requireMember(r.Ctx, chatID, r.UserID); err != nil {
		return nil, err
	}

	sender, perOwner, dup, err := h.store.SendChatMessage(r.Ctx, store.FanOut{
		ChatID: chatID, FromID: r.UserID, Text: req.Message, RandomID: req.RandomID,
		ReplyToMsgID: replyToMsgID,
	})
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("send chat message", "user_id", r.UserID, "chat_id", chatID, "err", err)
		return nil, errInternal
	}
	if !dup {
		for uid := range perOwner {
			h.notify(r.Ctx, uid)
		}
	}

	recipients := make(map[int64]bool, len(perOwner))
	for uid := range perOwner {
		recipients[uid] = true
	}
	users, err := h.loadUsers(r.Ctx, recipients, r.UserID)
	if err != nil {
		h.log.Error("send chat message users", "err", err)
		return nil, errInternal
	}
	chats, err := h.loadChats(r.Ctx, map[int64]bool{chatID: true}, r.UserID)
	if err != nil {
		h.log.Error("send chat message chats", "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: int(sender.LocalID), RandomID: req.RandomID},
			// sendMessage never carries media; the media send path builds its own reply.
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, nil, nil), Pts: perOwner[r.UserID], PtsCount: 1},
		},
		Users: users,
		Chats: chats,
		Date:  int(sender.Date.Unix()),
	}, nil
}

// handleGetHistory serves messages.getHistory, paged newest-first by offset_id.
func (h *handlers) handleGetHistory(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetHistoryRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	peerType, toID, err := h.inputPeer(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}
	if peerType == store.PeerTypeChat {
		if err = h.requireMember(r.Ctx, toID, r.UserID); err != nil {
			return nil, err
		}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}

	// A channel keeps one row per post rather than one per member, so it has its
	// own read path and its own reply type; store.History reads the caller's own
	// message rows and has nothing to return for a channel peer.
	if peerType == store.PeerTypeChannel {
		if _, err = h.requireChannelMember(r.Ctx, toID, r.UserID); err != nil {
			return nil, err
		}
		return h.channelHistory(r, toID, &req, limit)
	}

	msgs, err := h.store.History(r.Ctx, r.UserID, peerType, toID, req.OffsetID, limit)
	if err != nil {
		h.log.Error("get history", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if peerType == store.PeerTypeChat {
		return h.chatHistory(r, toID, msgs)
	}

	files, err := h.loadFiles(r.Ctx, msgs)
	if err != nil {
		h.log.Error("get history files", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	tlMsgs := make([]tg.MessageClass, len(msgs))
	for i, m := range msgs {
		tlMsgs[i] = messageToTL(m, nil, files, nil)
	}
	users, err := h.twoUsers(r.Ctx, r.UserID, toID)
	if err != nil {
		h.log.Error("get history users", "err", err)
		return nil, errInternal
	}
	return &tg.MessagesMessages{Messages: tlMsgs, Users: users}, nil
}

// chatHistory renders one page of a chat's history for the caller, who
// requireMember has already established is a member.
func (h *handlers) chatHistory(r *mtproto.Request, chatID int64, msgs []store.Message) (bin.Encoder, error) {
	// A page has as many authors as the chat has members, and a service row names
	// further users in its action, so the user list is collected from the page
	// itself; twoUsers is a 1:1 helper and would omit every author but the caller.
	// createUsers is the chat's current member set, which the caller is entitled
	// to see here — unlike getDifference, no removed viewer reaches this path.
	var createUsers []int64
	for _, m := range msgs {
		if m.Action == store.ChatActionCreate {
			parts, perr := h.store.Participants(r.Ctx, chatID)
			if perr != nil {
				h.log.Error("get history participants", "chat_id", chatID, "err", perr)
				return nil, errInternal
			}
			createUsers = make([]int64, len(parts))
			for i, p := range parts {
				createUsers[i] = p.UserID
			}
			break
		}
	}

	files, err := h.loadFiles(r.Ctx, msgs)
	if err != nil {
		h.log.Error("get history files", "user_id", r.UserID, "chat_id", chatID, "err", err)
		return nil, errInternal
	}
	tlMsgs := make([]tg.MessageClass, len(msgs))
	authors := map[int64]bool{r.UserID: true}
	for i, m := range msgs {
		tlMsgs[i] = messageToTL(m, createUsers, files, nil)
		authors[m.FromID] = true
		switch m.Action {
		case store.ChatActionAddUser, store.ChatActionDeleteUser:
			authors[m.ActionUserID] = true
		case store.ChatActionCreate:
			for _, id := range createUsers {
				authors[id] = true
			}
		}
	}

	users, err := h.loadUsers(r.Ctx, authors, r.UserID)
	if err != nil {
		h.log.Error("get history users", "err", err)
		return nil, errInternal
	}
	chats, err := h.loadChats(r.Ctx, map[int64]bool{chatID: true}, r.UserID)
	if err != nil {
		h.log.Error("get history chats", "err", err)
		return nil, errInternal
	}
	return &tg.MessagesMessages{Messages: tlMsgs, Users: users, Chats: chats}, nil
}

// handleReadHistory serves messages.readHistory: advances read state on both
// sides, nudges both users, and returns the caller's affected pts.
func (h *handlers) handleReadHistory(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesReadHistoryRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	toID, err := h.peerUserID(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}

	readerPts, _, err := h.store.ReadHistory(r.Ctx, r.UserID, toID, int64(req.MaxID))
	if err != nil {
		h.log.Error("read history", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	h.notify(r.Ctx, r.UserID)
	h.notify(r.Ctx, toID)
	return &tg.MessagesAffectedMessages{Pts: readerPts, PtsCount: 1}, nil
}

// handleEditMessage serves messages.editMessage: edits both sides and returns
// the updateEditMessage envelope.
func (h *handlers) handleEditMessage(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesEditMessageRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if !validText(req.Message) {
		return nil, errMessageEmpty
	}

	peerID, newPts, err := h.store.EditMessage(r.Ctx, r.UserID, int64(req.ID), req.Message)
	if errors.Is(err, store.ErrMessageInvalid) {
		return nil, errMessageIDInvalid
	}
	if err != nil {
		h.log.Error("edit message", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	h.notify(r.Ctx, r.UserID)
	h.notify(r.Ctx, peerID)

	edited, ok, err := h.store.MessageByOwnerLocal(r.Ctx, r.UserID, int64(req.ID))
	if err != nil || !ok {
		h.log.Error("reload edited message", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	files, err := h.loadFiles(r.Ctx, []store.Message{edited})
	if err != nil {
		h.log.Error("edit message files", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	users, err := h.twoUsers(r.Ctx, r.UserID, peerID)
	if err != nil {
		h.log.Error("edit message users", "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateEditMessage{Message: messageToTL(edited, nil, files, nil), Pts: newPts, PtsCount: 1},
		},
		Users: users,
		Date:  int(time.Now().Unix()),
	}, nil
}

// handleDeleteMessages serves messages.deleteMessages: marks the caller's ids
// deleted on both sides, nudges every affected user, and returns affected pts.
func (h *handlers) handleDeleteMessages(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesDeleteMessagesRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	ids := make([]int64, len(req.ID))
	for i, v := range req.ID {
		ids[i] = int64(v)
	}
	perOwner, err := h.store.DeleteMessages(r.Ctx, r.UserID, ids)
	if errors.Is(err, store.ErrMessageInvalid) {
		return nil, errMessageIDInvalid
	}
	if err != nil {
		h.log.Error("delete messages", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}
	return &tg.MessagesAffectedMessages{Pts: perOwner[r.UserID], PtsCount: len(req.ID)}, nil
}

// handleSetTyping serves messages.setTyping: it emits a transient typing nudge
// to the peer and returns true. Typing is never persisted.
func (h *handlers) handleSetTyping(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesSetTypingRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	toID, err := h.peerUserID(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}
	h.notifyTyping(r.Ctx, toID, r.UserID)
	return &tg.BoolTrue{}, nil
}

// handleForwardMessages serves messages.forwardMessages: forwards one or more
// messages the caller owns to a 1:1 peer or a group chat. Each forwarded message
// is a new message row with FwdFrom populated.
func (h *handlers) handleForwardMessages(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesForwardMessagesRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if len(req.ID) == 0 || len(req.RandomID) != len(req.ID) {
		return nil, errPeerIDInvalid
	}

	// Resolve destination peer.
	destPeerType, destPeerID, err := h.inputPeer(req.ToPeer, r.UserID)
	if err != nil {
		return nil, err
	}
	// Secret chats and channels are not supported destinations.
	if destPeerType == store.PeerTypeChannel {
		return nil, errPeerIDInvalid
	}
	if destPeerType == store.PeerTypeChat {
		if err = h.requireMember(r.Ctx, destPeerID, r.UserID); err != nil {
			return nil, err
		}
	}

	// Resolve source peer.
	srcPeerType, srcPeerID, err := h.inputPeer(req.FromPeer, r.UserID)
	if err != nil {
		return nil, err
	}
	// Secret chats are not supported sources.
	if srcPeerType == store.PeerTypeChannel {
		// Channel source: resolve each message from the channel.
		if _, err = h.requireChannelMember(r.Ctx, srcPeerID, r.UserID); err != nil {
			return nil, err
		}
		localIDs := make([]int64, len(req.ID))
		for i, v := range req.ID {
			localIDs[i] = int64(v)
		}
		chMsgs, err := h.store.ChannelMessages(r.Ctx, srcPeerID, localIDs)
		if err != nil {
			h.log.Error("forward channel messages", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		sources := make([]store.ForwardSource, 0, len(req.ID))
		for _, id := range req.ID {
			m, ok := chMsgs[int64(id)]
			if !ok || m.Deleted {
				return nil, errMessageIDInvalid
			}
			fileID := int64(0)
			if m.FileID != nil {
				fileID = *m.FileID
			}
			post := int32(m.LocalID) //nolint:gosec // G115: local_id fits int32
			sources = append(sources, store.ForwardSource{
				FromID:      m.FromID,
				Date:        m.Date,
				Text:        m.Message,
				ChannelID:   m.ChannelID,
				ChannelPost: post,
				FileID:      fileID,
			})
		}
		randomIDs := make([]int64, len(req.RandomID))
		copy(randomIDs, req.RandomID)
		perOwner, sentIDs, err := h.store.ForwardMessages(r.Ctx, r.UserID, destPeerType, destPeerID, sources, randomIDs)
		if errors.Is(err, store.ErrMessageInvalid) {
			return nil, errMessageIDInvalid
		}
		if errors.Is(err, store.ErrNotMember) {
			return nil, errPeerIDInvalid
		}
		if err != nil {
			h.log.Error("forward messages", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		return h.forwardReply(r, destPeerType, destPeerID, perOwner, sentIDs)
	}

	// User or chat source: resolve each message from the messages table.
	localIDs := make([]int64, len(req.ID))
	for i, v := range req.ID {
		localIDs[i] = int64(v)
	}

	// Validate ownership of each source message.
	sources := make([]store.ForwardSource, 0, len(req.ID))
	for _, id := range localIDs {
		var m store.Message
		var ok bool
		if srcPeerType == store.PeerTypeChat {
			m, ok, err = h.store.MessageByOwnerLocal(r.Ctx, r.UserID, id)
			if err != nil {
				h.log.Error("forward lookup chat source", "user_id", r.UserID, "err", err)
				return nil, errInternal
			}
			if !ok || m.Deleted {
				return nil, errMessageIDInvalid
			}
			// Must be the sender or a member (already checked).
			// Chat messages have from_id and are stored per-member.
		} else {
			// 1:1 source: caller must be sender or recipient.
			m, ok, err = h.store.MessageByOwnerLocal(r.Ctx, r.UserID, id)
			if err != nil {
				h.log.Error("forward lookup source", "user_id", r.UserID, "err", err)
				return nil, errInternal
			}
			if !ok || m.Deleted {
				return nil, errMessageIDInvalid
			}
		}
		sources = append(sources, store.ForwardSource{
			FromID: m.FromID,
			Date:   m.Date,
			Text:   m.Text,
			FileID: m.FileID,
		})
	}

	randomIDs := make([]int64, len(req.RandomID))
	copy(randomIDs, req.RandomID)
	perOwner, sentIDs, err := h.store.ForwardMessages(r.Ctx, r.UserID, destPeerType, destPeerID, sources, randomIDs)
	if errors.Is(err, store.ErrMessageInvalid) {
		return nil, errMessageIDInvalid
	}
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("forward messages", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return h.forwardReply(r, destPeerType, destPeerID, perOwner, sentIDs)
}

// forwardReply builds the UpdatesClass reply for a forward.
func (h *handlers) forwardReply(r *mtproto.Request, destPeerType store.PeerType, destPeerID int64, perOwner map[int64]int, sentIDs []int64) (bin.Encoder, error) {
	// Notify all affected owners.
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}

	return &tg.MessagesAffectedMessages{
		Pts:      perOwner[r.UserID],
		PtsCount: len(sentIDs),
	}, nil
}
