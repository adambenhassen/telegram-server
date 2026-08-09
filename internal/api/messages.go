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

	// maxSearchQueryLen bounds the query string in messages.search. A query past
	// this cap is rejected before reaching the database, following the same
	// precedent as oversized message payloads (errMessageTooLong). 500 matches
	// Telegram's own cap on fulltext search terms.
	maxSearchQueryLen = 500
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
	// Validate text and resolve peer before any write.
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

	// Check for a transport retry (already-stored random_id) before the rate
	// limit so that a resend returns the stored message, never FLOOD_WAIT.
	if req.RandomID != 0 {
		if existing, ok, err := h.store.MessageByRandomID(r.Ctx, r.UserID, req.RandomID); err == nil && ok {
			// Retry of a delivered message: return the sender's row with
			// the current pts without writing or rate-limiting.
			senderState, err := h.store.State(r.Ctx, r.UserID)
			if err != nil {
				h.log.Error("read sender pts on retry", "user_id", r.UserID, "err", err)
				return nil, errInternal
			}
			users, err := h.twoUsers(r.Ctx, r.UserID, toID)
			if err != nil {
				h.log.Error("load users on retry", "user_id", r.UserID, "err", err)
				return nil, errInternal
			}
			return &tg.Updates{
				Updates: []tg.UpdateClass{
					&tg.UpdateMessageID{ID: int(existing.LocalID), RandomID: req.RandomID},
					&tg.UpdateNewMessage{Message: messageToTL(existing, nil, nil, nil, nil), Pts: senderState.Pts, PtsCount: 1},
				},
				Users: users,
				Date:  int(existing.Date.Unix()),
			}, nil
		} else if err != nil {
			h.log.Error("random_id lookup", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
	}

	// Rate limit: new message, consume a token from the shared send budget.
	if err := h.checkRateLimit(r, "message_send", h.rateLimitMessageSend); err != nil {
		return nil, err
	}

	sender, senderPts, _, _, err := h.store.SendMessage(r.Ctx, r.UserID, toID, req.Message, req.RandomID, 0, replyToMsgID)
	if err != nil {
		h.log.Error("send message", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	h.notify(r.Ctx, r.UserID)
	h.notify(r.Ctx, toID)

	users, err := h.twoUsers(r.Ctx, r.UserID, toID)
	if err != nil {
		h.log.Error("send message users", "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: int(sender.LocalID), RandomID: req.RandomID},
			// sendMessage never carries media; the media send path builds its own reply.
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, nil, nil, nil), Pts: senderPts, PtsCount: 1},
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

	// Check for a transport retry before the rate limit.
	if req.RandomID != 0 {
		if existing, ok, err := h.store.MessageByRandomID(r.Ctx, r.UserID, req.RandomID); err == nil && ok {
			// Retry: return the stored message without rate-limiting.
			senderState, err := h.store.State(r.Ctx, r.UserID)
			if err != nil {
				h.log.Error("read sender pts on retry", "user_id", r.UserID, "err", err)
				return nil, errInternal
			}
			chats, err := h.loadChats(r.Ctx, map[int64]bool{chatID: true}, r.UserID)
			if err != nil {
				h.log.Error("load chats on retry", "err", err)
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
					&tg.UpdateNewMessage{Message: messageToTL(existing, nil, nil, nil, nil), Pts: senderState.Pts, PtsCount: 1},
				},
				Users: users,
				Chats: chats,
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

	sender, perOwner, _, err := h.store.SendChatMessage(r.Ctx, store.FanOut{
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
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
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
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, nil, nil, nil), Pts: perOwner[r.UserID], PtsCount: 1},
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
	// Load reactions for each message.
	reactionsByMsg := make(map[int64][]store.Reaction, len(msgs))
	for _, m := range msgs {
		reactions, rerr := h.store.ReactionsByOwnerLocal(r.Ctx, r.UserID, m.LocalID)
		if rerr != nil {
			h.log.Error("get history reactions", "user_id", r.UserID, "local_id", m.LocalID, "err", rerr)
			return nil, errInternal
		}
		if len(reactions) > 0 {
			reactionsByMsg[m.LocalID] = reactions
		}
	}
	tlMsgs := make([]tg.MessageClass, len(msgs))
	for i, m := range msgs {
		tlMsgs[i] = messageToTL(m, nil, files, nil, reactionsByMsg[m.LocalID])
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
	// Load reactions for each message.
	reactionsByMsg := make(map[int64][]store.Reaction, len(msgs))
	for _, m := range msgs {
		reactions, rerr := h.store.ReactionsByOwnerLocal(r.Ctx, r.UserID, m.LocalID)
		if rerr != nil {
			h.log.Error("get history reactions", "user_id", r.UserID, "local_id", m.LocalID, "err", rerr)
			return nil, errInternal
		}
		if len(reactions) > 0 {
			reactionsByMsg[m.LocalID] = reactions
		}
	}
	tlMsgs := make([]tg.MessageClass, len(msgs))
	authors := map[int64]bool{r.UserID: true}
	for i, m := range msgs {
		tlMsgs[i] = messageToTL(m, createUsers, files, nil, reactionsByMsg[m.LocalID])
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
			&tg.UpdateEditMessage{Message: messageToTL(edited, nil, files, nil, nil), Pts: newPts, PtsCount: 1},
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

	// Check for a full retry: if every random_id is already stored, return
	// the forwarded messages without consuming a token.
	allDup := true
	var dupMsgs []store.Message
	for _, rid := range req.RandomID {
		if rid == 0 {
			allDup = false
			break
		}
		existing, ok, err := h.store.MessageByRandomID(r.Ctx, r.UserID, rid)
		if err != nil {
			h.log.Error("random_id lookup", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		if !ok {
			allDup = false
			break
		}
		dupMsgs = append(dupMsgs, existing)
	}
	if allDup {
		// Full retry: return stored messages without rate-limiting.
		senderState, err := h.store.State(r.Ctx, r.UserID)
		if err != nil {
			h.log.Error("read sender pts on retry", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		perOwner := make(map[int64]int)
		sentMsgs := make([]store.ForwardedMessage, 0, len(dupMsgs))
		for _, m := range dupMsgs {
			sentMsgs = append(sentMsgs, store.ForwardedMessage{Message: m, Pts: senderState.Pts})
			if m.PeerType == store.PeerTypeChat {
				perOwner[m.OwnerID] = senderState.Pts
			}
		}
		destPeerType, destPeerID, err := h.inputPeer(req.ToPeer, r.UserID)
		if err != nil {
			return nil, err
		}
		return h.forwardReply(r, destPeerType, destPeerID, perOwner, sentMsgs, req.RandomID)
	}

	// Rate limit: at least one new forward, consume a token.
	if err := h.checkRateLimit(r, "message_send", h.rateLimitMessageSend); err != nil {
		return nil, err
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
		perOwner, sentMsgs, err := h.store.ForwardMessages(r.Ctx, r.UserID, destPeerType, destPeerID, sources, randomIDs)
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
		return h.forwardReply(r, destPeerType, destPeerID, perOwner, sentMsgs, randomIDs)
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
		m, ok, err = h.store.MessageByOwnerLocal(r.Ctx, r.UserID, id)
		if err != nil {
			h.log.Error("forward lookup source", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		if !ok || m.Deleted {
			return nil, errPeerIDInvalid
		}
		// The row must belong to the dialog the caller named in FromPeer.
		if m.PeerType != srcPeerType || m.PeerID != srcPeerID {
			return nil, errPeerIDInvalid
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
	perOwner, sentMsgs, err := h.store.ForwardMessages(r.Ctx, r.UserID, destPeerType, destPeerID, sources, randomIDs)
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
	return h.forwardReply(r, destPeerType, destPeerID, perOwner, sentMsgs, randomIDs)
}

// handleSendReaction serves messages.sendReaction: it records the caller's
// reaction (or clears it) and pushes updateMessageReactions to all parties.
func (h *handlers) handleSendReaction(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesSendReactionRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	peerType, peerID, err := h.inputPeer(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}
	// Reactions on channel messages are out of scope.
	if peerType == store.PeerTypeChannel {
		return nil, errPeerIDInvalid
	}
	if peerType == store.PeerTypeChat {
		if err = h.requireMember(r.Ctx, peerID, r.UserID); err != nil {
			return nil, err
		}
	}

	localID := int64(req.MsgID)
	reactions, hasReactions := req.GetReaction()

	var affected []store.ReactionTarget
	if !hasReactions || len(reactions) == 0 {
		// Clear reaction.
		affected, err = h.store.ClearReaction(r.Ctx, r.UserID, localID)
	} else {
		// Set reaction — use the first (and only) reaction emoji.
		var reactionStr string
		if emoji, ok := reactions[0].(*tg.ReactionEmoji); ok {
			reactionStr = emoji.Emoticon
		}
		if reactionStr == "" {
			return nil, errPeerIDInvalid
		}
		affected, err = h.store.SendReaction(r.Ctx, r.UserID, localID, reactionStr)
	}
	if errors.Is(err, store.ErrMessageInvalid) {
		return nil, errMessageIDInvalid
	}
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("send reaction", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	// Notify all affected message copies (transient push, no pts).
	for _, t := range affected {
		h.notifyReaction(r.Ctx, t.OwnerID, t.LocalID)
	}

	// messages.sendReaction returns Updates per the Telegram schema.
	return &tg.Updates{Date: int(time.Now().Unix())}, nil
}

// notifyReaction emits the cross-replica reaction nudge for userID (best-effort).
// ownerID is the owner of the message copy being pushed to; localID is that copy's
// local message id.
func (h *handlers) notifyReaction(ctx context.Context, userID, localID int64) {
	if err := h.store.Notify(ctx, store.ChannelReactions, store.ReactionPayload(userID, localID, userID)); err != nil {
		h.log.Error("notify reaction", "user_id", userID, "err", err)
	}
}

// forwardReply builds the UpdatesClass reply for a forward.
func (h *handlers) forwardReply(r *mtproto.Request, destPeerType store.PeerType, destPeerID int64, perOwner map[int64]int, sentMsgs []store.ForwardedMessage, randomIDs []int64) (bin.Encoder, error) {
	// Notify all affected owners.
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}

	// Collect user, chat and channel references from forwarded messages.
	userRefs := make(map[int64]bool)
	basicChatRefs := make(map[int64]bool)
	channelRefs := make(map[int64]bool)
	for _, fm := range sentMsgs {
		m := fm.Message
		if m.PeerType == store.PeerTypeChat {
			basicChatRefs[m.PeerID] = true
		} else {
			userRefs[m.PeerID] = true
		}
		userRefs[m.FromID] = true
		if m.FwdFromID != 0 {
			userRefs[m.FwdFromID] = true
		}
		if m.FwdChannelID != 0 {
			channelRefs[m.FwdChannelID] = true
		}
	}

	var users []tg.UserClass
	var chats []tg.ChatClass
	var err error
	if destPeerType == store.PeerTypeUser {
		users, err = h.twoUsers(r.Ctx, r.UserID, destPeerID)
		userRefs[r.UserID] = true
		userRefs[destPeerID] = true
	} else {
		recipients := make(map[int64]bool, len(perOwner))
		for uid := range perOwner {
			recipients[uid] = true
			userRefs[uid] = true
		}
		basicChatRefs[destPeerID] = true
		users, err = h.loadUsers(r.Ctx, recipients, r.UserID)
		if err == nil {
			chats, err = h.loadChats(r.Ctx, map[int64]bool{destPeerID: true}, r.UserID)
		}
	}
	// Load extra user references from fwd heads that the send/load did not cover.
	for uid := range userRefs {
		if uid != r.UserID {
			u, ok, uerr := h.store.UserByID(r.Ctx, uid)
			if uerr != nil {
				h.log.Error("forward reply fwd user", "err", uerr)
				return nil, errInternal
			}
			if ok {
				users = append(users, h.userToTL(u, r.UserID, uid == r.UserID))
			}
		}
	}
	// Load basic chat references.
	for chid := range basicChatRefs {
		if _, loaded := basicChatRefs[chid]; !loaded {
			continue
		}
		c, ok, cerr := h.store.ChatByID(r.Ctx, chid)
		if cerr != nil {
			h.log.Error("forward reply fwd chat", "err", cerr)
			return nil, errInternal
		}
		if ok {
			member, merr := h.store.IsMember(r.Ctx, chid, r.UserID)
			if merr != nil {
				h.log.Error("forward reply fwd chat member", "err", merr)
				return nil, errInternal
			}
			var count int
			if member {
				parts, perr := h.store.Participants(r.Ctx, chid)
				if perr != nil {
					h.log.Error("forward reply fwd chat participants", "err", perr)
					return nil, errInternal
				}
				count = len(parts)
			}
			chats = append(chats, chatToTL(c, count, r.UserID))
		}
	}
	// Load channel references with viewer-aware loader.
	if len(channelRefs) > 0 {
		channelTL, cerr := h.loadChannels(r.Ctx, channelRefs, r.UserID)
		if cerr != nil {
			h.log.Error("forward reply fwd channel", "err", cerr)
			return nil, errInternal
		}
		chats = append(chats, channelTL...)
	}
	if err != nil {
		h.log.Error("forward reply", "err", err)
		return nil, errInternal
	}

	// Load files for forwarded messages.
	msgs := make([]store.Message, len(sentMsgs))
	for i, fm := range sentMsgs {
		msgs[i] = fm.Message
	}
	files, err := h.loadFiles(r.Ctx, msgs)
	if err != nil {
		h.log.Error("forward reply files", "err", err)
		return nil, errInternal
	}

	updates := make([]tg.UpdateClass, 0, len(sentMsgs)*2)
	for i, fm := range sentMsgs {
		updates = append(updates,
			&tg.UpdateMessageID{ID: int(fm.Message.LocalID), RandomID: randomIDs[i]},
			&tg.UpdateNewMessage{Message: messageToTL(fm.Message, nil, files, nil, nil), Pts: fm.Pts, PtsCount: 1},
		)
	}
	date := time.Now().Unix()
	if len(sentMsgs) > 0 {
		date = sentMsgs[0].Message.Date.Unix()
	}
	return &tg.Updates{Updates: updates, Users: users, Chats: chats, Date: int(date)}, nil
}

// handleUpdatePinnedMessage serves messages.updatePinnedMessage: it pins or
// unpins a message in a group chat or channel, stores the pinned message id
// durably, and pushes updatePinnedMessages to all members.
//
// For chats: any member may pin (the chat is small and has no admin role). For
// channels: only an admin (role >= 1) may pin. Non-admin callers get
// CHAT_ADMIN_REQUIRED.
//
// Pinning the currently pinned message is idempotent: returns success without
// emitting a redundant push.
func (h *handlers) handleUpdatePinnedMessage(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesUpdatePinnedMessageRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	peerType, peerID, err := h.inputPeer(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}

	if peerType == store.PeerTypeChat {
		return h.pinChatMessage(r, peerID, &req)
	}
	if peerType == store.PeerTypeChannel {
		return h.pinChannelMessage(r, peerID, &req)
	}
	return nil, errPeerIDInvalid
}

// pinChatMessage pins or unpins a message in a group chat. Only the chat
// creator may pin or unpin.
func (h *handlers) pinChatMessage(r *mtproto.Request, chatID int64, req *tg.MessagesUpdatePinnedMessageRequest) (bin.Encoder, error) {
	// Check membership first.
	if err := h.requireMember(r.Ctx, chatID, r.UserID); err != nil {
		return nil, err
	}
	// Only the creator may pin — a group chat has no admin role, so creator is
	// the sole privileged member.
	ch, ok, err := h.store.ChatByID(r.Ctx, chatID)
	if err != nil {
		h.log.Error("pin chat lookup", "chat_id", chatID, "err", err)
		return nil, errInternal
	}
	if !ok || ch.CreatorID != r.UserID {
		return nil, errChatAdminRequired
	}

	msgID := int32(req.ID) //nolint:gosec // G115: message id fits int32 wire space
	var pinnedID *int32
	if req.Unpin || req.ID == 0 {
		// Both Unpin=true and ID=0 clear the pin.
		pinnedID = nil
	} else {
		pinnedID = &msgID
	}

	// Read current pinned state for idempotency check.
	// Note: this read occurs before the row lock in SetChatPinnedMessage, so
	// two concurrent identical requests can both see the old value and both
	// emit a push. This is accepted as a tolerable race — the transient push
	// model (same as reactions) means a duplicate push is harmless.
	currentPinned, err := h.store.ChatPinnedMessage(r.Ctx, chatID)
	if err != nil {
		h.log.Error("pin chat read", "chat_id", chatID, "err", err)
		return nil, errInternal
	}

	// Idempotency: pinning the same message that is already pinned returns
	// success without emitting a push.
	if pinnedID != nil && currentPinned != nil && *pinnedID == *currentPinned {
		return &tg.Updates{Date: int(time.Now().Unix())}, nil
	}
	// Idempotency: unpinning when nothing is pinned.
	if pinnedID == nil && currentPinned == nil {
		return &tg.Updates{Date: int(time.Now().Unix())}, nil
	}

	chat, _, err := h.store.SetChatPinnedMessage(r.Ctx, chatID, r.UserID, pinnedID)
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if errors.Is(err, store.ErrMessageInvalid) {
		return nil, errMessageIDInvalid
	}
	if err != nil {
		h.log.Error("pin chat", "chat_id", chatID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	// Emit the pinned notification so all members receive updatePinnedMessages.
	var pinnedMsgID int32
	if pinnedID != nil {
		pinnedMsgID = *pinnedID
	}
	h.notifyPinned(r.Ctx, store.PeerTypeChat, chatID, pinnedMsgID)

	chats, err := h.loadChats(r.Ctx, map[int64]bool{chat.ID: true}, r.UserID)
	if err != nil {
		h.log.Error("pin chat render", "chat_id", chat.ID, "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdatePinnedMessages{
				Pinned: pinnedID != nil,
				Peer:   &tg.PeerChat{ChatID: chat.ID},
				Messages: func() []int {
					if pinnedID != nil {
						return []int{int(*pinnedID)}
					}
					return nil
				}(),
			},
		},
		Chats: chats,
		Date:  int(time.Now().Unix()),
	}, nil
}

// pinChannelMessage pins or unpins a message in a channel. Only admins may pin.
//
// The handler-level role check is a cheap early filter. The authoritative check
// runs inside SetChannelPinnedMessage under the channel row lock, where it
// re-reads the caller's participant row and verifies role >= 1 and not banned.
func (h *handlers) pinChannelMessage(r *mtproto.Request, channelID int64, req *tg.MessagesUpdatePinnedMessageRequest) (bin.Encoder, error) {
	// Check membership and admin rights.
	member, err := h.requireChannelMember(r.Ctx, channelID, r.UserID)
	if err != nil {
		return nil, err
	}
	if member.Role < 1 {
		return nil, errChatAdminRequired
	}

	msgID := int32(req.ID) //nolint:gosec // G115: message id fits int32 wire space
	var pinnedID *int32
	if req.Unpin || req.ID == 0 {
		// Both Unpin=true and ID=0 clear the pin.
		pinnedID = nil
	} else {
		pinnedID = &msgID
	}

	// Read current pinned state for idempotency check.
	// Note: same TOCTOU caveat as pinChatMessage — concurrent identical
	// requests can both emit a push. Accepted as tolerable under the
	// transient-push model.
	currentPinned, err := h.store.ChannelPinnedMessage(r.Ctx, channelID)
	if err != nil {
		h.log.Error("pin channel read", "channel_id", channelID, "err", err)
		return nil, errInternal
	}

	// Idempotency: pinning the same message that is already pinned returns
	// success without emitting a push.
	if pinnedID != nil && currentPinned != nil && *pinnedID == *currentPinned {
		return &tg.Updates{Date: int(time.Now().Unix())}, nil
	}
	// Idempotency: unpinning when nothing is pinned.
	if pinnedID == nil && currentPinned == nil {
		return &tg.Updates{Date: int(time.Now().Unix())}, nil
	}

	ch, _, err := h.store.SetChannelPinnedMessage(r.Ctx, channelID, r.UserID, pinnedID)
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if errors.Is(err, store.ErrMessageInvalid) {
		return nil, errMessageIDInvalid
	}
	if err != nil {
		h.log.Error("pin channel", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	// Emit the pinned notification so all members receive updatePinnedMessages.
	var pinnedMsgID int32
	if pinnedID != nil {
		pinnedMsgID = *pinnedID
	}
	h.notifyPinned(r.Ctx, store.PeerTypeChannel, channelID, pinnedMsgID)

	channels, err := h.loadChannels(r.Ctx, map[int64]bool{ch.ID: true}, r.UserID)
	if err != nil {
		h.log.Error("pin channel render", "channel_id", ch.ID, "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdatePinnedMessages{
				Pinned: pinnedID != nil,
				Peer:   &tg.PeerChannel{ChannelID: ch.ID},
				Messages: func() []int {
					if pinnedID != nil {
						return []int{int(*pinnedID)}
					}
					return nil
				}(),
			},
		},
		Chats: channels,
		Date:  int(time.Now().Unix()),
	}, nil
}

// notifyPinned emits the cross-replica pinned nudge for peerID (best-effort).
// pinnedMsgID is nonzero on pin, zero on unpin.
func (h *handlers) notifyPinned(ctx context.Context, peerType store.PeerType, peerID int64, pinnedMsgID int32) {
	if err := h.store.Notify(ctx, store.ChannelPinned, store.PinnedPayload(peerType, peerID, pinnedMsgID)); err != nil {
		h.log.Error("notify pinned", "peer_id", peerID, "err", err)
	}
}

// handleSearch serves messages.search: keyword search within a dialog.
// Only InputMessagesFilterEmpty is accepted; other filters return INPUT_FILTER_INVALID.
// Results are the caller's messages (both directions) in the named peer, ordered
// newest-first. Channel peers return PEER_ID_INVALID.
func (h *handlers) handleSearch(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesSearchRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if req.Q == "" {
		return nil, errSearchQueryEmpty
	}
	if utf8.RuneCountInString(req.Q) > maxSearchQueryLen {
		return nil, errMessageTooLong
	}
	// Only InputMessagesFilterEmpty is supported; everything else is not implemented.
	if _, ok := req.Filter.(*tg.InputMessagesFilterEmpty); !ok {
		return nil, errInputFilterInvalid
	}
	peerType, peerID, err := h.inputPeer(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}
	// Channel search is a separate ticket.
	if peerType == store.PeerTypeChannel {
		return nil, errPeerIDInvalid
	}
	// Rate limit before any lookup: the membership probe below is a database
	// query, so charging after it would leave a non-member's chat-peer probe
	// uncharged and unbounded. Charging here also keeps the quota uniform —
	// neither membership nor what the query matches changes what the caller is
	// charged, so the quota cannot be read as an oracle. Everything above is
	// pure input validation with no database access.
	if err := h.checkRateLimit(r, "messages_search", h.rateLimitSearchMessages); err != nil {
		return nil, err
	}

	// Chat peers require membership.
	if peerType == store.PeerTypeChat {
		if err = h.requireMember(r.Ctx, peerID, r.UserID); err != nil {
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

	msgs, err := h.store.SearchMessages(r.Ctx, r.UserID, peerType, peerID, req.Q, req.OffsetID, limit)
	if err != nil {
		h.log.Error("search messages", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	files, err := h.loadFiles(r.Ctx, msgs)
	if err != nil {
		h.log.Error("search messages files", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	if peerType == store.PeerTypeChat {
		return h.chatSearch(r, peerID, msgs, files)
	}

	tlMsgs := make([]tg.MessageClass, len(msgs))
	for i, m := range msgs {
		tlMsgs[i] = messageToTL(m, nil, files, nil, nil)
	}

	users, err := h.twoUsers(r.Ctx, r.UserID, peerID)
	if err != nil {
		h.log.Error("search messages users", "err", err)
		return nil, errInternal
	}

	return &tg.MessagesMessages{Messages: tlMsgs, Users: users}, nil
}

// chatSearch renders search results for a chat peer. It collects all authors
// from the result set (plus the caller) and loads the chat metadata.
func (h *handlers) chatSearch(r *mtproto.Request, chatID int64, msgs []store.Message, files map[int64]*tg.Document) (bin.Encoder, error) {
	// Load createUsers for any create service rows, mirroring chatHistory.
	var createUsers []int64
	for _, m := range msgs {
		if m.Action == store.ChatActionCreate {
			parts, perr := h.store.Participants(r.Ctx, chatID)
			if perr != nil {
				h.log.Error("search messages participants", "chat_id", chatID, "err", perr)
				return nil, errInternal
			}
			createUsers = make([]int64, len(parts))
			for i, p := range parts {
				createUsers[i] = p.UserID
			}
			break
		}
	}

	tlMsgs := make([]tg.MessageClass, len(msgs))
	authors := map[int64]bool{r.UserID: true}
	for i, m := range msgs {
		tlMsgs[i] = messageToTL(m, createUsers, files, nil, nil)
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
		h.log.Error("search messages users", "err", err)
		return nil, errInternal
	}
	chats, err := h.loadChats(r.Ctx, map[int64]bool{chatID: true}, r.UserID)
	if err != nil {
		h.log.Error("search messages chats", "err", err)
		return nil, errInternal
	}
	return &tg.MessagesMessages{Messages: tlMsgs, Users: users, Chats: chats}, nil
}
