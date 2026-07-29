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
	peerType, toID, err := inputPeer(req.Peer)
	if err != nil {
		return nil, err
	}
	if peerType == store.PeerTypeChat {
		return h.sendChatMessage(r, toID, &req)
	}

	sender, senderPts, _, dup, err := h.store.SendMessage(r.Ctx, r.UserID, toID, req.Message, req.RandomID, 0)
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
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, nil), Pts: senderPts, PtsCount: 1},
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
func (h *handlers) sendChatMessage(r *mtproto.Request, chatID int64, req *tg.MessagesSendMessageRequest) (bin.Encoder, error) {
	if err := h.requireMember(r.Ctx, chatID, r.UserID); err != nil {
		return nil, err
	}

	sender, perOwner, dup, err := h.store.SendChatMessage(r.Ctx, store.FanOut{
		ChatID: chatID, FromID: r.UserID, Text: req.Message, RandomID: req.RandomID,
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
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, nil), Pts: perOwner[r.UserID], PtsCount: 1},
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
	peerType, toID, err := inputPeer(req.Peer)
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
		tlMsgs[i] = messageToTL(m, nil, files)
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
		tlMsgs[i] = messageToTL(m, createUsers, files)
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
	toID, err := peerUserID(req.Peer)
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
			&tg.UpdateEditMessage{Message: messageToTL(edited, nil, files), Pts: newPts, PtsCount: 1},
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
	toID, err := peerUserID(req.Peer)
	if err != nil {
		return nil, err
	}
	h.notifyTyping(r.Ctx, toID, r.UserID)
	return &tg.BoolTrue{}, nil
}
