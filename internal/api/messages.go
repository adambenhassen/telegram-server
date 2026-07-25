package api

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	defaultHistoryLimit = 20
	maxHistoryLimit     = 100
)

// peerUserID resolves an input peer to a 1:1 user id, validating the M1
// placeholder access hash (access_hash == user_id). Only user peers are in M4
// scope; anything else is PEER_ID_INVALID.
func peerUserID(peer tg.InputPeerClass) (int64, error) {
	p, ok := peer.(*tg.InputPeerUser)
	if !ok {
		return 0, errPeerIDInvalid
	}
	if p.AccessHash != p.UserID || p.UserID == 0 {
		return 0, errPeerIDInvalid
	}
	return p.UserID, nil
}

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
	toID, err := peerUserID(req.Peer)
	if err != nil {
		return nil, err
	}

	sender, senderPts, _, dup, err := h.store.SendMessage(r.Ctx, r.UserID, toID, req.Message, req.RandomID)
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
			&tg.UpdateNewMessage{Message: messageToTL(sender), Pts: senderPts, PtsCount: 1},
		},
		Users: users,
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
	toID, err := peerUserID(req.Peer)
	if err != nil {
		return nil, err
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultHistoryLimit
	}
	if limit > maxHistoryLimit {
		limit = maxHistoryLimit
	}

	msgs, err := h.store.History(r.Ctx, r.UserID, toID, req.OffsetID, limit)
	if err != nil {
		h.log.Error("get history", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	tlMsgs := make([]tg.MessageClass, len(msgs))
	for i, m := range msgs {
		tlMsgs[i] = messageToTL(m)
	}
	users, err := h.twoUsers(r.Ctx, r.UserID, toID)
	if err != nil {
		h.log.Error("get history users", "err", err)
		return nil, errInternal
	}
	return &tg.MessagesMessages{Messages: tlMsgs, Users: users}, nil
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
	users, err := h.twoUsers(r.Ctx, r.UserID, peerID)
	if err != nil {
		h.log.Error("edit message users", "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateEditMessage{Message: messageToTL(edited), Pts: newPts, PtsCount: 1},
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
