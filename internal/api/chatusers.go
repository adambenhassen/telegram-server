package api

import (
	"context"
	"errors"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// resolveTarget resolves an InputUserClass to an existing user id, returning a
// wire error ready to hand back. A target that no user row backs is
// PEER_ID_INVALID, the same error a bad access hash gets.
func (h *handlers) resolveTarget(ctx context.Context, u tg.InputUserClass, selfID int64, op string) (int64, error) {
	target, err := h.inputUserID(u, selfID)
	if err != nil {
		return 0, err
	}
	_, ok, err := h.store.UserByID(ctx, target)
	if err != nil {
		h.log.Error(op, "user_id", selfID, "err", err)
		return 0, errInternal
	}
	if !ok {
		return 0, errPeerIDInvalid
	}
	return target, nil
}

// chatMemberErr maps a membership mutation's store error to the wire. An absent
// chat and a non-member caller both arrive as store.ErrNotMember on purpose, so
// they must keep sharing one error here: chat ids come from a dense BIGSERIAL
// space and a distinct not-found would let a caller enumerate every chat.
func (h *handlers) chatMemberErr(op string, userID int64, err error) error {
	switch {
	case errors.Is(err, store.ErrNotMember):
		return errPeerIDInvalid
	case errors.Is(err, store.ErrChatFull):
		return errUsersTooMuch
	default:
		h.log.Error(op, "user_id", userID, "err", err)
		return errInternal
	}
}

// chatMembershipUpdates hydrates the caller's own view of a membership change:
// the service message as an updateNewMessage at the caller's new pts, the two
// users the action names, and the chat carrying its bumped version.
func (h *handlers) chatMembershipUpdates(ctx context.Context, selfID, target int64, sender store.Message, perOwner map[int64]int) (*tg.Updates, error) {
	users, err := h.twoUsers(ctx, selfID, target)
	if err != nil {
		return nil, err
	}
	chats, err := h.loadChats(ctx, map[int64]bool{sender.PeerID: true}, selfID, nil)
	if err != nil {
		return nil, err
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			// A service message never carries media.
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, nil, nil, nil), Pts: perOwner[selfID], PtsCount: 1},
		},
		Users: users,
		Chats: chats,
		Date:  int(sender.Date.Unix()),
	}, nil
}

// noUpdates is the envelope for a membership call that changed nothing: the
// store wrote no row, emitted no service message and moved no pts, so there is
// nothing for the client to apply.
func noUpdates() *tg.Updates {
	return &tg.Updates{Updates: []tg.UpdateClass{}, Date: int(time.Now().Unix())}
}

// handleAddChatUser serves messages.addChatUser: one store call adds the target
// and announces it, then every affected user is nudged.
//
// FwdLimit is accepted and ignored — M6 forwards no history to a new member, who
// sees the chat from the add onwards.
func (h *handlers) handleAddChatUser(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesAddChatUserRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	// Rate limit before any write.
	if err := h.checkRateLimit(r, "add_chat_user", h.rateLimitAddChatUser); err != nil {
		return nil, err
	}
	target, err := h.resolveTarget(r.Ctx, req.UserID, r.UserID, "add chat user target")
	if err != nil {
		return nil, err
	}

	added, sender, perOwner, err := h.store.AddChatUser(r.Ctx, req.ChatID, target, r.UserID)
	if err != nil {
		return nil, h.chatMemberErr("add chat user", r.UserID, err)
	}
	if !added {
		return &tg.MessagesInvitedUsers{Updates: noUpdates()}, nil
	}
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}

	ups, err := h.chatMembershipUpdates(r.Ctx, r.UserID, target, sender, perOwner)
	if err != nil {
		h.log.Error("add chat user updates", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.MessagesInvitedUsers{Updates: ups}, nil
}

// handleDeleteChatUser serves messages.deleteChatUser. target == caller is a
// member leaving, and is allowed. The removed user is in perOwner, so they are
// nudged too: the announcement is how their client learns it is out.
//
// RevokeHistory is accepted and ignored — under two-sided storage the removed
// member owns their copies of past messages, their dialog row and their pts, and
// none of it is deleted or rolled back.
func (h *handlers) handleDeleteChatUser(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesDeleteChatUserRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	target, err := h.resolveTarget(r.Ctx, req.UserID, r.UserID, "delete chat user target")
	if err != nil {
		return nil, err
	}

	removed, sender, perOwner, err := h.store.RemoveChatUser(r.Ctx, req.ChatID, target, r.UserID)
	if err != nil {
		return nil, h.chatMemberErr("delete chat user", r.UserID, err)
	}
	if !removed {
		return noUpdates(), nil
	}
	for uid := range perOwner {
		h.notify(r.Ctx, uid)
	}

	ups, err := h.chatMembershipUpdates(r.Ctx, r.UserID, target, sender, perOwner)
	if err != nil {
		h.log.Error("delete chat user updates", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return ups, nil
}
