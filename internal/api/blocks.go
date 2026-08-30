package api

import (
	"context"
	"errors"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	defaultBlockedUsersLimit = 20
	maxBlockedUsersLimit     = 100
)

// resolveBlockedPeer validates the caller-scoped user peer and confirms that a
// users row backs it. A self-block and every malformed or unknown peer share
// PEER_ID_INVALID, so this endpoint does not become a user-id oracle.
func (h *handlers) resolveBlockedPeer(ctx context.Context, peer tg.InputPeerClass, viewerID int64, op string) (int64, error) {
	target, err := h.peerUserID(peer, viewerID)
	if err != nil {
		return 0, err
	}
	if target == viewerID {
		return 0, errPeerIDInvalid
	}
	_, ok, err := h.store.UserByID(ctx, target)
	if err != nil {
		h.log.Error(op, "user_id", viewerID, "err", err)
		return 0, errInternal
	}
	if !ok {
		return 0, errPeerIDInvalid
	}
	return target, nil
}

// handleContactsBlock serves contacts.block for the main directed block list.
// Story blocking is accepted as a request flag but is outside this workstream;
// it is deliberately not persisted as a different kind of relationship.
func (h *handlers) handleContactsBlock(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsBlockRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	target, err := h.resolveBlockedPeer(r.Ctx, req.ID, r.UserID, "block user target")
	if err != nil {
		return nil, err
	}
	if _, err := h.store.BlockUser(r.Ctx, r.UserID, target); err != nil {
		if errors.Is(err, store.ErrInvalidBlock) {
			return nil, errPeerIDInvalid
		}
		h.log.Error("block user", "user_id", r.UserID, "target_id", target, "err", err)
		return nil, errInternal
	}
	return &tg.BoolTrue{}, nil
}

// handleContactsUnblock serves contacts.unblock for the main directed block
// list. Removing a missing row is a successful no-op.
func (h *handlers) handleContactsUnblock(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsUnblockRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	target, err := h.resolveBlockedPeer(r.Ctx, req.ID, r.UserID, "unblock user target")
	if err != nil {
		return nil, err
	}
	if _, err := h.store.UnblockUser(r.Ctx, r.UserID, target); err != nil {
		if errors.Is(err, store.ErrInvalidBlock) {
			return nil, errPeerIDInvalid
		}
		h.log.Error("unblock user", "user_id", r.UserID, "target_id", target, "err", err)
		return nil, errInternal
	}
	return &tg.BoolTrue{}, nil
}

// handleContactsGetBlocked serves contacts.getBlocked for the main directed
// block list. Blocked users are entitled to appear here because the list itself
// is the caller's private state; it therefore hydrates users directly instead
// of applying the live-edge gate used by ordinary message responses.
func (h *handlers) handleContactsGetBlocked(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsGetBlockedRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	offset := req.Offset
	offset = max(offset, 0)
	limit := req.Limit
	if limit <= 0 {
		limit = defaultBlockedUsersLimit
	}
	if limit > maxBlockedUsersLimit {
		limit = maxBlockedUsersLimit
	}

	page, err := h.store.BlockedUsers(r.Ctx, r.UserID, offset, limit)
	if err != nil {
		h.log.Error("get blocked", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	ids := make([]int64, len(page.Users))
	for i, blocked := range page.Users {
		ids[i] = blocked.UserID
	}
	users, err := h.store.UsersByID(r.Ctx, ids)
	if err != nil {
		h.log.Error("get blocked users", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	blockedPeers := make([]tg.PeerBlocked, 0, len(page.Users))
	blockedUsers := make([]tg.UserClass, 0, len(page.Users))
	for _, entry := range page.Users {
		user, ok := users[entry.UserID]
		if !ok {
			// The FK makes this unreachable for a healthy schema, but failing
			// closed avoids emitting a peer entry without its mandatory user.
			h.log.Error("get blocked: missing user", "user_id", r.UserID, "blocked_id", entry.UserID)
			return nil, errInternal
		}
		blockedPeers = append(blockedPeers, tg.PeerBlocked{
			PeerID: &tg.PeerUser{UserID: entry.UserID},
			Date:   int(entry.Date.Unix()),
		})
		blockedUsers = append(blockedUsers, h.userToTL(user, r.UserID, false))
	}

	if offset == 0 && len(page.Users) == page.Total {
		return &tg.ContactsBlocked{
			Blocked: blockedPeers,
			Chats:   []tg.ChatClass{},
			Users:   blockedUsers,
		}, nil
	}
	return &tg.ContactsBlockedSlice{
		Count:   page.Total,
		Blocked: blockedPeers,
		Chats:   []tg.ChatClass{},
		Users:   blockedUsers,
	}, nil
}
