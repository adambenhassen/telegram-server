package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// createUsersForDialog fetches the participant list for a ChatActionCreate row
// when the viewer is still a member. A removed viewer gets nil, matching the
// empty user list getDifference serves for the same event.
func (h *handlers) createUsersForDialog(ctx context.Context, chatID, viewerID int64) ([]int64, error) {
	member, err := h.store.IsMember(ctx, chatID, viewerID)
	if err != nil {
		return nil, err
	}
	if !member {
		return nil, nil
	}
	parts, err := h.store.Participants(ctx, chatID)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, len(parts))
	for i, p := range parts {
		ids[i] = p.UserID
	}
	return ids, nil
}

// handleGetDialogs serves messages.getDialogs: the caller's conversation list
// with each dialog's top message and the referenced peer users.
func (h *handlers) handleGetDialogs(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetDialogsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	dialogs, err := h.store.Dialogs(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get dialogs", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	tlDialogs := make([]tg.DialogClass, 0, len(dialogs))
	tlMsgs := make([]tg.MessageClass, 0, len(dialogs))
	peerIDs := map[int64]bool{r.UserID: true}
	chatIDs := map[int64]bool{}
	for _, d := range dialogs {
		tlDialogs = append(tlDialogs, &tg.Dialog{
			Peer:            peerToTL(d.PeerType, d.PeerID),
			TopMessage:      int(d.TopMessage),
			ReadInboxMaxID:  int(d.ReadInboxMaxID),
			ReadOutboxMaxID: int(d.ReadOutboxMaxID),
			UnreadCount:     d.UnreadCount,
		})
		if d.PeerType == store.PeerTypeChat {
			chatIDs[d.PeerID] = true
		} else {
			peerIDs[d.PeerID] = true
		}

		m, ok, err := h.store.MessageByOwnerLocal(r.Ctx, r.UserID, d.TopMessage)
		if err != nil {
			h.log.Error("get dialogs top message", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		if ok {
			// The author is the peer in a 1:1 but any member in a group, so it is
			// taken off the message rather than off the dialog's peer id.
			peerIDs[m.FromID] = true
			var createUsers []int64
			switch m.Action {
			case store.ChatActionAddUser, store.ChatActionDeleteUser:
				// ActionUserID is stored on the viewer's own row, so no membership
				// gate is needed — the row exists because fan-out wrote it here.
				peerIDs[m.ActionUserID] = true
			case store.ChatActionCreate:
				cu, cerr := h.createUsersForDialog(r.Ctx, m.PeerID, r.UserID)
				if cerr != nil {
					h.log.Error("get dialogs create users", "user_id", r.UserID, "err", cerr)
					return nil, errInternal
				}
				createUsers = cu
				for _, id := range cu {
					peerIDs[id] = true
				}
			}
			tlMsgs = append(tlMsgs, messageToTL(m, createUsers))
		}
	}

	users, err := h.loadUsers(r.Ctx, peerIDs, r.UserID)
	if err != nil {
		h.log.Error("get dialogs users", "err", err)
		return nil, errInternal
	}
	// No membership check on the list itself: a dialog row exists only because a
	// fan-out wrote it for this owner, so an attacker-chosen id never reaches here.
	// A removed member keeps their dialog row by design, which is why the viewer is
	// passed down — loadChats degrades those to tg.ChatForbidden, which carries
	// the id and an empty title and nothing else — no live title, version or
	// participant count reaches a removed member.
	chats, err := h.loadChats(r.Ctx, chatIDs, r.UserID)
	if err != nil {
		h.log.Error("get dialogs chats", "err", err)
		return nil, errInternal
	}
	return &tg.MessagesDialogs{Dialogs: tlDialogs, Messages: tlMsgs, Users: users, Chats: chats}, nil
}
