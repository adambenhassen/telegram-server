package api

import (
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

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
	for _, d := range dialogs {
		tlDialogs = append(tlDialogs, &tg.Dialog{
			Peer:            &tg.PeerUser{UserID: d.PeerID},
			TopMessage:      int(d.TopMessage),
			ReadInboxMaxID:  int(d.ReadInboxMaxID),
			ReadOutboxMaxID: int(d.ReadOutboxMaxID),
			UnreadCount:     d.UnreadCount,
		})
		peerIDs[d.PeerID] = true

		m, ok, err := h.store.MessageByOwnerLocal(r.Ctx, r.UserID, d.TopMessage)
		if err != nil {
			h.log.Error("get dialogs top message", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		if ok {
			tlMsgs = append(tlMsgs, messageToTL(m, nil))
		}
	}

	users, err := h.loadUsers(r.Ctx, peerIDs, r.UserID)
	if err != nil {
		h.log.Error("get dialogs users", "err", err)
		return nil, errInternal
	}
	return &tg.MessagesDialogs{Dialogs: tlDialogs, Messages: tlMsgs, Users: users}, nil
}
