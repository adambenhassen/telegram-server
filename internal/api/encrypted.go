package api

import (
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// handleSendEncryptedMessage serves messages.sendEncrypted: stores the
// encrypted payload for the recipient, bumps their qts, and returns the date.
func (h *handlers) handleSendEncryptedMessage(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesSendEncryptedRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	ec, err := h.store.GetEncryptedChat(r.Ctx, req.Peer.ChatID)
	if err != nil {
		h.log.Error("get encrypted chat", "chat_id", req.Peer.ChatID, "err", err)
		return nil, errInternal
	}
	if ec.ID == 0 {
		return nil, tgerr.New(400, "CHAT_ID_INVALID")
	}

	// Determine recipient: the other participant in the chat.
	recipientID := ec.User1ID
	if ec.User1ID == r.UserID {
		recipientID = ec.User2ID
	}

	// Bump recipient's qts and store the event.
	newQts, err := h.store.BumpQts(r.Ctx, recipientID)
	if err != nil {
		h.log.Error("bump qts", "user_id", recipientID, "err", err)
		return nil, errInternal
	}

	if err := h.store.InsertEncryptedEvent(r.Ctx, recipientID, newQts, req.RandomID, req.Data); err != nil {
		h.log.Error("insert encrypted event", "user_id", recipientID, "err", err)
		return nil, errInternal
	}

	h.notify(r.Ctx, recipientID)

	return &tg.MessagesSentEncryptedMessage{
		Date: int(time.Now().Unix()),
	}, nil
}

// handleReceivedQueue serves messages.receivedQueue: acknowledges encrypted
// events up to max_qts (clamped to current qts), deletes them, and returns
// their random_ids.
func (h *handlers) handleReceivedQueue(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesReceivedQueueRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	// Clamp max_qts to current qts — attacker-controlled input.
	currentQts, err := h.store.GetQts(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get qts", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	maxQts := min(int64(req.MaxQts), currentQts)

	// Read events up to clamped qts before deleting, so we can return random_ids.
	events, err := h.store.EncryptedEventsUpTo(r.Ctx, r.UserID, maxQts)
	if err != nil {
		h.log.Error("encrypted events up to", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	// Collect random_ids.
	randomIDs := make([]int64, 0, len(events))
	for _, e := range events {
		randomIDs = append(randomIDs, e.RandomID)
	}

	// Delete acknowledged events.
	if err := h.store.DeleteEncryptedEventsUpTo(r.Ctx, r.UserID, maxQts); err != nil {
		h.log.Error("delete encrypted events", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	return &tg.LongVector{Elems: randomIDs}, nil
}
