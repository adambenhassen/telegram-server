package api

import (
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

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

	currentQts, err := h.store.GetQts(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get qts", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	maxQts := min(int64(req.MaxQts), currentQts)

	randomIDs, err := h.store.AcknowledgeEncryptedEvents(r.Ctx, r.UserID, maxQts)
	if err != nil {
		h.log.Error("acknowledge encrypted events", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	return &tg.LongVector{Elems: randomIDs}, nil
}
