package api

import (
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func (h *handlers) handleGetConfig(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.HelpGetConfigRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	cfg := *h.cfg
	cfg.ThisDC = h.dcID
	return &cfg, nil
}
