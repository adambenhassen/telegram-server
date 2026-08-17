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
	// Both time fields are stamped per response, not once at startup: a client
	// reads date as the server's clock and refetches the config when expires has
	// passed, so a pair fixed at boot leaves a long-lived server serving a config
	// that is dated wrong and already expired.
	now := h.now()
	cfg := *h.cfg
	cfg.ThisDC = h.dcID
	cfg.Date = int(now.Unix())
	cfg.Expires = int(now.Add(configTTL).Unix())
	return &cfg, nil
}
