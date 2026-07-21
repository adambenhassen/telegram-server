package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
)

func (h *handlers) handleGetConfig(_ context.Context, in *bin.Buffer) (bin.Encoder, error) {
	var req tg.HelpGetConfigRequest
	if err := req.Decode(in); err != nil {
		return nil, errMethodNotImpl
	}
	cfg := *h.cfg
	cfg.ThisDC = h.dcID
	return &cfg, nil
}
