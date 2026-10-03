package api

import (
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/catalog"
	"github.com/teagramhq/teagram-server/internal/mtproto"
)

func (h *handlers) handleGetConfig(r *mtproto.Request) (bin.Encoder, error) {
	return h.handleGetConfigWithSystemLangCode(r, "")
}

func (h *handlers) handleGetConfigWithSystemLangCode(r *mtproto.Request, systemLangCode string) (bin.Encoder, error) {
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
	var snapshot *catalog.Snapshot
	if h.store != nil {
		snapshot = h.store.CatalogSnapshot()
	}
	cfg.SetSuggestedLangCode(catalog.SuggestedLanguageCode(snapshot, systemLangCode))
	return &cfg, nil
}

func (h *handlers) registerHelpPolling(d *mtproto.Dispatcher) {
	d.HandleFunc(tg.HelpGetTermsOfServiceUpdateRequestTypeID, func(c *mtproto.Conn, req *mtproto.Request) error {
		if req.UserID == 0 || req.Provisional {
			return h.handleUnknownGated(c, req)
		}
		var call tg.HelpGetTermsOfServiceUpdateRequest
		if err := call.Decode(req.Buf); err != nil || req.Buf.Len() != 0 {
			return c.SendErr(req, errMethodNotImpl)
		}
		return c.SendResult(req, &tg.HelpTermsOfServiceUpdateEmpty{
			Expires: int(h.now().Add(configTTL).Unix()),
		})
	})

	d.HandleFunc(tg.HelpGetPromoDataRequestTypeID, func(c *mtproto.Conn, req *mtproto.Request) error {
		if req.UserID == 0 || req.Provisional {
			return h.handleUnknownGated(c, req)
		}
		var call tg.HelpGetPromoDataRequest
		if err := call.Decode(req.Buf); err != nil || req.Buf.Len() != 0 {
			return c.SendErr(req, errMethodNotImpl)
		}
		return c.SendResult(req, &tg.HelpPromoDataEmpty{
			Expires: int(h.now().Add(configTTL).Unix()),
		})
	})
}
