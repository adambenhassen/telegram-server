package api

import (
	"context"
	"errors"
	"log/slog"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/gotd/td/tgtest"

	"github.com/adambenhassen/telegram-server/internal/store"
)

type handlers struct {
	store *store.Store
	cfg   *tg.Config
	dcID  int
	log   *slog.Logger
}

type methodFunc func(ctx context.Context, in *bin.Buffer) (bin.Encoder, error)

// New builds the RPC handler: dispatcher wrapped with UnpackInvoke so
// invokeWithLayer/initConnection wrappers are peeled before dispatch.
func New(s *store.Store, dcID int, cfg *tg.Config, log *slog.Logger) tgtest.Handler {
	h := &handlers{store: s, cfg: cfg, dcID: dcID, log: log}
	d := tgtest.NewDispatcher()
	register(d, tg.HelpGetConfigRequestTypeID, h.handleGetConfig)
	register(d, tg.AuthSendCodeRequestTypeID, h.handleSendCode)
	register(d, tg.AuthSignInRequestTypeID, h.handleSignIn)
	d.Fallback(tgtest.HandlerFunc(func(_ *tgtest.Server, req *tgtest.Request) error {
		id, err := req.Buf.PeekID()
		if err != nil {
			h.log.Warn("method not implemented: peek id failed", "err", err)
			return errMethodNotImpl
		}
		h.log.Warn("method not implemented", "type_id", id)
		return errMethodNotImpl
	}))
	return tgtest.UnpackInvoke(d)
}

func register(d *tgtest.Dispatcher, id uint32, fn methodFunc) {
	d.HandleFunc(id, func(srv *tgtest.Server, req *tgtest.Request) error {
		res, err := fn(req.RequestCtx, req.Buf)
		if err != nil {
			var rpc *tgerr.Error
			if !errors.As(err, &rpc) {
				rpc = errInternal
			}
			return srv.SendErr(req, rpc)
		}
		return srv.SendResult(req, res)
	})
}
