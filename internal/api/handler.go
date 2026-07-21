package api

import (
	"errors"
	"log/slog"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type handlers struct {
	store *store.Store
	cfg   *tg.Config
	dcID  int
	log   *slog.Logger
}

type methodFunc func(req *mtproto.Request) (bin.Encoder, error)

// New builds the RPC handler: dispatcher wrapped with UnpackInvoke so
// invokeWithLayer/initConnection wrappers are peeled before dispatch.
func New(s *store.Store, dcID int, cfg *tg.Config, log *slog.Logger) mtproto.Handler {
	h := &handlers{store: s, cfg: cfg, dcID: dcID, log: log}
	d := mtproto.NewDispatcher()
	register(d, tg.HelpGetConfigRequestTypeID, h.handleGetConfig)
	register(d, tg.AuthSendCodeRequestTypeID, h.handleSendCode)
	register(d, tg.AuthSignInRequestTypeID, h.handleSignIn)
	register(d, tg.UsersGetUsersRequestTypeID, h.handleGetUsers)
	d.Fallback(mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
		id, err := req.Buf.PeekID()
		if err != nil {
			h.log.Warn("method not implemented: peek id failed", "err", err)
			return errMethodNotImpl
		}
		h.log.Warn("method not implemented", "type_id", id)
		return errMethodNotImpl
	}))
	return mtproto.UnpackInvoke(d)
}

func register(d *mtproto.Dispatcher, id uint32, fn methodFunc) {
	d.HandleFunc(id, func(c *mtproto.Conn, req *mtproto.Request) error {
		res, err := fn(req)
		if err != nil {
			var rpc *tgerr.Error
			if !errors.As(err, &rpc) {
				rpc = errInternal
			}
			return c.SendErr(req, rpc)
		}
		return c.SendResult(req, res)
	})
}
