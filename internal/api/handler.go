package api

import (
	"errors"
	"log/slog"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/srp"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type handlers struct {
	store *store.Store
	cfg   *tg.Config
	dcID  int
	log   *slog.Logger
	srp   *srp.ChallengeStore
	// logLoginCodes gates the one log call that carries credential material.
	logLoginCodes bool
}

type methodFunc func(req *mtproto.Request) (bin.Encoder, error)

// revokeFunc is a methodFunc that also returns work to run once the reply is on
// the wire. Revocation needs it: the evict notification it emits can close this
// very socket from another goroutine, and nothing orders a Postgres round trip
// against a local socket write, so emitting before the reply would let a
// successful logOut or self-reset surface to the client as a transport error.
type revokeFunc func(req *mtproto.Request) (res bin.Encoder, afterReply func(), err error)

// New builds the RPC handler: dispatcher wrapped with UnpackInvoke so
// invokeWithLayer/initConnection wrappers are peeled before dispatch.
func New(s *store.Store, dcID int, cfg *tg.Config, log *slog.Logger, logLoginCodes bool) mtproto.Handler {
	h := &handlers{
		store:         s,
		cfg:           cfg,
		dcID:          dcID,
		log:           log,
		srp:           srp.NewChallengeStore(srp.DefaultTTL),
		logLoginCodes: logLoginCodes,
	}
	d := mtproto.NewDispatcher()
	register(d, tg.HelpGetConfigRequestTypeID, h.handleGetConfig)
	register(d, tg.AuthSendCodeRequestTypeID, h.handleSendCode)
	register(d, tg.AuthSignInRequestTypeID, h.handleSignIn)
	registerRevoke(d, tg.AuthLogOutRequestTypeID, h.handleLogOut)
	register(d, tg.UsersGetUsersRequestTypeID, h.handleGetUsers)
	register(d, tg.AccountGetAuthorizationsRequestTypeID, h.handleGetAuthorizations)
	registerRevoke(d, tg.AccountResetAuthorizationRequestTypeID, h.handleResetAuthorization)
	register(d, tg.AccountGetPasswordRequestTypeID, h.handleGetPassword)
	register(d, tg.AuthCheckPasswordRequestTypeID, h.handleCheckPassword)
	register(d, tg.AccountUpdatePasswordSettingsRequestTypeID, h.handleUpdatePasswordSettings)
	register(d, tg.AccountGetPasswordSettingsRequestTypeID, h.handleGetPasswordSettings)
	register(d, tg.UpdatesGetStateRequestTypeID, h.handleGetState)
	register(d, tg.UpdatesGetDifferenceRequestTypeID, h.handleGetDifference)
	register(d, tg.MessagesSendMessageRequestTypeID, h.handleSendMessage)
	register(d, tg.MessagesGetDialogsRequestTypeID, h.handleGetDialogs)
	register(d, tg.MessagesGetHistoryRequestTypeID, h.handleGetHistory)
	register(d, tg.MessagesReadHistoryRequestTypeID, h.handleReadHistory)
	register(d, tg.MessagesEditMessageRequestTypeID, h.handleEditMessage)
	register(d, tg.MessagesDeleteMessagesRequestTypeID, h.handleDeleteMessages)
	register(d, tg.MessagesSetTypingRequestTypeID, h.handleSetTyping)
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
	registerRevoke(d, id, func(req *mtproto.Request) (bin.Encoder, func(), error) {
		res, err := fn(req)
		return res, nil, err
	})
}

// registerRevoke registers fn and runs its afterReply hook once the reply write
// has been attempted, whether or not that write succeeded: the revocation it
// announces has already committed, so it must propagate either way.
func registerRevoke(d *mtproto.Dispatcher, id uint32, fn revokeFunc) {
	d.HandleFunc(id, func(c *mtproto.Conn, req *mtproto.Request) error {
		res, afterReply, err := fn(req)
		if err != nil {
			var rpc *tgerr.Error
			if !errors.As(err, &rpc) {
				rpc = errInternal
			}
			return c.SendErr(req, rpc)
		}
		sendErr := c.SendResult(req, res)
		if afterReply != nil {
			afterReply()
		}
		return sendErr
	})
}
