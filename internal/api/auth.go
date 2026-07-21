package api

import (
	"context"
	"errors"
	"regexp"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/store"
)

var phoneRE = regexp.MustCompile(`^\+?[0-9]{5,15}$`)

func validatePhone(phone string) error {
	if !phoneRE.MatchString(phone) {
		return errPhoneInvalid
	}
	return nil
}

func newSentCode(hash string) *tg.AuthSentCode {
	return &tg.AuthSentCode{
		Type:          &tg.AuthSentCodeTypeSMS{Length: 5},
		PhoneCodeHash: hash,
	}
}

func verifyToRPC(err error) *tgerr.Error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrCodeInvalid):
		return errCodeInvalid
	case errors.Is(err, store.ErrCodeExpired):
		return errCodeExpired
	default:
		return errInternal
	}
}

func (h *handlers) handleSendCode(ctx context.Context, in *bin.Buffer) (bin.Encoder, error) {
	var req tg.AuthSendCodeRequest
	if err := req.Decode(in); err != nil {
		return nil, errMethodNotImpl
	}
	if err := validatePhone(req.PhoneNumber); err != nil {
		return nil, err
	}
	hash, code, err := h.store.IssueCode(ctx, req.PhoneNumber)
	if err != nil {
		h.log.Error("issue code", "phone", req.PhoneNumber, "err", err)
		return nil, errInternal
	}
	h.log.Info("login code issued", "phone", req.PhoneNumber, "code", code)
	return newSentCode(hash), nil
}

func (h *handlers) handleSignIn(ctx context.Context, in *bin.Buffer) (bin.Encoder, error) {
	var req tg.AuthSignInRequest
	if err := req.Decode(in); err != nil {
		return nil, errMethodNotImpl
	}
	code, _ := req.GetPhoneCode()
	if err := h.store.VerifyCode(ctx, req.PhoneNumber, req.PhoneCodeHash, code); err != nil {
		if rpc := verifyToRPC(err); rpc != errInternal {
			return nil, rpc
		}
		h.log.Error("verify code", "phone", req.PhoneNumber, "err", err)
		return nil, errInternal
	}
	user, err := h.store.CreateUser(ctx, req.PhoneNumber)
	if err != nil {
		h.log.Error("create user", "phone", req.PhoneNumber, "err", err)
		return nil, errInternal
	}
	return &tg.AuthAuthorization{
		User: &tg.User{
			ID:         user.ID,
			Self:       true,
			Phone:      user.Phone,
			FirstName:  user.FirstName,
			AccessHash: user.ID, // M1: self access hash placeholder
		},
	}, nil
}
