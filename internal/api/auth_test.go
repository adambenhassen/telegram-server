package api_test

import (
	"errors"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestValidatePhone(t *testing.T) {
	if err := api.ValidatePhone("+123456789"); err != nil {
		t.Errorf("valid phone rejected: %v", err)
	}
	if err := api.ValidatePhone("abc"); err == nil {
		t.Error("letters accepted as phone")
	}
	if err := api.ValidatePhone(""); err == nil {
		t.Error("empty phone accepted")
	}
}

func TestSignInWrongCodeMapsToRPCError(t *testing.T) {
	err := api.VerifyToRPC(store.ErrCodeInvalid)
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Code != 400 || rpc.Message != "PHONE_CODE_INVALID" {
		t.Errorf("got %v, want PHONE_CODE_INVALID", err)
	}
	if got := api.VerifyToRPC(store.ErrCodeExpired); got.Message != "PHONE_CODE_EXPIRED" {
		t.Errorf("expired mapped to %v", got)
	}
	if api.VerifyToRPC(nil) != nil {
		t.Error("nil should map to nil")
	}
}

func TestSentCodeShape(t *testing.T) {
	sc := api.NewSentCode("deadbeef")
	if sc.PhoneCodeHash != "deadbeef" {
		t.Fatal("hash not set")
	}
	sms, ok := sc.Type.(*tg.AuthSentCodeTypeSMS)
	if !ok || sms.Length != 5 {
		t.Errorf("type = %#v, want SMS length 5", sc.Type)
	}
}
