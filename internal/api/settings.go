package api

import (
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func (h *handlers) handleGetContentSettings(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountGetContentSettingsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.AccountContentSettings{}, nil
}

func (h *handlers) handleGetGlobalPrivacySettings(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountGetGlobalPrivacySettingsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.GlobalPrivacySettings{}, nil
}

func (h *handlers) handleGetThemes(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.AccountGetThemesRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.AccountThemes{Themes: []tg.Theme{}}, nil
}

func (h *handlers) handleGetAppConfig(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.HelpGetAppConfigRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	return &tg.HelpAppConfig{Config: &tg.JSONObject{}}, nil
}
