package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func handleSettingsForTest(userID int64, req bin.Encoder, handle func(*handlers, *mtproto.Request) (bin.Encoder, error)) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return handle(testHandlers(nil), &mtproto.Request{
		Ctx:    context.Background(),
		UserID: userID,
		Buf:    &buf,
	})
}

func GetContentSettingsForTest(userID int64) (bin.Encoder, error) {
	return handleSettingsForTest(userID, &tg.AccountGetContentSettingsRequest{}, (*handlers).handleGetContentSettings)
}

func GetGlobalPrivacySettingsForTest(userID int64) (bin.Encoder, error) {
	return handleSettingsForTest(userID, &tg.AccountGetGlobalPrivacySettingsRequest{}, (*handlers).handleGetGlobalPrivacySettings)
}

func GetThemesForTest(userID int64) (bin.Encoder, error) {
	return handleSettingsForTest(userID, &tg.AccountGetThemesRequest{}, (*handlers).handleGetThemes)
}

func GetAppConfigForTest(userID int64) (bin.Encoder, error) {
	return handleSettingsForTest(userID, &tg.HelpGetAppConfigRequest{}, (*handlers).handleGetAppConfig)
}
