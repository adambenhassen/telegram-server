package api_test

import (
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
)

type settingsHandler struct {
	name   string
	handle func(int64) (bin.Encoder, error)
}

func settingsHandlers() []settingsHandler {
	return []settingsHandler{
		{
			name:   "content settings",
			handle: api.GetContentSettingsForTest,
		},
		{
			name:   "global privacy settings",
			handle: api.GetGlobalPrivacySettingsForTest,
		},
		{
			name:   "themes",
			handle: api.GetThemesForTest,
		},
		{
			name:   "app config",
			handle: api.GetAppConfigForTest,
		},
	}
}

func TestSettingsHandlersRequireAuthorization(t *testing.T) {
	for _, method := range settingsHandlers() {
		t.Run(method.name, func(t *testing.T) {
			res, err := method.handle(0)
			if res != nil {
				t.Fatalf("response = %T, want nil", res)
			}
			var rpc *tgerr.Error
			if !errors.As(err, &rpc) || rpc.Message != "AUTH_KEY_UNREGISTERED" {
				t.Fatalf("error = %v, want AUTH_KEY_UNREGISTERED", err)
			}
		})
	}
}

func TestSettingsHandlersReturnHonestDefaults(t *testing.T) {
	for _, method := range settingsHandlers() {
		t.Run(method.name, func(t *testing.T) {
			res, err := method.handle(1)
			if err != nil {
				t.Fatalf("handler: %v", err)
			}
			var encoded bin.Buffer
			if err := res.Encode(&encoded); err != nil {
				t.Fatalf("response does not encode: %v", err)
			}

			switch got := res.(type) {
			case *tg.AccountContentSettings:
				if got.SensitiveEnabled || got.SensitiveCanChange {
					t.Fatal("content settings advertise unsupported sensitive content")
				}
			case *tg.GlobalPrivacySettings:
				if got.Flags != 0 {
					t.Fatalf("global privacy settings flags = %#x, want none", got.Flags)
				}
			case *tg.AccountThemes:
				if got.Hash != 0 || len(got.Themes) != 0 {
					t.Fatalf("themes = hash %d, %d themes; want empty", got.Hash, len(got.Themes))
				}
			case *tg.HelpAppConfig:
				config, ok := got.Config.(*tg.JSONObject)
				if !ok || len(config.Value) != 0 {
					t.Fatalf("app config = %T with %v, want empty JSON object", got.Config, got.Config)
				}
			default:
				t.Fatalf("response = %T, want one of the settings success types", res)
			}
		})
	}
}
