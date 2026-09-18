package api_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

type settingsHandler struct {
	name                  string
	handle                func(int64) (bin.Encoder, error)
	request               func() bin.Encoder
	response              func() bin.Decoder
	assert                func(*testing.T, bin.Decoder)
	requiresAuthorization bool
}

func settingsHandlers() []settingsHandler {
	return []settingsHandler{
		{
			name:                  "content settings",
			handle:                api.GetContentSettingsForTest,
			request:               func() bin.Encoder { return &tg.AccountGetContentSettingsRequest{} },
			response:              func() bin.Decoder { return &tg.AccountContentSettings{} },
			requiresAuthorization: true,
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				got, ok := response.(*tg.AccountContentSettings)
				if !ok {
					t.Fatalf("response = %T, want *tg.AccountContentSettings", response)
				}
				if got.SensitiveEnabled || got.SensitiveCanChange {
					t.Fatal("content settings advertise unsupported sensitive content")
				}
			},
		},
		{
			name:                  "global privacy settings",
			handle:                api.GetGlobalPrivacySettingsForTest,
			request:               func() bin.Encoder { return &tg.AccountGetGlobalPrivacySettingsRequest{} },
			response:              func() bin.Decoder { return &tg.GlobalPrivacySettings{} },
			requiresAuthorization: true,
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				got, ok := response.(*tg.GlobalPrivacySettings)
				if !ok {
					t.Fatalf("response = %T, want *tg.GlobalPrivacySettings", response)
				}
				if got.Flags != 0 {
					t.Fatalf("global privacy settings flags = %#x, want none", got.Flags)
				}
			},
		},
		{
			name:                  "themes",
			handle:                api.GetThemesForTest,
			request:               func() bin.Encoder { return &tg.AccountGetThemesRequest{} },
			response:              func() bin.Decoder { return &tg.AccountThemes{} },
			requiresAuthorization: true,
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				got, ok := response.(*tg.AccountThemes)
				if !ok {
					t.Fatalf("response = %T, want *tg.AccountThemes", response)
				}
				if got.Hash != 0 || len(got.Themes) != 0 {
					t.Fatalf("themes = hash %d, %d themes; want empty", got.Hash, len(got.Themes))
				}
			},
		},
		{
			name: "app config",
			handle: func(userID int64) (bin.Encoder, error) {
				return api.GetAppConfigForTestWithMode(userID, config.RegistrationInvite)
			},
			request:  func() bin.Encoder { return &tg.HelpGetAppConfigRequest{} },
			response: func() bin.Decoder { return &tg.HelpAppConfig{} },
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				got, ok := response.(*tg.HelpAppConfig)
				if !ok {
					t.Fatalf("response = %T, want *tg.HelpAppConfig", response)
				}
				jsonConfig, ok := got.Config.(*tg.JSONObject)
				if !ok || len(jsonConfig.Value) != 1 {
					t.Fatalf("app config = %T with %v, want registration mode", got.Config, got.Config)
				}
				if jsonConfig.Value[0].Key != "registration_mode" {
					t.Fatalf("app config key = %q, want registration_mode", jsonConfig.Value[0].Key)
				}
				mode, ok := jsonConfig.Value[0].Value.(*tg.JSONString)
				if !ok || mode.Value != string(config.RegistrationInvite) {
					t.Fatalf("registration mode = %T %v, want %q", jsonConfig.Value[0].Value, jsonConfig.Value[0].Value, config.RegistrationInvite)
				}
			},
		},
	}
}

func TestSettingsHandlersRequireAuthorization(t *testing.T) {
	for _, method := range settingsHandlers() {
		t.Run(method.name, func(t *testing.T) {
			if !method.requiresAuthorization {
				return
			}
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
				jsonConfig, ok := got.Config.(*tg.JSONObject)
				if !ok || len(jsonConfig.Value) != 1 {
					t.Fatalf("app config = %T with %v, want registration mode", got.Config, got.Config)
				}
				mode, ok := jsonConfig.Value[0].Value.(*tg.JSONString)
				if jsonConfig.Value[0].Key != "registration_mode" || !ok || mode.Value != string(config.RegistrationInvite) {
					t.Fatalf("registration mode = %v, want %q", jsonConfig.Value[0], config.RegistrationInvite)
				}
			default:
				t.Fatalf("response = %T, want one of the settings success types", res)
			}
		})
	}
}

type settingsDispatcherTransport struct {
	mu   sync.Mutex
	sent [][]byte
}

func (t *settingsDispatcherTransport) Send(_ context.Context, b *bin.Buffer) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sent = append(t.sent, slices.Clone(b.Buf))
	return nil
}

func (*settingsDispatcherTransport) Recv(context.Context, *bin.Buffer) error { return io.EOF }
func (*settingsDispatcherTransport) Close() error                            { return nil }

func (transport *settingsDispatcherTransport) result(t *testing.T, key crypto.AuthKey, msgID int64) []byte {
	t.Helper()
	transport.mu.Lock()
	frames := slices.Clone(transport.sent)
	transport.mu.Unlock()

	cipher := crypto.NewClientCipher(crypto.DefaultRand())
	for _, frame := range frames {
		message := &crypto.EncryptedMessage{}
		if err := message.DecodeWithoutCopy(&bin.Buffer{Buf: frame}); err != nil {
			t.Fatalf("decode server frame: %v", err)
		}
		decrypted, err := cipher.Decrypt(key, message)
		if err != nil {
			t.Fatalf("decrypt server frame: %v", err)
		}
		var result proto.Result
		if err := result.Decode(&bin.Buffer{Buf: decrypted.Data()}); err != nil {
			continue
		}
		if result.RequestMessageID == msgID {
			return slices.Clone(result.Result)
		}
	}
	t.Fatalf("no RPC result for msg id %d in %d replies", msgID, len(frames))
	return nil
}

func dispatchSettings(t *testing.T, h mtproto.Handler, method settingsHandler, userID int64, provisional bool) []byte {
	t.Helper()
	key := testKey()
	transport := &settingsDispatcherTransport{}
	conn := mtproto.NewTestConn(transport, key)
	var body bin.Buffer
	if err := method.request().Encode(&body); err != nil {
		t.Fatalf("encode %s request: %v", method.name, err)
	}
	const msgID = int64(1 << 32)
	if err := h.OnMessage(conn, &mtproto.Request{
		AuthKeyID:   key.ID,
		UserID:      userID,
		Provisional: provisional,
		MsgID:       msgID,
		Buf:         &body,
		Ctx:         context.Background(),
	}); err != nil {
		t.Fatalf("dispatch %s: %v", method.name, err)
	}
	return transport.result(t, key, msgID)
}

func TestSettingsHandlersThroughDispatcher(t *testing.T) {
	h := api.New(
		nil,
		2,
		&tg.Config{},
		slog.New(slog.DiscardHandler),
		false,
		1,
		nil,
		1,
		pgtest.PeerDeriver(),
		config.RateLimitsConfig{},
		config.RegistrationInvite,
	)

	for _, method := range settingsHandlers() {
		t.Run(method.name, func(t *testing.T) {
			t.Run("authorized", func(t *testing.T) {
				body := dispatchSettings(t, h, method, 1, false)
				response := method.response()
				if err := response.Decode(&bin.Buffer{Buf: body}); err != nil {
					t.Fatalf("decode authorized response: %v", err)
				}
				method.assert(t, response)
			})

			for _, session := range []struct {
				name        string
				userID      int64
				provisional bool
			}{
				{name: "unauthenticated"},
				{name: "provisional", userID: 1, provisional: true},
			} {
				t.Run(session.name, func(t *testing.T) {
					if !method.requiresAuthorization {
						body := dispatchSettings(t, h, method, session.userID, session.provisional)
						response := method.response()
						if err := response.Decode(&bin.Buffer{Buf: body}); err != nil {
							t.Fatalf("decode allowed response: %v", err)
						}
						method.assert(t, response)
						return
					}
					body := dispatchSettings(t, h, method, session.userID, session.provisional)
					rpc := &mt.RPCError{}
					if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
						t.Fatalf("decode rejection: %v", err)
					}
					if rpc.ErrorCode != 401 || rpc.ErrorMessage != "AUTH_KEY_UNREGISTERED" {
						t.Fatalf("rejection = %d %q, want 401 AUTH_KEY_UNREGISTERED", rpc.ErrorCode, rpc.ErrorMessage)
					}
				})
			}
		})
	}
}

func TestAppConfigAdvertisesRegistrationModeWithoutAuthorization(t *testing.T) {
	t.Parallel()

	for _, mode := range []config.RegistrationMode{
		config.RegistrationClosed,
		config.RegistrationOpen,
	} {
		t.Run(string(mode), func(t *testing.T) {
			res, err := api.GetAppConfigForTestWithMode(0, mode)
			if err != nil {
				t.Fatalf("help.getAppConfig: %v", err)
			}
			appConfig, ok := res.(*tg.HelpAppConfig)
			if !ok {
				t.Fatalf("response = %T, want *tg.HelpAppConfig", res)
			}
			object, ok := appConfig.Config.(*tg.JSONObject)
			if !ok || len(object.Value) != 1 {
				t.Fatalf("config = %T with %v, want one registration_mode field", appConfig.Config, appConfig.Config)
			}
			if object.Value[0].Key != "registration_mode" {
				t.Fatalf("config key = %q, want registration_mode", object.Value[0].Key)
			}
			value, ok := object.Value[0].Value.(*tg.JSONString)
			if !ok || value.Value != string(mode) {
				t.Fatalf("registration mode = %T %v, want %q", object.Value[0].Value, object.Value[0].Value, mode)
			}
		})
	}
}
