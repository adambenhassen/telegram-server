package api_test

import (
	"bytes"
	"context"
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

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

type startupMethod struct {
	name       string
	request    func() bin.Encoder
	response   func() bin.Decoder
	assert     func(*testing.T, bin.Decoder)
	repeatable bool
}

func startupMethods() []startupMethod {
	return []startupMethod{
		{
			name:    "saved reaction tags",
			request: func() bin.Encoder { return &tg.MessagesGetSavedReactionTagsRequest{} },
			response: func() bin.Decoder {
				return &tg.MessagesSavedReactionTagsBox{}
			},
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				box, ok := response.(*tg.MessagesSavedReactionTagsBox)
				if !ok {
					t.Fatalf("response = %T, want *tg.MessagesSavedReactionTagsBox", response)
				}
				got, ok := box.SavedReactionTags.(*tg.MessagesSavedReactionTags)
				if !ok {
					t.Fatalf("saved reaction tags = %T, want *tg.MessagesSavedReactionTags", box.SavedReactionTags)
				}
				if got.Hash != 0 || len(got.Tags) != 0 {
					t.Fatalf("saved reaction tags = hash %d, %d tags; want empty", got.Hash, len(got.Tags))
				}
			},
		},
		{
			name:    "attach menu bots",
			request: func() bin.Encoder { return &tg.MessagesGetAttachMenuBotsRequest{} },
			response: func() bin.Decoder {
				return &tg.AttachMenuBotsBox{}
			},
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				box, ok := response.(*tg.AttachMenuBotsBox)
				if !ok {
					t.Fatalf("response = %T, want *tg.AttachMenuBotsBox", response)
				}
				got, ok := box.AttachMenuBots.(*tg.AttachMenuBots)
				if !ok {
					t.Fatalf("attach menu bots = %T, want *tg.AttachMenuBots", box.AttachMenuBots)
				}
				if got.Hash != 0 || len(got.Bots) != 0 || len(got.Users) != 0 {
					t.Fatalf("attach menu bots = hash %d, %d bots, %d users; want empty", got.Hash, len(got.Bots), len(got.Users))
				}
			},
		},
		{
			name:       "sticker set",
			repeatable: true,
			request:    func() bin.Encoder { return &tg.MessagesGetStickerSetRequest{Stickerset: &tg.InputStickerSetEmpty{}} },
			response: func() bin.Decoder {
				return &tg.MessagesStickerSetBox{}
			},
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				box, ok := response.(*tg.MessagesStickerSetBox)
				if !ok {
					t.Fatalf("response = %T, want *tg.MessagesStickerSetBox", response)
				}
				if _, ok := box.StickerSet.(*tg.MessagesStickerSetNotModified); !ok {
					t.Fatalf("sticker set = %T, want *tg.MessagesStickerSetNotModified", box.StickerSet)
				}
			},
		},
		{
			name:     "all drafts",
			request:  func() bin.Encoder { return &tg.MessagesGetAllDraftsRequest{} },
			response: func() bin.Decoder { return &tg.UpdatesBox{} },
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				box, ok := response.(*tg.UpdatesBox)
				if !ok {
					t.Fatalf("response = %T, want *tg.UpdatesBox", response)
				}
				got, ok := box.Updates.(*tg.Updates)
				if !ok {
					t.Fatalf("drafts = %T, want *tg.Updates", box.Updates)
				}
				if len(got.Updates) != 0 || len(got.Users) != 0 || len(got.Chats) != 0 || got.Seq != 0 {
					t.Fatalf("drafts = %d updates, %d users, %d chats, seq %d; want empty", len(got.Updates), len(got.Users), len(got.Chats), got.Seq)
				}
			},
		},
		{
			name:     "received messages",
			request:  func() bin.Encoder { return &tg.MessagesReceivedMessagesRequest{} },
			response: func() bin.Decoder { return &tg.ReceivedNotifyMessageVector{} },
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				got, ok := response.(*tg.ReceivedNotifyMessageVector)
				if !ok {
					t.Fatalf("response = %T, want *tg.ReceivedNotifyMessageVector", response)
				}
				if len(got.Elems) != 0 {
					t.Fatalf("received messages = %d, want empty", len(got.Elems))
				}
			},
		},
		{
			name:     "joined communities",
			request:  func() bin.Encoder { return &tg.CommunitiesGetJoinedCommunitiesRequest{} },
			response: func() bin.Decoder { return &tg.MessagesChatsBox{} },
			assert: func(t *testing.T, response bin.Decoder) {
				t.Helper()
				box, ok := response.(*tg.MessagesChatsBox)
				if !ok {
					t.Fatalf("response = %T, want *tg.MessagesChatsBox", response)
				}
				got, ok := box.Chats.(*tg.MessagesChats)
				if !ok {
					t.Fatalf("joined communities = %T, want *tg.MessagesChats", box.Chats)
				}
				if len(got.Chats) != 0 {
					t.Fatalf("joined communities = %d, want empty", len(got.Chats))
				}
			},
		},
	}
}

type startupDispatcherTransport struct {
	mu   sync.Mutex
	sent [][]byte
}

func (t *startupDispatcherTransport) Send(_ context.Context, b *bin.Buffer) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sent = append(t.sent, slices.Clone(b.Buf))
	return nil
}

func (*startupDispatcherTransport) Recv(context.Context, *bin.Buffer) error { return io.EOF }
func (*startupDispatcherTransport) Close() error                            { return nil }

func (transport *startupDispatcherTransport) result(t *testing.T, key crypto.AuthKey, msgID int64) []byte {
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

func dispatchStartup(t *testing.T, h mtproto.Handler, method startupMethod, userID int64, provisional bool) []byte {
	t.Helper()
	key := testKey()
	transport := &startupDispatcherTransport{}
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

func TestStartupMethodsThroughDispatcher(t *testing.T) {
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

	for _, method := range startupMethods() {
		t.Run(method.name, func(t *testing.T) {
			t.Run("authorized", func(t *testing.T) {
				body := dispatchStartup(t, h, method, 1, false)
				response := method.response()
				if err := response.Decode(&bin.Buffer{Buf: body}); err != nil {
					t.Fatalf("decode authorized response: %v", err)
				}
				method.assert(t, response)
			})

			if method.repeatable {
				t.Run("repeated", func(t *testing.T) {
					first := dispatchStartup(t, h, method, 1, false)
					second := dispatchStartup(t, h, method, 1, false)
					if !bytes.Equal(first, second) {
						t.Fatalf("repeated response changed: %x != %x", first, second)
					}
				})
			}

			for _, session := range []struct {
				name        string
				userID      int64
				provisional bool
			}{
				{name: "unauthenticated"},
				{name: "provisional", userID: 1, provisional: true},
			} {
				t.Run(session.name, func(t *testing.T) {
					body := dispatchStartup(t, h, method, session.userID, session.provisional)
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
