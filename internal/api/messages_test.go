package api_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestPeerUserIDValidatesAccessHash(t *testing.T) {
	t.Parallel()

	id, err := api.PeerUserID(&tg.InputPeerUser{UserID: 5, AccessHash: 5})
	if err != nil || id != 5 {
		t.Fatalf("valid peer: id=%d err=%v", id, err)
	}

	for name, peer := range map[string]tg.InputPeerClass{
		"wrong hash": &tg.InputPeerUser{UserID: 5, AccessHash: 6},
		"zero id":    &tg.InputPeerUser{UserID: 0, AccessHash: 0},
		"self":       &tg.InputPeerSelf{},
		"chat":       &tg.InputPeerChat{ChatID: 1},
	} {
		if _, err := api.PeerUserID(peer); err == nil {
			t.Errorf("%s: expected PEER_ID_INVALID, got nil", name)
		}
	}
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}

func TestHandleSendMessagePersistsAndReturnsUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551291001")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551291002")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	enc, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer:     &tg.InputPeerUser{UserID: b.ID, AccessHash: b.ID},
		Message:  "hello",
		RandomID: 4242,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result type = %T, want *tg.Updates", enc)
	}
	var sawMsgID, sawNewMsg bool
	for _, u := range ups.Updates {
		switch up := u.(type) {
		case *tg.UpdateMessageID:
			if up.RandomID == 4242 {
				sawMsgID = true
			}
		case *tg.UpdateNewMessage:
			if m, ok := up.Message.(*tg.Message); ok && m.Message == "hello" && m.Out {
				sawNewMsg = true
			}
		}
	}
	if !sawMsgID || !sawNewMsg {
		t.Fatalf("updates missing pieces: msgID=%v newMsg=%v", sawMsgID, sawNewMsg)
	}

	// Recipient got the inbox copy.
	recv, ok, err := s.MessageByOwnerLocal(ctx, b.ID, 1)
	if err != nil || !ok {
		t.Fatalf("recipient copy: ok=%v err=%v", ok, err)
	}
	if recv.Text != "hello" || recv.Out {
		t.Fatalf("recipient copy wrong: %+v", recv)
	}
}

func TestHandleSendMessageUnauthorized(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.SendMessageForTest(s, 0, &tg.MessagesSendMessageRequest{
		Peer:    &tg.InputPeerUser{UserID: 1, AccessHash: 1},
		Message: "x",
	})
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Message != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("unbound send: got %v, want AUTH_KEY_UNREGISTERED", err)
	}
}
