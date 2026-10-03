package api

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestChatAdminPushLeavesPtsSlotForNextMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	creator, err := s.CreateUser(ctx, "+15559701101")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15559701102")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	chat, err := s.CreateChat(ctx, creator.ID, "admin push pts", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	registry := mtproto.NewSessionRegistry()
	transport := &recordingNotifyTransport{}
	key := retryTestKey(81)
	conn := mtproto.NewTestConn(transport, key)
	conn.SetOwner(member.ID)
	if !registry.Add(member.ID, conn) {
		t.Fatal("register member connection")
	}
	t.Cleanup(func() { registry.Remove(member.ID, conn) })
	updater := NewUpdater(s, registry, nil, pgtest.PeerDeriver())

	_, stop, err := store.StartListener(ctx, dsn,
		updater.Deliver,
		func(context.Context, int64, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64) {},
		func(context.Context, int64, int64) {},
		func(context.Context, int64, bool) {},
		func(context.Context, int64, int) {},
		func(context.Context, int64, int64, int64) {},
		func(context.Context, store.PeerType, int64, int32) {},
		nil,
	)
	if err != nil {
		t.Fatalf("start listener: %v", err)
	}
	t.Cleanup(func() { _ = stop() }) //nolint:errcheck // teardown
	if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
		t.Fatalf("wait for listener: %v", err)
	}

	var request bin.Buffer
	if err := (&tg.MessagesEditChatAdminRequest{
		ChatID:  chat.ID,
		UserID:  InputUser(creator.ID, member.ID),
		IsAdmin: true,
	}).Encode(&request); err != nil {
		t.Fatalf("encode editChatAdmin: %v", err)
	}
	if _, err := testHandlers(s).handleEditChatAdmin(&mtproto.Request{
		Ctx: ctx, UserID: creator.ID, Buf: &request,
	}); err != nil {
		t.Fatalf("editChatAdmin: %v", err)
	}
	waitTransportCount(t, transport, 1)
	if got := conn.LastPushedPts(); got != 0 {
		t.Fatalf("pts watermark after transient promotion = %d, want 0", got)
	}
	state, err := s.State(ctx, member.ID)
	if err != nil || state.Pts != 0 {
		t.Fatalf("member state after promotion = %+v, err=%v, want pts 0", state, err)
	}

	sender, perOwner, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: creator.ID, Text: "next message", RandomID: 81001,
	})
	if err != nil {
		t.Fatalf("send next message: %v", err)
	}
	if err := s.Notify(ctx, store.ChannelUpdates, strconv.FormatInt(member.ID, 10)); err != nil {
		t.Fatalf("notify next message: %v", err)
	}
	waitTransportCount(t, transport, 2)
	if got := conn.LastPushedPts(); got != perOwner[member.ID] || got != 1 {
		t.Fatalf("pts watermark after next message = %d, want message pts 1", got)
	}

	recipient, ok, err := s.MessageByOwnerLocal(ctx, member.ID, sender.PeerLocalID)
	if err != nil || !ok {
		t.Fatalf("load member message copy: ok=%v err=%v", ok, err)
	}
	frames := decodeServerFrames(t, key, transport.framesFrom(0))
	if len(frames) != 2 || frames[0].push == nil || frames[0].rpc != nil {
		t.Fatalf("frames = %+v, want transient admin update then message push", frames)
	}
	if len(frames[0].push.Updates) != 1 {
		t.Fatalf("admin push updates = %d, want one", len(frames[0].push.Updates))
	}
	adminUpdate, ok := frames[0].push.Updates[0].(*tg.UpdateChatParticipantAdmin)
	if !ok || adminUpdate.ChatID != chat.ID || adminUpdate.UserID != member.ID || !adminUpdate.IsAdmin || adminUpdate.Version != chat.Version+1 {
		t.Fatalf("admin push update = %#v, want promotion for chat %d user %d at version %d", frames[0].push.Updates[0], chat.ID, member.ID, chat.Version+1)
	}
	assertNewMessageFrame(t, frames[1], recipient, 1, "message after promotion")
}
