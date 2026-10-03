package api

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestDeliverPinnedKeepsOneChatPinSnapshotAcrossMembers(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		unpin bool
	}{
		{name: "repin"},
		{name: "unpin", unpin: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
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

			creator, err := s.CreateUser(ctx, "+15559701001")
			if err != nil {
				t.Fatalf("create creator: %v", err)
			}
			member, err := s.CreateUser(ctx, "+15559701002")
			if err != nil {
				t.Fatalf("create member: %v", err)
			}
			priorSender, err := s.CreateUser(ctx, "+15559701003")
			if err != nil {
				t.Fatalf("create prior sender: %v", err)
			}
			if _, _, _, _, err := s.SendMessage(ctx, priorSender.ID, member.ID, "prior", 1, 0, 0); err != nil {
				t.Fatalf("seed member-local ID offset: %v", err)
			}

			chat, err := s.CreateChat(ctx, creator.ID, "pin snapshot", []int64{member.ID})
			if err != nil {
				t.Fatalf("create chat: %v", err)
			}
			first, _, _, err := s.SendChatMessage(ctx, store.FanOut{
				ChatID: chat.ID, FromID: creator.ID, Text: "first pinned text", RandomID: 11,
			})
			if err != nil {
				t.Fatalf("send first message: %v", err)
			}
			second, _, _, err := s.SendChatMessage(ctx, store.FanOut{
				ChatID: chat.ID, FromID: creator.ID, Text: "second pinned text", RandomID: 12,
			})
			if err != nil {
				t.Fatalf("send second message: %v", err)
			}
			firstPin := pinnedMessageID(t, first.LocalID)
			if _, _, err := s.SetChatPinnedMessage(ctx, chat.ID, creator.ID, &firstPin); err != nil {
				t.Fatalf("pin first message: %v", err)
			}
			firstIDs := make(map[int64]int64, 2)
			for _, userID := range []int64{creator.ID, member.ID} {
				localID, found, err := s.ChatPinnedMessageForOwner(ctx, chat.ID, userID)
				if err != nil || !found {
					t.Fatalf("resolve initial pin for %d: found=%v err=%v", userID, found, err)
				}
				firstIDs[userID] = localID
			}
			if firstIDs[creator.ID] == firstIDs[member.ID] {
				t.Fatalf("test copies have the same local ID %d", firstIDs[creator.ID])
			}

			registry := mtproto.NewSessionRegistry()
			creatorTransport := &recordingNotifyTransport{}
			memberTransport := &recordingNotifyTransport{}
			creatorKey := retryTestKey(41)
			memberKey := retryTestKey(42)
			creatorConn := mtproto.NewTestConn(creatorTransport, creatorKey)
			creatorConn.SetOwner(creator.ID)
			if !registry.Add(creator.ID, creatorConn) {
				t.Fatal("register creator connection")
			}
			t.Cleanup(func() { registry.Remove(creator.ID, creatorConn) })
			memberConn := mtproto.NewTestConn(memberTransport, memberKey)
			memberConn.SetOwner(member.ID)
			if !registry.Add(member.ID, memberConn) {
				t.Fatal("register member connection")
			}
			t.Cleanup(func() { registry.Remove(member.ID, memberConn) })

			updater := NewUpdater(s, registry, nil, pgtest.PeerDeriver())
			delivered := make(chan struct{}, 2)
			_, stop, err := store.StartListener(ctx, dsn,
				func(context.Context, int64) {},
				func(context.Context, int64, int64) {},
				func(context.Context, int64, int64) {},
				func(context.Context, int64) {},
				func(context.Context, int64, int64) {},
				func(context.Context, int64, bool) {},
				func(context.Context, int64, int) {},
				func(context.Context, int64, int64, int64) {},
				func(pushCtx context.Context, peerType store.PeerType, peerID int64, pinnedMsgID int32) {
					updater.DeliverPinned(pushCtx, peerType, peerID, pinnedMsgID)
					delivered <- struct{}{}
				},
				nil,
			)
			if err != nil {
				t.Fatalf("start pin listener: %v", err)
			}
			t.Cleanup(func() {
				if err := stop(); err != nil {
					t.Errorf("stop pin listener: %v", err)
				}
			})
			if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
				t.Fatalf("wait for pin listener: %v", err)
			}
			rpcCtx, cancelRPC := context.WithCancel(ctx)
			interleaved := false
			updater.pinSnapshotHook = func() {
				if interleaved {
					return
				}
				interleaved = true
				var nextPin *int32
				if !tc.unpin {
					id := pinnedMessageID(t, second.LocalID)
					nextPin = &id
				}
				if _, _, err := s.SetChatPinnedMessage(ctx, chat.ID, creator.ID, nextPin); err != nil {
					t.Errorf("concurrent pin update: %v", err)
				}
			}

			cancelRPC()
			notifyPin := func(pinnedMsgID int32) {
				t.Helper()
				notifyCtx, cancelNotify := store.NotificationContext(rpcCtx)
				defer cancelNotify()
				if err := s.Notify(notifyCtx, store.ChannelPinned, store.PinnedPayload(store.PeerTypeChat, chat.ID, pinnedMsgID)); err != nil {
					t.Fatalf("notify pin after caller cancellation: %v", err)
				}
				select {
				case <-delivered:
				case <-time.After(5 * time.Second):
					t.Fatal("pin notification was not delivered")
				}
			}
			notifyPin(firstPin)
			if !interleaved {
				t.Fatal("pin update did not interleave with recipient resolution")
			}

			assertPinnedSnapshotDelivery(t, s, creatorKey, creatorTransport, 0, creator.ID, true, firstIDs, "first pinned text")
			assertPinnedSnapshotDelivery(t, s, memberKey, memberTransport, 0, member.ID, true, firstIDs, "first pinned text")
			if tc.unpin {
				notifyPin(0)
				assertPinnedSnapshotDelivery(t, s, creatorKey, creatorTransport, 1, creator.ID, false, nil, "")
				assertPinnedSnapshotDelivery(t, s, memberKey, memberTransport, 1, member.ID, false, nil, "")
				return
			}

			secondIDs := make(map[int64]int64, 2)
			for _, userID := range []int64{creator.ID, member.ID} {
				localID, found, err := s.ChatPinnedMessageForOwner(ctx, chat.ID, userID)
				if err != nil || !found {
					t.Fatalf("resolve repinned copy for %d: found=%v err=%v", userID, found, err)
				}
				secondIDs[userID] = localID
			}
			notifyPin(pinnedMessageID(t, second.LocalID))
			assertPinnedSnapshotDelivery(t, s, creatorKey, creatorTransport, 1, creator.ID, true, secondIDs, "second pinned text")
			assertPinnedSnapshotDelivery(t, s, memberKey, memberTransport, 1, member.ID, true, secondIDs, "second pinned text")
		})
	}
}

func pinnedMessageID(t *testing.T, localID int64) int32 {
	t.Helper()
	if localID < math.MinInt32 || localID > math.MaxInt32 {
		t.Fatalf("message ID %d does not fit Telegram's 32-bit pin ID", localID)
	}
	return int32(localID) //nolint:gosec // bounds checked above for Telegram's 32-bit wire ID
}

func assertPinnedSnapshotDelivery(
	t *testing.T,
	s *store.Store,
	key crypto.AuthKey,
	transport *recordingNotifyTransport,
	frame int,
	ownerID int64,
	wantPinned bool,
	wantIDs map[int64]int64,
	wantText string,
) {
	t.Helper()
	frames := transport.framesFrom(frame)
	if len(frames) != 1 {
		t.Fatalf("delivery frame count = %d, want one", len(frames))
	}
	cipher := crypto.NewClientCipher(crypto.DefaultRand())
	data, err := cipher.DecryptFromBuffer(key, &bin.Buffer{Buf: frames[0]})
	if err != nil {
		t.Fatalf("decrypt pinned update: %v", err)
	}
	var short tg.UpdateShort
	if err := short.Decode(&bin.Buffer{Buf: data.Data()}); err != nil {
		t.Fatalf("decode pinned update: %v", err)
	}
	pinned, ok := short.Update.(*tg.UpdatePinnedMessages)
	if !ok {
		t.Fatalf("pushed update = %T, want *tg.UpdatePinnedMessages", short.Update)
	}
	if pinned.Pinned != wantPinned {
		t.Errorf("pinned = %t, want %t", pinned.Pinned, wantPinned)
	}
	wantID, hasID := wantIDs[ownerID]
	if hasID {
		if len(pinned.Messages) != 1 || pinned.Messages[0] != int(wantID) {
			t.Fatalf("message IDs = %v, want [%d] for owner %d", pinned.Messages, wantID, ownerID)
		}
		message, found, err := s.MessageByOwnerLocal(context.Background(), ownerID, wantID)
		if err != nil || !found {
			t.Fatalf("load pinned copy for owner %d: found=%v err=%v", ownerID, found, err)
		}
		if message.Text != wantText {
			t.Errorf("owner %d pinned text = %q, want %q", ownerID, message.Text, wantText)
		}
	} else if len(pinned.Messages) != 0 {
		t.Errorf("message IDs = %v, want none", pinned.Messages)
	}
}
