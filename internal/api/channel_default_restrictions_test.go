package api_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/store"
)

type megagroupWriteStats struct {
	messages int64
	events   int64
}

func channelWriteStats(t *testing.T, conn *pgx.Conn, channelID int64) megagroupWriteStats {
	t.Helper()
	ctx := context.Background()
	var got megagroupWriteStats
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM channel_messages WHERE channel_id = $1`, channelID).Scan(&got.messages); err != nil {
		t.Fatalf("count channel messages: %v", err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM channel_events WHERE channel_id = $1`, channelID).Scan(&got.events); err != nil {
		t.Fatalf("count channel events: %v", err)
	}
	return got
}

func assertChannelWriteStats(t *testing.T, conn *pgx.Conn, channelID int64, want megagroupWriteStats) {
	t.Helper()
	if got := channelWriteStats(t, conn, channelID); got != want {
		t.Fatalf("channel write stats = %+v, want %+v", got, want)
	}
}

func listenForChannelPosts(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect channel post listener: %v", err)
	}
	if _, err = conn.Exec(ctx, "LISTEN "+store.ChannelPost); err != nil {
		if closeErr := conn.Close(ctx); closeErr != nil {
			t.Errorf("close channel post listener after LISTEN failure: %v", closeErr)
		}
		t.Fatalf("listen for channel posts: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close channel post listener: %v", err)
		}
	})
	return conn
}

func assertNoChannelPostNotification(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	notification, err := conn.WaitForNotification(ctx)
	if err == nil {
		t.Fatalf("unexpected channel post notification: %+v", notification)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting for channel post notification: %v", err)
	}
}

func TestMegagroupDefaultTextRestrictionsBlockMembersAndPreserveAdmins(t *testing.T) {
	t.Parallel()
	for i, right := range []string{"send_messages", "send_plain"} {
		t.Run(right, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() {
				if err := conn.Close(ctx); err != nil {
					t.Errorf("close: %v", err)
				}
			}()

			base := i * 3
			creator, err := s.CreateUser(ctx, fmt.Sprintf("+155512930%03d", base+1))
			if err != nil {
				t.Fatalf("create creator: %v", err)
			}
			admin, err := s.CreateUser(ctx, fmt.Sprintf("+155512930%03d", base+2))
			if err != nil {
				t.Fatalf("create admin: %v", err)
			}
			member, err := s.CreateUser(ctx, fmt.Sprintf("+155512930%03d", base+3))
			if err != nil {
				t.Fatalf("create member: %v", err)
			}
			channel, err := s.CreateChannel(ctx, creator.ID, "Restricted team", "", true)
			if err != nil {
				t.Fatalf("create megagroup: %v", err)
			}
			joinChannelByInvite(t, s, channel, admin.ID)
			joinChannelByInvite(t, s, channel, member.ID)
			if err = s.SetChannelRole(ctx, channel.ID, creator.ID, admin.ID, 1); err != nil {
				t.Fatalf("promote admin: %v", err)
			}
			channelExec(t, ctx, dsn,
				`UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, channel.ID, []string{right})

			listener := listenForChannelPosts(t, ctx, dsn)
			before := channelWriteStats(t, conn, channel.ID)
			_, err = sendToChannel(t, s, member.ID, channel.ID, "member post", 93001)
			wantRPC(t, err, "CHAT_WRITE_FORBIDDEN")
			assertChannelWriteStats(t, conn, channel.ID, before)
			assertNoChannelPostNotification(t, listener)

			memberState, ok, err := s.ChannelMemberOf(ctx, channel.ID, member.ID)
			if err != nil || !ok {
				t.Fatalf("read member state: ok=%v err=%v", ok, err)
			}
			if memberState.Role != 0 || memberState.BannedUntil != nil {
				t.Fatalf("denied post changed membership: %+v", memberState)
			}

			if _, err = sendToChannel(t, s, admin.ID, channel.ID, "admin post", 93002); err != nil {
				t.Fatalf("restricted admin post: %v", err)
			}
			if _, err = sendToChannel(t, s, creator.ID, channel.ID, "creator post", 93003); err != nil {
				t.Fatalf("restricted creator post: %v", err)
			}
			assertChannelWriteStats(t, conn, channel.ID, megagroupWriteStats{
				messages: before.messages + 2,
				events:   before.events + 2,
			})
		})
	}
}

func TestMegagroupMediaAndForwardDestinationsRemainUnavailable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551293101")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551293102")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	channel, err := s.CreateChannel(ctx, creator.ID, "No channel media", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, channel, member.ID)
	channelExec(t, ctx, dsn,
		`UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, channel.ID, []string{"send_media", "send_docs"})
	listener := listenForChannelPosts(t, ctx, dsn)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	before := channelWriteStats(t, conn, channel.ID)

	_, err = api.SendMediaForTest(s, member.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer: channelPeer(member.ID, channel.ID), Media: uploadedDocument(93101, 1, "file.bin", "application/octet-stream"),
		Message: "media", RandomID: 93102,
	})
	wantRPC(t, err, "PEER_ID_INVALID")
	_, err = api.ForwardMessagesForTest(s, member.ID, &tg.MessagesForwardMessagesRequest{
		FromPeer: &tg.InputPeerSelf{}, ID: []int{1}, ToPeer: channelPeer(member.ID, channel.ID), RandomID: []int64{93103},
	})
	wantRPC(t, err, "PEER_ID_INVALID")
	assertChannelWriteStats(t, conn, channel.ID, before)
	assertNoChannelPostNotification(t, listener)
}
