package api_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
)

func TestGetPeerDialogsReturnsRequestedUserDialog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	viewer, err := s.CreateUser(ctx, "+15551298001")
	if err != nil {
		t.Fatalf("viewer: %v", err)
	}
	peer, err := s.CreateUser(ctx, "+15551298002")
	if err != nil {
		t.Fatalf("peer: %v", err)
	}
	if _, err := api.SendMessageForTest(s, peer.ID, &tg.MessagesSendMessageRequest{
		Peer:     api.InputPeerUser(peer.ID, viewer.ID),
		Message:  "hello",
		RandomID: 98001,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}

	enc, err := api.GetPeerDialogsForTest(s, viewer.ID, &tg.MessagesGetPeerDialogsRequest{
		Peers: []tg.InputDialogPeerClass{
			&tg.InputDialogPeer{Peer: api.InputPeerUser(viewer.ID, peer.ID)},
		},
	})
	if err != nil {
		t.Fatalf("get peer dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesPeerDialogs)
	if !ok {
		t.Fatalf("result = %T, want *tg.MessagesPeerDialogs", enc)
	}
	if len(res.Dialogs) != 1 || len(res.Messages) != 1 {
		t.Fatalf("dialogs/messages = %d/%d, want 1/1", len(res.Dialogs), len(res.Messages))
	}
	dialog, ok := res.Dialogs[0].(*tg.Dialog)
	if !ok {
		t.Fatalf("dialog = %T, want *tg.Dialog", res.Dialogs[0])
	}
	peerRef, ok := dialog.Peer.(*tg.PeerUser)
	if !ok || peerRef.UserID != peer.ID {
		t.Fatalf("dialog peer = %T/%v, want user %d", dialog.Peer, dialog.Peer, peer.ID)
	}
	if res.Messages[0].GetID() != 1 {
		t.Fatalf("top message id = %d, want 1", res.Messages[0].GetID())
	}
	if res.State.Pts != 1 || res.State.UnreadCount != 1 {
		t.Fatalf("state = %+v, want pts=1 unread=1", res.State)
	}
	assertEncodes(t, enc)
}

func TestGetPeerDialogsValidatesBeforeStorage(t *testing.T) {
	t.Parallel()

	tooMany := make([]tg.InputDialogPeerClass, 101)
	for i := range tooMany {
		tooMany[i] = &tg.InputDialogPeer{Peer: &tg.InputPeerChat{ChatID: int64(i + 1)}}
	}

	tests := []struct {
		name   string
		userID int64
		req    *tg.MessagesGetPeerDialogsRequest
		want   string
	}{
		{
			name:   "unbound before peer validation",
			userID: 0,
			req: &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{
				&tg.InputDialogPeer{Peer: &tg.InputPeerEmpty{}},
			}},
			want: "AUTH_KEY_UNREGISTERED",
		},
		{
			name:   "empty",
			userID: 1,
			req:    &tg.MessagesGetPeerDialogsRequest{},
			want:   "INPUT_PEERS_EMPTY",
		},
		{
			name:   "too many",
			userID: 1,
			req:    &tg.MessagesGetPeerDialogsRequest{Peers: tooMany},
			want:   "LIMIT_INVALID",
		},
		{
			name:   "folder",
			userID: 1,
			req: &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{
				&tg.InputDialogPeerFolder{FolderID: 1},
			}},
			want: "PEER_ID_INVALID",
		},
		{
			name:   "community",
			userID: 1,
			req: &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{
				&tg.InputDialogPeerCommunity{Community: &tg.InputChannel{ChannelID: 1, AccessHash: 1}},
			}},
			want: "PEER_ID_INVALID",
		},
		{
			name:   "nested empty",
			userID: 1,
			req: &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{
				&tg.InputDialogPeer{Peer: &tg.InputPeerEmpty{}},
			}},
			want: "PEER_ID_INVALID",
		},
		{
			name:   "wrong user hash",
			userID: 1,
			req: &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{
				&tg.InputDialogPeer{Peer: api.InputPeerUser(2, 9)},
			}},
			want: "PEER_ID_INVALID",
		},
		{
			name:   "wrong channel hash",
			userID: 1,
			req: &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{
				&tg.InputDialogPeer{Peer: &tg.InputPeerChannel{ChannelID: 9, AccessHash: api.DeriveChannelHash(2, 9)}},
			}},
			want: "PEER_ID_INVALID",
		},
		{
			name:   "zero user id",
			userID: 1,
			req: &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{
				&tg.InputDialogPeer{Peer: api.InputPeerUser(1, 0)},
			}},
			want: "PEER_ID_INVALID",
		},
		{
			name:   "zero chat id",
			userID: 1,
			req: &tg.MessagesGetPeerDialogsRequest{Peers: []tg.InputDialogPeerClass{
				&tg.InputDialogPeer{Peer: &tg.InputPeerChat{ChatID: 0}},
			}},
			want: "PEER_ID_INVALID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := api.GetPeerDialogsForTest(nil, tt.userID, tt.req)
			rpcError(t, err, tt.want)
		})
	}
}

func TestGetPeerDialogsPreservesFirstOrderAndOmitsUnknown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	viewer, err := s.CreateUser(ctx, "+15551298003")
	if err != nil {
		t.Fatalf("viewer: %v", err)
	}
	first, err := s.CreateUser(ctx, "+15551298004")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := s.CreateUser(ctx, "+15551298005")
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	unknown, err := s.CreateUser(ctx, "+15551298006")
	if err != nil {
		t.Fatalf("unknown: %v", err)
	}
	if _, err := api.SendMessageForTest(s, first.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(first.ID, viewer.ID), Message: "first", RandomID: 98003,
	}); err != nil {
		t.Fatalf("first send: %v", err)
	}
	if _, err := api.SendMessageForTest(s, second.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(second.ID, viewer.ID), Message: "second", RandomID: 98004,
	}); err != nil {
		t.Fatalf("second send: %v", err)
	}

	beforeState, err := s.State(ctx, viewer.ID)
	if err != nil {
		t.Fatalf("state before: %v", err)
	}
	beforeDialogs, err := s.Dialogs(ctx, viewer.ID, 0, 100)
	if err != nil {
		t.Fatalf("dialogs before: %v", err)
	}
	beforeEvents, err := s.EventsSince(ctx, viewer.ID, 0)
	if err != nil {
		t.Fatalf("events before: %v", err)
	}

	enc, err := api.GetPeerDialogsForTest(s, viewer.ID, &tg.MessagesGetPeerDialogsRequest{
		Peers: []tg.InputDialogPeerClass{
			&tg.InputDialogPeer{Peer: api.InputPeerUser(viewer.ID, second.ID)},
			&tg.InputDialogPeer{Peer: api.InputPeerUser(viewer.ID, unknown.ID)},
			&tg.InputDialogPeer{Peer: api.InputPeerUser(viewer.ID, first.ID)},
			&tg.InputDialogPeer{Peer: api.InputPeerUser(viewer.ID, second.ID)},
		},
	})
	if err != nil {
		t.Fatalf("get peer dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesPeerDialogs)
	if !ok {
		t.Fatalf("result = %T, want *tg.MessagesPeerDialogs", enc)
	}
	if len(res.Dialogs) != 2 || len(res.Messages) != 2 {
		t.Fatalf("dialogs/messages = %d/%d, want 2/2", len(res.Dialogs), len(res.Messages))
	}
	for i, want := range []int64{second.ID, first.ID} {
		d, ok := res.Dialogs[i].(*tg.Dialog)
		if !ok {
			t.Fatalf("dialog %d = %T, want *tg.Dialog", i, res.Dialogs[i])
		}
		peer, ok := d.Peer.(*tg.PeerUser)
		if !ok || peer.UserID != want {
			t.Fatalf("dialog %d peer = %T/%v, want user %d", i, d.Peer, d.Peer, want)
		}
	}
	if got := res.Messages[0].GetID(); got != 2 {
		t.Fatalf("first returned message id = %d, want 2", got)
	}
	if got := res.Messages[1].GetID(); got != 1 {
		t.Fatalf("second returned message id = %d, want 1", got)
	}
	if res.State.Pts != beforeState.Pts || res.State.UnreadCount != beforeState.UnreadCount {
		t.Fatalf("response state = %+v, want pts=%d unread=%d", res.State, beforeState.Pts, beforeState.UnreadCount)
	}
	assertEncodes(t, enc)

	afterState, err := s.State(ctx, viewer.ID)
	if err != nil {
		t.Fatalf("state after: %v", err)
	}
	afterDialogs, err := s.Dialogs(ctx, viewer.ID, 0, 100)
	if err != nil {
		t.Fatalf("dialogs after: %v", err)
	}
	afterEvents, err := s.EventsSince(ctx, viewer.ID, 0)
	if err != nil {
		t.Fatalf("events after: %v", err)
	}
	if !reflect.DeepEqual(beforeState, afterState) || !reflect.DeepEqual(beforeDialogs, afterDialogs) || !reflect.DeepEqual(beforeEvents, afterEvents) {
		t.Fatal("getPeerDialogs changed state, dialogs, or events")
	}
}

func TestGetPeerDialogsRemovedChatKeepsForbiddenDialog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551298007", "+15551298008")
	if _, err := api.SendMessageForTest(s, users[0].ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerChat(users[0].ID, chat.ID), Message: "chat", RandomID: 98005,
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, _, _, err := s.RemoveChatUser(ctx, chat.ID, users[1].ID, users[0].ID); err != nil {
		t.Fatalf("remove viewer: %v", err)
	}

	enc, err := api.GetPeerDialogsForTest(s, users[1].ID, &tg.MessagesGetPeerDialogsRequest{
		Peers: []tg.InputDialogPeerClass{
			&tg.InputDialogPeer{Peer: api.InputPeerChat(users[1].ID, chat.ID)},
		},
	})
	if err != nil {
		t.Fatalf("get peer dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesPeerDialogs)
	if !ok || len(res.Dialogs) != 1 || len(res.Messages) != 1 || len(res.Chats) != 1 {
		t.Fatalf("result = %T dialogs=%d messages=%d chats=%d", enc, len(res.Dialogs), len(res.Messages), len(res.Chats))
	}
	forbidden, ok := res.Chats[0].(*tg.ChatForbidden)
	if !ok || forbidden.ID != chat.ID || forbidden.Title != "" {
		t.Fatalf("chat = %T/%v, want empty ChatForbidden for %d", res.Chats[0], res.Chats[0], chat.ID)
	}
	assertEncodes(t, enc)
}

func TestGetPeerDialogsOmitsRemovedAndBannedChannels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, member, removed := channelWith(t, s, "+15551298009", "+15551298010")
	joinChannelByInvite(t, s, removed, member.ID)
	if _, err := sendToChannel(t, s, creator.ID, removed.ID, "removed", 98006); err != nil {
		t.Fatalf("removed channel send: %v", err)
	}

	bannedCreator, _, banned := channelWith(t, s, "+15551298011", "+15551298012")
	joinChannelByInvite(t, s, banned, member.ID)
	if _, err := sendToChannel(t, s, bannedCreator.ID, banned.ID, "banned", 98007); err != nil {
		t.Fatalf("banned channel send: %v", err)
	}
	activeEnc, err := api.GetPeerDialogsForTest(s, member.ID, &tg.MessagesGetPeerDialogsRequest{
		Peers: []tg.InputDialogPeerClass{
			&tg.InputDialogPeer{Peer: api.InputPeerChannel(member.ID, removed.ID)},
		},
	})
	if err != nil {
		t.Fatalf("get active channel dialog: %v", err)
	}
	active, ok := activeEnc.(*tg.MessagesPeerDialogs)
	if !ok || len(active.Dialogs) != 1 || len(active.Messages) != 1 || len(active.Chats) != 1 {
		t.Fatalf("active result = %T dialogs=%d messages=%d chats=%d", activeEnc, len(active.Dialogs), len(active.Messages), len(active.Chats))
	}
	activeChannel, ok := active.Chats[0].(*tg.Channel)
	if !ok || activeChannel.ID != removed.ID || activeChannel.AccessHash != api.DeriveChannelHash(member.ID, removed.ID) {
		t.Fatalf("active channel = %T/%v, want viewer-scoped channel %d", active.Chats[0], active.Chats[0], removed.ID)
	}
	assertEncodes(t, activeEnc)
	if left, err := s.LeaveChannel(ctx, removed.ID, member.ID); err != nil || !left {
		t.Fatalf("leave removed channel: left=%v err=%v", left, err)
	}
	banChannelMember(t, ctx, dsn, banned.ID, member.ID, time.Now().Add(time.Hour))

	enc, err := api.GetPeerDialogsForTest(s, member.ID, &tg.MessagesGetPeerDialogsRequest{
		Peers: []tg.InputDialogPeerClass{
			&tg.InputDialogPeer{Peer: api.InputPeerChannel(member.ID, removed.ID)},
			&tg.InputDialogPeer{Peer: api.InputPeerChannel(member.ID, banned.ID)},
		},
	})
	if err != nil {
		t.Fatalf("get peer dialogs: %v", err)
	}
	res, ok := enc.(*tg.MessagesPeerDialogs)
	if !ok {
		t.Fatalf("result = %T, want *tg.MessagesPeerDialogs", enc)
	}
	if len(res.Dialogs) != 0 || len(res.Messages) != 0 || len(res.Chats) != 0 {
		t.Fatalf("unauthorized channels leaked dialogs=%d messages=%d chats=%d", len(res.Dialogs), len(res.Messages), len(res.Chats))
	}
	assertEncodes(t, enc)
}
