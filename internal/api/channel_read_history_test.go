package api_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestChannelsReadHistoryAdvancesMonotonicallyAndPersists(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551239901")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	reader, err := s.CreateUser(ctx, "+15551239902")
	if err != nil {
		t.Fatalf("create reader: %v", err)
	}
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Read state"})
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}
	first := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "first", 123901)
	second := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "second", 123902)
	h := fullChannelDispatcher(s)

	requireChannelReadState(t, s, reader.ID, channel.ID, 0, 2)
	requirePeerChannelReadState(t, s, reader.ID, channel.ID, 0, 2)

	readerState, err := s.State(ctx, reader.ID)
	if err != nil {
		t.Fatalf("reader update state before read: %v", err)
	}
	readerEvents, err := s.EventsSince(ctx, reader.ID, 0)
	if err != nil {
		t.Fatalf("reader events before read: %v", err)
	}
	channelPts, err := s.ChannelState(ctx, channel.ID)
	if err != nil {
		t.Fatalf("channel state before read: %v", err)
	}
	channelEvents, err := s.ChannelEventsWindow(ctx, channel.ID, 0, channelPts, 10)
	if err != nil {
		t.Fatalf("channel events before read: %v", err)
	}

	if ok, rpc := channelsReadHistoryViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(first.LocalID)); rpc != nil || !ok {
		t.Fatalf("partial channels.readHistory: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, first.LocalID, 1)
	requirePeerChannelReadState(t, s, reader.ID, channel.ID, first.LocalID, 1)

	if ok, rpc := channelsReadHistoryViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(second.LocalID)); rpc != nil || !ok {
		t.Fatalf("channels.readHistory through second post: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, second.LocalID, 0)
	requirePeerChannelReadState(t, s, reader.ID, channel.ID, second.LocalID, 0)

	fullResponse, rpc := getFullChannelViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID))
	if rpc != nil {
		t.Fatalf("reader getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	full := fullChannelInfo(t, fullResponse)
	if full.ReadInboxMaxID != int(second.LocalID) || full.UnreadCount != 0 {
		t.Fatalf("full channel read state = marker %d unread %d, want %d/0", full.ReadInboxMaxID, full.UnreadCount, second.LocalID)
	}

	// A fresh store handle models another authenticated session and a server
	// restart: both observe the committed marker on their next fetch.
	reopened, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened store: %v", err)
		}
	})
	requireChannelReadState(t, reopened, reader.ID, channel.ID, second.LocalID, 0)
	requirePeerChannelReadState(t, reopened, reader.ID, channel.ID, second.LocalID, 0)

	if ok, rpc := channelsReadHistoryViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(first.LocalID)); rpc != nil || !ok {
		t.Fatalf("lower repeated channels.readHistory: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, second.LocalID, 0)

	third := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "third", 123903)
	requireChannelReadState(t, reopened, reader.ID, channel.ID, second.LocalID, 1)
	if ok, rpc := channelsReadHistoryViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(second.LocalID)); rpc != nil || !ok {
		t.Fatalf("read through second post after later post: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, second.LocalID, 1)
	if ok, rpc := channelsReadHistoryViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(third.LocalID+100)); rpc != nil || !ok {
		t.Fatalf("clamped channels.readHistory: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, third.LocalID, 0)

	readerStateAfter, err := s.State(ctx, reader.ID)
	if err != nil {
		t.Fatalf("reader update state after read: %v", err)
	}
	if readerStateAfter.Pts != readerState.Pts {
		t.Errorf("reader pts changed from %d to %d", readerState.Pts, readerStateAfter.Pts)
	}
	readerEventsAfter, err := s.EventsSince(ctx, reader.ID, 0)
	if err != nil {
		t.Fatalf("reader events after read: %v", err)
	}
	if !reflect.DeepEqual(readerEventsAfter, readerEvents) {
		t.Errorf("channel read changed reader events: before=%+v after=%+v", readerEvents, readerEventsAfter)
	}
	channelPtsAfter, err := s.ChannelState(ctx, channel.ID)
	if err != nil {
		t.Fatalf("channel state after read: %v", err)
	}
	channelEventsAfter, err := s.ChannelEventsWindow(ctx, channel.ID, 0, channelPtsAfter, 10)
	if err != nil {
		t.Fatalf("channel events after read: %v", err)
	}
	if channelPtsAfter != channelPts+1 || len(channelEventsAfter) != len(channelEvents)+1 {
		// The third post adds exactly one channel event. The read calls add none.
		t.Errorf("channel read changed channel pts/events: before pts=%d events=%+v after pts=%d events=%+v", channelPts, channelEvents, channelPtsAfter, channelEventsAfter)
	}
	if len(channelEventsAfter) >= len(channelEvents)+1 {
		if !reflect.DeepEqual(channelEventsAfter[:len(channelEvents)], channelEvents) || channelEventsAfter[len(channelEvents)].LocalID != third.LocalID {
			t.Errorf("channel events after later post = %+v, want only the later post appended", channelEventsAfter)
		}
	}
}

func TestNewChannelMemberStartsAtCommittedTopAndOwnPostsAreNotUnread(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551239911")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551239912")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Megagroup: true, Title: "New member"})
	first := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "before join", 123911)
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, member.ID); err != nil {
		t.Fatalf("join member: %v", err)
	}
	requireChannelReadState(t, s, member.ID, channel.ID, first.LocalID, 0)

	postReadHistoryChannelMessage(t, ctx, s, channel.ID, member.ID, "my own post", 123912)
	third := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "after join", 123913)
	requireChannelReadState(t, s, member.ID, channel.ID, first.LocalID, 1)
	if third.LocalID != first.LocalID+2 {
		t.Fatalf("post ids = %d then %d, want a member post between them", first.LocalID, third.LocalID)
	}
	channelExec(t, ctx, dsn, `UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`, channel.ID, third.LocalID)
	requireChannelReadState(t, s, member.ID, channel.ID, first.LocalID, 0)
	if ok, rpc := channelsReadHistoryViaDispatcher(t, fullChannelDispatcher(s), member.ID, false, api.InputChannel(member.ID, channel.ID), int(first.LocalID+1)); rpc != nil || !ok {
		t.Fatalf("read own post: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, member.ID, channel.ID, first.LocalID+1, 0)
	if left, err := s.LeaveChannel(ctx, channel.ID, member.ID); err != nil || !left {
		t.Fatalf("leave channel: left=%v err=%v", left, err)
	}
	if got := readChannelMarker(t, ctx, dsn, channel.ID, member.ID); got != 0 {
		t.Fatalf("leave retained marker %d, want the membership-scoped row removed", got)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, member.ID); err != nil {
		t.Fatalf("rejoin channel: %v", err)
	}
	requireChannelReadState(t, s, member.ID, channel.ID, third.LocalID, 0)
}

func TestChannelAdmissionPathsStartReadStateAtCommittedTop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := readHistoryUser(t, s, ctx, "+15551239913")
	inviteMember := readHistoryUser(t, s, ctx, "+15551239914")
	addedMember := readHistoryUser(t, s, ctx, "+15551239915")
	publicMember := readHistoryUser(t, s, ctx, "+15551239916")

	inviteChannel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Invite top"})
	inviteTop := postReadHistoryChannelMessage(t, ctx, s, inviteChannel.ID, creator.ID, "existing", 123914)
	invite, err := s.CreateChannelInvite(ctx, inviteChannel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, inviteMember.ID); err != nil {
		t.Fatalf("join by invite: %v", err)
	}
	requireChannelReadState(t, s, inviteMember.ID, inviteChannel.ID, inviteTop.LocalID, 0)

	addedChannel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Added top"})
	addedTop := postReadHistoryChannelMessage(t, ctx, s, addedChannel.ID, creator.ID, "existing", 123915)
	added, err := s.AddChannelMembers(ctx, addedChannel.ID, creator.ID, []int64{addedMember.ID})
	if err != nil {
		t.Fatalf("add member: %v", err)
	}
	if len(added) != 1 || added[0] != addedMember.ID {
		t.Fatalf("added members = %v, want [%d]", added, addedMember.ID)
	}
	requireChannelReadState(t, s, addedMember.ID, addedChannel.ID, addedTop.LocalID, 0)

	publicChannel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Public top"})
	publicTop := postReadHistoryChannelMessage(t, ctx, s, publicChannel.ID, creator.ID, "existing", 123916)
	if err := api.ClaimChannelUsernameForTest(s, publicChannel.ID, "publicreadtop"); err != nil {
		t.Fatalf("publish channel: %v", err)
	}
	if _, _, err := s.JoinChannelByUsername(ctx, publicChannel.ID, publicMember.ID); err != nil {
		t.Fatalf("join by username: %v", err)
	}
	requireChannelReadState(t, s, publicMember.ID, publicChannel.ID, publicTop.LocalID, 0)
}

func TestChannelUnreadCountSaturatesAtOneThousand(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator := readHistoryUser(t, s, ctx, "+15551239917")
	reader := readHistoryUser(t, s, ctx, "+15551239918")
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Unread cap"})
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}
	channelExec(t, ctx, dsn, `
		INSERT INTO channel_messages (channel_id, local_id, from_id, message)
		SELECT $1, post_id, $2, 'bulk-' || post_id::text
		FROM generate_series(1, 1002) AS post_id`, channel.ID, creator.ID)
	channelExec(t, ctx, dsn, `UPDATE channel_state SET pts = 1002, next_local_id = 1003 WHERE channel_id = $1`, channel.ID)
	channelExec(t, ctx, dsn, `
		INSERT INTO channel_events (channel_id, pts, type, local_id)
		SELECT $1, post_id, 1, post_id
		FROM generate_series(1, 1002) AS post_id`, channel.ID)
	requireChannelReadState(t, s, reader.ID, channel.ID, 0, 1000)
	requirePeerChannelReadState(t, s, reader.ID, channel.ID, 0, 1000)
	if ok, rpc := channelsReadHistoryViaDispatcher(t, fullChannelDispatcher(s), reader.ID, false, api.InputChannel(reader.ID, channel.ID), 1000); rpc != nil || !ok {
		t.Fatalf("partial capped read: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, 1000, 2)
}

func TestChannelsReadHistoryRefusesUnauthorizedReaders(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator := readHistoryUser(t, s, ctx, "+15551239921")
	member := readHistoryUser(t, s, ctx, "+15551239922")
	banned := readHistoryUser(t, s, ctx, "+15551239923")
	removed := readHistoryUser(t, s, ctx, "+15551239924")
	outsider := readHistoryUser(t, s, ctx, "+15551239925")
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Private reads"})
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	for _, user := range []store.User{member, banned, removed} {
		if _, _, err := s.JoinChannelByInvite(ctx, invite, user.ID); err != nil {
			t.Fatalf("join user %d: %v", user.ID, err)
		}
	}
	postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "unread", 123921)
	banUntil := time.Now().Add(time.Hour)
	if err := s.SetChannelBan(ctx, channel.ID, creator.ID, banned.ID, &banUntil, false); err != nil {
		t.Fatalf("ban member: %v", err)
	}
	if left, err := s.LeaveChannel(ctx, channel.ID, removed.ID); err != nil || !left {
		t.Fatalf("remove member: left=%v err=%v", left, err)
	}
	channelPtsBefore, err := s.ChannelState(ctx, channel.ID)
	if err != nil {
		t.Fatalf("channel state before refused reads: %v", err)
	}
	channelEventsBefore, err := s.ChannelEventsWindow(ctx, channel.ID, 0, channelPtsBefore, 10)
	if err != nil {
		t.Fatalf("channel events before refused reads: %v", err)
	}
	h := fullChannelDispatcher(s)
	validMemberInput := api.InputChannel(member.ID, channel.ID)
	borrowedInput := api.InputChannel(creator.ID, channel.ID)
	for _, tc := range []struct {
		name        string
		userID      int64
		input       tg.InputChannelClass
		provisional bool
		want        string
	}{
		{name: "outsider", userID: outsider.ID, input: api.InputChannel(outsider.ID, channel.ID), want: "PEER_ID_INVALID"},
		{name: "borrowed hash", userID: member.ID, input: borrowedInput, want: "PEER_ID_INVALID"},
		{name: "forged hash", userID: member.ID, input: &tg.InputChannel{ChannelID: channel.ID, AccessHash: channel.ID}, want: "PEER_ID_INVALID"},
		{name: "banned member", userID: banned.ID, input: api.InputChannel(banned.ID, channel.ID), want: "PEER_ID_INVALID"},
		{name: "removed member", userID: removed.ID, input: api.InputChannel(removed.ID, channel.ID), want: "PEER_ID_INVALID"},
		{name: "unauthenticated", userID: 0, input: validMemberInput, want: "AUTH_KEY_UNREGISTERED"},
		{name: "provisional", userID: member.ID, input: validMemberInput, provisional: true, want: "AUTH_KEY_UNREGISTERED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, rpc := channelsReadHistoryViaDispatcher(t, h, tc.userID, tc.provisional, tc.input, 1)
			if ok || rpc == nil || rpc.ErrorMessage != tc.want {
				t.Fatalf("channels.readHistory = ok:%v rpc:%v, want %s", ok, rpc, tc.want)
			}
			if tc.userID != 0 {
				if got := readChannelMarker(t, ctx, dsn, channel.ID, tc.userID); got != 0 {
					t.Errorf("refused request changed caller marker to %d", got)
				}
			}
		})
	}
	requireChannelReadState(t, s, member.ID, channel.ID, 0, 1)
	channelPtsAfter, err := s.ChannelState(ctx, channel.ID)
	if err != nil {
		t.Fatalf("channel state after refused reads: %v", err)
	}
	channelEventsAfter, err := s.ChannelEventsWindow(ctx, channel.ID, 0, channelPtsAfter, 10)
	if err != nil {
		t.Fatalf("channel events after refused reads: %v", err)
	}
	if channelPtsAfter != channelPtsBefore || !reflect.DeepEqual(channelEventsAfter, channelEventsBefore) {
		t.Errorf("refused reads changed channel stream: before pts=%d events=%+v after pts=%d events=%+v", channelPtsBefore, channelEventsBefore, channelPtsAfter, channelEventsAfter)
	}
}

func TestConcurrentChannelPostPastReadPositionRemainsUnread(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s := openStore(t)
	creator := readHistoryUser(t, s, ctx, "+15551239931")
	reader := readHistoryUser(t, s, ctx, "+15551239932")
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Concurrent read"})
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}
	first := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "first", 123931)
	second := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "second", 123932)
	start := make(chan struct{})
	readDone := make(chan struct {
		ok  bool
		rpc *mt.RPCError
	}, 1)
	postDone := make(chan error, 1)
	go func() {
		<-start
		ok, rpc := channelsReadHistoryViaDispatcher(t, fullChannelDispatcher(s), reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(second.LocalID))
		readDone <- struct {
			ok  bool
			rpc *mt.RPCError
		}{ok: ok, rpc: rpc}
	}()
	go func() {
		<-start
		_, _, _, err := s.PostChannelMessageAs(ctx, channel.ID, creator.ID, "third", 123933, nil, 0)
		postDone <- err
	}()
	close(start)
	read := <-readDone
	if !read.ok || read.rpc != nil {
		t.Fatalf("concurrent channels.readHistory: ok=%v rpc=%v", read.ok, read.rpc)
	}
	if err := <-postDone; err != nil {
		t.Fatalf("concurrent later post: %v", err)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, second.LocalID, 1)
	if second.LocalID != first.LocalID+1 {
		t.Fatalf("first two post ids = %d/%d, want adjacent positions", first.LocalID, second.LocalID)
	}
}

func readHistoryUser(t *testing.T, s *store.Store, ctx context.Context, phone string) store.User {
	t.Helper()
	user, err := s.CreateUser(ctx, phone)
	if err != nil {
		t.Fatalf("create user %s: %v", phone, err)
	}
	return user
}

func postReadHistoryChannelMessage(t *testing.T, ctx context.Context, s *store.Store, channelID, authorID int64, text string, randomID int64) store.ChannelMessage {
	t.Helper()
	message, _, _, err := s.PostChannelMessageAs(ctx, channelID, authorID, text, randomID, nil, 0)
	if err != nil {
		t.Fatalf("post channel message %q: %v", text, err)
	}
	return message
}

func requireChannelReadState(t *testing.T, s *store.Store, viewerID, channelID, wantMarker int64, wantUnread int) {
	t.Helper()
	dialog := channelDialogForTest(t, s, viewerID, channelID)
	if dialog.ReadInboxMaxID != int(wantMarker) || dialog.UnreadCount != wantUnread {
		t.Fatalf("getDialogs channel read state = marker %d unread %d, want %d/%d", dialog.ReadInboxMaxID, dialog.UnreadCount, wantMarker, wantUnread)
	}
}

func requirePeerChannelReadState(t *testing.T, s *store.Store, viewerID, channelID, wantMarker int64, wantUnread int) {
	t.Helper()
	result, err := api.GetPeerDialogsForTest(s, viewerID, &tg.MessagesGetPeerDialogsRequest{
		Peers: []tg.InputDialogPeerClass{&tg.InputDialogPeer{Peer: api.InputPeerChannel(viewerID, channelID)}},
	})
	if err != nil {
		t.Fatalf("getPeerDialogs: %v", err)
	}
	response, ok := result.(*tg.MessagesPeerDialogs)
	if !ok || len(response.Dialogs) != 1 {
		t.Fatalf("getPeerDialogs response = %T with %d dialogs, want one", result, len(response.Dialogs))
	}
	dialog, ok := response.Dialogs[0].(*tg.Dialog)
	if !ok {
		t.Fatalf("getPeerDialogs dialog = %T, want *tg.Dialog", response.Dialogs[0])
	}
	if dialog.ReadInboxMaxID != int(wantMarker) || dialog.UnreadCount != wantUnread {
		t.Fatalf("getPeerDialogs channel read state = marker %d unread %d, want %d/%d", dialog.ReadInboxMaxID, dialog.UnreadCount, wantMarker, wantUnread)
	}
}

func channelDialogForTest(t *testing.T, s *store.Store, viewerID, channelID int64) *tg.Dialog {
	t.Helper()
	result, err := api.GetDialogsForTest(s, viewerID)
	if err != nil {
		t.Fatalf("getDialogs: %v", err)
	}
	var dialogs []tg.DialogClass
	switch response := result.(type) {
	case *tg.MessagesDialogs:
		dialogs = response.Dialogs
	case *tg.MessagesDialogsSlice:
		dialogs = response.Dialogs
	default:
		t.Fatalf("getDialogs response = %T, want MessagesDialogs or MessagesDialogsSlice", result)
	}
	for _, item := range dialogs {
		dialog, ok := item.(*tg.Dialog)
		if !ok {
			continue
		}
		peer, ok := dialog.Peer.(*tg.PeerChannel)
		if ok && peer.ChannelID == channelID {
			return dialog
		}
	}
	t.Fatalf("getDialogs omitted channel %d for viewer %d", channelID, viewerID)
	return nil
}

func channelsReadHistoryViaDispatcher(
	t *testing.T,
	h mtproto.Handler,
	userID int64,
	provisional bool,
	channel tg.InputChannelClass,
	maxID int,
) (bool, *mt.RPCError) {
	t.Helper()
	body := dispatchSettings(t, h, settingsHandler{
		name: "channels.readHistory",
		request: func() bin.Encoder {
			return &tg.ChannelsReadHistoryRequest{Channel: channel, MaxID: maxID}
		},
	}, userID, provisional)
	var success tg.BoolTrue
	if err := success.Decode(&bin.Buffer{Buf: body}); err == nil {
		return true, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode channels.readHistory response: %v", err)
	}
	return false, &rpc
}

func readChannelMarker(t *testing.T, ctx context.Context, dsn string, channelID, userID int64) int64 {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect for channel marker: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close marker connection: %v", err)
		}
	}()
	var marker int64
	if err := conn.QueryRow(ctx, `SELECT COALESCE((SELECT read_max_id FROM channel_read_state WHERE channel_id = $1 AND user_id = $2), 0)`, channelID, userID).Scan(&marker); err != nil {
		t.Fatalf("read channel marker: %v", err)
	}
	return marker
}
