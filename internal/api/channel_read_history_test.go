package api_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
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
	basicSender, err := s.CreateUser(ctx, "+15551239903")
	if err != nil {
		t.Fatalf("create basic-message sender: %v", err)
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
	if _, _, _, _, err := s.SendMessage(ctx, basicSender.ID, reader.ID, "basic unread", 123903, 0, 0); err != nil {
		t.Fatalf("send basic unread message: %v", err)
	}
	creatorState, err := s.State(ctx, creator.ID)
	if err != nil {
		t.Fatalf("creator update state before read: %v", err)
	}
	creatorEvents, err := s.EventsSince(ctx, creator.ID, 0)
	if err != nil {
		t.Fatalf("creator events before read: %v", err)
	}
	creatorReadMarker := readChannelMarker(t, ctx, dsn, channel.ID, creator.ID)
	readerReadMarker := readChannelMarker(t, ctx, dsn, channel.ID, reader.ID)
	channelExec(t, ctx, dsn, `DELETE FROM update_state WHERE user_id = $1`, reader.ID)
	h := fullChannelDispatcher(s)

	requireChannelReadState(t, s, reader.ID, channel.ID, readerReadMarker, 2)
	requirePeerChannelReadState(t, s, reader.ID, channel.ID, readerReadMarker, 2)
	requireChannelOwnerUnreadTotal(t, s, reader.ID, channel.ID, 3)

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
	requireChannelOwnerUnreadTotal(t, s, reader.ID, channel.ID, 2)

	if ok, rpc := channelsReadHistoryViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(second.LocalID)); rpc != nil || !ok {
		t.Fatalf("channels.readHistory through second post: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, second.LocalID, 0)
	requirePeerChannelReadState(t, s, reader.ID, channel.ID, second.LocalID, 0)
	requireChannelOwnerUnreadTotal(t, s, reader.ID, channel.ID, 1)

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
	requireChannelOwnerUnreadTotal(t, s, reader.ID, channel.ID, 2)
	if ok, rpc := channelsReadHistoryViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(second.LocalID)); rpc != nil || !ok {
		t.Fatalf("read through second post after later post: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, second.LocalID, 1)
	requireChannelOwnerUnreadTotal(t, s, reader.ID, channel.ID, 2)
	if ok, rpc := channelsReadHistoryViaDispatcher(t, h, reader.ID, false, api.InputChannel(reader.ID, channel.ID), int(third.LocalID+100)); rpc != nil || !ok {
		t.Fatalf("clamped channels.readHistory: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, third.LocalID, 0)
	requireChannelOwnerUnreadTotal(t, s, reader.ID, channel.ID, 1)

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
	creatorStateAfter, err := s.State(ctx, creator.ID)
	if err != nil {
		t.Fatalf("creator update state after read: %v", err)
	}
	if !reflect.DeepEqual(creatorStateAfter, creatorState) {
		t.Errorf("channel read changed author update state: before=%+v after=%+v", creatorState, creatorStateAfter)
	}
	creatorEventsAfter, err := s.EventsSince(ctx, creator.ID, 0)
	if err != nil {
		t.Fatalf("creator events after read: %v", err)
	}
	if !reflect.DeepEqual(creatorEventsAfter, creatorEvents) {
		t.Errorf("channel read created author events: before=%+v after=%+v", creatorEvents, creatorEventsAfter)
	}
	if got := readChannelMarker(t, ctx, dsn, channel.ID, creator.ID); got != creatorReadMarker {
		t.Errorf("channel read changed author marker from %d to %d", creatorReadMarker, got)
	}
	requireChannelReadState(t, s, creator.ID, channel.ID, creatorReadMarker, 0)
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

func TestChannelsReadHistoryIsScopedToMemberAndChannel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator := readHistoryUser(t, s, ctx, "+15551239931")
	reader := readHistoryUser(t, s, ctx, "+15551239932")
	otherReader := readHistoryUser(t, s, ctx, "+15551239933")
	firstChannel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "First read scope"})
	secondChannel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Second read scope"})
	for _, channelID := range []int64{firstChannel.ID, secondChannel.ID} {
		invite, err := s.CreateChannelInvite(ctx, channelID, creator.ID)
		if err != nil {
			t.Fatalf("create invite for channel %d: %v", channelID, err)
		}
		for _, member := range []store.User{reader, otherReader} {
			if _, _, err := s.JoinChannelByInvite(ctx, invite, member.ID); err != nil {
				t.Fatalf("join user %d to channel %d: %v", member.ID, channelID, err)
			}
		}
	}
	firstPost := postReadHistoryChannelMessage(t, ctx, s, firstChannel.ID, creator.ID, "first channel one", 123931)
	secondPost := postReadHistoryChannelMessage(t, ctx, s, firstChannel.ID, creator.ID, "second channel one", 123932)
	otherChannelPost := postReadHistoryChannelMessage(t, ctx, s, secondChannel.ID, creator.ID, "channel two", 123933)
	assertScopedReadStates := func(wantReaderFirstMarker int64, wantReaderFirstUnread int, wantReaderSecondMarker int64, wantReaderSecondUnread int) {
		t.Helper()
		requireChannelReadState(t, s, reader.ID, firstChannel.ID, wantReaderFirstMarker, wantReaderFirstUnread)
		requireChannelReadState(t, s, otherReader.ID, firstChannel.ID, 1, 2)
		requireChannelReadState(t, s, reader.ID, secondChannel.ID, wantReaderSecondMarker, wantReaderSecondUnread)
		requireChannelReadState(t, s, otherReader.ID, secondChannel.ID, 1, 1)
	}
	assertScopedReadStates(1, 2, 1, 1)

	if ok, rpc := channelsReadHistoryViaDispatcher(t, fullChannelDispatcher(s), reader.ID, false, api.InputChannel(reader.ID, firstChannel.ID), int(firstPost.LocalID)); rpc != nil || !ok {
		t.Fatalf("partial first-channel read: ok=%v rpc=%v", ok, rpc)
	}
	assertScopedReadStates(firstPost.LocalID, 1, 1, 1)

	if ok, rpc := channelsReadHistoryViaDispatcher(t, fullChannelDispatcher(s), reader.ID, false, api.InputChannel(reader.ID, firstChannel.ID), int(secondPost.LocalID)); rpc != nil || !ok {
		t.Fatalf("full first-channel read: ok=%v rpc=%v", ok, rpc)
	}
	assertScopedReadStates(secondPost.LocalID, 0, 1, 1)
	if ok, rpc := channelsReadHistoryViaDispatcher(t, fullChannelDispatcher(s), reader.ID, false, api.InputChannel(reader.ID, firstChannel.ID), int(firstPost.LocalID)); rpc != nil || !ok {
		t.Fatalf("lower repeated first-channel read: ok=%v rpc=%v", ok, rpc)
	}
	assertScopedReadStates(secondPost.LocalID, 0, 1, 1)

	if ok, rpc := channelsReadHistoryViaDispatcher(t, fullChannelDispatcher(s), reader.ID, false, api.InputChannel(reader.ID, secondChannel.ID), int(otherChannelPost.LocalID)); rpc != nil || !ok {
		t.Fatalf("full second-channel read: ok=%v rpc=%v", ok, rpc)
	}
	assertScopedReadStates(secondPost.LocalID, 0, otherChannelPost.LocalID, 0)
}

func TestChannelUnreadCountsStayExactWithLongExcludedRuns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator := readHistoryUser(t, s, ctx, "+15551239941")
	reader := readHistoryUser(t, s, ctx, "+15551239942")
	deletedChannel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Bound deleted scan"})
	selfPostChannel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Bound own-post scan"})
	invite, err := s.CreateChannelInvite(ctx, deletedChannel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}
	selfPostInvite, err := s.CreateChannelInvite(ctx, selfPostChannel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create self-post channel invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, selfPostInvite, reader.ID); err != nil {
		t.Fatalf("join reader to self-post channel: %v", err)
	}
	channelExec(t, ctx, dsn, `
INSERT INTO channel_messages (channel_id, local_id, from_id, message, deleted)
SELECT $1::bigint, local_id::bigint, $2::bigint, 'deleted history', true
FROM generate_series(2, 1003) local_id`, deletedChannel.ID, creator.ID)
	channelExec(t, ctx, dsn, `
INSERT INTO channel_messages (channel_id, local_id, from_id, message)
VALUES ($1, 1004, $2, 'live post after deleted run')`, deletedChannel.ID, creator.ID)
	channelExec(t, ctx, dsn, `
INSERT INTO channel_messages (channel_id, local_id, from_id, message)
SELECT $1::bigint, local_id::bigint, $2::bigint, 'own history'
FROM generate_series(2, 1003) local_id`, selfPostChannel.ID, creator.ID)
	channelExec(t, ctx, dsn, `
INSERT INTO channel_messages (channel_id, local_id, from_id, message)
VALUES ($1, 1004, $2, 'live post after own-post run')`, selfPostChannel.ID, reader.ID)

	for _, tc := range []struct {
		name      string
		channelID int64
		viewerID  int64
		markerID  int64
		total     int
	}{
		{name: "deleted posts", channelID: deletedChannel.ID, viewerID: reader.ID, markerID: 1, total: 1001},
		{name: "own posts", channelID: selfPostChannel.ID, viewerID: creator.ID, total: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The live post follows 1,002 excluded rows, beyond the old scan cap.
			requireChannelReadState(t, s, tc.viewerID, tc.channelID, tc.markerID, 1)
			requirePeerChannelReadState(t, s, tc.viewerID, tc.channelID, tc.markerID, 1)
			requireChannelOwnerUnreadTotal(t, s, tc.viewerID, tc.channelID, tc.total)
			fullResponse, rpc := getFullChannelViaDispatcher(t, fullChannelDispatcher(s), tc.viewerID, false, api.InputChannel(tc.viewerID, tc.channelID))
			if rpc != nil {
				t.Fatalf("reader getFullChannel: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
			}
			full := fullChannelInfo(t, fullResponse)
			if int64(full.ReadInboxMaxID) != tc.markerID || full.UnreadCount != 1 {
				t.Fatalf("full channel read state = marker %d unread %d, want %d/1 across long excluded run", full.ReadInboxMaxID, full.UnreadCount, tc.markerID)
			}
		})
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
		FROM generate_series(2, 1003) AS post_id`, channel.ID, creator.ID)
	channelExec(t, ctx, dsn, `UPDATE channel_state SET pts = 1003, next_local_id = 1004 WHERE channel_id = $1`, channel.ID)
	channelExec(t, ctx, dsn, `
		INSERT INTO channel_events (channel_id, pts, type, local_id)
		SELECT $1, post_id, 1, post_id
		FROM generate_series(2, 1003) AS post_id`, channel.ID)
	requireChannelReadState(t, s, reader.ID, channel.ID, 1, 1000)
	requirePeerChannelReadState(t, s, reader.ID, channel.ID, 1, 1000)
	if ok, rpc := channelsReadHistoryViaDispatcher(t, fullChannelDispatcher(s), reader.ID, false, api.InputChannel(reader.ID, channel.ID), 1001); rpc != nil || !ok {
		t.Fatalf("partial capped read: ok=%v rpc=%v", ok, rpc)
	}
	requireChannelReadState(t, s, reader.ID, channel.ID, 1001, 2)
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
			var markerBefore int64
			if tc.userID != 0 {
				markerBefore = readChannelMarker(t, ctx, dsn, channel.ID, tc.userID)
			}
			ok, rpc := channelsReadHistoryViaDispatcher(t, h, tc.userID, tc.provisional, tc.input, 1)
			if ok || rpc == nil || rpc.ErrorMessage != tc.want {
				t.Fatalf("channels.readHistory = ok:%v rpc:%v, want %s", ok, rpc, tc.want)
			}
			if tc.userID != 0 {
				if got := readChannelMarker(t, ctx, dsn, channel.ID, tc.userID); got != markerBefore {
					t.Errorf("refused request changed caller marker from %d to %d", markerBefore, got)
				}
			}
		})
	}
	requireChannelReadState(t, s, member.ID, channel.ID, 1, 1)
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

func TestChannelsReadHistoryRechecksBanAfterBlockedMutation(t *testing.T) {
	t.Parallel()
	runChannelsReadHistoryRefusalRace(t,
		"channel_read_history_ban_wait",
		`UPDATE channel_participants
		    SET banned_until = now() + interval '1 hour'
		  WHERE channel_id = $1 AND user_id = $2`,
		false,
	)
}

func TestChannelsReadHistoryRechecksRemovalAfterBlockedMutation(t *testing.T) {
	t.Parallel()
	runChannelsReadHistoryRefusalRace(t,
		"channel_read_history_remove_wait",
		`DELETE FROM channel_participants WHERE channel_id = $1 AND user_id = $2`,
		true,
	)
}

func runChannelsReadHistoryRefusalRace(t *testing.T, applicationName, mutation string, removed bool) {
	t.Helper()
	ctx, dsn, s, creator, reader, channel := channelReadHistoryRaceFixture(t, applicationName, "Read after membership mutation")
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}
	post := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "unread before mutation", 123941)

	before := readChannelReadHistoryAudit(t, ctx, dsn, channel.ID, reader.ID)
	if !before.markerExists || before.marker >= post.LocalID {
		t.Fatalf("initial marker = %d exists=%v, want a marker before post %d", before.marker, before.markerExists, post.LocalID)
	}
	beforeRoots := readChannelReadHistorySummaryRoots(t, ctx, dsn, channel.ID, creator.ID)
	beforeReference := readChannelReadHistoryReferenceCounts(t, ctx, dsn, channel.ID, creator.ID, reader.ID, before.marker)
	if beforeRoots != [2]int64{beforeReference.total, beforeReference.author} || beforeReference.unread != 1 {
		t.Fatalf("initial summary/reference = %v/%+v, want 1 live unread post", beforeRoots, beforeReference)
	}
	deliveryBefore := readChannelReadHistoryDeliveryState(t, ctx, s, reader.ID, channel.ID)
	listener := listenForChannelReadHistoryNotifications(t, ctx, dsn)

	blockerTx, blockerPID := holdChannelReadHistoryLock(
		t,
		ctx,
		dsn,
		`SELECT id FROM channels WHERE id = $1 FOR NO KEY UPDATE`,
		channel.ID,
	)
	requestCtx, cancelRequest := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRequest()
	readDone := make(chan channelReadHistoryCall, 1)
	go func() {
		ok, rpc := channelsReadHistoryViaDispatcherWithContext(
			t,
			fullChannelDispatcher(s),
			reader.ID,
			false,
			api.InputChannel(reader.ID, channel.ID),
			int(post.LocalID),
			requestCtx,
		)
		readDone <- channelReadHistoryCall{ok: ok, rpc: rpc}
	}()

	observer, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect channel lock observer: %v", err)
	}
	defer func() {
		if err := observer.Close(ctx); err != nil {
			t.Errorf("close channel lock observer: %v", err)
		}
	}()
	waitForChannelLockWaiter(t, ctx, observer, applicationName, blockerPID, blockerPID)

	if _, err := blockerTx.Exec(ctx, mutation, channel.ID, reader.ID); err != nil {
		t.Fatalf("apply membership mutation while read waits: %v", err)
	}
	if err := blockerTx.Commit(ctx); err != nil {
		t.Fatalf("commit membership mutation: %v", err)
	}
	call := waitForChannelReadHistoryCall(t, requestCtx, readDone)
	if call.ok || call.rpc == nil || call.rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Fatalf("read after committed membership mutation = ok:%v rpc:%v, want PEER_ID_INVALID", call.ok, call.rpc)
	}

	after := readChannelReadHistoryAudit(t, ctx, dsn, channel.ID, reader.ID)
	requireChannelReadHistoryAuditChanges(t, before, after, false, true, removed)
	if removed && (after.markerExists || after.marker != 0) {
		t.Fatalf("removed member marker after refused read = %d exists=%v, want absent", after.marker, after.markerExists)
	}
	if !removed && (!after.markerExists || after.marker != before.marker) {
		t.Fatalf("ban/refused read changed marker from %d to %d (exists=%v)", before.marker, after.marker, after.markerExists)
	}
	if roots := readChannelReadHistorySummaryRoots(t, ctx, dsn, channel.ID, creator.ID); roots != beforeRoots {
		t.Fatalf("refused read changed summary roots from %v to %v", beforeRoots, roots)
	}
	reference := readChannelReadHistoryReferenceCounts(t, ctx, dsn, channel.ID, creator.ID, reader.ID, after.marker)
	if reference != beforeReference {
		t.Fatalf("membership mutation/refused read changed posts or unread reference from %+v to %+v", beforeReference, reference)
	}
	if _, err := s.ChannelPostUnreadCount(ctx, channel.ID, reader.ID, before.marker); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("unread lookup after membership mutation = %v, want ErrNotMember", err)
	}
	deliveryAfter := readChannelReadHistoryDeliveryState(t, ctx, s, reader.ID, channel.ID)
	requireChannelReadHistoryDeliveryUnchanged(t, "refused read", deliveryBefore, deliveryAfter)
	requireNoChannelReadHistoryNotification(t, listener)
}

func TestChannelsReadHistoryMatchesReferenceAfterConcurrentPostDeletion(t *testing.T) {
	t.Parallel()
	const applicationName = "channel_read_history_delete_wait"
	ctx, dsn, s, creator, reader, channel := channelReadHistoryRaceFixture(t, applicationName, "Read concurrent delete")
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}
	first := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "read this post", 123951)
	second := postReadHistoryChannelMessage(t, ctx, s, channel.ID, creator.ID, "delete this post", 123952)

	before := readChannelReadHistoryAudit(t, ctx, dsn, channel.ID, reader.ID)
	if !before.markerExists || before.marker >= first.LocalID {
		t.Fatalf("initial marker = %d exists=%v, want before first post %d", before.marker, before.markerExists, first.LocalID)
	}
	beforeRoots := readChannelReadHistorySummaryRoots(t, ctx, dsn, channel.ID, creator.ID)
	beforeReference := readChannelReadHistoryReferenceCounts(t, ctx, dsn, channel.ID, creator.ID, reader.ID, before.marker)
	if beforeRoots != [2]int64{beforeReference.total, beforeReference.author} || beforeReference.unread != 2 {
		t.Fatalf("initial summary/reference = %v/%+v, want two live unread posts", beforeRoots, beforeReference)
	}
	deliveryBefore := readChannelReadHistoryDeliveryState(t, ctx, s, reader.ID, channel.ID)
	listener := listenForChannelReadHistoryNotifications(t, ctx, dsn)
	blockerTx, blockerPID := holdChannelReadHistoryLock(
		t,
		ctx,
		dsn,
		`SELECT channel_id FROM channel_state WHERE channel_id = $1 FOR UPDATE`,
		channel.ID,
	)
	requestCtx, cancelRequest := context.WithTimeout(ctx, 10*time.Second)
	defer cancelRequest()
	readDone := make(chan channelReadHistoryCall, 1)
	go func() {
		ok, rpc := channelsReadHistoryViaDispatcherWithContext(
			t,
			fullChannelDispatcher(s),
			reader.ID,
			false,
			api.InputChannel(reader.ID, channel.ID),
			int(first.LocalID),
			requestCtx,
		)
		readDone <- channelReadHistoryCall{ok: ok, rpc: rpc}
	}()

	observer, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect channel state lock observer: %v", err)
	}
	defer func() {
		if err := observer.Close(ctx); err != nil {
			t.Errorf("close channel state lock observer: %v", err)
		}
	}()
	waitForChannelLockWaiter(t, ctx, observer, applicationName, blockerPID, blockerPID)
	if _, err := blockerTx.Exec(ctx,
		`UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`,
		channel.ID,
		second.LocalID,
	); err != nil {
		t.Fatalf("delete post while read waits: %v", err)
	}
	if err := blockerTx.Commit(ctx); err != nil {
		t.Fatalf("commit post deletion: %v", err)
	}
	call := waitForChannelReadHistoryCall(t, requestCtx, readDone)
	if !call.ok || call.rpc != nil {
		t.Fatalf("read after concurrent deletion = ok:%v rpc:%v, want success", call.ok, call.rpc)
	}

	after := readChannelReadHistoryAudit(t, ctx, dsn, channel.ID, reader.ID)
	requireChannelReadHistoryAuditChanges(t, before, after, true, false, true)
	if !after.markerExists || after.marker != first.LocalID {
		t.Fatalf("marker after delete/read = %d exists=%v, want %d", after.marker, after.markerExists, first.LocalID)
	}
	reference := readChannelReadHistoryReferenceCounts(t, ctx, dsn, channel.ID, creator.ID, reader.ID, after.marker)
	roots := readChannelReadHistorySummaryRoots(t, ctx, dsn, channel.ID, creator.ID)
	if roots != [2]int64{reference.total, reference.author} {
		t.Fatalf("summary roots %v disagree with uncapped live-post reference %+v", roots, reference)
	}
	unread, err := s.ChannelPostUnreadCount(ctx, channel.ID, reader.ID, after.marker)
	if err != nil {
		t.Fatalf("count unread after delete/read: %v", err)
	}
	if unread != reference.unread || unread != 0 {
		t.Fatalf("summary unread = %d, uncapped reference = %d, want both zero", unread, reference.unread)
	}
	deliveryAfter := readChannelReadHistoryDeliveryState(t, ctx, s, reader.ID, channel.ID)
	requireChannelReadHistoryDeliveryUnchanged(t, "delete/read", deliveryBefore, deliveryAfter)
	requireNoChannelReadHistoryNotification(t, listener)
}

type channelReadHistoryCall struct {
	ok  bool
	rpc *mt.RPCError
}

type channelReadHistoryAudit struct {
	source       [5]string
	summaryRows  string
	summaryState string
	markerExists bool
	marker       int64
}

type channelReadHistoryReference struct {
	total  int64
	author int64
	unread int64
}

type channelReadHistoryDeliveryState struct {
	ownerPts      int
	ownerEvents   []store.Event
	channelPts    int
	channelEvents []store.ChannelEvent
}

func channelReadHistoryRaceFixture(
	t *testing.T,
	applicationName, title string,
) (context.Context, string, *store.Store, store.User, store.User, *tg.Channel) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	dsn := pgtest.DSN(t)
	s := openStoreWithApplicationName(t, dsn, applicationName)
	creator := readHistoryUser(t, s, ctx, "+15551239941")
	reader := readHistoryUser(t, s, ctx, "+15551239942")
	channel := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: title})
	return ctx, dsn, s, creator, reader, channel
}

func readChannelReadHistoryDeliveryState(t *testing.T, ctx context.Context, s *store.Store, userID, channelID int64) channelReadHistoryDeliveryState {
	t.Helper()
	var snapshot channelReadHistoryDeliveryState
	owner, err := s.State(ctx, userID)
	if err != nil {
		t.Fatalf("read owner update state: %v", err)
	}
	snapshot.ownerPts = owner.Pts
	snapshot.ownerEvents, err = s.EventsSince(ctx, userID, 0)
	if err != nil {
		t.Fatalf("read owner events: %v", err)
	}
	snapshot.channelPts, err = s.ChannelState(ctx, channelID)
	if err != nil {
		t.Fatalf("read channel pts: %v", err)
	}
	snapshot.channelEvents, err = s.ChannelEventsWindow(ctx, channelID, 0, snapshot.channelPts, 20)
	if err != nil {
		t.Fatalf("read channel events: %v", err)
	}
	return snapshot
}

func requireChannelReadHistoryDeliveryUnchanged(
	t *testing.T,
	action string,
	before, after channelReadHistoryDeliveryState,
) {
	t.Helper()
	if after.ownerPts != before.ownerPts || !reflect.DeepEqual(after.ownerEvents, before.ownerEvents) {
		t.Errorf("%s changed owner delivery state: pts %d/%d events %+v/%+v", action, before.ownerPts, after.ownerPts, before.ownerEvents, after.ownerEvents)
	}
	if after.channelPts != before.channelPts || !reflect.DeepEqual(after.channelEvents, before.channelEvents) {
		t.Errorf("%s changed channel delivery state: pts %d/%d events %+v/%+v", action, before.channelPts, after.channelPts, before.channelEvents, after.channelEvents)
	}
}

func readChannelReadHistoryAudit(t *testing.T, ctx context.Context, dsn string, channelID, viewerID int64) channelReadHistoryAudit {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect channel read audit: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close channel read audit: %v", err)
		}
	}()
	var audit channelReadHistoryAudit
	err = conn.QueryRow(ctx, `
		SELECT
		  md5(COALESCE((SELECT jsonb_agg(to_jsonb(post) ORDER BY post.local_id)::text
		                  FROM channel_messages AS post WHERE post.channel_id = $1), '[]')),
		  md5(COALESCE((SELECT jsonb_agg(to_jsonb(participant) ORDER BY participant.user_id)::text
		                  FROM channel_participants AS participant WHERE participant.channel_id = $1), '[]')),
		  md5(COALESCE((SELECT jsonb_agg(to_jsonb(marker) ORDER BY marker.user_id)::text
		                  FROM channel_read_state AS marker WHERE marker.channel_id = $1), '[]')),
		  md5(COALESCE((SELECT row_to_json(state)::text
		                  FROM channel_state AS state WHERE state.channel_id = $1), '')),
		  md5(COALESCE((SELECT jsonb_agg(to_jsonb(event) ORDER BY event.pts)::text
		                  FROM channel_events AS event WHERE event.channel_id = $1), '[]')),
		  md5(COALESCE((SELECT jsonb_agg(to_jsonb(summary)
		                      ORDER BY summary.scope_kind, summary.author_id, summary.depth, summary.prefix)::text
		                  FROM channel_post_summaries AS summary WHERE summary.channel_id = $1), '[]')),
		  md5(COALESCE((SELECT row_to_json(state)::text
		                  FROM channel_post_summary_state AS state WHERE state.channel_id = $1), '')),
		  EXISTS (SELECT 1 FROM channel_read_state WHERE channel_id = $1 AND user_id = $2),
		  COALESCE((SELECT read_max_id FROM channel_read_state WHERE channel_id = $1 AND user_id = $2), 0)
	`, channelID, viewerID).Scan(
		&audit.source[0], &audit.source[1], &audit.source[2], &audit.source[3], &audit.source[4],
		&audit.summaryRows, &audit.summaryState, &audit.markerExists, &audit.marker,
	)
	if err != nil {
		t.Fatalf("read channel read audit: %v", err)
	}
	return audit
}

func readChannelReadHistorySummaryRoots(t *testing.T, ctx context.Context, dsn string, channelID, authorID int64) [2]int64 {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect summary root reader: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close summary root reader: %v", err)
		}
	}()
	var roots [2]int64
	err = conn.QueryRow(ctx, `
		SELECT COALESCE(max(live_count) FILTER (
		           WHERE scope_kind = 0 AND author_id = 0 AND depth = 0 AND prefix = 0), 0),
		       COALESCE(max(live_count) FILTER (
		           WHERE scope_kind = 1 AND author_id = $2 AND depth = 0 AND prefix = 0), 0)
		FROM channel_post_summaries
		WHERE channel_id = $1
	`, channelID, authorID).Scan(&roots[0], &roots[1])
	if err != nil {
		t.Fatalf("read summary root counts: %v", err)
	}
	return roots
}

func readChannelReadHistoryReferenceCounts(
	t *testing.T,
	ctx context.Context,
	dsn string,
	channelID, authorID, viewerID, marker int64,
) channelReadHistoryReference {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect uncapped reference reader: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close uncapped reference reader: %v", err)
		}
	}()
	var reference channelReadHistoryReference
	err = conn.QueryRow(ctx, `
		SELECT count(*)::bigint,
		       count(*) FILTER (WHERE from_id = $2)::bigint,
		       count(*) FILTER (WHERE local_id > $4 AND from_id <> $3)::bigint
		FROM channel_messages
		WHERE channel_id = $1 AND deleted = false AND action_type = 0
	`, channelID, authorID, viewerID, marker).Scan(&reference.total, &reference.author, &reference.unread)
	if err != nil {
		t.Fatalf("read uncapped post reference: %v", err)
	}
	return reference
}

func requireChannelReadHistoryAuditChanges(
	t *testing.T,
	before, after channelReadHistoryAudit,
	postsChanged, participantsChanged, markersChanged bool,
) {
	t.Helper()
	fields := []struct {
		name          string
		before, after string
		changed       bool
	}{
		{name: "channel posts", before: before.source[0], after: after.source[0], changed: postsChanged},
		{name: "participants", before: before.source[1], after: after.source[1], changed: participantsChanged},
		{name: "read markers", before: before.source[2], after: after.source[2], changed: markersChanged},
		{name: "channel state", before: before.source[3], after: after.source[3]},
		{name: "channel events", before: before.source[4], after: after.source[4]},
	}
	for _, field := range fields {
		if (field.before != field.after) != field.changed {
			t.Errorf("%s changed=%v, want %v", field.name, field.before != field.after, field.changed)
		}
	}
	if (before.summaryRows != after.summaryRows) != postsChanged {
		t.Errorf("summary rows changed=%v, want %v", before.summaryRows != after.summaryRows, postsChanged)
	}
	if before.summaryState != after.summaryState {
		t.Errorf("summary readiness changed from %s to %s", before.summaryState, after.summaryState)
	}
}

func holdChannelReadHistoryLock(t *testing.T, ctx context.Context, dsn, query string, channelID int64) (pgx.Tx, int) {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect lock holder: %v", err)
	}
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock holder: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := tx.Rollback(cleanupCtx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Errorf("release lock-holder transaction: %v", err)
		}
		if err := conn.Close(cleanupCtx); err != nil {
			t.Errorf("close lock holder: %v", err)
		}
	})
	var lockedChannelID int64
	if err := tx.QueryRow(ctx, query, channelID).Scan(&lockedChannelID); err != nil {
		t.Fatalf("acquire channel read race lock: %v", err)
	}
	if lockedChannelID != channelID {
		t.Fatalf("locked channel = %d, want %d", lockedChannelID, channelID)
	}
	var blockerPID int
	if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&blockerPID); err != nil {
		t.Fatalf("read lock-holder pid: %v", err)
	}
	return tx, blockerPID
}

func waitForChannelReadHistoryCall(t *testing.T, ctx context.Context, result <-chan channelReadHistoryCall) channelReadHistoryCall {
	t.Helper()
	select {
	case call := <-result:
		return call
	case <-ctx.Done():
		t.Fatalf("channels.readHistory did not return before context ended: %v", ctx.Err())
		return channelReadHistoryCall{}
	}
}

func listenForChannelReadHistoryNotifications(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect channel notification listener: %v", err)
	}
	if _, err := conn.Exec(ctx, `LISTEN tg_channel_post`); err != nil {
		if closeErr := conn.Close(ctx); closeErr != nil {
			t.Errorf("close failed notification listener: %v", closeErr)
		}
		t.Fatalf("listen for channel post notifications: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := conn.Close(cleanupCtx); err != nil {
			t.Errorf("close channel notification listener: %v", err)
		}
	})
	return conn
}

func requireNoChannelReadHistoryNotification(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	notification, err := conn.WaitForNotification(ctx)
	if err == nil {
		t.Fatalf("channel read/delete emitted notification %q", notification.Payload)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait for channel notification: %v", err)
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

func requireChannelOwnerUnreadTotal(t *testing.T, s *store.Store, viewerID, channelID int64, want int) {
	t.Helper()
	result, err := api.GetPeerDialogsForTest(s, viewerID, &tg.MessagesGetPeerDialogsRequest{
		Peers: []tg.InputDialogPeerClass{&tg.InputDialogPeer{Peer: api.InputPeerChannel(viewerID, channelID)}},
	})
	if err != nil {
		t.Fatalf("getPeerDialogs for unread total: %v", err)
	}
	peerDialogs, ok := result.(*tg.MessagesPeerDialogs)
	if !ok {
		t.Fatalf("getPeerDialogs response = %T, want state", result)
	}
	if peerDialogs.State.UnreadCount != want {
		t.Errorf("getPeerDialogs total unread = %d, want %d", peerDialogs.State.UnreadCount, want)
	}
	stateResponse, err := api.GetStateForTest(s, viewerID)
	if err != nil {
		t.Fatalf("updates.getState for unread total: %v", err)
	}
	state, ok := stateResponse.(*tg.UpdatesState)
	if !ok || state == nil {
		t.Fatalf("updates.getState response = %T, want *tg.UpdatesState", stateResponse)
	}
	if state.UnreadCount != want {
		t.Errorf("updates.getState = %T unread %d, want %d", stateResponse, state.UnreadCount, want)
	}
	storedState, err := s.State(context.Background(), viewerID)
	if err != nil {
		t.Fatalf("store state for unread total: %v", err)
	}
	if storedState.UnreadCount != want {
		t.Errorf("store total unread = %d, want %d", storedState.UnreadCount, want)
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
	return channelsReadHistoryViaDispatcherWithContext(t, h, userID, provisional, channel, maxID, context.Background())
}

func channelsReadHistoryViaDispatcherWithContext(
	t *testing.T,
	h mtproto.Handler,
	userID int64,
	provisional bool,
	channel tg.InputChannelClass,
	maxID int,
	ctx context.Context,
) (bool, *mt.RPCError) {
	t.Helper()
	body := dispatchSettingsWithContext(t, h, settingsHandler{
		name: "channels.readHistory",
		request: func() bin.Encoder {
			return &tg.ChannelsReadHistoryRequest{Channel: channel, MaxID: maxID}
		},
	}, userID, provisional, ctx)
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
