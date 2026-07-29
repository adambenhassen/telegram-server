package api_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// channelFixture creates a channel owned by creator and returns the store,
// creator user, and channel. An optional member count adds fresh users as
// participants (role 0).
func channelFixture(t *testing.T, phonePrefix string, members int) (*store.Store, store.User, store.Channel, []store.User) {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	creator, err := s.CreateUser(ctx, phonePrefix+"001")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "test channel", "", true)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	inviteHash, err := s.CreateChannelInvite(ctx, ch.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	memberUsers := make([]store.User, 0, members)
	for i := range members {
		u, err := s.CreateUser(ctx, fmt.Sprintf("%s%03d", phonePrefix, i+2))
		if err != nil {
			t.Fatalf("member %d: %v", i, err)
		}
		_, _, err = s.JoinChannelByInvite(ctx, inviteHash, u.ID)
		if err != nil {
			t.Fatalf("join member %d: %v", i, err)
		}
		memberUsers = append(memberUsers, u)
	}
	return s, creator, ch, memberUsers
}

func TestGetChannelDifferenceThreePosts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, members := channelFixture(t, "+1555140", 1)
	member := members[0]

	// Post three messages as creator.
	for i := range 3 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// pts:0 returns all three, Final=true, Pts=3.
	enc, err := api.GetChannelDifferenceForTest(s, member.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=0): %v", err)
	}
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if !diff.Final {
		t.Fatal("Final = false, want true")
	}
	if diff.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", diff.Pts)
	}
	if len(diff.NewMessages) != 3 {
		t.Fatalf("NewMessages = %d, want 3", len(diff.NewMessages))
	}
}

func TestGetChannelDifferencePartialPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, members := channelFixture(t, "+1555141", 1)
	member := members[0]

	for i := range 3 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// pts:2 returns one message.
	enc, err := api.GetChannelDifferenceForTest(s, member.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     2,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=2): %v", err)
	}
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if !diff.Final {
		t.Fatal("Final = false, want true")
	}
	if diff.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", diff.Pts)
	}
	if len(diff.NewMessages) != 1 {
		t.Fatalf("NewMessages = %d, want 1", len(diff.NewMessages))
	}
}

func TestGetChannelDifferenceCaughtUp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, members := channelFixture(t, "+1555142", 1)
	member := members[0]

	for i := range 3 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// pts:3 (caught up) returns ChannelDifferenceEmpty.
	enc, err := api.GetChannelDifferenceForTest(s, member.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     3,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=3): %v", err)
	}
	empty, ok := enc.(*tg.UpdatesChannelDifferenceEmpty)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifferenceEmpty", enc)
	}
	if !empty.Final {
		t.Fatal("Final = false, want true")
	}
	if empty.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", empty.Pts)
	}
}

func TestGetChannelDifferenceAheadOfServer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, members := channelFixture(t, "+1555143", 1)
	member := members[0]

	for i := range 3 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// pts:99 (ahead of server) returns empty, not error.
	enc, err := api.GetChannelDifferenceForTest(s, member.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     99,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=99): %v", err)
	}
	empty, ok := enc.(*tg.UpdatesChannelDifferenceEmpty)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifferenceEmpty", enc)
	}
	if empty.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", empty.Pts)
	}
}

func TestGetChannelDifferenceJoinPtsClamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, _ := channelFixture(t, "+1555144", 0)

	// Post two messages before late joiner arrives.
	for i := range 2 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// Join late user at pts=2 (join_pts = 2).
	lateUser, err := s.CreateUser(ctx, "+1555144002")
	if err != nil {
		t.Fatalf("late user: %v", err)
	}
	inviteHash, err := s.CreateChannelInvite(ctx, ch.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	_, _, err = s.JoinChannelByInvite(ctx, inviteHash, lateUser.ID)
	if err != nil {
		t.Fatalf("join channel: %v", err)
	}

	// Post one more after join.
	_, _, _, err = s.PostChannelMessage(ctx, ch.ID, creator.ID, "after join", 3, nil)
	if err != nil {
		t.Fatalf("post after join: %v", err)
	}

	// Late joiner calling with pts:0 gets only events after join_pts (2).
	enc, err := api.GetChannelDifferenceForTest(s, lateUser.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(pts=0): %v", err)
	}
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if len(diff.NewMessages) != 1 {
		t.Fatalf("NewMessages = %d, want 1 (clamped to join_pts=2)", len(diff.NewMessages))
	}
	if diff.Pts != 3 {
		t.Fatalf("Pts = %d, want 3", diff.Pts)
	}
}

func TestGetChannelDifferenceNonMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, _ := channelFixture(t, "+1555145", 0)

	_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg", 1, nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	// Outsider gets PEER_ID_INVALID.
	outsider, err := s.CreateUser(ctx, "+1555145002")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	_, err = api.GetChannelDifferenceForTest(s, outsider.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err == nil {
		t.Fatal("expected error for non-member, got nil")
	}
}

func TestGetChannelDifferenceBanned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, members := channelFixture(t, "+1555146", 1)
	member := members[0]

	_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, "msg", 1, nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}

	// Ban the member.
	banUntil := time.Now().Add(time.Hour)
	err = s.SetChannelBan(ctx, ch.ID, creator.ID, member.ID, &banUntil, false)
	if err != nil {
		t.Fatalf("set channel ban: %v", err)
	}

	// Banned member gets PEER_ID_INVALID.
	_, err = api.GetChannelDifferenceForTest(s, member.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   100,
	})
	if err == nil {
		t.Fatal("expected error for banned member, got nil")
	}
}

func TestGetChannelDifferenceTruncated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, members := channelFixture(t, "+1555147", 1)
	member := members[0]

	// Post 5 messages.
	for i := range 5 {
		_, _, _, err := s.PostChannelMessage(ctx, ch.ID, creator.ID, fmt.Sprintf("msg %d", i), int64(i+1), nil)
		if err != nil {
			t.Fatalf("post %d: %v", i, err)
		}
	}

	// Request with limit=2 (capped to maxDiffEvents if higher, but 2 is within range).
	enc, err := api.GetChannelDifferenceForTest(s, member.ID, &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Filter:  &tg.ChannelMessagesFilterEmpty{},
		Pts:     0,
		Limit:   2,
	})
	if err != nil {
		t.Fatalf("getChannelDifference(limit=2): %v", err)
	}
	diff, ok := enc.(*tg.UpdatesChannelDifference)
	if !ok {
		t.Fatalf("type = %T, want *tg.UpdatesChannelDifference", enc)
	}
	if diff.Final {
		t.Fatal("Final = true, want false (truncated)")
	}
	if len(diff.NewMessages) != 2 {
		t.Fatalf("NewMessages = %d, want 2", len(diff.NewMessages))
	}
	if diff.Pts != 2 {
		t.Fatalf("Pts = %d, want 2 (last included event's pts)", diff.Pts)
	}
}

// inputUser names a target for editAdmin under the M1 placeholder access hash.
func inputUser(id int64) tg.InputUserClass {
	return &tg.InputUser{UserID: id, AccessHash: id}
}

// promote runs channels.editAdmin as callerID with a single admin right set,
// which the coarse role collapses to role 1.
func promote(t *testing.T, s *store.Store, callerID int64, ch store.Channel, targetID int64) (bin.Encoder, error) {
	t.Helper()
	return api.EditAdminForTest(s, callerID, &tg.ChannelsEditAdminRequest{
		Channel:     &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		UserID:      inputUser(targetID),
		AdminRights: tg.ChatAdminRights{BanUsers: true},
		Rank:        "boss",
	})
}

// banForever runs channels.editBanned as callerID with the permanent form: view
// messages revoked and no until date.
func banForever(t *testing.T, s *store.Store, callerID int64, ch store.Channel, targetID int64) (bin.Encoder, error) {
	t.Helper()
	return api.EditBannedForTest(s, callerID, &tg.ChannelsEditBannedRequest{
		Channel:      &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Participant:  &tg.InputPeerUser{UserID: targetID, AccessHash: targetID},
		BannedRights: tg.ChatBannedRights{ViewMessages: true},
	})
}

func channelRole(t *testing.T, s *store.Store, channelID, userID int64) int {
	t.Helper()
	m, found, err := s.ChannelMemberOf(context.Background(), channelID, userID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}
	if !found {
		t.Fatalf("user %d has no participant row in channel %d", userID, channelID)
	}
	return m.Role
}

// The whole promotion chain: the creator makes a member an admin, and that new
// admin can then reach a role-0 member with a ban. A single admin right is set
// on the request, which is what pins the coarse collapse — BanUsers alone
// produces a full admin.
func TestEditAdminPromotesAndThePromotedAdminCanBan(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298001")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551298002")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298003")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, admin.ID)
	joinChannelByInvite(t, s, ch, member.ID)

	res, err := promote(t, s, creator.ID, ch, admin.ID)
	if err != nil {
		t.Fatalf("promote: %v", err)
	}
	assertEncodes(t, res)
	if got := channelRole(t, s, ch.ID, admin.ID); got != 1 {
		t.Fatalf("role after promote = %d, want 1", got)
	}

	res, err = banForever(t, s, admin.ID, ch, member.ID)
	if err != nil {
		t.Fatalf("promoted admin ban: %v", err)
	}
	assertEncodes(t, res)
	banned, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("member of: found=%v err=%v", found, err)
	}
	if !banned.Banned(time.Now()) || !banned.Forever() {
		t.Errorf("ban is not permanent: banned=%v forever=%v", banned.Banned(time.Now()), banned.Forever())
	}
}

// Only the creator grants rights. A plain member and an admin both get the one
// error, and neither leaves a role behind.
func TestEditAdminRejectsEveryoneButTheCreator(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298011")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551298012")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298013")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, admin.ID)
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = promote(t, s, creator.ID, ch, admin.ID); err != nil {
		t.Fatalf("seed promote: %v", err)
	}

	// A role-0 member may promote nobody, not even themselves through another.
	if _, err = promote(t, s, member.ID, ch, admin.ID); err == nil {
		t.Fatal("member promote: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("member promote: got %s, want PEER_ID_INVALID", msg)
	}
	// An admin may ban, but may not grant rights to anyone.
	if _, err = promote(t, s, admin.ID, ch, member.ID); err == nil {
		t.Fatal("admin promote: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("admin promote: got %s, want PEER_ID_INVALID", msg)
	}
	if got := channelRole(t, s, ch.ID, member.ID); got != 0 {
		t.Errorf("member role = %d, want 0 — a rejected editAdmin wrote a role", got)
	}
	// The creator is not a target either, whoever asks.
	if _, err = promote(t, s, admin.ID, ch, creator.ID); err == nil {
		t.Fatal("promote creator: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("promote creator: got %s, want PEER_ID_INVALID", msg)
	}
}

// A retried promotion must not report failure. SetChannelRole rejects a
// transition to the role the target already holds with the same ErrNotMember a
// rights rejection returns, and MTProto clients retry.
func TestEditAdminRetryIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298021")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	admin, err := s.CreateUser(ctx, "+15551298022")
	if err != nil {
		t.Fatalf("admin: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, admin.ID)

	if _, err = promote(t, s, creator.ID, ch, admin.ID); err != nil {
		t.Fatalf("promote: %v", err)
	}
	res, err := promote(t, s, creator.ID, ch, admin.ID)
	if err != nil {
		t.Fatalf("retried promote: %v", err)
	}
	assertEncodes(t, res)
	if got := channelRole(t, s, ch.ID, admin.ID); got != 1 {
		t.Errorf("role after retry = %d, want 1", got)
	}

	// The demotion back to role 0 is a real transition and still works.
	if _, err = api.EditAdminForTest(s, creator.ID, &tg.ChannelsEditAdminRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		UserID:  inputUser(admin.ID),
	}); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if got := channelRole(t, s, ch.ID, admin.ID); got != 0 {
		t.Errorf("role after demote = %d, want 0", got)
	}
}

// A permanent ban revokes reads, and a zero rights struct gives them back.
func TestEditBannedBansPermanentlyAndUnbans(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298031")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298032")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "before the ban", 8100); err != nil {
		t.Fatalf("seed post: %v", err)
	}
	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(ch.ID)}); err != nil {
		t.Fatalf("history before ban: %v", err)
	}

	res, err := banForever(t, s, creator.ID, ch, member.ID)
	if err != nil {
		t.Fatalf("ban: %v", err)
	}
	assertEncodes(t, res)
	// The caller's own view of the channel is unaffected by banning someone else.
	if chats := updatesOf(t, res).Chats; len(chats) != 1 {
		t.Fatalf("ban reply: %d chats, want 1", len(chats))
	} else if _, ok := chats[0].(*tg.Channel); !ok {
		t.Fatalf("ban reply: got %T, want *tg.Channel for the caller", chats[0])
	}

	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(ch.ID)}); err == nil {
		t.Fatal("history after ban: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("history after ban: got %s, want PEER_ID_INVALID", msg)
	}

	// The zero rights struct is the unban.
	if _, err = api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:     &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Participant: &tg.InputPeerUser{UserID: member.ID, AccessHash: member.ID},
	}); err != nil {
		t.Fatalf("unban: %v", err)
	}
	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(ch.ID)}); err != nil {
		t.Fatalf("history after unban: %v", err)
	}
	m, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("member of: found=%v err=%v", found, err)
	}
	if m.Banned(time.Now()) {
		t.Error("member is still banned after the unban")
	}
}

// An until_date that has already passed is a client mistake, not a ban: writing
// it would report a ban that ChannelMember.Banned already reads as lapsed.
func TestEditBannedRejectsAPastUntilDate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298041")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298042")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)

	_, err = api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:     &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Participant: &tg.InputPeerUser{UserID: member.ID, AccessHash: member.ID},
		BannedRights: tg.ChatBannedRights{
			ViewMessages: true,
			UntilDate:    int(time.Now().Add(-time.Hour).Unix()),
		},
	})
	if err == nil {
		t.Fatal("past until_date: expected UNTIL_DATE_INVALID, got nil")
	}
	if msg := rpcMessage(t, err); msg != "UNTIL_DATE_INVALID" {
		t.Errorf("past until_date: got %s, want UNTIL_DATE_INVALID", msg)
	}
	m, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("member of: found=%v err=%v", found, err)
	}
	if m.BannedUntil != nil {
		t.Error("a rejected until_date still wrote banned_until")
	}
}

// A target with no participant row is rejected and no row is created: neither
// RPC is a push primitive re-entering through the side door.
func TestChannelEditsRejectATargetWithNoRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, outsider, ch := channelWith(t, s, "+15551298051", "+15551298052")

	if _, err := promote(t, s, creator.ID, ch, outsider.ID); err == nil {
		t.Fatal("editAdmin on a non-participant: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("editAdmin on a non-participant: got %s, want PEER_ID_INVALID", msg)
	}
	if _, err := banForever(t, s, creator.ID, ch, outsider.ID); err == nil {
		t.Fatal("editBanned on a non-participant: expected PEER_ID_INVALID, got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("editBanned on a non-participant: got %s, want PEER_ID_INVALID", msg)
	}

	if _, found, err := s.ChannelMemberOf(ctx, ch.ID, outsider.ID); err != nil {
		t.Fatalf("member of: %v", err)
	} else if found {
		t.Error("a rejected edit created a participant row")
	}
}

// The enumeration oracle the threat model closes: a stranger naming a channel
// that does not exist and a member with insufficient rights must get the
// byte-identical error, on both RPCs. Anything else confirms that a channel id
// is live, or what a target's role is.
func TestChannelEditFailuresAreIndistinguishable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, stranger, ch := channelWith(t, s, "+15551298061", "+15551298062")
	member, err := s.CreateUser(ctx, "+15551298063")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	other, err := s.CreateUser(ctx, "+15551298064")
	if err != nil {
		t.Fatalf("other: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	joinChannelByInvite(t, s, ch, other.ID)

	// An id past every channel this test created, so the row genuinely is absent.
	absent := store.Channel{ID: ch.ID + 1_000_000, CreatorID: creator.ID}

	_, adminNoChannel := promote(t, s, stranger.ID, absent, other.ID)
	_, adminNoRights := promote(t, s, member.ID, ch, other.ID)
	_, banNoChannel := banForever(t, s, stranger.ID, absent, other.ID)
	_, banNoRights := banForever(t, s, member.ID, ch, other.ID)

	for name, err := range map[string]error{
		"editAdmin unknown channel":  adminNoChannel,
		"editAdmin insufficient":     adminNoRights,
		"editBanned unknown channel": banNoChannel,
		"editBanned insufficient":    banNoRights,
	} {
		if err == nil {
			t.Fatalf("%s: got nil error", name)
		}
		if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
			t.Errorf("%s: got %s, want PEER_ID_INVALID", name, msg)
		}
	}
}

// Neither RPC serves an unauthenticated connection, and neither accepts a
// participant that is not a user.
func TestChannelEditsRejectBadInput(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	creator, _, ch := channelWith(t, s, "+15551298071", "+15551298072")

	if _, err := api.EditAdminForTest(s, 0, &tg.ChannelsEditAdminRequest{
		Channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID}, UserID: inputUser(creator.ID),
	}); err == nil {
		t.Fatal("editAdmin unauthenticated: got nil")
	} else if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Errorf("editAdmin unauthenticated: got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
	if _, err := api.EditBannedForTest(s, 0, &tg.ChannelsEditBannedRequest{
		Channel:     &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Participant: &tg.InputPeerUser{UserID: creator.ID, AccessHash: creator.ID},
	}); err == nil {
		t.Fatal("editBanned unauthenticated: got nil")
	} else if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Errorf("editBanned unauthenticated: got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
	// A chat peer names no channel participant row.
	if _, err := api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:     &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Participant: &tg.InputPeerChat{ChatID: 1},
	}); err == nil {
		t.Fatal("editBanned on a chat peer: got nil")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("editBanned on a chat peer: got %s, want PEER_ID_INVALID", msg)
	}
}

// A rights struct that revokes something other than view_messages has nothing
// M7 can store, and it must not fall through to the unban path: a caller
// tightening a restriction on a banned member would otherwise clear the ban and
// be told it worked.
func TestEditBannedRejectsAPartialRestriction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298081")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298082")
	if err != nil {
		t.Fatalf("member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Team", "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = banForever(t, s, creator.ID, ch, member.ID); err != nil {
		t.Fatalf("seed ban: %v", err)
	}

	_, err = api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:      &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Participant:  &tg.InputPeerUser{UserID: member.ID, AccessHash: member.ID},
		BannedRights: tg.ChatBannedRights{SendMessages: true},
	})
	if err == nil {
		t.Fatal("partial restriction: expected BANNED_RIGHTS_INVALID, got nil")
	}
	if msg := rpcMessage(t, err); msg != "BANNED_RIGHTS_INVALID" {
		t.Errorf("partial restriction: got %s, want BANNED_RIGHTS_INVALID", msg)
	}

	m, found, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !found {
		t.Fatalf("member of: found=%v err=%v", found, err)
	}
	if !m.Banned(time.Now()) || !m.Forever() {
		t.Fatalf("ban was cleared by a rejected edit: banned=%v forever=%v", m.Banned(time.Now()), m.Forever())
	}
	if _, err = api.GetHistoryForTest(s, member.ID, &tg.MessagesGetHistoryRequest{Peer: channelPeer(ch.ID)}); err == nil {
		t.Fatal("history after a rejected edit: the ban no longer revokes reads")
	} else if msg := rpcMessage(t, err); msg != "PEER_ID_INVALID" {
		t.Errorf("history after a rejected edit: got %s, want PEER_ID_INVALID", msg)
	}

	// The rejection is decided on the rights struct alone, so it does not need a
	// channel to exist — which is what keeps it off the post-read error collapse.
	_, err = api.EditBannedForTest(s, creator.ID, &tg.ChannelsEditBannedRequest{
		Channel:      &tg.InputChannel{ChannelID: ch.ID + 1_000_000, AccessHash: ch.ID + 1_000_000},
		Participant:  &tg.InputPeerUser{UserID: member.ID, AccessHash: member.ID},
		BannedRights: tg.ChatBannedRights{SendMessages: true},
	})
	if err == nil {
		t.Fatal("partial restriction on an absent channel: got nil")
	} else if msg := rpcMessage(t, err); msg != "BANNED_RIGHTS_INVALID" {
		t.Errorf("partial restriction on an absent channel: got %s, want BANNED_RIGHTS_INVALID", msg)
	}
}
