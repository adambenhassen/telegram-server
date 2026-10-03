package api_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/store"
)

func inviteChannelFixture(t *testing.T) (*store.Store, store.User, store.Channel, store.User) {
	t.Helper()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551299991")
	if err != nil {
		t.Fatal(err)
	}
	borrower, err := s.CreateUser(ctx, "+15551299992")
	if err != nil {
		t.Fatal(err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Invite", "", false)
	if err != nil {
		t.Fatal(err)
	}
	return s, creator, ch, borrower
}

func inviteUsers(viewerID int64, userIDs ...int64) []tg.InputUserClass {
	users := make([]tg.InputUserClass, len(userIDs))
	for i, userID := range userIDs {
		users[i] = api.InputUser(viewerID, userID)
	}
	return users
}

func rpcErrorMessage(err error) string {
	if rpc, ok := errors.AsType[*tgerr.Error](err); ok {
		return rpc.Message
	}
	return ""
}

func TestInviteToChannelRequiresViewerHashesAndAdminRole(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, borrower := inviteChannelFixture(t)
	member, err := s.CreateUser(ctx, "+15551299994")
	if err != nil {
		t.Fatal(err)
	}
	bannedCaller, err := s.CreateUser(ctx, "+15551299990")
	if err != nil {
		t.Fatal(err)
	}
	hash, err := s.CreateChannelInvite(ctx, ch.ID, creator.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, userID := range []int64{member.ID, bannedCaller.ID} {
		if _, _, err = s.JoinChannelByInvite(ctx, hash, userID); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.SetChannelBan(ctx, ch.ID, creator.ID, bannedCaller.ID, nil, true); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		caller int64
		ch     tg.InputChannelClass
		users  []tg.InputUserClass
		want   string
	}{
		{
			name:   "borrowed channel hash",
			caller: creator.ID,
			ch:     api.InputChannel(borrower.ID, ch.ID),
			users:  inviteUsers(creator.ID, borrower.ID),
			want:   "PEER_ID_INVALID",
		},
		{
			name:   "borrowed user hash",
			caller: creator.ID,
			ch:     api.InputChannel(creator.ID, ch.ID),
			users:  inviteUsers(borrower.ID, member.ID),
			want:   "PEER_ID_INVALID",
		},
		{
			name:   "user without a per-viewer hash",
			caller: creator.ID,
			ch:     api.InputChannel(creator.ID, ch.ID),
			users:  []tg.InputUserClass{&tg.InputUser{UserID: member.ID}},
			want:   "PEER_ID_INVALID",
		},
		{
			name:   "outsider caller",
			caller: borrower.ID,
			ch:     api.InputChannel(borrower.ID, ch.ID),
			users:  inviteUsers(borrower.ID, member.ID),
			want:   "PEER_ID_INVALID",
		},
		{
			name:   "member caller",
			caller: member.ID,
			ch:     api.InputChannel(member.ID, ch.ID),
			users:  inviteUsers(member.ID, borrower.ID),
			want:   "PEER_ID_INVALID",
		},
		{
			name:   "banned caller",
			caller: bannedCaller.ID,
			ch:     api.InputChannel(bannedCaller.ID, ch.ID),
			users:  inviteUsers(bannedCaller.ID, member.ID),
			want:   "PEER_ID_INVALID",
		},
		{
			name:   "input user self has no hash",
			caller: creator.ID,
			ch:     api.InputChannel(creator.ID, ch.ID),
			users:  []tg.InputUserClass{&tg.InputUserSelf{}},
			want:   "PEER_ID_INVALID",
		},
		{
			name:   "provisional session",
			caller: 0,
			ch:     api.InputChannel(creator.ID, ch.ID),
			users:  inviteUsers(creator.ID, borrower.ID),
			want:   "AUTH_KEY_UNREGISTERED",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := api.InviteToChannelForTest(s, tt.caller, &tg.ChannelsInviteToChannelRequest{
				Channel: tt.ch,
				Users:   tt.users,
			})
			if got := rpcErrorMessage(err); got != tt.want {
				t.Fatalf("invite error = %q (%v), want %q", got, err, tt.want)
			}
		})
	}
	if _, found, err := s.ChannelMemberOf(ctx, ch.ID, borrower.ID); err != nil || found {
		t.Errorf("refused invitation wrote membership: found=%v err=%v", found, err)
	}
}

func TestInviteToChannelAdminMayAddTarget(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, _ := inviteChannelFixture(t)
	admin, err := s.CreateUser(ctx, "+15551299989")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "+15551299988")
	if err != nil {
		t.Fatal(err)
	}
	hash, err := s.CreateChannelInvite(ctx, ch.ID, creator.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, hash, admin.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.SetChannelRole(ctx, ch.ID, creator.ID, admin.ID, 1); err != nil {
		t.Fatal(err)
	}
	res, err := api.InviteToChannelForTest(s, admin.ID, &tg.ChannelsInviteToChannelRequest{
		Channel: api.InputChannel(admin.ID, ch.ID),
		Users:   inviteUsers(admin.ID, target.ID),
	})
	if err != nil {
		t.Fatalf("admin invite: %v", err)
	}
	if _, ok := res.(*tg.MessagesInvitedUsers); !ok {
		t.Fatalf("admin invite response = %T, want *tg.MessagesInvitedUsers", res)
	}
	if _, found, err := s.ChannelMemberOf(ctx, ch.ID, target.ID); err != nil || !found {
		t.Fatalf("admin invite membership: found=%v err=%v", found, err)
	}
}

func TestInviteToChannelChargesPerTargetAndCapsRequestSize(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, _ := inviteChannelFixture(t)
	first, err := s.CreateUser(ctx, "+15551299995")
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.CreateUser(ctx, "+15551299996")
	if err != nil {
		t.Fatal(err)
	}
	req := &tg.ChannelsInviteToChannelRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Users:   inviteUsers(creator.ID, first.ID, second.ID),
	}
	_, err = api.InviteToChannelForTestWithLimits(s, creator.ID, store.RateLimitConfig{
		Limit: 1, Window: time.Minute,
	}, req)
	if !isFloodWait(err) {
		t.Fatalf("two targets at limit one: err=%v, want FLOOD_WAIT", err)
	}
	for _, userID := range []int64{first.ID, second.ID} {
		if _, found, err := s.ChannelMemberOf(ctx, ch.ID, userID); err != nil || found {
			t.Errorf("rate-limited target %d admitted: found=%v err=%v", userID, found, err)
		}
	}

	tooMany := make([]tg.InputUserClass, 101)
	for i := range tooMany {
		tooMany[i] = api.InputUser(creator.ID, first.ID)
	}
	_, err = api.InviteToChannelForTest(s, creator.ID, &tg.ChannelsInviteToChannelRequest{
		Channel: api.InputChannel(creator.ID, ch.ID), Users: tooMany,
	})
	if got := rpcErrorMessage(err); got != "USERS_TOO_MUCH" {
		t.Fatalf("101 targets: error = %q (%v), want USERS_TOO_MUCH", got, err)
	}
	if _, found, err := s.ChannelMemberOf(ctx, ch.ID, first.ID); err != nil || found {
		t.Errorf("oversized request admitted target: found=%v err=%v", found, err)
	}
}

func TestInviteToChannelSkipsBannedAndExistingRowsWithoutTargetDetails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, creator, ch, _ := inviteChannelFixture(t)
	existing, err := s.CreateUser(ctx, "+15551299997")
	if err != nil {
		t.Fatal(err)
	}
	banned, err := s.CreateUser(ctx, "+15551299998")
	if err != nil {
		t.Fatal(err)
	}
	newUser, err := s.CreateUser(ctx, "+15551299999")
	if err != nil {
		t.Fatal(err)
	}
	hash, err := s.CreateChannelInvite(ctx, ch.ID, creator.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, userID := range []int64{existing.ID, banned.ID} {
		if _, _, err := s.JoinChannelByInvite(ctx, hash, userID); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.SetChannelBan(ctx, ch.ID, creator.ID, banned.ID, nil, true); err != nil {
		t.Fatalf("ban target: %v", err)
	}
	before, found, err := s.ChannelMemberOf(ctx, ch.ID, banned.ID)
	if err != nil || !found || !before.Banned(time.Now()) {
		t.Fatalf("banned target before invite: member=%+v found=%v err=%v", before, found, err)
	}

	res, err := api.InviteToChannelForTest(s, creator.ID, &tg.ChannelsInviteToChannelRequest{
		Channel: api.InputChannel(creator.ID, ch.ID),
		Users:   inviteUsers(creator.ID, banned.ID, existing.ID, newUser.ID),
	})
	if err != nil {
		t.Fatalf("invite with skipped targets: %v", err)
	}
	invited, ok := res.(*tg.MessagesInvitedUsers)
	if !ok {
		t.Fatalf("invite response = %T, want *tg.MessagesInvitedUsers", res)
	}
	if len(invited.MissingInvitees) != 0 {
		t.Fatalf("invite response disclosed %d skipped targets", len(invited.MissingInvitees))
	}
	after, found, err := s.ChannelMemberOf(ctx, ch.ID, banned.ID)
	if err != nil || !found || after.Role != before.Role || after.JoinPts != before.JoinPts || !after.Banned(time.Now()) {
		t.Errorf("banned row after invite = %+v, want unchanged %+v (found=%v err=%v)", after, before, found, err)
	}
	if _, found, err := s.ChannelMemberOf(ctx, ch.ID, newUser.ID); err != nil || !found {
		t.Errorf("eligible target not added: found=%v err=%v", found, err)
	}
	if _, found, err := s.ChannelMemberOf(ctx, ch.ID, existing.ID); err != nil || !found {
		t.Errorf("existing target removed: found=%v err=%v", found, err)
	}
}

func TestInviteToChannelAppearsInDialogsAfterMembershipNotifyLoss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551282101")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	invitee, err := s.CreateUser(ctx, "+15551282102")
	if err != nil {
		t.Fatalf("create invitee: %v", err)
	}
	empty, err := s.CreateChannel(ctx, creator.ID, "Empty invite", "", false)
	if err != nil {
		t.Fatalf("create empty channel: %v", err)
	}
	history, err := s.CreateChannel(ctx, creator.ID, "History invite", "", false)
	if err != nil {
		t.Fatalf("create history channel: %v", err)
	}
	for i, text := range []string{"first", "latest"} {
		if _, _, _, err := s.PostChannelMessage(ctx, history.ID, creator.ID, text, int64(i+1), nil, 0); err != nil {
			t.Fatalf("post history %q: %v", text, err)
		}
	}

	// Seed 100 older memberships with ids below the channel-id range used by
	// CreateChannel. This makes the new membership fall beyond the old 100-row
	// dialog cap regardless of the random id drawn for the invite target.
	channelExec(t, ctx, dsn, `
		INSERT INTO channels (id, title, creator_id)
		SELECT 1000000 + n, 'Existing ' || n, $1
		FROM generate_series(1, 100) AS n`, invitee.ID)
	channelExec(t, ctx, dsn, `
		INSERT INTO channel_state (channel_id)
		SELECT 1000000 + n FROM generate_series(1, 100) AS n`)
	channelExec(t, ctx, dsn, `
		INSERT INTO channel_participants (channel_id, user_id, role, join_pts, date)
		SELECT 1000000 + n, $1, 2, 0, now() - interval '1 day'
		FROM generate_series(1, 100) AS n`, invitee.ID)

	older, err := s.ChannelDialogsForUser(ctx, invitee.ID)
	if err != nil || len(older) != 100 {
		t.Fatalf("seeded memberships = %d, err %v; want 100", len(older), err)
	}

	// testHandlers has no store listener, so these membership NOTIFYs have no
	// receiver. A fresh getDialogs call must replay the committed membership.
	for _, ch := range []store.Channel{empty, history} {
		if _, err := api.InviteToChannelForTest(s, creator.ID, &tg.ChannelsInviteToChannelRequest{
			Channel: api.InputChannel(creator.ID, ch.ID),
			Users:   inviteUsers(creator.ID, invitee.ID),
		}); err != nil {
			t.Fatalf("invite to %q: %v", ch.Title, err)
		}
	}

	pull := func() *tg.MessagesDialogs {
		t.Helper()
		enc, err := api.GetDialogsForTest(s, invitee.ID)
		if err != nil {
			t.Fatalf("getDialogs: %v", err)
		}
		assertEncodes(t, enc)
		got, ok := enc.(*tg.MessagesDialogs)
		if !ok {
			t.Fatalf("getDialogs = %T, want *tg.MessagesDialogs", enc)
		}
		return got
	}
	first := pull()
	dialogsByChannel := make(map[int64]*tg.Dialog)
	for _, class := range first.Dialogs {
		d, ok := class.(*tg.Dialog)
		if !ok {
			continue
		}
		peer, ok := d.Peer.(*tg.PeerChannel)
		if ok {
			dialogsByChannel[peer.ChannelID] = d
		}
	}
	for _, ch := range []store.Channel{empty, history} {
		if _, ok := dialogsByChannel[ch.ID]; !ok {
			t.Fatalf("fresh getDialogs omitted invited channel %d with 102 memberships", ch.ID)
		}
	}
	if d := dialogsByChannel[empty.ID]; d.TopMessage != 0 {
		t.Errorf("empty channel top_message = %d, want 0", d.TopMessage)
	} else if pts, hasPts := d.GetPts(); !hasPts || pts != 0 {
		t.Errorf("empty channel pts = %d present=%v, want 0", pts, hasPts)
	}
	historyDialog := dialogsByChannel[history.ID]
	if historyDialog.TopMessage != 2 {
		t.Errorf("history top_message = %d, want 2", historyDialog.TopMessage)
	}
	if pts, hasPts := historyDialog.GetPts(); !hasPts || pts != 2 {
		t.Errorf("history pts = %d present=%v, want 2", pts, hasPts)
	}
	if len(first.Messages) != 1 || first.Messages[0].GetID() != 2 {
		t.Errorf("getDialogs messages = %v, want only history message 2", first.Messages)
	}
	member, found, err := s.ChannelMemberOf(ctx, history.ID, invitee.ID)
	if err != nil || !found || member.JoinPts != 2 {
		t.Errorf("history membership = %+v found=%v err=%v, want join_pts 2", member, found, err)
	}
	channelHashes := make(map[int64]int64)
	for _, class := range first.Chats {
		if ch, ok := class.(*tg.Channel); ok {
			channelHashes[ch.ID] = ch.AccessHash
		}
	}
	if got, want := channelHashes[empty.ID], api.DeriveChannelHash(invitee.ID, empty.ID); got != want {
		t.Errorf("invitee channel hash = %d, want viewer-specific hash %d", got, want)
	}
	if got, want := channelHashes[history.ID], api.DeriveChannelHash(invitee.ID, history.ID); got != want {
		t.Errorf("history channel hash = %d, want viewer-specific hash %d", got, want)
	}

	assertOmitted := func(channelID int64) {
		t.Helper()
		got := pull()
		for _, class := range got.Dialogs {
			if d, ok := class.(*tg.Dialog); ok {
				if peer, ok := d.Peer.(*tg.PeerChannel); ok && peer.ChannelID == channelID {
					t.Errorf("getDialogs retained channel %d after access ended", channelID)
				}
			}
		}
		for _, class := range got.Chats {
			switch ch := class.(type) {
			case *tg.Channel:
				if ch.ID == channelID {
					t.Errorf("getDialogs retained channel peer %d after access ended", channelID)
				}
			case *tg.ChannelForbidden:
				if ch.ID == channelID {
					t.Errorf("getDialogs returned forbidden channel %d instead of omitting it", channelID)
				}
			}
		}
	}

	if err := s.SetChannelBan(ctx, empty.ID, creator.ID, invitee.ID, nil, true); err != nil {
		t.Fatalf("ban invitee: %v", err)
	}
	assertOmitted(empty.ID)
	if err := s.SetChannelBan(ctx, empty.ID, creator.ID, invitee.ID, nil, false); err != nil {
		t.Fatalf("unban invitee: %v", err)
	}
	if _, err := s.LeaveChannel(ctx, empty.ID, invitee.ID); err != nil {
		t.Fatalf("invitee leaves: %v", err)
	}
	assertOmitted(empty.ID)
}
