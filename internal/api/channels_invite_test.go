package api_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
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
