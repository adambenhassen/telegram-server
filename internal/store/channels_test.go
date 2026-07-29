package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func mustChannel(t *testing.T, s *store.Store, creatorID int64, title string) store.Channel {
	t.Helper()
	ch, err := s.CreateChannel(context.Background(), creatorID, title, "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	return ch
}

func TestCreateChannelSeatsTheCreatorAsOwner(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551291001")

	ch, err := s.CreateChannel(ctx, u.ID, "News", "about", false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if ch.ID == 0 || ch.CreatorID != u.ID || ch.Title != "News" || ch.About != "about" {
		t.Fatalf("channel = %+v, want creator %d titled News", ch, u.ID)
	}
	if ch.Megagroup || ch.Version != 1 {
		t.Errorf("megagroup = %v, version = %d, want false/1", ch.Megagroup, ch.Version)
	}

	got, ok, err := s.ChannelByID(ctx, ch.ID)
	if err != nil || !ok {
		t.Fatalf("read back: ok=%v err=%v", ok, err)
	}
	if got.ID != ch.ID || got.Title != ch.Title {
		t.Errorf("read back = %+v, want %+v", got, ch)
	}

	m, ok, err := s.ChannelMemberOf(ctx, ch.ID, u.ID)
	if err != nil || !ok {
		t.Fatalf("creator membership: ok=%v err=%v", ok, err)
	}
	if m.UserID != u.ID || m.Role != 2 || m.JoinPts != 0 || m.BannedUntil != nil {
		t.Errorf("creator row = %+v, want role 2 join_pts 0 unbanned", m)
	}

	members, err := s.ChannelMembers(ctx, ch.ID)
	if err != nil {
		t.Fatalf("members: %v", err)
	}
	if len(members) != 1 || members[0].UserID != u.ID {
		t.Errorf("members = %+v, want just the creator", members)
	}
}

func TestChannelByIDAndMemberOfReportAbsence(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291002")
	outsider := mustUser(t, s, "+15551291003")
	ch := mustChannel(t, s, owner.ID, "News")

	if _, ok, err := s.ChannelByID(ctx, ch.ID+(1<<40)); err != nil || ok {
		t.Errorf("absent channel: ok=%v err=%v, want false/nil", ok, err)
	}
	m, ok, err := s.ChannelMemberOf(ctx, ch.ID, outsider.ID)
	if err != nil || ok {
		t.Errorf("non-member: ok=%v err=%v, want false/nil", ok, err)
	}
	if m != (store.ChannelMember{}) {
		t.Errorf("non-member row = %+v, want zero value", m)
	}
}

func TestCreateChannelInviteIsA22CharHash(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291004")
	ch := mustChannel(t, s, owner.ID, "News")

	a, err := s.CreateChannelInvite(ctx, ch.ID, owner.ID)
	if err != nil {
		t.Fatalf("invite: %v", err)
	}
	if len(a) != 22 {
		t.Errorf("hash %q is %d chars, want 22", a, len(a))
	}
	b, err := s.CreateChannelInvite(ctx, ch.ID, owner.ID)
	if err != nil {
		t.Fatalf("second invite: %v", err)
	}
	if a == b {
		t.Error("two invites drew the same hash")
	}
}

func TestJoinChannelByInviteRecordsTheCurrentPts(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291005")
	joiner := mustUser(t, s, "+15551291006")
	ch := mustChannel(t, s, owner.ID, "News")
	hash, err := s.CreateChannelInvite(ctx, ch.ID, owner.ID)
	if err != nil {
		t.Fatalf("invite: %v", err)
	}
	if err := store.SetChannelPts(ctx, s, ch.ID, 5); err != nil {
		t.Fatalf("set pts: %v", err)
	}

	got, m, err := s.JoinChannelByInvite(ctx, hash, joiner.ID)
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if got.ID != ch.ID {
		t.Errorf("joined channel %d, want %d", got.ID, ch.ID)
	}
	if m.UserID != joiner.ID || m.Role != 0 || m.JoinPts != 5 {
		t.Fatalf("member = %+v, want role 0 join_pts 5", m)
	}

	// The joiner's difference floor must not move backwards on a re-join, and a
	// post landing in between must not be replayable by rejoining.
	if err := store.SetChannelPts(ctx, s, ch.ID, 9); err != nil {
		t.Fatalf("set pts: %v", err)
	}
	_, again, err := s.JoinChannelByInvite(ctx, hash, joiner.ID)
	if err != nil {
		t.Fatalf("re-join: %v", err)
	}
	if again.JoinPts != 5 || again.Role != 0 {
		t.Errorf("re-join = %+v, want the original join_pts 5", again)
	}

	chans, err := s.ChannelsForUser(ctx, joiner.ID)
	if err != nil {
		t.Fatalf("channels for user: %v", err)
	}
	if len(chans) != 1 || chans[0].ID != ch.ID {
		t.Errorf("channels for joiner = %+v, want just %d", chans, ch.ID)
	}
}

func TestJoinChannelByInviteRejectsAnUnknownHash(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291007")
	joiner := mustUser(t, s, "+15551291008")
	ch := mustChannel(t, s, owner.ID, "News")

	_, _, err := s.JoinChannelByInvite(ctx, "AAAAAAAAAAAAAAAAAAAAAA", joiner.ID)
	if !errors.Is(err, store.ErrInviteInvalid) {
		t.Fatalf("unknown hash: err = %v, want ErrInviteInvalid", err)
	}
	if _, ok, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID); err != nil || ok {
		t.Errorf("rejected join wrote a row: ok=%v err=%v", ok, err)
	}
}

func TestLeaveChannelIsFalseTheSecondTime(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291009")
	joiner := mustUser(t, s, "+15551291010")
	ch := mustChannel(t, s, owner.ID, "News")
	hash, err := s.CreateChannelInvite(ctx, ch.ID, owner.ID)
	if err != nil {
		t.Fatalf("invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, hash, joiner.ID); err != nil {
		t.Fatalf("join: %v", err)
	}

	left, err := s.LeaveChannel(ctx, ch.ID, joiner.ID)
	if err != nil || !left {
		t.Fatalf("leave: left=%v err=%v", left, err)
	}
	left, err = s.LeaveChannel(ctx, ch.ID, joiner.ID)
	if err != nil || left {
		t.Fatalf("second leave: left=%v err=%v, want false/nil", left, err)
	}
	if _, ok, err := s.ChannelMemberOf(ctx, ch.ID, joiner.ID); err != nil || ok {
		t.Errorf("row survived leave: ok=%v err=%v", ok, err)
	}
}

func TestChannelMemberBanned(t *testing.T) {
	t.Parallel()
	now := time.Now()
	future := now.Add(time.Hour)
	past := now.Add(-time.Hour)

	cases := []struct {
		name  string
		until *time.Time
		want  bool
	}{
		{"null is not banned", nil, false},
		{"future is banned", &future, true},
		{"past is not banned", &past, false},
		{"exactly now is not banned", &now, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			m := store.ChannelMember{UserID: 1, BannedUntil: c.until}
			if got := m.Banned(now); got != c.want {
				t.Errorf("Banned(%v) = %v, want %v", c.until, got, c.want)
			}
		})
	}
}

// A ban survives a re-join: the ON CONFLICT DO NOTHING path returns the row as
// it stands rather than writing a fresh unbanned one.
func TestRejoinDoesNotClearABan(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291011")
	joiner := mustUser(t, s, "+15551291012")
	ch := mustChannel(t, s, owner.ID, "News")
	hash, err := s.CreateChannelInvite(ctx, ch.ID, owner.ID)
	if err != nil {
		t.Fatalf("invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, hash, joiner.ID); err != nil {
		t.Fatalf("join: %v", err)
	}
	until := time.Now().Add(24 * time.Hour)
	if err := store.SetChannelBan(ctx, s, ch.ID, joiner.ID, &until); err != nil {
		t.Fatalf("ban: %v", err)
	}

	_, m, err := s.JoinChannelByInvite(ctx, hash, joiner.ID)
	if err != nil {
		t.Fatalf("re-join: %v", err)
	}
	if !m.Banned(time.Now()) {
		t.Errorf("re-join cleared the ban: %+v", m)
	}
	if m.Forever() {
		t.Errorf("finite ban reported as permanent: %+v", m)
	}
}

// 'infinity' is a permanent ban. pgx decodes it as a zero time.Time carrying an
// infinity modifier, so a naive conversion turns it into a ban that expired in
// year 1 — the one decode this type has to get right.
func TestInfiniteBanIsPermanent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291013")
	ch := mustChannel(t, s, owner.ID, "News")
	if err := store.SetChannelBanInfinite(ctx, s, ch.ID, owner.ID); err != nil {
		t.Fatalf("ban: %v", err)
	}

	m, ok, err := s.ChannelMemberOf(ctx, ch.ID, owner.ID)
	if err != nil || !ok {
		t.Fatalf("member: ok=%v err=%v", ok, err)
	}
	if !m.Banned(time.Now()) {
		t.Errorf("infinite ban read as not banned: %+v", m)
	}
	// Forever is how a handler recognises this without knowing the year-9999
	// stand-in, which must never reach the wire.
	if !m.Forever() {
		t.Errorf("infinite ban not reported as permanent: %+v", m)
	}
}

// roleOf reads the target's role, failing the test if it holds no row.
func roleOf(t *testing.T, s *store.Store, channelID, userID int64) int {
	t.Helper()
	m, ok, err := s.ChannelMemberOf(context.Background(), channelID, userID)
	if err != nil || !ok {
		t.Fatalf("member of %d: ok=%v err=%v", channelID, ok, err)
	}
	return m.Role
}

func TestSetChannelRolePromotesAndDemotesForTheCreatorOnly(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291030")
	member := mustUser(t, s, "+15551291031")
	ch, _, err := store.SeedChannelWithMember(ctx, s, owner.ID, member.ID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, member.ID, 1); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if got := roleOf(t, s, ch.ID, member.ID); got != 1 {
		t.Fatalf("role after promote = %d, want 1", got)
	}
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, member.ID, 0); err != nil {
		t.Fatalf("demote: %v", err)
	}
	if got := roleOf(t, s, ch.ID, member.ID); got != 0 {
		t.Errorf("role after demote = %d, want 0", got)
	}

	// G3 lists promotion and demotion and nothing else, so a role the target
	// already holds is unlisted and fails closed.
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, member.ID, 0); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("member to member: err = %v, want ErrNotMember", err)
	}
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, member.ID, 1); err != nil {
		t.Fatalf("re-promote: %v", err)
	}
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, member.ID, 1); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("admin to admin: err = %v, want ErrNotMember", err)
	}
	if got := roleOf(t, s, ch.ID, member.ID); got != 1 {
		t.Errorf("role after rejected no-ops = %d, want 1", got)
	}
}

// G3: an admin holds no promotion right at all, and the rejection is the same
// error an outsider gets.
func TestSetChannelRoleRejectsAnAdminCaller(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291032")
	admin := mustUser(t, s, "+15551291033")
	member := mustUser(t, s, "+15551291034")
	ch, hash, err := store.SeedChannelWithMember(ctx, s, owner.ID, admin.ID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, hash, member.ID); err != nil {
		t.Fatalf("join member: %v", err)
	}
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}

	if err := s.SetChannelRole(ctx, ch.ID, admin.ID, member.ID, 1); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("admin promoting: err = %v, want ErrNotMember", err)
	}
	if got := roleOf(t, s, ch.ID, member.ID); got != 0 {
		t.Errorf("role moved to %d on a rejected promotion", got)
	}
	// Nor may an admin demote another admin, and nobody may demote the creator.
	if err := s.SetChannelRole(ctx, ch.ID, admin.ID, admin.ID, 0); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("admin demoting itself: err = %v, want ErrNotMember", err)
	}
	if err := s.SetChannelRole(ctx, ch.ID, admin.ID, owner.ID, 0); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("admin demoting the creator: err = %v, want ErrNotMember", err)
	}
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, owner.ID, 0); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("creator demoting itself: err = %v, want ErrNotMember", err)
	}
	if got := roleOf(t, s, ch.ID, owner.ID); got != 2 {
		t.Errorf("creator role = %d, want 2", got)
	}
	if got := roleOf(t, s, ch.ID, admin.ID); got != 1 {
		t.Errorf("admin role = %d, want 1", got)
	}
	// Role 2 is never assignable.
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, member.ID, 2); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("assigning role 2: err = %v, want ErrNotMember", err)
	}
	if got := roleOf(t, s, ch.ID, member.ID); got != 0 {
		t.Errorf("role moved to %d on a rejected role value", got)
	}
}

func TestSetChannelBanAdminMayBanAMemberOnly(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291035")
	admin := mustUser(t, s, "+15551291036")
	other := mustUser(t, s, "+15551291037")
	member := mustUser(t, s, "+15551291038")
	ch, hash, err := store.SeedChannelWithMember(ctx, s, owner.ID, admin.ID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	for _, u := range []int64{other.ID, member.ID} {
		if _, _, err := s.JoinChannelByInvite(ctx, hash, u); err != nil {
			t.Fatalf("join %d: %v", u, err)
		}
	}
	for _, u := range []int64{admin.ID, other.ID} {
		if err := s.SetChannelRole(ctx, ch.ID, owner.ID, u, 1); err != nil {
			t.Fatalf("promote %d: %v", u, err)
		}
	}

	until := time.Now().Add(time.Hour)
	if err := s.SetChannelBan(ctx, ch.ID, admin.ID, member.ID, &until, false); err != nil {
		t.Fatalf("admin bans member: %v", err)
	}
	m, _, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}
	if !m.Banned(time.Now()) {
		t.Fatalf("member not banned: %+v", m)
	}

	// An admin over another admin, and anyone over the creator, are rejected and
	// write nothing.
	if err := s.SetChannelBan(ctx, ch.ID, admin.ID, other.ID, &until, false); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("admin bans admin: err = %v, want ErrNotMember", err)
	}
	for _, caller := range []int64{admin.ID, member.ID, owner.ID} {
		if err := s.SetChannelBan(ctx, ch.ID, caller, owner.ID, &until, false); !errors.Is(err, store.ErrNotMember) {
			t.Errorf("caller %d bans the creator: err = %v, want ErrNotMember", caller, err)
		}
	}
	for _, u := range []int64{other.ID, owner.ID} {
		got, _, err := s.ChannelMemberOf(ctx, ch.ID, u)
		if err != nil {
			t.Fatalf("member of: %v", err)
		}
		if got.BannedUntil != nil {
			t.Errorf("rejected ban wrote banned_until on %d: %+v", u, got)
		}
	}

	// A plain member holds no rights on either method.
	if err := s.SetChannelBan(ctx, ch.ID, member.ID, other.ID, &until, false); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("member bans: err = %v, want ErrNotMember", err)
	}
}

// A banned caller has no rights on either method: the ban would otherwise be
// toothless against an admin, whose row also survives leaving.
func TestChannelRightsRejectABannedCaller(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291046")
	admin := mustUser(t, s, "+15551291047")
	member := mustUser(t, s, "+15551291048")
	ch, hash, err := store.SeedChannelWithMember(ctx, s, owner.ID, admin.ID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, hash, member.ID); err != nil {
		t.Fatalf("join member: %v", err)
	}
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	until := time.Now().Add(time.Hour)
	if err := s.SetChannelBan(ctx, ch.ID, owner.ID, admin.ID, &until, false); err != nil {
		t.Fatalf("creator bans admin: %v", err)
	}

	if err := s.SetChannelBan(ctx, ch.ID, admin.ID, member.ID, &until, false); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("banned admin bans: err = %v, want ErrNotMember", err)
	}
	if err := s.SetChannelRole(ctx, ch.ID, admin.ID, member.ID, 1); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("banned admin promotes: err = %v, want ErrNotMember", err)
	}
	got, ok, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil || !ok {
		t.Fatalf("member of: ok=%v err=%v", ok, err)
	}
	if got.Role != 0 || got.BannedUntil != nil {
		t.Errorf("a banned caller wrote to the target: %+v", got)
	}
}

func TestChannelRightsRejectSelfAndAbsentTargets(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291039")
	member := mustUser(t, s, "+15551291040")
	outsider := mustUser(t, s, "+15551291041")
	ch, _, err := store.SeedChannelWithMember(ctx, s, owner.ID, member.ID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	until := time.Now().Add(time.Hour)

	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, owner.ID, 1); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("self role: err = %v, want ErrNotMember", err)
	}
	if err := s.SetChannelBan(ctx, ch.ID, owner.ID, owner.ID, &until, false); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("self ban: err = %v, want ErrNotMember", err)
	}

	// A target with no participant row is rejected, and no row is created — the
	// push primitive must not re-enter through these methods.
	if err := s.SetChannelRole(ctx, ch.ID, owner.ID, outsider.ID, 1); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("absent target role: err = %v, want ErrNotMember", err)
	}
	if err := s.SetChannelBan(ctx, ch.ID, owner.ID, outsider.ID, &until, false); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("absent target ban: err = %v, want ErrNotMember", err)
	}
	if _, ok, err := s.ChannelMemberOf(ctx, ch.ID, outsider.ID); err != nil || ok {
		t.Errorf("a rejected mutation created a row: ok=%v err=%v", ok, err)
	}

	// A caller with no row of its own, and a channel that does not exist, are the
	// same rejection.
	if err := s.SetChannelRole(ctx, ch.ID, outsider.ID, member.ID, 1); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("outsider caller: err = %v, want ErrNotMember", err)
	}
	if err := s.SetChannelBan(ctx, ch.ID+(1<<40), owner.ID, member.ID, &until, false); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("unknown channel: err = %v, want ErrNotMember", err)
	}
}

func TestSetChannelBanForeverThenUnban(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291042")
	member := mustUser(t, s, "+15551291043")
	ch, _, err := store.SeedChannelWithMember(ctx, s, owner.ID, member.ID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := s.SetChannelBan(ctx, ch.ID, owner.ID, member.ID, nil, true); err != nil {
		t.Fatalf("ban forever: %v", err)
	}
	m, _, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}
	if !m.Banned(time.Now()) || !m.Forever() {
		t.Fatalf("forever ban read back as %+v, want banned and permanent", m)
	}

	if err := s.SetChannelBan(ctx, ch.ID, owner.ID, member.ID, nil, false); err != nil {
		t.Fatalf("unban: %v", err)
	}
	m, _, err = s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}
	if m.BannedUntil != nil || m.Banned(time.Now()) {
		t.Errorf("unban left %+v, want banned_until NULL", m)
	}
}

// A ban outlives membership: leaving does not delete a banned row, so a re-join
// on the same hash comes back banned and at the original join_pts.
func TestBannedMemberCannotLeaveAwayTheBan(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291044")
	member := mustUser(t, s, "+15551291045")
	ch, hash, err := store.SeedChannelWithMember(ctx, s, owner.ID, member.ID)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	before, _, err := s.ChannelMemberOf(ctx, ch.ID, member.ID)
	if err != nil {
		t.Fatalf("member of: %v", err)
	}
	if err := s.SetChannelBan(ctx, ch.ID, owner.ID, member.ID, nil, true); err != nil {
		t.Fatalf("ban: %v", err)
	}

	// The caller sees a normal leave.
	left, err := s.LeaveChannel(ctx, ch.ID, member.ID)
	if err != nil || !left {
		t.Fatalf("leave: left=%v err=%v, want true/nil", left, err)
	}
	if _, ok, err := s.ChannelMemberOf(ctx, ch.ID, member.ID); err != nil || !ok {
		t.Fatalf("banned row deleted by leave: ok=%v err=%v", ok, err)
	}

	_, m, err := s.JoinChannelByInvite(ctx, hash, member.ID)
	if err != nil {
		t.Fatalf("re-join: %v", err)
	}
	if !m.Banned(time.Now()) || !m.Forever() {
		t.Errorf("re-join cleared the ban: %+v", m)
	}
	if m.JoinPts != before.JoinPts {
		t.Errorf("join_pts moved to %d, want %d", m.JoinPts, before.JoinPts)
	}
}

func TestCreateChannelRejectsPastThePerAccountCap(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551291014")
	store.SetChannelCaps(s, 10000, 1)

	first := mustChannel(t, s, u.ID, "First")
	// One row held, cap 1: the >= boundary rejects here rather than at 2.
	if _, err := s.CreateChannel(ctx, u.ID, "Second", "", false); !errors.Is(err, store.ErrTooManyChannels) {
		t.Fatalf("second create: err = %v, want ErrTooManyChannels", err)
	}

	chans, err := s.ChannelsForUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("channels for user: %v", err)
	}
	if len(chans) != 1 || chans[0].ID != first.ID {
		t.Errorf("rejected create wrote rows: %+v", chans)
	}
}

func TestJoinChannelByInviteRejectsAFullChannel(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291015")
	first := mustUser(t, s, "+15551291016")
	second := mustUser(t, s, "+15551291017")
	ch := mustChannel(t, s, owner.ID, "News") // the creator takes seat 1
	hash, err := s.CreateChannelInvite(ctx, ch.ID, owner.ID)
	if err != nil {
		t.Fatalf("invite: %v", err)
	}
	store.SetChannelCaps(s, 2, 500)

	// Seat 2 of 2 is still free, so this one is admitted: the boundary is >=,
	// not >.
	if _, _, err := s.JoinChannelByInvite(ctx, hash, first.ID); err != nil {
		t.Fatalf("join at the last free seat: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, hash, second.ID); !errors.Is(err, store.ErrChannelFull) {
		t.Fatalf("join past the cap: err = %v, want ErrChannelFull", err)
	}
	if _, ok, err := s.ChannelMemberOf(ctx, ch.ID, second.ID); err != nil || ok {
		t.Errorf("rejected join wrote a row: ok=%v err=%v", ok, err)
	}
}

func TestJoinChannelByInviteRejectsPastThePerAccountCap(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551291018")
	joiner := mustUser(t, s, "+15551291019")
	// Both channels are created before the cap is lowered, so this exercises the
	// join path's check and not the create path's.
	one := mustChannel(t, s, owner.ID, "One")
	two := mustChannel(t, s, owner.ID, "Two")
	hashOne, err := s.CreateChannelInvite(ctx, one.ID, owner.ID)
	if err != nil {
		t.Fatalf("invite one: %v", err)
	}
	hashTwo, err := s.CreateChannelInvite(ctx, two.ID, owner.ID)
	if err != nil {
		t.Fatalf("invite two: %v", err)
	}

	store.SetChannelCaps(s, 10000, 1)
	if _, _, err := s.JoinChannelByInvite(ctx, hashOne, joiner.ID); err != nil {
		t.Fatalf("first join: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, hashTwo, joiner.ID); !errors.Is(err, store.ErrTooManyChannels) {
		t.Fatalf("second join: err = %v, want ErrTooManyChannels", err)
	}
	if _, ok, err := s.ChannelMemberOf(ctx, two.ID, joiner.ID); err != nil || ok {
		t.Errorf("rejected join wrote a row: ok=%v err=%v", ok, err)
	}

	// An account already holding the row is not the cap's business: a re-join of
	// a channel it is in still returns, even though it is at the cap.
	if _, m, err := s.JoinChannelByInvite(ctx, hashOne, joiner.ID); err != nil || m.UserID != joiner.ID {
		t.Errorf("re-join at the cap: member=%+v err=%v", m, err)
	}
}
