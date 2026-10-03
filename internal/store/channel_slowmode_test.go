package store_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func openSlowModeTestDB(t *testing.T, ctx context.Context) (*store.Store, *pgx.Conn) {
	t.Helper()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s, conn
}

func setSlowModeTestState(t *testing.T, ctx context.Context, conn *pgx.Conn, channelID int64, seconds int16) {
	t.Helper()
	if _, err := conn.Exec(ctx, `UPDATE channels SET slowmode_seconds = $2 WHERE id = $1`, channelID, seconds); err != nil {
		t.Fatalf("set slow mode: %v", err)
	}
}

func setLastPostTestState(t *testing.T, ctx context.Context, conn *pgx.Conn, channelID, userID int64, age time.Duration) {
	t.Helper()
	if _, err := conn.Exec(ctx, `
		WITH updated_participant AS (
			UPDATE channel_participants AS participant
			SET last_post_at = clock_timestamp() - $3::interval
			WHERE participant.channel_id = $1 AND participant.user_id = $2
			RETURNING participant.channel_id, participant.user_id, participant.last_post_at
		)
		INSERT INTO channel_post_markers (channel_id, user_id, last_post_at)
		SELECT updated_participant.channel_id, updated_participant.user_id, updated_participant.last_post_at
		FROM updated_participant
		ON CONFLICT (channel_id, user_id) DO UPDATE
		SET last_post_at = EXCLUDED.last_post_at
	`, channelID, userID, age.String()); err != nil {
		t.Fatalf("set slow-mode post state: %v", err)
	}
}

func channelLastPostTestState(t *testing.T, ctx context.Context, conn *pgx.Conn, channelID, userID int64) pgtype.Timestamptz {
	t.Helper()
	var lastPostAt pgtype.Timestamptz
	if err := conn.QueryRow(ctx, `
		SELECT last_post_at FROM channel_participants WHERE channel_id = $1 AND user_id = $2
	`, channelID, userID).Scan(&lastPostAt); err != nil {
		t.Fatalf("read last-post state: %v", err)
	}
	return lastPostAt
}

func TestPostChannelMessageAsSlowModeWaitsAfterCommittedPostAndDedupsRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, conn := openSlowModeTestDB(t, ctx)
	creator := mustUser(t, s, "+15551269001")
	member := mustUser(t, s, "+15551269002")
	channel := mustMegagroup(t, s, creator.ID, "slow mode")
	seat(t, s, channel, creator.ID, member.ID, 0)

	// A post made with slow mode disabled still establishes the next-send marker.
	first, firstPts, dup, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "first", 69001, nil, 0)
	if err != nil || dup || firstPts != 2 {
		t.Fatalf("first post = %+v pts=%d duplicate=%v err=%v", first, firstPts, dup, err)
	}
	second, secondPts, dup, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "second", 69004, nil, 0)
	if err != nil || dup || secondPts != 3 || second.Message != "second" {
		t.Fatalf("second post with slow mode disabled = %+v pts=%d duplicate=%v err=%v", second, secondPts, dup, err)
	}
	markerBefore := channelLastPostTestState(t, ctx, conn, channel.ID, member.ID)
	if !markerBefore.Valid {
		t.Fatal("accepted post did not establish last_post_at while slow mode was disabled")
	}

	setSlowModeTestState(t, ctx, conn, channel.ID, 10)
	_, _, duplicate, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "distinct", 69002, nil, 0)
	if err == nil || duplicate || !strings.HasPrefix(err.Error(), "SLOWMODE_WAIT_") {
		t.Fatalf("distinct post error = %v, want SLOWMODE_WAIT_<seconds>", err)
	}
	markerAfter := channelLastPostTestState(t, ctx, conn, channel.ID, member.ID)
	if !markerAfter.Valid || !markerAfter.Time.Equal(markerBefore.Time) {
		t.Fatalf("refused post changed last_post_at from %v to %v", markerBefore, markerAfter)
	}
	if pts, err := s.ChannelState(ctx, channel.ID); err != nil || pts != 3 {
		t.Fatalf("state after refusal = %d err=%v, want pts 3", pts, err)
	}
	events, err := s.ChannelEventsWindow(ctx, channel.ID, 0, 3, 10)
	if err != nil || len(events) != 3 || events[2].LocalID != second.LocalID {
		t.Fatalf("events after refusal = %+v err=%v, want creation and two disabled-mode posts", events, err)
	}

	retry, retryPts, dup, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "retry", 69001, nil, 0)
	if err != nil || !dup {
		t.Fatalf("retry = %+v pts=%d duplicate=%v err=%v", retry, retryPts, dup, err)
	}
	if retry.LocalID != first.LocalID || retry.Message != "first" || retryPts != firstPts {
		t.Fatalf("retry = %+v pts=%d, want original %+v pts=%d", retry, retryPts, first, firstPts)
	}
	markerAfterRetry := channelLastPostTestState(t, ctx, conn, channel.ID, member.ID)
	if !markerAfterRetry.Valid || !markerAfterRetry.Time.Equal(markerBefore.Time) {
		t.Fatalf("retry changed last_post_at from %v to %v", markerBefore, markerAfterRetry)
	}

	setLastPostTestState(t, ctx, conn, channel.ID, member.ID, 11*time.Second)
	afterInterval, afterIntervalPts, dup, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "after interval", 69003, nil, 0)
	if err != nil || dup || afterIntervalPts != 4 || afterInterval.Message != "after interval" {
		t.Fatalf("post after interval = %+v pts=%d duplicate=%v err=%v", afterInterval, afterIntervalPts, dup, err)
	}
}

func TestPostChannelMessageAsSlowModeSurvivesLeaveAndRejoin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, conn := openSlowModeTestDB(t, ctx)
	creator := mustUser(t, s, "+15551269041")
	member := mustUser(t, s, "+15551269042")
	channel := mustMegagroup(t, s, creator.ID, "slow mode rejoin")
	if err := s.EditChannelUsername(ctx, channel.ID, creator.ID, "slowmoderejoin"); err != nil {
		t.Fatalf("make channel public: %v", err)
	}
	if _, _, err := s.JoinChannelByUsername(ctx, channel.ID, member.ID); err != nil {
		t.Fatalf("join channel: %v", err)
	}
	setSlowModeTestState(t, ctx, conn, channel.ID, 10)
	first := postAs(t, s, channel.ID, member.ID, "first", 69501)

	left, err := s.LeaveChannel(ctx, channel.ID, member.ID)
	if err != nil || !left {
		t.Fatalf("leave channel = %v, err=%v", left, err)
	}
	if _, _, err := s.JoinChannelByUsername(ctx, channel.ID, member.ID); err != nil {
		t.Fatalf("rejoin channel: %v", err)
	}
	if marker := channelLastPostTestState(t, ctx, conn, channel.ID, member.ID); marker.Valid {
		t.Fatalf("new participant row retained last_post_at %v after rejoin", marker)
	}

	_, _, duplicate, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "distinct", 69502, nil, 0)
	if err == nil || duplicate || !strings.HasPrefix(err.Error(), "SLOWMODE_WAIT_") {
		t.Fatalf("post after rejoin = duplicate %v, err=%v; want SLOWMODE_WAIT_<seconds>", duplicate, err)
	}
	if pts, err := s.ChannelState(ctx, channel.ID); err != nil || pts != 2 {
		t.Fatalf("state after refused post = %d, err=%v; want pts 2", pts, err)
	}
	events, err := s.ChannelEventsWindow(ctx, channel.ID, 0, 2, 10)
	if err != nil || len(events) != 2 || events[1].LocalID != first.LocalID {
		t.Fatalf("events after refused post = %+v, err=%v; want creation and the first post", events, err)
	}
}

func TestPostChannelMessageAsSlowModeSerializesDistinctConcurrentPosts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, conn := openSlowModeTestDB(t, ctx)
	creator := mustUser(t, s, "+15551269011")
	member := mustUser(t, s, "+15551269012")
	channel := mustMegagroup(t, s, creator.ID, "concurrent slow mode")
	seat(t, s, channel, creator.ID, member.ID, 0)
	setSlowModeTestState(t, ctx, conn, channel.ID, 10)

	const posters = 8
	start := make(chan struct{})
	results := make([]error, posters)
	var wg sync.WaitGroup
	for i := range posters {
		wg.Go(func() {
			<-start
			_, _, _, results[i] = s.PostChannelMessageAs(ctx, channel.ID, member.ID, "concurrent", int64(69100+i), nil, 0)
		})
	}
	close(start)
	wg.Wait()

	succeeded, refused := 0, 0
	for i, err := range results {
		switch {
		case err == nil:
			succeeded++
		case strings.HasPrefix(err.Error(), "SLOWMODE_WAIT_"):
			refused++
		default:
			t.Errorf("post %d error = %v, want success or SLOWMODE_WAIT", i, err)
		}
	}
	if succeeded != 1 || refused != posters-1 {
		t.Fatalf("concurrent outcomes = %d succeeded, %d refused; want 1 and %d", succeeded, refused, posters-1)
	}
	if pts, err := s.ChannelState(ctx, channel.ID); err != nil || pts != 2 {
		t.Fatalf("state after concurrent posts = %d err=%v, want pts 2", pts, err)
	}
	events, err := s.ChannelEventsWindow(ctx, channel.ID, 0, 2, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("events after concurrent posts = %+v err=%v, want creation and one post event", events, err)
	}
	if marker := channelLastPostTestState(t, ctx, conn, channel.ID, member.ID); !marker.Valid {
		t.Fatal("successful concurrent post did not establish last_post_at")
	}
}

func TestPostChannelMessageAsSlowModeExemptsAdminsAndBroadcasts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, conn := openSlowModeTestDB(t, ctx)
	creator := mustUser(t, s, "+15551269021")
	admin := mustUser(t, s, "+15551269022")
	member := mustUser(t, s, "+15551269023")
	megagroup := mustMegagroup(t, s, creator.ID, "slow mode exemptions")
	seat(t, s, megagroup, creator.ID, admin.ID, 1)
	seat(t, s, megagroup, creator.ID, member.ID, 0)
	setSlowModeTestState(t, ctx, conn, megagroup.ID, 10)
	setLastPostTestState(t, ctx, conn, megagroup.ID, creator.ID, 0)
	setLastPostTestState(t, ctx, conn, megagroup.ID, admin.ID, 0)
	setLastPostTestState(t, ctx, conn, megagroup.ID, member.ID, 0)
	if _, _, _, err := s.PostChannelMessageAs(ctx, megagroup.ID, member.ID, "member", 69290, nil, 0); err == nil || !strings.HasPrefix(err.Error(), "SLOWMODE_WAIT_") {
		t.Fatalf("ordinary member post = %v, want SLOWMODE_WAIT_<seconds>", err)
	}

	for i, userID := range []int64{creator.ID, admin.ID} {
		for j := range 2 {
			if _, _, _, err := s.PostChannelMessageAs(ctx, megagroup.ID, userID, "exempt", int64(69200+i*10+j), nil, 0); err != nil {
				t.Fatalf("exempt role %d post %d: %v", userID, j, err)
			}
		}
	}

	broadcast, err := s.CreateChannel(ctx, creator.ID, "broadcast slow mode", "", false)
	if err != nil {
		t.Fatalf("create broadcast: %v", err)
	}
	seat(t, s, broadcast, creator.ID, admin.ID, 1)
	seat(t, s, broadcast, creator.ID, member.ID, 0)
	setSlowModeTestState(t, ctx, conn, broadcast.ID, 10)
	setLastPostTestState(t, ctx, conn, broadcast.ID, admin.ID, 0)
	for j := range 2 {
		if _, _, _, err := s.PostChannelMessageAs(ctx, broadcast.ID, admin.ID, "broadcast", int64(69300+j), nil, 0); err != nil {
			t.Fatalf("broadcast admin post %d: %v", j, err)
		}
	}
	if _, _, _, err := s.PostChannelMessageAs(ctx, broadcast.ID, member.ID, "forbidden", 69302, nil, 0); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("broadcast member post = %v, want ErrNotMember", err)
	}
}

func TestPostChannelMessageAsAuthorizationPrecedesSlowModeWait(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, conn := openSlowModeTestDB(t, ctx)
	creator := mustUser(t, s, "+15551269031")
	banned := mustUser(t, s, "+15551269032")
	restricted := mustUser(t, s, "+15551269033")
	eligible := mustUser(t, s, "+15551269034")
	bannedChannel := mustMegagroup(t, s, creator.ID, "banned slow mode")
	restrictedChannel := mustMegagroup(t, s, creator.ID, "restricted slow mode")
	seat(t, s, bannedChannel, creator.ID, banned.ID, 0)
	seat(t, s, bannedChannel, creator.ID, eligible.ID, 0)
	seat(t, s, restrictedChannel, creator.ID, restricted.ID, 0)
	setSlowModeTestState(t, ctx, conn, bannedChannel.ID, 10)
	setSlowModeTestState(t, ctx, conn, restrictedChannel.ID, 10)
	setLastPostTestState(t, ctx, conn, bannedChannel.ID, banned.ID, 0)
	setLastPostTestState(t, ctx, conn, bannedChannel.ID, eligible.ID, 0)
	setLastPostTestState(t, ctx, conn, restrictedChannel.ID, restricted.ID, 0)
	if _, _, _, err := s.PostChannelMessageAs(ctx, bannedChannel.ID, eligible.ID, "eligible", 69400, nil, 0); err == nil || !strings.HasPrefix(err.Error(), "SLOWMODE_WAIT_") {
		t.Fatalf("eligible post = %v, want SLOWMODE_WAIT_<seconds>", err)
	}

	until := time.Now().Add(time.Hour)
	if err := store.SetChannelBan(ctx, s, bannedChannel.ID, banned.ID, &until); err != nil {
		t.Fatalf("ban member: %v", err)
	}
	if _, err := conn.Exec(ctx, `UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, restrictedChannel.ID, []string{"send_plain"}); err != nil {
		t.Fatalf("set default restriction: %v", err)
	}

	if _, _, _, err := s.PostChannelMessageAs(ctx, bannedChannel.ID, banned.ID, "banned", 69401, nil, 0); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("banned post = %v, want ErrNotMember before slow-mode wait", err)
	}
	if _, _, _, err := s.PostChannelMessageAs(ctx, restrictedChannel.ID, restricted.ID, "restricted", 69402, nil, 0); !errors.Is(err, store.ErrChatWriteForbidden) {
		t.Fatalf("restricted post = %v, want ErrChatWriteForbidden before slow-mode wait", err)
	}
}
