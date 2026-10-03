package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestPostChannelMessageAsClassifiesEveryStoredDefaultRight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	creator := mustUser(t, s, "+15551293201")
	member := mustUser(t, s, "+15551293202")
	channel := mustMegagroup(t, s, creator.ID, "Rights map")
	seat(t, s, channel, creator.ID, member.ID, 0)

	// Only send_messages and send_plain govern the current channel text-post
	// path. The remaining stored flags name send actions that this server does
	// not expose to channels; API tests keep channel media and forwards refused.
	cases := []struct {
		right      string
		blocksText bool
	}{
		{right: "send_messages", blocksText: true},
		{right: "send_media"},
		{right: "send_stickers"},
		{right: "send_gifs"},
		{right: "send_games"},
		{right: "send_inline"},
		{right: "embed_links"},
		{right: "send_polls"},
		{right: "change_info"},
		{right: "invite_users"},
		{right: "pin_messages"},
		{right: "manage_topics"},
		{right: "send_photos"},
		{right: "send_videos"},
		{right: "send_roundvideos"},
		{right: "send_audios"},
		{right: "send_voices"},
		{right: "send_docs"},
		{right: "send_plain", blocksText: true},
	}
	var pts int
	for i, tc := range cases {
		t.Run(tc.right, func(t *testing.T) {
			if _, err := conn.Exec(ctx, `UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, channel.ID, []string{tc.right}); err != nil {
				t.Fatalf("set %s: %v", tc.right, err)
			}
			_, gotPts, _, postErr := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "post "+tc.right, int64(93200+i), nil, 0)
			if tc.blocksText {
				if errors.Is(postErr, store.ErrChatWriteForbidden) {
					return
				}
				if postErr != nil {
					t.Errorf("post with %s = %v, want ErrChatWriteForbidden", tc.right, postErr)
					return
				}
				t.Errorf("post with %s succeeded, want ErrChatWriteForbidden", tc.right)
				pts++
				if gotPts != pts {
					t.Errorf("post with %s pts = %d, want %d", tc.right, gotPts, pts)
				}
				return
			}
			if postErr != nil {
				t.Errorf("text post with unrelated %s restriction: %v", tc.right, postErr)
				return
			}
			pts++
			if gotPts != pts {
				t.Errorf("post with %s pts = %d, want %d", tc.right, gotPts, pts)
			}
		})
	}
}

func TestPostChannelMessageAsChecksMediaRestrictionsForFilePosts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	creator := mustUser(t, s, "+15551293501")
	member := mustUser(t, s, "+15551293502")
	channel := mustMegagroup(t, s, creator.ID, "Media restrictions")
	seat(t, s, channel, creator.ID, member.ID, 0)
	file := storedFile(t, s, member.ID)
	cases := []string{"send_media", "send_docs", "send_videos"}

	for i, right := range cases {
		t.Run(right, func(t *testing.T) {
			if _, err := conn.Exec(ctx, `UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, channel.ID, []string{right}); err != nil {
				t.Fatalf("set %s: %v", right, err)
			}
			fileID := file.ID
			if _, _, _, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "attachment", int64(93500+i), &fileID, 0); !errors.Is(err, store.ErrChatWriteForbidden) {
				t.Fatalf("post with file and %s = %v, want ErrChatWriteForbidden", right, err)
			}
			if pts, err := s.ChannelState(ctx, channel.ID); err != nil || pts != 0 {
				t.Fatalf("channel pts after refused %s post = %d, err %v; want 0", right, pts, err)
			}
			events, err := s.ChannelEventsWindow(ctx, channel.ID, 0, 0, 10)
			if err != nil {
				t.Fatalf("events after refused %s post: %v", right, err)
			}
			if len(events) != 0 {
				t.Fatalf("events after refused %s post = %d, want 0", right, len(events))
			}
			messages, err := s.ChannelMessages(ctx, channel.ID, []int64{1})
			if err != nil {
				t.Fatalf("messages after refused %s post: %v", right, err)
			}
			if len(messages) != 0 {
				t.Fatalf("messages after refused %s post = %d, want 0", right, len(messages))
			}
		})
	}
}

func TestPostChannelMessageDedupPrecedesDefaultRestriction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	}()

	creator := mustUser(t, s, "+15551293401")
	member := mustUser(t, s, "+15551293402")
	channel := mustMegagroup(t, s, creator.ID, "Restricted retry")
	seat(t, s, channel, creator.ID, member.ID, 0)

	first, pts, dup, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "original", 93401, nil, 0)
	if err != nil || dup {
		t.Fatalf("initial post: duplicate=%v err=%v", dup, err)
	}
	if _, err = conn.Exec(ctx, `UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, channel.ID, []string{"send_plain"}); err != nil {
		t.Fatalf("set default rights: %v", err)
	}

	retry, retryPts, dup, err := s.PostChannelMessageAs(ctx, channel.ID, member.ID, "original", 93401, nil, 0)
	if err != nil || !dup {
		t.Fatalf("committed retry: duplicate=%v err=%v", dup, err)
	}
	if retry.LocalID != first.LocalID || retryPts != pts {
		t.Fatalf("retry = local_id %d pts %d, want %d/%d", retry.LocalID, retryPts, first.LocalID, pts)
	}
	if _, _, _, err = s.PostChannelMessageAs(ctx, channel.ID, member.ID, "new post", 93402, nil, 0); !errors.Is(err, store.ErrChatWriteForbidden) {
		t.Fatalf("new post after restriction = %v, want ErrChatWriteForbidden", err)
	}
	if got, err := s.ChannelState(ctx, channel.ID); err != nil || got != pts {
		t.Fatalf("channel state after retry and refusal = %d, err %v; want %d", got, err, pts)
	}
}

func waitForBlockedChannelPost(t *testing.T, ctx context.Context, conn *pgx.Conn) error {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var blocked bool
		err := conn.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_stat_activity
				WHERE datname = current_database()
				  AND pid <> pg_backend_pid()
				  AND wait_event_type = 'Lock'
				  AND wait_event = 'advisory'
				  AND query LIKE '%INSERT INTO channel_messages%'
			)
		`).Scan(&blocked)
		if err != nil {
			return fmt.Errorf("inspect blocked channel post: %w", err)
		}
		if blocked {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return errors.New("channel post did not reach the blocked insert")
}

func TestPostChannelMessageKeepsThePermissionReadWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	creator := mustUser(t, s, "+15551293301")
	member := mustUser(t, s, "+15551293302")
	channel := mustMegagroup(t, s, creator.ID, "Permission window")
	seat(t, s, channel, creator.ID, member.ID, 0)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	postDone := make(chan error, 1)
	released := false
	release := func() {
		if released {
			return
		}
		if _, err := conn.Exec(context.Background(), `SELECT pg_advisory_unlock(1032001, 1032002)`); err != nil {
			t.Errorf("release channel post barrier: %v", err)
		}
		released = true
	}
	defer func() {
		release()
		if _, err := conn.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_channel_post_barrier ON channel_messages`); err != nil {
			t.Errorf("drop channel post trigger: %v", err)
		}
		if _, err := conn.Exec(context.Background(), `DROP FUNCTION IF EXISTS test_channel_post_barrier()`); err != nil {
			t.Errorf("drop channel post function: %v", err)
		}
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	if _, err = conn.Exec(ctx, `
		CREATE FUNCTION test_channel_post_barrier() RETURNS trigger AS $body$
		BEGIN
			PERFORM pg_advisory_xact_lock(1032001, 1032002);
			RETURN NEW;
		END;
		$body$ LANGUAGE plpgsql
	`); err != nil {
		t.Fatalf("create channel post barrier function: %v", err)
	}
	if _, err = conn.Exec(ctx, `
		CREATE TRIGGER test_channel_post_barrier
		BEFORE INSERT ON channel_messages
		FOR EACH ROW EXECUTE FUNCTION test_channel_post_barrier()
	`); err != nil {
		t.Fatalf("create channel post barrier trigger: %v", err)
	}
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(1032001, 1032002)`); err != nil {
		t.Fatalf("hold channel post barrier: %v", err)
	}

	postCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	go func() {
		_, _, _, postErr := s.PostChannelMessageAs(postCtx, channel.ID, member.ID, "in flight", 93301, nil, 0)
		postDone <- postErr
	}()
	if err = waitForBlockedChannelPost(t, ctx, conn); err != nil {
		release()
		postErr := <-postDone
		t.Fatalf("wait for post after permission read: %v (post returned %v)", err, postErr)
	}

	// This auto-committed rights write models Save replying after its commit. The
	// post has already read unrestricted defaults and is paused only before INSERT.
	if _, err = conn.Exec(ctx, `UPDATE channels SET default_banned_rights = $2 WHERE id = $1`, channel.ID, []string{"send_plain"}); err != nil {
		release()
		postErr := <-postDone
		t.Fatalf("commit default rights: %v (post returned %v)", err, postErr)
	}
	release()
	if postErr := <-postDone; postErr != nil {
		t.Fatalf("post whose permission read preceded the rights commit: %v", postErr)
	}

	if pts, err := s.ChannelState(ctx, channel.ID); err != nil || pts != 1 {
		t.Fatalf("channel pts after in-flight post = %d, err %v; want 1", pts, err)
	}
	if _, _, _, err = s.PostChannelMessageAs(ctx, channel.ID, member.ID, "after restriction", 93302, nil, 0); !errors.Is(err, store.ErrChatWriteForbidden) {
		t.Fatalf("post whose permission read follows the rights commit = %v, want ErrChatWriteForbidden", err)
	}
	if pts, err := s.ChannelState(ctx, channel.ID); err != nil || pts != 1 {
		t.Fatalf("channel pts after refused post = %d, err %v; want 1", pts, err)
	}
}
