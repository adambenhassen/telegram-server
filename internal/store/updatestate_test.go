package store_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestUpdateStateEventsSince(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15551239001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := s.EnsureUpdateState(ctx, u.ID); err != nil {
		t.Fatalf("ensure state: %v", err)
	}

	st, err := s.State(ctx, u.ID)
	if err != nil {
		t.Fatalf("state: %v", err)
	}
	if st.Pts != 0 {
		t.Fatalf("fresh pts = %d, want 0", st.Pts)
	}

	// Two events at pts 1 and 2 (see InsertTestEvent — bumps pts then logs).
	if err := store.InsertTestEvent(ctx, s, u.ID, store.EventNewMessage, 1); err != nil {
		t.Fatalf("event 1: %v", err)
	}
	if err := store.InsertTestEvent(ctx, s, u.ID, store.EventEdit, 1); err != nil {
		t.Fatalf("event 2: %v", err)
	}

	all, err := s.EventsSince(ctx, u.ID, 0)
	if err != nil {
		t.Fatalf("events since 0: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("events since 0 = %d, want 2", len(all))
	}
	if all[0].Pts != 1 || all[1].Pts != 2 {
		t.Fatalf("event pts ordering = %d,%d, want 1,2", all[0].Pts, all[1].Pts)
	}
	if all[0].Type != store.EventNewMessage || all[1].Type != store.EventEdit {
		t.Fatalf("event types = %d,%d", all[0].Type, all[1].Type)
	}

	tail, err := s.EventsSince(ctx, u.ID, 1)
	if err != nil {
		t.Fatalf("events since 1: %v", err)
	}
	if len(tail) != 1 || tail[0].Pts != 2 {
		t.Fatalf("events since 1 = %+v, want single pts 2", tail)
	}

	st, err = s.State(ctx, u.ID)
	if err != nil {
		t.Fatalf("state after events: %v", err)
	}
	if st.Pts != 2 {
		t.Fatalf("pts after two events = %d, want 2", st.Pts)
	}
}

func TestStateCountsUnreadWithoutUpdateStateRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	creator := mustUser(t, s, "+15551239011")
	reader := mustUser(t, s, "+15551239012")
	channel := mustChannel(t, s, creator.ID, "missing update state")
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join channel: %v", err)
	}
	post(t, s, channel.ID, creator.ID, "unread post", 901101)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(func() {
		if closeErr := conn.Close(ctx); closeErr != nil {
			t.Errorf("close test database connection: %v", closeErr)
		}
	})
	if _, err = conn.Exec(ctx, `DELETE FROM update_state WHERE user_id = $1`, reader.ID); err != nil {
		t.Fatalf("remove update state row: %v", err)
	}

	state, err := s.State(ctx, reader.ID)
	if err != nil {
		t.Fatalf("read state without update_state row: %v", err)
	}
	if state.Pts != 0 || state.Qts != 0 || state.Seq != 0 || state.Date != 0 || state.UnreadCount != 1 {
		t.Fatalf("state without update_state row = %+v, want zero update state and unread 1", state)
	}
}
