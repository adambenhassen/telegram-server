package store_test

import (
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
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
