package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestDeleteMessagesConcurrentSelfAndRevoke is the overlap the walk's
// already-deleted check exists to close: C clears its own copy of B's message
// while B's revoke delete is blocked on C's owner lock. The hook commits C's
// self-delete in the window between the walk's fan-out snapshot and its locks,
// so the walk resumes holding a snapshot that still shows C's copy live. The
// walk must skip C rather than bump C's pts a second time or emit a second
// event for a row that is already gone from C's view.
func TestDeleteMessagesConcurrentSelfAndRevoke(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290161")
	b := mustUser(t, s, "+15551290162")
	c := mustUser(t, s, "+15551290163")
	chat := chatWith(t, s, a, b, c)

	// B's message is local 1 for every member; the create service message
	// consumes no local id.
	sender, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: b.ID, Text: "racing", RandomID: 1})
	if sender.LocalID != 1 {
		t.Fatalf("sender local id = %d, want 1", sender.LocalID)
	}

	// The hook performs C's self-delete in the window the walk's snapshot is
	// stale. It fires on B's revoke call (phase 1); the fired guard stops the
	// re-entrant hook hit from the inner self-delete call.
	phase := 0
	fired := false
	var cErr error
	store.SetDeleteWalkHook(s, func() {
		if phase != 1 || fired {
			return
		}
		fired = true
		_, cErr = s.DeleteMessages(ctx, c.ID, []int64{sender.LocalID}, false)
	})
	defer store.SetDeleteWalkHook(s, nil)

	// B's revoke delete: the hook commits C's self-delete between the snapshot
	// and the locks, so the walk resumes with a stale view of C's copy.
	phase = 1
	perOwner, err := s.DeleteMessages(ctx, b.ID, []int64{sender.LocalID}, true)
	if err != nil {
		t.Fatalf("b revoke delete: %v", err)
	}
	if !fired {
		t.Fatal("hook did not fire the competing self-delete")
	}
	if cErr != nil {
		t.Fatalf("competing c self-delete: %v", cErr)
	}

	// The walk deleted a's and b's copies and bumped both; it skipped c, whose
	// copy was already cleared by the competing self-delete, so c is not in the
	// affected set and its pts is not bumped a second time.
	if len(perOwner) != 2 || perOwner[a.ID] != 2 || perOwner[b.ID] != 2 {
		t.Fatalf("perOwner = %+v, want a and b at pts 2", perOwner)
	}
	// C's pts is exactly where its own self-delete left it, with one delete
	// event: no second bump or event from the walk for a row already gone.
	if got := ptsOf(t, s, c.ID); got != 2 {
		t.Errorf("c pts = %d, want 2 (send + self-delete, no walk bump)", got)
	}
	if ev := eventsOf(t, s, c.ID, 0); len(ev) != 2 {
		t.Fatalf("c events = %+v, want exactly the send and the self-delete", ev)
	}
	if ev := eventsOf(t, s, c.ID, 2); len(ev) != 0 {
		t.Errorf("c events after its self-delete = %+v, want none", ev)
	}
	if m, ok := msgOpt(t, s, c.ID, sender.LocalID); !ok || !m.Deleted {
		t.Errorf("c copy not deleted: ok=%v %+v", ok, m)
	}

	// The other two members were deleted for by the walk, each with one event.
	for _, u := range []store.User{a, b} {
		if m, ok := msgOpt(t, s, u.ID, sender.LocalID); !ok || !m.Deleted {
			t.Errorf("owner %d copy not deleted: ok=%v %+v", u.ID, ok, m)
		}
		if got := ptsOf(t, s, u.ID); got != 2 {
			t.Errorf("owner %d pts = %d, want 2", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 0); len(ev) != 2 {
			t.Errorf("owner %d events = %+v, want the send and the delete", u.ID, ev)
		}
	}
}

// TestDeleteMessagesConcurrentSelfDeleteFailsClosed is the fail-closed half of
// the same overlap: a self-delete whose pre-lock snapshot still shows the row
// live, but a competing self-delete commits in the window between that snapshot
// and the write. The write's row count is zero, so the batch must fail closed
// with the uniform error and move no pts — the loser of the race must not bump
// the owner's pts or emit an event for a row that is already gone. A pre-lock
// deleted check would have missed this (the snapshot says live); only the write
// can decide it.
func TestDeleteMessagesConcurrentSelfDeleteFailsClosed(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290171")
	b := mustUser(t, s, "+15551290172")
	c := mustUser(t, s, "+15551290173")
	chat := chatWith(t, s, a, b, c)

	sender, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "racing", RandomID: 1})
	if sender.LocalID != 1 {
		t.Fatalf("sender local id = %d, want 1", sender.LocalID)
	}

	// The hook performs a competing self-delete in the window between this
	// call's snapshot and its write, so the call's write sees a row that is
	// already gone even though its own snapshot read it live. The fired guard
	// stops the re-entrant hook hit from the inner call.
	fired := false
	var cErr error
	store.SetDeleteWalkHook(s, func() {
		if fired {
			return
		}
		fired = true
		_, cErr = s.DeleteMessages(ctx, c.ID, []int64{sender.LocalID}, false)
	})
	defer store.SetDeleteWalkHook(s, nil)

	// The loser of the race: its snapshot says live, but the competing delete
	// committed first, so the write changes zero rows and the batch fails closed.
	if _, err := s.DeleteMessages(ctx, c.ID, []int64{sender.LocalID}, false); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("self-only delete with a stale snapshot: want ErrMessageInvalid, got %v", err)
	}
	if !fired {
		t.Fatal("hook did not fire the competing self-delete")
	}
	if cErr != nil {
		t.Fatalf("competing c self-delete: %v", cErr)
	}
	// The loser moved no pts and emitted no event: c is exactly where the
	// competing (winning) self-delete left it, with one delete event.
	if got := ptsOf(t, s, c.ID); got != 2 {
		t.Errorf("c pts = %d, want 2 (send + the winning self-delete only)", got)
	}
	if ev := eventsOf(t, s, c.ID, 0); len(ev) != 2 {
		t.Errorf("c events = %+v, want the send and the one winning self-delete", ev)
	}
}
