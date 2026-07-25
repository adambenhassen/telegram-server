package mtproto_test

import (
	"sync"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestSessionRegistryAddRemove(t *testing.T) {
	t.Parallel()
	r := mtproto.NewSessionRegistry()
	c1, c2 := &mtproto.Conn{}, &mtproto.Conn{}

	r.Add(7, c1)
	r.Add(7, c2)
	if got := r.Conns(7); len(got) != 2 {
		t.Fatalf("Conns(7) = %d, want 2", len(got))
	}
	if got := r.Conns(9); len(got) != 0 {
		t.Fatalf("Conns(9) = %d, want 0", len(got))
	}

	r.Remove(7, c1)
	got := r.Conns(7)
	if len(got) != 1 || got[0] != c2 {
		t.Fatalf("after remove, Conns(7) = %+v, want [c2]", got)
	}

	r.Remove(7, c2)
	if got := r.Conns(7); len(got) != 0 {
		t.Fatalf("after remove all, Conns(7) = %d, want 0", len(got))
	}
}

// TestSessionRegistryCap covers the per-user connection cap: past it a new
// connection is refused and no live one is evicted, and a slot freed by a
// departing connection is reusable.
func TestSessionRegistryCap(t *testing.T) {
	t.Parallel()
	r := mtproto.NewSessionRegistry()
	conns := make([]*mtproto.Conn, mtproto.MaxUserConns)
	for i := range conns {
		conns[i] = &mtproto.Conn{}
		if !r.Add(3, conns[i]) {
			t.Fatalf("Add %d of %d refused under the cap", i+1, mtproto.MaxUserConns)
		}
	}

	extra := &mtproto.Conn{}
	if r.Add(3, extra) {
		t.Fatal("Add past the cap must be refused")
	}
	got := r.Conns(3)
	if len(got) != mtproto.MaxUserConns {
		t.Fatalf("Conns(3) = %d, want %d", len(got), mtproto.MaxUserConns)
	}
	if got[0] != conns[0] {
		t.Fatal("the oldest connection was evicted; the cap must refuse, never evict")
	}
	// A different user is unaffected by another's cap.
	if !r.Add(4, extra) {
		t.Fatal("another user's Add refused by user 3's cap")
	}

	r.Remove(3, conns[0])
	if !r.Add(3, extra) {
		t.Fatal("Add refused after a connection freed a slot")
	}
}

func TestSessionRegistryConcurrent(t *testing.T) {
	t.Parallel()
	r := mtproto.NewSessionRegistry()
	var wg sync.WaitGroup
	for range 50 {
		c := &mtproto.Conn{}
		wg.Go(func() {
			r.Add(1, c)
			_ = r.Conns(1)
			r.Remove(1, c)
		})
	}
	wg.Wait()
	if got := r.Conns(1); len(got) != 0 {
		t.Fatalf("residual conns after concurrent churn: %d", len(got))
	}
}
