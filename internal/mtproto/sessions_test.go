package mtproto

import (
	"sync"
	"testing"
)

func TestSessionRegistryAddRemove(t *testing.T) {
	t.Parallel()
	r := NewSessionRegistry()
	c1, c2 := &Conn{}, &Conn{}

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

func TestSessionRegistryConcurrent(t *testing.T) {
	t.Parallel()
	r := NewSessionRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		c := &Conn{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Add(1, c)
			_ = r.Conns(1)
			r.Remove(1, c)
		}()
	}
	wg.Wait()
	if got := r.Conns(1); len(got) != 0 {
		t.Fatalf("residual conns after concurrent churn: %d", len(got))
	}
}
