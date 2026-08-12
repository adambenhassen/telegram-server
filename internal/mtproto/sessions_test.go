package mtproto_test

import (
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"testing"
)

func TestSessionRegistry_TotalConns(t *testing.T) {
	r := mtproto.NewSessionRegistry()

	if got := r.TotalConns(); got != 0 {
		t.Errorf("empty registry: expected 0, got %d", got)
	}

	c1 := &mtproto.Conn{}
	c2 := &mtproto.Conn{}
	c3 := &mtproto.Conn{}

	// One user with one conn.
	r.Add(1, c1)
	if got := r.TotalConns(); got != 1 {
		t.Errorf("one conn: expected 1, got %d", got)
	}

	// Two users with one conn each.
	r.Add(2, c2)
	if got := r.TotalConns(); got != 2 {
		t.Errorf("two conns: expected 2, got %d", got)
	}

	// One user with two conns.
	r.Add(1, c3)
	if got := r.TotalConns(); got != 3 {
		t.Errorf("three conns: expected 3, got %d", got)
	}

	// Remove one conn.
	r.Remove(1, c1)
	if got := r.TotalConns(); got != 2 {
		t.Errorf("after remove: expected 2, got %d", got)
	}

	// Remove all conns for user 1.
	r.Remove(1, c3)
	if got := r.TotalConns(); got != 1 {
		t.Errorf("after second remove: expected 1, got %d", got)
	}

	// Remove last conn.
	r.Remove(2, c2)
	if got := r.TotalConns(); got != 0 {
		t.Errorf("empty: expected 0, got %d", got)
	}
}

func TestSessionRegistry_TotalSessions(t *testing.T) {
	r := mtproto.NewSessionRegistry()

	if got := r.TotalSessions(); got != 0 {
		t.Errorf("empty registry: expected 0, got %d", got)
	}

	c1 := &mtproto.Conn{}
	c2 := &mtproto.Conn{}
	c3 := &mtproto.Conn{}

	// One user with one conn = one session.
	r.Add(1, c1)
	if got := r.TotalSessions(); got != 1 {
		t.Errorf("one session: expected 1, got %d", got)
	}

	// Same user with two conns = still one session.
	r.Add(1, c2)
	if got := r.TotalSessions(); got != 1 {
		t.Errorf("same user, two conns: expected 1, got %d", got)
	}

	// Two users = two sessions.
	r.Add(2, c3)
	if got := r.TotalSessions(); got != 2 {
		t.Errorf("two sessions: expected 2, got %d", got)
	}

	// Remove one user's conns.
	r.Remove(1, c1)
	r.Remove(1, c2)
	if got := r.TotalSessions(); got != 1 {
		t.Errorf("after removing user 1: expected 1, got %d", got)
	}

	// Remove last user.
	r.Remove(2, c3)
	if got := r.TotalSessions(); got != 0 {
		t.Errorf("empty: expected 0, got %d", got)
	}
}
