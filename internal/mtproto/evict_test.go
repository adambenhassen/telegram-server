package mtproto_test

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// seededKey builds a distinct auth key per seed, so a test can tell two
// registered connections apart by their key id.
func seededKey(seed byte) crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i) + seed
	}
	return raw.WithID()
}

// registered wires a conn to its own transport, binds it to the key and owner,
// and registers it, returning both so the test can assert on the socket.
func registered(r *mtproto.SessionRegistry, userID int64, key crypto.AuthKey) (*mtproto.Conn, *fakeConn) {
	fc := &fakeConn{}
	c := mtproto.NewTestConn(fc, key)
	c.SetOwner(userID)
	r.Add(userID, c)
	return c, fc
}

// TestEvictClosesOnlyTheRevokedKey pins the whole matching rule of the evict
// path: exactly the socket holding the revoked key is closed, and every miss —
// a key id nobody holds, a user with no conns, the zero pair a malformed
// payload would degrade to — closes nothing. Widening any of those into
// "close the user's conns" would let one forged NOTIFY line disconnect a whole
// account, which is why each miss is asserted rather than assumed.
func TestEvictClosesOnlyTheRevokedKey(t *testing.T) {
	t.Parallel()
	revoked, kept, other := seededKey(1), seededKey(2), seededKey(3)
	reg := mtproto.NewSessionRegistry()
	u := api.NewUpdater(nil, reg, slog.New(slog.DiscardHandler), nil)
	ctx := context.Background()

	const victim, bystander int64 = 7, 9
	_, revokedSock := registered(reg, victim, revoked)
	_, keptSock := registered(reg, victim, kept)
	// Another user holding the same key id: eviction is addressed to one user, so
	// this socket must survive a revocation aimed at the victim.
	_, otherUserSameKey := registered(reg, bystander, revoked)
	_, otherUserSock := registered(reg, bystander, other)

	survivors := map[string]*fakeConn{
		"the victim's other session":                   keptSock,
		"another user's session on the revoked key id": otherUserSameKey,
		"another user's unrelated session":             otherUserSock,
	}
	assertSurvivors := func(after string) {
		t.Helper()
		for what, fc := range survivors {
			if fc.closed() {
				t.Fatalf("%s was closed by %s", what, after)
			}
		}
	}

	// Misses first, so a later close cannot be mistaken for one of these.
	u.Evict(ctx, victim, kept.IntID()+1) // a key id nobody holds
	u.Evict(ctx, 12345, revoked.IntID()) // a user with no registered conns
	u.Evict(ctx, 0, 0)                   // what an unparsable payload would degrade to
	if revokedSock.closed() {
		t.Fatal("a non-matching evict closed the revoked-key socket")
	}
	assertSurvivors("a non-matching evict")

	u.Evict(ctx, victim, revoked.IntID())
	if !revokedSock.closed() {
		t.Fatal("the socket holding the revoked key was not closed")
	}
	assertSurvivors("the matching evict")

	// Deregistering is the serve goroutine's job, reached through the Recv the
	// close unblocks. Evict must not touch stored state itself.
	if users := reg.Users(); !slices.Contains(users, victim) || !slices.Contains(users, bystander) {
		t.Fatalf("registry users = %v, want both %d and %d still present", users, victim, bystander)
	}
	if got := len(reg.Conns(victim)); got != 2 {
		t.Fatalf("victim conns = %d, want 2: evict must not mutate the registry", got)
	}
}

// TestAuthKeyIDTracksSetKeyUnderPush is the -race gate on the lock-free key id:
// eviction reads it off the listener goroutine while the conn's own goroutines
// rebind the key and push, and it must end up reporting the last key set. If it
// were guarded by writeMu instead, one blackholed socket would stall every
// user's delivery for a whole write timeout.
func TestAuthKeyIDTracksSetKeyUnderPush(t *testing.T) {
	t.Parallel()
	first, last := seededKey(4), seededKey(5)
	fc := &fakeConn{}
	c := mtproto.NewTestConn(fc, first)
	c.SetOwner(7)
	ctx := context.Background()

	if got := c.AuthKeyID(); got != first.IntID() {
		t.Fatalf("AuthKeyID = %d, want %d after the initial bind", got, first.IntID())
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			c.SetKey(first)
			c.SetKey(last)
		}
	})
	wg.Go(func() {
		for range 100 {
			if _, err := c.PushTo(ctx, 7, &mt.Pong{PingID: 1}, 0); err != nil {
				t.Errorf("push: %v", err)
				return
			}
		}
	})
	wg.Go(func() {
		for range 100 {
			_ = c.AuthKeyID()
		}
	})
	wg.Wait()

	if got := c.AuthKeyID(); got != last.IntID() {
		t.Fatalf("AuthKeyID = %d, want %d: the last key set must be the one evict matches", got, last.IntID())
	}
}
