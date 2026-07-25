package api_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// storeAndEvicts opens a store and a raw connection listening on tg_evict over
// the same test database. The raw listener is what makes the ordering
// observable: it sees the moment a revocation is published, not the moment some
// replica reacts to one, so an assertion never depends on eviction latency.
func storeAndEvicts(ctx context.Context, t *testing.T) (*store.Store, *pgx.Conn) {
	t.Helper()
	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})
	l, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("listener connect: %v", err)
	}
	t.Cleanup(func() { _ = l.Close(context.Background()) }) //nolint:errcheck // best-effort close
	if _, err := l.Exec(ctx, "LISTEN "+store.ChannelEvict); err != nil {
		t.Fatalf("listen: %v", err)
	}
	return s, l
}

// noEvictYet fails if anything has been published on tg_evict. A NOTIFY reaches
// a listening connection as soon as it commits, so an announcement the handler
// emitted before returning is already waiting here.
func noEvictYet(ctx context.Context, t *testing.T, l *pgx.Conn, what string) {
	t.Helper()
	qctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	n, err := l.WaitForNotification(qctx)
	if err == nil {
		t.Fatalf("%s: %q", what, n.Payload)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait for quiet: %v", err)
	}
}

// nextEvict returns the next tg_evict payload, failing if none arrives.
func nextEvict(ctx context.Context, t *testing.T, l *pgx.Conn, what string) string {
	t.Helper()
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n, err := l.WaitForNotification(wctx)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	return n.Payload
}

// boundKey saves a fresh auth key derived from seed and binds it to userID.
func boundKey(ctx context.Context, t *testing.T, s *store.Store, userID int64, seed byte) crypto.AuthKey {
	t.Helper()
	k := savedKey(ctx, t, s, seed)
	if err := s.BindAuthKeyUser(ctx, k.IntID(), userID); err != nil {
		t.Fatalf("bind key %d: %v", k.IntID(), err)
	}
	return k
}

// savedKey saves a fresh auth key derived from seed, leaving it unbound.
func savedKey(ctx context.Context, t *testing.T, s *store.Store, seed byte) crypto.AuthKey {
	t.Helper()
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i) + seed
	}
	k := raw.WithID()
	if err := s.SaveAuthKey(ctx, k.IntID(), k.Value[:]); err != nil {
		t.Fatalf("save key: %v", err)
	}
	return k
}

// TestRevocationPublishesEvictAroundTheReply pins when each revocation announces
// itself, which no timing-based test can pin: the e2e self-revocation case races
// a Postgres round trip against a local socket write and the write all but
// always wins, so it passes whether the announcement is emitted inside the
// handler or after the reply. Here the handler hands the announcement back
// unrun, so "before the reply" and "after the reply" are two distinguishable
// states rather than two orderings of a race.
//
// Both directions matter. A revocation aimed at another session must publish
// before the caller can observe success, or an update NOTIFY triggered by that
// caller can overtake the evict and reach the socket being revoked. The caller's
// own session must publish after, or the evict closes the socket the reply is
// still going out on and a successful logOut surfaces as a transport error.
func TestRevocationPublishesEvictAroundTheReply(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, evicts := storeAndEvicts(ctx, t)

	user, err := s.CreateUser(ctx, "+15551296001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	caller := boundKey(ctx, t, s, user.ID, 1)
	target := boundKey(ctx, t, s, user.ID, 2)

	// Another session of the caller's own account: published inside the handler.
	res, afterReply, err := api.ResetAuthorizationForTest(s, user.ID, caller.ID, target.IntID())
	if err != nil {
		t.Fatalf("reset another session: %v", err)
	}
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("reset result = %T, want *tg.BoolTrue", res)
	}
	if afterReply != nil {
		t.Fatal("resetting another session deferred its evict past the reply")
	}
	if got, want := nextEvict(ctx, t, evicts, "reset of another session published no evict"), store.EvictPayload(user.ID, target.IntID()); got != want {
		t.Fatalf("evict payload = %q, want %q", got, want)
	}

	// The caller's own session: nothing published until the reply is out.
	res, afterReply, err = api.ResetAuthorizationForTest(s, user.ID, caller.ID, caller.IntID())
	if err != nil {
		t.Fatalf("reset own session: %v", err)
	}
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("self reset result = %T, want *tg.BoolTrue", res)
	}
	if afterReply == nil {
		t.Fatal("resetting the caller's own session must defer its evict past the reply")
	}
	noEvictYet(ctx, t, evicts, "self reset published its evict before the reply")
	afterReply()
	if got, want := nextEvict(ctx, t, evicts, "self reset published no evict"), store.EvictPayload(user.ID, caller.IntID()); got != want {
		t.Fatalf("self reset evict payload = %q, want %q", got, want)
	}

	// logOut always revokes the key it arrived on, so it is always deferred.
	out := boundKey(ctx, t, s, user.ID, 3)
	res, afterReply, err = api.LogOutForTest(s, out.ID)
	if err != nil {
		t.Fatalf("logout: %v", err)
	}
	if _, ok := res.(*tg.AuthLoggedOut); !ok {
		t.Fatalf("logout result = %T, want *tg.AuthLoggedOut", res)
	}
	if afterReply == nil {
		t.Fatal("logOut must defer its evict past the reply")
	}
	noEvictYet(ctx, t, evicts, "logOut published its evict before the reply")
	afterReply()
	if got, want := nextEvict(ctx, t, evicts, "logOut published no evict"), store.EvictPayload(user.ID, out.IntID()); got != want {
		t.Fatalf("logout evict payload = %q, want %q", got, want)
	}

	// An unbound key is registered under nobody: the logOut still succeeds and
	// announces nothing, since an evict naming user 0 is a signal no replica can
	// act on and every replica would have to parse.
	unbound := savedKey(ctx, t, s, 4)
	res, afterReply, err = api.LogOutForTest(s, unbound.ID)
	if err != nil {
		t.Fatalf("logout on an unbound key: %v", err)
	}
	if _, ok := res.(*tg.AuthLoggedOut); !ok {
		t.Fatalf("unbound logout result = %T, want *tg.AuthLoggedOut", res)
	}
	if afterReply != nil {
		t.Fatal("logOut on an unbound key must announce nothing")
	}
	noEvictYet(ctx, t, evicts, "unbound logOut published an evict")

	// Every key the handlers reported revoked is really gone.
	for _, k := range []crypto.AuthKey{caller, target, out, unbound} {
		if _, ok, err := s.AuthKeyByID(ctx, k.IntID()); err != nil || ok {
			t.Fatalf("auth key %d still present after revocation: ok=%v err=%v", k.IntID(), ok, err)
		}
	}
}
