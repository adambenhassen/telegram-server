package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestChannelsIDRequiresExplicitValue proves the column default is gone: an
// insert that omits id must fail, and one that supplies an id must succeed.
// Without this, a path that forgets to allocate an id silently mints a dense
// sequential one from channels_id_seq — exactly the disclosure MAIN-246 exists
// to stop.
func TestChannelsIDRequiresExplicitValue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := open(t)
	conn := store.StorePool(s)
	u := mustUser(t, s, "+15551292005")

	var hasDefault bool
	if err := conn.QueryRow(ctx, `
		SELECT column_default IS NOT NULL
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'channels' AND column_name = 'id'`).Scan(&hasDefault); err != nil {
		t.Fatalf("column default lookup: %v", err)
	}
	if hasDefault {
		t.Fatal("channels.id still has a column default")
	}

	if _, err := conn.Exec(ctx, `
		INSERT INTO channels (title, about, creator_id, megagroup)
		VALUES ('No id', '', $1, false)`, u.ID); err == nil {
		t.Fatal("insert omitting id succeeded, want an error")
	}

	const explicitID = store.ChannelIDMin + 9001
	if _, err := conn.Exec(ctx, `
		INSERT INTO channels (id, title, about, creator_id, megagroup)
		VALUES ($1, 'With id', '', $2, false)`, explicitID, u.ID); err != nil {
		t.Fatalf("insert with explicit id: %v", err)
	}
}

// TestCreateChannelDrawsSparseIDs is the anti-enumeration property itself: the
// ids a discovery surface hands out must not let a caller read the private
// channels off the gaps between them. Sequential ids fail all three assertions
// below; a uniform draw over the documented range fails each with probability
// far under any realistic flake budget (20 draws land in increasing order with
// probability 1/20!, and two draws land adjacent with probability 2/10^12).
func TestCreateChannelDrawsSparseIDs(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551292001")

	const created = 20
	ids := make([]int64, created)
	seen := make(map[int64]bool, created)
	for i := range ids {
		ch, err := s.CreateChannel(ctx, u.ID, "News", "", false)
		if err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
		if ch.ID < store.ChannelIDMin || ch.ID > store.ChannelIDMax {
			t.Fatalf("id %d outside the documented range [%d, %d]", ch.ID, store.ChannelIDMin, store.ChannelIDMax)
		}
		if seen[ch.ID] {
			t.Fatalf("id %d drawn twice", ch.ID)
		}
		seen[ch.ID] = true
		ids[i] = ch.ID
	}

	// Two consecutive creations are not consecutive integers. The distance is
	// taken absolute rather than as previous+1: a descending counter is just as
	// much a creation-order oracle as an ascending one, and it would satisfy the
	// monotonicity check below.
	for i := 1; i < len(ids); i++ {
		if diff := ids[i] - ids[i-1]; diff == 1 || diff == -1 {
			t.Errorf("creations %d and %d got adjacent ids %d, %d", i-1, i, ids[i-1], ids[i])
		}
	}

	// The sequence as a whole does not track creation order.
	ascending := true
	for i := 1; i < len(ids); i++ {
		if ids[i] < ids[i-1] {
			ascending = false
			break
		}
	}
	if ascending {
		t.Errorf("ids rose monotonically across %d creations: %v", created, ids)
	}
}

// TestCreateChannelRetriesOnIDCollision proves the redraw. A collision is a
// one-in-a-million event at production scale, so the draw is replaced rather
// than waited for: the source hands back an id that is already taken twice
// before yielding a free one, and the row must land on the free draw. What it
// must NOT do is fall back to channels_id_seq, which is why the assertion is on
// the exact third draw and not merely on "some id was allocated".
func TestCreateChannelRetriesOnIDCollision(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551292002")

	taken := mustChannel(t, s, u.ID, "First")
	const free = int64(store.ChannelIDMin + 4242)

	draws := 0
	store.SetChannelIDSource(s, func() (int64, error) {
		draws++
		if draws < 3 {
			return taken.ID, nil
		}
		return free, nil
	})

	ch, err := s.CreateChannel(ctx, u.ID, "Second", "", false)
	if err != nil {
		t.Fatalf("create after collisions: %v", err)
	}
	if ch.ID != free {
		t.Fatalf("id = %d, want the third draw %d — a collision must redraw, never fall back to the sequence", ch.ID, free)
	}
	if draws != 3 {
		t.Errorf("took %d draws, want 3", draws)
	}

	// The colliding attempts must leave the channel they collided with intact,
	// and the retry must carry the rest of the create with it.
	if got, ok, err := s.ChannelByID(ctx, taken.ID); err != nil || !ok || got.Title != "First" {
		t.Errorf("collided-with channel = %+v ok=%v err=%v, want it untouched", got, ok, err)
	}
	if _, ok, err := s.ChannelMemberOf(ctx, ch.ID, u.ID); err != nil || !ok {
		t.Errorf("creator participant row: ok=%v err=%v", ok, err)
	}
}

// TestCreateChannelFailsClosedOnEntropyError pins the fail-closed rule: an
// unusable draw fails the creation. Any fallback — the sequence, a counter, a
// clock — would put a guessable id on a channel precisely when the randomness
// is broken.
func TestCreateChannelFailsClosedOnEntropyError(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551292003")

	entropyErr := errors.New("no entropy")
	store.SetChannelIDSource(s, func() (int64, error) { return 0, entropyErr })

	if _, err := s.CreateChannel(ctx, u.ID, "News", "", false); !errors.Is(err, entropyErr) {
		t.Fatalf("create with a broken draw: err = %v, want %v", err, entropyErr)
	}
	channels, err := s.ChannelsForUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("channels for user: %v", err)
	}
	if len(channels) != 0 {
		t.Fatalf("failed create left %d channels behind", len(channels))
	}
}

// TestCreateChannelFailsAfterRepeatedCollisions covers the other end of the
// retry: a source that never yields a free id is broken, not unlucky, so the
// redraw is bounded and the creation fails rather than spinning or reaching for
// the sequence. The draw count is compared against the shipped
// store.ChannelIDAttempts, so what it verifies is that the loop exhausts that
// bound exactly — an unbounded loop, or one that gives up early, fails here.
// It cannot catch the constant and the loop being changed together, which is
// the price of comparing against the constant rather than a copy of its value.
func TestCreateChannelFailsAfterRepeatedCollisions(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15551292004")

	taken := mustChannel(t, s, u.ID, "First")
	draws := 0
	store.SetChannelIDSource(s, func() (int64, error) {
		draws++
		return taken.ID, nil
	})

	if _, err := s.CreateChannel(ctx, u.ID, "Second", "", false); err == nil {
		t.Fatal("create with an always-colliding draw succeeded, want an error")
	}
	if draws != store.ChannelIDAttempts {
		t.Errorf("took %d draws, want exactly %d (the documented bound)", draws, store.ChannelIDAttempts)
	}
	channels, err := s.ChannelsForUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("channels for user: %v", err)
	}
	if len(channels) != 1 {
		t.Fatalf("holds %d channels, want just the first — the failed create wrote a row", len(channels))
	}
}
