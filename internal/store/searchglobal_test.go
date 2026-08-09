package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// globalKey identifies one hit the way the cursor does, so a page sequence can
// be compared against a single-page read without depending on row contents.
type globalKey struct {
	peerType store.PeerType
	peerID   int64
	msgID    int64
}

func keysOf(hits []store.GlobalSearchHit) []globalKey {
	out := make([]globalKey, len(hits))
	for i, h := range hits {
		out[i] = globalKey{peerType: h.PeerType, peerID: h.PeerID, msgID: hitID(h)}
	}
	return out
}

func hitID(h store.GlobalSearchHit) int64 {
	if h.Post != nil {
		return h.Post.LocalID
	}
	return h.Owned.LocalID
}

func hitText(t *testing.T, h store.GlobalSearchHit) string {
	t.Helper()
	switch {
	case h.Post != nil:
		return h.Post.Message
	case h.Owned != nil:
		return h.Owned.Text
	default:
		t.Fatalf("hit %+v carries neither an owned row nor a post", h)
		return ""
	}
}

func searchGlobal(t *testing.T, s *store.Store, userID int64, q string, cursor *store.GlobalSearchCursor, limit int) []store.GlobalSearchHit {
	t.Helper()
	hits, err := s.SearchGlobal(context.Background(), userID, q, cursor, limit)
	if err != nil {
		t.Fatalf("search global: %v", err)
	}
	return hits
}

// dateOwned pins an owned row's date so a test can assert an order that does not
// depend on how fast the seed ran. Both search arms sort on whole seconds, so a
// seed that lands every row in one second only ever exercises the tie-breakers.
func dateOwned(t *testing.T, s *store.Store, ownerID, localID int64, at time.Time) {
	t.Helper()
	if _, err := store.StorePool(s).Exec(context.Background(),
		`UPDATE messages SET date = $1 WHERE owner_id = $2 AND local_id = $3`, at, ownerID, localID); err != nil {
		t.Fatalf("set owned date: %v", err)
	}
}

func datePost(t *testing.T, s *store.Store, channelID, localID int64, at time.Time) {
	t.Helper()
	if _, err := store.StorePool(s).Exec(context.Background(),
		`UPDATE channel_messages SET date = $1 WHERE channel_id = $2 AND local_id = $3`, at, channelID, localID); err != nil {
		t.Fatalf("set post date: %v", err)
	}
}

// A caller in a 1:1, a chat and a channel gets hits from all three in one page,
// newest-first, each carrying the peer it belongs to.
func TestSearchGlobalUnionsEveryPeerKind(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	alice := mustUser(t, s, "+15551320001")
	bob := mustUser(t, s, "+15551320002")

	dm := send(t, s, bob, alice, "the deadline is monday", 9001)
	chat, err := s.CreateChat(ctx, alice.ID, "Team", []int64{bob.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	chatMsg, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: bob.ID, Text: "chat deadline moved", RandomID: 9002,
	})
	if err != nil {
		t.Fatalf("send chat message: %v", err)
	}
	ch := mustChannel(t, s, alice.ID, "News")
	notice, _ := post(t, s, ch.ID, alice.ID, "channel deadline notice", 9003)
	// A post nobody searched for, so a match is never just "everything".
	post(t, s, ch.ID, alice.ID, "unrelated chatter", 9004)

	// alice's own copy of the fan-out carries her local_id, not the sender's.
	aliceChatCopy, err := s.SearchMessages(ctx, alice.ID, store.PeerTypeChat, chat.ID, "deadline", 0, 10)
	if err != nil || len(aliceChatCopy) != 1 {
		t.Fatalf("locate alice's chat copy: %d rows, err %v", len(aliceChatCopy), err)
	}
	if aliceChatCopy[0].Text != chatMsg.Text {
		t.Fatalf("chat copy = %q, want %q", aliceChatCopy[0].Text, chatMsg.Text)
	}

	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	datePost(t, s, ch.ID, notice.LocalID, base)
	dateOwned(t, s, alice.ID, aliceChatCopy[0].LocalID, base.Add(time.Second))
	dateOwned(t, s, alice.ID, dm.PeerLocalID, base.Add(2*time.Second))

	hits := searchGlobal(t, s, alice.ID, "deadline", nil, 20)
	if len(hits) != 3 {
		t.Fatalf("hits = %d, want 3: %+v", len(hits), keysOf(hits))
	}
	want := []struct {
		peerType store.PeerType
		peerID   int64
		text     string
	}{
		{store.PeerTypeUser, bob.ID, "the deadline is monday"},
		{store.PeerTypeChat, chat.ID, "chat deadline moved"},
		{store.PeerTypeChannel, ch.ID, "channel deadline notice"},
	}
	for i, w := range want {
		if hits[i].PeerType != w.peerType || hits[i].PeerID != w.peerID || hitText(t, hits[i]) != w.text {
			t.Errorf("hit %d = %v/%d %q, want %v/%d %q",
				i, hits[i].PeerType, hits[i].PeerID, hitText(t, hits[i]), w.peerType, w.peerID, w.text)
		}
	}
	if hits[2].Post == nil || hits[0].Owned == nil || hits[1].Owned == nil {
		t.Errorf("arms mixed up: %+v", hits)
	}
}

// The channel arm is membership-gated, not merely peer-scoped: a non-member and
// a banned member get the same empty channel result, and a caller in no dialogs
// at all gets an empty page rather than an error.
func TestSearchGlobalGatesChannelPostsOnUnbannedMembership(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551320011")
	member := mustUser(t, s, "+15551320012")
	outsider := mustUser(t, s, "+15551320013")

	ch := mustChannel(t, s, owner.ID, "Ops")
	hash, err := s.CreateChannelInvite(ctx, ch.ID, owner.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, hash, member.ID); err != nil {
		t.Fatalf("join: %v", err)
	}
	post(t, s, ch.ID, owner.ID, "deadline for the migration", 9101)
	// The member also has a 1:1 hit, so a lost channel arm is visible as the
	// channel row disappearing rather than as an empty response.
	send(t, s, member, owner, "deadline in dm", 9102)

	if hits := searchGlobal(t, s, member.ID, "deadline", nil, 20); len(hits) != 2 {
		t.Fatalf("member hits = %d, want 2 (channel post + dm): %+v", len(hits), keysOf(hits))
	}
	if hits := searchGlobal(t, s, outsider.ID, "deadline", nil, 20); len(hits) != 0 {
		t.Fatalf("outsider hits = %+v, want none", keysOf(hits))
	}

	until := time.Now().Add(time.Hour)
	if err = store.SetChannelBan(ctx, s, ch.ID, member.ID, &until); err != nil {
		t.Fatalf("ban: %v", err)
	}
	hits := searchGlobal(t, s, member.ID, "deadline", nil, 20)
	if len(hits) != 1 || hits[0].PeerType != store.PeerTypeUser {
		t.Fatalf("banned member hits = %+v, want only the dm", keysOf(hits))
	}
}

// Paging to exhaustion one row at a time yields exactly the rows a single large
// page yields, in the same order: no duplicate, no skip, across three peer kinds
// whose id spaces are not comparable and whose dates collide.
func TestSearchGlobalCursorPagesEachHitExactlyOnce(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	alice := mustUser(t, s, "+15551320021")
	bob := mustUser(t, s, "+15551320022")

	for i := range 3 {
		send(t, s, bob, alice, "deadline dm", int64(9200+i))
	}
	chat, err := s.CreateChat(ctx, alice.ID, "Team", []int64{bob.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	for i := range 3 {
		if _, _, _, err = s.SendChatMessage(ctx, store.FanOut{
			ChatID: chat.ID, FromID: bob.ID, Text: "deadline chat", RandomID: int64(9210 + i),
		}); err != nil {
			t.Fatalf("send chat message: %v", err)
		}
	}
	first := mustChannel(t, s, alice.ID, "First")
	second := mustChannel(t, s, alice.ID, "Second")
	for i := range 3 {
		post(t, s, first.ID, alice.ID, "deadline post", int64(9220+i))
		post(t, s, second.ID, alice.ID, "deadline post", int64(9230+i))
	}
	// Half the rows share one second, so the tie-breakers carry the ordering for
	// them; the other half are spread out so date ordering carries it.
	tie := time.Now().Add(-time.Hour).Truncate(time.Second)
	if _, err = store.StorePool(s).Exec(ctx,
		`UPDATE messages SET date = $1 WHERE owner_id = $2 AND local_id % 2 = 0`, tie, alice.ID); err != nil {
		t.Fatalf("collide owned dates: %v", err)
	}
	if _, err = store.StorePool(s).Exec(ctx,
		`UPDATE channel_messages SET date = $1 WHERE channel_id = $2`, tie, first.ID); err != nil {
		t.Fatalf("collide post dates: %v", err)
	}

	full := searchGlobal(t, s, alice.ID, "deadline", nil, 100)
	if len(full) != 12 {
		t.Fatalf("single page = %d hits, want 12: %+v", len(full), keysOf(full))
	}

	var paged []store.GlobalSearchHit
	var cursor *store.GlobalSearchCursor
	for range len(full) + 1 {
		page := searchGlobal(t, s, alice.ID, "deadline", cursor, 1)
		if len(page) == 0 {
			break
		}
		if len(page) != 1 {
			t.Fatalf("limit=1 page returned %d hits", len(page))
		}
		paged = append(paged, page[0])
		cursor = &store.GlobalSearchCursor{
			Rate: page[0].Rate, PeerType: page[0].PeerType, PeerID: page[0].PeerID, MsgID: hitID(page[0]),
		}
	}

	wantKeys, gotKeys := keysOf(full), keysOf(paged)
	if len(gotKeys) != len(wantKeys) {
		t.Fatalf("paged %d hits, want %d\npaged: %+v\nfull:  %+v", len(gotKeys), len(wantKeys), gotKeys, wantKeys)
	}
	for i := range wantKeys {
		if gotKeys[i] != wantKeys[i] {
			t.Fatalf("page sequence diverges at %d: got %+v, want %+v", i, gotKeys[i], wantKeys[i])
		}
	}
	seen := map[globalKey]bool{}
	for _, k := range gotKeys {
		if seen[k] {
			t.Fatalf("key %+v served twice", k)
		}
		seen[k] = true
	}
}
