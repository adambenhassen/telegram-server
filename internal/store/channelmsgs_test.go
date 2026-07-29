package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func post(t *testing.T, s *store.Store, channelID, fromID int64, text string, rid int64) (store.ChannelMessage, int) {
	t.Helper()
	m, pts, dup, err := s.PostChannelMessage(context.Background(), channelID, fromID, text, rid, nil)
	if err != nil {
		t.Fatalf("post %q: %v", text, err)
	}
	if dup {
		t.Fatalf("post %q flagged dup", text)
	}
	return m, pts
}

func TestPostChannelMessageAdvancesStream(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	author := mustUser(t, s, "+15551260001")
	ch := mustChannel(t, s, author.ID, "news").ID

	if pts, err := s.ChannelState(ctx, ch); err != nil || pts != 0 {
		t.Fatalf("fresh channel state = %d, err %v; want 0", pts, err)
	}

	first, pts1 := post(t, s, ch, author.ID, "hi", 42)
	if first.LocalID != 1 || pts1 != 1 {
		t.Fatalf("first post: local_id %d pts %d, want 1,1", first.LocalID, pts1)
	}
	if first.ChannelID != ch || first.FromID != author.ID || first.Message != "hi" || first.RandomID != 42 {
		t.Fatalf("first post row wrong: %+v", first)
	}
	if first.FileID != nil {
		t.Fatalf("first post file_id = %v, want nil", *first.FileID)
	}
	if first.Date.IsZero() {
		t.Fatal("first post date is zero")
	}

	second, pts2 := post(t, s, ch, author.ID, "again", 43)
	if second.LocalID != 2 || pts2 != 2 {
		t.Fatalf("second post: local_id %d pts %d, want 2,2", second.LocalID, pts2)
	}

	pts, err := s.ChannelState(ctx, ch)
	if err != nil || pts != 2 {
		t.Fatalf("channel state = %d, err %v; want 2", pts, err)
	}

	// One event per post, at the pts that post produced.
	events, err := s.ChannelEventsWindow(ctx, ch, 0, pts, 10)
	if err != nil {
		t.Fatalf("events window: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2", len(events))
	}
	for i, want := range []struct{ pts, localID int64 }{{1, 1}, {2, 2}} {
		if int64(events[i].Pts) != want.pts || events[i].LocalID != want.localID {
			t.Fatalf("event %d = pts %d local_id %d, want %d,%d", i, events[i].Pts, events[i].LocalID, want.pts, want.localID)
		}
		if events[i].Type != store.EventNewMessage {
			t.Fatalf("event %d type = %d, want %d", i, events[i].Type, store.EventNewMessage)
		}
	}

	msgs, err := s.ChannelMessages(ctx, ch, []int64{1, 2, 99})
	if err != nil {
		t.Fatalf("channel messages: %v", err)
	}
	if len(msgs) != 2 || msgs[1].Message != "hi" || msgs[2].Message != "again" {
		t.Fatalf("channel messages = %+v", msgs)
	}
}

func TestPostChannelMessageDedupsRandomID(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	author := mustUser(t, s, "+15551260002")
	ch := mustChannel(t, s, author.ID, "news").ID

	first, pts1 := post(t, s, ch, author.ID, "hi", 42)

	again, pts2, dup, err := s.PostChannelMessage(ctx, ch, author.ID, "hi", 42, nil)
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	if !dup {
		t.Fatal("resend not flagged dup")
	}
	if again.LocalID != first.LocalID {
		t.Fatalf("resend local_id = %d, want %d", again.LocalID, first.LocalID)
	}
	if pts2 != pts1 {
		t.Fatalf("resend advanced pts %d -> %d", pts1, pts2)
	}

	pts, err := s.ChannelState(ctx, ch)
	if err != nil || pts != 1 {
		t.Fatalf("channel state = %d, err %v; want 1", pts, err)
	}
	history, err := s.ChannelHistory(ctx, ch, 0, 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("history = %d rows, want 1", len(history))
	}
	events, err := s.ChannelEventsWindow(ctx, ch, 0, 10, 10)
	if err != nil {
		t.Fatalf("events window: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
}

func TestChannelEventsWindowIsBounded(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	author := mustUser(t, s, "+15551260003")
	ch := mustChannel(t, s, author.ID, "news").ID

	post(t, s, ch, author.ID, "one", 1)
	post(t, s, ch, author.ID, "two", 2)
	post(t, s, ch, author.ID, "three", 3)

	// limit wins over the window.
	events, err := s.ChannelEventsWindow(ctx, ch, 0, 2, 1)
	if err != nil {
		t.Fatalf("events window: %v", err)
	}
	if len(events) != 1 || events[0].Pts != 1 {
		t.Fatalf("limited window = %+v, want one event at pts 1", events)
	}

	// toPts wins over the log: the third event is never advertised.
	events, err = s.ChannelEventsWindow(ctx, ch, 0, 2, 10)
	if err != nil {
		t.Fatalf("events window: %v", err)
	}
	if len(events) != 2 || events[1].Pts != 2 {
		t.Fatalf("upper-bounded window = %+v, want pts 1 and 2", events)
	}
}

func TestChannelHistoryNewestFirstSkipsDeleted(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	author := mustUser(t, s, "+15551260004")
	ch := mustChannel(t, s, author.ID, "news").ID

	post(t, s, ch, author.ID, "one", 1)
	post(t, s, ch, author.ID, "two", 2)
	post(t, s, ch, author.ID, "three", 3)

	history, err := s.ChannelHistory(ctx, ch, 0, 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 3 || history[0].LocalID != 3 || history[2].LocalID != 1 {
		t.Fatalf("history not newest-first: %+v", history)
	}

	if err := store.SetChannelPostDeleted(ctx, s, ch, 2); err != nil {
		t.Fatalf("mark deleted: %v", err)
	}
	history, err = s.ChannelHistory(ctx, ch, 0, 10)
	if err != nil {
		t.Fatalf("history after delete: %v", err)
	}
	if len(history) != 2 || history[0].LocalID != 3 || history[1].LocalID != 1 {
		t.Fatalf("deleted row not skipped: %+v", history)
	}

	// offsetID pages strictly older.
	history, err = s.ChannelHistory(ctx, ch, 3, 10)
	if err != nil {
		t.Fatalf("history page: %v", err)
	}
	if len(history) != 1 || history[0].LocalID != 1 {
		t.Fatalf("offset page = %+v, want local_id 1 only", history)
	}
}

// The channel_state row lock ahead of the dedup read is the one thing here that
// the per-account original does not have, so it gets its own test: two posts of
// the same random_id landing at once must serialise on that row, and exactly one
// of them must come back a duplicate. Without the lock both can miss the lookup
// and the second dies on channel_messages_random_uniq instead.
func TestPostChannelMessageDedupsUnderConcurrency(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	author := mustUser(t, s, "+15551260005")
	ch := mustChannel(t, s, author.ID, "news").ID

	const posters = 4
	var wg sync.WaitGroup
	results := make([]struct {
		msg store.ChannelMessage
		pts int
		dup bool
		err error
	}, posters)
	start := make(chan struct{})
	for i := range posters {
		wg.Go(func() {
			<-start
			r := &results[i]
			r.msg, r.pts, r.dup, r.err = s.PostChannelMessage(ctx, ch, author.ID, "hi", 42, nil)
		})
	}
	close(start)
	wg.Wait()

	dups := 0
	for i, r := range results {
		if r.err != nil {
			t.Fatalf("post %d: %v", i, r.err)
		}
		if r.msg.LocalID != 1 || r.pts != 1 {
			t.Fatalf("post %d: local_id %d pts %d, want 1,1", i, r.msg.LocalID, r.pts)
		}
		if r.dup {
			dups++
		}
	}
	if dups != posters-1 {
		t.Fatalf("%d posts flagged dup, want %d", dups, posters-1)
	}

	pts, err := s.ChannelState(ctx, ch)
	if err != nil || pts != 1 {
		t.Fatalf("channel state = %d, err %v; want 1", pts, err)
	}
	history, err := s.ChannelHistory(ctx, ch, 0, 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("history = %d rows, want 1", len(history))
	}
}

// ChannelMessages keeps deleted rows and ChannelHistory drops them. The split is
// intentional — event hydration has to be able to name a post a delete event
// removed — so it is asserted rather than left to read as an oversight.
func TestChannelMessagesKeepsDeletedRows(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	author := mustUser(t, s, "+15551260006")
	ch := mustChannel(t, s, author.ID, "news").ID

	post(t, s, ch, author.ID, "one", 1)
	if err := store.SetChannelPostDeleted(ctx, s, ch, 1); err != nil {
		t.Fatalf("mark deleted: %v", err)
	}

	msgs, err := s.ChannelMessages(ctx, ch, []int64{1})
	if err != nil {
		t.Fatalf("channel messages: %v", err)
	}
	got, ok := msgs[1]
	if !ok {
		t.Fatal("deleted post missing from ChannelMessages")
	}
	if !got.Deleted || got.Message != "one" {
		t.Fatalf("deleted post = %+v", got)
	}

	history, err := s.ChannelHistory(ctx, ch, 0, 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("history = %+v, want empty", history)
	}
}

// seat admits userID to the channel through the only admission path there is,
// then forces the role the case under test needs.
func seat(t *testing.T, s *store.Store, ch store.Channel, creatorID, userID int64, role int16) {
	t.Helper()
	ctx := context.Background()
	hash, err := s.CreateChannelInvite(ctx, ch.ID, creatorID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, hash, userID); err != nil {
		t.Fatalf("join: %v", err)
	}
	if role != 0 {
		if err = store.SetChannelRole(ctx, s, ch.ID, userID, role); err != nil {
			t.Fatalf("set role: %v", err)
		}
	}
}

func mustMegagroup(t *testing.T, s *store.Store, creatorID int64, title string) store.Channel {
	t.Helper()
	ch, err := s.CreateChannel(context.Background(), creatorID, title, "", true)
	if err != nil {
		t.Fatalf("create megagroup: %v", err)
	}
	return ch
}

// postAs asserts the checked entry point accepted the post.
func postAs(t *testing.T, s *store.Store, channelID, fromID int64, text string, rid int64) store.ChannelMessage {
	t.Helper()
	m, _, dup, err := s.PostChannelMessageAs(context.Background(), channelID, fromID, text, rid, nil)
	if err != nil {
		t.Fatalf("post as %d: %v", fromID, err)
	}
	if dup {
		t.Fatalf("post %q flagged dup", text)
	}
	return m
}

// refusedAs asserts the checked entry point rejected the post with ErrNotMember
// and wrote nothing: pts unmoved and no new event row.
func refusedAs(t *testing.T, s *store.Store, channelID, fromID int64, rid int64) {
	t.Helper()
	ctx := context.Background()
	before, err := s.ChannelState(ctx, channelID)
	if err != nil {
		t.Fatalf("state before: %v", err)
	}
	eventsBefore, err := s.ChannelEventsWindow(ctx, channelID, 0, before, 100)
	if err != nil {
		t.Fatalf("events before: %v", err)
	}

	if _, _, _, err = s.PostChannelMessageAs(ctx, channelID, fromID, "nope", rid, nil); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("post as %d = %v, want ErrNotMember", fromID, err)
	}

	after, err := s.ChannelState(ctx, channelID)
	if err != nil {
		t.Fatalf("state after: %v", err)
	}
	if after != before {
		t.Fatalf("rejected post moved pts %d -> %d", before, after)
	}
	eventsAfter, err := s.ChannelEventsWindow(ctx, channelID, 0, after, 100)
	if err != nil {
		t.Fatalf("events after: %v", err)
	}
	if len(eventsAfter) != len(eventsBefore) {
		t.Fatalf("rejected post wrote %d event(s)", len(eventsAfter)-len(eventsBefore))
	}
}

func TestPostChannelMessageAsBroadcastNeedsAdmin(t *testing.T) {
	t.Parallel()
	s := open(t)
	creator := mustUser(t, s, "+15551260701")
	admin := mustUser(t, s, "+15551260702")
	member := mustUser(t, s, "+15551260703")
	outsider := mustUser(t, s, "+15551260704")
	ch := mustChannel(t, s, creator.ID, "news")
	seat(t, s, ch, creator.ID, admin.ID, 1)
	seat(t, s, ch, creator.ID, member.ID, 0)

	postAs(t, s, ch.ID, creator.ID, "from creator", 1)
	postAs(t, s, ch.ID, admin.ID, "from admin", 2)
	refusedAs(t, s, ch.ID, member.ID, 3)
	refusedAs(t, s, ch.ID, outsider.ID, 4)
}

func TestPostChannelMessageAsMegagroupAdmitsMembers(t *testing.T) {
	t.Parallel()
	s := open(t)
	creator := mustUser(t, s, "+15551260711")
	member := mustUser(t, s, "+15551260712")
	outsider := mustUser(t, s, "+15551260713")
	ch := mustMegagroup(t, s, creator.ID, "chat")
	seat(t, s, ch, creator.ID, member.ID, 0)

	postAs(t, s, ch.ID, member.ID, "hi", 1)
	refusedAs(t, s, ch.ID, outsider.ID, 2)
}

func TestPostChannelMessageAsRejectsBanned(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551260721")
	bannedAdmin := mustUser(t, s, "+15551260722")
	bannedMember := mustUser(t, s, "+15551260723")

	broadcast := mustChannel(t, s, creator.ID, "news")
	seat(t, s, broadcast, creator.ID, bannedAdmin.ID, 1)
	megagroup := mustMegagroup(t, s, creator.ID, "chat")
	seat(t, s, megagroup, creator.ID, bannedMember.ID, 0)

	until := time.Now().Add(time.Hour)
	if err := store.SetChannelBan(ctx, s, broadcast.ID, bannedAdmin.ID, &until); err != nil {
		t.Fatalf("ban admin: %v", err)
	}
	if err := store.SetChannelBan(ctx, s, megagroup.ID, bannedMember.ID, &until); err != nil {
		t.Fatalf("ban member: %v", err)
	}

	// A ban outranks the role: role 1 on a broadcast is not a way past it.
	refusedAs(t, s, broadcast.ID, bannedAdmin.ID, 1)
	refusedAs(t, s, megagroup.ID, bannedMember.ID, 2)
}

func TestPostChannelMessageAsAllowsLapsedBan(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551260731")
	member := mustUser(t, s, "+15551260732")
	ch := mustMegagroup(t, s, creator.ID, "chat")
	seat(t, s, ch, creator.ID, member.ID, 0)

	past := time.Now().Add(-time.Hour)
	if err := store.SetChannelBan(ctx, s, ch.ID, member.ID, &past); err != nil {
		t.Fatalf("ban: %v", err)
	}

	postAs(t, s, ch.ID, member.ID, "back", 1)
}

func TestPostChannelMessageAsRejectsPermanentBan(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551260741")
	member := mustUser(t, s, "+15551260742")
	ch := mustMegagroup(t, s, creator.ID, "chat")
	seat(t, s, ch, creator.ID, member.ID, 0)

	// banned_until = 'infinity' decodes through its own path (bannedForever); a
	// permanent ban is the one the check must never let through.
	if err := store.SetChannelBanInfinite(ctx, s, ch.ID, member.ID); err != nil {
		t.Fatalf("ban forever: %v", err)
	}

	refusedAs(t, s, ch.ID, member.ID, 1)
}

func TestPostChannelMessageAsHidesUnknownChannel(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	user := mustUser(t, s, "+15551260751")
	ch := mustChannel(t, s, user.ID, "news")

	// A channel id with no row must return the same ErrNotMember as a channel the
	// caller is not in, not the channel_state foreign-key error that would say the
	// id is free.
	missing := ch.ID + 1_000_000
	if _, _, _, err := s.PostChannelMessageAs(ctx, missing, user.ID, "nope", 1, nil); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("post to unknown channel = %v, want ErrNotMember", err)
	}
	if pts, err := s.ChannelState(ctx, missing); err != nil || pts != 0 {
		t.Fatalf("unknown channel state = %d, err %v; want 0", pts, err)
	}
}
