package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// storedFile allocates a file and marks it stored, which is the state every
// reference-creating path meets it in.
func storedFile(t *testing.T, s *store.Store, uploaderID int64) store.File {
	t.Helper()
	f := allocate(t, s, uploaderID, 11)
	if err := s.MarkFileStored(context.Background(), f.ID); err != nil {
		t.Fatalf("mark stored: %v", err)
	}
	return f
}

// historyLen is the number of live messages ownerID holds in a dialog. The
// fail-closed assertions are all "no row was written", and this is what reads
// that off the table an insert would have landed in.
func historyLen(t *testing.T, s *store.Store, ownerID int64, peerType store.PeerType, peerID int64) int {
	t.Helper()
	msgs, err := s.History(context.Background(), ownerID, peerType, peerID, 0, 50)
	if err != nil {
		t.Fatalf("history %d: %v", ownerID, err)
	}
	return len(msgs)
}

// A 1:1 media send takes the referenced file's row lock, so a file row that is
// gone by the time the send runs fails the send instead of writing a message
// row that points at nothing.
func TestSendMediaFailsClosedOnAbsentFileRow(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559120001")
	b := mustUser(t, s, "+15559120002")

	f := storedFile(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, f.ID); err != nil {
		t.Fatalf("erase file row: %v", err)
	}

	_, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID, 0) //nolint:dogsled // only the error is under test
	if !errors.Is(err, store.ErrFileMissing) {
		t.Fatalf("send with absent file row: err = %v, want ErrFileMissing", err)
	}
	if n := historyLen(t, s, a.ID, store.PeerTypeUser, b.ID); n != 0 {
		t.Fatalf("sender holds %d messages after a failed send, want 0", n)
	}
	if n := historyLen(t, s, b.ID, store.PeerTypeUser, a.ID); n != 0 {
		t.Fatalf("recipient holds %d messages after a failed send, want 0", n)
	}
}

// A chat fan-out is all-or-nothing: the file row is gone, so no member's copy is
// written — not the sender's, and not the copy of a member the loop would have
// reached before it noticed.
func TestSendChatMediaFanOutIsAllOrNothing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559120011")
	b := mustUser(t, s, "+15559120012")
	c := mustUser(t, s, "+15559120013")
	chat := chatWith(t, s, a, b, c)

	f := storedFile(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, f.ID); err != nil {
		t.Fatalf("erase file row: %v", err)
	}

	before := map[int64]int{}
	for _, u := range []store.User{a, b, c} {
		before[u.ID] = historyLen(t, s, u.ID, store.PeerTypeChat, chat.ID)
	}

	_, _, _, err := s.SendChatMessage(ctx, store.FanOut{ //nolint:dogsled // only the error is under test
		ChatID: chat.ID, FromID: a.ID, Text: "here", RandomID: 1, FileID: f.ID,
	})
	if !errors.Is(err, store.ErrFileMissing) {
		t.Fatalf("chat send with absent file row: err = %v, want ErrFileMissing", err)
	}
	for _, u := range []store.User{a, b, c} {
		if n := historyLen(t, s, u.ID, store.PeerTypeChat, chat.ID); n != before[u.ID] {
			t.Fatalf("member %d holds %d chat messages, want %d — the fan-out wrote a partial reference",
				u.ID, n, before[u.ID])
		}
	}
}

// The forward path carries a file id it read in an earlier transaction. Both
// destinations fail closed on it: a user destination writes two rows and a chat
// destination one per member, and neither is written.
func TestForwardFailsClosedOnAbsentFileRow(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559120021")
	b := mustUser(t, s, "+15559120022")
	c := mustUser(t, s, "+15559120023")
	chat := chatWith(t, s, a, b, c)

	f := storedFile(t, s, a.ID)
	src, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID, 0) //nolint:dogsled // only the stored row is needed
	if err != nil {
		t.Fatalf("seed send: %v", err)
	}
	if err = store.EraseFileRow(ctx, s, f.ID); err != nil {
		t.Fatalf("erase file row: %v", err)
	}
	sources := []store.ForwardSource{{FromID: a.ID, Date: src.Date, Text: src.Text, FileID: f.ID}}

	_, _, err = s.ForwardMessages(ctx, a.ID, store.PeerTypeUser, c.ID, sources, []int64{2})
	if !errors.Is(err, store.ErrFileMissing) {
		t.Fatalf("forward to user with absent file row: err = %v, want ErrFileMissing", err)
	}
	if n := historyLen(t, s, c.ID, store.PeerTypeUser, a.ID); n != 0 {
		t.Fatalf("destination holds %d messages after a failed forward, want 0", n)
	}

	chatBefore := historyLen(t, s, b.ID, store.PeerTypeChat, chat.ID)
	_, _, err = s.ForwardMessages(ctx, a.ID, store.PeerTypeChat, chat.ID, sources, []int64{3})
	if !errors.Is(err, store.ErrFileMissing) {
		t.Fatalf("forward to chat with absent file row: err = %v, want ErrFileMissing", err)
	}
	if n := historyLen(t, s, b.ID, store.PeerTypeChat, chat.ID); n != chatBefore {
		t.Fatalf("chat member holds %d messages after a failed forward, want %d", n, chatBefore)
	}
}

// The regression test for the forward race in MAIN-331 finding 1: a reference
// insert running against a transaction that holds the file row exclusively must
// serialize behind it rather than commit alongside it, and must then see the
// row is gone. Before the interlock the forward read no lock at all, so it
// committed two rows naming a file the other transaction had just deleted.
func TestForwardSerializesAgainstFileRowRemoval(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559120031")
	b := mustUser(t, s, "+15559120032")
	c := mustUser(t, s, "+15559120033")

	f := storedFile(t, s, a.ID)
	src, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID, 0) //nolint:dogsled // only the stored row is needed
	if err != nil {
		t.Fatalf("seed send: %v", err)
	}

	// The eraser's half: hold the file row exclusively, decide to delete it, and
	// commit only once the forward is demonstrably parked behind the lock.
	hold, err := store.HoldFileRow(ctx, s, f.ID)
	if err != nil {
		t.Fatalf("hold file row: %v", err)
	}

	done := make(chan struct{})
	var fwdErr error
	go func() {
		defer close(done)
		_, _, fwdErr = s.ForwardMessages(ctx, b.ID, store.PeerTypeUser, c.ID,
			[]store.ForwardSource{{FromID: a.ID, Date: src.Date, Text: src.Text, FileID: f.ID}},
			[]int64{7})
	}()

	if err = store.WaitForLockWaiters(ctx, s, 1); err != nil {
		hold.Release()
		t.Fatalf("wait for the forward to block: %v", err)
	}
	select {
	case <-done:
		t.Fatal("the forward committed while the file row was held exclusively — it took no lock on the row")
	default:
	}

	if err = hold.EraseAndCommit(ctx); err != nil {
		t.Fatalf("erase and commit: %v", err)
	}
	<-done

	if !errors.Is(fwdErr, store.ErrFileMissing) {
		t.Fatalf("forward after the row was erased: err = %v, want ErrFileMissing", fwdErr)
	}
	if n := historyLen(t, s, c.ID, store.PeerTypeUser, b.ID); n != 0 {
		t.Fatalf("destination holds %d messages, want 0 — a reference to an erased file committed", n)
	}
}

// The lock is shared, so two references to one file are concurrent: a send runs
// to completion while another holder has the row locked in share mode. Taking
// the row FOR UPDATE instead would serialize every media send that names the
// same file behind every other, and this send would park until the release.
func TestConcurrentMediaReferencesDoNotBlockEachOther(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559120041")
	b := mustUser(t, s, "+15559120042")

	f := storedFile(t, s, a.ID)
	release, err := store.HoldFileRowShared(ctx, s, f.ID)
	if err != nil {
		t.Fatalf("hold shared: %v", err)
	}
	defer release()

	done := make(chan error, 1)
	go func() {
		_, _, _, _, e := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID, 0)
		done <- e
	}()
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("send alongside a shared holder: %v", e)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("send blocked behind a shared holder — the reference lock is not shared")
	}
}

// Two halves of one statement about which sends reach for the file row: a media
// send blocks on an exclusive holder, a text send in the same conditions does
// not. The pair is what rules out a lock taken unconditionally, which would
// pass a media-only assertion and put every text message behind the eraser.
func TestTextSendTakesNoFileLock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559120051")
	b := mustUser(t, s, "+15559120052")

	f := storedFile(t, s, a.ID)
	hold, err := store.HoldFileRow(ctx, s, f.ID)
	if err != nil {
		t.Fatalf("hold file row: %v", err)
	}
	defer hold.Release()

	text := make(chan error, 1)
	go func() {
		_, _, _, _, e := s.SendMessage(ctx, a.ID, b.ID, "no media", 1, 0, 0)
		text <- e
	}()
	select {
	case e := <-text:
		if e != nil {
			t.Fatalf("text send alongside an exclusive file holder: %v", e)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("text send blocked on a files row lock it must never take")
	}

	media := make(chan struct{})
	var mediaErr error
	go func() {
		defer close(media)
		_, _, _, _, mediaErr = s.SendMessage(ctx, a.ID, b.ID, "media", 2, f.ID, 0)
	}()
	if err = store.WaitForLockWaiters(ctx, s, 1); err != nil {
		t.Fatalf("wait for the media send to block: %v", err)
	}
	select {
	case <-media:
		t.Fatal("media send committed while the file row was held exclusively — it took no lock on the row")
	default:
	}
	hold.Release()
	<-media
	if mediaErr != nil {
		t.Fatalf("media send after the holder released: %v", mediaErr)
	}
}

// The other half of "the text path is unchanged": it issues no query against
// files at all, not merely a query that happens not to lock. Asserted on
// Postgres's own scan counters for the table, in this test's private database.
//
// The media send at the end is the control on the counter itself: it is one
// reference insert and it has to show up, so a counter that cannot see a query
// fails here rather than silently passing the text assertion above it.
func TestTextPathIssuesNoFilesQuery(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559120061")
	b := mustUser(t, s, "+15559120062")
	c := mustUser(t, s, "+15559120063")
	chat := chatWith(t, s, a, b, c)

	// Allocation and the stored flag touch files themselves, so the baseline is
	// read after them and the window below measures only the sends.
	f := storedFile(t, s, a.ID)
	base := filesAccesses(t, s)

	src, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "plain", 1, 0, 0) //nolint:dogsled // only the stored row is needed
	if err != nil {
		t.Fatalf("text send: %v", err)
	}
	if _, _, _, err = s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "plain", RandomID: 2,
	}); err != nil {
		t.Fatalf("text chat send: %v", err)
	}
	if _, _, err = s.ForwardMessages(ctx, a.ID, store.PeerTypeUser, c.ID,
		[]store.ForwardSource{{FromID: a.ID, Date: src.Date, Text: src.Text}}, []int64{3}); err != nil {
		t.Fatalf("text forward: %v", err)
	}

	if got := filesAccesses(t, s); got != base {
		t.Fatalf("files table accessed %d times by the text paths (%d before), want none", got-base, base)
	}

	if _, _, _, _, err = s.SendMessage(ctx, a.ID, b.ID, "media", 4, f.ID, 0); err != nil {
		t.Fatalf("media send: %v", err)
	}
	if got := filesAccesses(t, s); got <= base {
		t.Fatalf("files accesses = %d after a media send, want more than %d — the counter cannot see a reference insert, "+
			"so the text assertion above proves nothing", got, base)
	}
}

func filesAccesses(t *testing.T, s *store.Store) int64 {
	t.Helper()
	n, err := store.FilesTableAccesses(context.Background(), s)
	if err != nil {
		t.Fatalf("files table accesses: %v", err)
	}
	return n
}
