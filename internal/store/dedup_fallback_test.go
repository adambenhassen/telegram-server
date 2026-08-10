package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// The dedup branches answer a resend from the stored message's own pts, and
// dedup_pts_test.go pins that. Two of them keep a fallback to the owner's
// current pts for the one input the key cannot classify: a random_id the sender
// already spent on another destination, which arrives holding a row that is not
// this send's message at all. The value that fallback returns is the one the
// contract exists to forbid, so the branch is not allowed to fire quietly —
// these tests drive each fallback end to end and require the log record that
// makes the occurrence findable afterwards.
//
// Neither branch needs a race to reach: the dedup key is (sender, random_id)
// with no destination in it, so one sender reusing one random_id across two
// destinations walks into both, in order, from the shipped API.

// logSink collects one Store's diagnostics as decoded JSON records. The store
// writes them from inside a send transaction, on whichever goroutine the caller
// runs on, so the buffer is guarded: an unsynchronised one is a race-detector
// failure rather than a flake to chase later.
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logSink) lines(t *testing.T) []map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(l.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

// openLogged is open(t) with the store's diagnostics captured instead of sent to
// the process logger. Scoped to this Store, so a parallel test's records never
// land in this sink.
func openLogged(t *testing.T) (*store.Store, *logSink) {
	t.Helper()
	sink := &logSink{}
	s, err := store.Open(context.Background(), pgtest.DSN(t), pgtest.EncKey(),
		store.WithLogger(slog.New(slog.NewJSONHandler(sink, nil))))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s, sink
}

// onlyFallback returns the one fallback record the sink holds. Firing twice is
// as wrong as not firing at all: the count is how many resends took the branch.
func onlyFallback(t *testing.T, sink *logSink) map[string]any {
	t.Helper()
	var hits []map[string]any
	all := sink.lines(t)
	for _, rec := range all {
		if rec["msg"] == "dedup pts fallback" {
			hits = append(hits, rec)
		}
	}
	if len(hits) != 1 {
		t.Fatalf("dedup pts fallback records = %d, want 1 (logged: %v)", len(hits), all)
	}
	return hits[0]
}

// hasAttrs requires the record to carry every listed attribute. Numbers cross
// JSON as float64, which is exact for every id these tests produce.
func hasAttrs(t *testing.T, rec map[string]any, want map[string]any) {
	t.Helper()
	for k, v := range want {
		got, ok := rec[k]
		if !ok {
			t.Errorf("log record has no %q attribute: %v", k, rec)
			continue
		}
		if id, isID := v.(int64); isID {
			v = float64(id)
		}
		if got != v {
			t.Errorf("log %s = %v, want %v", k, got, v)
		}
	}
}

// TestChatDedupFallbackToCurrentPtsIsLogged drives the fan-out fallback: a
// random_id spent on a 1:1 send, whose sender row carries no fanout_id, is
// presented again as a chat send. There are no copies to read a pts from, so
// every member is answered with where its log stands now — the outcome the
// contract forbids for a message that has one, reached by an input that has
// none. It stays the outcome on purpose (the reused id can carry a real send),
// and the record is what makes it findable.
func TestChatDedupFallbackToCurrentPtsIsLogged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, sink := openLogged(t)
	a := mustUser(t, s, "+15559992001")
	b := mustUser(t, s, "+15559992002")
	chat := chatWith(t, s, a, b)

	dm := send(t, s, a, b, "dm", 71)
	sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "chat", RandomID: 72})

	stored, perOwner, dup, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "dm", RandomID: 71,
	})
	if err != nil || !dup {
		t.Fatalf("cross-destination resend: dup=%v err=%v", dup, err)
	}
	if stored.LocalID != dm.LocalID {
		t.Fatalf("resend local_id = %d, want the 1:1 row's %d", stored.LocalID, dm.LocalID)
	}

	dmPts, err := s.MessagePts(ctx, a.ID, dm.LocalID)
	if err != nil {
		t.Fatalf("stored message pts: %v", err)
	}
	for _, u := range []store.User{a, b} {
		current := ptsOf(t, s, u.ID)
		if current == dmPts {
			t.Fatalf("member %d: current pts %d equals the stored message's — the test staged no gap", u.ID, current)
		}
		if perOwner[u.ID] != current {
			t.Errorf("member %d pts = %d, want the current %d", u.ID, perOwner[u.ID], current)
		}
	}

	hasAttrs(t, onlyFallback(t, sink), map[string]any{
		"level":           "WARN",
		"path":            "chat fanout",
		"chat_id":         chat.ID,
		"from_id":         a.ID,
		"random_id":       int64(71),
		"stored_local_id": dm.LocalID,
		"stored_peer_id":  b.ID,
	})
}

// TestOneToOneDedupMirrorFallbackToCurrentPtsIsLogged drives the mirror
// fallback: a random_id spent on a send to one peer, presented again as a send
// to another. The stored row has no counterpart on this recipient's side to name
// a pts from, so the recipient is answered with its current one. No caller reads
// that value today and only a comment says so, which is exactly why the branch
// has to announce itself rather than rely on the value staying unread.
func TestOneToOneDedupMirrorFallbackToCurrentPtsIsLogged(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, sink := openLogged(t)
	a := mustUser(t, s, "+15559992011")
	b := mustUser(t, s, "+15559992012")
	c := mustUser(t, s, "+15559992013")

	toC := send(t, s, a, c, "for c", 81)
	// Give b a log of its own, so the answered value is recognisably b's current
	// pts and not a coincidence of two fresh accounts sitting at the same number.
	send(t, s, c, b, "hi", 82)
	send(t, s, c, b, "again", 83)

	stored, senderPts, recipientPts, dup, err := s.SendMessage(ctx, a.ID, b.ID, "for c", 81, 0, 0)
	if err != nil || !dup {
		t.Fatalf("cross-destination resend: dup=%v err=%v", dup, err)
	}
	if stored.LocalID != toC.LocalID {
		t.Fatalf("resend local_id = %d, want the stored row's %d", stored.LocalID, toC.LocalID)
	}
	// The sender's own side is unaffected: it holds the message and answers from it.
	storedPts, err := s.MessagePts(ctx, a.ID, toC.LocalID)
	if err != nil {
		t.Fatalf("stored message pts: %v", err)
	}
	if senderPts != storedPts {
		t.Errorf("sender pts = %d, want the stored message's %d", senderPts, storedPts)
	}
	if current := ptsOf(t, s, b.ID); recipientPts != current {
		t.Errorf("recipient pts = %d, want b's current %d", recipientPts, current)
	}

	hasAttrs(t, onlyFallback(t, sink), map[string]any{
		"level":           "WARN",
		"path":            "1:1 mirror",
		"from_id":         a.ID,
		"to_id":           b.ID,
		"random_id":       int64(81),
		"stored_local_id": toC.LocalID,
		"stored_peer_id":  c.ID,
	})
}

// TestNoFallbackLoggedOnAnOrdinaryResend is the other half: the branch reports
// an exception, so a resend that takes the ordinary path must leave the log
// empty. Without this, a record that fired on every resend would still pass the
// two tests above.
func TestNoFallbackLoggedOnAnOrdinaryResend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, sink := openLogged(t)
	a := mustUser(t, s, "+15559992021")
	b := mustUser(t, s, "+15559992022")
	chat := chatWith(t, s, a, b)

	send(t, s, a, b, "dm", 91)
	sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "chat", RandomID: 92})

	if _, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "dm", 91, 0, 0); err != nil {
		t.Fatalf("1:1 resend: %v", err)
	}
	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "chat", RandomID: 92,
	}); err != nil {
		t.Fatalf("chat resend: %v", err)
	}

	for _, rec := range sink.lines(t) {
		if rec["msg"] == "dedup pts fallback" {
			t.Errorf("ordinary resend logged a fallback: %v", rec)
		}
	}
}

// TestChatDedupPtsFollowTheSendTimeRecipients pins who a chat resend answers
// for. It answers from the stored copies, so the set is the members the message
// was fanned out to: a member who joined afterwards holds no copy and gets no
// entry, and one who has since left still holds theirs. The API turns this map
// into the retry reply's Users, so it decides membership of that reply too.
func TestChatDedupPtsFollowTheSendTimeRecipients(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := open(t)
	a := mustUser(t, s, "+15559992031")
	b := mustUser(t, s, "+15559992032")
	leaver := mustUser(t, s, "+15559992033")
	joiner := mustUser(t, s, "+15559992034")
	chat := chatWith(t, s, a, b, leaver)

	_, firstPts := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "one", RandomID: 101})

	if added, _, _, err := s.AddChatUser(ctx, chat.ID, joiner.ID, a.ID); err != nil || !added {
		t.Fatalf("add joiner: added=%v err=%v", added, err)
	}
	if err := store.RemoveChatParticipant(ctx, s, chat.ID, leaver.ID); err != nil {
		t.Fatalf("remove leaver: %v", err)
	}

	_, perOwner, dup, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "one", RandomID: 101,
	})
	if err != nil || !dup {
		t.Fatalf("resend: dup=%v err=%v", dup, err)
	}
	if _, ok := perOwner[joiner.ID]; ok {
		t.Errorf("member who joined after the send got pts %d, want no entry", perOwner[joiner.ID])
	}
	for _, u := range []store.User{a, b, leaver} {
		if perOwner[u.ID] != firstPts[u.ID] {
			t.Errorf("recipient %d pts = %d, want the copy's %d", u.ID, perOwner[u.ID], firstPts[u.ID])
		}
	}
	if len(perOwner) != 3 {
		t.Errorf("resend answered %d owners, want the 3 the message reached: %v", len(perOwner), perOwner)
	}
}
