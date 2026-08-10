package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// Every dedup branch in this package answers a resend with a pts, and the pts it
// answers with is an update-delivery contract: the client applies the reply's
// update at that pts with a count of one. Report where the owner's log stands
// now instead of where the stored message sits and a client one update behind
// applies the old message into the newer update's slot, then counts itself
// caught up — the update it never saw is gone, and no gap is left for
// getDifference to find it by.
//
// Each test below advances the owner's pts past the message before resending, so
// "current pts" and "the message's pts" are different numbers and the branch has
// to name the right one.

func TestSendMessageDedupReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := open(t)
	a := mustUser(t, s, "+15559991001")
	b := mustUser(t, s, "+15559991002")

	first := send(t, s, a, b, "one", 11)
	send(t, s, a, b, "two", 12)

	stored, senderPts, recipientPts, dup, err := s.SendMessage(ctx, a.ID, b.ID, "one", 11, 0, 0)
	if err != nil || !dup {
		t.Fatalf("resend: dup=%v err=%v", dup, err)
	}
	if stored.LocalID != first.LocalID {
		t.Fatalf("resend local_id = %d, want %d", stored.LocalID, first.LocalID)
	}
	// Two sends: the first took pts 1 on both sides, the second pts 2.
	if senderPts != 1 {
		t.Errorf("sender pts = %d, want 1 (the stored message's), current is %d", senderPts, ptsOf(t, s, a.ID))
	}
	if recipientPts != 1 {
		t.Errorf("recipient pts = %d, want 1 (its own copy's), current is %d", recipientPts, ptsOf(t, s, b.ID))
	}
	// The resend still moved nothing.
	if got := ptsOf(t, s, a.ID); got != 2 {
		t.Errorf("sender state pts = %d, want 2", got)
	}
	if got := ptsOf(t, s, b.ID); got != 2 {
		t.Errorf("recipient state pts = %d, want 2", got)
	}
}

func TestSendChatMessageDedupReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := open(t)
	a := mustUser(t, s, "+15559991011")
	b := mustUser(t, s, "+15559991012")
	chat := chatWith(t, s, a, b)

	first, firstPts := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "one", RandomID: 21})
	sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "two", RandomID: 22})

	stored, perOwner, dup, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "one", RandomID: 21,
	})
	if err != nil || !dup {
		t.Fatalf("resend: dup=%v err=%v", dup, err)
	}
	if stored.LocalID != first.LocalID {
		t.Fatalf("resend local_id = %d, want %d", stored.LocalID, first.LocalID)
	}
	// Every member's copy, not just the sender's: each one is a separate pts
	// space and each is answered from its own copy.
	for _, u := range []store.User{a, b} {
		if perOwner[u.ID] != firstPts[u.ID] {
			t.Errorf("member %d pts = %d, want the stored copy's %d, current is %d",
				u.ID, perOwner[u.ID], firstPts[u.ID], ptsOf(t, s, u.ID))
		}
		if ptsOf(t, s, u.ID) != 2 {
			t.Errorf("member %d state pts = %d, want 2", u.ID, ptsOf(t, s, u.ID))
		}
	}
}

func TestPostChannelMessageDedupReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := open(t)
	author := mustUser(t, s, "+15559991021")
	ch := mustChannel(t, s, author.ID, "news").ID

	first, firstPts := post(t, s, ch, author.ID, "one", 31)
	post(t, s, ch, author.ID, "two", 32)

	stored, pts, dup, err := s.PostChannelMessage(ctx, ch, author.ID, "one", 31, nil)
	if err != nil || !dup {
		t.Fatalf("resend: dup=%v err=%v", dup, err)
	}
	if stored.LocalID != first.LocalID {
		t.Fatalf("resend local_id = %d, want %d", stored.LocalID, first.LocalID)
	}
	current, err := s.ChannelState(ctx, ch)
	if err != nil {
		t.Fatalf("channel state: %v", err)
	}
	if pts != firstPts {
		t.Errorf("resend pts = %d, want the stored post's %d, current is %d", pts, firstPts, current)
	}
	if current != 2 {
		t.Errorf("channel state pts = %d, want 2", current)
	}
}

func TestForwardMessagesDedupReportsTheStoredPts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := open(t)
	a := mustUser(t, s, "+15559991031")
	b := mustUser(t, s, "+15559991032")
	c := mustUser(t, s, "+15559991033")

	src := send(t, s, a, b, "original", 41)
	sources := []store.ForwardSource{{FromID: a.ID, Date: src.Date, Text: src.Text}}

	_, sent, err := s.ForwardMessages(ctx, a.ID, store.PeerTypeUser, c.ID, sources, []int64{42})
	if err != nil || len(sent) != 1 {
		t.Fatalf("forward: sent=%d err=%v", len(sent), err)
	}
	forwardPts := sent[0].Pts
	send(t, s, a, b, "after", 43)

	_, again, err := s.ForwardMessages(ctx, a.ID, store.PeerTypeUser, c.ID, sources, []int64{42})
	if err != nil || len(again) != 1 {
		t.Fatalf("forward retry: sent=%d err=%v", len(again), err)
	}
	if again[0].Message.LocalID != sent[0].Message.LocalID {
		t.Fatalf("retry local_id = %d, want %d", again[0].Message.LocalID, sent[0].Message.LocalID)
	}
	if again[0].Pts != forwardPts {
		t.Errorf("retry pts = %d, want the stored forward's %d, current is %d",
			again[0].Pts, forwardPts, ptsOf(t, s, a.ID))
	}
	if got := ptsOf(t, s, a.ID); got != 3 {
		t.Errorf("sender state pts = %d, want 3", got)
	}
}

// TestMessagePtsMatchesTheEventLog pins the two readers of one fact together:
// MessagePts is what a resend answers with, and the event log is what
// getDifference replays. A message that reports one pts to a retry and sits at
// another in the log is a message a client can apply twice or not at all.
func TestMessagePtsMatchesTheEventLog(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := open(t)
	a := mustUser(t, s, "+15559991041")
	b := mustUser(t, s, "+15559991042")

	for i, rid := range []int64{51, 52, 53} {
		m := send(t, s, a, b, "m", rid)
		pts, err := s.MessagePts(ctx, a.ID, m.LocalID)
		if err != nil {
			t.Fatalf("message pts %d: %v", i, err)
		}
		if pts != i+1 {
			t.Errorf("message %d pts = %d, want %d", i, pts, i+1)
		}
	}
	for _, ev := range eventsOf(t, s, a.ID, 0) {
		if ev.Type != store.EventNewMessage {
			continue
		}
		pts, err := s.MessagePts(ctx, a.ID, ev.LocalID)
		if err != nil {
			t.Fatalf("message pts for local %d: %v", ev.LocalID, err)
		}
		if pts != ev.Pts {
			t.Errorf("local %d: MessagePts %d, event log %d", ev.LocalID, pts, ev.Pts)
		}
	}
}

// TestMessagePtsRefusesAnUnknownMessage covers the branch a retry must never
// answer from the owner's current pts. There is no such state in normal
// operation — a send writes the row and the event in one transaction — so the
// unknown message here is one that was never written at all.
func TestMessagePtsRefusesAnUnknownMessage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := open(t)
	a := mustUser(t, s, "+15559991051")
	b := mustUser(t, s, "+15559991052")
	send(t, s, a, b, "one", 61)

	if _, err := s.MessagePts(ctx, a.ID, 99); !errors.Is(err, store.ErrPtsUnknown) {
		t.Errorf("MessagePts on an absent message: err = %v, want ErrPtsUnknown", err)
	}
	if _, err := s.ChannelPostPts(ctx, 12345, 1); !errors.Is(err, store.ErrPtsUnknown) {
		t.Errorf("ChannelPostPts on an absent post: err = %v, want ErrPtsUnknown", err)
	}
}
