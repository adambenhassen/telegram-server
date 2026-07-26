package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func chatWith(t *testing.T, s *store.Store, creator store.User, members ...store.User) store.Chat {
	t.Helper()
	ids := make([]int64, len(members))
	for i, m := range members {
		ids[i] = m.ID
	}
	c, err := s.CreateChat(context.Background(), creator.ID, "Group", ids)
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	return c
}

func sendChat(t *testing.T, s *store.Store, f store.FanOut) (store.Message, map[int64]int) {
	t.Helper()
	m, perOwner, dup, err := s.SendChatMessage(context.Background(), f)
	if err != nil {
		t.Fatalf("send chat message: %v", err)
	}
	if dup {
		t.Fatal("first send flagged dup")
	}
	return m, perOwner
}

func msgOpt(t *testing.T, s *store.Store, ownerID, localID int64) (store.Message, bool) {
	t.Helper()
	m, ok, err := s.MessageByOwnerLocal(context.Background(), ownerID, localID)
	if err != nil {
		t.Fatalf("message (%d,%d): %v", ownerID, localID, err)
	}
	return m, ok
}

func ptsOf(t *testing.T, s *store.Store, userID int64) int {
	t.Helper()
	st, err := s.State(context.Background(), userID)
	if err != nil {
		t.Fatalf("state %d: %v", userID, err)
	}
	return st.Pts
}

func eventsOf(t *testing.T, s *store.Store, userID int64, fromPts int) []store.Event {
	t.Helper()
	ev, err := s.EventsSince(context.Background(), userID, fromPts)
	if err != nil {
		t.Fatalf("events %d: %v", userID, err)
	}
	return ev
}

func dialogOf(t *testing.T, s *store.Store, ownerID, chatID int64) store.Dialog {
	t.Helper()
	ds, err := s.Dialogs(context.Background(), ownerID)
	if err != nil {
		t.Fatalf("dialogs %d: %v", ownerID, err)
	}
	for _, d := range ds {
		if d.PeerType == store.PeerTypeChat && d.PeerID == chatID {
			return d
		}
	}
	t.Fatalf("owner %d has no dialog with chat %d: %+v", ownerID, chatID, ds)
	return store.Dialog{}
}

func TestSendChatMessageFansOutToEveryMember(t *testing.T) {
	t.Parallel()
	s := open(t)
	a := mustUser(t, s, "+15551290001")
	b := mustUser(t, s, "+15551290002")
	c := mustUser(t, s, "+15551290003")
	chat := chatWith(t, s, a, b, c)

	sender, perOwner := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "hi", RandomID: 77})

	if len(perOwner) != 3 {
		t.Fatalf("perOwner = %+v, want 3 entries", perOwner)
	}
	if sender.FanoutID == 0 {
		t.Fatal("sender copy carries the fanout_id = 0 sentinel")
	}
	if !sender.Out || sender.OwnerID != a.ID || sender.FromID != a.ID {
		t.Fatalf("sender copy wrong: %+v", sender)
	}
	if sender.PeerType != store.PeerTypeChat || sender.PeerID != chat.ID {
		t.Fatalf("sender copy peer = %d/%d, want chat %d", sender.PeerType, sender.PeerID, chat.ID)
	}
	if sender.RandomID != 77 || sender.PeerLocalID != 0 {
		t.Fatalf("sender copy random_id=%d peer_local_id=%d, want 77/0", sender.RandomID, sender.PeerLocalID)
	}

	for _, u := range []store.User{a, b, c} {
		if perOwner[u.ID] != 1 {
			t.Errorf("owner %d pts = %d, want 1", u.ID, perOwner[u.ID])
		}
		if got := ptsOf(t, s, u.ID); got != 1 {
			t.Errorf("owner %d stored pts = %d, want 1", u.ID, got)
		}

		// Every owner's copy is the first message in their own id space.
		m, ok := msgOpt(t, s, u.ID, 1)
		if !ok {
			t.Fatalf("owner %d got no message row", u.ID)
		}
		if m.FanoutID != sender.FanoutID {
			t.Errorf("owner %d fanout_id = %d, want shared %d", u.ID, m.FanoutID, sender.FanoutID)
		}
		if m.PeerType != store.PeerTypeChat || m.PeerID != chat.ID || m.FromID != a.ID || m.Text != "hi" {
			t.Errorf("owner %d copy wrong: %+v", u.ID, m)
		}
		if m.Out != (u.ID == a.ID) {
			t.Errorf("owner %d out = %v, want %v", u.ID, m.Out, u.ID == a.ID)
		}
		// The dedup token lives on the sender's copy only.
		wantRandom := int64(0)
		if u.ID == a.ID {
			wantRandom = 77
		}
		if m.RandomID != wantRandom {
			t.Errorf("owner %d random_id = %d, want %d", u.ID, m.RandomID, wantRandom)
		}

		ev := eventsOf(t, s, u.ID, 0)
		if len(ev) != 1 || ev[0].Type != store.EventNewMessage || ev[0].LocalID != m.LocalID {
			t.Errorf("owner %d events = %+v, want one new-message for local %d", u.ID, ev, m.LocalID)
		}

		d := dialogOf(t, s, u.ID, chat.ID)
		wantUnread := 1
		if u.ID == a.ID {
			wantUnread = 0
		}
		if d.UnreadCount != wantUnread || d.TopMessage != m.LocalID {
			t.Errorf("owner %d dialog = %+v, want unread %d top %d", u.ID, d, wantUnread, m.LocalID)
		}
	}
}

func TestSendChatMessageRandomIDDedup(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290011")
	b := mustUser(t, s, "+15551290012")
	c := mustUser(t, s, "+15551290013")
	chat := chatWith(t, s, a, b, c)

	first, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "once", RandomID: 4242})

	again, perOwner, dup, err := s.SendChatMessage(ctx, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "once", RandomID: 4242})
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	if !dup {
		t.Fatal("resend with the same random_id not flagged dup")
	}
	if again.LocalID != first.LocalID || again.FanoutID != first.FanoutID {
		t.Fatalf("dup returned %+v, want the original %+v", again, first)
	}

	for _, u := range []store.User{a, b, c} {
		if perOwner[u.ID] != 1 {
			t.Errorf("owner %d dup pts = %d, want unchanged 1", u.ID, perOwner[u.ID])
		}
		if got := ptsOf(t, s, u.ID); got != 1 {
			t.Errorf("owner %d stored pts = %d, want unchanged 1", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 0); len(ev) != 1 {
			t.Errorf("owner %d events = %+v, want still one", u.ID, ev)
		}
		if _, ok := msgOpt(t, s, u.ID, 2); ok {
			t.Errorf("owner %d gained a second message row from a dedup'd resend", u.ID)
		}
	}
}

func TestEditChatMessageEveryCopy(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290021")
	b := mustUser(t, s, "+15551290022")
	c := mustUser(t, s, "+15551290023")
	chat := chatWith(t, s, a, b, c)

	sender, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "orig", RandomID: 1})

	peerID, newPts, err := s.EditMessage(ctx, a.ID, sender.LocalID, "edited")
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if peerID != chat.ID {
		t.Fatalf("edit peer = %d, want chat %d", peerID, chat.ID)
	}
	if newPts != 2 {
		t.Fatalf("editor pts = %d, want 2", newPts)
	}

	for _, u := range []store.User{a, b, c} {
		m, ok := msgOpt(t, s, u.ID, 1)
		if !ok {
			t.Fatalf("owner %d lost its copy", u.ID)
		}
		if m.Text != "edited" || m.EditDate == nil {
			t.Errorf("owner %d copy not edited: %+v", u.ID, m)
		}
		if got := ptsOf(t, s, u.ID); got != 2 {
			t.Errorf("owner %d pts = %d, want 2", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 1); len(ev) != 1 || ev[0].Type != store.EventEdit {
			t.Errorf("owner %d edit event = %+v", u.ID, ev)
		}
	}

	// Only the author may edit: another member's copy is inbound.
	if _, _, err := s.EditMessage(ctx, b.ID, 1, "hijack"); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("member editing someone else's message: want ErrMessageInvalid, got %v", err)
	}
}

func TestDeleteChatMessageEveryCopy(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290031")
	b := mustUser(t, s, "+15551290032")
	c := mustUser(t, s, "+15551290033")
	chat := chatWith(t, s, a, b, c)

	sender, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "bye", RandomID: 1})

	perOwner, err := s.DeleteMessages(ctx, a.ID, []int64{sender.LocalID})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(perOwner) != 3 {
		t.Fatalf("delete perOwner = %+v, want 3 entries", perOwner)
	}
	for _, u := range []store.User{a, b, c} {
		if perOwner[u.ID] != 2 {
			t.Errorf("owner %d delete pts = %d, want 2", u.ID, perOwner[u.ID])
		}
		m, ok := msgOpt(t, s, u.ID, 1)
		if !ok || !m.Deleted {
			t.Errorf("owner %d copy not deleted: ok=%v %+v", u.ID, ok, m)
		}
		if ev := eventsOf(t, s, u.ID, 1); len(ev) != 1 || ev[0].Type != store.EventDelete {
			t.Errorf("owner %d delete event = %+v", u.ID, ev)
		}
	}
}

// TestDeleteChatMessageIsAuthorOnly matches the delete path to the edit path. A
// chat delete walks the same copy set an edit does, so a member deleting someone
// else's message would destroy it for every member, and the edit path's
// service-message guard would buy nothing if the announcement stayed deletable.
func TestDeleteChatMessageIsAuthorOnly(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290121")
	b := mustUser(t, s, "+15551290122")
	c := mustUser(t, s, "+15551290123")
	chat := chatWith(t, s, a, b, c)

	text, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "keep", RandomID: 1})
	service, _ := sendChat(t, s, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "New title", Action: store.ChatActionEditTitle,
	})

	// b holds an inbound copy of a's message: same fan-out, not b's to destroy.
	if _, err := s.DeleteMessages(ctx, b.ID, []int64{1}); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("member deleting another member's chat message: want ErrMessageInvalid, got %v", err)
	}
	// a triggered the title change, so its copy is outgoing — still undeletable.
	if _, err := s.DeleteMessages(ctx, a.ID, []int64{service.LocalID}); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("deleting a service message: want ErrMessageInvalid, got %v", err)
	}

	for _, u := range []store.User{a, b, c} {
		for _, local := range []int64{1, 2} {
			if m, ok := msgOpt(t, s, u.ID, local); !ok || m.Deleted {
				t.Errorf("owner %d local %d moved: ok=%v %+v", u.ID, local, ok, m)
			}
		}
		if got := ptsOf(t, s, u.ID); got != 2 {
			t.Errorf("owner %d pts = %d, want unchanged 2", u.ID, got)
		}
	}

	// The author still deletes their own text message for everyone.
	perOwner, err := s.DeleteMessages(ctx, a.ID, []int64{text.LocalID})
	if err != nil {
		t.Fatalf("author delete: %v", err)
	}
	if len(perOwner) != 3 {
		t.Fatalf("perOwner = %+v, want 3 entries", perOwner)
	}
	for _, u := range []store.User{a, b, c} {
		if m, ok := msgOpt(t, s, u.ID, 1); !ok || !m.Deleted {
			t.Errorf("owner %d copy not deleted: ok=%v %+v", u.ID, ok, m)
		}
	}
}

// TestRemovedMemberCannotEditOrDeleteChatMessage is the F1 control: editMessage
// and deleteMessages take no peer, so a user removed from a chat still holds a
// lever on every current member's rows through their own retained copy.
func TestRemovedMemberCannotEditOrDeleteChatMessage(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290041")
	b := mustUser(t, s, "+15551290042")
	c := mustUser(t, s, "+15551290043")
	chat := chatWith(t, s, a, b, c)

	sender, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "still here", RandomID: 1})
	if err := store.RemoveChatParticipant(ctx, s, chat.ID, a.ID); err != nil {
		t.Fatalf("remove participant: %v", err)
	}

	if _, _, err := s.EditMessage(ctx, a.ID, sender.LocalID, "hijacked"); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("removed member edit: want ErrMessageInvalid, got %v", err)
	}
	if _, err := s.DeleteMessages(ctx, a.ID, []int64{sender.LocalID}); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("removed member delete: want ErrMessageInvalid, got %v", err)
	}

	for _, u := range []store.User{a, b, c} {
		m, ok := msgOpt(t, s, u.ID, 1)
		if !ok {
			t.Fatalf("owner %d lost its copy", u.ID)
		}
		if m.Text != "still here" || m.Deleted || m.EditDate != nil {
			t.Errorf("owner %d copy moved: %+v", u.ID, m)
		}
		if got := ptsOf(t, s, u.ID); got != 1 {
			t.Errorf("owner %d pts = %d, want unchanged 1", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 0); len(ev) != 1 {
			t.Errorf("owner %d events = %+v, want still one", u.ID, ev)
		}
	}
}

// TestChatWriteSkipsMemberRemovedAfterSend is the other direction of F3: the
// removed member keeps the copy they already had, so a later edit or delete must
// stop at the current member set. New text reaching their row would be content
// delivered to a non-member, and the delete would push an event at an account the
// server has stopped talking to.
func TestChatWriteSkipsMemberRemovedAfterSend(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290111")
	b := mustUser(t, s, "+15551290112")
	c := mustUser(t, s, "+15551290113")
	chat := chatWith(t, s, a, b, c)

	sender, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "orig", RandomID: 1})
	if err := store.RemoveChatParticipant(ctx, s, chat.ID, c.ID); err != nil {
		t.Fatalf("remove participant: %v", err)
	}

	assertFrozen := func(when string) {
		t.Helper()
		m, ok := msgOpt(t, s, c.ID, 1)
		if !ok {
			t.Fatalf("%s: removed member lost its copy", when)
		}
		if m.Text != "orig" || m.Deleted || m.EditDate != nil {
			t.Fatalf("%s: removed member's copy moved: %+v", when, m)
		}
		if got := ptsOf(t, s, c.ID); got != 1 {
			t.Fatalf("%s: removed member pts = %d, want unchanged 1", when, got)
		}
		if ev := eventsOf(t, s, c.ID, 0); len(ev) != 1 || ev[0].Type != store.EventNewMessage {
			t.Fatalf("%s: removed member events = %+v, want only the original send", when, ev)
		}
	}

	_, newPts, err := s.EditMessage(ctx, a.ID, sender.LocalID, "edited")
	if err != nil {
		t.Fatalf("edit: %v", err)
	}
	if newPts != 2 {
		t.Fatalf("editor pts = %d, want 2", newPts)
	}
	assertFrozen("after the edit")
	for _, u := range []store.User{a, b} {
		m, ok := msgOpt(t, s, u.ID, 1)
		if !ok || m.Text != "edited" {
			t.Errorf("remaining member %d not edited: ok=%v %+v", u.ID, ok, m)
		}
		if got := ptsOf(t, s, u.ID); got != 2 {
			t.Errorf("remaining member %d pts = %d, want 2", u.ID, got)
		}
	}

	perOwner, err := s.DeleteMessages(ctx, a.ID, []int64{sender.LocalID})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if len(perOwner) != 2 || perOwner[a.ID] != 3 || perOwner[b.ID] != 3 {
		t.Fatalf("perOwner = %+v, want only a and b at pts 3", perOwner)
	}
	assertFrozen("after the delete")
	for _, u := range []store.User{a, b} {
		if m, ok := msgOpt(t, s, u.ID, 1); !ok || !m.Deleted {
			t.Errorf("remaining member %d copy not deleted: ok=%v %+v", u.ID, ok, m)
		}
	}
}

// TestChatWriteNeverTouchesOneToOneRows is the F2 control: fanout_id = 0 is the
// sentinel every 1:1 row carries, so a chat-peer row holding it must be rejected
// rather than walked as a fan-out — walking it would reach the whole 1:1 table.
func TestChatWriteNeverTouchesOneToOneRows(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290051")
	b := mustUser(t, s, "+15551290052")

	private := send(t, s, a, b, "private", 1) // 1:1, fanout_id 0 on both sides
	chat := chatWith(t, s, a, b)
	group, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "group", RandomID: 2})

	bogus, err := store.InsertChatMessageNoFanout(ctx, s, a.ID, chat.ID, "bogus")
	if err != nil {
		t.Fatalf("insert zero-fanout row: %v", err)
	}

	assertPrivateIntact := func(when string) {
		t.Helper()
		own, ok := msgOpt(t, s, a.ID, private.LocalID)
		if !ok || own.Text != "private" || own.Deleted || own.EditDate != nil {
			t.Fatalf("%s: sender 1:1 row moved: ok=%v %+v", when, ok, own)
		}
		mirror, ok := msgOpt(t, s, b.ID, private.PeerLocalID)
		if !ok || mirror.Text != "private" || mirror.Deleted || mirror.EditDate != nil {
			t.Fatalf("%s: recipient 1:1 row moved: ok=%v %+v", when, ok, mirror)
		}
	}

	beforeA, beforeB := ptsOf(t, s, a.ID), ptsOf(t, s, b.ID)
	if _, _, err := s.EditMessage(ctx, a.ID, bogus, "rewrite everything"); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("edit zero-fanout chat row: want ErrMessageInvalid, got %v", err)
	}
	if _, err := s.DeleteMessages(ctx, a.ID, []int64{bogus}); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("delete zero-fanout chat row: want ErrMessageInvalid, got %v", err)
	}
	assertPrivateIntact("after rejected zero-fanout writes")
	if got := ptsOf(t, s, a.ID); got != beforeA {
		t.Errorf("rejected write moved a's pts: %d, want %d", got, beforeA)
	}
	if got := ptsOf(t, s, b.ID); got != beforeB {
		t.Errorf("rejected write moved b's pts: %d, want %d", got, beforeB)
	}

	// A well-formed chat edit and delete stay inside their own fan-out.
	if _, _, err := s.EditMessage(ctx, a.ID, group.LocalID, "edited"); err != nil {
		t.Fatalf("edit chat message: %v", err)
	}
	assertPrivateIntact("after a chat edit")
	if _, err := s.DeleteMessages(ctx, a.ID, []int64{group.LocalID}); err != nil {
		t.Fatalf("delete chat message: %v", err)
	}
	assertPrivateIntact("after a chat delete")
}

// TestSendChatMessageSkipsMemberRemovedBeforeSend is F3 in its deterministic
// form: a removal that has committed must leave the removed user out of every
// later fan-out — no row, no event, no pts bump.
func TestSendChatMessageSkipsMemberRemovedBeforeSend(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290061")
	b := mustUser(t, s, "+15551290062")
	c := mustUser(t, s, "+15551290063")
	chat := chatWith(t, s, a, b, c)

	if err := store.RemoveChatParticipant(ctx, s, chat.ID, c.ID); err != nil {
		t.Fatalf("remove participant: %v", err)
	}
	_, perOwner := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "after you left", RandomID: 1})

	if len(perOwner) != 2 || perOwner[a.ID] != 1 || perOwner[b.ID] != 1 {
		t.Fatalf("perOwner = %+v, want only a and b at pts 1", perOwner)
	}
	if _, ok := msgOpt(t, s, c.ID, 1); ok {
		t.Fatal("removed member received a message row")
	}
	if got := ptsOf(t, s, c.ID); got != 0 {
		t.Errorf("removed member pts = %d, want 0", got)
	}
	if ev := eventsOf(t, s, c.ID, 0); len(ev) != 0 {
		t.Errorf("removed member events = %+v, want none", ev)
	}
}

// TestConcurrentChatSendAndRemoval races a fan-out against a removal on the same
// chat. Either order is allowed; what is not is a torn result — a member with a
// message row but no event, an event with no row, or a pts that disagrees with
// either.
func TestConcurrentChatSendAndRemoval(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	for i := range 10 {
		a := mustUser(t, s, fmt.Sprintf("+1555129%04d1", i))
		b := mustUser(t, s, fmt.Sprintf("+1555129%04d2", i))
		c := mustUser(t, s, fmt.Sprintf("+1555129%04d3", i))
		chat := chatWith(t, s, a, b, c)

		var wg sync.WaitGroup
		var sendErr, removeErr error
		wg.Go(func() {
			_, _, _, sendErr = s.SendChatMessage(ctx, store.FanOut{
				ChatID: chat.ID, FromID: a.ID, Text: "racing", RandomID: int64(i + 1),
			})
		})
		wg.Go(func() { removeErr = store.RemoveChatParticipant(ctx, s, chat.ID, c.ID) })
		wg.Wait()
		if sendErr != nil {
			t.Fatalf("round %d send: %v", i, sendErr)
		}
		if removeErr != nil {
			t.Fatalf("round %d remove: %v", i, removeErr)
		}

		// The sender is never the one removed, so it always gets its copy.
		if _, ok := msgOpt(t, s, a.ID, 1); !ok {
			t.Fatalf("round %d: sender got no row", i)
		}
		for _, u := range []store.User{a, b, c} {
			_, hasRow := msgOpt(t, s, u.ID, 1)
			ev := eventsOf(t, s, u.ID, 0)
			if hasRow != (len(ev) == 1) {
				t.Fatalf("round %d owner %d: row=%v events=%+v", i, u.ID, hasRow, ev)
			}
			if got := ptsOf(t, s, u.ID); got != len(ev) {
				t.Fatalf("round %d owner %d: pts %d disagrees with %d events", i, u.ID, got, len(ev))
			}
		}
	}
}

// TestSendChatMessageRejectsNonMemberSender is the text half of F4: the handler's
// membership check runs in a different transaction, so the write re-checks.
func TestSendChatMessageRejectsNonMemberSender(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290071")
	b := mustUser(t, s, "+15551290072")
	outsider := mustUser(t, s, "+15551290073")
	chat := chatWith(t, s, a, b)

	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: outsider.ID, Text: "let me in", RandomID: 1,
	}); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("outsider send: want ErrNotMember, got %v", err)
	}
	// An unknown chat is indistinguishable from one the sender is not in.
	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID + 10_000, FromID: a.ID, Text: "nowhere", RandomID: 2,
	}); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("unknown chat send: want ErrNotMember, got %v", err)
	}

	if err := store.RemoveChatParticipant(ctx, s, chat.ID, a.ID); err != nil {
		t.Fatalf("remove participant: %v", err)
	}
	if _, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "one more", RandomID: 3,
	}); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("removed member send: want ErrNotMember, got %v", err)
	}

	for _, u := range []store.User{a, b, outsider} {
		if got := ptsOf(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0 after rejected sends", u.ID, got)
		}
		if _, ok := msgOpt(t, s, u.ID, 1); ok {
			t.Errorf("owner %d gained a row from a rejected send", u.ID)
		}
	}
}

// TestSendChatMessageRejectsNonMemberBeforeTheRowLock is the send half of the
// same invariant the chat mutations carry: a non-member is turned away without
// taking the chats row lock, so it cannot serialise the members' writes behind
// itself or time the wait to learn whether the chat exists. The lock is held by
// another transaction for the length of the call, so a path that reaches for it
// fails on the context instead of returning ErrNotMember.
func TestSendChatMessageRejectsNonMemberBeforeTheRowLock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290091")
	b := mustUser(t, s, "+15551290092")
	outsider := mustUser(t, s, "+15551290093")
	chat := chatWith(t, s, a, b)

	release, err := store.HoldChatRowLock(ctx, s, chat.ID)
	if err != nil {
		t.Fatalf("hold chat row lock: %v", err)
	}
	defer release()

	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, _, _, err := s.SendChatMessage(callCtx, store.FanOut{
		ChatID: chat.ID, FromID: outsider.ID, Text: "let me in", RandomID: 1,
	}); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("outsider send under a held chats row lock: want ErrNotMember, got %v", err)
	}
}

// TestServiceMessageReachesRemovedSubject is the exception half of F4 plus F8: a
// removal announcement is written by a sender who is already out, and it must
// reach the removed user through Extra without letting Extra hand anyone two rows.
func TestServiceMessageReachesRemovedSubject(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290081")
	b := mustUser(t, s, "+15551290082")
	c := mustUser(t, s, "+15551290083")
	chat := chatWith(t, s, a, b, c)

	if err := store.RemoveChatParticipant(ctx, s, chat.ID, c.ID); err != nil {
		t.Fatalf("remove participant: %v", err)
	}
	// c announces its own removal: not a member any more, and Extra repeats both
	// itself and a member already in the set.
	_, perOwner := sendChat(t, s, store.FanOut{
		ChatID: chat.ID, FromID: c.ID, Action: store.ChatActionDeleteUser,
		ActionUserID: c.ID, Extra: []int64{c.ID, c.ID, a.ID},
	})

	if len(perOwner) != 3 {
		t.Fatalf("perOwner = %+v, want a, b and the removed c", perOwner)
	}
	for _, u := range []store.User{a, b, c} {
		if perOwner[u.ID] != 1 {
			t.Errorf("owner %d pts = %d, want 1", u.ID, perOwner[u.ID])
		}
		m, ok := msgOpt(t, s, u.ID, 1)
		if !ok {
			t.Fatalf("owner %d got no service message", u.ID)
		}
		if m.Action != store.ChatActionDeleteUser || m.ActionUserID != c.ID || m.FromID != c.ID {
			t.Errorf("owner %d service row wrong: %+v", u.ID, m)
		}
		if m.Out != (u.ID == c.ID) {
			t.Errorf("owner %d out = %v, want %v", u.ID, m.Out, u.ID == c.ID)
		}
		// Deduped against the member set: exactly one row from this fan-out.
		if _, ok := msgOpt(t, s, u.ID, 2); ok {
			t.Errorf("owner %d took two rows from one fan-out", u.ID)
		}
	}
}

// TestEditRejectsChatServiceMessage keeps a service message immutable: its
// sender's copy is outgoing and its sender is usually still a member, so nothing
// else would stop them rewriting the announcement in everyone's history.
func TestEditRejectsChatServiceMessage(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290091")
	b := mustUser(t, s, "+15551290092")
	chat := chatWith(t, s, a, b)

	service, _ := sendChat(t, s, store.FanOut{
		ChatID: chat.ID, FromID: a.ID, Text: "New title", Action: store.ChatActionEditTitle,
	})
	if !service.Out {
		t.Fatalf("service message not outgoing for its sender: %+v", service)
	}

	if _, _, err := s.EditMessage(ctx, a.ID, service.LocalID, "Something else"); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("edit service message: want ErrMessageInvalid, got %v", err)
	}
	for _, u := range []store.User{a, b} {
		m, ok := msgOpt(t, s, u.ID, 1)
		if !ok || m.Text != "New title" || m.EditDate != nil {
			t.Errorf("owner %d service row moved: ok=%v %+v", u.ID, ok, m)
		}
	}
}

// TestDeleteMessagesAcrossChatAndOneToOne exercises one batch spanning both peer
// types: the chat target takes its whole fan-out with it, the 1:1 target takes
// only its mirror, and a missing id still fails the whole batch.
func TestDeleteMessagesAcrossChatAndOneToOne(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551290101")
	b := mustUser(t, s, "+15551290102")
	c := mustUser(t, s, "+15551290103")
	chat := chatWith(t, s, a, b, c)

	private := send(t, s, a, b, "private", 1)
	group, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: a.ID, Text: "group", RandomID: 2})

	// Each member holds the chat copy at its own local id; history is scoped by
	// peer type, so this is also where a 1:1 row would show up if it leaked in.
	chatCopy := map[int64]int64{}
	for _, u := range []store.User{a, b, c} {
		h, err := s.History(ctx, u.ID, store.PeerTypeChat, chat.ID, 0, 10)
		if err != nil {
			t.Fatalf("chat history %d: %v", u.ID, err)
		}
		if len(h) != 1 || h[0].Text != "group" {
			t.Fatalf("owner %d chat history = %+v, want one group message", u.ID, h)
		}
		chatCopy[u.ID] = h[0].LocalID
	}

	if _, err := s.DeleteMessages(ctx, a.ID, []int64{private.LocalID, group.LocalID, 999}); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("batch with a missing id: want ErrMessageInvalid, got %v", err)
	}
	if m, ok := msgOpt(t, s, a.ID, private.LocalID); !ok || m.Deleted {
		t.Fatal("failed batch deleted the 1:1 row anyway")
	}

	perOwner, err := s.DeleteMessages(ctx, a.ID, []int64{private.LocalID, group.LocalID})
	if err != nil {
		t.Fatalf("mixed batch delete: %v", err)
	}
	if len(perOwner) != 3 {
		t.Fatalf("perOwner = %+v, want a, b and c", perOwner)
	}
	for _, u := range []store.User{a, b, c} {
		m, ok := msgOpt(t, s, u.ID, chatCopy[u.ID])
		if !ok || !m.Deleted {
			t.Errorf("owner %d chat copy not deleted: ok=%v %+v", u.ID, ok, m)
		}
	}
	if m, ok := msgOpt(t, s, b.ID, private.PeerLocalID); !ok || !m.Deleted {
		t.Errorf("1:1 mirror not deleted: ok=%v %+v", ok, m)
	}
}
