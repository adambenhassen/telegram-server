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

func participantIDs(t *testing.T, s *store.Store, chatID int64) []int64 {
	t.Helper()
	ps, err := s.Participants(context.Background(), chatID)
	if err != nil {
		t.Fatalf("participants %d: %v", chatID, err)
	}
	ids := make([]int64, len(ps))
	for i, p := range ps {
		ids[i] = p.UserID
	}
	return ids
}

func chatVersion(t *testing.T, s *store.Store, chatID int64) int {
	t.Helper()
	c, ok, err := s.ChatByID(context.Background(), chatID)
	if err != nil || !ok {
		t.Fatalf("chat %d: ok=%v err=%v", chatID, ok, err)
	}
	return c.Version
}

// assertService checks one owner's copy of a service message: same fan-out, the
// expected action, and the subject it names.
func assertService(t *testing.T, s *store.Store, ownerID, localID int64, fanoutID int64, action store.ChatAction, subject int64) {
	t.Helper()
	m, ok := msgOpt(t, s, ownerID, localID)
	if !ok {
		t.Fatalf("owner %d got no service message copy", ownerID)
	}
	if m.Action != action || m.ActionUserID != subject {
		t.Errorf("owner %d copy = action %d subject %d, want %d/%d", ownerID, m.Action, m.ActionUserID, action, subject)
	}
	if m.FanoutID != fanoutID {
		t.Errorf("owner %d fanout_id = %d, want shared %d", ownerID, m.FanoutID, fanoutID)
	}
}

func TestAddChatUserAnnouncesToEveryMemberIncludingTheNewOne(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551271001")
	b := mustUser(t, s, "+15551271002")
	c := mustUser(t, s, "+15551271003")
	chat := chatWith(t, s, a, b)

	added, sender, perOwner, err := s.AddChatUser(ctx, chat.ID, c.ID, a.ID)
	if err != nil || !added {
		t.Fatalf("add: added=%v err=%v", added, err)
	}
	if len(perOwner) != 3 {
		t.Fatalf("perOwner = %+v, want 3 entries", perOwner)
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 3 {
		t.Fatalf("participants = %v, want 3", got)
	}
	if v := chatVersion(t, s, chat.ID); v != chat.Version+1 {
		t.Errorf("version = %d, want %d", v, chat.Version+1)
	}
	if sender.Action != store.ChatActionAddUser || sender.ActionUserID != c.ID {
		t.Fatalf("sender copy = %+v, want AddUser on %d", sender, c.ID)
	}
	for _, u := range []store.User{a, b, c} {
		if perOwner[u.ID] != 1 {
			t.Errorf("owner %d pts = %d, want 1", u.ID, perOwner[u.ID])
		}
		if got := ptsOf(t, s, u.ID); got != 1 {
			t.Errorf("owner %d stored pts = %d, want 1", u.ID, got)
		}
		assertService(t, s, u.ID, 1, sender.FanoutID, store.ChatActionAddUser, c.ID)
	}
	// The new member's own copy is what puts the chat in their dialog list.
	if d := dialogOf(t, s, c.ID, chat.ID); d.TopMessage != 1 {
		t.Errorf("new member dialog top = %d, want 1", d.TopMessage)
	}
}

func TestAddChatUserAlreadyMemberChangesNothing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551271011")
	b := mustUser(t, s, "+15551271012")
	chat := chatWith(t, s, a, b)

	added, _, perOwner, err := s.AddChatUser(ctx, chat.ID, b.ID, a.ID)
	if err != nil {
		t.Fatalf("re-add: %v", err)
	}
	if added || perOwner != nil {
		t.Fatalf("re-add: added=%v perOwner=%+v, want false/nil", added, perOwner)
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 2 {
		t.Errorf("participants = %v, want 2", got)
	}
	if v := chatVersion(t, s, chat.ID); v != chat.Version {
		t.Errorf("version = %d, want unchanged %d", v, chat.Version)
	}
	for _, u := range []store.User{a, b} {
		if got := ptsOf(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 0); len(ev) != 0 {
			t.Errorf("owner %d events = %+v, want none", u.ID, ev)
		}
	}
}

func TestAddChatUserAtCapRejectsAndWritesNothing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551272000")
	members := make([]int64, 0, 199)
	for i := range 199 {
		members = append(members, mustUser(t, s, fmt.Sprintf("+1555129%05d", i)).ID)
	}
	chat, err := s.CreateChat(ctx, a.ID, "Full", members)
	if err != nil {
		t.Fatalf("create full chat: %v", err)
	}
	outsider := mustUser(t, s, "+15551272999")

	if _, _, _, err := s.AddChatUser(ctx, chat.ID, outsider.ID, a.ID); !errors.Is(err, store.ErrChatFull) {
		t.Fatalf("add at cap: want ErrChatFull, got %v", err)
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 200 {
		t.Fatalf("participants = %d, want unchanged 200", len(got))
	}
	if v := chatVersion(t, s, chat.ID); v != chat.Version {
		t.Errorf("version = %d, want unchanged %d", v, chat.Version)
	}
	if got := ptsOf(t, s, a.ID); got != 0 {
		t.Errorf("caller pts = %d, want 0", got)
	}
}

func TestRemoveChatUserAnnouncesToTheRemovedUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551273001")
	b := mustUser(t, s, "+15551273002")
	c := mustUser(t, s, "+15551273003")
	chat := chatWith(t, s, a, b, c)

	// c speaks before being removed: their existing copies must survive intact.
	sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: c.ID, Text: "bye", RandomID: 5})

	removed, sender, perOwner, err := s.RemoveChatUser(ctx, chat.ID, c.ID, a.ID)
	if err != nil || !removed {
		t.Fatalf("remove: removed=%v err=%v", removed, err)
	}
	if len(perOwner) != 3 {
		t.Fatalf("perOwner = %+v, want the two remaining members and the removed user", perOwner)
	}
	got := participantIDs(t, s, chat.ID)
	if len(got) != 2 {
		t.Fatalf("participants = %v, want 2", got)
	}
	for _, id := range got {
		if id == c.ID {
			t.Fatalf("removed user still a participant: %v", got)
		}
	}
	if v := chatVersion(t, s, chat.ID); v != chat.Version+1 {
		t.Errorf("version = %d, want %d", v, chat.Version+1)
	}
	for _, u := range []store.User{a, b, c} {
		if perOwner[u.ID] != 2 {
			t.Errorf("owner %d pts = %d, want 2", u.ID, perOwner[u.ID])
		}
		assertService(t, s, u.ID, 2, sender.FanoutID, store.ChatActionDeleteUser, c.ID)
	}
	// The removed user's earlier message is untouched, not deleted.
	m, ok := msgOpt(t, s, c.ID, 1)
	if !ok || m.Deleted || m.Text != "bye" {
		t.Fatalf("removed user's earlier row = ok:%v %+v", ok, m)
	}
}

func TestRemoveChatUserNotAMemberChangesNothing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551273011")
	b := mustUser(t, s, "+15551273012")
	outsider := mustUser(t, s, "+15551273013")
	chat := chatWith(t, s, a, b)

	removed, _, perOwner, err := s.RemoveChatUser(ctx, chat.ID, outsider.ID, a.ID)
	if err != nil {
		t.Fatalf("remove non-member: %v", err)
	}
	if removed || perOwner != nil {
		t.Fatalf("remove non-member: removed=%v perOwner=%+v, want false/nil", removed, perOwner)
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 2 {
		t.Errorf("participants = %v, want 2", got)
	}
	if v := chatVersion(t, s, chat.ID); v != chat.Version {
		t.Errorf("version = %d, want unchanged %d", v, chat.Version)
	}
	for _, u := range []store.User{a, b, outsider} {
		if got := ptsOf(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0", u.ID, got)
		}
	}
}

func TestRemoveChatUserRequiresCreatorForOtherMembers(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551273101")
	member := mustUser(t, s, "+15551273102")
	target := mustUser(t, s, "+15551273103")
	chat := chatWith(t, s, creator, member, target)

	removed, _, perOwner, err := s.RemoveChatUser(ctx, chat.ID, target.ID, member.ID)
	if !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("member removing another member: err = %v, want ErrNotMember", err)
	}
	if removed || perOwner != nil {
		t.Fatalf("rejected removal: removed=%v perOwner=%+v, want false/nil", removed, perOwner)
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 3 {
		t.Fatalf("participants = %v, want unchanged 3", got)
	}
	if v := chatVersion(t, s, chat.ID); v != chat.Version {
		t.Errorf("version = %d, want unchanged %d", v, chat.Version)
	}
	for _, u := range []store.User{creator, member, target} {
		if got := ptsOf(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 0); len(ev) != 0 {
			t.Errorf("owner %d events = %+v, want none", u.ID, ev)
		}
	}
}

func TestRemoveChatUserCannotRemoveCreator(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551273201")
	member := mustUser(t, s, "+15551273202")
	chat := chatWith(t, s, creator, member)

	for _, caller := range []store.User{creator, member} {
		removed, _, perOwner, err := s.RemoveChatUser(ctx, chat.ID, creator.ID, caller.ID)
		if !errors.Is(err, store.ErrNotMember) {
			t.Errorf("caller %d removing creator: err = %v, want ErrNotMember", caller.ID, err)
		}
		if removed || perOwner != nil {
			t.Errorf("caller %d rejected removal: removed=%v perOwner=%+v, want false/nil", caller.ID, removed, perOwner)
		}
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 2 {
		t.Fatalf("participants = %v, want unchanged 2", got)
	}
	if v := chatVersion(t, s, chat.ID); v != chat.Version {
		t.Errorf("version = %d, want unchanged %d", v, chat.Version)
	}
	for _, u := range []store.User{creator, member} {
		if got := ptsOf(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 0); len(ev) != 0 {
			t.Errorf("owner %d events = %+v, want none", u.ID, ev)
		}
	}
}

func TestRemoveChatUserAllowsMemberSelfLeaveButNotCreatorSelfLeave(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551273301")
	member := mustUser(t, s, "+15551273302")
	chat := chatWith(t, s, creator, member)

	removed, _, _, err := s.RemoveChatUser(ctx, chat.ID, creator.ID, creator.ID)
	if !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("creator self-removal: err = %v, want ErrNotMember", err)
	}
	if removed {
		t.Fatal("creator self-removal succeeded")
	}

	removed, _, _, err = s.RemoveChatUser(ctx, chat.ID, member.ID, member.ID)
	if err != nil || !removed {
		t.Fatalf("member self-removal: removed=%v err=%v, want true/nil", removed, err)
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 1 || got[0] != creator.ID {
		t.Fatalf("participants = %v, want only creator %d", got, creator.ID)
	}
}

// TestRemoveChatUserSelfLeave is the F4 exception: the announcement's sender is
// no longer a member by the time the fan-out runs, and it must still be written.
func TestRemoveChatUserSelfLeave(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551273021")
	b := mustUser(t, s, "+15551273022")
	chat := chatWith(t, s, a, b)

	removed, sender, perOwner, err := s.RemoveChatUser(ctx, chat.ID, b.ID, b.ID)
	if err != nil || !removed {
		t.Fatalf("self-removal: removed=%v err=%v", removed, err)
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 1 || got[0] != a.ID {
		t.Fatalf("participants = %v, want only %d", got, a.ID)
	}
	if len(perOwner) != 2 {
		t.Fatalf("perOwner = %+v, want the remaining member and the leaver", perOwner)
	}
	if sender.OwnerID != b.ID || !sender.Out {
		t.Fatalf("leaver got no own copy: %+v", sender)
	}
	for _, u := range []store.User{a, b} {
		assertService(t, s, u.ID, 1, sender.FanoutID, store.ChatActionDeleteUser, b.ID)
	}
}

func TestSetChatTitleAnnouncesToEveryMember(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551274001")
	b := mustUser(t, s, "+15551274002")
	chat := chatWith(t, s, a, b)

	got, sender, perOwner, err := s.SetChatTitle(ctx, chat.ID, a.ID, "Renamed")
	if err != nil {
		t.Fatalf("set title: %v", err)
	}
	if got.Title != "Renamed" || got.Version != chat.Version+1 {
		t.Fatalf("chat = %+v, want title Renamed at version %d", got, chat.Version+1)
	}
	if sender.Text != "Renamed" || sender.Action != store.ChatActionEditTitle {
		t.Fatalf("sender copy = %+v, want an EditTitle carrying the new title", sender)
	}
	for _, u := range []store.User{a, b} {
		if perOwner[u.ID] != 1 {
			t.Errorf("owner %d pts = %d, want 1", u.ID, perOwner[u.ID])
		}
		assertService(t, s, u.ID, 1, sender.FanoutID, store.ChatActionEditTitle, 0)
	}
	stored, ok, err := s.ChatByID(ctx, chat.ID)
	if err != nil || !ok || stored.Title != "Renamed" {
		t.Fatalf("stored chat = %+v ok=%v err=%v", stored, ok, err)
	}
}

func TestSetChatTitleRequiresCreator(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551274101")
	member := mustUser(t, s, "+15551274102")
	chat := chatWith(t, s, creator, member)

	got, _, perOwner, err := s.SetChatTitle(ctx, chat.ID, member.ID, "Hijacked")
	if !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("member rename: err = %v, want ErrNotMember", err)
	}
	if got != (store.Chat{}) || perOwner != nil {
		t.Fatalf("rejected rename: chat=%+v perOwner=%+v, want zero/nil", got, perOwner)
	}
	stored, ok, err := s.ChatByID(ctx, chat.ID)
	if err != nil || !ok {
		t.Fatalf("chat by id: ok=%v err=%v", ok, err)
	}
	if stored.Title != chat.Title || stored.Version != chat.Version {
		t.Fatalf("chat after rejected rename = %+v, want unchanged %+v", stored, chat)
	}
	for _, u := range []store.User{creator, member} {
		if got := ptsOf(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 0); len(ev) != 0 {
			t.Errorf("owner %d events = %+v, want none", u.ID, ev)
		}
	}
}

// TestChatMutationsRejectNonMemberCaller is F4: the in-transaction caller check
// under the chats row lock. It is the only membership check these three get,
// because fanOut skips the sender check for a non-zero Action.
func TestChatMutationsRejectNonMemberCaller(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551275001")
	b := mustUser(t, s, "+15551275002")
	outsider := mustUser(t, s, "+15551275003")
	target := mustUser(t, s, "+15551275004")
	chat := chatWith(t, s, a, b)

	if _, _, _, err := s.AddChatUser(ctx, chat.ID, target.ID, outsider.ID); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("add by non-member: want ErrNotMember, got %v", err)
	}
	if _, _, _, err := s.RemoveChatUser(ctx, chat.ID, b.ID, outsider.ID); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("remove by non-member: want ErrNotMember, got %v", err)
	}
	if _, _, _, err := s.SetChatTitle(ctx, chat.ID, outsider.ID, "Hijacked"); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("rename by non-member: want ErrNotMember, got %v", err)
	}

	// An unknown chat id reports the same error as a chat the caller is not in,
	// so the pair stays indistinguishable to a prober walking the id space.
	if _, _, _, err := s.SetChatTitle(ctx, chat.ID+10_000, a.ID, "Ghost"); !errors.Is(err, store.ErrNotMember) {
		t.Errorf("unknown chat: want ErrNotMember, got %v", err)
	}

	if got := participantIDs(t, s, chat.ID); len(got) != 2 {
		t.Errorf("participants = %v, want unchanged 2", got)
	}
	c, _, err := s.ChatByID(ctx, chat.ID)
	if err != nil {
		t.Fatalf("chat by id: %v", err)
	}
	if c.Version != chat.Version || c.Title != chat.Title {
		t.Errorf("chat = %+v, want unchanged %+v", c, chat)
	}
	for _, u := range []store.User{a, b, outsider, target} {
		if got := ptsOf(t, s, u.ID); got != 0 {
			t.Errorf("owner %d pts = %d, want 0", u.ID, got)
		}
		if ev := eventsOf(t, s, u.ID, 0); len(ev) != 0 {
			t.Errorf("owner %d events = %+v, want none", u.ID, ev)
		}
	}
}

// TestChatMutationsRejectNonMemberBeforeTheRowLock pins the other half of the
// non-member invariant: same error, and no side effect — including not taking
// the chats row lock. A non-member that takes it holds it for the rest of its
// transaction, which serialises the real members' renames, adds and removals
// behind an outsider, and turns the wait into a timing oracle for exactly the
// chat existence the uniform ErrNotMember exists to hide.
//
// The lock is held by a third transaction throughout, so the assertion is on the
// rejection itself rather than on a wall clock: a call that reaches for the lock
// blocks until its context expires and fails with something other than
// ErrNotMember. The three run concurrently because they must not serialise on
// one another either.
func TestChatMutationsRejectNonMemberBeforeTheRowLock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551276001")
	b := mustUser(t, s, "+15551276002")
	outsider := mustUser(t, s, "+15551276003")
	target := mustUser(t, s, "+15551276004")
	chat := chatWith(t, s, a, b)

	release, err := store.HoldChatRowLock(ctx, s, chat.ID)
	if err != nil {
		t.Fatalf("hold chat row lock: %v", err)
	}
	defer release()

	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	errs := make([]error, 3)
	var wg sync.WaitGroup
	wg.Go(func() { _, _, _, errs[0] = s.SetChatTitle(callCtx, chat.ID, outsider.ID, "Hijacked") })
	wg.Go(func() { _, _, _, errs[1] = s.AddChatUser(callCtx, chat.ID, target.ID, outsider.ID) })
	wg.Go(func() { _, _, _, errs[2] = s.RemoveChatUser(callCtx, chat.ID, b.ID, outsider.ID) })
	wg.Wait()

	for i, err := range errs {
		if !errors.Is(err, store.ErrNotMember) {
			t.Errorf("call %d under a held chats row lock: want ErrNotMember, got %v", i, err)
		}
	}
	if got := participantIDs(t, s, chat.ID); len(got) != 2 {
		t.Errorf("participants = %v, want unchanged 2", got)
	}
}

func TestChatMutationAuthorityRefusalDoesNotTakeOwnerLock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551276101")
	member := mustUser(t, s, "+15551276102")
	target := mustUser(t, s, "+15551276103")
	chat := chatWith(t, s, creator, member, target)

	ownerID := min(creator.ID, member.ID, target.ID)
	release, err := store.HoldOwnerLock(ctx, s, ownerID)
	if err != nil {
		t.Fatalf("hold owner lock: %v", err)
	}
	defer release()

	cases := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "rename",
			call: func(callCtx context.Context) error {
				_, _, _, err := s.SetChatTitle(callCtx, chat.ID, member.ID, "Hijacked")
				return err
			},
		},
		{
			name: "remove another member",
			call: func(callCtx context.Context) error {
				_, _, _, err := s.RemoveChatUser(callCtx, chat.ID, target.ID, member.ID)
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The generous deadline avoids turning scheduler load into a false
			// refusal failure. Without the fix, the held owner lock keeps this
			// call blocked until the deadline.
			callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if err := tc.call(callCtx); !errors.Is(err, store.ErrNotMember) {
				t.Fatalf("authority refusal under a held owner lock: want ErrNotMember, got %v", err)
			}
		})
	}
}

// TestChatMutationsCommitMembershipWithTheirAnnouncement is F5. The failure it
// guards against is a membership change committing without its service message:
// an add whose fan-out is lost leaves a member no client ever saw join, and a
// remove whose fan-out is lost leaves the removed client showing the chat
// forever.
//
// The fan-out failure cannot be injected from a test without adding a seam to
// production code: every error fanOut can raise on this path is either
// unreachable (the participant cap is rejected earlier, under the same lock) or
// blocked by a foreign key that the participant write hits first. So atomicity is
// asserted the other way — after a successful call both halves are present, and
// after a rejected one neither is — and structurally, by there being no commit
// between the mutation and the fan-out in either method.
func TestChatMutationsCommitMembershipWithTheirAnnouncement(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551276001")
	b := mustUser(t, s, "+15551276002")
	chat := chatWith(t, s, a, b)

	// Add: membership and the announcement land together.
	_, addSender, _, err := s.AddChatUser(ctx, chat.ID, mustUser(t, s, "+15551276003").ID, a.ID)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	added := participantIDs(t, s, chat.ID)
	if len(added) != 3 {
		t.Fatalf("participants = %v, want 3", added)
	}
	for _, id := range added {
		assertService(t, s, id, 1, addSender.FanoutID, store.ChatActionAddUser, addSender.ActionUserID)
	}

	// Remove: the same, plus the removed user's own copy.
	_, rmSender, rmPerOwner, err := s.RemoveChatUser(ctx, chat.ID, b.ID, a.ID)
	if err != nil {
		t.Fatalf("remove: %v", err)
	}
	if _, ok := rmPerOwner[b.ID]; !ok {
		t.Fatalf("removed user missing from perOwner %+v", rmPerOwner)
	}
	for id := range rmPerOwner {
		assertService(t, s, id, 2, rmSender.FanoutID, store.ChatActionDeleteUser, b.ID)
	}
	for _, id := range participantIDs(t, s, chat.ID) {
		if id == b.ID {
			t.Fatal("removal committed its announcement but not its membership change")
		}
	}
}

func TestCreateChatDedupesMembers(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270001")
	b := mustUser(t, s, "+15551270002")

	// Creator repeated in memberIDs, and one member listed twice.
	c, err := s.CreateChat(ctx, a.ID, "Team", []int64{b.ID, b.ID, a.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	if c.ID == 0 || c.Title != "Team" || c.CreatorID != a.ID {
		t.Fatalf("chat = %+v", c)
	}
	if c.Version != 1 {
		t.Fatalf("version = %d, want 1", c.Version)
	}

	// Ascending user_id, deduped: creator and the one member.
	want := []int64{a.ID, b.ID}
	if a.ID > b.ID {
		want = []int64{b.ID, a.ID}
	}
	got := participantIDs(t, s, c.ID)
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("participants = %v, want %v", got, want)
	}

	ps, err := s.Participants(ctx, c.ID)
	if err != nil {
		t.Fatalf("participants: %v", err)
	}
	for _, p := range ps {
		if p.InviterID != a.ID {
			t.Errorf("participant %d inviter = %d, want %d", p.UserID, p.InviterID, a.ID)
		}
	}
}

func TestChatByID(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270011")

	c, err := s.CreateChat(ctx, a.ID, "Solo", nil)
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	got, ok, err := s.ChatByID(ctx, c.ID)
	if err != nil || !ok {
		t.Fatalf("chat by id: ok=%v err=%v", ok, err)
	}
	if got.ID != c.ID || got.Title != "Solo" {
		t.Fatalf("chat = %+v, want %+v", got, c)
	}
	if _, ok, err = s.ChatByID(ctx, c.ID+10_000); err != nil || ok {
		t.Fatalf("unknown chat: ok=%v err=%v", ok, err)
	}
}

func TestIsMember(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270021")
	b := mustUser(t, s, "+15551270022")
	outsider := mustUser(t, s, "+15551270023")

	c, err := s.CreateChat(ctx, a.ID, "Members", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	for _, u := range []store.User{a, b} {
		ok, err := s.IsMember(ctx, c.ID, u.ID)
		if err != nil {
			t.Fatalf("is member %d: %v", u.ID, err)
		}
		if !ok {
			t.Errorf("user %d not a member of its own chat", u.ID)
		}
	}
	ok, err := s.IsMember(ctx, c.ID, outsider.ID)
	if err != nil {
		t.Fatalf("is member outsider: %v", err)
	}
	if ok {
		t.Error("non-member reported as member")
	}
	ok, err = s.IsMember(ctx, c.ID+10_000, a.ID)
	if err != nil {
		t.Fatalf("is member unknown chat: %v", err)
	}
	if ok {
		t.Error("unknown chat id reported as member")
	}
}

func TestParticipantsAscending(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270031")
	members := make([]int64, 0, 4)
	for i := range 4 {
		members = append(members, mustUser(t, s, fmt.Sprintf("+1555127004%d", i)).ID)
	}
	// Hand them over in descending order; storage order must not depend on it.
	reversed := make([]int64, len(members))
	for i, id := range members {
		reversed[len(members)-1-i] = id
	}

	c, err := s.CreateChat(ctx, a.ID, "Ordered", reversed)
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	got := participantIDs(t, s, c.ID)
	if len(got) != 5 {
		t.Fatalf("participants = %v, want 5", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1] >= got[i] {
			t.Fatalf("participants not ascending: %v", got)
		}
	}
}

func TestChatsForUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270051")
	b := mustUser(t, s, "+15551270052")
	lonely := mustUser(t, s, "+15551270053")

	both, err := s.CreateChat(ctx, a.ID, "Both", []int64{b.ID})
	if err != nil {
		t.Fatalf("create both: %v", err)
	}
	solo, err := s.CreateChat(ctx, a.ID, "SoloA", nil)
	if err != nil {
		t.Fatalf("create solo: %v", err)
	}

	as, err := s.ChatsForUser(ctx, a.ID)
	if err != nil {
		t.Fatalf("chats for a: %v", err)
	}
	if len(as) != 2 {
		t.Fatalf("a chats = %+v, want 2", as)
	}
	bs, err := s.ChatsForUser(ctx, b.ID)
	if err != nil {
		t.Fatalf("chats for b: %v", err)
	}
	if len(bs) != 1 || bs[0].ID != both.ID {
		t.Fatalf("b chats = %+v, want only %d", bs, both.ID)
	}
	if bs[0].ID == solo.ID {
		t.Error("b sees a chat it is not in")
	}
	none, err := s.ChatsForUser(ctx, lonely.ID)
	if err != nil {
		t.Fatalf("chats for lonely: %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("lonely chats = %+v, want none", none)
	}
}

func TestCreateChatRejectsOversizeAndWritesNothing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551270061")

	members := make([]int64, 0, 201)
	for i := range 201 {
		members = append(members, mustUser(t, s, fmt.Sprintf("+1555128%05d", i)).ID)
	}

	if _, err := s.CreateChat(ctx, a.ID, "Too big", members); !errors.Is(err, store.ErrChatFull) {
		t.Fatalf("create oversize chat: want ErrChatFull, got %v", err)
	}
	// No chat row, so no participant row either.
	for _, id := range append([]int64{a.ID}, members...) {
		cs, err := s.ChatsForUser(ctx, id)
		if err != nil {
			t.Fatalf("chats for %d: %v", id, err)
		}
		if len(cs) != 0 {
			t.Fatalf("user %d gained chats %+v after a rejected create", id, cs)
		}
	}
	// ChatsForUser joins chat_participants, so it cannot see an orphan chats row.
	// This database is cloned per test and starts empty, so no id may resolve.
	for id := int64(1); id <= 10; id++ {
		c, ok, err := s.ChatByID(ctx, id)
		if err != nil {
			t.Fatalf("chat by id %d: %v", id, err)
		}
		if ok {
			t.Fatalf("rejected create left chat row %+v", c)
		}
	}

	// The cap itself is inclusive: creator + 199 members is exactly 200 and must
	// succeed. Runs after the emptiness assertions so it cannot mask them.
	c, err := s.CreateChat(ctx, a.ID, "Exactly full", members[:199])
	if err != nil {
		t.Fatalf("create chat at the 200 limit: %v", err)
	}
	if got := participantIDs(t, s, c.ID); len(got) != 200 {
		t.Fatalf("participants at the limit = %d, want 200", len(got))
	}
}

func TestSetChatPinnedMessageRejectsDeletedMessage(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551294001")
	b := mustUser(t, s, "+15551294002")
	ch, err := s.CreateChat(ctx, a.ID, "test", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	// Send a message in the chat.
	sender, _, _, err := s.SendChatMessage(ctx, store.FanOut{
		ChatID: ch.ID, FromID: a.ID, Text: "hello", RandomID: 1,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	// Mark the sender's copy as deleted.
	err = store.SetMessageDeleted(ctx, s, a.ID, sender.LocalID)
	if err != nil {
		t.Fatalf("mark deleted: %v", err)
	}

	// Attempt to pin the deleted message.
	pinnedID := int32(sender.LocalID) //nolint:gosec // local_id fits int32
	_, _, err = s.SetChatPinnedMessage(ctx, ch.ID, a.ID, &pinnedID)
	if !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("pin deleted message: err = %v, want ErrMessageInvalid", err)
	}
}

func TestSetChatPinnedMessageRejectsWrongPeerMessage(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15551294003")
	b := mustUser(t, s, "+15551294004")
	ch, err := s.CreateChat(ctx, a.ID, "test", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	// Send a 1:1 DM (not in the chat).
	dm, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "dm", 1, 0, 0) //nolint:dogsled
	if err != nil {
		t.Fatalf("send DM: %v", err)
	}

	// Attempt to pin the DM message in the chat.
	pinnedID := int32(dm.LocalID) //nolint:gosec // local_id fits int32
	_, _, err = s.SetChatPinnedMessage(ctx, ch.ID, a.ID, &pinnedID)
	if !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("pin wrong-peer message: err = %v, want ErrMessageInvalid", err)
	}
}
