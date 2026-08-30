package store_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestBlockUserIsDirectedAndIdempotent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	blocker := mustUser(t, s, "+15551500001")
	blocked := mustUser(t, s, "+15551500002")
	other := mustUser(t, s, "+15551500003")

	changed, err := s.BlockUser(ctx, blocker.ID, blocked.ID)
	if err != nil || !changed {
		t.Fatalf("block: changed=%v err=%v", changed, err)
	}
	changed, err = s.BlockUser(ctx, blocker.ID, blocked.ID)
	if err != nil || changed {
		t.Fatalf("duplicate block: changed=%v err=%v, want false/nil", changed, err)
	}

	blockedState, err := s.IsBlocked(ctx, blocker.ID, blocked.ID)
	if err != nil || !blockedState {
		t.Fatalf("directed block state: blocked=%v err=%v", blockedState, err)
	}
	reverseState, err := s.IsBlocked(ctx, blocked.ID, blocker.ID)
	if err != nil || reverseState {
		t.Fatalf("reverse block state: blocked=%v err=%v, want false/nil", reverseState, err)
	}

	page, err := s.BlockedUsers(ctx, blocker.ID, 0, 100)
	if err != nil {
		t.Fatalf("blocked list: %v", err)
	}
	if page.Total != 1 || len(page.Users) != 1 || page.Users[0].UserID != blocked.ID {
		t.Fatalf("blocked list = %+v, want only %d", page, blocked.ID)
	}
	page, err = s.BlockedUsers(ctx, blocker.ID, 1, 100)
	if err != nil {
		t.Fatalf("empty blocked page: %v", err)
	}
	if page.Total != 1 || len(page.Users) != 0 {
		t.Fatalf("empty blocked page = %+v, want total 1", page)
	}
	page, err = s.BlockedUsers(ctx, other.ID, 0, 100)
	if err != nil {
		t.Fatalf("other blocked list: %v", err)
	}
	if page.Total != 0 || len(page.Users) != 0 {
		t.Fatalf("other blocked list = %+v, want empty", page)
	}

	changed, err = s.UnblockUser(ctx, blocker.ID, blocked.ID)
	if err != nil || !changed {
		t.Fatalf("unblock: changed=%v err=%v", changed, err)
	}
	changed, err = s.UnblockUser(ctx, blocker.ID, blocked.ID)
	if err != nil || changed {
		t.Fatalf("duplicate unblock: changed=%v err=%v, want false/nil", changed, err)
	}
	blockedState, err = s.IsBlocked(ctx, blocker.ID, blocked.ID)
	if err != nil || blockedState {
		t.Fatalf("unblocked state: blocked=%v err=%v, want false/nil", blockedState, err)
	}
}

func TestBlockedUsersSurviveStoreReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	blocker := mustUser(t, s, "+15551500101")
	blocked := mustUser(t, s, "+15551500102")
	if _, err := s.BlockUser(ctx, blocker.ID, blocked.ID); err != nil {
		t.Fatalf("block: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s, err = store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close
	page, err := s.BlockedUsers(ctx, blocker.ID, 0, 100)
	if err != nil {
		t.Fatalf("blocked list after reopen: %v", err)
	}
	if page.Total != 1 || len(page.Users) != 1 || page.Users[0].UserID != blocked.ID {
		t.Fatalf("blocked list after reopen = %+v, want only %d", page, blocked.ID)
	}
}

func TestBlockedAddDoesNotWriteAndUnblockAllowsIt(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	caller := mustUser(t, s, "+15551500201")
	target := mustUser(t, s, "+15551500202")
	member := mustUser(t, s, "+15551500203")
	chat := chatWith(t, s, caller, member)

	if _, err := s.BlockUser(ctx, target.ID, caller.ID); err != nil {
		t.Fatalf("block: %v", err)
	}
	wantParticipants := participantIDs(t, s, chat.ID)
	wantVersion := chatVersion(t, s, chat.ID)
	wantCallerPts := ptsOf(t, s, caller.ID)
	wantCallerEvents := len(eventsOf(t, s, caller.ID, 0))
	wantTargetPts := ptsOf(t, s, target.ID)
	wantTargetEvents := len(eventsOf(t, s, target.ID, 0))

	added, sender, perOwner, err := s.AddChatUser(ctx, chat.ID, target.ID, caller.ID)
	if !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("blocked add: added=%v sender=%+v perOwner=%+v err=%v, want ErrNotMember", added, sender, perOwner, err)
	}
	if added || perOwner != nil || sender.LocalID != 0 || sender.FanoutID != 0 {
		t.Fatalf("blocked add result: added=%v sender=%+v perOwner=%+v, want no-op", added, sender, perOwner)
	}
	if got := participantIDs(t, s, chat.ID); !reflect.DeepEqual(got, wantParticipants) {
		t.Errorf("participants after blocked add = %v, want %v", got, wantParticipants)
	}
	if got := chatVersion(t, s, chat.ID); got != wantVersion {
		t.Errorf("version after blocked add = %d, want %d", got, wantVersion)
	}
	if got := ptsOf(t, s, caller.ID); got != wantCallerPts {
		t.Errorf("caller pts after blocked add = %d, want %d", got, wantCallerPts)
	}
	if got := len(eventsOf(t, s, caller.ID, 0)); got != wantCallerEvents {
		t.Errorf("caller events after blocked add = %d, want %d", got, wantCallerEvents)
	}
	if got := ptsOf(t, s, target.ID); got != wantTargetPts {
		t.Errorf("target pts after blocked add = %d, want %d", got, wantTargetPts)
	}
	if got := len(eventsOf(t, s, target.ID, 0)); got != wantTargetEvents {
		t.Errorf("target events after blocked add = %d, want %d", got, wantTargetEvents)
	}

	changed, err := s.UnblockUser(ctx, target.ID, caller.ID)
	if err != nil || !changed {
		t.Fatalf("unblock: changed=%v err=%v", changed, err)
	}
	added, _, perOwner, err = s.AddChatUser(ctx, chat.ID, target.ID, caller.ID)
	if err != nil || !added || len(perOwner) != 3 {
		t.Fatalf("unblocked add: added=%v perOwner=%+v err=%v", added, perOwner, err)
	}
}

func TestBlockedAddRefusalDoesNotTakeOwnerLock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	caller := mustUser(t, s, "+15551500251")
	target := mustUser(t, s, "+15551500252")
	member := mustUser(t, s, "+15551500253")
	chat := chatWith(t, s, caller, member)
	if _, err := s.BlockUser(ctx, target.ID, caller.ID); err != nil {
		t.Fatalf("block: %v", err)
	}

	release, err := store.HoldOwnerLock(ctx, s, target.ID)
	if err != nil {
		t.Fatalf("hold target owner lock: %v", err)
	}
	defer release()
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	added, _, perOwner, err := s.AddChatUser(callCtx, chat.ID, target.ID, caller.ID)
	if !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("blocked add under held owner lock: added=%v perOwner=%+v err=%v, want ErrNotMember", added, perOwner, err)
	}
	if added || perOwner != nil {
		t.Fatalf("blocked add under held owner lock: added=%v perOwner=%+v, want no-op", added, perOwner)
	}
}

func TestBlockedAddRefusesAfterConcurrentBlockCommits(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	// BlockUser locks ids in ascending order. Creating target first makes its
	// lock the first one both operations reach.
	target := mustUser(t, s, "+15551500261")
	caller := mustUser(t, s, "+15551500262")
	if target.ID >= caller.ID {
		t.Fatalf("user ids = %d/%d, want target below caller", target.ID, caller.ID)
	}
	chat := chatWith(t, s, caller)
	wantParticipants := participantIDs(t, s, chat.ID)
	wantVersion := chatVersion(t, s, chat.ID)
	wantCallerPts := ptsOf(t, s, caller.ID)
	wantCallerEvents := len(eventsOf(t, s, caller.ID, 0))
	wantTargetPts := ptsOf(t, s, target.ID)
	wantTargetEvents := len(eventsOf(t, s, target.ID, 0))

	// Holding the caller lock lets BlockUser acquire target's lock, then park
	// before its insert. AddChatUser starts after that point, reads no committed
	// block, and parks on target's lock held by the in-flight block.
	releaseCaller, err := store.HoldOwnerLock(ctx, s, caller.ID)
	if err != nil {
		t.Fatalf("hold caller owner lock: %v", err)
	}
	released := false
	defer func() {
		if !released {
			releaseCaller()
		}
	}()
	callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	type blockResult struct {
		changed bool
		err     error
	}
	blockDone := make(chan blockResult, 1)
	go func() {
		changed, err := s.BlockUser(callCtx, target.ID, caller.ID)
		blockDone <- blockResult{changed: changed, err: err}
	}()
	if err := store.WaitForLockWaiters(callCtx, s, 1); err != nil {
		t.Fatalf("block did not park on caller lock: %v", err)
	}

	type addResult struct {
		added    bool
		sender   store.Message
		perOwner map[int64]int
		err      error
	}
	addDone := make(chan addResult, 1)
	go func() {
		added, sender, perOwner, err := s.AddChatUser(callCtx, chat.ID, target.ID, caller.ID)
		addDone <- addResult{added: added, sender: sender, perOwner: perOwner, err: err}
	}()
	if err := store.WaitForLockWaiters(callCtx, s, 2); err != nil {
		t.Fatalf("add did not park behind the uncommitted block: %v", err)
	}
	select {
	case result := <-addDone:
		t.Fatalf("add returned before the block committed: %+v", result)
	default:
	}

	// Releasing caller lets BlockUser acquire its second lock, commit the block,
	// and release target. The queued add then acquires the same sorted set and
	// must re-read the now-committed predicate before inserting.
	releaseCaller()
	released = true
	block := <-blockDone
	if block.err != nil || !block.changed {
		t.Fatalf("concurrent block: changed=%v err=%v", block.changed, block.err)
	}
	result := <-addDone
	if !errors.Is(result.err, store.ErrNotMember) {
		t.Fatalf("add after concurrent block: added=%v sender=%+v perOwner=%+v err=%v, want ErrNotMember", result.added, result.sender, result.perOwner, result.err)
	}
	if result.added || result.perOwner != nil || result.sender.LocalID != 0 || result.sender.FanoutID != 0 {
		t.Fatalf("add after concurrent block result: added=%v sender=%+v perOwner=%+v, want no-op", result.added, result.sender, result.perOwner)
	}
	if got := participantIDs(t, s, chat.ID); !reflect.DeepEqual(got, wantParticipants) {
		t.Errorf("participants after concurrent blocked add = %v, want %v", got, wantParticipants)
	}
	if got := chatVersion(t, s, chat.ID); got != wantVersion {
		t.Errorf("version after concurrent blocked add = %d, want %d", got, wantVersion)
	}
	if got := ptsOf(t, s, caller.ID); got != wantCallerPts {
		t.Errorf("caller pts after concurrent blocked add = %d, want %d", got, wantCallerPts)
	}
	if got := len(eventsOf(t, s, caller.ID, 0)); got != wantCallerEvents {
		t.Errorf("caller events after concurrent blocked add = %d, want %d", got, wantCallerEvents)
	}
	if got := ptsOf(t, s, target.ID); got != wantTargetPts {
		t.Errorf("target pts after concurrent blocked add = %d, want %d", got, wantTargetPts)
	}
	if got := len(eventsOf(t, s, target.ID, 0)); got != wantTargetEvents {
		t.Errorf("target events after concurrent blocked add = %d, want %d", got, wantTargetEvents)
	}
}

func TestBlockedAddCoversAbsentAndFormerMemberWithoutRemovingExistingMembership(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	caller := mustUser(t, s, "+15551500301")
	target := mustUser(t, s, "+15551500302")
	other := mustUser(t, s, "+15551500303")
	absentChat := chatWith(t, s, caller, other)
	formerChat := chatWith(t, s, caller, target)
	keptChat := chatWith(t, s, caller, target)
	if removed, _, _, err := s.RemoveChatUser(ctx, formerChat.ID, target.ID, caller.ID); err != nil || !removed {
		t.Fatalf("remove former member: removed=%v err=%v", removed, err)
	}
	if _, err := s.BlockUser(ctx, target.ID, caller.ID); err != nil {
		t.Fatalf("block: %v", err)
	}

	for name, chat := range map[string]store.Chat{"absent": absentChat, "former": formerChat} {
		before := participantIDs(t, s, chat.ID)
		added, _, perOwner, err := s.AddChatUser(ctx, chat.ID, target.ID, caller.ID)
		if !errors.Is(err, store.ErrNotMember) {
			t.Errorf("%s add: err=%v, want ErrNotMember", name, err)
		}
		if added || perOwner != nil {
			t.Errorf("%s add: added=%v perOwner=%+v, want no-op", name, added, perOwner)
		}
		if got := participantIDs(t, s, chat.ID); !reflect.DeepEqual(got, before) {
			t.Errorf("%s participants = %v, want %v", name, got, before)
		}
	}

	// Blocking does not remove an already-established membership, and the
	// existing-member path remains a true no-op.
	if got := participantIDs(t, s, keptChat.ID); len(got) != 2 {
		t.Fatalf("kept chat participants = %v, want caller and target", got)
	}
	added, _, perOwner, err := s.AddChatUser(ctx, keptChat.ID, target.ID, caller.ID)
	if err != nil || added || perOwner != nil {
		t.Fatalf("already-member add after block: added=%v perOwner=%+v err=%v", added, perOwner, err)
	}
}

func TestCreateChatSkipsBlockedInvitee(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551500401")
	blocked := mustUser(t, s, "+15551500402")
	member := mustUser(t, s, "+15551500403")
	if _, err := s.BlockUser(ctx, blocked.ID, creator.ID); err != nil {
		t.Fatalf("block: %v", err)
	}
	chat, err := s.CreateChat(ctx, creator.ID, "Filtered", []int64{blocked.ID, member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	got := participantIDs(t, s, chat.ID)
	want := []int64{creator.ID, member.ID}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("participants = %v, want %v", got, want)
	}
}

func TestCreateChatDoesNotTakeOwnerLock(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551500411")
	invitee := mustUser(t, s, "+15551500412")

	release, err := store.HoldOwnerLock(ctx, s, invitee.ID)
	if err != nil {
		t.Fatalf("hold invitee owner lock: %v", err)
	}
	defer release()
	callCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	chat, err := s.CreateChat(callCtx, creator.ID, "No owner lock", []int64{invitee.ID})
	if err != nil {
		t.Fatalf("create chat under held owner lock: %v", err)
	}
	if got := participantIDs(t, s, chat.ID); !reflect.DeepEqual(got, []int64{creator.ID, invitee.ID}) {
		t.Fatalf("participants = %v, want creator and invitee", got)
	}
}
