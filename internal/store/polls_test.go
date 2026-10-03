package store_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestCreatePollDeduplicatesAcrossMessageCopies(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551400001")
	member := mustUser(t, s, "+15551400002")
	chat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "poll", RandomID: 140001})
	ref := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	draft := store.PollDraft{
		Question:         []byte("Which option?"),
		Answers:          []store.PollAnswer{{Option: []byte("a"), Text: []byte("A"), Correct: true}, {Option: []byte("b"), Text: []byte("B")}},
		Quiz:             true,
		OpenAnswers:      true,
		ShuffleAnswers:   true,
		MultipleChoice:   true,
		RevotingDisabled: false,
		Solution:         []byte("A is correct"),
	}

	created, duplicate, err := s.CreatePoll(ctx, creator.ID, ref, draft)
	if err != nil {
		t.Fatalf("create poll: %v", err)
	}
	if duplicate {
		t.Fatal("first poll create reported a duplicate")
	}
	if created.ID <= 0 {
		t.Fatalf("poll id = %d, want a positive server id", created.ID)
	}
	if created.OpenAnswers || !created.RevotingDisabled || !created.ShuffleAnswers || !created.MultipleChoice {
		t.Fatalf("stored flags = open:%v revoting-disabled:%v shuffle:%v multiple:%v, want open stripped and quiz no-revote enforced", created.OpenAnswers, created.RevotingDisabled, created.ShuffleAnswers, created.MultipleChoice)
	}
	if created.Answers[0].Correct || len(created.Solution) != 0 {
		t.Fatalf("creator's initial quiz view disclosed the key: %+v", created)
	}

	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil || len(memberHistory) != 1 {
		t.Fatalf("member history = %d messages, err=%v", len(memberHistory), err)
	}
	memberRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberHistory[0].LocalID}
	memberView, err := s.PollForMessage(ctx, member.ID, memberRef)
	if err != nil {
		t.Fatalf("read member poll copy: %v", err)
	}
	if memberView.ID != created.ID || memberView.Answers[0].Correct || len(memberView.Solution) != 0 {
		t.Fatalf("member poll view = %+v, want shared id and withheld answer key", memberView)
	}
	voted, err := s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte("a")})
	if err != nil {
		t.Fatalf("member vote: %v", err)
	}
	if !voted.Answers[0].Correct || string(voted.Solution) != "A is correct" || voted.VoterCount != 1 {
		t.Fatalf("voter poll view = %+v, want the answer key and current total", voted)
	}
	creatorView, err := s.PollForMessage(ctx, creator.ID, ref)
	if err != nil {
		t.Fatalf("read creator poll after another viewer voted: %v", err)
	}
	if creatorView.Answers[0].Correct || len(creatorView.Solution) != 0 {
		t.Fatalf("creator poll view disclosed another viewer's answer key: %+v", creatorView)
	}

	draft.Question = []byte("changed retry payload")
	retried, duplicate, err := s.CreatePoll(ctx, creator.ID, ref, draft)
	if err != nil {
		t.Fatalf("retry poll create: %v", err)
	}
	if !duplicate || retried.ID != created.ID || string(retried.Question) != "Which option?" {
		t.Fatalf("retry = (%+v, duplicate=%v), want original canonical poll %d", retried, duplicate, created.ID)
	}
}

func TestTimedPollRetrySurvivesNearAndPastDeadline(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551400014")
	member := mustUser(t, s, "+15551400015")
	chat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "timed poll", RandomID: 140008})
	ref := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	now := time.Now().UTC()
	closeDate := now.Add(5 * time.Minute)
	draft := ordinaryPollDraft()
	draft.CloseDate = &closeDate
	store.SetNowFunc(s, func() time.Time { return now })
	original, duplicate, err := s.CreatePoll(ctx, creator.ID, ref, draft)
	if err != nil || duplicate {
		t.Fatalf("create timed poll = duplicate %v, err %v", duplicate, err)
	}

	store.SetNowFunc(s, func() time.Time { return closeDate.Add(-3 * time.Second) })
	nearDeadline, duplicate, err := s.CreatePoll(ctx, creator.ID, ref, draft)
	if err != nil || !duplicate || nearDeadline.ID != original.ID {
		t.Fatalf("near-deadline retry = poll %d duplicate %v err %v, want stored poll %d", nearDeadline.ID, duplicate, err, original.ID)
	}

	store.SetNowFunc(s, func() time.Time { return closeDate.Add(time.Second) })
	afterDeadline, duplicate, err := s.CreatePoll(ctx, creator.ID, ref, draft)
	if err != nil || !duplicate || afterDeadline.ID != original.ID {
		t.Fatalf("expired retry = poll %d duplicate %v err %v, want stored poll %d", afterDeadline.ID, duplicate, err, original.ID)
	}
}

func TestCreatePollSerializesWithConcurrentRestriction(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	owner := mustUser(t, s, "+15551400016")
	sender := mustUser(t, s, "+15551400017")
	chat := chatWith(t, s, owner, sender)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: sender.ID, Text: "poll", RandomID: 140009})
	ref := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}

	release, err := store.HoldChatRowLock(ctx, s, chat.ID)
	if err != nil {
		t.Fatalf("hold chat row: %v", err)
	}
	lockReleased := false
	releaseLock := func() {
		if !lockReleased {
			release()
			lockReleased = true
		}
	}
	defer releaseLock()

	type restrictionResult struct {
		changed bool
		err     error
	}
	restrictionDone := make(chan struct{})
	var restriction restrictionResult
	go func() {
		_, restriction.changed, restriction.err = s.SetChatDefaultBannedRights(ctx, chat.ID, owner.ID, []string{"send_polls"})
		close(restrictionDone)
	}()
	if err = store.WaitForLockWaiters(ctx, s, 1); err != nil {
		releaseLock()
		<-restrictionDone
		t.Fatalf("wait for restriction to block on chat row: %v", err)
	}

	type createResult struct {
		poll      store.Poll
		duplicate bool
		err       error
	}
	createDone := make(chan struct{})
	var created createResult
	go func() {
		created.poll, created.duplicate, created.err = s.CreatePoll(ctx, sender.ID, ref, ordinaryPollDraft())
		close(createDone)
	}()
	blocked, waitErr := waitForLockWaiterOrDone(ctx, s, 2, createDone)
	if waitErr != nil {
		releaseLock()
		<-restrictionDone
		<-createDone
		t.Fatalf("wait for restriction and create to serialize: %v", waitErr)
	}
	if !blocked {
		releaseLock()
		<-restrictionDone
		if created.err == nil {
			t.Fatal("poll creation completed while the restriction held the chat row")
		}
		t.Fatalf("poll creation completed before the restriction: %v", created.err)
	}

	releaseLock()
	<-restrictionDone
	if restriction.err != nil || !restriction.changed {
		t.Fatalf("set send_polls restriction = changed %v err %v", restriction.changed, restriction.err)
	}
	<-createDone
	if !errors.Is(created.err, store.ErrChatWriteForbidden) {
		t.Fatalf("poll create after committed restriction = %v, want ErrChatWriteForbidden", created.err)
	}
	if got := pollRowCount(t, s); got != 0 {
		t.Fatalf("restricted create left %d polls, want none", got)
	}
}

func TestPollRejectsUnownedAndInvalidMessageCopiesWithoutWrites(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551400003")
	member := mustUser(t, s, "+15551400004")
	outsider := mustUser(t, s, "+15551400005")
	chat := chatWith(t, s, creator, member)
	otherChat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "ordinary", RandomID: 140002})
	ref := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	draft := store.PollDraft{Question: []byte("Question"), Answers: []store.PollAnswer{{Option: []byte("a"), Text: []byte("A")}, {Option: []byte("b"), Text: []byte("B")}}}
	beforeInvalidCreates := map[int64]pollUpdateState{
		creator.ID: capturePollUpdateState(t, s, creator.ID),
		member.ID:  capturePollUpdateState(t, s, member.ID),
	}

	if _, _, err := s.CreatePoll(ctx, outsider.ID, ref, draft); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("nonmember create error = %v, want ErrNotMember", err)
	}
	wrongPeer := ref
	wrongPeer.PeerID = otherChat.ID
	if _, _, err := s.CreatePoll(ctx, creator.ID, wrongPeer, draft); !errors.Is(err, store.ErrMessageInvalid) {
		t.Fatalf("wrong-peer create error = %v, want ErrMessageInvalid", err)
	}
	badDraft := draft
	badDraft.Answers = badDraft.Answers[:1]
	if _, _, err := s.CreatePoll(ctx, creator.ID, ref, badDraft); !errors.Is(err, store.ErrPollInvalid) {
		t.Fatalf("one-option create error = %v, want ErrPollInvalid", err)
	}
	if got := pollRowCount(t, s); got != 0 {
		t.Fatalf("denied and invalid creates left %d poll rows, want none", got)
	}
	assertPollUpdateStateUnchanged(t, s, beforeInvalidCreates)

	if _, _, err := s.CreatePoll(ctx, creator.ID, ref, draft); err != nil {
		t.Fatalf("create valid poll for vote rejection checks: %v", err)
	}
	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil || len(memberHistory) != 1 {
		t.Fatalf("member history = %d messages, err=%v", len(memberHistory), err)
	}
	memberRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberHistory[0].LocalID}
	beforeDeniedVote := map[int64]pollUpdateState{
		creator.ID: capturePollUpdateState(t, s, creator.ID),
		member.ID:  capturePollUpdateState(t, s, member.ID),
	}
	if _, err := s.CastPollVote(ctx, outsider.ID, ref, [][]byte{[]byte("a")}); !errors.Is(err, store.ErrNotMember) {
		t.Fatalf("nonmember vote error = %v, want ErrNotMember", err)
	}
	if _, err := s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte("unknown")}); !errors.Is(err, store.ErrPollInvalid) {
		t.Fatalf("unknown option vote error = %v, want ErrPollInvalid", err)
	}
	assertPollUpdateStateUnchanged(t, s, beforeDeniedVote)
	if got := pollRowCount(t, s); got != 1 {
		t.Fatalf("denied and invalid votes changed poll row count to %d, want 1", got)
	}

	if _, _, err := s.SetChatDefaultBannedRights(ctx, chat.ID, creator.ID, []string{"send_polls"}); err != nil {
		t.Fatalf("restrict poll sends: %v", err)
	}
	memberMessage, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: member.ID, Text: "ordinary", RandomID: 140003})
	memberRef = store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberMessage.LocalID}
	beforeRightsDeniedCreate := map[int64]pollUpdateState{
		creator.ID: capturePollUpdateState(t, s, creator.ID),
		member.ID:  capturePollUpdateState(t, s, member.ID),
	}
	if _, _, err := s.CreatePoll(ctx, member.ID, memberRef, draft); !errors.Is(err, store.ErrChatWriteForbidden) {
		t.Fatalf("restricted poll create error = %v, want ErrChatWriteForbidden", err)
	}
	assertPollUpdateStateUnchanged(t, s, beforeRightsDeniedCreate)
	if got := pollRowCount(t, s); got != 1 {
		t.Fatalf("denied poll create changed poll row count to %d, want 1", got)
	}
}

func TestPollVoteReplacementCountsOneVoterAndSurvivesStoreReopen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s := openStore(t, dsn)
	creator := mustUser(t, s, "+15551400006")
	member := mustUser(t, s, "+15551400007")
	chat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "poll", RandomID: 140004})
	creatorRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	poll, duplicate, err := s.CreatePoll(ctx, creator.ID, creatorRef, ordinaryPollDraft())
	if err != nil || duplicate {
		t.Fatalf("create poll = duplicate %v, err %v", duplicate, err)
	}
	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil || len(memberHistory) != 1 {
		t.Fatalf("member history = %d messages, err=%v", len(memberHistory), err)
	}
	memberRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberHistory[0].LocalID}

	first, err := s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte("a")})
	if err != nil {
		t.Fatalf("first selection: %v", err)
	}
	if first.VoterCount != 1 || pollAnswer(t, first, "a").VoterCount != 1 || !pollAnswer(t, first, "a").Chosen {
		t.Fatalf("first result = %+v, want one voter selecting A", first)
	}
	second, err := s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte("b")})
	if err != nil {
		t.Fatalf("replacement selection: %v", err)
	}
	if second.VoterCount != 1 || pollAnswer(t, second, "a").VoterCount != 0 || pollAnswer(t, second, "a").Chosen || pollAnswer(t, second, "b").VoterCount != 1 || !pollAnswer(t, second, "b").Chosen {
		t.Fatalf("replacement result = %+v, want one voter selecting only B", second)
	}
	if second.ID != poll.ID {
		t.Fatalf("vote poll id = %d, created poll id = %d", second.ID, poll.ID)
	}
	beforeInvalidVote := map[int64]pollUpdateState{
		creator.ID: capturePollUpdateState(t, s, creator.ID),
		member.ID:  capturePollUpdateState(t, s, member.ID),
	}
	if _, err := s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte("unknown")}); !errors.Is(err, store.ErrPollInvalid) {
		t.Fatalf("invalid replacement vote error = %v, want ErrPollInvalid", err)
	}
	assertPollUpdateStateUnchanged(t, s, beforeInvalidVote)

	// A second Store has no in-memory poll state to inherit from the first.
	restarted := openStore(t, dsn)
	recovered, err := restarted.PollForMessage(ctx, member.ID, memberRef)
	if err != nil {
		t.Fatalf("read after Store reopen: %v", err)
	}
	if recovered.ID != poll.ID || recovered.VoterCount != 1 || pollAnswer(t, recovered, "a").VoterCount != 0 || pollAnswer(t, recovered, "b").VoterCount != 1 || !pollAnswer(t, recovered, "b").Chosen {
		t.Fatalf("recovered poll = %+v, want the committed replacement vote", recovered)
	}
}

func TestConcurrentFirstQuizVotesAllowOnlyOneSelection(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551400008")
	member := mustUser(t, s, "+15551400009")
	chat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "quiz", RandomID: 140005})
	creatorRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	draft := store.PollDraft{
		Question:       []byte("Choose a correct answer"),
		Answers:        []store.PollAnswer{{Option: []byte("a"), Text: []byte("A"), Correct: true}, {Option: []byte("b"), Text: []byte("B"), Correct: true}},
		Quiz:           true,
		MultipleChoice: true,
	}
	if _, _, err := s.CreatePoll(ctx, creator.ID, creatorRef, draft); err != nil {
		t.Fatalf("create quiz: %v", err)
	}
	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil || len(memberHistory) != 1 {
		t.Fatalf("member history = %d messages, err=%v", len(memberHistory), err)
	}
	memberRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberHistory[0].LocalID}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, option := range []string{"a", "b"} {
		wg.Add(1)
		go func(selected string) {
			defer wg.Done()
			<-start
			_, voteErr := s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte(selected)})
			results <- voteErr
		}(option)
	}
	close(start)
	wg.Wait()
	close(results)
	wins, refused := 0, 0
	for voteErr := range results {
		switch {
		case voteErr == nil:
			wins++
		case errors.Is(voteErr, store.ErrPollVoteNotAllowed):
			refused++
		default:
			t.Fatalf("concurrent quiz vote error = %v", voteErr)
		}
	}
	if wins != 1 || refused != 1 {
		t.Fatalf("concurrent quiz votes: accepted %d, refused %d, want one each", wins, refused)
	}
	view, err := s.PollForMessage(ctx, member.ID, memberRef)
	if err != nil {
		t.Fatalf("read quiz after concurrent votes: %v", err)
	}
	chosen := 0
	for _, answer := range view.Answers {
		if answer.Chosen {
			chosen++
		}
	}
	if !view.HasVoted || view.VoterCount != 1 || chosen != 1 {
		t.Fatalf("committed quiz selection = %+v, want exactly one selected option and one voter", view)
	}
}

func TestVoteCloseRaceAndRepeatedClosePreservePollMetadata(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551400010")
	member := mustUser(t, s, "+15551400011")
	chat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "poll", RandomID: 140006})
	creatorRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	draft := ordinaryPollDraft()
	draft.PublicVoters = true
	draft.MultipleChoice = true
	draft.ShuffleAnswers = true
	date := time.Now().Add(5 * time.Minute)
	draft.CloseDate = &date
	created, _, err := s.CreatePoll(ctx, creator.ID, creatorRef, draft)
	if err != nil {
		t.Fatalf("create poll: %v", err)
	}
	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil || len(memberHistory) != 1 {
		t.Fatalf("member history = %d messages, err=%v", len(memberHistory), err)
	}
	memberRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberHistory[0].LocalID}
	creatorBefore := capturePollUpdateState(t, s, creator.ID)
	memberBefore := capturePollUpdateState(t, s, member.ID)
	if _, err = s.ClosePoll(ctx, member.ID, memberRef); !errors.Is(err, store.ErrPollDenied) {
		t.Fatalf("member close = %v, want ErrPollDenied", err)
	}
	assertPollUpdateStateUnchanged(t, s, map[int64]pollUpdateState{creator.ID: creatorBefore, member.ID: memberBefore})

	start := make(chan struct{})
	var wg sync.WaitGroup
	var voteErr error
	var closeChanged bool
	var closeErr error
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, voteErr = s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte("a")})
	}()
	go func() {
		defer wg.Done()
		<-start
		closeChanged, closeErr = s.ClosePoll(ctx, creator.ID, creatorRef)
	}()
	close(start)
	wg.Wait()
	if closeErr != nil || !closeChanged {
		t.Fatalf("close = changed %v err %v, want one closed transition", closeChanged, closeErr)
	}
	if voteErr != nil && !errors.Is(voteErr, store.ErrPollClosed) {
		t.Fatalf("racing vote error = %v, want success or ErrPollClosed", voteErr)
	}
	closed, err := s.PollForMessage(ctx, creator.ID, creatorRef)
	if err != nil {
		t.Fatalf("read closed poll: %v", err)
	}
	if !closed.Closed || closed.ID != created.ID || string(closed.Question) != string(created.Question) || !closed.PublicVoters || !closed.MultipleChoice || !closed.ShuffleAnswers || closed.CloseDate == nil || !closed.CloseDate.Equal(*created.CloseDate) {
		t.Fatalf("closed poll = %+v, want canonical metadata preserved", closed)
	}
	if changed, closeErr := s.ClosePoll(ctx, creator.ID, creatorRef); closeErr != nil || changed {
		t.Fatalf("repeat close = changed %v err %v, want no-op", changed, closeErr)
	}
	closedAgain, err := s.PollForMessage(ctx, creator.ID, creatorRef)
	if err != nil {
		t.Fatalf("read repeated close: %v", err)
	}
	if !reflect.DeepEqual(closed, closedAgain) {
		t.Fatalf("repeat close changed poll metadata/results: before %+v after %+v", closed, closedAgain)
	}
	memberAfter, err := s.PollForMessage(ctx, member.ID, memberRef)
	if err != nil {
		t.Fatalf("read member's result after close: %v", err)
	}
	if voteErr == nil && (!memberAfter.HasVoted || memberAfter.VoterCount != 1) {
		t.Fatalf("vote won close race but result is %+v", memberAfter)
	}
	if errors.Is(voteErr, store.ErrPollClosed) && (memberAfter.HasVoted || memberAfter.VoterCount != 0) {
		t.Fatalf("close won race but a vote was stored: %+v", memberAfter)
	}
	assertPollUpdateStateUnchanged(t, s, map[int64]pollUpdateState{creator.ID: creatorBefore, member.ID: memberBefore})
}

func TestPollCleanupKeepsAnotherPeersRetainedCopy(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551400012")
	member := mustUser(t, s, "+15551400013")
	chat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "poll", RandomID: 140007})
	creatorRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	poll, _, err := s.CreatePoll(ctx, creator.ID, creatorRef, ordinaryPollDraft())
	if err != nil {
		t.Fatalf("create poll: %v", err)
	}
	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil || len(memberHistory) != 1 {
		t.Fatalf("member history = %d messages, err=%v", len(memberHistory), err)
	}
	memberMessage := memberHistory[0]
	memberRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberMessage.LocalID}
	if removed, _, _, err := s.RemoveChatUser(ctx, chat.ID, member.ID, creator.ID); err != nil || !removed {
		t.Fatalf("remove member = %v err=%v", removed, err)
	}
	if _, err := s.DeleteMessages(ctx, creator.ID, []int64{message.LocalID}, true); err != nil {
		t.Fatalf("delete current member copies: %v", err)
	}
	if got := pollRowCount(t, s); got != 1 {
		t.Fatalf("poll rows after creator copy deletion = %d, want retained poll", got)
	}
	retained, ok, err := s.MessageByOwnerLocal(ctx, member.ID, memberMessage.LocalID)
	if err != nil || !ok || retained.Deleted {
		t.Fatalf("removed member's retained message = %+v ok=%v err=%v", retained, ok, err)
	}
	if added, _, _, addErr := s.AddChatUser(ctx, chat.ID, member.ID, creator.ID); addErr != nil || !added {
		t.Fatalf("re-add member = %v err=%v", added, addErr)
	}
	view, err := s.PollForMessage(ctx, member.ID, memberRef)
	if err != nil || view.ID != poll.ID {
		t.Fatalf("rejoined member poll = %+v err=%v, want retained poll %d", view, err, poll.ID)
	}
	if _, err = s.DeleteMessages(ctx, member.ID, []int64{memberMessage.LocalID}, false); err != nil {
		t.Fatalf("delete final retained copy: %v", err)
	}
	if got := pollRowCount(t, s); got != 0 {
		t.Fatalf("poll rows after last copy deletion = %d, want cleanup", got)
	}
	retained, ok, err = s.MessageByOwnerLocal(ctx, member.ID, memberMessage.LocalID)
	if err != nil || !ok || !retained.Deleted {
		t.Fatalf("ordinary message data after poll cleanup = %+v ok=%v err=%v", retained, ok, err)
	}
}

func TestConcurrentLastCopyDeletesSerializePollCleanup(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551400018")
	member := mustUser(t, s, "+15551400019")
	chat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "poll", RandomID: 140010})
	creatorRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	poll, _, err := s.CreatePoll(ctx, creator.ID, creatorRef, ordinaryPollDraft())
	if err != nil {
		t.Fatalf("create poll: %v", err)
	}
	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil || len(memberHistory) != 1 {
		t.Fatalf("member history = %d messages, err=%v", len(memberHistory), err)
	}
	memberRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberHistory[0].LocalID}
	if _, err = s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte("a")}); err != nil {
		t.Fatalf("record vote before deletes: %v", err)
	}

	first, err := store.StorePool(s).Begin(ctx)
	if err != nil {
		t.Fatalf("begin first delete: %v", err)
	}
	defer func() { _ = first.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	second, err := store.StorePool(s).Begin(ctx)
	if err != nil {
		t.Fatalf("begin second delete: %v", err)
	}
	defer func() { _ = second.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if _, err = first.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, creator.ID); err != nil {
		t.Fatalf("lock first owner: %v", err)
	}
	if _, err = second.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, member.ID); err != nil {
		t.Fatalf("lock second owner: %v", err)
	}
	if tag, e := first.Exec(ctx, `UPDATE messages SET deleted = true WHERE owner_id = $1 AND local_id = $2 AND NOT deleted`, creator.ID, message.LocalID); e != nil || tag.RowsAffected() != 1 {
		t.Fatalf("delete creator copy = rows %d err %v", tag.RowsAffected(), e)
	}

	deleteDone := make(chan struct{})
	var deleteErr error
	go func() {
		_, deleteErr = second.Exec(ctx, `UPDATE messages SET deleted = true WHERE owner_id = $1 AND local_id = $2 AND NOT deleted`, member.ID, memberHistory[0].LocalID)
		close(deleteDone)
	}()
	blocked, waitErr := waitForLockWaiterOrDone(ctx, s, 1, deleteDone)
	if waitErr != nil {
		_ = first.Rollback(ctx) //nolint:errcheck // unblock second delete before failing
		<-deleteDone
		t.Fatalf("wait for second cleanup to lock the canonical poll: %v", waitErr)
	}
	if err = first.Commit(ctx); err != nil {
		t.Fatalf("commit first copy delete: %v", err)
	}
	if blocked {
		<-deleteDone
	}
	if deleteErr != nil {
		t.Fatalf("delete member copy: %v", deleteErr)
	}
	if err = second.Commit(ctx); err != nil {
		t.Fatalf("commit second copy delete: %v", err)
	}
	if got := pollRowCount(t, s); got != 0 {
		t.Fatalf("concurrent last-copy deletes left %d poll rows, want cleanup", got)
	}
	var votes int64
	if err = store.StorePool(s).QueryRow(ctx, `SELECT count(*) FROM poll_votes WHERE poll_id = $1`, poll.ID).Scan(&votes); err != nil {
		t.Fatalf("count retained votes: %v", err)
	}
	if votes != 0 {
		t.Fatalf("concurrent last-copy deletes left %d votes, want cleanup", votes)
	}
}

func TestExpiredQuizReadRevealsKeyWithoutExplicitClose(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551400020")
	member := mustUser(t, s, "+15551400021")
	chat := chatWith(t, s, creator, member)
	message, _ := sendChat(t, s, store.FanOut{ChatID: chat.ID, FromID: creator.ID, Text: "quiz", RandomID: 140011})
	creatorRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: message.LocalID}
	draft := store.PollDraft{
		Question:         []byte("Question"),
		Answers:          []store.PollAnswer{{Option: []byte("a"), Text: []byte("A"), Correct: true}, {Option: []byte("b"), Text: []byte("B")}},
		Quiz:             true,
		RevotingDisabled: true,
		CloseDate:        func() *time.Time { date := time.Now().Add(5 * time.Minute); return &date }(),
		Solution:         []byte("A is correct"),
	}
	if _, _, err := s.CreatePoll(ctx, creator.ID, creatorRef, draft); err != nil {
		t.Fatalf("create timed quiz: %v", err)
	}
	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil || len(memberHistory) != 1 {
		t.Fatalf("member history = %d messages, err=%v", len(memberHistory), err)
	}
	memberRef := store.PollMessageRef{PeerType: store.PeerTypeChat, PeerID: chat.ID, LocalID: memberHistory[0].LocalID}
	if _, err = store.StorePool(s).Exec(ctx, `UPDATE polls SET close_date = clock_timestamp() - interval '1 second' WHERE creator_id = $1`, creator.ID); err != nil {
		t.Fatalf("expire quiz deadline: %v", err)
	}
	view, err := s.PollForMessage(ctx, member.ID, memberRef)
	if err != nil {
		t.Fatalf("read expired quiz: %v", err)
	}
	if !view.Closed || !pollAnswer(t, view, "a").Correct || string(view.Solution) != "A is correct" {
		t.Fatalf("expired quiz view = %+v, want effective closed state and revealed answer key", view)
	}
	if _, err = s.CastPollVote(ctx, member.ID, memberRef, [][]byte{[]byte("a")}); !errors.Is(err, store.ErrPollClosed) {
		t.Fatalf("vote after expiry = %v, want ErrPollClosed", err)
	}
}

type pollUpdateState struct {
	pts    int
	events int
}

func ordinaryPollDraft() store.PollDraft {
	return store.PollDraft{
		Question: []byte("Choose one"),
		Answers:  []store.PollAnswer{{Option: []byte("a"), Text: []byte("A")}, {Option: []byte("b"), Text: []byte("B")}},
	}
}

func pollAnswer(t *testing.T, poll store.Poll, option string) store.PollAnswer {
	t.Helper()
	for _, answer := range poll.Answers {
		if string(answer.Option) == option {
			return answer
		}
	}
	t.Fatalf("poll %d has no answer option %q: %+v", poll.ID, option, poll.Answers)
	return store.PollAnswer{}
}

func pollRowCount(t *testing.T, s *store.Store) int64 {
	t.Helper()
	var count int64
	if err := store.StorePool(s).QueryRow(context.Background(), "SELECT count(*) FROM polls").Scan(&count); err != nil {
		t.Fatalf("count poll rows: %v", err)
	}
	return count
}

func capturePollUpdateState(t *testing.T, s *store.Store, userID int64) pollUpdateState {
	t.Helper()
	ctx := context.Background()
	state, err := s.State(ctx, userID)
	if err != nil {
		t.Fatalf("read state for %d: %v", userID, err)
	}
	events, err := s.EventsSince(ctx, userID, 0)
	if err != nil {
		t.Fatalf("read events for %d: %v", userID, err)
	}
	return pollUpdateState{pts: state.Pts, events: len(events)}
}

func assertPollUpdateStateUnchanged(t *testing.T, s *store.Store, before map[int64]pollUpdateState) {
	t.Helper()
	for userID, prior := range before {
		if got := capturePollUpdateState(t, s, userID); got != prior {
			t.Errorf("user %d update state changed: before %+v after %+v", userID, prior, got)
		}
	}
}

func waitForLockWaiterOrDone(ctx context.Context, s *store.Store, minWaiters int, done <-chan struct{}) (bool, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case <-done:
			return false, nil
		default:
		}
		var waiters int
		if err := store.StorePool(s).QueryRow(ctx,
			`SELECT count(*) FROM pg_stat_activity
			 WHERE datname = current_database() AND wait_event_type = 'Lock'`,
		).Scan(&waiters); err != nil {
			return false, err
		}
		if waiters >= minWaiters {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, fmt.Errorf("waited 10s for %d lock waiters", minWaiters)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
