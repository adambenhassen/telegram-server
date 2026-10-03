package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/teagramhq/teagram-server/internal/store/db"
)

const (
	maxPollAnswers     = 10
	maxPollOptionBytes = 100
	maxPollTextBytes   = 4096
	maxPollIDAttempts  = 5
)

var (
	ErrPollInvalid        = errors.New("poll invalid")
	ErrPollClosed         = errors.New("poll closed")
	ErrPollVoteNotAllowed = errors.New("poll vote change not allowed")
	ErrPollDenied         = errors.New("poll operation denied")
)

// PollMessageRef addresses a caller-owned message copy. Poll identity is
// resolved from this message, never from an id supplied as authority.
type PollMessageRef struct {
	PeerType PeerType
	PeerID   int64
	LocalID  int64
}

// PollDraft is fixed-answer poll metadata before canonical normalization.
// OpenAnswers is accepted only so callers can normalize the supported client
// payload; it is always stripped before storage and readback.
type PollDraft struct {
	Question         []byte
	Answers          []PollAnswer
	PublicVoters     bool
	MultipleChoice   bool
	Quiz             bool
	OpenAnswers      bool
	ShuffleAnswers   bool
	RevotingDisabled bool
	CloseDate        *time.Time
	Solution         []byte
}

// PollAnswer contains canonical option data plus the viewer-specific result
// fields returned by PollForMessage and CastPollVote.
type PollAnswer struct {
	Option     []byte
	Text       []byte
	Correct    bool
	VoterCount int64
	Chosen     bool
}

// Poll is one viewer's rendering of the canonical poll and committed results.
// Correct answers and the solution are present only after this viewer votes or
// the poll closes. Voter identities are never returned here.
type Poll struct {
	ID               int64
	Creator          bool
	Question         []byte
	Answers          []PollAnswer
	PublicVoters     bool
	MultipleChoice   bool
	Quiz             bool
	OpenAnswers      bool
	ShuffleAnswers   bool
	RevotingDisabled bool
	Closed           bool
	CloseDate        *time.Time
	Solution         []byte
	HasVoted         bool
	VoterCount       int64
}

// CreatePoll stores one canonical poll for every live copy of the outgoing
// message. The message random_id deduplicates retries; every retry returns the
// originally stored, per-viewer rendering without changing poll state.
func (s *Store) CreatePoll(ctx context.Context, creatorID int64, ref PollMessageRef, draft PollDraft) (poll Poll, duplicate bool, err error) {
	if err = validatePollRef(creatorID, ref); err != nil {
		return Poll{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Poll{}, false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	msg, copies, err := lockPollMessage(ctx, tx, qtx, creatorID, ref)
	if err != nil {
		return Poll{}, false, err
	}
	if !msg.Out || msg.FromID != creatorID {
		return Poll{}, false, ErrMessageInvalid
	}
	canonical, err := normalizePollDraft(draft, s.now())
	if err != nil {
		return Poll{}, false, err
	}
	if msg.RandomID != 0 {
		prior, e := qtx.PollByCreatorRandomID(ctx, db.PollByCreatorRandomIDParams{
			CreatorID: creatorID,
			RandomID:  msg.RandomID,
		})
		switch {
		case e == nil:
			if prior.SourceLocalID != msg.LocalID {
				return Poll{}, false, ErrPollInvalid
			}
			poll, err = pollView(ctx, qtx, prior, creatorID)
			if err != nil {
				return Poll{}, false, err
			}
			if err = tx.Commit(ctx); err != nil {
				return Poll{}, false, fmt.Errorf("commit duplicate poll: %w", err)
			}
			return poll, true, nil
		case !errors.Is(e, pgx.ErrNoRows):
			return Poll{}, false, fmt.Errorf("poll create dedup lookup: %w", e)
		}
	}
	if ref.PeerType == PeerTypeChat {
		chat, e := qtx.ChatByID(ctx, ref.PeerID)
		switch {
		case errors.Is(e, pgx.ErrNoRows):
			return Poll{}, false, ErrNotMember
		case e != nil:
			return Poll{}, false, fmt.Errorf("load poll chat: %w", e)
		case creatorID != chat.CreatorID && hasChatRight(chat.DefaultBannedRights, "send_polls"):
			return Poll{}, false, ErrChatWriteForbidden
		}
	}

	closeDate := pgtype.Timestamptz{}
	if canonical.CloseDate != nil {
		closeDate = pgtype.Timestamptz{Time: *canonical.CloseDate, Valid: true}
	}
	var row db.Poll
	created := false
	for range maxPollIDAttempts {
		id, e := randomPollID()
		if e != nil {
			return Poll{}, false, fmt.Errorf("generate poll id: %w", e)
		}
		row, e = qtx.InsertPoll(ctx, db.InsertPollParams{
			ID:               id,
			CreatorID:        creatorID,
			RandomID:         msg.RandomID,
			SourceLocalID:    msg.LocalID,
			Question:         canonical.Question,
			PublicVoters:     canonical.PublicVoters,
			MultipleChoice:   canonical.MultipleChoice,
			Quiz:             canonical.Quiz,
			ShuffleAnswers:   canonical.ShuffleAnswers,
			RevotingDisabled: canonical.RevotingDisabled,
			CloseDate:        closeDate,
			Solution:         canonical.Solution,
		})
		if e == nil {
			created = true
			break
		}
		if errors.Is(e, pgx.ErrNoRows) {
			if msg.RandomID != 0 {
				prior, lookupErr := qtx.PollByCreatorRandomID(ctx, db.PollByCreatorRandomIDParams{
					CreatorID: creatorID,
					RandomID:  msg.RandomID,
				})
				if lookupErr == nil {
					if prior.SourceLocalID != msg.LocalID {
						return Poll{}, false, ErrPollInvalid
					}
					poll, err = pollView(ctx, qtx, prior, creatorID)
					if err != nil {
						return Poll{}, false, err
					}
					if err = tx.Commit(ctx); err != nil {
						return Poll{}, false, fmt.Errorf("commit duplicate poll: %w", err)
					}
					return poll, true, nil
				}
				if !errors.Is(lookupErr, pgx.ErrNoRows) {
					return Poll{}, false, fmt.Errorf("poll create dedup lookup: %w", lookupErr)
				}
			}
			continue
		}
		return Poll{}, false, fmt.Errorf("insert poll: %w", e)
	}
	if !created {
		return Poll{}, false, errors.New("generate unique poll id: retry limit reached")
	}

	for position, answer := range canonical.Answers {
		if err = qtx.InsertPollOption(ctx, db.InsertPollOptionParams{
			PollID:   row.ID,
			Option:   answer.Option,
			Text:     answer.Text,
			Correct:  answer.Correct,
			Position: int16(position),
		}); err != nil {
			return Poll{}, false, fmt.Errorf("insert poll option %d: %w", position, err)
		}
	}

	for _, copy := range copies {
		if copy.Deleted {
			continue
		}
		n, e := qtx.InsertPollMessageCopy(ctx, db.InsertPollMessageCopyParams{
			OwnerID: copy.OwnerID,
			LocalID: copy.LocalID,
			PollID:  row.ID,
		})
		if e != nil {
			return Poll{}, false, fmt.Errorf("link poll to message copy %d/%d: %w", copy.OwnerID, copy.LocalID, e)
		}
		if n != 1 {
			return Poll{}, false, ErrPollInvalid
		}
	}

	poll, err = pollView(ctx, qtx, row, creatorID)
	if err != nil {
		return Poll{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Poll{}, false, fmt.Errorf("commit poll: %w", err)
	}
	return poll, false, nil
}

// PollForMessage returns a caller-authorized poll view for one of their message
// copies. The read uses one database snapshot for membership and poll results.
func (s *Store) PollForMessage(ctx context.Context, viewerID int64, ref PollMessageRef) (Poll, error) {
	if err := validatePollRef(viewerID, ref); err != nil {
		return Poll{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Poll{}, fmt.Errorf("begin poll read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	if err = pollPeerAccess(ctx, qtx, viewerID, ref); err != nil {
		return Poll{}, err
	}
	msg, err := qtx.PollByMessage(ctx, db.PollByMessageParams{
		OwnerID:  viewerID,
		LocalID:  ref.LocalID,
		PeerType: int16(ref.PeerType),
		PeerID:   ref.PeerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return Poll{}, ErrMessageInvalid
	}
	if err != nil {
		return Poll{}, fmt.Errorf("poll by message: %w", err)
	}
	poll, err := pollView(ctx, qtx, msg, viewerID)
	if err != nil {
		return Poll{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Poll{}, fmt.Errorf("commit poll read: %w", err)
	}
	return poll, nil
}

// CastPollVote atomically replaces one viewer's current selection. The poll row
// lock is acquired after the message-owner advisory locks and membership check,
// following the existing owner-before-poll lock order.
func (s *Store) CastPollVote(ctx context.Context, viewerID int64, ref PollMessageRef, selected [][]byte) (Poll, error) {
	if err := validatePollRef(viewerID, ref); err != nil {
		return Poll{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Poll{}, fmt.Errorf("begin poll vote: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	_, _, pollRow, err := lockPollForMutation(ctx, tx, qtx, viewerID, ref)
	if err != nil {
		return Poll{}, err
	}
	options, err := normalizePollSelection(selected)
	if err != nil {
		return Poll{}, err
	}
	_, voteErr := qtx.PollVoteByVoter(ctx, db.PollVoteByVoterParams{PollID: pollRow.ID, VoterID: viewerID})
	hasVote := voteErr == nil
	if voteErr != nil && !errors.Is(voteErr, pgx.ErrNoRows) {
		return Poll{}, fmt.Errorf("poll vote lookup: %w", voteErr)
	}
	previous, err := qtx.PollVoteOptionsByVoter(ctx, db.PollVoteOptionsByVoterParams{
		PollID:  pollRow.ID,
		VoterID: viewerID,
	})
	if err != nil {
		return Poll{}, fmt.Errorf("poll vote options: %w", err)
	}
	if samePollSelection(previous, options) {
		poll, viewErr := pollView(ctx, qtx, pollRow, viewerID)
		if viewErr != nil {
			return Poll{}, viewErr
		}
		if err = tx.Commit(ctx); err != nil {
			return Poll{}, fmt.Errorf("commit unchanged poll vote: %w", err)
		}
		return poll, nil
	}
	closed, err := qtx.PollIsClosed(ctx, pollRow.ID)
	if err != nil {
		return Poll{}, fmt.Errorf("poll closed state: %w", err)
	}
	if closed {
		return Poll{}, ErrPollClosed
	}
	if len(options) > 1 && !pollRow.MultipleChoice {
		return Poll{}, ErrPollInvalid
	}
	optionRows, err := qtx.PollOptionResults(ctx, db.PollOptionResultsParams{
		PollID:   pollRow.ID,
		ViewerID: viewerID,
	})
	if err != nil {
		return Poll{}, fmt.Errorf("poll options: %w", err)
	}
	allowed := make(map[string]bool, len(optionRows))
	for _, option := range optionRows {
		allowed[string(option.Option)] = true
	}
	for _, option := range options {
		if !allowed[string(option)] {
			return Poll{}, ErrPollInvalid
		}
	}
	if pollRow.RevotingDisabled && hasVote {
		return Poll{}, ErrPollVoteNotAllowed
	}
	if len(options) == 0 {
		if hasVote {
			if err = qtx.DeletePollVote(ctx, db.DeletePollVoteParams{PollID: pollRow.ID, VoterID: viewerID}); err != nil {
				return Poll{}, fmt.Errorf("retract poll vote: %w", err)
			}
		}
	} else {
		if err = qtx.InsertPollVote(ctx, db.InsertPollVoteParams{PollID: pollRow.ID, VoterID: viewerID}); err != nil {
			return Poll{}, fmt.Errorf("record poll voter: %w", err)
		}
		if err = qtx.DeletePollVoteOptionsByVoter(ctx, db.DeletePollVoteOptionsByVoterParams{PollID: pollRow.ID, VoterID: viewerID}); err != nil {
			return Poll{}, fmt.Errorf("replace poll selections: %w", err)
		}
		for _, option := range options {
			if err = qtx.InsertPollVoteOption(ctx, db.InsertPollVoteOptionParams{
				PollID:  pollRow.ID,
				VoterID: viewerID,
				Option:  option,
			}); err != nil {
				return Poll{}, fmt.Errorf("save poll selection: %w", err)
			}
		}
	}

	poll, err := pollView(ctx, qtx, pollRow, viewerID)
	if err != nil {
		return Poll{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Poll{}, fmt.Errorf("commit poll vote: %w", err)
	}
	return poll, nil
}

// ClosePoll applies only the closed transition. A repeated close is a no-op,
// and no caller-supplied poll metadata can overwrite the canonical record.
func (s *Store) ClosePoll(ctx context.Context, callerID int64, ref PollMessageRef) (bool, error) {
	if err := validatePollRef(callerID, ref); err != nil {
		return false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin poll close: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	_, _, err = lockPollMessage(ctx, tx, qtx, callerID, ref)
	if err != nil {
		return false, err
	}
	pollRow, err := qtx.PollByMessage(ctx, db.PollByMessageParams{
		OwnerID:  callerID,
		LocalID:  ref.LocalID,
		PeerType: int16(ref.PeerType),
		PeerID:   ref.PeerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrMessageInvalid
	}
	if err != nil {
		return false, fmt.Errorf("poll by message: %w", err)
	}
	if pollRow.CreatorID != callerID {
		return false, ErrPollDenied
	}
	pollRow, err = qtx.PollByIDForUpdate(ctx, pollRow.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrMessageInvalid
	}
	if err != nil {
		return false, fmt.Errorf("lock poll: %w", err)
	}
	changed := false
	if !pollRow.Closed {
		n, e := qtx.ClosePoll(ctx, pollRow.ID)
		if e != nil {
			return false, fmt.Errorf("close poll: %w", e)
		}
		changed = n == 1
	}
	if err = tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit poll close: %w", err)
	}
	return changed, nil
}

func validatePollRef(viewerID int64, ref PollMessageRef) error {
	if viewerID <= 0 || ref.PeerID <= 0 || ref.LocalID <= 0 {
		return ErrMessageInvalid
	}
	switch ref.PeerType {
	case PeerTypeUser:
		if ref.PeerID != viewerID {
			return ErrMessageInvalid
		}
	case PeerTypeChat:
	default:
		return ErrMessageInvalid
	}
	return nil
}

func pollPeerAccess(ctx context.Context, q *db.Queries, viewerID int64, ref PollMessageRef) error {
	if err := validatePollRef(viewerID, ref); err != nil {
		return err
	}
	if ref.PeerType != PeerTypeChat {
		return nil
	}
	member, err := q.IsChatMember(ctx, db.IsChatMemberParams{ChatID: ref.PeerID, UserID: viewerID})
	if err != nil {
		return fmt.Errorf("poll chat membership: %w", err)
	}
	if !member {
		return ErrNotMember
	}
	return nil
}

func lockPollMessage(ctx context.Context, tx pgx.Tx, q *db.Queries, viewerID int64, ref PollMessageRef) (db.Message, []db.Message, error) {
	if err := pollPeerAccess(ctx, q, viewerID, ref); err != nil {
		return db.Message{}, nil, err
	}
	pre, err := q.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: viewerID, LocalID: ref.LocalID})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Message{}, nil, ErrMessageInvalid
	}
	if err != nil {
		return db.Message{}, nil, fmt.Errorf("load poll message: %w", err)
	}
	if !pollMessageMatches(pre, ref) {
		return db.Message{}, nil, ErrMessageInvalid
	}
	copies := []db.Message{pre}
	if ref.PeerType == PeerTypeChat {
		copies, err = chatCopies(ctx, q, pre)
		if err != nil {
			return db.Message{}, nil, err
		}
	}
	if err = lockOwners(ctx, tx, copyOwners(copies)...); err != nil {
		return db.Message{}, nil, err
	}
	msg, err := q.MessageByOwnerLocal(ctx, db.MessageByOwnerLocalParams{OwnerID: viewerID, LocalID: ref.LocalID})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Message{}, nil, ErrMessageInvalid
	}
	if err != nil {
		return db.Message{}, nil, fmt.Errorf("reload poll message: %w", err)
	}
	if !pollMessageMatches(msg, ref) {
		return db.Message{}, nil, ErrMessageInvalid
	}
	if ref.PeerType == PeerTypeChat {
		members, e := chatMembers(ctx, q, ref.PeerID)
		if e != nil {
			return db.Message{}, nil, e
		}
		if !members[viewerID] {
			return db.Message{}, nil, ErrNotMember
		}
		copies, err = chatCopies(ctx, q, msg)
		if err != nil {
			return db.Message{}, nil, err
		}
	}
	return msg, copies, nil
}

func lockPollForMutation(ctx context.Context, tx pgx.Tx, q *db.Queries, viewerID int64, ref PollMessageRef) (db.Message, []db.Message, db.Poll, error) {
	msg, copies, err := lockPollMessage(ctx, tx, q, viewerID, ref)
	if err != nil {
		return db.Message{}, nil, db.Poll{}, err
	}
	row, err := q.PollByMessage(ctx, db.PollByMessageParams{
		OwnerID:  viewerID,
		LocalID:  ref.LocalID,
		PeerType: int16(ref.PeerType),
		PeerID:   ref.PeerID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Message{}, nil, db.Poll{}, ErrMessageInvalid
	}
	if err != nil {
		return db.Message{}, nil, db.Poll{}, fmt.Errorf("poll by message: %w", err)
	}
	locked, err := q.PollByIDForUpdate(ctx, row.ID)
	if errors.Is(err, pgx.ErrNoRows) {
		return db.Message{}, nil, db.Poll{}, ErrMessageInvalid
	}
	if err != nil {
		return db.Message{}, nil, db.Poll{}, fmt.Errorf("lock poll: %w", err)
	}
	return msg, copies, locked, nil
}

func pollMessageMatches(msg db.Message, ref PollMessageRef) bool {
	if msg.Deleted || msg.ActionType != int16(ChatActionNone) || PeerType(msg.PeerType) != ref.PeerType || msg.PeerID != ref.PeerID {
		return false
	}
	return ref.PeerType != PeerTypeUser || msg.FromID == msg.OwnerID && msg.Out
}

func normalizePollDraft(draft PollDraft, now time.Time) (PollDraft, error) {
	if len(draft.Question) == 0 || len(draft.Question) > maxPollTextBytes || !utf8.Valid(draft.Question) {
		return PollDraft{}, ErrPollInvalid
	}
	if len(draft.Answers) < 2 || len(draft.Answers) > maxPollAnswers {
		return PollDraft{}, ErrPollInvalid
	}
	if draft.CloseDate != nil {
		until := draft.CloseDate.Sub(now)
		if until < 5*time.Second || until > 10*time.Minute {
			return PollDraft{}, ErrPollInvalid
		}
	}
	canonical := draft
	canonical.Question = bytes.Clone(draft.Question)
	canonical.Solution = bytes.Clone(draft.Solution)
	canonical.OpenAnswers = false
	if canonical.Quiz {
		canonical.RevotingDisabled = true
	}
	canonical.Answers = make([]PollAnswer, len(draft.Answers))
	seen := make(map[string]bool, len(draft.Answers))
	correctCount := 0
	for i, answer := range draft.Answers {
		if len(answer.Option) == 0 || len(answer.Option) > maxPollOptionBytes || len(answer.Text) == 0 || len(answer.Text) > maxPollTextBytes || !utf8.Valid(answer.Text) {
			return PollDraft{}, ErrPollInvalid
		}
		key := string(answer.Option)
		if seen[key] {
			return PollDraft{}, ErrPollInvalid
		}
		seen[key] = true
		if answer.Correct {
			correctCount++
		}
		canonical.Answers[i] = PollAnswer{Option: bytes.Clone(answer.Option), Text: bytes.Clone(answer.Text), Correct: answer.Correct}
	}
	if canonical.Quiz {
		if correctCount == 0 || (!canonical.MultipleChoice && correctCount != 1) {
			return PollDraft{}, ErrPollInvalid
		}
	} else if correctCount != 0 {
		return PollDraft{}, ErrPollInvalid
	}
	if canonical.CloseDate != nil {
		date := canonical.CloseDate.UTC()
		canonical.CloseDate = &date
	}
	return canonical, nil
}

func normalizePollSelection(selected [][]byte) ([][]byte, error) {
	seen := make(map[string]bool, len(selected))
	options := make([][]byte, 0, min(len(selected), maxPollAnswers))
	for _, option := range selected {
		if len(option) == 0 || len(option) > maxPollOptionBytes {
			return nil, ErrPollInvalid
		}
		key := string(option)
		if seen[key] {
			continue
		}
		seen[key] = true
		options = append(options, bytes.Clone(option))
	}
	if len(options) > maxPollAnswers {
		return nil, ErrPollInvalid
	}
	return options, nil
}

func samePollSelection(previous [][]byte, selected [][]byte) bool {
	if len(previous) != len(selected) {
		return false
	}
	set := make(map[string]bool, len(previous))
	for _, option := range previous {
		set[string(option)] = true
	}
	for _, option := range selected {
		if !set[string(option)] {
			return false
		}
	}
	return true
}

func pollView(ctx context.Context, q *db.Queries, row db.Poll, viewerID int64) (Poll, error) {
	voterCount, err := q.PollVoterCount(ctx, row.ID)
	if err != nil {
		return Poll{}, fmt.Errorf("poll voter count: %w", err)
	}
	_, voteErr := q.PollVoteByVoter(ctx, db.PollVoteByVoterParams{PollID: row.ID, VoterID: viewerID})
	hasVoted := voteErr == nil
	if voteErr != nil && !errors.Is(voteErr, pgx.ErrNoRows) {
		return Poll{}, fmt.Errorf("poll viewer vote: %w", voteErr)
	}
	options, err := q.PollOptionResults(ctx, db.PollOptionResultsParams{PollID: row.ID, ViewerID: viewerID})
	if err != nil {
		return Poll{}, fmt.Errorf("poll option results: %w", err)
	}
	reveal := row.Closed || hasVoted
	poll := Poll{
		ID:               row.ID,
		Creator:          row.CreatorID == viewerID,
		Question:         bytes.Clone(row.Question),
		PublicVoters:     row.PublicVoters,
		MultipleChoice:   row.MultipleChoice,
		Quiz:             row.Quiz,
		OpenAnswers:      false,
		ShuffleAnswers:   row.ShuffleAnswers,
		RevotingDisabled: row.RevotingDisabled,
		Closed:           row.Closed,
		HasVoted:         hasVoted,
		VoterCount:       voterCount,
	}
	if row.CloseDate.Valid {
		date := row.CloseDate.Time
		poll.CloseDate = &date
	}
	if reveal {
		poll.Solution = bytes.Clone(row.Solution)
	}
	poll.Answers = make([]PollAnswer, 0, len(options))
	for _, option := range options {
		poll.Answers = append(poll.Answers, PollAnswer{
			Option:     bytes.Clone(option.Option),
			Text:       bytes.Clone(option.Text),
			Correct:    reveal && option.Correct,
			VoterCount: option.VoterCount,
			Chosen:     option.Chosen,
		})
	}
	return poll, nil
}

func randomPollID() (int64, error) {
	limit := big.NewInt(math.MaxInt64)
	id, err := rand.Int(rand.Reader, limit)
	if err != nil {
		return 0, fmt.Errorf("crypto random: %w", err)
	}
	return id.Int64() + 1, nil
}
