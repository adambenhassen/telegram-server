-- name: InsertPoll :one
INSERT INTO polls (
    id, creator_id, random_id, source_local_id, question, public_voters,
    multiple_choice, quiz, shuffle_answers, revoting_disabled, close_date, solution
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT DO NOTHING
RETURNING *;

-- name: PollByCreatorRandomID :one
SELECT * FROM polls
WHERE creator_id = $1 AND random_id = $2 AND random_id <> 0;

-- name: PollByMessage :one
SELECT p.*
FROM polls p
JOIN poll_message_copies c ON c.poll_id = p.id
JOIN messages m ON m.owner_id = c.owner_id AND m.local_id = c.local_id
WHERE c.owner_id = $1
  AND c.local_id = $2
  AND m.peer_type = $3
  AND m.peer_id = $4
  AND m.deleted = false;

-- name: PollByIDForUpdate :one
SELECT * FROM polls WHERE id = $1 FOR UPDATE;

-- name: InsertPollOption :exec
INSERT INTO poll_options (poll_id, option, text, correct, position)
VALUES ($1, $2, $3, $4, $5);

-- name: PollOptionResults :many
SELECT o.option, o.text, o.correct, o.position,
       count(v.voter_id)::bigint AS voter_count,
       EXISTS (
           SELECT 1 FROM poll_vote_options selected
           WHERE selected.poll_id = o.poll_id
             AND selected.option = o.option
             AND selected.voter_id = sqlc.arg(viewer_id)::bigint
       ) AS chosen
FROM poll_options o
LEFT JOIN poll_vote_options v
  ON v.poll_id = o.poll_id AND v.option = o.option
WHERE o.poll_id = sqlc.arg(poll_id)
GROUP BY o.poll_id, o.option, o.text, o.correct, o.position
ORDER BY o.position;

-- name: PollVoterCount :one
SELECT count(*)::bigint FROM poll_votes WHERE poll_id = $1;

-- name: PollVoteByVoter :one
SELECT poll_id, voter_id, first_voted_at
FROM poll_votes
WHERE poll_id = $1 AND voter_id = $2;

-- name: PollVoteOptionsByVoter :many
SELECT option FROM poll_vote_options
WHERE poll_id = $1 AND voter_id = $2
ORDER BY option;

-- name: InsertPollVote :exec
INSERT INTO poll_votes (poll_id, voter_id) VALUES ($1, $2)
ON CONFLICT (poll_id, voter_id) DO NOTHING;

-- name: InsertPollVoteOption :exec
INSERT INTO poll_vote_options (poll_id, voter_id, option) VALUES ($1, $2, $3);

-- name: DeletePollVoteOptionsByVoter :exec
DELETE FROM poll_vote_options WHERE poll_id = $1 AND voter_id = $2;

-- name: DeletePollVote :exec
DELETE FROM poll_votes WHERE poll_id = $1 AND voter_id = $2;

-- name: ClosePoll :execrows
UPDATE polls SET closed = true
WHERE id = $1 AND closed = false;

-- Poll close_date is evaluated by Postgres while the caller holds the poll row
-- lock, so replicas with different wall clocks agree on vote admission.
-- name: PollIsClosed :one
SELECT COALESCE(closed OR (close_date IS NOT NULL AND close_date <= clock_timestamp()), false)::boolean AS is_closed
FROM polls
WHERE id = $1;

-- name: InsertPollMessageCopy :execrows
INSERT INTO poll_message_copies (owner_id, local_id, poll_id)
VALUES ($1, $2, $3)
ON CONFLICT (owner_id, local_id) DO NOTHING;
