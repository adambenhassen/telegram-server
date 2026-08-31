-- name: InsertCode :exec
-- Inserts a new code row. Each IssueCode call creates its own row keyed by
-- code_hash, so attempt counters are isolated between callers.
INSERT INTO phone_codes (phone, code_hash, code, expires_at)
VALUES ($1, $2, $3, $4);

-- name: GetLatestCode :one
-- Returns the most recently created code row for a phone. Used by IssueCode
-- to enforce the resend cooldown: if the latest row is not consumed and was
-- created within the cooldown window, a new issue is rejected.
SELECT * FROM phone_codes
WHERE phone = $1
ORDER BY created_at DESC
LIMIT 1;

-- name: GetCodeByHash :one
-- Looks up a code row by its hash. Used by VerifyCode to find the exact row
-- the caller's code_hash belongs to, so attempts are charged only against that
-- row — never against a different caller's code for the same phone.
SELECT * FROM phone_codes WHERE code_hash = $1;

-- name: GetCodeByHashAndPhone :one
-- Looks up a code row by hash AND phone binding. Used by username-mode signIn
-- to validate the hash alone (without the code value) while confirming the hash
-- was issued for the expected identifier. Returns the hash-check fields and
-- the code value that auth.signUp consumes as the invite secret.
SELECT code_hash, phone, code, consumed_at, expires_at, attempts FROM phone_codes
WHERE code_hash = $1 AND phone = $2;

-- name: SetCodeByHashAndPhone :execrows
-- Stores the caller-supplied username signIn code on the exact live code row.
-- The guards keep a concurrent consume or expiry from rewriting terminal state.
UPDATE phone_codes SET code = $3
WHERE code_hash = $1
  AND phone = $2
  AND consumed_at IS NULL
  AND expires_at >= now()
  AND attempts < 3
  AND code ~ '^[0-9]{5}$'
  AND $3 !~ '^[0-9]{5}$';

-- name: ClearCodeValue :execrows
-- Removes the handoff secret after successful invite consumption. The hash and
-- phone binding scope the update to the exact admission code row.
UPDATE phone_codes SET code = ''
WHERE code_hash = $1
  AND phone = $2;

-- name: IncrementCodeAttempts :exec
-- Scoped to the exact issued code by its hash so a concurrent resend (new hash)
-- is never charged for a failed attempt against the old code.
UPDATE phone_codes SET attempts = attempts + 1
WHERE code_hash = $1;

-- name: ConsumeCode :execrows
-- Compare-and-swap: consume only the exact issued code, and only while it is
-- still verifiable. The phone binding in the WHERE enforces that a code_hash
-- issued for one identifier cannot be consumed under a different one — the
-- database-level guard that backs the Go check in VerifyCode.
UPDATE phone_codes SET consumed_at = now()
WHERE phone = $1
  AND code_hash = $2
  AND code = $3
  AND consumed_at IS NULL
  AND expires_at >= now()
  AND attempts < $4;

-- name: DeleteExpiredCodes :execrows
DELETE FROM phone_codes WHERE expires_at < now();
