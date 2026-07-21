-- name: UpsertCode :exec
INSERT INTO phone_codes (phone, code_hash, code, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (phone) DO UPDATE
  SET code_hash = EXCLUDED.code_hash,
      code = EXCLUDED.code,
      expires_at = EXCLUDED.expires_at;

-- name: GetCode :one
SELECT * FROM phone_codes WHERE phone = $1;
