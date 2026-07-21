-- name: PasswordByUser :one
SELECT * FROM user_passwords WHERE user_id = $1;

-- name: UpsertPassword :exec
INSERT INTO user_passwords (user_id, salt1, salt2, verifier, hint, recovery_email, has_recovery)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (user_id) DO UPDATE
  SET salt1          = EXCLUDED.salt1,
      salt2          = EXCLUDED.salt2,
      verifier       = EXCLUDED.verifier,
      hint           = EXCLUDED.hint,
      recovery_email = EXCLUDED.recovery_email,
      has_recovery   = EXCLUDED.has_recovery,
      updated_at     = now();

-- name: DeletePassword :execrows
DELETE FROM user_passwords WHERE user_id = $1;
