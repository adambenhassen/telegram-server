-- name: CreateUser :one
INSERT INTO users (phone) VALUES ($1)
ON CONFLICT (phone) DO UPDATE SET phone = EXCLUDED.phone
RETURNING *;

-- name: UserByPhone :one
SELECT * FROM users WHERE phone = $1;

-- name: UserByID :one
SELECT * FROM users WHERE id = $1;
