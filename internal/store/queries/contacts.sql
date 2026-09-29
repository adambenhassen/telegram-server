-- name: ContactExists :one
SELECT EXISTS (
    SELECT 1 FROM user_contacts
    WHERE owner_id = $1 AND contact_id = $2
);

-- name: CountContacts :one
SELECT COUNT(*) FROM user_contacts WHERE owner_id = $1;

-- name: InsertContact :execrows
INSERT INTO user_contacts (owner_id, contact_id)
VALUES ($1, $2)
ON CONFLICT (owner_id, contact_id) DO NOTHING;

-- name: DeleteContact :execrows
DELETE FROM user_contacts
WHERE owner_id = $1 AND contact_id = $2;

-- name: ListContacts :many
SELECT uc.contact_id,
       EXISTS (
           SELECT 1 FROM user_contacts reverse
           WHERE reverse.owner_id = uc.contact_id
             AND reverse.contact_id = uc.owner_id
       ) AS mutual,
       COUNT(*) OVER () AS total
FROM user_contacts uc
WHERE uc.owner_id = $1
ORDER BY uc.contact_id;
