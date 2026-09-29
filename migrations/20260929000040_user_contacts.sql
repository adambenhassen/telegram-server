-- A contact belongs to the account that added it. Reciprocal edges are
-- independent rows; mutual state is derived when contacts are read.
CREATE TABLE user_contacts (
    owner_id   BIGINT NOT NULL REFERENCES users(id),
    contact_id BIGINT NOT NULL REFERENCES users(id),
    PRIMARY KEY (owner_id, contact_id),
    CHECK (owner_id <> contact_id)
);
