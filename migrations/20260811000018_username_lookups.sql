-- M8: username lookup — per-account quota for contacts.resolveUsername.
--
-- Separate from phone_lookups so a username-harvesting bot cannot exhaust the
-- caller's phone-lookup quota. Same rolling-window structure: per-caller
-- advisory lock, per-caller prune, COUNT DISTINCT handle.

CREATE TABLE username_lookups (
    caller_id   BIGINT    NOT NULL REFERENCES users (id),
    handle      TEXT      NOT NULL,
    looked_up_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (caller_id, handle, looked_up_at)
);

-- Index for the rolling-window count: caller + time range.
CREATE INDEX username_lookups_caller_time ON username_lookups (caller_id, looked_up_at);
