-- Track one-time default folder initialization independently from folder rows
-- so user deletions do not cause defaults to be recreated on later reads.
ALTER TABLE user_dialog_filter_state
    ADD COLUMN defaults_seeded_at TIMESTAMPTZ;
