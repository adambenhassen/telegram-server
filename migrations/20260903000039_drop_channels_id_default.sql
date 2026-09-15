-- MAIN-250: Drop the BIGINT sequence default on channels.id.
-- Application code allocates random ids since MAIN-246; an insert that omits id
-- must fail loudly instead of silently drawing from channels_id_seq.
--
-- Reversible: ALTER TABLE channels ALTER COLUMN id SET DEFAULT nextval('channels_id_seq').
-- Existing rows, existing ids, and the sequence object are untouched.

ALTER TABLE channels ALTER COLUMN id DROP DEFAULT;
