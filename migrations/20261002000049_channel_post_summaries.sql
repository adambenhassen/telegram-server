-- Durable per-member history markers and exact live-post range summaries.
-- Existing channels remain unready until the explicit initializer populates
-- their derived rows; this migration never scans or rewrites source data.
CREATE TABLE channel_read_state (
    channel_id  BIGINT NOT NULL,
    user_id     BIGINT NOT NULL,
    read_max_id BIGINT NOT NULL DEFAULT 0 CHECK (read_max_id >= 0),
    PRIMARY KEY (channel_id, user_id),
    FOREIGN KEY (channel_id, user_id)
        REFERENCES channel_participants (channel_id, user_id)
        ON DELETE CASCADE
);

-- No row means a pre-existing channel has not been initialized. New channels
-- are inserted ready with empty summaries by the channel insert hook below.
CREATE TABLE channel_post_summary_state (
    -- Deliberately no channels FK: rebuilding readiness already holds
    -- channel_state, and a post-state FK check would reverse the lock order.
    channel_id BIGINT PRIMARY KEY,
    version    SMALLINT NOT NULL,
    ready      BOOL NOT NULL,
    CHECK (version > 0)
);

-- One sparse binary prefix tree for the channel and one per actual author.
-- The zero author key belongs only to scope 0; author scopes use positive ids.
CREATE TABLE channel_post_summaries (
    -- channel_state is already locked before node mutations. Referencing it
    -- preserves integrity without acquiring channels after that lock.
    channel_id BIGINT NOT NULL REFERENCES channel_state (channel_id) ON DELETE CASCADE,
    scope_kind SMALLINT NOT NULL,
    author_id  BIGINT NOT NULL,
    depth      SMALLINT NOT NULL,
    prefix     BIGINT NOT NULL,
    live_count BIGINT NOT NULL CHECK (live_count >= 0),
    PRIMARY KEY (channel_id, scope_kind, author_id, depth, prefix),
    CHECK ((scope_kind = 0 AND author_id = 0) OR
           (scope_kind = 1 AND author_id > 0)),
    CHECK (depth BETWEEN 0 AND 63),
    CHECK (prefix >= 0),
    CHECK (prefix::NUMERIC < power(2::NUMERIC, depth))
);

CREATE FUNCTION channel_post_summary_mark_new_channel()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO channel_post_summary_state (channel_id, version, ready)
    VALUES (NEW.id, 1, true)
    ON CONFLICT (channel_id) DO NOTHING;
    RETURN NEW;
END;
$$;

CREATE TRIGGER channels_post_summary_ready_after_insert
AFTER INSERT ON channels
FOR EACH ROW
EXECUTE FUNCTION channel_post_summary_mark_new_channel();

-- A new membership starts at the allocator top committed under channel_state.
-- Existing rows are never touched, and deleting a membership cascades its
-- marker so a later admission gets a fresh marker at that later top.
CREATE FUNCTION channel_read_state_initialize_member()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    committed_top BIGINT;
BEGIN
    SELECT next_local_id - 1
      INTO committed_top
      FROM channel_state
     WHERE channel_id = NEW.channel_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'channel % has no state row for membership marker', NEW.channel_id;
    END IF;
    IF committed_top < 0 THEN
        RAISE EXCEPTION 'channel % has an invalid committed post top', NEW.channel_id;
    END IF;

    INSERT INTO channel_read_state (channel_id, user_id, read_max_id)
    VALUES (NEW.channel_id, NEW.user_id, committed_top)
    ON CONFLICT (channel_id, user_id) DO NOTHING;
    RETURN NEW;
END;
$$;

CREATE TRIGGER channel_participants_read_state_after_insert
AFTER INSERT ON channel_participants
FOR EACH ROW
EXECUTE FUNCTION channel_read_state_initialize_member();

-- The suffix above a marker is decomposed into at most 63 disjoint prefix
-- nodes. Marker zero can use the root directly.
CREATE FUNCTION channel_post_summary_suffix_keys(p_marker BIGINT)
RETURNS TABLE (depth SMALLINT, prefix BIGINT)
LANGUAGE plpgsql
IMMUTABLE
STRICT
AS $$
BEGIN
    IF p_marker < 0 THEN
        RAISE EXCEPTION 'channel post marker must be nonnegative';
    END IF;
    IF p_marker = 0 THEN
        RETURN QUERY SELECT 0::SMALLINT, 0::BIGINT;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT bits.depth::SMALLINT,
           (((p_marker >> (64 - bits.depth)) << 1) | 1)::BIGINT
      FROM generate_series(1, 63) AS bits(depth)
     WHERE ((p_marker >> (63 - bits.depth)) & 1) = 0;
END;
$$;

-- Source IDs are the range keys and author IDs are scope keys. Reject values
-- that could alias the total scope or fall outside the positive BIGINT tree.
CREATE FUNCTION channel_post_summary_validate_source_ids()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.local_id <= 0 OR OLD.from_id <= 0 THEN
            RAISE EXCEPTION 'channel post identity IDs must be positive';
        END IF;
        RETURN OLD;
    END IF;

    IF NEW.local_id <= 0 OR NEW.from_id <= 0 THEN
        RAISE EXCEPTION 'channel post identity IDs must be positive';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER channel_messages_summary_validate_ids
BEFORE INSERT OR UPDATE OR DELETE ON channel_messages
FOR EACH ROW
EXECUTE FUNCTION channel_post_summary_validate_source_ids();

-- Every contribution mutation first serializes on all affected channel_state
-- rows in channel-id order, then applies summary-node changes in key order.
-- Under READ COMMITTED, statements after each lock read readiness after any
-- wait, so a writer released by initialization sees its committed ready state.
-- Higher isolation keeps a stale snapshot across that wait, so reject it.
CREATE FUNCTION channel_post_summary_maintain_source()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    old_live BOOL := false;
    new_live BOOL := false;
    old_channel_id BIGINT;
    old_local_id BIGINT;
    old_from_id BIGINT;
    new_channel_id BIGINT;
    new_local_id BIGINT;
    new_from_id BIGINT;
    target_channel_id BIGINT;
    state_version SMALLINT;
BEGIN
    IF TG_OP <> 'INSERT' THEN
        old_channel_id := OLD.channel_id;
        old_local_id := OLD.local_id;
        old_from_id := OLD.from_id;
        old_live := NOT OLD.deleted;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        new_channel_id := NEW.channel_id;
        new_local_id := NEW.local_id;
        new_from_id := NEW.from_id;
        new_live := NOT NEW.deleted;
    END IF;

    IF NOT old_live AND NOT new_live THEN
        IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'UPDATE'
       AND old_live AND new_live
       AND old_channel_id = new_channel_id
       AND old_local_id = new_local_id
       AND old_from_id = new_from_id THEN
        RETURN NEW;
    END IF;

    FOR target_channel_id IN
        SELECT changed.channel_id
          FROM (VALUES
                    (CASE WHEN old_live THEN old_channel_id END),
                    (CASE WHEN new_live THEN new_channel_id END)
               ) AS changed(channel_id)
         WHERE changed.channel_id IS NOT NULL
         GROUP BY changed.channel_id
         ORDER BY changed.channel_id
    LOOP
        PERFORM channel_id
          FROM channel_state
         WHERE channel_id = target_channel_id
         FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'channel % has no state row for summary maintenance', target_channel_id;
        END IF;

        SELECT version
          INTO state_version
          FROM channel_post_summary_state
         WHERE channel_id = target_channel_id;
        IF FOUND AND state_version <> 1 THEN
            RAISE EXCEPTION 'channel % has unsupported post summary version %',
                target_channel_id, state_version;
        END IF;
    END LOOP;

    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'channel post summary mutations require READ COMMITTED isolation (got %)',
            current_setting('transaction_isolation');
    END IF;

    WITH source_changes (channel_id, local_id, from_id, delta) AS (
        SELECT old_channel_id, old_local_id, old_from_id, -1::BIGINT WHERE old_live
        UNION ALL
        SELECT new_channel_id, new_local_id, new_from_id, 1::BIGINT WHERE new_live
    ), scope_changes AS (
        SELECT source.channel_id, 0::SMALLINT AS scope_kind, 0::BIGINT AS author_id,
               source.local_id, source.delta
          FROM source_changes AS source
        UNION ALL
        SELECT source.channel_id, 1::SMALLINT AS scope_kind, source.from_id AS author_id,
               source.local_id, source.delta
          FROM source_changes AS source
    ), node_changes AS (
        SELECT scope.channel_id, scope.scope_kind, scope.author_id,
               depth.depth::SMALLINT AS depth,
               (scope.local_id >> (63 - depth.depth))::BIGINT AS prefix,
               SUM(scope.delta)::BIGINT AS delta
          FROM scope_changes AS scope
          CROSS JOIN generate_series(0, 63) AS depth(depth)
         GROUP BY scope.channel_id, scope.scope_kind, scope.author_id,
                  depth.depth, (scope.local_id >> (63 - depth.depth))
        HAVING SUM(scope.delta) <> 0
    )
    -- Insert the final value for each key, not the signed delta: the table
    -- check applies before ON CONFLICT, while a negative delta can still yield
    -- a valid nonnegative existing count. A missing/underflowed node stays
    -- negative here and fails the same check.
    INSERT INTO channel_post_summaries
        (channel_id, scope_kind, author_id, depth, prefix, live_count)
    SELECT nodes.channel_id, nodes.scope_kind, nodes.author_id,
           nodes.depth, nodes.prefix,
           COALESCE(current_summary.live_count, 0)::BIGINT + nodes.delta
      FROM node_changes AS nodes
      JOIN channel_post_summary_state AS summary_state
        ON summary_state.channel_id = nodes.channel_id
       AND summary_state.version = 1
       AND summary_state.ready = true
      LEFT JOIN channel_post_summaries AS current_summary
        ON current_summary.channel_id = nodes.channel_id
       AND current_summary.scope_kind = nodes.scope_kind
       AND current_summary.author_id = nodes.author_id
       AND current_summary.depth = nodes.depth
       AND current_summary.prefix = nodes.prefix
     ORDER BY nodes.channel_id, nodes.scope_kind, nodes.author_id,
              nodes.depth, nodes.prefix
    ON CONFLICT (channel_id, scope_kind, author_id, depth, prefix)
    DO UPDATE SET live_count = EXCLUDED.live_count;

    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER channel_messages_summary_maintain
AFTER INSERT OR UPDATE OR DELETE ON channel_messages
FOR EACH ROW
EXECUTE FUNCTION channel_post_summary_maintain_source();

CREATE FUNCTION channel_post_summary_reject_truncate()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'TRUNCATE channel_messages is refused while post summaries are installed';
END;
$$;

CREATE TRIGGER channel_messages_summary_reject_truncate
BEFORE TRUNCATE ON channel_messages
FOR EACH STATEMENT
EXECUTE FUNCTION channel_post_summary_reject_truncate();

-- Called by explicit maintenance only, outside the Atlas migration. The caller
-- first commits readiness=false; source writers then skip these derived rows
-- until this per-channel transaction commits the complete rebuild as ready.
CREATE FUNCTION initialize_channel_post_summaries(p_channel_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_channel_id <= 0 THEN
        RAISE EXCEPTION 'channel ID must be positive';
    END IF;

    PERFORM channel_id
      FROM channel_state
     WHERE channel_id = p_channel_id
     FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'channel % has no state row for summary initialization', p_channel_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM channel_messages
         WHERE channel_id = p_channel_id
           AND (local_id <= 0 OR from_id <= 0)
    ) THEN
        RAISE EXCEPTION 'channel % has invalid source post IDs', p_channel_id;
    END IF;

    DELETE FROM channel_post_summaries WHERE channel_id = p_channel_id;

    INSERT INTO channel_post_summaries
        (channel_id, scope_kind, author_id, depth, prefix, live_count)
    SELECT p_channel_id, contributions.scope_kind, contributions.author_id,
           contributions.depth::SMALLINT, contributions.prefix,
           COUNT(*)::BIGINT
      FROM channel_messages AS post
      CROSS JOIN generate_series(0, 63) AS depth(depth)
      CROSS JOIN LATERAL (
          VALUES (0::SMALLINT, 0::BIGINT),
                 (1::SMALLINT, post.from_id)
      ) AS scope(scope_kind, author_id)
      CROSS JOIN LATERAL (
          SELECT scope.scope_kind, scope.author_id,
                 depth.depth::SMALLINT AS depth,
                 (post.local_id >> (63 - depth.depth))::BIGINT AS prefix
      ) AS contributions
     WHERE post.channel_id = p_channel_id
       AND post.deleted = false
     GROUP BY contributions.scope_kind, contributions.author_id,
              contributions.depth, contributions.prefix
     ORDER BY contributions.scope_kind, contributions.author_id,
              contributions.depth, contributions.prefix;

    INSERT INTO channel_post_summary_state (channel_id, version, ready)
    VALUES (p_channel_id, 1, true)
    ON CONFLICT (channel_id)
    DO UPDATE SET version = 1, ready = true;
END;
$$;
