-- Existing empty channels predate channel-create service messages. Backfill
-- only channels whose message id and pts streams are still untouched. Each
-- statement handles at most 500 channels; the update makes completed batches
-- ineligible if the migration is retried after an interrupted run.
DO $$
DECLARE
    batch_size INTEGER;
BEGIN
    LOOP
        WITH candidates AS MATERIALIZED (
            SELECT c.id, c.creator_id, c.title, c.date
            FROM channels c
            JOIN channel_state cs ON cs.channel_id = c.id
            WHERE cs.pts = 0
              AND cs.next_local_id = 1
              AND NOT EXISTS (
                  SELECT 1 FROM channel_messages cm WHERE cm.channel_id = c.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM channel_events ce WHERE ce.channel_id = c.id
              )
            ORDER BY c.id
            LIMIT 500
            FOR UPDATE OF cs
        ), inserted_messages AS (
            INSERT INTO channel_messages (channel_id, local_id, from_id, date, message, action_type)
            SELECT id, 1, creator_id, date, title, 1
            FROM candidates
            RETURNING channel_id, local_id
        ), inserted_events AS (
            INSERT INTO channel_events (channel_id, pts, type, local_id)
            SELECT channel_id, 1, 1, local_id
            FROM inserted_messages
            RETURNING channel_id
        )
        UPDATE channel_state cs
        SET pts = 1, next_local_id = 2, date = candidates.date
        FROM candidates
        WHERE cs.channel_id = candidates.id
          AND EXISTS (
              SELECT 1 FROM inserted_events e WHERE e.channel_id = candidates.id
          );

        GET DIAGNOSTICS batch_size = ROW_COUNT;
        EXIT WHEN batch_size = 0;
    END LOOP;
END
$$;
