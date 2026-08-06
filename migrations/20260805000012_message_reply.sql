-- Add reply_to_msg_id column to messages table
-- Supports quoting replies to specific local_message_id.
alter table messages
    add column reply_to_msg_id integer null;
