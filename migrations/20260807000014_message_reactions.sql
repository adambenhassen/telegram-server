-- message_reactions records per-user reactions to messages.
-- owner_id is the user who owns this copy of the message (i.e. the user whose
-- messages table carries the row with the given local_id). Each reacting user
-- gets their own row so that reactions are durable and queryable per owner.
--
-- The unique index on (owner_id, local_id, reactor_id) ensures one reaction
-- per reactor per message copy. reactor_id is the user who reacted.
-- reaction is a non-empty emoji string.
create table message_reactions (
    owner_id   bigint not null,
    local_id   bigint not null,
    reactor_id bigint not null,
    reaction   text   not null,
    created_at timestamptz not null default now(),
    constraint message_reactions_pk primary key (owner_id, local_id, reactor_id)
);

-- Index for looking up all reactions on a message copy (for getHistory).
create index message_reactions_msg_idx on message_reactions (owner_id, local_id);

-- Index for looking up all reactions by a specific reactor (for cleanup).
create index message_reactions_reactor_idx on message_reactions (reactor_id);
