# Roadmap

A from-scratch MTProto server in Go, built directly on gotd's exported packages
(`transport`, `exchange`, `crypto`, `mt`, `tg`) — no `tgtest`. Every milestone is
proven against a real gotd client as the compatibility oracle, with an E2E gate.

## Stack

- **Language:** Go.
- **Wire:** MTProto 2.0 over gotd codecs; `tg`/`mt` type schema (gotd v0.161.0).
- **Storage:** Postgres via `pgx/v5`; `sqlc`-generated queries; Atlas migrations.
- **At-rest crypto:** auth keys and SRP verifiers encrypted (keycrypt).
- **Realtime bus:** Postgres `LISTEN`/`NOTIFY` (no Redis/Kafka).
- **Quality gates:** `golangci-lint`; container-backed Postgres tests; per-milestone E2E.

## Architecture

- **One process = accept loop + key exchange + session bookkeeping + dispatch.**
  Auth keys live in shared Postgres, so any replica can serve any connection.
- **Two-sided messaging model.** Each account owns its own message-id space and
  its own `pts`. A send writes the sender's outbox row and the recipient's inbox
  row in one transaction under sorted advisory locks (deadlock-free).
- **Single update path.** A per-owner `message_events` log feeds one
  `buildUpdates` computation shared by `updates.getDifference` (the reliable
  pull) and real-time push (the optimization) — so a live push and a poll can
  never disagree.
- **Cross-replica delivery.** After an event commits, `NOTIFY`; each process runs
  a listener that pushes to that user's local connections. A missed notify loses
  nothing — the next `getDifference` backfills.

## Positioning

Existing open MTProto servers reach production scale by going wide and
distributed — many microservices, a message broker, a cache tier, service
discovery, separate object storage. This project takes the opposite bet.

**Design bet:** a small, fully-open, verifiable core that runs on one datastore
and no message broker. Postgres is the whole backend — rows *and* the realtime
bus (`LISTEN`/`NOTIFY`) — betting it carries update fan-out far enough that
Kafka/Redis/etcd stay unnecessary well into groups and channels. If a broker is
ever needed, it's an optimization behind the same `buildUpdates` path, not a
rewrite.

**Priorities in order:** correctness and openness first, operational simplicity
second, feature breadth last. Every milestone stays provable against a real gotd
client. Breadth is the deliberate trade; a core you can fully run, read, and
verify is the return.

## Current RPC surface

Auth & account
- `auth.sendCode`, `auth.signIn`, `auth.logOut`, `auth.checkPassword`
- `account.getPassword`, `account.getPasswordSettings`,
  `account.updatePasswordSettings`
- `account.getAuthorizations`, `account.resetAuthorization`

Users & help
- `help.getConfig`, `users.getUsers`

Messaging
- `messages.sendMessage`, `messages.getDialogs`, `messages.getHistory`,
  `messages.readHistory`, `messages.editMessage`, `messages.deleteMessages`,
  `messages.setTyping`, `messages.createChat`, `messages.addChatUser`,
  `messages.deleteChatUser`, `messages.editChatTitle`

Updates
- `updates.getState`, `updates.getDifference`

## Shipped

### M1 — MTProto core + login
- Accept loop, transport, Diffie–Hellman key exchange, encrypted message dispatch
  built directly on gotd's exported packages.
- `help.getConfig`, `auth.sendCode`, `auth.signIn`, `users.getUsers`.
- Postgres-backed users and login codes; Atlas migrations; sqlc queries; the
  build toolchain and lint gates.

### M2 — Auth hardening & persistence
- Auth keys persisted and encrypted at rest; single-read lookup per frame;
  fail-fast startup on an unmigrated schema.
- Last-seen advanced only on MAC-authenticated frames, so a crafted frame bearing
  a valid cleartext key id cannot spoof "last active".
- Login-code hardening: attempt caps, resend cooldown, exhaustion — closed the
  resend-cooldown bypass via code exhaustion.
- Session management: `account.getAuthorizations`, `account.resetAuthorization`,
  `auth.logOut`; background sweep drained on clean shutdown before the pool closes.
- Sign-in fails closed if auth-key→user binding writes zero rows.

### M3 — 2FA cloud passwords (SRP-6a)
- `account.getPassword`, `auth.checkPassword`, `account.updatePasswordSettings`,
  `account.getPasswordSettings`.
- SRP verifier encrypted at rest; per-user challenge eviction (one active
  challenge per user); strict input validation (reject non-canonical A/M1,
  degenerate verifiers).
- Half-authorized `pending_user_id` state between sign-in and password check —
  never grants access on its own; staging 2FA atomically clears any existing
  authorization.
- Email-only password updates are strictly non-destructive.

### M4 — 1:1 real-time messaging core
- Full messaging surface: send, dialogs, history, read state, edit, delete,
  typing; plus `updates.getState` / `updates.getDifference`.
- Two-sided storage + per-owner `pts`/event log; `random_id` send dedup
  (idempotent resend); edit/delete mirror both sides via a stored linkage.
- Real-time delivery: in-process session registry + `Conn.PushTo` (server-initiated
  writes serialized against replies, and skipped if the conn no longer belongs to
  the user the batch was built for) + `LISTEN`/`NOTIFY` fan-out.
- The `LISTEN` listener reconnects with bounded backoff and re-issues `LISTEN` on
  all three channels, so a broken Postgres connection interrupts delivery on that
  replica instead of ending it until restart.
- One update batch is built per user per notification and reused across that
  user's connections; the live connections one user may hold in a process are
  capped, so an account opening sockets in a loop cannot multiply delivery cost.
- Bounded difference: capped per batch, returns `differenceSlice` with an
  intermediate state when truncated; state read before events so the advertised
  `pts` never runs past an omitted event.
- Revoked session dropped and closed on its next frame, and closed on every
  replica without waiting for one via a `tg_evict` NOTIFY; a revocation aimed at
  the caller's own session publishes only once its reply is on the wire.
- E2E gates prove live send/read/edit/delete/typing between two gotd clients,
  offline `getDifference` backfill, and cross-process delivery over `LISTEN`/`NOTIFY`.

### M6 — basic group chats
- `chat` as a peer type; a `peer_type` discriminator column separates 1:1 and
  group rows in `messages` and `dialogs`.
- Membership tables track current members; the member set is the authorization
  boundary — an unknown chat and a non-member chat return the same
  `PEER_ID_INVALID`.
- Four RPCs: `messages.createChat`, `messages.addChatUser`,
  `messages.deleteChatUser`, `messages.editChatTitle`.
- Fan-out: a send to a chat writes one message row, one event, and one `pts`
  bump per member in a single transaction under sorted advisory locks, all
  sharing one `fanout_id`. Edit and delete walk the `fanout_id` copy set; only
  the author may delete, and service messages cannot be deleted.
- Service messages for chat create, member add, member remove, and title change;
  written in the same mutation transaction.
- 200-member cap enforced at create and add time; bounds the advisory-lock set
  and the write volume per transaction.
- Design bet confirmed: the per-owner `pts`/event-log model and
  `LISTEN`/`NOTIFY` delivery carried the first real fan-out unchanged — no
  per-chat `pts` stream, no broker, no change to the `buildUpdates` path.
  Offline `updates.getDifference` and real-time push use the same event log for
  chat messages as for 1:1.
- A removed member's dialog returns `ChatForbidden`; their message rows and `pts`
  are retained but `getHistory` is gated on current membership.
- E2E gates prove the chat lifecycle live between real gotd clients — create,
  send, title change, member add and remove, and a removed member going inert —
  plus offline `getDifference` backfill of chat messages and service messages,
  and cross-replica delivery over `LISTEN`/`NOTIFY`.

## Planned — feature track

### M5 — Media & files
- `upload.saveFilePart` / `upload.saveBigFilePart`, `upload.getFile`.
- Photo/document messages, thumbnails, `messages.sendMedia`.
- Blob store abstraction: local filesystem first, RustFS (S3-compatible) later.
- Reference-counted storage; file `access_hash` and expiry.

### M7 — Channels & supergroups
- Broadcast channels and megagroups; the channel `pts` stream.
- `channels.*` surface (create, join/leave, getMessages, editAdmin, editBanned).
- Larger fan-out likely forces the `message_events` GC + `differenceTooLong`
  path (see deferrals).

### M8 and later
- Secret chats (end-to-end, separate key exchange, `qts` stream).
- Message features: forwarding, reply threading, reactions, pinned messages,
  scheduled messages.
- Contacts, usernames, and real peer `access_hash` derivation.
- Presence/online status, `updateUserStatus`.
- Server-side full-text search.

## Planned — operational track

Runs in parallel with features; currently the weakest area for production.

- **Packaging & deploy.** No Dockerfile, compose, or k8s manifests yet. Add a
  container image, a local `docker-compose` (server + Postgres), and k8s
  manifests (the design assumes horizontal replicas behind a load balancer).
- **CI.** No pipeline yet. Add build + `go test ./...` + `golangci-lint` +
  `atlas migrate validate` on every push.
- **API layer target.** Pin and document a target Telegram API layer, and track
  the gotd schema version the server is validated against.
- **Multi-DC.** Config advertises a single DC (self). Real Telegram clients expect
  a DC list and migration; needs a DC registry and `PHONE_MIGRATE`/`NETWORK_MIGRATE`.
- **Observability.** Structured logs exist; add metrics (connections, pts lag,
  push latency, NOTIFY throughput) and tracing.
- **Rate limiting & abuse.** Per-account/per-IP limits beyond the login-code
  cooldown; flood-wait on messaging RPCs, `messages.createChat`, and
  `messages.addChatUser`. The 200-member cap bounds one transaction but is not a
  rate control: one account can create a 200-member chat and send in a loop,
  each send holding up to 200 advisory locks and serialising against 200
  uninvolved accounts' 1:1 sends. Additionally, `messages.getDialogs` does not
  paginate the chat list, so an account in many chats makes each dialog call
  expensive independently — a cost input for sizing the flood-wait limit.
- **Backups & retention.** Postgres backup story; `message_events` retention.

## Known deferrals & tech debt

Tracked so shortcuts don't rot into "later means never".

- **`message_events` GC + `updates.differenceTooLong`.** All events retained;
  `getDifference` is capped and returns `differenceSlice` when truncated, but
  there is no event-log trimming or too-long path yet. — M4.
- **Peer `access_hash`.** Placeholder scheme: `access_hash == user_id`, validated
  but not cryptographically derived. Real hashing deferred; it ships with a
  peer-lookup RPC or not at all, since the self-satisfying check is currently the
  only way to name a peer you have never talked to. — M8.
- **`qts`.** Column kept at 0; no secret-chat / bot update stream. — M4.
- **Client-pts-ahead resync.** A client `pts` past the server is clamped to empty
  (single-writer invariant), not an explicit resync response. — M4.
- **`seq` on update envelopes.** Minimal; gotd relies on `pts` for dedup, so `seq`
  is not yet a meaningful per-user counter. — M4.
- **Chat admin rights.** Any member may add or remove any member, including the
  chat's creator. `ChatAdminRights` is not stored or enforced; `revoke_history`,
  `fwd_limit`, chat photos, and invite links are not implemented. `revoke_history`
  and `fwd_limit` are accepted and ignored, so a removed member keeps their own
  copies of past messages and a new member sees no history before they joined. — M6
- **Removed-member history access.** A removed member's dialog returns
  `ChatForbidden`; their retained message rows and `pts` are reachable only
  through `updates.getDifference` replay, not through `messages.getHistory`. — M6
- **Chat read state and chat typing.** `messages.readHistory` and
  `messages.setTyping` reject an `InputPeerChat` with `PEER_ID_INVALID`. No
  unread count advances for a chat and no `updateReadHistoryOutbox` is emitted,
  so a group sender sees no read ticks. Typing additionally needs `peer_type` in
  the `LISTEN`/`NOTIFY` payload, which today carries a bare user id. — M6
- **`messages.getChats`, `messages.getFullChat`, `messages.migrateChat`.** Not
  implemented. A client learns a chat's title and version from the `Chats` list
  on updates and dialogs, and cannot fetch the participant list. — M6
- **No control over who may add you to a group.** Any account can create a chat
  and add any user id it can name, giving it a push channel into that user's
  update stream until they leave, and nothing stops it re-adding them. The check
  needs contacts or a block list, neither of which exists yet. — M6
- **Chat message deletion has three gaps and they are one decision.** Chat
  deletion is closed to the author, service messages cannot be deleted at all,
  and an author who has left the chat can no longer delete their own messages.
  Fail-closed, and three things follow. `messages.deleteMessages` has no `revoke`
  flag, so every delete that is allowed is a delete-for-everyone. A member cannot
  clear anyone else's message from their own view. And a message left behind by a
  departed member is permanent: there is no moderation path, so the creator
  cannot remove abusive content. 1:1 keeps its existing posture, where either
  side deletes both copies. Whoever picks this up decides author-delete,
  self-only delete and creator moderation together across both peer types —
  deciding them one at a time is how the two peer types diverge. — M6
- **A difference batch fetches the participant list once per create event.**
  `maxDiffEvents` caps a batch at 500 events, not at payload size, so 500
  `ChatActionCreate` rows in one `updates.getDifference` mean 500 `IsMember`
  plus 500 `Participants` round trips, and at the 200-member cap up to 100k user
  ids in a single response. Reaching it needs an account that created 500 chats
  naming the victim, which is the primitive already accepted as residual for M6.
  Raised as low, not a gate. — M6

## Engineering invariants

Apply to every milestone.

- Errors are values; fail closed at trust boundaries; never `_ =` an error.
- Every messaging/account RPC requires a bound `UserID`, else
  `AUTH_KEY_UNREGISTERED`.
- Wire integers (`pts`/`date`/`seq`/message id) are gotd `int`; DB columns are
  `BIGINT`; convert at the wire boundary.
- Per-change gates: `go build ./...`, `go test ./...`, `golangci-lint run`,
  `atlas migrate hash|validate`.
- A real gotd client is the compatibility oracle; each milestone ships an E2E gate.
