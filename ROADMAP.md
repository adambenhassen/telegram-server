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
- `auth.sendCode`, `auth.signIn`, `auth.signUp`, `auth.logOut`, `auth.checkPassword`
- `account.getPassword`, `account.getPasswordSettings`,
  `account.updatePasswordSettings`
- `account.getAuthorizations`, `account.resetAuthorization`
- `account.updateStatus`
- `account.updateUsername`

Users & help
- `help.getConfig`, `users.getUsers`

Contacts
- `contacts.resolvePhone`, `contacts.resolveUsername`, `contacts.search`

Messaging
- `messages.sendMessage`, `messages.getDialogs`, `messages.getHistory`,
  `messages.readHistory`, `messages.editMessage`, `messages.deleteMessages`,
  `messages.setTyping`, `messages.forwardMessages`, `messages.sendReaction`,
  `messages.updatePinnedMessage`, `messages.createChat`, `messages.addChatUser`,
  `messages.deleteChatUser`, `messages.editChatTitle`, `messages.search`,
  `messages.searchGlobal`

Media
- `upload.saveFilePart`, `upload.saveBigFilePart`, `upload.getFile`,
  `messages.sendMedia`

Updates
- `updates.getState`, `updates.getDifference`

Channels
- `channels.createChannel`, `channels.getChannels`, `channels.joinChannel`,
  `channels.leaveChannel`, `channels.editAdmin`, `channels.editBanned`,
  `channels.getMessages`, `channels.updateUsername`
- `messages.exportChatInvite`, `messages.checkChatInvite`,
  `messages.importChatInvite`, `messages.revokeExportedChatInvite`
- `updates.getChannelDifference`

Secret chats
- `messages.getDhConfig`, `messages.requestEncryption`, `messages.acceptEncryption`,
  `messages.discardEncryption`, `messages.sendEncrypted`, `messages.receivedQueue`

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

### M5 — Media & files
- RPC surface: `upload.saveFilePart`, `upload.saveBigFilePart`, `upload.getFile`,
  `messages.sendMedia`.
- Blob store abstraction behind a two-method interface (`WriteAt`, `ReadAt`) with a
  local filesystem backend. The interface is range-oriented — `ReadAt(key, offset,
  limit)` rather than whole-object — which is what keeps encryption-at-rest a layer
  to add later rather than a rewrite.
- Documents only. No photos, no thumbnails. Serving a `tg.Photo` requires pixel
  dimensions the server cannot produce without decoding an uploaded image, which
  would put an image parser on attacker-supplied bytes in the main process.
  Stored document attributes are limited to `mime_type`, `file_name`, and `size` —
  the three the server actually knows.
- **Download authorization is ownership of a message row**, not a capability.
  `upload.getFile` serves a file only when the caller owns a non-deleted `messages`
  row whose `file_id` matches. This was possible without a new model because
  `messages` is per-owner and a chat send already writes one row per entitled
  account, so the entitled set is fully enumerated on disk. What it buys: a leaked
  `(id, access_hash)` pair is inert in a stranger's hands, and deleting a media
  message actually revokes retrieval on both sides instead of being cosmetic.
- Every download rejection — unknown file, wrong `access_hash`, no owning row —
  returns one identical `LOCATION_INVALID`. `files.id` is dense `BIGSERIAL`;
  two distinguishable errors would be an enumeration oracle over the whole corpus.
- Upload parts land in Postgres under `PRIMARY KEY (user_id, file_id, part_index)`.
  The client chooses `file_id`; without `user_id` in the key, one account could
  write bytes into another account's in-flight upload.
- Upload bounds: 512 KiB per part (protocol maximum); `TG_MAX_FILE_BYTES` caps one
  assembled file (100 MiB default); a per-user outstanding cap of twice that on
  unassembled bytes and on part row count; `TG_MAX_USER_STORAGE_BYTES` (2 GiB
  default) checked at assembly; a `TG_UPLOAD_PART_TTL` sweeper (6h default) on a
  TTL/4 ticker.
- `file_total_parts` is validated on `saveBigFilePart` but not stored; the per-file
  byte cap is enforced on every individual save inside the store transaction, so
  a client changing its declared total between calls cannot get more bytes into the
  parts table than the cap allows.
- The client-supplied `mime_type` and file name are constrained at the boundary
  (`mime_type`: ≤255 printable ASCII, must match `type/subtype`; file name: the
  existing `validText` gate plus a CR/LF check) and stored as opaque metadata.
  Neither ever selects a code path, and the blob key is derived from the file id
  alone so traversal is closed by construction.
- One `upload.getFile` in flight per account, enforced by a per-account mutex. This
  composes with the existing per-user connection cap (20 connections per account per
  replica) into a bound that holds without a tuned rate number.
- `file_reference` is a placeholder — the 8-byte big-endian file id, echoed
  deterministically and ignored on input. Half-validating it would make it an
  oracle; ignoring it does not.
- E2E gate (`TestMediaRoundTrip`, `TestMediaDownloadRequiresOwnMessage`,
  `TestMediaInChatFanOut`): A uploads a 512 KiB + 7 byte multi-part payload,
  sends it to B, and B downloads bytes identical to what A uploaded; C holds the
  exact `(id, access_hash)` pair but owns no message row and is refused with
  `LOCATION_INVALID`; a chat send to a three-member group entitles all members
  to download via one stored `files` row.

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

### M7 — Channels & supergroups
- Channel as a peer type; megagroup and broadcast share one `channels` table
  with a `megagroup` flag, differing only in who may post: admins in broadcast,
  anyone in megagroup.
- One message row per channel, not per member. A channel post writes a single
  `channel_messages` row that every entitled member reads. The per-account
  `pts` stream cannot represent an event whose audience grows after the fact,
  so channels carry their own: `channel_state` + `channel_events`, keyed by
  channel the way `update_state` + `message_events` are keyed by user. This is
  the second update stream the architecture now holds.
- Admission is by invite hash alone; there is no join-by-channel-id path.
  At M7, `channels.id` was dense BIGSERIAL and the peer `access_hash` placeholder
  was `access_hash == id`, so a join keyed on the id would let any account
  enumerate and join every channel on the server. The invite hash was the
  secret; the channel id was never an input to the join path. That rule is
  unchanged: rows created before the MAIN-246 cutover remain dense, new ids are
  sparse random draws from `randomChannelID`, and density is no longer the
  premise — the per-viewer HMAC `access_hash` (see M8) is the guard.
  `20260729000008_channels.sql` is hashed and cannot be amended; the corrected
  rationale is in `20260819000026_channel_id_random.sql`.
- Coarse `role` + `banned_until` rights model: 0 = member, 1 = admin,
  2 = creator. `ChatAdminRights` and `ChatBannedRights` flag sets are accepted
  and collapsed to a single bit. An admin may post in a broadcast channel; a
  member may not.
- One `NOTIFY` per post, not one per member. Each replica's listener checks
  whether it holds connections for any of the channel's members and pushes to
  those; a server with 10 000 members in a channel emits exactly one
  notification.
- Design bet extended: `LISTEN`/`NOTIFY` delivery and the `buildUpdates` push
  path carry channel fan-out without a broker and without per-member event
  rows. The second `pts` stream was the only structural addition; the delivery
  and difference machinery reuse the M4 foundation.
- `join_pts` records the channel's `pts` at the moment each member joined and
  floors the channel difference for that member, bounding cold-start cost.
- E2E gates prove the channel lifecycle live between gotd clients — create,
  join by invite, post, get history, editAdmin, editBanned, leaveChannel, and
  `getChannelDifference` backfill.

### M8 — Peer identity
- Derived per-viewer `access_hash` on every user and channel the server puts on
  the wire. The value is HMAC-SHA256 over a versioned label with fixed-width
  big-endian viewer id, peer kind, and peer id, truncated to 64 bits. Peer kind
  is included because user and channel ids share no sequence and collide
  numerically.
- The derivation subkey is produced at process start from the auth-key master via
  HKDF; only the subkey reaches the RPC layer, so the master's reach stays at
  storage. Stateless: no new column, no migration, no new secret.
- An `access_hash` is a peer reference, not an admission credential. Holding a
  valid derived hash for a channel you are not a member of leaves you no more a
  member than before; channel admission is still the invite hash and the
  participant row. Per `(viewer, peer)` pair: a hash issued to one account is
  inert in another's hands, and a leaked pair confines the damage to one
  already-identifiable account.
- Hard cutover from the M1 placeholder (`access_hash == id`) for both user and
  channel peers, sequenced so the lookup RPC lands before the placeholder retires.
  `InputPeerChat` is untouched — chat membership is their admission boundary and
  chats carry no hash.
- Per-pair derivation hardens the one-update-batch-per-user invariant: a rendered
  peer is valid for exactly one viewer, so a push batch built for one user may
  never be shared with another. The `buildUpdates` path held this before M8; M8
  makes it a stated requirement.
- A per-pair hash is not transferable between accounts, so forwarding and any
  contacts export must re-derive for the recipient rather than pass a peer
  reference along. If key handling ever adds session-surviving rotation, the peer
  hash must gain an epoch or an accept-previous window in the same change or every
  cached peer on every live session breaks silently.
- `contacts.resolvePhone` — resolve a phone number to a peer, for a caller who
  knows the number out of band. Per-account quota: 20 distinct phones per 24-hour
  rolling window, durable DB counter. Miss and refusal return the same error; the
  endpoint is not an existence oracle. Phone normalization (strip leading `+`)
  unified across sign-in and lookup paths.
- `messages.revokeExportedChatInvite` — retire a specific invite hash (admin-only,
  idempotent). Revocation is per hash: other outstanding hashes for the same
  channel continue admitting and no existing member is removed. A revoked hash
  returns the same error as an unknown hash; revocation is not detectable by the
  holder.
- Every rejection on peer-hash and invite-revocation paths returns one uniform
  error; neither a bad hash nor a revoked invite is an existence oracle.
- Batched channel dialog reads: `getDialogs` fetches all channel rows in a single
  query rather than one per channel.
- E2E gates prove stranger-to-stranger start via `contacts.resolvePhone`, that the
  M1 placeholder hash is refused for user and channel peers, and that a hash issued
  to one account is refused when submitted by another.

### M9 — Online/offline presence
- `account.updateStatus` as the explicit online/offline toggle. Online/offline state is per-user,
  not per-connection: a user with two open connections stays online until their last connection
  closes or they call `account.updateStatus(offline=true)`. A single close on a multi-connection
  account is a no-op for status.
- Connection lifecycle is the implicit trigger. The mtproto server exposes an `OnStatusChange`
  callback that fires `online=true` when a session first binds to a user on this process and
  `online=false` when their last session exits. The production wiring calls `SetUserStatus` then
  `NOTIFY` on `tg_status`, so connect and disconnect follow the same fan-out path as the explicit
  RPC.
- Fan-out over `LISTEN`/`NOTIFY`. Each replica's listener calls `DeliverStatus` on a `tg_status`
  notification: it queries the changed user's 1:1 dialog partners and pushes `updateUserStatus` to
  each partner's live connections on that replica. The changed user's own connections receive no
  push — the client updates its own display from the RPC response, not a server-initiated message.
- Fan-out target is the set of accounts sharing a non-deleted 1:1 dialog with the changed user. A
  user who shares no dialog receives no status push for them. Group and channel membership do not
  contribute to the target set; presence for bots and channels is out of scope as a user-status
  concept.
- `UserStatusRecently` is the self-status sentinel. `userToTL` always returns it when `self=true`,
  so a caller's own account never discloses its real online state to itself. Every other peer
  renders `UserStatusOnline` (5-minute `Expires`, the canonical Telegram value, not tunable) or
  `UserStatusOffline` (with `WasOnline` from the stored `last_seen_at` timestamp). An account whose
  `last_seen_at` is null — created but never put through the status lifecycle — renders as
  `UserStatusEmpty`, not `UserStatusOffline{WasOnline:0}`.
- Status pushes are transient: they carry no `pts`, are never written to the per-owner event log,
  and never appear in `updates.getDifference`. A missed push is not a loss — the partner's next
  `getDialogs` or `users.getUsers` reflects the current stored state.
- E2E gates prove: connect triggers an online push to dialog partners; disconnect triggers an offline
  push with a nonzero `WasOnline` timestamp; `account.updateStatus(offline=true)` while still
  connected pushes offline without closing the connection; `getDialogs` returns `UserStatusOnline`
  for a connected peer; a never-connected account renders as `UserStatusEmpty`; self always returns
  `UserStatusRecently`; a user sharing no dialog receives no status push for the changed user.

### M10 — Secret chats
- Key exchange RPC surface: `messages.requestEncryption`, `messages.acceptEncryption`,
  `messages.discardEncryption`, `messages.getDhConfig`. Diffie–Hellman parameters
  served from `dh_config`; key fingerprint stored and verified on accept.
- `messages.sendEncryptedMessage` — opaque relay: the server stores and fans out the
  encrypted blob without inspecting it; no plaintext ever leaves the sender's device.
- `receivedQueue` acknowledgement: the server records which encrypted message ids the
  recipient has confirmed so the sender can clear its local outbox.
- `updates.getDifference` qts gap recovery: missed secret-chat updates are replayed
  via the `qts` stream so clients that come back online do not lose events.
- Schema additions: `encrypted_events` table for the opaque relay log,
  `secret_chats` table for per-chat key state and metadata,
  `update_state.qts` counter advancing on each secret-chat event.

### M11 — Message features
- **Reply threading.** `reply_to_msg_id` stored and echoed on send; history and
  update payloads carry the reply reference so clients can render threads. Covers
  1:1 and group-chat peers; channel posts do not carry reply threading (MAIN-328).
- **Message forwarding.** `messages.forwardMessages` with a forwarded-from header;
  per-pair `access_hash` re-derived for the recipient rather than passed through.
  Channel peers are not valid forwarding destinations; forwarding a channel post to
  a 1:1 or group destination carries the channel peer and post id in `FwdFrom` and
  is gated on the forwarder's current channel membership.
- **Reactions.** `messages.sendReaction` sets or clears the caller's emoji reaction
  on 1:1 and group-chat messages; channel messages are out of scope. Reaction counts
  embedded in `getHistory` and update payloads; `updateMessageReactions` pushed to
  all entitled peers on change. Reactions are rendered on read paths and pushed on
  change; `messages.getMessagesReactions` refetches reactions for a bounded set of
  message ids (up to 100) for the caller's own messages in 1:1 and group chats; channel
  peers are refused.
- **Pinned messages.** `messages.updatePinnedMessage` pins or unpins a message;
  admin-only in channels; `updatePinnedMessages` pushed to members carrying the
  current pinned message id.
- E2E gates (`test/e2e/`): reply threading — `reply_test.go`; forwarding —
  `forward_test.go` (1:1, group, channel-origin, auth rejection, dedup, multi-id);
  reactions — `reactions_test.go` (realtime and chat fan-out); pinning —
  `pinned_test.go` (chat and channel).

### M12 — Usernames & public channels
- Shared `usernames` table: globally unique, case-insensitive handles covering
  both user and channel names.
- `account.updateUsername` — self-service username set/clear; 5–32 chars
  `[a-z0-9_]` letter-first; reserved-handle blocklist; 2 changes/24h rate limit.
- `channels.updateUsername` — admin-only; same validation and rate limit as
  `account.updateUsername`.
- `contacts.resolveUsername` — resolves @username to user or channel peer with
  per-viewer `access_hash`; 100 distinct lookups/24h + 20/min burst cap;
  pre-charged quota (no hit/miss oracle).
- `channels.joinChannel` extended: public channel join via resolved `access_hash`;
  publicness verified inside the admission transaction (TOCTOU-safe); same ban and
  cap checks as invite-join; `join_pts` set under the state lock.
- E2E gate covers: set/resolve user username, uniqueness conflict, public channel
  join and post delivery, private channel refuses direct join, rate limit.

### M13 — Server-side full-text search
- Schema additions: `message_tsv` tsvector column on `messages`, `name_tsv`
  tsvector column on `users`; GIN indexes on both; `'simple'` dictionary (no
  stemming, exact-token match across all locales).
- `messages.search` — full-text keyword search over the caller's own outbound
  messages within a named 1:1 dialog (`out = true`). Non-user peers and
  inbound-message search are out of scope (extended in M15). Authorization
  boundary: results are bounded to rows the caller owns and sent — a received
  message is never returned even if the caller can read it via `getHistory`.
- `contacts.search` — full-text search over display names (first name + last
  name) of the caller's 1:1 dialog peers. Results are scoped to accounts that
  share a non-deleted dialog with the caller; username fields are not searched.
- E2E gates: `TestSearchMessages` proves keyword matching and the outbound-only
  boundary; `TestContactsSearch` proves dialog-scoped name matching and that
  non-dialog users are excluded.

### M14 — Rate limiting and abuse controls
- Postgres-backed rate limiter: O(1) state per (subject, surface), constant row
  operations per check-and-consume, exact under concurrency (no cross-subject
  serialization), old state self-cleans without unbounded row growth. Limits are
  env-configured per surface with documented defaults; an explicit zero disables
  enforcement for that surface.
- Dynamic `FLOOD_WAIT_<seconds>` error contract (error 420) with the real remaining
  wait (minimum 1 second) for all limiter-issued denials. A denied request stores
  nothing, advances no pts, delivers no update — no partial effect anywhere.
- Per-account limits shipped with defaults (all env-overridable): message sends
  (all paths — 1:1, chat, channel, media — share one budget) 60/60s;
  `messages.createChat` 20/24h; `messages.addChatUser` 120/24h;
  `channels.createChannel` 20/24h; `messages.search` 300/hr;
  `contacts.search` 300/hr; `upload.saveFilePart` and `upload.saveBigFilePart`
  600/60s on one shared budget.
- Connection-layer client-address plumbing: the peer address is captured before
  any client bytes are interpreted and carried with the request. Two trust modes:
  `socket` — the connection's own peer address, the default — and PROXY-v2 — the
  client address from a PROXY protocol v2 header, accepted only from configured
  balancer CIDRs (an empty allowlist in PROXY mode is a startup failure). No
  autodetection and no heuristics; any unsupported mode value is a startup failure.
  Fail closed in both directions: an allowlisted source with no valid PROXY v2
  header is refused; a PROXY header from any non-allowlisted source is never
  honoured. In `socket` mode with per-IP limits enabled, the server logs once at
  warn level that running behind a proxy without switching to PROXY mode collapses
  every client into one bucket.
- `auth.sendCode` per-IP limit: 10 calls per hour per IP key (IPv4 /32, IPv6 /64);
  20 distinct phone numbers per IP key per 24h. The IP check runs before any
  phone-dependent work; denial is uniform regardless of whether the phone is
  registered or a code is already live, preserving the no-registration-oracle
  property. Postgres-backed so the limit holds across replicas.
- Upload-part TTL measured from insert time, not last-touch: re-saving a part no
  longer resets its expiry. Expired-part sweep runs in bounded batches.
- Deliberately not in M14: `upload.getFile` rate number (still needs measured
  per-replica read throughput under concurrent download; the M14 limiter makes it
  cheap to add once measured); escalating penalties for repeated denials; per-IP
  coverage beyond `auth.sendCode` — unauthenticated key exchange (RSA/DH CPU an
  attacker can burn before ever calling sendCode) and per-IP concurrent-connection
  cap, both needing throughput measurement rather than judgment, and both cheap to
  add with the M14 plumbing in place; coarser /48 grouping against IPv6 address
  rotation; `messages.getDialogs` limiting and pagination; metrics on limit hits
  (observability milestone; structured logs on denial are available). Accepted
  residual: `socket` mode behind a load balancer without PROXY mode configured gives
  one global bucket instead of per-client ones — mitigated by a startup warning,
  deliberately not by autodetection.
- E2E gates prove: over-limit message send returns `FLOOD_WAIT_<n>` to a real gotd
  client, a different account is unaffected, and the limited account succeeds after
  the window; the full login flow completes under default per-IP limits.

### M15 — Extended search surface
- Schema additions: `message_tsv` tsvector column on `channel_messages`
  (GIN index, `'simple'` dictionary) covering post text; `title_tsv` tsvector
  column on `channels` (GIN index, `'simple'` dictionary) covering channel
  titles.
- `messages.search` extended to three peer types. User-peer search now matches
  both inbound and outbound messages — the M13 `out = true` restriction is
  lifted. Chat-peer search (membership-gated) searches the caller's own message
  rows for that chat. Channel-peer search (membership-gated) searches
  `channel_messages`, returning `MessagesChannelMessages`, the same split
  `getHistory` makes for channels.
- `contacts.search` extended beyond M13's dialog-scoped user-name matching.
  The reply carries two vectors: `MyResults` holds dialog-peer users plus
  channels the caller belongs to (including private ones) under one shared
  budget — users fill first, channels take what remains, both ordered by id;
  `Results` holds public channels discovered by title under its own separate
  limit, so the caller's memberships cannot shrink caller-independent
  discovery. A non-member's query against a private channel returns no match
  and no existence signal — the visibility filter runs in SQL before the result
  limit, not as a post-filter.
- `messages.searchGlobal` — cross-dialog keyword search over every peer type
  the caller participates in (user, chat, and channel) in one reply. The
  result set is the union of two authorized sets — owned rows for user and
  chat peers, membership-gated shared rows for channel peers — and widens
  neither. `folder_id`, `min_date`/`max_date`, and the broadcasts/groups/
  users-only booleans are accepted and silently dropped; any non-empty
  `Filter` value is rejected with the same error `messages.search` uses. A
  page that loses rows to a concurrent delete signals retryably rather than
  reading as exhausted.
- Search authorization invariants established (rules a future milestone must
  not silently undo): each peer type's existing read predicate is reused and
  never widened — ownership for user and chat rows, current membership for
  channel rows; channel members search the full post history because
  `join_pts` is a cost control, not a confidentiality boundary; a non-member
  keyword query against a private channel returns the same error as a query
  against a non-existent channel (no existence oracle); the `'simple'`
  dictionary applies to all search surfaces with no stemming.
- E2E gates prove: keyword search over user peers including inbound matches
  (`TestSearchMessages`); chat-peer search and non-member rejection
  (`TestSearchChatPeer`); channel-peer post search (`TestSearchChannelPosts`);
  public channel discovery by title, private-channel invisibility to
  non-members, and the two-vector result structure
  (`TestContactsSearchChannelDiscovery`); cross-dialog search across all three
  peer types (`TestSearchGlobalAcrossDialogs`).

## Planned — operational track

Runs in parallel with features; currently the weakest area for production.

- **Packaging & deploy.** Dockerfile and a `docker-compose` (server + Postgres)
  are in the repo. Add k8s manifests (the design assumes horizontal replicas
  behind a load balancer).
- **CI.** Pipeline runs on every push and pull request: build, `golangci-lint`,
  `atlas migrate validate`, and `make test` (full suite including e2e); a `docker`
  job builds the image and smoke-boots it against a migrated database; a `compose`
  job proves the named volumes survive a stack restart.
- **API layer target.** Pin and document a target Telegram API layer, and track
  the gotd schema version the server is validated against.
- **Multi-DC.** Config advertises a single DC (self). Real Telegram clients expect
  a DC list and migration; needs a DC registry and `PHONE_MIGRATE`/`NETWORK_MIGRATE`.
- **Observability.** Structured logs exist; add metrics (connections, pts lag,
  push latency, NOTIFY throughput) and tracing.
- **Rate limiting & abuse.** M14 shipped per-account flood-wait on message sends,
  `messages.createChat`, `messages.addChatUser`, `channels.createChannel`,
  `messages.search`, `contacts.search`, and `upload.saveFilePart`; per-IP limits
  on `auth.sendCode`; and connection-layer client-address plumbing with `socket`
  and PROXY-v2 trust modes. Remaining open items: `upload.getFile` rate number
  (needs measured per-replica read throughput under concurrent download);
  escalating penalties for repeated denials; per-IP coverage beyond
  `auth.sendCode` (unauthenticated key exchange and per-IP concurrent-connection
  cap); coarser /48 grouping against IPv6 address rotation; `messages.getDialogs`
  limiting and pagination; and metrics on limit hits (observability milestone).
- **Backups & retention.** Postgres backup story; `message_events` retention.

## Known deferrals & tech debt

Tracked so shortcuts don't rot into "later means never".

- **`message_events` GC + `updates.differenceTooLong`.** All events retained;
  `getDifference` is capped and returns `differenceSlice` when truncated, but
  there is no event-log trimming or too-long path yet. — M4.
- **Peer-hash key epoch.** The `access_hash` derivation has no epoch field.
  Session-surviving key rotation is not implemented — rotation today is a total
  re-auth event, so every cached peer reference is already invalid when it happens
  and an epoch buys nothing in the current model. If rotation ever becomes
  session-safe, an epoch or accept-previous window must be added to the derivation
  in the same change, or every cached peer on every live session breaks silently.
  — M8.
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
  `ChatForbidden`; their retained message rows and `pts` are reachable through
  `updates.getDifference` replay and through `messages.searchGlobal` (the owned
  arm reaches rows by `owner_id` without a membership check), but not through
  `messages.getHistory`. The
  `ChatForbidden` title is served empty rather than live, a deliberate divergence
  from upstream — which is assumed to serve the chat's current title — because any
  remaining member may rename the chat and that would keep writing into a removed
  account. The cost is that a removed member's client renders the chat nameless on
  a fresh sync. — M6
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
- **No blob deleter.** M5 maintains no reference count and deletes no stored file
  body. A delete removes only that message's own copies — the caller's row and,
  on a revoke, the mirror for a user-peer message, or every current-member
  fan-out copy for a chat message — so a forward or a channel post naming the
  same file keeps it retrievable through the download gate, but the bytes stay
  on disk indefinitely. A deletion request is not erasure, so
  any retention or deletion promise made to a user is false for media until a
  deleter ships. The reference set is `messages.file_id` and
  `channel_messages.file_id`: a file id is live when any non-deleted row in either
  table names it. The two are not symmetric: `channel_messages.file_id` carries a
  foreign key to `files`, so a database-level delete of a `files` row is refused on
  the channel side and silently orphans the message side. The forward path copies the
  source file id into new message rows, so references to an existing blob can appear
  at any time. A future deleter derives its count from both tables rather than from a
  stored counter, which is why no counter exists to drift. — M5
- **Per-account storage is a lifetime quota.** Nothing decrements
  `TG_MAX_USER_STORAGE_BYTES`, so an account that reaches it can never upload
  again, and the aggregate disk is `accounts × quota` of permanent storage. That
  runway's length is the argument for the deleter's priority. — M5
- **No encryption at rest for media.** Deliberate, with a stated trigger rather
  than a date: the blob store is a directory on the same host as the process
  holding `TG_AUTHKEY_ENC_KEY`, so anything reaching the blobs already reaches the
  key. It stops being free the moment the blob backend is a separate service with
  separate credentials — the RustFS/S3 step. The design constraint that keeps it
  cheap at that point: the sealing layer must be chunk-framed AEAD with a
  per-chunk nonce, not one `Seal` over the object, because a single-object seal
  cannot serve a byte range without decrypting the whole file per request. — M5
- **`file_reference` is a placeholder** — the 8-byte big-endian file id, echoed
  deterministically and ignored entirely on input. Half-validating it would make
  it an oracle, which is why it is ignored rather than partially checked. — M5
- **No rate limit on `upload.getFile`.** One in-flight download per account is the
  bound M5 ships. M14 explicitly deferred the rate number: it cannot be chosen
  without measured per-replica read throughput under concurrent `getFile`; the
  M14 limiter infrastructure makes it cheap to add once that measurement exists.
  — M5, M14
- **No content scanning of any kind** — no malware detection, no format validation,
  no sniffing. An explicit non-goal: the server stores and returns opaque bytes. — M5
- **A removed member retains access to media posted while they were a member.**
  They keep their `messages` rows (the M6 `revoke_history` deferral), so the
  ownership gate still passes. This is the existing M6 posture applied unchanged
  to a new content type, not a new residual — see the removed-member history access
  deferral above. — M5
- **`upload.saveBigFilePart` has no E2E path.** Unit coverage exists in
  `internal/api/upload_test.go`, but the `InputFileBig` wire shape has never
  been exercised against a real gotd client. The logic is the same as
  `saveFilePart` with the addition of `file_total_parts` validation; the gap is
  coverage debt, not a defect. — M5
- **`deleteMessages` soft-deletes both sides of a user-peer exchange.** The store
  deletes both the owner's row and the mirror row in one transaction, so either
  side deleting a message revokes the other side's download access too. Upstream
  `messages.deleteMessages` deletes only for the caller unless `revoke` is set.
  Whether the current two-sided behaviour is a deliberate M5 simplification or a
  defect, and whether M5 wants the `revoke` flag at all, is the open question
  tracked in MAIN-79. — M5
- **Full history to any current member.** `messages.getHistory` serves a member
  the channel's whole history, including posts from before they joined.
  `join_pts` bounds only how far back `getChannelDifference` will replay — a
  cost control, not a confidentiality boundary. Nothing may later be built on it
  as one. — M7
- **Invite hash is bearer-grade.** No expiry, no usage limit, no per-invite
  revocation. `expire_date` and `usage_limit` are accepted and silently ignored.
  A leaked hash is a leaked channel until the row is deleted. — M7
- **No audit trail for membership or role changes.** No service message is written
  for a join, a promotion, or a ban; the participant row records only the current
  state. Prior states are not recoverable. — M7
- **`ChatAdminRights` and `ChatBannedRights` flags collapsed to one bit.** The
  coarse `role` column maps any right set to admin (role 1) and collapses every
  partial restriction to the same banned state as a full ban. Individual flags are
  accepted and discarded. With no fine-grained rights, an admin who bans every
  other member holds the only mutable position, recoverable only by the creator. — M7
- **No `channelDifferenceTooLong`.** Nothing trims `channel_events`, so the
  too-long path is unreachable. Rides with the `message_events` GC deferral
  already recorded above. — M7
- **No typing, read state, or unread counts for channels.** Channel dialogs are
  appended unpaged to `getDialogs`; a client in many channels makes each dialog
  call expensive. — M7
- **No channel ownership transfer.** A creator who leaves cannot assign the
  creator role to another member. Once the creator leaves, no admin can elevate
  themselves to creator. — M7
- **No channel media send path.** `messages.sendMedia` rejects channel peers;
  a channel post cannot carry a document in M7. The download side is already
  built — the M5 `FileForDownload` gate grew its channel branch in M7 — so the
  gap is send only. — M7
- **`store.channels.version` has no wire counterpart.** The column is written
  and kept in Postgres but never rendered: `tg.Channel` in gotd v0.161.0
  carries no `Version` field, unlike `tg.Chat`. — M7

- **Status privacy settings.** Any account sharing a non-deleted 1:1 dialog can see any other
  account's live status. The Telegram hide-online-status controls (hide-all,
  hide-from-non-contacts) are deferred pending a contacts and block-list model; the fan-out target
  set has no membership boundary to gate on until one exists. — M9
- **Cross-replica status delivery in the E2E gate.** The M9 E2E status tests run single-process;
  cross-replica delivery is covered by the existing `LISTEN`/`NOTIFY` architecture but is not gated
  against a two-replica topology. — M9

- **Username privacy controls.** Any holder of an @username can resolve it to a peer. The
  Telegram hide-from-non-contacts control is deferred pending a contacts and block-list model;
  the resolution endpoint has no membership boundary to gate on until one exists. — M12
- **`maxChannelParticipants` cap on public channels.** Public channels (username set) are
  attacker-reachable via `contacts.resolveUsername` + `channels.joinChannel` without an invite
  hash; the existing 200-member cap is retained as a medium availability control. A follow-up
  should revisit whether a higher cap or a per-joiner rate limit is warranted once join-by-username
  traffic is observable. — M12

- **Media filename indexing rejected.** `message_tsv` covers message text;
  document file names are not indexed and will not be added to the current
  index. `InputMessagesFilter*` variants are wholly unimplemented: a filename
  match folded into a plain-text query would pollute results unpredictably —
  a result that matches because its file name contains the keyword is
  indistinguishable from a message-text match. Revisit when a filter surface
  exists that lets the client and server separate the two match types. — M13
- **Secret-chat messages are permanently non-indexable.** The server stores encrypted blobs
  without inspecting plaintext, so full-text indexing of secret-chat content is not possible by
  design and will not be added. — M13

### M16 — Username/password authentication

- `auth.sendCode` and `auth.signIn` extended to accept a username (5–32 chars `[a-z0-9_]`
  letter-first) in place of a phone number. `auth.signUp` added as the registration entry point.
  On `auth.sendCode` with a username the server issues a code hash (no code value is delivered
  anywhere; the log delivery channel `TG_LOG_LOGIN_CODES=true` still applies).
- **Fail-closed invariant.** A username-mode account with no SRP verifier cannot complete
  sign-in: `auth.signIn` returns an internal error rather than binding the session. The
  only way to clear the state is `account.updatePasswordSettings`, which installs the verifier,
  or `auth.logOut`, which removes the key.
- **Provisional account.** `auth.signUp` creates an account in provisional state
  (`provisional=true` on the auth-key binding). Until `account.updatePasswordSettings`
  installs a verifier, only four methods may be called: `help.getConfig`,
  `account.getPassword`, `account.updatePasswordSettings`, and `auth.logOut`. Every other
  RPC returns `AUTH_KEY_UNREGISTERED`.
- **No re-registration.** A username that already maps to any account cannot be used to
  create a new one: `auth.signUp` returns `USERNAME_OCCUPIED` regardless of the existing
  account's state. A username-mode account cannot be "reclaimed" by re-registering through
  the sign-up flow.
- **Stock client incompatibility.** A server in which any account uses username-mode auth
  cannot be signed into by a stock Telegram Desktop or mobile client for that account: stock
  clients put a phone number in the `phone_number` field of `auth.sendCode`, the server
  rejects that input as neither a valid E.164 phone nor a valid username, and no SMS code
  delivery exists.
- **`TG_REGISTRATION`** (`closed` / `open`, default `closed`). Controls whether `auth.signUp`
  creates new accounts. In `closed` mode the RPC is rejected at the boundary; sign-in for
  accounts that already exist is unaffected. An unrecognized value fails startup.
- **`TG_BOOTSTRAP_USERNAME` / `TG_BOOTSTRAP_PASSWORD`** (or `TG_BOOTSTRAP_PASSWORD_FILE`).
  Creates a seed username-mode account with a verifier before the server binds its port.
  Idempotent when the username already exists, is a user-type owner with `login_mode='username'`,
  and the stored verifier SRP-verifies against the supplied password; otherwise fails startup.
  Does not rotate passwords: changing `TG_BOOTSTRAP_PASSWORD` on an existing account fails
  startup until the value is restored or the credential is updated through the application.
  Password must be at least 12 bytes. Warning: `TG_BOOTSTRAP_PASSWORD` places the cleartext
  password in the process environment and retains it for the process lifetime; use
  `TG_BOOTSTRAP_PASSWORD_FILE` (mode 0600) in any environment where `/proc/self/environ` or
  orchestrator inspect output is visible to untrusted parties.
- New rate-limit env vars (defaults apply; `0` disables a surface):
  - `TG_RATE_LIMIT_CHECK_PASSWORD` / `_WINDOW` — failed `auth.checkPassword` SRP proofs per
    account (default 5/10 min). Charged only on failures; a valid proof is never charged.
  - `TG_RATE_LIMIT_CHECK_PASSWORD_IP` / `_WINDOW` — failed `auth.checkPassword` proofs per
    client network (default 10/h). Charged only on failures.
  - `TG_RATE_LIMIT_SIGN_UP_IP` / `_WINDOW` — `auth.signUp` calls per client network (default
    5/h). No-op in `closed` mode.
- E2E gates prove the full login flow (`auth.sendCode` → `auth.signIn` →
  `SESSION_PASSWORD_NEEDED` → `account.getPassword` + `auth.checkPassword` →
  `auth.Authorization`) and the registration flow (`auth.sendCode` → `auth.signIn` →
  `authorizationSignUpRequired` → `auth.signUp` → `account.updatePasswordSettings`) against
  a real gotd client.

## Engineering invariants

Apply to every milestone.

- Errors are values; fail closed at trust boundaries; never `_ =` an error.
- Every messaging/account RPC requires a bound `UserID`, else
  `AUTH_KEY_UNREGISTERED`.
- Wire integers (`pts`/`date`/`seq`/message id) are gotd `int`; DB columns are
  `BIGINT`; convert at the wire boundary.
- Per-change gates: `go build ./...`, `go test ./...`, `golangci-lint run`,
  `atlas migrate hash|validate`. Run the tests via `make test` / `make test-db`,
  and read `docs/testing.md` first if you are working inside a container — the
  Postgres suites need a Docker networking step those targets handle.
- A real gotd client is the compatibility oracle; each milestone ships an E2E gate.
