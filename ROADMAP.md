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
  `messages.setTyping`

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
- Real-time delivery: in-process session registry + `Conn.Push` (server-initiated
  writes serialized against replies) + `LISTEN`/`NOTIFY` fan-out.
- Bounded difference: capped per batch, returns `differenceSlice` with an
  intermediate state when truncated; state read before events so the advertised
  `pts` never runs past an omitted event.
- Revoked session dropped and closed on its next frame, and closed on every
  replica without waiting for one via a `tg_evict` NOTIFY; a revocation aimed at
  the caller's own session publishes only once its reply is on the wire.
- E2E gates prove live send/read/edit/delete/typing between two gotd clients,
  offline `getDifference` backfill, and cross-process delivery over `LISTEN`/`NOTIFY`.

## Planned — feature track

### M5 — Media & files
- `upload.saveFilePart` / `upload.saveBigFilePart`, `upload.getFile`.
- Photo/document messages, thumbnails, `messages.sendMedia`.
- Blob store abstraction: local filesystem first, RustFS (S3-compatible) later.
- Reference-counted storage; file `access_hash` and expiry.

### M6 — Group chats (basic)
- `chat` as a peer type; membership tables.
- `messages.createChat`, `addChatUser`, `deleteChatUser`, `editChatTitle`.
- Fan-out: a send writes N per-member rows reusing the two-sided/event-log model;
  member set drives the recipient list.
- Service messages (member added/removed, title changed).

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
  cooldown; flood-wait on messaging RPCs.
- **Backups & retention.** Postgres backup story; `message_events` retention.

## Known deferrals & tech debt

Tracked so shortcuts don't rot into "later means never".

- **`message_events` GC + `updates.differenceTooLong`.** All events retained;
  `getDifference` is capped and returns `differenceSlice` when truncated, but
  there is no event-log trimming or too-long path yet. — M4.
- **Peer `access_hash`.** Placeholder scheme: `access_hash == user_id`, validated
  but not cryptographically derived. Real hashing deferred. — M1/M4.
- **`qts`.** Column kept at 0; no secret-chat / bot update stream. — M4.
- **Client-pts-ahead resync.** A client `pts` past the server is clamped to empty
  (single-writer invariant), not an explicit resync response. — M4.
- **`seq` on update envelopes.** Minimal; gotd relies on `pts` for dedup, so `seq`
  is not yet a meaningful per-user counter. — M4.

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
