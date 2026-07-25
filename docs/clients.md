# Connecting a client to telegramd

`telegramd` speaks real MTProto (transport, handshake, encryption via
`gotd/tgtest`), but it is a milestone-1 (M1) server: one DC, one RPC method
set, login codes delivered to the server log instead of SMS (and only when
`TG_LOG_LOGIN_CODES=true` — off by default, and then not delivered at all),
and sessions held in memory. A stock Telegram Desktop/mobile client cannot reach it —
those clients hardcode Telegram's production DCs and RSA keys. You need a
client built (or patched) to dial our address and trust our key. This is how
the in-repo e2e test (a real `gotd/td` client) talks to the server; the same
steps apply to any gotd-based client or a patched official client.

## 1. Build and run the server

```bash
go build -o telegramd ./cmd/telegramd
TG_POSTGRES_DSN="postgres://user:pass@localhost:5432/telegramd?sslmode=disable" \
  TG_AUTHKEY_ENC_KEY="$(openssl rand -hex 32)" \
  ./telegramd
```

Configuration is read from environment variables in `internal/config/config.go`:

| Variable            | Default          | Notes                                      |
|---------------------|------------------|---------------------------------------------|
| `TG_LISTEN_ADDR`    | `:2443`          | `host:port` (or `:port`) the server binds   |
| `TG_ADVERTISE_ADDR` | *(derived from `TG_LISTEN_ADDR`)* | `host:port` clients are told to dial, used verbatim. Derived when unset: the listen address with an empty or wildcard host (`:2443`, `0.0.0.0`, `::`) replaced by `127.0.0.1`. A value that is not `host:port`, has no host, or has a non-integer port fails startup |
| `TG_POSTGRES_DSN`   | *(required)*     | Postgres connection string; no default, server fails to start without it |
| `TG_AUTHKEY_ENC_KEY`| *(required)*     | 64 hex chars (32 bytes) — master key that encrypts auth keys at rest; must stay stable, or persisted sessions can no longer be decrypted |
| `TG_RSA_KEY_PATH`   | `server_key.pem` | Path to the server's RSA private key        |
| `TG_DC_ID`          | `2`              | DC id this server advertises as `ThisDC`    |
| `TG_LOG_LOGIN_CODES`| `false`          | Write issued login codes to the log in cleartext. Off by default; with it off no code is delivered anywhere and sign-in cannot complete. A non-boolean value fails startup |

Postgres must already be reachable at `TG_POSTGRES_DSN`, and its schema must
already be migrated with Atlas (`atlas migrate apply --env local`; see
`docs/migrations.md`). The server does not apply migrations — `store.Open`
verifies the schema is current and fails fast otherwise.

On first run, if the file at `TG_RSA_KEY_PATH` does not exist, the server
generates a new 2048-bit RSA key and writes it there (PKCS1 PEM, mode
`0600`) — see `rsakey.LoadOrGenerate` / `rsakey.generate` in
`internal/rsakey/rsakey.go`. On subsequent runs it loads and reuses that
same file, so the key (and its fingerprint) stays stable across restarts as
long as the file persists.

## 2. Get the RSA key fingerprint

At startup the server logs the key it loaded/generated:

```
level=INFO msg="server RSA key" fingerprint=<int64> path=server_key.pem
```

(`cmd/telegramd/main.go`, right after `rsakey.LoadOrGenerate`). A client
must be built with this exact public key (read `path`, e.g.
`server_key.pem`, and derive/embed the PEM) and its fingerprint, since
gotd-style clients select the RSA key to use for the auth-key handshake by
fingerprint. Also note the `listening addr=... advertise=... dc=...` log line
that follows — it confirms the actual bind address, the address clients are
told to dial, and the DC id the process is using, which may differ from what
you passed if `TG_LISTEN_ADDR` was left at default.

## 3. Point a client at this server

To connect, a client needs to be built or patched so that:

- Its DC address/config table points at the advertised `host:port` — the
  `advertise` field in the log line above, i.e. `TG_ADVERTISE_ADDR` or, unset,
  the address derived from `TG_LISTEN_ADDR` (e.g. via a custom
  `tg.DCOption`/test-DC override), matching the `dc` logged above (`TG_DC_ID`,
  default `2`).
- It trusts the server's RSA public key (fingerprint from step 2) instead of
  Telegram's production keys.

This is exactly how `gotd/tgtest`-based clients are pointed at a test
server, and how the in-repo e2e test drives this same handler. There is no
support for connecting an unmodified Telegram Desktop/mobile app — those
ship with production DC addresses and keys baked in and have no user-facing
way to override either.

Once connected, the client's `help.getConfig` call gets back a single-DC
`tg.Config` (see `api.DefaultConfig` in `internal/api/config.go`) describing
this server as `ThisDC`.

## 4. Logging in — the code goes to the server log, not SMS

Start the server with `TG_LOG_LOGIN_CODES=true` first. It is off by default,
and while it is off the code is not written to the log — and since the log is
the only delivery channel there is, the code reaches nobody and sign-in cannot
complete. The server says so once at startup when the flag is on:

```
level=WARN msg="TG_LOG_LOGIN_CODES is on: login codes are written to the log in cleartext"
```

Call `auth.sendCode` with a phone number, then watch the server log for:

```
level=INFO msg="login code issued" phone=<phone> code=<5-digit code>
```

(`internal/api/auth.go`, `handleSendCode`). Grep for `"login code issued"`
to find it. There is no SMS/push delivery in M1 — this log line *is* the
delivery channel. Feed that code back into `auth.signIn` with the
`phone_code_hash` returned by `sendCode`.

Anyone who can read the server's output can therefore sign in as any account
that has no 2FA cloud password. Turn the flag on for development against fake
numbers only.

On a successful `auth.signIn`, the phone number is auto-registered as a new
user if it hasn't been seen before (`store.CreateUser`) — there is no
separate registration step or admin approval.

## Known M1 ceilings

- **Single DC.** The server only ever advertises itself (`api.DefaultConfig`
  builds one `tg.DCOption`). No multi-DC routing, no migration between DCs.
- **Log-only code delivery.** With `TG_LOG_LOGIN_CODES=true`, `auth.sendCode`
  logs the code via `slog` instead of sending SMS. Fine for
  development/testing, not for real users — and with the flag at its default
  there is no delivery channel at all.
- **In-memory sessions.** Auth state lives in the running process. Restart
  `telegramd` and every connected client must redo the auth-key handshake
  and re-authenticate — nothing survives a restart except the Postgres-backed
  users and phone codes.
- **Only four RPC methods are implemented**: `help.getConfig`,
  `auth.sendCode`, `auth.signIn`, `users.getUsers`. `users.getUsers` always
  reports the caller as unregistered/unauthorized (`AUTH_KEY_UNREGISTERED`) —
  it exists only so a client's initial auth-status check gets an answer
  and falls through into the sign-in flow, not as a real user-lookup
  endpoint. Every other method falls to a fallback handler that returns
  `INPUT_METHOD_INVALID` and logs `"method not implemented"` with the
  method's type ID — expect a real client to trip this frequently once past
  login.
- **Placeholder access hash.** The user returned by `auth.signIn` uses its
  own numeric ID as `AccessHash` (see the `M1: self access hash placeholder`
  comment in `internal/api/auth.go`) rather than a real per-session hash.
