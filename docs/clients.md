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
| `TG_ADVERTISE_ADDR` | *(derived from `TG_LISTEN_ADDR`)* | `host:port` clients are told to dial, used verbatim. Derived when unset: the listen address with an empty or wildcard host (`:2443`, `0.0.0.0`, `::`) replaced by `127.0.0.1`. A value that is not `host:port`, has no host, or has a port that is not an integer in 1–65535 fails startup |
| `TG_POSTGRES_DSN`   | *(required)*     | Postgres connection string; no default, server fails to start without it |
| `TG_AUTHKEY_ENC_KEY`| *(required)*     | 64 hex chars (32 bytes) — master key that encrypts auth keys at rest; must stay stable, or persisted sessions can no longer be decrypted |
| `TG_AUTHKEY_ENC_KEY_FILE`| *(unset)*   | Path the master key is read from when `TG_AUTHKEY_ENC_KEY` is empty, and generated into (0600) on first start when that file does not exist. One of the two must be set — with neither, startup fails. A generated key is a dev key and the server logs a warning saying so |
| `TG_RSA_KEY_PATH`   | `server_key.pem` | Path to the server's RSA private key        |
| `TG_DC_ID`          | `2`              | DC id this server advertises as `ThisDC`    |
| `TG_LOG_LOGIN_CODES`| `false`          | Write issued login codes to the log in cleartext. Off by default; with it off no code is delivered anywhere and sign-in cannot complete. A non-boolean value fails startup |
| `TG_CLIENT_ADDR_TRUST`| `socket`       | Where the address a per-IP limit is keyed on comes from: `socket` or `proxy-v2`. Any other value fails startup by name. `socket` is the connection's own peer address and assumes one peer address is one client, which fails from either end: behind a proxy or an L4 load balancer every peer address is the balancer's, so one bucket holds every client and the per-IP cap becomes a global one; behind a carrier NAT one address covers thousands of mobile subscribers, who then spend each other's budget. The server warns about both once at startup while any per-IP limit is on. `proxy-v2` takes the address from a PROXY protocol v2 header and is what to run behind an L4 load balancer; it needs `TG_CLIENT_ADDR_PROXY_CIDRS` and emits no such warning, because the misconfiguration it warns about fails the start instead |
| `TG_MAX_PREAUTH_CONNS`| `1024`       | Concurrent connections that have not authenticated yet, process-wide. Checked on the accept loop, so a socket past the cap is closed before it costs a goroutine, a deadline or a read — refusing is cheaper than accepting, which is what makes the cap shed load rather than apply it. `0` disables it; a negative or non-integer value fails startup |
| `TG_MAX_PREAUTH_CONNS_PER_IP`| `64`    | The same, per client network, so one peer cannot spend the process-wide cap alone and lock everybody else out. Keyed on the network the per-IP rate limits already use — an address for IPv4, a **/64** for IPv6, since a host on a routed v6 allocation mints addresses inside its own /64 for free — and on the address `TG_CLIENT_ADDR_TRUST` names, which in `proxy-v2` mode is the one the balancer reports and never the socket peer. A connection carrying no address at all (a `LOCAL` health check, or a socket peer the transport could not report) is charged to nothing and stays bounded by the other two. It is a concurrency cap and not a rate: a handshake takes milliseconds, so 64 at once is hundreds of new sessions a second from one network, which leaves room for a carrier NAT or a corporate egress. `0` disables it; a negative or non-integer value fails startup |
| `TG_PREAUTH_LIFETIME`| `2m`          | How long a connection may stay unauthenticated, measured from accept. Past it the socket is closed whatever it is sending, which is the only bound that reaches a peer that stays inside every deadline by dripping one small frame per read timeout. It ends at the first frame that decrypts under a key the server issued, so a client between key exchange and sign-in — waiting on a human reading a code — is not cut off. Do not set it below a minute: gotd applies a 60s timeout per read inside key exchange, a shorter ceiling starts cutting handshakes that are merely slow, and the server warns at startup if you do. `0` disables it; a negative or unparseable duration fails startup |
| `TG_MAX_CONNS_PER_UNBOUND_KEY`| `8`  | Concurrent connections one auth key with nobody signed in on it may hold. It is the analogue, for keys with no user, of the per-user connection cap, and it covers the population between the two: the `TG_*PREAUTH*` bounds end at the first frame that decrypts under a server-issued key, and the per-user cap counts only signed-in sessions, so a key that completed one exchange and never signed in used to be counted by neither. A connection is charged only once one of its frames has decrypted, never on the auth key id alone — that id is on the wire in cleartext, and charging on it would let anyone who reads one fill a stranger's budget. The default is small because the legitimate holder of such a key is one client waiting on a human to read a login code, holding one connection; more keys, not more connections per key, is what a real population grows by, and each new key costs a key exchange that the pre-auth bounds already price. Past the cap the frame in hand is answered and the socket is then closed. `0` disables it; a negative or non-integer value fails startup |
| `TG_CLIENT_ADDR_PROXY_CIDRS`| *(unset)* | Comma-separated addresses or CIDRs (`10.0.0.0/8, 192.0.2.7`) of the balancers a PROXY protocol v2 header is accepted from. An IPv4-mapped entry takes its IPv4 meaning (`::ffff:192.0.2.0/120` is `192.0.2.0/24`), since peer addresses are matched unmapped; one too short to name an IPv4 network fails startup rather than starting and matching nothing. Required by, and only read in, `TG_CLIENT_ADDR_TRUST=proxy-v2`: an empty list there fails startup, and a list set in `socket` mode does too, since it means the balancer is in place but every client is being keyed on its address. Both directions then fail closed — a connection from a listed balancer without a valid v2 header is dropped rather than served on the balancer's address, and a header from anywhere else is dropped rather than believed. Only v2: the v1 text form is refused. An address is read only from `PROXY` over `AF_INET`/`AF_INET6` with the `STREAM` transport; the two headers that name no client — the `LOCAL` command a health check sends, and `AF_UNSPEC` — connect but carry no address and so cannot call `auth.sendCode`; every other family or transport is refused. Keep this list to the balancer addresses, not a VPC or subnet range: a connection whose header names no client is charged to no bucket, so anything inside an allowlisted CIDR can send a `LOCAL`/`AF_UNSPEC` header and sit outside `TG_MAX_PREAUTH_CONNS_PER_IP` entirely — with `10.0.0.0/8` that is every workload in the network, with the balancer's own addresses it is the balancer |

### What the pre-auth bounds do and do not cover

The three `TG_*PREAUTH*` settings bound connections that have not authenticated:
an unauthenticated population spread over k client networks holds at most
`min(TG_MAX_PREAUTH_CONNS, k × TG_MAX_PREAUTH_CONNS_PER_IP)` sockets, each for at
most `TG_PREAUTH_LIFETIME`.

They end at the first frame that decrypts under a key the server issued, not at
sign-in: a client waiting on a human reading a login code must not be closed, so
one completed key exchange buys connections those three settings no longer
count. `TG_MAX_CONNS_PER_UNBOUND_KEY` is what counts those, so the worst case
extends rather than stopping there. A peer holding m keys nobody has signed in
on holds at most `m × TG_MAX_CONNS_PER_UNBOUND_KEY` connections beyond the
pre-auth population above, and each of those m keys costs its own key exchange,
which had to pass the pre-auth bounds to happen at all. Once someone does sign
in on a key, what its connections may hold is the per-user connection cap's to
decide and this bound lets them go for good — including if the key is unbound
again afterwards, as the 2FA path does.

One thing still sits outside all of it, on purpose: a connection carrying no
client address is charged to no network bucket, so keep
`TG_CLIENT_ADDR_PROXY_CIDRS` to the balancers.

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
