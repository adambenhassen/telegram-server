# Connecting a client to telegramd

`telegramd` speaks real MTProto — transport, key exchange and encryption on
gotd's exported packages, with the accept loop and session bookkeeping this
repo's own (`gotd/tgtest` was dropped in M2; see `ROADMAP.md`). Through M16 it
serves 58 RPC methods, and auth keys, sessions, users, messages and files all
live in Postgres, so a restart keeps them. It is still a single-DC server, and
login codes are delivered to the server log rather than by SMS, and only when
`TG_LOG_LOGIN_CODES=true` — off by default, and then not delivered at all.

A stock Telegram Desktop/mobile client cannot reach it: those clients hardcode
Telegram's production DCs and RSA keys. You need a client built or patched to
dial our address and trust our key. This is how the in-repo e2e test (a real
`gotd/td` client) talks to the server; the same steps apply to any gotd-based
client, and section 6 covers a patched Telegram Desktop.

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
| `TG_REGISTRATION`   | `closed`         | Controls whether `auth.signUp` creates new accounts. `closed` (default) rejects all registration RPCs; `open` allows them. An unrecognized value fails startup. Sign-in for accounts that already exist is unaffected by this setting |
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
which had to pass the pre-auth bounds to happen at all. The number m itself is
unbounded until MAIN-252 ships retention of persisted unbound keys, which bounds
m over time by expiring keys nobody signed in on. Once someone does sign
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

## 2. Get the RSA key identity

At startup the server logs the key it loaded/generated:

```
level=INFO msg="server RSA key" key_id=<64 hex chars in 16 dash-separated groups of 4> fingerprint=<int64> path=server_key.pem
```

(`cmd/telegramd/main.go`, right after `rsakey.LoadOrGenerate`).

- `key_id` is the SHA-256 of the DER SubjectPublicKeyInfo encoding of the
  public key, hex-encoded as 16 dash-separated groups of 4 characters
  (e.g. `a1b2-c3d4-e5f6-a7b8-...`). **This is the value to compare out of
  band** — a client UI that displays the same digest for the key it loaded
  must render the identical grouped format. The int64 `fingerprint` is the
  legacy Telegram value; it is retained for clients that still match on it
  but is too short to be a trustworthy out-of-band check. Byte-level oracle
  for the digest, against a public key file (SPKI or PKCS#1 PEM — OpenSSL
  normalises both to SPKI on `-pubin`):

  ```bash
  openssl pkey -pubin -in server_pub.pem -outform DER | sha256sum
  ```
- A client must be built with this exact public key (read `path`, e.g.
  `server_key.pem`, and derive/embed the PEM) and its fingerprint, since
  gotd-style clients select the RSA key to use for the auth-key handshake by
  fingerprint.

Also note the `listening addr=... advertise=... dc=...` log line that
follows — it confirms the actual bind address, the address clients are told
to dial, and the DC id the process is using, which may differ from what you
passed if `TG_LISTEN_ADDR` was left at default.

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

## 5. Username-mode accounts

Username-mode accounts authenticate with a username and a password (SRP-6a cloud
password) instead of a phone number and SMS code. A stock Telegram Desktop or
mobile client **cannot** authenticate as a username-mode account: stock clients
put a phone number in `auth.sendCode`'s `phone_number` field, the server rejects
it as neither a valid E.164 phone nor a valid username, and no SMS code delivery
exists. You need a gotd-based client or the patched Telegram Desktop (section 6)
with the username credential supplied in the `phone_number` field.

### 5a. Logging in as a username account

A username account must already exist and have its SRP verifier set before
login is possible (see section 5b for how accounts are created).

```
1. auth.sendCode(phone_number=<username>)
   → auth.SentCode  (hash only; no code is delivered)

2. auth.signIn(phone_number=<username>, phone_code_hash=<hash>, phone_code="")
   → SESSION_PASSWORD_NEEDED

3. account.getPassword()
   → account.Password  (SRP algorithm, salt, server modulus)

4. auth.checkPassword(password=<SRP proof>)
   → auth.Authorization
```

The `phone_code` value passed in step 2 is ignored; only the `phone_code_hash`
from step 1 is validated. If the username is unknown, step 2 returns
`authorizationSignUpRequired` instead of `SESSION_PASSWORD_NEEDED` — see
section 5b. If the username resolves to an account whose verifier was never set
(a partially-created account), step 2 returns an internal error and access is
denied.

### 5b. Registering a new account

Registration requires `TG_REGISTRATION=open`. With the default `closed` value,
`auth.signUp` is rejected at the RPC boundary and new accounts cannot be created
through the sign-up flow (existing accounts are unaffected).

Once `TG_REGISTRATION=open`:

```
1. auth.sendCode(phone_number=<username>)
   → auth.SentCode  (hash only)

2. auth.signIn(phone_number=<username>, phone_code_hash=<hash>, phone_code="")
   → authorizationSignUpRequired  (username is unknown)

3. auth.signUp(phone_number=<username>, phone_code_hash=<hash>,
               first_name=<name>, last_name="")
   → auth.Authorization  (provisional session — password not yet set)

4. account.updatePasswordSettings(...)
   → account.PasswordSettings  (verifier installed; session becomes full-access)
```

After step 3 the session is in provisional state: only `help.getConfig`,
`account.getPassword`, `account.updatePasswordSettings`, and `auth.logOut` may
be called. Every other RPC returns `AUTH_KEY_UNREGISTERED` until step 4
completes. If the client disconnects before step 4, the account remains
provisional and the next sign-in attempt (section 5a step 2) will fail with an
internal error — the only exits are `account.updatePasswordSettings` to set the
password, or `auth.logOut` to remove the key.

A username that already exists returns `USERNAME_OCCUPIED` from `auth.signUp`.
There is no re-registration path: once a username is claimed it cannot be
reclaimed by starting a new sign-up flow.

### 5c. Seed account at startup (TG_BOOTSTRAP_USERNAME)

`TG_BOOTSTRAP_USERNAME` and `TG_BOOTSTRAP_PASSWORD` (or `TG_BOOTSTRAP_PASSWORD_FILE`)
create a fully provisioned username-mode operator account before the server binds
its port, bypassing the registration flow entirely. This is the recommended way
to create the first account on a server running in `closed` mode.

```bash
TG_BOOTSTRAP_USERNAME=operator \
TG_BOOTSTRAP_PASSWORD=<at-least-12-chars> \
  ./telegramd
```

The operation is idempotent: if the username already exists as a user-type account
with `login_mode='username'` and the stored verifier matches the supplied password,
startup succeeds without modifying anything. It does not rotate passwords: if the
password in the environment differs from the one stored, startup fails. To change
a bootstrap account's password, update it through `account.updatePasswordSettings`
and then update the env var to match.

`TG_BOOTSTRAP_PASSWORD` places the cleartext password in the process environment
for the life of the process — visible via `/proc/self/environ`, orchestrator
inspect output, and crash dumps. Use `TG_BOOTSTRAP_PASSWORD_FILE` pointing to a
mode-0600 file in production environments.

## 6. Telegram Desktop, patched

Stock Telegram Desktop has no user-facing way to change either the DC address
or the RSA key, so reaching this server needs a patched build. The patches live
on a branch of our fork, deliberately not vendored here: the checkout is
~330 MB, the two build systems share nothing, and no CI job builds it.

| | |
|---|---|
| Fork | `https://github.com/adambenhassen/tdesktop` |
| Branch | `spike/MAIN-263-telegramd-endpoint` |
| Upstream base | `8e18cb71103d83d7d98994ff27f0a2bca55c489c` (`dev`) |
| Schema layer | 228, the same layer `gotd/td v0.161.0` pins — constructor ids match, no translation needed |

### The three patch points

1. `Telegram/SourceFiles/mtproto/mtproto_dc_options.cpp` — `constructFromBuiltIn`
   replaces the built-in DC table with a single entry, and
   `readBuiltInPublicKeys` replaces Telegram's production keys with one read
   off disk. Both are driven by environment variables rather than compiled-in
   constants, so the address and the key can change without a rebuild — which
   matters because the build is hours and the server's advertised address is
   not known until it runs.
2. `api_id` / `api_hash`, cmake options rather than a patch. `telegramd` never
   reads either, so any values work; the branch was built with
   `-D TDESKTOP_API_TEST=ON`, which selects upstream's public test pair.
3. `Telegram/SourceFiles/mtproto/special_config_request.cpp` — the DNS and
   Firebase fallback resolver is skipped whenever a custom DC is configured.
   Left live, a failed connect to our address sends the client resolving
   Telegram's real DCs and then talking to whichever it finds.

### Runtime configuration

| Variable | Meaning |
|---|---|
| `TDESKTOP_CUSTOM_DC_ADDRESS` | `host:port` to dial — the server's `advertise` address. Setting it is what activates all three patches |
| `TDESKTOP_CUSTOM_DC_ID` | DC id the address is registered under; must equal `TG_DC_ID`. Defaults to `2` |
| `TDESKTOP_CUSTOM_DC_RSA_KEY_FILE` | Path to the server's RSA **public** key in PKCS#1 PEM. The client derives the fingerprint itself, so it only has to match what the server logs at startup |

Derive that public key from the server's private key:

```bash
openssl rsa -in server_key.pem -pubout -RSAPublicKey_out -out server_pub.pem
```

### Build

The upstream Linux build runs in `ghcr.io/telegramdesktop/tdesktop/centos_env`,
which is **published for linux/amd64 only**. The image *definition* is not
architecture-locked, though: rendering its Dockerfile with `DEBUG=` and `LTO=`
and building it natively on aarch64 works, and takes about two hours on four
cores. Everything below was run against such an image, tagged
`tdesktop:centos_env-arm64`.

Render it from our fork's `dev`, not from upstream. rnnoise v0.2's NEON and
generic paths in `src/vec.h` include a header rnnoise does not vendor, so they
compile on no architecture at all — upstream never notices because its AVX/SSE2
branch never reaches that line, and an aarch64 render from upstream fails there.
The fix is already merged on the fork as `81f5657`, so nothing has to be applied
by hand.

Clone with `--recursive`; the build needs all 36 submodules.

```bash
docker run --rm -u $(id -u) \
  -v "$PWD:/usr/src/tdesktop" \
  -v "$HOME/.cache/tdesktop-ccache:/var/cache/ccache" \
  -e CONFIG=Debug \
  <image-tag> \
  env -u CCACHE_DISABLE \
  /usr/src/tdesktop/Telegram/build/docker/centos_env/build.sh \
  -D CMAKE_CONFIGURATION_TYPES=Debug \
  -D CMAKE_C_FLAGS_DEBUG="-O0 -fpch-preprocess" \
  -D CMAKE_CXX_FLAGS_DEBUG="-O0 -fpch-preprocess" \
  -D TDESKTOP_API_TEST=ON \
  -D DESKTOP_APP_DISABLE_AUTOUPDATE=ON \
  -D DESKTOP_APP_DISABLE_CRASH_REPORTS=ON
```

`-D TDESKTOP_API_TEST=ON` is the public test api id/hash pair, and is enough
here because the server reads neither. The image sets `CCACHE_DISABLE=true`, so
`env -u CCACHE_DISABLE` is what makes the ccache mount do anything, and
`-fpch-preprocess` is what lets ccache hash the build's precompiled headers
rather than silently missing on every one — both are upstream's own CI line.

The binary lands in `out/Debug/Telegram` and is around 1.2 GB. A cold build is
2209 targets, roughly an hour on four cores.

Bind mounts do not work from an agent runtime — the Docker daemon resolves `-v`
paths on its own host, not in the workdir, and silently mounts an empty
directory. There, create a long-lived container and copy the tree in instead:

```bash
docker create --name tdbuild --user root -w /work <image-tag> sleep infinity
docker start tdbuild
docker cp ./tdesktop tdbuild:/work/tdesktop
docker exec tdbuild bash -c 'cd /work/tdesktop && env -u CCACHE_DISABLE CONFIG=Debug \
  ./Telegram/build/docker/centos_env/build.sh <same -D flags as above>'
```

### Connect

Run the server with `TG_LOG_LOGIN_CODES=true` (section 4 covers code delivery)
and start the client against it:

```bash
TDESKTOP_CUSTOM_DC_ADDRESS=127.0.0.1:2443 \
TDESKTOP_CUSTOM_DC_ID=2 \
TDESKTOP_CUSTOM_DC_RSA_KEY_FILE=/path/to/server_pub.pem \
  out/Debug/Telegram -workdir ./tdata-telegramd
```

`-workdir` keeps this profile away from any real Telegram account on the
machine. The client logs the key it loaded as `MTP Info: using custom public
RSA key ... fingerprint <int64>`; that number must equal the `fingerprint` the
server logs at startup, or key exchange fails with no useful client-side
error. For the out-of-band check, compare the `key_id` the server logs at
startup (section 2) against the SHA-256 of the DER SubjectPublicKeyInfo
encoding of the key file you gave the client — the same value the login
screen of the tdesktop fork (MAIN-314) renders from `server_pub.pem`.

A mismatched fingerprint is not, however, the failure to expect first: against
an unmodified server key exchange never completes at all, for reasons that have
nothing to do with the key. See "What stops it today" below before debugging
anything here.

Telegram Desktop is GPLv3. Internal use carries no obligation, but any binary
handed to someone else must ship its source.

### What stops it today

A patched Telegram Desktop built as above does **not** reach the server on an
unmodified `telegramd`. Three things block it, in the order they bite. None is
an RPC the client could route around, and the first two stop it before any
handler runs:

1. **Transport obfuscation.** Telegram Desktop always wraps the TCP stream in
   obfuscated2 — `TcpConnection::prepareConnectionStartPrefix` sends a 64-byte
   nonce and AES-CTR encrypts everything after it, with no way to turn it off.
   The codec sniff in `internal/mtproto/accept.go` reads that nonce as a
   plaintext codec tag, falls through to `Full`, and every frame after it fails
   as `invalid message length`. gotd already ships the fix as
   `transport.ObfuscatedListener`; what it needs is a sniff that still accepts
   the plain codecs the e2e client uses.
2. **`auth.bindTempAuthKey`.** The client's PFS step. Unimplemented, it answers
   `INPUT_METHOD_INVALID`, and because the client only clears its binder on
   `ENCRYPTED_MESSAGE_INVALID` it then retries without any backoff — measured at
   about 1400 calls a second from one client.
3. **`config.expires`.** `DefaultConfig` sends `Date: 0, Expires: 0`.
   `Instance::Private::configLoadDone` computes `expires - now`, reads the
   config as already stale, and re-requests it immediately — about 400
   `help.getConfig` calls a second, forever.

With those three worked around locally, the client signs in against this server
and reaches its main window. MAIN-263 carries the wire-verified inventory of
what it calls on the way and what breaks after.

## Known ceilings

- **Single DC.** The server only ever advertises itself (`api.DefaultConfig`
  builds one `tg.DCOption`). No multi-DC routing, no migration between DCs.
- **Log-only code delivery.** With `TG_LOG_LOGIN_CODES=true`, `auth.sendCode`
  logs the code via `slog` instead of sending SMS. Fine for
  development/testing, not for real users — and with the flag at its default
  there is no delivery channel at all.
- **A partial RPC surface.** The 56 methods registered in `api.New`
  (`internal/api/handler.go`) are the whole of it. Every other method falls to
  `handlers.handleUnknown`, which returns `INPUT_METHOD_INVALID` and logs one
  line per call:

  ```
  level=WARN msg="method not implemented" type_id=0xec86017a method=account.registerDevice#ec86017a error_code=400 error=INPUT_METHOD_INVALID
  ```

  `method` is resolved through `tg.TypesMap()` and reads `unknown` for a
  constructor the pinned layer has no name for, which means either a client on
  a different layer or a method added after `gotd/td v0.161.0`. Grep the log
  for `"method not implemented"` to inventory what a client wanted and did not
  get. A full-featured client trips this often once past login.
- **Placeholder access hash.** The user returned by `auth.signIn` uses its
  own numeric ID as `AccessHash` (see the `self access hash placeholder`
  comment in `userTL`, `internal/api/passwords.go`) rather than a real
  per-session hash. Peer access hashes everywhere else are derived per viewer
  by `internal/peerhash`.
