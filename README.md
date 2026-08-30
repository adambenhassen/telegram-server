# telegram-server

An MTProto (Telegram protocol) server in Go, built on [gotd](https://github.com/gotd/td)'s
exported packages (transport, key exchange, crypto and proto) with the accept
loop, session bookkeeping and RPC dispatch implemented in this repository. It is
a single-DC server: auth keys, sessions, users, messages and files persist in
Postgres, so a restart keeps them. Real gotd clients (including the in-repo e2e
suite) can complete the full key exchange, sign in and exchange messages against
it; `docs/clients.md` covers connecting clients, including a patched Telegram
Desktop.

Two ways to sign in:

- **Username + password**: SRP-6a cloud-password accounts that authenticate
  with a username and password, no code delivery involved. Sign-up is gated by
  `TG_REGISTRATION` (default `closed`), and `TG_BOOTSTRAP_USERNAME` /
  `TG_BOOTSTRAP_PASSWORD` seed a fully provisioned operator account at startup,
  the recommended way to create the first account on a closed server.
- **Phone number + login code**: there is no SMS or push transport, so codes
  are written to the server log, and only when `TG_LOG_LOGIN_CODES=true`.
  Development against fake numbers only.

This is a development and research server, not something to expose to the
internet.

## Architecture

A request flows top to bottom:

```
gotd client
    |  MTProto over TCP (:2443)
    v
internal/mtproto     accept loop, key exchange, session bookkeeping,
    |                message dispatch (on gotd's exported packages)
    v
internal/api         the MTProto RPC method handlers
    |
    v
internal/store       Postgres persistence: pgx, with sqlc-generated
    |                queries in internal/store/db
    v
Postgres             schema from migrations/, applied with atlas
```

In prose: a gotd client connects to `internal/mtproto`, which owns the accept
loop, key exchange and session bookkeeping and dispatches each RPC to
`internal/api`'s handlers, which read and write through `internal/store` to
Postgres.

`cmd/telegramd` wires this together from the environment variables
`internal/config` reads, and when `TG_ADMIN_LISTEN_ADDR` is set it also serves
`internal/admin` (read-only operational metrics and dashboard) on a separate
admin-only HTTP listener.

Supporting packages: `internal/rsakey` (server RSA identity for the auth-key
exchange), `internal/keycrypt` (AES-256-GCM sealing of auth keys at rest),
`internal/srp` (server side of Telegram's SRP-6a cloud password / 2FA),
`internal/peerhash` (per-viewer peer access hashes), `internal/blob` (opaque
blob storage for uploaded file bodies), `internal/pgtest` (the Postgres test
harness).

The schema is managed with [Atlas](https://atlasgo.io) migrations in
`migrations/`; queries are written as SQL in `internal/store/queries` and
compiled to Go by [sqlc](https://sqlc.dev) into `internal/store/db`.

## Requirements

- **Go 1.25+** (`go.mod`)
- **Docker**: required for the tests (the Postgres harness starts a
  `postgres:16-alpine` container), for Atlas's dev database, and for the
  compose stack
- **Atlas CLI**: migrations; CI and the compose stack pin `v1.2.0`
  ([install](https://atlasgo.io/getting-started))
- **golangci-lint**: `make lint`; CI pins `v2.12.2`
- **sqlc**: not installed separately; `make sqlc` builds the pinned binary
  from the `tools/` module into `./bin/sqlc`
- **Node 22 + pnpm 10**: only for the Playwright admin-dashboard e2e suite

`make tools-check` verifies the sqlc and atlas toolchain.

## Quick start (Docker Compose)

The fastest way to a running server. Postgres, migrations and the server, in
order:

```bash
cp .env.example .env && chmod 600 .env
docker compose up
```

The server listens on `127.0.0.1:2443`. The stack enables
`TG_LOG_LOGIN_CODES`, so phone-mode login codes appear in
`docker compose logs telegramd`; a first phone-mode sign-in auto-registers the
account, so that is all a gotd client needs to get in. Username/password
accounts are also supported here: set the `TG_BOOTSTRAP_*` variables in `.env`
if you want a username/password operator account. `docker compose down` keeps
the local volumes (rows, RSA identity, auth-key master key, and filesystem
blobs); `down -v` destroys them and every client has to re-handshake.
`.env.example` documents each variable.

To run the same stack with the S3-compatible RustFS backend, use the tracked
local-only overlay. It starts RustFS, creates the `telegram` bucket, and
selects a separate named RustFS data volume in one command:

```bash
docker compose -f docker-compose.yml -f docker-compose.rustfs.yml up
```

The overlay uses fixed throwaway credentials and loopback-only plaintext HTTP.
Use the same two files with `down` when stopping it; `down -v` also destroys
the RustFS data volume. Do not copy this overlay's endpoint, credentials, or
HTTP setting into a deployment.

## Build and run locally

```bash
make build   # go build ./...
make run     # go run ./cmd/telegramd
```

Configuration is environment variables, read in `internal/config/config.go`.
The server refuses to start without a database and a master key:

| Variable | Default | Purpose |
|---|---|---|
| `TG_POSTGRES_DSN` | *(required)* | Postgres connection string, migrated schema (see below) |
| `TG_AUTHKEY_ENC_KEY` | *(one of two required)* | 64 hex chars, the AES-256-GCM master key over stored auth keys. Alternatively set `TG_AUTHKEY_ENC_KEY_FILE` to read/generate the key from a file; one of the two must be set |
| `TG_LISTEN_ADDR` | `:2443` | Address the MTProto listener binds |
| `TG_RSA_KEY_PATH` | `server_key.pem` | Server RSA private key; generated on first start |
| `TG_BLOB_DIR` | `blobs` | Where uploaded file bodies are written |
| `TG_BLOB_S3_ENDPOINT` | *(unset)* | Enables the S3-compatible blob backend when non-empty; requires the other `TG_BLOB_S3_*` settings below |
| `TG_BLOB_S3_BUCKET` | *(unset)* | Private bucket containing blobs |
| `TG_BLOB_S3_PREFIX` | *(unset)* | Required non-root prefix assigned to this server |
| `TG_BLOB_S3_REGION` | `us-east-1` | SigV4 signing region |
| `TG_BLOB_S3_ACCESS_KEY_ID` | *(unset)* | Operations-only object-store access key |
| `TG_BLOB_S3_SECRET_ACCESS_KEY` | *(unset)* | Raw secret accepted only for compose or CI secret injection; prefer the file form |
| `TG_BLOB_S3_SECRET_ACCESS_KEY_FILE` | *(unset)* | Preferred file containing the object-store secret; the file is read at startup |
| `TG_BLOB_S3_CA_PATH` | *(unset)* | PEM bundle for a private endpoint CA; TLS verification remains enabled |
| `TG_BLOB_S3_ALLOW_INSECURE_HTTP` | `false` | Explicit loopback/compose-only plaintext opt-in; startup warns when enabled |
| `TG_DC_ID` | `2` | DC id the server advertises |
| `TG_BOOTSTRAP_USERNAME` | *(unset)* | Seed a username/password operator account at startup; requires exactly one of `TG_BOOTSTRAP_PASSWORD` or `TG_BOOTSTRAP_PASSWORD_FILE` |
| `TG_BOOTSTRAP_PASSWORD_FILE` | *(unset)* | File (mode 0600) the bootstrap password is read from. Prefer it over `TG_BOOTSTRAP_PASSWORD`: an env value stays visible in `/proc/<pid>/environ`, orchestrator inspect output and crash dumps for the life of the process |
| `TG_REGISTRATION` | `closed` | `open` allows `auth.signUp` to create accounts |
| `TG_LOG_LOGIN_CODES` | `false` | Write phone-mode login codes to the log; with it off, phone-number sign-in cannot complete (username/password sign-in is unaffected) |
| `TG_ADMIN_LISTEN_ADDR` | *(unset)* | Enables the admin HTTP server; requires `TG_ADMIN_TOKEN_HASH` (SHA-256 hex of the operator token) |

### Object-store backend

With all `TG_BLOB_S3_*` variables unset or empty, uploaded blobs use the local
filesystem at `TG_BLOB_DIR`, exactly as before. Setting any object-store
variable to a non-empty value selects S3 mode and requires an endpoint, bucket,
non-empty prefix, access key, and exactly one secret source. The server validates
the S3 client
and lists the configured namespace before it opens for service; a failed check
stops startup and never falls back to local storage.

Use `TG_BLOB_S3_SECRET_ACCESS_KEY_FILE` for the secret. The file should be
readable by the server process and contain only the secret, optionally followed
by a newline. The raw `TG_BLOB_S3_SECRET_ACCESS_KEY` form is accepted for
compose or CI secret injection only; do not put it in a developer shell,
checked-in environment file, command line, or ordinary process environment.

The access key must be scoped to operations on this one bucket and this one
prefix: list, get, put, and delete objects below the prefix only. Do not grant
bucket-root, wildcard, ACL, or policy-management permissions. The server does
not set object ACLs; keep the bucket private.

Switching a deployment that already has local filesystem blobs to S3 is a
manual data move. There is no automated migration path: copy and verify the
existing objects into the configured bucket and prefix before switching the
backend. RustFS has no backup in this project; its named volume is persistence,
not a backup or restore mechanism.

HTTPS certificate verification is always enabled. Set
`TG_BLOB_S3_CA_PATH` only when the endpoint uses a private CA bundle. Plaintext
HTTP is rejected unless `TG_BLOB_S3_ALLOW_INSECURE_HTTP=true` is explicitly
set for a loopback or compose-only endpoint; startup logs a warning when this
escape hatch is used. There is no TLS verification bypass setting.

A minimal run against a local Postgres, seeding an operator account you can
sign in to with username + password:

```bash
export TG_POSTGRES_DSN="postgres://user:pass@localhost:5432/telegram?sslmode=disable"
make migrate
printf '%s' 'change-me-at-least-12-chars' > bootstrap_password && chmod 600 bootstrap_password
TG_AUTHKEY_ENC_KEY="$(openssl rand -hex 32)" \
TG_BOOTSTRAP_USERNAME=operator \
TG_BOOTSTRAP_PASSWORD_FILE=./bootstrap_password \
  make run
```

The password goes through a file rather than `TG_BOOTSTRAP_PASSWORD` on
purpose: an environment value is readable in `/proc/<pid>/environ` and
orchestrator inspect output for as long as the server runs.

The full variable reference (rate limits, pre-auth connection bounds, PROXY
protocol support, upload limits) is in `docs/clients.md` and
`internal/config/config.go`, along with the sign-in and sign-up flows for both
account modes.

## Tests and lint

```bash
make test        # everything, -race; e2e runs in its own -count=1 invocation
make test-unit   # all packages except test/e2e, fast development loop
make test-db     # just internal/store
make lint        # golangci-lint run
```

These are the same targets CI runs (`.github/workflows/ci.yml` runs
`make test`). The Postgres tests need no setup beyond Docker:
`internal/pgtest` starts one reusable `tg-test-pg` container and clones a
fresh database per test from a template. When the tests themselves run inside
a container, the `make` targets join Docker's default bridge network first;
`docs/testing.md` explains why, and what to do when invoking `go test`
directly.

The admin dashboard has a browser e2e suite (Playwright, `test/e2e/admin.spec.ts`,
configured in `playwright.config.ts`), run as a separate CI job:

```bash
pnpm install
pnpm exec playwright install chromium
pnpm test        # playwright test; starts its own server on 127.0.0.1:2444
pnpm lint        # eslint over test/e2e
```

## Migrations and codegen

Schema changes are Atlas migration files in `migrations/`, tracked by
`migrations/atlas.sum`. `atlas.hcl` defines the `local` env: the target
database comes from `TG_POSTGRES_DSN`, and validate/diff use a throwaway
`docker://postgres/16` dev database.

```bash
export TG_POSTGRES_DSN="postgres://user:pass@localhost:5432/telegram?sslmode=disable"
make migrate-new name=add_sessions   # diff current schema into a new migration
make migrate                         # atlas migrate apply --env local
```

Hand-written migrations need `atlas migrate hash --env local` to update the
sum file. Details, including validation: `docs/migrations.md`.

Query changes: edit the SQL in `internal/store/queries`, then

```bash
make sqlc        # regenerate internal/store/db (alias: make generate)
```

## Repository layout

| Path | Contents |
|---|---|
| `cmd/telegramd` | Server entrypoint |
| `internal/mtproto` | MTProto server loop: accept, key exchange, sessions, dispatch |
| `internal/api` | RPC method handlers |
| `internal/store` | Postgres persistence; sqlc-generated code in `store/db`, SQL in `store/queries` |
| `internal/admin` | Read-only operational metrics and dashboard handlers |
| `internal/config` | Environment-variable configuration |
| `internal/blob`, `internal/keycrypt`, `internal/rsakey`, `internal/srp`, `internal/peerhash` | Blob storage, auth-key sealing, RSA identity, SRP 2FA, peer access hashes |
| `internal/pgtest` | Postgres test harness (testcontainers) |
| `migrations/` | Atlas migration files + `atlas.sum` |
| `test/e2e` | End-to-end suite: a real gotd client against a full server; `admin.spec.ts` + `adminserver` for the browser suite (config: `playwright.config.ts`) |
| `tools/` | Separate Go module pinning the sqlc binary |
| `docs/` | `clients.md` (connecting clients, sign-in flows, full config reference), `migrations.md`, `testing.md` |
| `ROADMAP.md` | Milestone history and plans |

## Further reading

- `docs/clients.md`: connecting a gotd client or a patched Telegram Desktop,
  the sign-in and sign-up flows, the complete configuration reference
- `docs/migrations.md`: the Atlas workflow in detail
- `docs/testing.md`: how the Postgres tests get a database, running inside
  containers
- `ROADMAP.md`: where the project has been and where it is going
