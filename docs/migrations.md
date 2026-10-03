# Database migrations

Schema changes are managed with [Atlas](https://atlasgo.io). Migration files live
in `migrations/` and are tracked by `migrations/atlas.sum` (an integrity hash).
Atlas records which files it has applied, so migrations run exactly once — no
`IF NOT EXISTS` guards.

Config is in `atlas.hcl` under `env "local"`:

- `url` — the target database, read from `TG_POSTGRES_DSN`.
- `dev` — a throwaway Postgres 16 container Atlas spins up to parse/execute SQL
  for `validate` and `diff` (`docker://postgres/16`). Requires Docker.

## Add a migration

Either hand-write a file `migrations/<version>_<name>.sql` (version is a sortable
timestamp, e.g. `20260721000001`), then re-hash:

```bash
atlas migrate hash --env local
```

Or diff against a desired schema to generate one:

```bash
make migrate-new name=add_sessions   # atlas migrate diff ... --env local
```

## Validate

```bash
atlas migrate validate --env local                 # checksum only
atlas migrate validate --env local \
  --dev-url "docker://postgres/16/dev?search_path=public"   # + SQL semantics
```

## Apply

```bash
export TG_POSTGRES_DSN="postgres://user:pass@host:5432/db?sslmode=disable"
make migrate   # atlas migrate apply --env local
```

The channel post summary migration adds hooks and derived-state tables without
backfilling existing channels. Initialize one channel explicitly with:

```bash
telegramd maintenance initialize-channel-post-summaries --channel-id <id>
```

The command rebuilds only that channel's summary nodes and readiness in a
separate transaction from the Atlas migration. It serializes with that
channel's posts; draining writers is an operational performance precaution,
not a correctness requirement.

The test harness (`internal/pgtest`) and production both apply these same
migrations to reach the current schema. (Wiring pgtest to apply them lands in a
later task.)
