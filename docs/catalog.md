# English language catalog

`catalogctl` is the only catalog write command. The normal `telegramd` binary,
MTProto handlers, API handlers, and admin listener only load a validated
read-only snapshot.

## Build offline

Obtain `Telegram/Resources/langs/lang.strings` from a reviewed tdesktop checkout
and record its pinned source URL, revision, and SHA-256. The build command reads
only the local file and makes no network request or credential lookup:

```bash
sha256sum Telegram/Resources/langs/lang.strings
go run ./cmd/catalogctl build \
  --input Telegram/Resources/langs/lang.strings \
  --output catalog/english.json \
  --source-url "https://github.com/telegramdesktop/tdesktop/blob/REVISION/Telegram/Resources/langs/lang.strings" \
  --source-revision REVISION \
  --source-sha256 SHA256
```

The input is the tdesktop GPL source file, not a Telegram-hosted community
translation. The generated artifact retains this notice and attribution:

> This artifact contains source strings from Telegram Desktop, licensed under
> GPLv3 with the OpenSSL exception. See
> https://github.com/telegramdesktop/tdesktop/blob/master/LEGAL.

Attribution is retained as `Telegram Desktop,
https://github.com/telegramdesktop/tdesktop`. Only `tdesktop/en` is accepted;
volunteer and non-English inputs fail with the fixed English-only diagnostic.

The normalized artifact uses sorted keys and stable JSON. It rejects invalid
UTF-8, NUL bytes, malformed or duplicate keys, keys over 128 bytes, values over
16 KiB, malformed plural groups, a source checksum mismatch, and an encoded
artifact over the 4 MiB transport-safe ceiling. Diagnostics never print source
or translation values.

## Publish explicitly

Commit the reviewed artifact and run from a clean repository worktree. The
command records the reviewed `HEAD` SHA, source provenance, manifest and content
checksums, the OS user, and the UTC database timestamp. It reads the Postgres
DSN from `TG_POSTGRES_DSN` or `--dsn`; the DSN is never printed or stored in
audit data.

```bash
TG_POSTGRES_DSN='postgres://...' \
  go run ./cmd/catalogctl publish \
  --artifact catalog/english.json \
  --repo .
```

Publication takes a per-pack transaction lock, compares effective content, and
does nothing for an identical artifact. A changed artifact inserts one
immutable version, per-key changes, deletion tombstones, and one append-only
audit row before activating the current pointer. Versions are monotonic. A
rollback is a forward publication of previously reviewed content; no version is
decremented and no history is deleted.

The runtime snapshot loader uses one repeatable-read, read-only transaction and
atomically replaces the prior snapshot only after validation. A failed refresh
keeps the previous snapshot. With no valid snapshot, telegramd retains its
normal built-in English behavior. Anonymous reads use the in-memory snapshot,
not Postgres.

The dependency boundary can be checked without a database:

```bash
make check-catalog-deps
```
