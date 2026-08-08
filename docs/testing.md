# Running the tests

```bash
make test        # everything, -race
make test-db     # just internal/store, for a quick check while working in it
```

`test-db` stops at `internal/store` on purpose. The e2e suite wants the machine
to itself: under full-parallel `-race` on a shared host it blows its own 30s
client-login timeouts, and tests that pass alone in seconds fail in a full run.
Leave e2e to `make test` and CI, or narrow it with `-run`.

Both targets are self-contained: they set up the Docker networking the Postgres
harness needs before running `go test`. Nothing else to install, no DSN to
export, no environment variables.

## How the Postgres tests get a database

`internal/pgtest` starts one reusable Postgres container (`tg-test-pg`) over the
Docker socket and clones a fresh database per test from a content-addressed
template, so the suite can run fully parallel. See the harness comments for the
template and reuse rules.

The container is attached to Docker's **default bridge** network, and the
harness connects straight to its bridge IP (`172.17.x.x`) rather than a
published port, because docker-proxy drops connections under the suite's
parallelism.

## Running the tests from inside a container

This is the case for CI-style containers and for agent runtimes on this repo.
A container attached to a user-defined Docker network cannot reach the default
bridge — the two are isolated — so every DB test fails like this:

```
pgtest setup: admin connect: failed to connect to `user=postgres database=postgres`:
172.17.0.4:5432 (172.17.0.4): dial error: timeout: dial tcp 172.17.0.4:5432: connect: connection timed out
```

That is a routing problem, not a broken Docker socket: the container starts
fine, it just is not reachable. The fix is to join the bridge network as well:

```bash
docker network connect bridge "$(cat /etc/hostname)"
```

`make test` and `make test-db` do this for you via the `docker-bridge` target.
It is skipped when not running inside a container, it touches only the calling
container's own network attachments, and it is quiet only about already being
in the network — any other failure stops the run rather than letting it die on
the timeout above twenty minutes later. If you invoke `go test` directly instead
of going through `make`, run that command first.

## `no route to host` / `connection refused` mid-run

Different signature, different cause: the address the harness resolved at setup
stopped belonging to `tg-test-pg`. Two ways that happens.

The container is shared by everything running on the host, keyed only by its
name. A concurrent run, or somebody's `docker rm -f tg-test-pg` (which is the
documented way to pick up a changed `internal/pgtest/testdata/postgres-fast.conf`),
pulls it out from under your suite; the replacement comes back on a different
bridge IP and every test binary holding the old one fails.

The worst version of this is automatic. Ryuk, testcontainers' reaper, SIGKILLs
the session's containers when the last test binary of *that* session exits — and
since the container is shared by name, it kills concurrent runs too, which shows
up as `unexpected EOF` followed by connection refused for everything after. So
inside a container the `make` targets set `TESTCONTAINERS_RYUK_DISABLED=true`,
as CI does. The cost is that `tg-test-pg` outlives the run; that is the point of
a reusable container, and `docker rm -f tg-test-pg` still refreshes it. On a
laptop the reaper stays on, so nothing is left behind there.

The other way is self-inflicted: changing the container's network attachments
while a suite is running shifts the bridge address pool under the running
binaries. Let `make` do it before `go test` starts and leave it alone after.

Requirement: the container must have access to the host Docker socket. Without
it the harness cannot start Postgres at all, and there is no fallback — the
suites cannot run locally.
