# Running the tests

```bash
make test        # everything, -race
make test-db     # only the Postgres-backed suites (internal/store, test/)
```

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
It is idempotent, it is skipped when not running inside a container, and it
touches only the calling container's own network attachments. If you invoke
`go test` directly instead of going through `make`, run that command first.

Do not change the container's network attachments while a suite is running:
the bridge address pool shifts under the running test binaries and packages
start failing with `connection refused` against an address that has been
recycled. Let `make` do it before `go test` starts.

Requirement: the container must have access to the host Docker socket. Without
it the harness cannot start Postgres at all, and there is no fallback — the
suites cannot run locally.
