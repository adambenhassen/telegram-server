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
How the harness reaches Postgres depends on which Docker daemon the container
talks to, and both cases are handled by `make` with nothing to configure.

### A daemon reached over `DOCKER_HOST` (a DinD sidecar)

There is no bind-mounted `/var/run/docker.sock`; `DOCKER_HOST` points at a
separate daemon, which is how agent runtimes on this repo are set up. That
daemon runs the Postgres container in its own network namespace, so its bridge
addresses are not routable from here and this container is not one of its
containers — it cannot be joined to anything.

Nothing is needed: `pgtest` probes the bridge address, finds it unreachable and
connects on the container's published port instead, which testcontainers
resolves against the daemon's host. `make`'s `docker-bridge` step prints one
line saying it skipped the join and gets out of the way. Postgres actually
being unreachable is still a hard failure, from `pgtest` setup, naming the
host and port it gave up on.

### A bind-mounted host socket

The daemon is the host's and this container is one of its containers.
A container attached to a user-defined Docker network cannot reach the default
bridge — the two are isolated — so every DB test fails like this:

```
pgtest setup: admin connect: failed to connect to `user=postgres database=postgres`:
172.17.0.4:5432 (172.17.0.4): dial error: timeout: dial tcp 172.17.0.4:5432: connect: connection timed out
```

That is a routing problem, not a broken Docker socket: the container starts
fine, it just is not reachable. The fix is to join the bridge network as well:

```bash
docker network connect --gw-priority=-100 bridge "$(cat /etc/hostname)"
```

`make test` and `make test-db` do this for you via the `docker-bridge` target.
It is skipped when not running inside a container, it touches only the calling
container's own network attachments, and it is quiet only about already being
in the network or about the daemon not owning this container — any other
failure stops the run rather than letting it die on the timeout above twenty
minutes later. If you invoke `go test` directly instead of going through `make`,
run that command first; on a DinD daemon you do not need to run anything.

`--gw-priority` is what keeps the join from being felt outside the test run.
Plain `docker network connect bridge` also makes the bridge gateway the
container's **default route**, and nothing puts the old one back: a container
that booted with `default via 172.18.0.1` sends every non-local packet via
`172.17.0.1` from the first `make test` onward, for the rest of its life. The
negative priority loses the gateway election while still giving the suite its
route to `tg-test-pg`, and embedded DNS on the user-defined network is
unaffected.

The flag needs Docker 28 (API 1.48). On an older daemon the target prints a
warning that the default route moves and joins without it, rather than failing
setup — the tests still run there, at the old cost. Priority is fixed at attach
time, so a container that already joined the bridge the old way keeps the
hijacked route; `docker network disconnect bridge "$(cat /etc/hostname)"` once,
then let `make` reattach it.

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

Requirement: the container must be able to reach a Docker daemon, whether by a
bind-mounted socket or by `DOCKER_HOST`. Without one the harness cannot start
Postgres at all, and there is no fallback — the suites cannot run locally.
