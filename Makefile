.PHONY: tools-check sqlc generate migrate-new migrate test test-db docker-bridge lint build run

# sqlc lives in a separate tools module (tools/go.mod) so its broken transitive
# dep graph (grpc test deps -> a non-existent gonum package) stays out of the
# main module. We build the pinned binary into ./bin and run it from repo root.
tools-check: bin/sqlc
	@command -v atlas >/dev/null 2>&1 || { echo "atlas not installed: see https://atlasgo.io/getting-started"; exit 1; }
	@echo "tools ok: sqlc $$(./bin/sqlc version), atlas $$(atlas version | head -1)"

bin/sqlc:
	go -C tools build -o "$(CURDIR)/bin/sqlc" github.com/sqlc-dev/sqlc/cmd/sqlc

sqlc: bin/sqlc
	"$(CURDIR)/bin/sqlc" generate

generate: sqlc

# Diff the current migration dir against the desired schema into a new file.
# Usage: make migrate-new name=add_sessions
migrate-new:
	atlas migrate diff "$(name)" --env local

migrate:
	atlas migrate apply --env local

# -race is not optional here: the hard parts of this server are ordering
# (Conn.Push/writeMu, per-owner pts, store lock ordering), and a plain run
# passes a build that corrupts state under concurrent load.
#
# e2e runs in its own invocation with -count=1, and the packages below exclude
# it so it is not run twice. Go caches a test result keyed on the package's
# sources and recorded inputs, so a commit that changed only another package
# replays e2e's earlier result — the run then reports success for an
# end-to-end path that never started on that commit. That is exactly the layer
# where wiring and environment breakage shows up, so it is re-run every time;
# every other package keeps its cache.
E2E_PKG := github.com/adambenhassen/telegram-server/test/e2e

test: docker-bridge
	$(TESTENV) go test -race $$(go list ./... | grep -v '^$(E2E_PKG)$$')
	$(TESTENV) go test -race -count=1 -timeout 20m $(E2E_PKG)

# The store suite alone, for a quick check while working in internal/store.
# Deliberately not ./test/... — e2e wants the whole machine to itself and is
# minutes, not seconds; `make test` and CI cover it.
test-db: docker-bridge
	$(TESTENV) go test -race ./internal/store/...

# Inside a container the host is shared with other test runs, and tg-test-pg is
# shared with them too — it is keyed only by name. Ryuk, testcontainers' reaper,
# SIGKILLs that container when *its own* session's last binary exits, taking
# every concurrent run down with it ("unexpected EOF", then connection refused
# against the old IP). CI disables it for the same reason. Off only in a
# container, where the sandbox is thrown away anyway; on a laptop it stays on so
# a leaked container does not outlive the run.
TESTENV := $(shell [ -f /.dockerenv ] && echo TESTCONTAINERS_RYUK_DISABLED=true)

# pgtest starts its Postgres on Docker's default bridge network. When the tests
# themselves run inside a container attached to some other user-defined network,
# the two networks are isolated and every DB test dies on a connect timeout to
# 172.17.x.x. Joining the bridge restores the route. Skipped entirely when not
# running in a container.
#
# Joining also hands the bridge's gateway the container's default route, and
# nothing puts the old one back — from the first `make test` onward every
# non-local packet leaves by a gateway the container did not boot with.
# --gw-priority=-100 joins for reachability while losing the gateway election,
# so the user-defined network keeps the default route. It needs Docker 28
# (API 1.48); an older daemon refuses the flag, and setup must not start
# failing where it used to work, so we retry plainly and say what moves.
#
# Only "already in the network" is ignored, and nothing else: a silent failure
# here buys back the exact timeout this target exists to prevent, minutes later
# and looking like a broken test. /etc/hostname is not the container's Docker
# name under --hostname or pod-style networking, and the socket may be absent —
# both must be loud. A container already on the bridge keeps whatever priority
# it joined with; `docker network disconnect bridge <name>` once, then rerun.
docker-bridge:
	@[ -f /.dockerenv ] || exit 0; \
	name=$$(cat /etc/hostname); \
	out=$$(docker network connect --gw-priority=-100 bridge "$$name" 2>&1) && exit 0; \
	case "$$out" in \
	*"already exists in network"*) exit 0;; \
	*"unknown flag"*|*"unknown shorthand flag"*|*"requires API version"*) \
	   echo "make: this docker daemon predates --gw-priority (Docker 28+); joining the bridge without it, which moves this container's default route to the bridge gateway. See docs/testing.md" >&2; \
	   out=$$(docker network connect bridge "$$name" 2>&1) && exit 0; \
	   case "$$out" in *"already exists in network"*) exit 0;; esac;; \
	esac; \
	echo "$$out" >&2; \
	echo "make: cannot join the docker bridge network; the Postgres tests will time out. See docs/testing.md" >&2; \
	exit 1

lint:
	golangci-lint run

build:
	go build ./...

run:
	go run ./cmd/telegramd
