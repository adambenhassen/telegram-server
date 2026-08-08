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
test: docker-bridge
	go test -race ./...

# Postgres-backed suites only, for a quick check while working in internal/store.
test-db: docker-bridge
	go test -race ./internal/store/... ./test/...

# pgtest starts its Postgres on Docker's default bridge network. When the tests
# themselves run inside a container attached to some other user-defined network,
# the two networks are isolated and every DB test dies on a connect timeout to
# 172.17.x.x. Joining the bridge restores the route. Idempotent, and skipped
# entirely when not running in a container.
docker-bridge:
	@[ -f /.dockerenv ] || exit 0; \
	docker network connect bridge "$$(cat /etc/hostname)" 2>/dev/null || true

lint:
	golangci-lint run

build:
	go build ./...

run:
	go run ./cmd/telegramd
