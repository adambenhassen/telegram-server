.PHONY: tools-check sqlc generate migrate-new migrate test lint build run

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

test:
	go test ./...

lint:
	golangci-lint run

build:
	go build ./...

run:
	go run ./cmd/telegramd
