# golang:1.25 matches the `go 1.25.0` directive in go.mod; the module file stays
# the single source of the version, so a bump there is the only thing to change.
FROM golang:1.25 AS build

WORKDIR /src

# Dependencies first: this layer survives every source edit.
COPY go.mod go.sum ./
RUN go mod download

# An allowlist rather than `COPY . .` — a file nobody remembered to put in
# .dockerignore still cannot reach a layer. These four paths are the whole
# build input; nothing under cmd/ or internal/ embeds anything else.
COPY cmd/ ./cmd/
COPY internal/ ./internal/

# The module has no cgo dependency (pgx is pure Go), so a static binary is both
# available and required — the distroless static base has no libc to link to.
RUN CGO_ENABLED=0 go build -o /telegramd ./cmd/telegramd

# The server writes exactly one file, its RSA identity key, and the final image
# has no shell to mkdir with — so the directory is created here and copied in.
# Mode and owner are set on the COPY below, not here: BuildKit creates a COPY
# destination directory itself and never carries the source directory's mode.
RUN install -d /var/lib/telegramd

# distroless/static: no shell and no package manager, ships CA certificates for
# a TLS Postgres DSN, and already defines the non-root uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot

# root-owned 0755 on purpose: the process must not be able to rewrite the binary
# it is executing.
COPY --from=build /telegramd /usr/local/bin/telegramd
# 0700 and owned by the runtime uid: nothing else in the image is writable, and
# a named volume mounted here inherits both. --chmod is load-bearing — without
# it the destination is created 0755 and the volume carries that outwards.
COPY --from=build --chown=65532:65532 --chmod=700 /var/lib/telegramd /var/lib/telegramd

# A path, not a secret. Without it the key would default to the working
# directory and land in the container's writable layer, giving a fresh server
# identity on every run and putting a live private key into anything that
# snapshots the container. Mount a volume here so the identity survives restarts.
ENV TG_RSA_KEY_PATH=/var/lib/telegramd/server_key.pem

# TG_POSTGRES_DSN and TG_AUTHKEY_ENC_KEY are deliberately absent: both are
# secrets, both are injected at run time, and startup fails loudly without them.
# Do not add a default for either, not even a placeholder.

USER 65532:65532
EXPOSE 2443
ENTRYPOINT ["/usr/local/bin/telegramd"]
