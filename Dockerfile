# golang:1.26 matches the `go 1.26.6` directive in go.mod; the module file stays
# the single source of the version, so a bump there is the only thing to change.
FROM golang:1.26 AS build

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
# 0700 and owned by the runtime uid: nothing else in the image is writable.
#
# Staged under /rootfs because `COPY <dir> <dir>` copies a directory's contents
# rather than the directory itself, and this one is empty — so it has no content
# to carry the mode across, and the destination would be created at 0755.
# Copying the staging root instead makes the directory itself content. The
# parents are created in a separate call at 0755 so 0700 cannot leak onto /var
# and /var/lib, which the final image already has.
#
# Uploaded file bodies get their own directory, and a deployment gives it its
# own volume: they are unbounded attacker-supplied bytes, while the key
# directory is the one thing here that has to be backed up and has to stay
# small. Separate mounts keep a disk-full on media from becoming a failure to
# write the identity key.
RUN install -d -m 0755 /rootfs/var /rootfs/var/lib \
    && install -d -m 0700 -o 65532 -g 65532 /rootfs/var/lib/telegramd \
    && install -d -m 0700 -o 65532 -g 65532 /rootfs/var/lib/telegramd-blobs

# distroless/static: no shell and no package manager, ships CA certificates for
# a TLS Postgres DSN, and already defines the non-root uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot

# root-owned 0755 on purpose: the process must not be able to rewrite the binary
# it is executing.
COPY --from=build /telegramd /usr/local/bin/telegramd

# No --chown: a stage-to-stage copy preserves the ownership set above, and
# forcing one here would hand /var and /var/lib to the runtime user too.
COPY --from=build /rootfs/ /

# A path, not a secret. Without it the key would default to the working
# directory and land in the container's writable layer, giving a fresh server
# identity on every run and putting a live private key into anything that
# snapshots the container. Mount a volume here so the identity survives restarts.
ENV TG_RSA_KEY_PATH=/var/lib/telegramd/server_key.pem

# Also a path, not a secret. The config default is relative, which is right for
# a run from a source checkout and wrong here: this image runs read-only, so a
# relative directory cannot be created and the server refuses to start. Mount a
# volume here — a separate one from the key volume.
ENV TG_BLOB_DIR=/var/lib/telegramd-blobs

# TG_POSTGRES_DSN and TG_AUTHKEY_ENC_KEY are deliberately absent: both are
# secrets, both are injected at run time, and startup fails loudly without them.
# Do not add a default for either, not even a placeholder.

USER 65532:65532
EXPOSE 2443
ENTRYPOINT ["/usr/local/bin/telegramd"]
