# Build the MinIO client from the pinned upstream source revision because the
# upstream release images and binaries are no longer publicly available.
FROM golang:1.26 AS build

ENV CGO_ENABLED=0

RUN go install github.com/minio/mc@v0.0.0-20250813083541-7394ce0dd2a8

FROM alpine:3.22

COPY --from=build /go/bin/mc /usr/local/bin/mc

ENTRYPOINT ["mc"]
