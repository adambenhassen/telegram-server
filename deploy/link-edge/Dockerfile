FROM golang:1.27 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/linklanding/ ./cmd/linklanding/
COPY cmd/linkselector/ ./cmd/linkselector/
COPY cmd/linkprobe/ ./cmd/linkprobe/
COPY internal/linklanding/ ./internal/linklanding/
COPY internal/linkselector/ ./internal/linkselector/

RUN CGO_ENABLED=0 go build -trimpath -o /linklanding ./cmd/linklanding \
	&& CGO_ENABLED=0 go build -trimpath -o /linkselector ./cmd/linkselector \
	&& CGO_ENABLED=0 go build -trimpath -o /linkprobe ./cmd/linkprobe

FROM gcr.io/distroless/static-debian12:nonroot AS linklanding
COPY --from=build /linklanding /usr/local/bin/linklanding
COPY --from=build /linkprobe /usr/local/bin/linkprobe
USER 65532:65532
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/linklanding"]

FROM gcr.io/distroless/static-debian12:nonroot AS selector
COPY --from=build /linkselector /usr/local/bin/linkselector
COPY --from=build /linkprobe /usr/local/bin/linkprobe
USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/linkselector"]
