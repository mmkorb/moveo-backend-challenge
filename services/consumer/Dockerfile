# syntax=docker/dockerfile:1
# check=error=true
ARG GO_VERSION=1.27.2
FROM --platform=$BUILDPLATFORM golang:${GO_VERSION} AS build
WORKDIR /src
ENV GOWORK=off GOFLAGS=-mod=readonly
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=bind,source=go.mod,target=go.mod \
    --mount=type=bind,source=go.sum,target=go.sum \
    go mod download
ARG TARGETOS TARGETARCH
# COVER=1 builds with `go build -cover` for end-to-end coverage (docs/load-testing.md, "E2E coverage");
# the default leaves the image unchanged.
ARG COVER=0
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    flags=""; if [ "$COVER" = "1" ]; then flags="-cover"; fi; \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build $flags -trimpath -ldflags="-s -w" -o /out/app ./cmd/consumer

FROM build AS test
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=bind,target=. \
    go test ./...

FROM gcr.io/distroless/static-debian13:nonroot AS final
COPY --from=build /out/app /app
USER nonroot:nonroot
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --start-interval=2s --retries=3 \
    CMD ["/app", "healthcheck"]
ENTRYPOINT ["/app"]
CMD ["run"]
