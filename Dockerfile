# syntax=docker/dockerfile:1

# Builds one Shadowtap binary into a static, non-root distroless image.
#
#   docker build -t shadowtap/shadowtap-tap:dev .
#   docker build --build-arg CMD=./examples/demo/cmd/demo-api -t shadowtap/demo-api:dev .
#
# The build stage runs on the builder's own platform and cross-compiles, so a multi-arch
# build (docker buildx build --platform linux/amd64,linux/arm64 .) never runs Go under QEMU.

FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src

# Dependencies first, so source-only changes reuse this layer and the module cache.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Declared after the dependency layer: changing them must not invalidate it.
ARG CMD=./cmd/shadowtap-tap
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS="$TARGETOS" GOARCH="$TARGETARCH" \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$VERSION" -o /out/app "$CMD"

FROM gcr.io/distroless/static-debian12:nonroot
ARG VERSION=dev
LABEL org.opencontainers.image.source="https://github.com/Jenil133/Shadowtap" \
      org.opencontainers.image.version="$VERSION"
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
