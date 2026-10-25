# syntax=docker/dockerfile:1

# ---- build ----
FROM golang:1.24-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
# Retry flaky downloads; the cache mount keeps modules already fetched between attempts and builds
RUN --mount=type=cache,target=/go/pkg/mod \
    for i in 1 2 3 4 5; do go mod download && exit 0; echo "go mod download failed, retry $i"; sleep 5; done; exit 1

COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/fraud-processor ./cmd/fraud-processor && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

# ---- runtime ----
# distroless/static: no shell, no package manager, runs as non-root (uid 65532)
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/fraud-processor /usr/local/bin/fraud-processor
COPY --from=build /out/loadgen /usr/local/bin/loadgen
EXPOSE 2112
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/fraud-processor"]
