# syntax=docker/dockerfile:1.7
# MinIO server + mc client built from source.
#
# Why from source: MinIO archived the minio/minio and minio/mc repositories in
# 2026 and removed the community images from Docker Hub and Quay; dl.min.io
# returns 410. The last tagged releases are still available as GitHub source
# tarballs, so we build them ourselves (docs/design/08-operations.md §13).
# The resulting image carries both `minio` and `mc`; minio-init reuses it.
#
# Versions (the version matrix in docs/design/08-operations.md is the source of
# truth; tests/integration/test_minio.py reads the ARG defaults below):
ARG GO_IMAGE=golang:1.24.13-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${GO_IMAGE} AS build
ARG MINIO_TAG=RELEASE.2025-10-15T17-29-55Z
ARG MC_TAG=RELEASE.2025-08-13T08-35-41Z
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src

# Download the tagged sources (no git needed).
RUN set -eux; \
    curl -fsSL "https://codeload.github.com/minio/minio/tar.gz/refs/tags/${MINIO_TAG}" | tar xz; \
    mv "minio-${MINIO_TAG}" minio; \
    curl -fsSL "https://codeload.github.com/minio/mc/tar.gz/refs/tags/${MC_TAG}" | tar xz; \
    mv "mc-${MC_TAG}" mc

# Build with the same ldflags the upstream Makefile generates
# (buildscripts/gen-ldflags.go): ReleaseTag = the tag, Version = RFC3339 time of
# the tag. CommitID stays at its source default because the tarball has no git
# metadata. Module and build caches are BuildKit cache mounts, so a rebuild
# after a tag bump only compiles what changed.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    ver="${MINIO_TAG#RELEASE.}"; ver="$(echo "$ver" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z$/T\1:\2:\3Z/')"; \
    cd /src/minio && go build -trimpath \
      -ldflags "-s -w -X github.com/minio/minio/cmd.ReleaseTag=${MINIO_TAG} -X github.com/minio/minio/cmd.Version=${ver} -X github.com/minio/minio/cmd.CopyrightYear=${ver%%-*}" \
      -o /out/minio .; \
    ver="${MC_TAG#RELEASE.}"; ver="$(echo "$ver" | sed -E 's/T([0-9]{2})-([0-9]{2})-([0-9]{2})Z$/T\1:\2:\3Z/')"; \
    cd /src/mc && go build -trimpath \
      -ldflags "-s -w -X github.com/minio/mc/cmd.ReleaseTag=${MC_TAG} -X github.com/minio/mc/cmd.Version=${ver} -X github.com/minio/mc/cmd.CopyrightYear=${ver%%-*}" \
      -o /out/mc .; \
    /out/minio --version; /out/mc --version

FROM ${RUNTIME_IMAGE}
ARG MINIO_TAG
ARG MC_TAG
LABEL org.opencontainers.image.title="wpdb/minio" \
      org.opencontainers.image.description="MinIO ${MINIO_TAG} and mc ${MC_TAG} built from source" \
      org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.version="${MINIO_TAG}"
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/minio /out/mc /usr/local/bin/
ENV MC_CONFIG_DIR=/tmp/.mc
VOLUME ["/data"]
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
