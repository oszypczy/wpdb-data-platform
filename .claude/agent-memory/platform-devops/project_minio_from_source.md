---
name: project-minio-from-source
description: Decision (2026-10-20) to build MinIO and mc from GitHub source tarballs because the community images were withdrawn; CVE-2026-40344 mitigation by localhost-only ports
metadata:
  type: project
---

MinIO is built from source in `minio/Dockerfile` (tags `RELEASE.2025-10-15T17-29-55Z` for `minio`, `RELEASE.2025-08-13T08-35-41Z` for `mc`; Go 1.24.13; one image with both binaries, `minio-init` reuses it).

**Why:** MinIO archived `minio/minio` and `minio/mc` (2026-04-24) and removed the Docker Hub/Quay images (2026-09); `dl.min.io` returns 410. The project spec requires MinIO, so the user chose a from-source build over an S3-compatible replacement. The last release carries an unpatched auth bypass (CVE-2026-40344), so ports 9000/9091 are bound to 127.0.0.1 only. Documented fallback (unused): `bitnamilegacy/minio:2025.7.23-debian-12-r5`.
**How to apply:** do not look for official MinIO images again; bump only the ARG defaults in `minio/Dockerfile` (the integration test reads them). Keep the localhost binding and the CVE note in 06/08 when touching MinIO compose config.
