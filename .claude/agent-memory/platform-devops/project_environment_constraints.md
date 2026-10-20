---
name: project-environment-constraints
description: Facts about the author's machine and network that shape how the stack is built and pulled (Docker Hub quota exhausted, port 9001 taken, arm64, RAM budget)
metadata:
  type: project
---

Author's machine: macOS Apple Silicon (arm64), Docker Desktop 27.3 / Compose v2.29, 8 CPU, 9.7 GB RAM for Docker, uv 0.9, Node 26; no `psql`/`mc` on the host.

- **Docker Hub anonymous pull quota (100/h per IP) is routinely exhausted on this network over IPv6** (`docker-ratelimit-source` is a /64 prefix, shared). Pulls return `429 Too Many Requests`. Working mirrors: `public.ecr.aws/docker/library/<image>` for Docker official images, `mirror.gcr.io/<org>/<image>` for others (e.g. `confluentinc/cp-kafka`), `quay.io/curl/curl` for curlimages. Pull from the mirror, then `docker tag` to the canonical name so compose does not pull.
- **Host port 9001 is taken by another container** (`unimot-caddy`, also holds 8080, 9090, 443). MinIO console is therefore on host port 9091; keep 5432/9092/8081/8083/9000/9091/4040/8501 as the contract.
- Stage-0 stack reaches healthy in ~47 s (fresh or warm); the time is dominated by healthcheck `start_period`/intervals, not by work.

**Why:** these are environment facts, not derivable from the repo, and they cost an hour of waiting when forgotten.
**How to apply:** before pulling new images, check the quota (`ratelimitpreview/test` manifest headers) or go straight to a mirror; never propose host port 9001/8080/9090.
