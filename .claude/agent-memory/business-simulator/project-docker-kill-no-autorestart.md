---
name: docker-kill-no-autorestart
description: docker kill (SIGKILL) does not trigger restart: unless-stopped; fault tests must `compose up -d` the service again
metadata:
  type: project
---

`docker kill` / `docker compose kill` marks the container as manually stopped, so
`restart: unless-stopped` does NOT bring it back (verified empirically 2026-10-20 with
an alpine probe and with the simulator). Only `restart: always` would.

**Why:** 08 §5 originally claimed "Docker restarts it after docker kill"; the stage-1
kill/restart test showed the container stays `exited`. The policy still covers OOM,
process crashes and daemon restarts.

**How to apply:** every kill/restart test (simulator now, connect/kafka/spark/minio in
stage 6) does `stack.kill(svc)` then `stack.up([svc])`, then waits for the service's own
"resumed" evidence (log line / state row), not just Docker's `healthy`.
