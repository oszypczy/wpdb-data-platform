# minio

Object store of the lakehouse (`docs/design/06-minio-delta.md`). Owner:
`streaming-lakehouse` for policies and layout, `platform-devops` for the image
and `minio-init`.

| File | Purpose |
|---|---|
| `Dockerfile` | multi-stage build of `minio` and `mc` from the pinned source tags (the community images were withdrawn in 2026; see 08-operations.md §13) |
| `init.sh` | `minio-init` one-shot: buckets `bronze silver gold ops checkpoints`, policies, accounts `spark` (RW all) and `dashboard` (RO gold+ops); idempotent |
| `policies/*.json` | IAM policies attached to the service accounts |

Host access: API `127.0.0.1:9000`, console `127.0.0.1:9091` (bound to localhost
only because the last community release carries an unpatched auth bypass,
CVE-2026-40344). Inside the network: `http://minio:9000`.
