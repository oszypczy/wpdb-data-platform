#!/bin/sh
# minio-init: buckets, policies and service accounts (docs/design/06-minio-delta.md §2).
# Idempotent: every step is an upsert or guarded, so a second run changes nothing
# and exits 0. Runs in the wpdb/minio image (has mc) as a one-shot container.
set -eu

MINIO_URL="${MINIO_URL:-http://minio:9000}"
BUCKETS="bronze silver gold ops checkpoints"
: "${MINIO_ROOT_USER:?}" "${MINIO_ROOT_PASSWORD:?}"
: "${MINIO_SPARK_ACCESS_KEY:?}" "${MINIO_SPARK_SECRET_KEY:?}"
: "${MINIO_DASHBOARD_ACCESS_KEY:?}" "${MINIO_DASHBOARD_SECRET_KEY:?}"

# depends_on: service_healthy already waited for MinIO; this retry only covers
# the window between the health probe and the API accepting authenticated calls.
i=0
until mc alias set local "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 15 ]; then echo "minio-init: cannot reach $MINIO_URL" >&2; exit 1; fi
  sleep 2
done

for b in $BUCKETS; do
  mc mb --ignore-existing "local/$b"
done

# `mc admin policy create` replaces an existing policy of the same name (upsert).
mc admin policy create local spark /minio/policies/spark.json
mc admin policy create local dashboard /minio/policies/dashboard.json

# `mc admin user add` creates the user or resets its secret key (upsert).
mc admin user add local "$MINIO_SPARK_ACCESS_KEY" "$MINIO_SPARK_SECRET_KEY"
mc admin user add local "$MINIO_DASHBOARD_ACCESS_KEY" "$MINIO_DASHBOARD_SECRET_KEY"

# `mc admin policy attach` fails when the policy is already attached, so guard it
# with the user's current policy list.
attach_once() {
  policy="$1"; user="$2"
  if mc admin user info --json local "$user" | grep -Eq "\"policyName\":\"([^\"]*,)?${policy}(,[^\"]*)?\""; then
    echo "policy $policy already attached to $user"
  else
    mc admin policy attach local "$policy" --user "$user"
  fi
}
attach_once spark "$MINIO_SPARK_ACCESS_KEY"
attach_once dashboard "$MINIO_DASHBOARD_ACCESS_KEY"

echo "minio-init: done"
mc ls local
mc admin user list local
