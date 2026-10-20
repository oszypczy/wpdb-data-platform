#!/bin/sh
# connect-init: register every connector config found in /connect/*.json with
# PUT /connectors/<name>/config (create or update, hence idempotent), then wait
# until the connector and its tasks report RUNNING. The connector name is the
# file name without .json. No files: nothing to do, exit 0 (stage 0).
# Runs in curlimages/curl (busybox sh, no jq).
set -eu
CONNECT_URL="${CONNECT_URL:-http://connect:8083}"
TIMEOUT="${CONNECT_INIT_TIMEOUT:-120}"
DIR="${CONNECTORS_DIR:-/connect}"

found=0
for f in "$DIR"/*.json; do
  [ -e "$f" ] || continue
  found=1
  name=$(basename "$f" .json)
  echo "connect-init: PUT $CONNECT_URL/connectors/$name/config"
  code=$(curl -s -o /tmp/resp -w '%{http_code}' -X PUT "$CONNECT_URL/connectors/$name/config" \
           -H 'Content-Type: application/json' --data-binary "@$f")
  if [ "$code" != "200" ] && [ "$code" != "201" ]; then
    echo "connect-init: registration of $name failed with HTTP $code:" >&2
    cat /tmp/resp >&2; echo >&2
    exit 1
  fi
  waited=0
  until curl -sf "$CONNECT_URL/connectors/$name/status" -o /tmp/status \
        && grep -q '"connector":{"state":"RUNNING"' /tmp/status \
        && grep -q '"tasks":\[{' /tmp/status \
        && ! grep -q '"state":"FAILED"' /tmp/status; do
    waited=$((waited + 2))
    if [ "$waited" -ge "$TIMEOUT" ]; then
      echo "connect-init: $name not RUNNING after ${TIMEOUT}s:" >&2
      cat /tmp/status >&2 2>/dev/null; echo >&2
      exit 1
    fi
    sleep 2
  done
  echo "connect-init: $name RUNNING"
  cat /tmp/status; echo
done
if [ "$found" -eq 0 ]; then
  echo "connect-init: no connectors to register in $DIR"
fi
echo "connect-init: registered connectors: $(curl -sf "$CONNECT_URL/connectors")"
echo "connect-init: done"
