#!/usr/bin/env bash
# Block until every compose service with a healthcheck is healthy and every
# one-shot init service has exited 0. Usage: wait-healthy.sh [timeout_seconds]
set -euo pipefail
timeout="${1:-300}"
deadline=$(( $(date +%s) + timeout ))
while :; do
  bad=0
  # '|' as separator: a tab is IFS whitespace, so empty fields (no health) would collapse
  while IFS='|' read -r name state health exit_code; do
    case "$state" in
      running) # a one-shot *-init container is done only once it exited 0
               if [[ "$name" == *-init ]]; then bad=1
               else [[ "$health" == "healthy" || -z "$health" ]] || bad=1; fi ;;
      exited)  [[ "$exit_code" == "0" ]] || { echo "FAILED: $name exited $exit_code"; exit 1; } ;;
      *)       bad=1 ;;
    esac
  done < <(docker compose ps -a --format '{{.Name}}|{{.State}}|{{.Health}}|{{.ExitCode}}')
  if [[ $bad -eq 0 ]]; then echo "all services healthy"; docker compose ps; exit 0; fi
  if (( $(date +%s) > deadline )); then echo "TIMEOUT after ${timeout}s"; docker compose ps -a; exit 1; fi
  sleep 3
done
