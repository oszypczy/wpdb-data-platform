#!/bin/bash
# kafka-init: create the internal topics listed in /kafka/topics.txt.
# Idempotent: --if-not-exists makes a second run a no-op; a topic that exists with
# different settings is reported but not altered (change the file and reset the
# volume for that). Uses the Confluent CLI wrappers from the cp-kafka image.
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-kafka:29092}"
TOPICS_FILE="${TOPICS_FILE:-/kafka/topics.txt}"

echo "kafka-init: bootstrap=$BOOTSTRAP file=$TOPICS_FILE"
while read -r name partitions config; do
  [[ -z "$name" || "$name" == \#* ]] && continue
  args=(--bootstrap-server "$BOOTSTRAP" --create --if-not-exists
        --topic "$name" --partitions "$partitions" --replication-factor 1)
  IFS=',' read -ra cfgs <<< "$config"
  for c in "${cfgs[@]}"; do [[ -n "$c" ]] && args+=(--config "$c"); done
  kafka-topics "${args[@]}"
done < "$TOPICS_FILE"

echo "kafka-init: current state"
while read -r name _; do
  [[ -z "$name" || "$name" == \#* ]] && continue
  kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$name"
done < "$TOPICS_FILE"
echo "kafka-init: done"
