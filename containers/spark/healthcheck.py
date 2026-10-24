"""Docker healthcheck of the `spark` service (08 §3).

Healthy = the supervisor's health file is fresh (< 60 s: the driver loop is alive) and
its status is ``ok``: the Kafka -> bronze query and every query started so far
(silver per table, gold, aggregates) are active. Exit code 0 = healthy, 1 = not.
"""

import json
import os
import sys
import time

PATH = os.environ.get("SPARK_HEALTH_FILE", "/tmp/spark/health.json")


def main() -> int:
    try:
        with open(PATH) as f:
            doc = json.load(f)
    except (OSError, ValueError) as exc:
        print(f"no health file: {exc}")
        return 1
    age = time.time() - float(doc.get("ts", 0))
    down = [n for n, q in doc.get("queries", {}).items() if not q.get("active")]
    print(
        f"status={doc.get('status')} age={age:.0f}s queries={len(doc.get('queries', {}))} "
        f"inactive={down}"
    )
    return 0 if age < 60 and doc.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
