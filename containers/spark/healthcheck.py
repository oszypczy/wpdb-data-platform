"""Docker healthcheck of the `spark` service (08 §3).

Healthy = the supervisor's health file is fresh (< 60 s: the driver loop is alive) and
its status is ``ok``: the Kafka -> bronze query and every query started so far
(silver per table, gold, aggregates) are active, and none of them is ``recovering``
(failed within the last 5 min and has not completed a trigger since, e.g. while MinIO
rejects writes, 08 §12.8). Exit code 0 = healthy, 1 = not.
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
    queries = doc.get("queries", {})
    down = [n for n, q in queries.items() if not q.get("active")]
    failing = [n for n, q in queries.items() if q.get("recovering")]
    causes = sorted({c for n in down + failing if (c := queries[n].get("last_cause"))})
    print(
        f"status={doc.get('status')} age={age:.0f}s queries={len(queries)} "
        f"inactive={down} recovering={failing} s3={causes}"
    )
    return 0 if age < 60 and doc.get("status") == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
