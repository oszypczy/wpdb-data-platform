"""Container healthcheck: the heartbeat file is fresh and the preload is over.

Healthy only in the `replay` and `done` phases, so a service that must see the
preloaded history as a Debezium snapshot can `depends_on: simulator: service_healthy`.
"""

import os
import sys
import time

MAX_AGE_SECONDS = 30
HEALTHY_PHASES = ("replay", "done")


def main() -> int:
    path = os.environ.get("SIM_HEARTBEAT_FILE", "/tmp/simulator/heartbeat")
    try:
        age = time.time() - os.stat(path).st_mtime
        with open(path) as f:
            phase = f.read().strip()
    except OSError:
        return 1
    return 0 if age < MAX_AGE_SECONDS and phase in HEALTHY_PHASES else 1


if __name__ == "__main__":
    sys.exit(main())
