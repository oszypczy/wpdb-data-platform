"""Entry point: `python -m simulator`."""

from __future__ import annotations

import logging
import os
import signal
import sys

from simulator.config import Config, ConfigError
from simulator.db import Database
from simulator.engine import Heartbeat, Simulator

log = logging.getLogger("simulator")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    try:
        cfg = Config.from_env(os.environ)
    except ConfigError as exc:
        log.error("configuration: %s", exc)
        return 2
    sim = Simulator(cfg, Database(cfg.conninfo), heartbeat=Heartbeat(cfg.heartbeat_file))

    def stop(signum: int, _frame: object) -> None:
        log.info("%s received: stopping after the current transaction", signal.Signals(signum).name)
        sim.request_stop()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    sim.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
