"""E2E fixtures (08 §9, skill `fault-injection`).

Every e2e module starts its own fresh stack (`down -v`, `up -d --build` with env
overrides such as a short `SIM_START`/`SIM_END` window, optionally an extra compose
file) through the session's ``StackManager``; tests of one module share it. The e2e
tests never use the integration `stack` fixture: its plain `docker compose up` (with
the `.env` window) would recreate the simulator of an e2e stack.

At the end of the session the default stack (`.env`, wiped volumes) is brought up and
waited for healthy, so the machine is left as after `make reset && make up`; set
``WPDB_E2E_KEEP=1`` to keep the last e2e stack for debugging instead. Evidence of each
fault (timings, events during the outage, reconciled counts) is appended to
``.pytest_cache/wpdb-e2e/report.jsonl``.
"""

from __future__ import annotations

import os

import pytest

from tests.helpers.e2e import StackManager


@pytest.fixture(scope="session")
def stacks(root):
    manager = StackManager(root)
    yield manager
    if os.environ.get("WPDB_E2E_KEEP") == "1":
        return
    manager.restore_default()
