"""Polling helper: no `sleep`-based synchronization in tests (run-tests skill)."""

from __future__ import annotations

import time
from collections.abc import Callable


def wait_until[T](
    pred: Callable[[], T],
    timeout: float,
    every: float = 1.0,
    desc: str = "condition",
) -> T:
    """Call `pred` until it returns a truthy value or `timeout` seconds pass.

    Returns the truthy value. Exceptions raised by `pred` count as "not yet" and
    the last one is attached to the TimeoutError so the failure explains itself.
    """
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while True:
        try:
            value = pred()
            if value:
                return value
            last_exc = None
        except Exception as exc:  # noqa: BLE001 - polling swallows transient errors by design
            last_exc = exc
        if time.monotonic() >= deadline:
            detail = f" (last error: {last_exc!r})" if last_exc else ""
            raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {desc}{detail}")
        time.sleep(every)
