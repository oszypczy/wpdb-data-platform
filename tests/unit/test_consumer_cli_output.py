"""The consumer CLI writes to a pipe line by line (04-kafka.md §6, QA finding of stage 5).

`docker compose run -T consumer …`, `uv run python -m consumer … | tee` and every other
non-TTY use must see each event line and each --stats block while the consumer runs, not
only when it exits; SIGTERM still ends with the final report; a reader that closes the
pipe early (`| head`) stops the consumer cleanly instead of a BrokenPipeError traceback.

No Docker: the real `Runner.run()` loop runs in a child process with a stub Kafka
consumer that hands out one tombstone of `olist.shop.orders` every 50 ms, and the child
is started without PYTHONUNBUFFERED (as on the host), with stdout connected to a pipe.
"""

from __future__ import annotations

import os
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

CONSUMER_DIR = Path(__file__).resolve().parents[2] / "containers" / "consumer"

CHILD = r"""
import sys, time
import consumer.cli as cli

class Raw:
    def __init__(self, n): self.n = n
    def topic(self): return "olist.shop.orders"
    def partition(self): return 0
    def offset(self): return self.n
    def key(self): return None
    def value(self): return None
    def error(self): return None

class StubConsumer:
    def __init__(self, conf): self.n = 0
    def subscribe(self, topics): pass
    def poll(self, timeout):
        time.sleep(0.05)
        self.n += 1
        return Raw(self.n)
    def assignment(self): return []
    def close(self): print("stub-consumer-closed", file=sys.stderr, flush=True)

cli.Consumer = StubConsumer
sys.exit(cli.main(sys.argv[1:]))
"""


def start(*args: str) -> subprocess.Popen:
    env = {k: v for k, v in os.environ.items() if k != "PYTHONUNBUFFERED"}
    env["PYTHONPATH"] = str(CONSUMER_DIR)
    return subprocess.Popen(
        [sys.executable, "-c", CHILD, *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def first_line(proc: subprocess.Popen, timeout: float = 10.0) -> str:
    """The first stdout line, or fail if none arrives within `timeout` seconds."""
    ready, _, _ = select.select([proc.stdout], [], [], timeout)
    if not ready:
        proc.kill()
        pytest.fail(f"no output through the pipe within {timeout} s (block-buffered stdout?)")
    return proc.stdout.readline()


def stop(proc: subprocess.Popen) -> tuple[str, str]:
    proc.send_signal(signal.SIGTERM)
    out, err = proc.communicate(timeout=10)
    assert proc.returncode == 0, err
    return out, err


def test_live_lines_reach_the_pipe_while_the_consumer_runs():
    proc = start()
    line = first_line(proc)
    assert line.startswith("[orders] t "), line  # tombstone line of describe()
    assert proc.poll() is None, "the line arrived only because the process exited"
    out, err = stop(proc)
    assert out.splitlines()[-1].startswith("SUMMARY events=")
    assert "stub-consumer-closed" in err


def test_stats_blocks_reach_the_pipe_while_the_consumer_runs():
    proc = start("--stats", "--stats-interval", "0.3")
    line = first_line(proc)
    assert line.startswith("olist.shop.orders ") and "/s " in line, line
    assert proc.poll() is None, "the stats block arrived only because the process exited"
    out, err = stop(proc)
    lines = out.splitlines()
    assert lines[-1].startswith("SUMMARY events=")
    assert lines[-2].startswith("olist.shop.orders "), "final stats block missing"
    assert not any(line.startswith("[orders]") for line in lines), "--stats prints no events"


def test_reader_closing_the_pipe_stops_the_consumer_cleanly():
    proc = start()
    first_line(proc)
    proc.stdout.close()  # like `| head -n 1` exiting
    deadline = time.monotonic() + 10
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.05)
    if proc.poll() is None:
        proc.kill()
        pytest.fail("consumer kept running after its reader closed the pipe")
    err = proc.stderr.read()
    assert proc.returncode == 0, err
    assert "Traceback" not in err and "BrokenPipe" not in err, err
    assert "stub-consumer-closed" in err
