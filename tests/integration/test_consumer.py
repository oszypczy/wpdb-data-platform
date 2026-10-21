"""The validating AVRO consumer as a container and as a host process (04-kafka.md §6).

`containers/consumer/` is the `consumer` service of compose profile `tools`
(`docker compose run --rm consumer …`). The tests build the image once per session,
read the olist.* topics with it and parse its `--json` output; the rules themselves
are unit-tested in tests/unit/test_consumer_rules.py.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

from tests.helpers import cdc, clients
from tests.helpers.cdc import topic
from tests.helpers.compose import ComposeStack

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
CONSUMER_DIR = ROOT / "containers" / "consumer"


@pytest.fixture
def fresh_lifecycle(stack, settings, topic_reader):
    """A probe lifecycle written right before the consumer runs: the topics are compacted
    mirrors (segment.ms = 1 h), so the per-key history of the session-wide probe may
    already be compacted away when this module runs; fresh records sit in the active
    segment, which the cleaner never touches."""
    app = clients.pg_connect("shop_app", settings["SHOP_APP_PASSWORD"])
    app.autocommit = True
    try:
        return cdc.run_probe_lifecycle(app, topic_reader)
    finally:
        app.close()


@pytest.fixture(scope="module")
def consumer_image(stack: ComposeStack):
    stack.run("--profile", "tools", "build", "consumer")
    return "wpdb/consumer"


def run_consumer(stack: ComposeStack, *args: str, timeout: float = 300) -> list[str]:
    """`docker compose run --rm consumer <args>`: stdout lines (compose chatter is on stderr)."""
    proc = stack.run("run", "--rm", "consumer", *args, check=False)
    assert proc.returncode == 0, f"consumer exited {proc.returncode}:\n{proc.stderr[-2000:]}"
    return [line for line in proc.stdout.splitlines() if line.strip()]


def run_consumer_on_host(*args: str) -> list[str]:
    """The same package as a host process against localhost:9092 (04 §6: works from the host)."""
    proc = subprocess.run(
        [sys.executable, "-m", "consumer", *args],
        cwd=ROOT,
        env={"PYTHONPATH": str(CONSUMER_DIR), "PATH": "/usr/bin:/bin"},
        text=True,
        capture_output=True,
        timeout=300,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr[-2000:]
    return [line for line in proc.stdout.splitlines() if line.strip()]


def parse_json_lines(lines: list[str]) -> tuple[list[dict], dict]:
    events = [json.loads(line) for line in lines if line.startswith("{")]
    summary = next(e["summary"] for e in events if "summary" in e)
    return [e for e in events if "summary" not in e], summary


def test_consumer_container_decodes_keys_and_values_through_the_registry(
    consumer_image, stack, probe_lifecycle
):
    """04 §1/§6: the container decodes AVRO keys and values of every olist.shop.* topic
    with schemas from Schema Registry; one-shot mode stops after --max-messages."""
    lines = run_consumer(
        stack, "--from-beginning", "--max-messages", "300", "--json", "--topics", topic("orders")
    )
    events, summary = parse_json_lines(lines)
    assert len(events) == 300 and summary["events"] == 300
    assert summary["decode_errors"] == 0
    for e in events:
        assert e["topic"] == topic("orders") and e["table"] == "orders"
        assert set(e["key"]) == {"order_id"}
        assert e["key_schema_id"]
        assert e["op"] in ("c", "u", "d", "r", "t")
        if e["op"] != "t":
            assert e["value_schema_id"]
            assert e["value"]["source"]["table"] == "orders"
            row = e["value"]["after"] or e["value"]["before"]
            assert row["order_id"] == e["key"]["order_id"]
            assert re.match(r"\d{4}-\d{2}-\d{2}T", row["order_purchase_timestamp"])
    # the snapshot came first: the first 300 records of a partition are mostly op=r
    assert summary["topics"][topic("orders")]["ops"].get("r", 0) > 0


def test_consumer_validate_flags_only_the_bad_probe(consumer_image, stack, fresh_lifecycle):
    """04 §6/§8: --validate on the whole stream reports zero violations on the simulator's
    data; the deliberately broken probe (delivered → created, 04 §6) is caught by the
    status-transition rule and nothing else fires."""
    lc = fresh_lifecycle
    lines = run_consumer(
        stack,
        "--from-beginning",
        "--idle-timeout",
        "8",
        "--validate",
        "--json",
        "--topics",
        ",".join(topic(t) for t in ("orders", "order_items", "order_payments", "customers")),
    )
    events, summary = parse_json_lines(lines)
    assert summary["events"] > 1000
    flagged = [e for e in events if e["violations"]]
    assert flagged, "the bad probe must be flagged"
    for e in flagged:
        assert e["topic"] == topic("orders") and e["op"] == "u", e
        assert e["key"]["order_id"].startswith("test-stage2-bad-"), e["key"]
        assert e["violations"] == ["status_transition: orders: delivered → created is not allowed"]
    assert any(e["key"]["order_id"] == lc.bad.order_id for e in flagged)
    assert summary["violations"] == len(flagged)
    assert not any(
        e["key"].get("order_id") == lc.clean.order_id and e["violations"] for e in events
    )


def test_consumer_stats_mode_reports_rates_ops_lag_and_schemas(consumer_image, stack):
    """04 §6: --stats prints, per interval and topic, events/s, the c/u/d/r/t split, the
    consumer lag and the schema ids seen."""
    lines = run_consumer(
        stack,
        "--from-beginning",
        "--stats",
        "--stats-interval",
        "1",
        "--max-messages",
        "2000",
        "--idle-timeout",
        "5",
        "--topics",
        topic("order_items"),
    )
    stats = [line for line in lines if line.startswith(topic("order_items") + " ")]
    assert stats, lines[-5:]
    pattern = re.compile(
        rf"^{re.escape(topic('order_items'))} \d+\.\d/s c=\d+ u=\d+ d=\d+ r=\d+ t=\d+ "
        r"lag=\d+ schemas=[\d,]+ violations=\d+$"
    )
    assert all(pattern.match(line) for line in stats), stats
    m = re.match(r"SUMMARY events=(\d+) violations=0 decode_errors=0", lines[-1])
    assert m and int(m.group(1)) > 0, lines[-1]
    # the last stats line counts exactly the events of the summary
    last = stats[-1]
    counted = sum(int(x) for x in re.findall(r" [cudrt]=(\d+)", last))
    assert counted == int(m.group(1)), (last, lines[-1])


def test_follow_order_prints_the_lifecycle_across_topics(consumer_image, stack, fresh_lifecycle):
    """04 §6: --follow-order <order_id> collects the order's events from every topic
    (customer, order, items, payment, status updates, deletion) in LSN order."""
    lc = fresh_lifecycle
    lines = run_consumer(
        stack, "--from-beginning", "--until-end", "--follow-order", lc.clean.order_id
    )
    start = lines.index(f"--- lifecycle of order {lc.clean.order_id} ---")
    timeline = [line for line in lines[start + 1 :] if line.startswith("[")]
    heads = [line.split(" lsn=")[0] for line in timeline]
    short, cust = lc.clean.order_id[:8], lc.clean.customer_id[:8]
    assert heads == [
        f"[customers] c {cust}",
        f"[orders] c {short} created {lc.clean.purchase_ts.isoformat(sep=' ')}+00:00",
        f"[order_items] c {short}/1 price=58.90 freight=13.29",
        f"[order_payments] c {short}/1 credit_card 72.19",
        f"[orders] u {short} created → approved",
        f"[order_payments] d {short}/1 credit_card 72.19",
        f"[order_payments] t {short}/1 tombstone",
        f"[order_items] d {short}/1 price=58.90 freight=13.29",
        f"[order_items] t {short}/1 tombstone",
        f"[orders] d {short} was approved",
        f"[orders] t {short} tombstone",
        f"[customers] d {cust}",
        f"[customers] t {cust} tombstone",
    ]
    assert all(" schema v" in line for line in timeline if not line.endswith("tombstone"))


def test_consumer_runs_from_the_host_too():
    """04 §6: the same script works outside Docker against localhost:9092 / :8081."""
    lines = run_consumer_on_host(
        "--from-beginning", "--max-messages", "20", "--topics", topic("sellers"), "--json"
    )
    events, summary = parse_json_lines(lines)
    assert len(events) == 20 and summary["events"] == 20
    assert all(e["op"] == "r" and set(e["key"]) == {"seller_id"} for e in events)
