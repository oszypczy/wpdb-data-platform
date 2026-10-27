"""05 §10 integration rows: Spark on the live stack (Kafka + Schema Registry + MinIO).

The tests write their own probe rows through Postgres (as `shop_app`, like the
simulator) and read the result back from Delta without Spark (DuckDB with the read-only
`agent` account, delta-rs), so they check the running `spark` container end to end.
"""

from __future__ import annotations

import json
from decimal import ROUND_HALF_UP, Decimal

import pytest

from tests.helpers import cdc
from tests.helpers.clients import pg_connect
from tests.helpers.lake import Lake
from tests.helpers.wait import wait_until

pytestmark = pytest.mark.integration

TABLES = (
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "product_category_name_translation",
)
PK_PROPERTY = "wpdb.primaryKey"
TIMEOUT = 240  # trigger 10 s, a few chained micro-batches, plus slack for a busy laptop


@pytest.fixture(scope="module")
def lake(stack, settings) -> Lake:
    return Lake(settings, "agent")


@pytest.fixture
def app(stack, settings):
    conn = pg_connect("shop_app", settings["SHOP_APP_PASSWORD"])
    conn.autocommit = True
    yield conn
    conn.close()


def _silver_row(lake: Lake, table: str, where: str, params: list) -> dict | None:
    rel = lake.con.execute(f"SELECT * FROM delta_scan('s3://silver/{table}') WHERE {where}", params)
    cols = [d[0] for d in rel.description]
    row = rel.fetchone()
    return dict(zip(cols, row, strict=True)) if row else None


def test_real_debezium_messages_reach_bronze_and_silver_with_types(
    lake, app, pg_reader, probe_shop
):
    """05 §10: "prawdziwe wiadomości Debezium" → bronze i silver zapisane; kolumny i typy
    poprawne (`decimal`, `timestamp`). A purchase written like the simulator's (one
    transaction over 4 tables) goes through, then an UPDATE and the DELETE. Written into
    the probe shop (mirrors of the business tables with the same column types), so no
    status change reaches gold."""
    shop = probe_shop
    items_t, orders_t = shop.table("order_items"), shop.table("orders")
    clock = cdc.sim_state(pg_reader)["sim_clock"]
    order = cdc.probe_purchase(app, shop, "stage3", "created", clock)
    try:
        item = wait_until(
            lambda: _silver_row(lake, items_t, "order_id = ?", [order.order_id]),
            TIMEOUT,
            every=2,
            desc="probe item in silver",
        )
        pg = pg_reader.execute(
            f"SELECT price, freight_value, shipping_limit_date FROM shop.{items_t} "
            "WHERE order_id = %s",
            (order.order_id,),
        ).fetchone()
        assert (item["price"], item["freight_value"]) == (Decimal("58.90"), Decimal("13.29"))
        assert (item["price"], item["freight_value"]) == (pg[0], pg[1])
        # timestamp-millis of a naive Postgres TIMESTAMP: same wall-clock value, UTC
        assert item["shipping_limit_date"].replace(tzinfo=None) == pg[2]
        assert item["_is_deleted"] is False and item["_op"] == "c" and item["_lsn"] > 0

        schema = {f.name: f.type for f in lake.table(f"s3://silver/{items_t}").schema().fields}
        assert str(schema["price"]) == 'PrimitiveType("decimal(10,2)")'
        assert str(schema["shipping_limit_date"]) == 'PrimitiveType("timestamp")'
        assert str(schema["order_item_id"]) == 'PrimitiveType("integer")'

        # every table of the transaction is in bronze with the full envelope
        for role in cdc.PROBE_SHOP_TABLES:
            key = "customer_id" if role == "customers" else "order_id"
            val = order.customer_id if role == "customers" else order.order_id
            rows = lake.sql(
                f"SELECT op, before IS NULL, after.{key}, source_lsn, transaction_id, "
                f"value_schema_id, kafka_offset FROM delta_scan('s3://bronze/"
                f"{shop.table(role)}') "
                f"WHERE after.{key} = ?",
                [val],
            )
            assert len(rows) == 1, (role, rows)
            op, no_before, _, lsn, tx, sid, _ = rows[0]
            assert (op, no_before) == ("c", True) and lsn > 0 and tx and sid > 0

        # UPDATE then DELETE: silver follows, the delete is soft (05 §4)
        approved = clock
        cdc.probe_set_status(app, shop, order, "approved", order_approved_at=approved)
        wait_until(
            lambda: (
                (_silver_row(lake, orders_t, "order_id = ?", [order.order_id]) or {}).get(
                    "order_status"
                )
                == "approved"
            ),
            TIMEOUT,
            every=2,
            desc="approved in silver",
        )
    finally:
        cdc.probe_delete(app, shop, order)
    gone = wait_until(
        lambda: (
            (r := _silver_row(lake, items_t, "order_id = ? AND _is_deleted", [order.order_id]))
            and r
        ),
        TIMEOUT,
        every=2,
        desc="soft delete in silver",
    )
    assert gone["_op"] == "d" and gone["price"] == Decimal("58.90")  # the before image
    bronze_ops = lake.sql(
        f"SELECT op FROM delta_scan('s3://bronze/{orders_t}') WHERE coalesce(after.order_id, "
        "before.order_id) = ? ORDER BY source_lsn",
        [order.order_id],
    )
    assert [r[0] for r in bronze_ops] == ["c", "u", "d"]


def test_primary_keys_come_from_the_key_schema(lake, pg_reader, schema_registry):
    """05 §10: "PK ze schematu klucza" → PK zgodne z tabelami (the table added at runtime
    is covered by test_add_column_reaches_silver_without_restart)."""
    for t in TABLES:
        topic = f"olist.shop.{t}"
        key = json.loads(schema_registry.json(f"/subjects/{topic}-key/versions/latest")["schema"])
        from_registry = [f["name"] for f in key["fields"]]
        assert from_registry == cdc.primary_key(pg_reader, t), t
        assert lake.properties(f"s3://silver/{t}")[PK_PROPERTY] == ",".join(from_registry), t
        assert lake.properties(f"s3://bronze/{t}")[PK_PROPERTY] == ",".join(from_registry), t


def test_spark_never_connects_to_postgres(stack, pg_reader):
    """05 §4, CLAUDE.md: Spark has no Postgres credentials, no JDBC driver, and no session
    from its container shows up in pg_stat_activity."""
    env = json.loads(stack.docker("inspect", "spark").stdout)[0]["Config"]["Env"]
    assert not [e for e in env if e.startswith(("PG", "POSTGRES", "SHOP_APP", "DEBEZIUM"))]
    jars = stack.exec("spark", "ls", "/opt/spark/jars").stdout
    assert "postgresql" not in jars.lower()
    nets = json.loads(stack.docker("inspect", "spark").stdout)[0]["NetworkSettings"]["Networks"]
    ip = next(iter(nets.values()))["IPAddress"]

    def sessions_from_spark() -> int:
        return pg_reader.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE client_addr = %s::inet", (ip,)
        ).fetchone()[0]

    assert [sessions_from_spark() for _ in range(5)] == [0] * 5


def test_add_column_reaches_silver_without_restart(lake, app, stack, topic_reader, settings):
    """05 §10: "ADD COLUMN w trakcie działania" → nowa kolumna w silver bez restartu, and
    "PK ze schematu klucza" for a table added at runtime: a new captured table gets its
    bronze/silver tables and its PK from `<topic>-key`; then `ALTER TABLE ADD COLUMN`
    reaches silver while the container keeps running (same start time, no restart)."""
    from tests.helpers.clients import CONNECT_URL, SCHEMA_REGISTRY_URL, Http, kafka_admin

    before = json.loads(stack.docker("inspect", "spark").stdout)[0]["State"]
    admin = kafka_admin()
    probe = cdc.CapturedProbe.create(app, topic_reader, admin)
    try:
        uri = f"s3://silver/{probe.table}"
        row1 = wait_until(
            lambda: lake.exists(uri) and _silver_row(lake, probe.table, "id = 1", []),
            TIMEOUT,
            every=3,
            desc="runtime table in silver",
        )
        assert row1["v"] == "1"
        assert lake.properties(uri)[PK_PROPERTY] == "id"
        assert lake.properties(f"s3://bronze/{probe.table}")[PK_PROPERTY] == "id"

        start = topic_reader.watermarks(probe.topic)
        app.execute(f"ALTER TABLE shop.{probe.table} ADD COLUMN w text")
        app.execute(f"INSERT INTO shop.{probe.table} VALUES (2, 'b', 'new')")
        rec = topic_reader.read_new(start, lambda rs: any(r.value for r in rs), timeout=60)
        for r in rec:
            if r.value:
                probe.note(r)
        row2 = wait_until(
            lambda: _silver_row(lake, probe.table, "id = 2", []),
            TIMEOUT,
            every=3,
            desc="evolved row in silver",
        )
        assert row2["w"] == "new"
        assert _silver_row(lake, probe.table, "id = 1", [])["w"] is None
        after = json.loads(stack.docker("inspect", "spark").stdout)[0]["State"]
        assert after["StartedAt"] == before["StartedAt"] and after["Status"] == "running"
    finally:
        probe.teardown(app, Http(CONNECT_URL), Http(SCHEMA_REGISTRY_URL), admin, topic_reader)
    assert settings  # the module-level fixtures stay alive for the teardown


@pytest.mark.slow
def test_restart_from_checkpoint_keeps_bronze_offsets_unique(
    lake, app, stack, pg_reader, probe_shop
):
    """05 §10: "restart z checkpointu" → w bronze unikalne `(topic, partition, offset)`.
    The container is restarted while the simulator keeps writing; a probe (probe shop)
    written after the restart arrives exactly once; no Kafka record is in bronze twice."""
    stack.run("restart", "spark")
    stack.wait_healthy(timeout=300)
    clock = cdc.sim_state(pg_reader)["sim_clock"]
    order = cdc.probe_purchase(app, probe_shop, "restart", "created", clock)
    try:
        wait_until(
            lambda: (
                lake.scalar(
                    f"SELECT count(*) FROM delta_scan('s3://bronze/{probe_shop.table('orders')}') "
                    "WHERE after.order_id = ?",
                    [order.order_id],
                )
                == 1
            ),
            TIMEOUT,
            every=3,
            desc="probe in bronze after restart",
        )
    finally:
        cdc.probe_delete(app, probe_shop, order)
    for t in TABLES:
        total, distinct = lake.sql(
            "SELECT count(*), count(DISTINCT (kafka_topic, kafka_partition, kafka_offset)) "
            f"FROM delta_scan('s3://bronze/{t}')"
        )[0]
        assert total == distinct, (t, total, distinct)


GOLD_VS_TRUTH = {
    # truth view -> the same aggregate over gold (DuckDB), keyed by the first column
    "v_truth_reviews": (
        "SELECT review_score, count(*), count(DISTINCT order_id) "
        "FROM delta_scan('s3://gold/fact_reviews') GROUP BY 1"
    ),
    "v_truth_reviews_by_delivery": (
        "SELECT NOT is_late, count(*), sum(review_score) "
        "FROM delta_scan('s3://gold/fact_reviews') WHERE is_delivered GROUP BY 1"
    ),
    "v_truth_payments": (
        "SELECT payment_type, count(*), sum(payment_value) "
        "FROM delta_scan('s3://gold/fact_payments') GROUP BY 1"
    ),
    "v_truth_payment_installments": (
        "SELECT payment_installments, count(*), sum(payment_value) "
        "FROM delta_scan('s3://gold/fact_payments') GROUP BY 1"
    ),
}


def _avg2(total: int, n: int) -> Decimal:
    """avg(...)::numeric(4,2) as Postgres rounds it (half away from zero)."""
    return (Decimal(total) / Decimal(n)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _gold_aggregates(lake: Lake) -> dict[str, dict]:
    out = {}
    for view, sql in GOLD_VS_TRUTH.items():
        rows = lake.sql(sql)
        if view == "v_truth_reviews_by_delivery":
            out[view] = {k: (int(n), _avg2(int(s), int(n))) for k, n, s in rows}
        else:
            out[view] = {k: (int(a), b if isinstance(b, Decimal) else int(b)) for k, a, b in rows}
    return out


def _truth_aggregates(pg) -> dict[str, dict]:
    out = {}
    for view in GOLD_VS_TRUTH:
        rows = pg.execute(f"SELECT * FROM sim.{view}").fetchall()
        out[view] = {k: (int(a), b if isinstance(b, Decimal) else int(b)) for k, a, b in rows}
    return out


def test_payments_and_reviews_in_gold_reconcile_with_the_truth_views(lake, pg_reader):
    """05 §10: gold/fact_payments and gold/fact_reviews equal the Postgres truth views
    once Spark has caught up: reviews and distinct orders per score, reviews and average
    score of delivered orders per on-time flag (is_late of fact_orders_lifecycle), and
    payments with their value per type and per number of installments. Both sides are
    re-read every round, so the test also passes while the simulator is still running."""
    for t in ("fact_payments", "fact_reviews"):
        assert lake.exists(f"s3://gold/{t}"), t
    state: dict[str, dict] = {}

    def equal() -> bool:
        state["truth"] = _truth_aggregates(pg_reader)
        state["gold"] = _gold_aggregates(lake)
        return state["gold"] == state["truth"]

    try:
        wait_until(equal, 300, every=5, desc="gold payments/reviews = sim.v_truth_*")
    except TimeoutError:
        diff = {
            v: (state["gold"][v], state["truth"][v])
            for v in GOLD_VS_TRUTH
            if state["gold"].get(v) != state["truth"].get(v)
        }
        pytest.fail(f"gold != truth after 300 s (gold, truth): {diff}")
    truth = state["truth"]
    assert sum(n for n, _ in truth["v_truth_reviews"].values()) > 0
    assert sum(n for n, _ in truth["v_truth_payments"].values()) > 0
