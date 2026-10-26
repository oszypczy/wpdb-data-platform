"""05 §8: the supervisor retires only the silver query of a table whose topic is gone
(no JVM needed: the queries are fakes)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from lakehouse import supervisor as sv

pytestmark = pytest.mark.unit


def test_retire_dropped_stops_only_idle_silver_queries_without_topic(monkeypatch):
    stopped = []

    def managed(name):
        q = SimpleNamespace(isActive=True, stop=lambda: stopped.append(name))
        return sv.Managed(name, starter=lambda: q, query=q, started_at=0.0)

    names = ["silver_orders", "silver_dropped_test", "silver_to_gold", "gold_agg_revenue"]
    sup = sv.Supervisor.__new__(sv.Supervisor)
    sup.managed = {n: managed(n) for n in names}
    sup.listener = SimpleNamespace(last_input={})
    monkeypatch.setattr(sv.time, "time", lambda: sv.RETIRE_IDLE_S + 1.0)

    sup.retire_dropped(topics={"olist.shop.orders"})

    assert stopped == ["silver_dropped_test"]
    assert set(sup.managed) == {"silver_orders", "silver_to_gold", "gold_agg_revenue"}
