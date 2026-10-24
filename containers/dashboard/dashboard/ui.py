"""Shared Streamlit plumbing for the pages: the per-process reader, live fragments,
empty states and number formatting. Layout and charts only; logic is in ``queries``."""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import streamlit as st

from dashboard.lake import Lake, LakeConfig, VersionedReader

# 07 §1: data refresh every 10 s (only the fragment with the data reruns, 07 §3)
REFRESH_S = float(os.environ.get("DASHBOARD_REFRESH_S", "10"))

ITEMS = "gold/fact_order_items"
LIFECYCLE = "gold/fact_orders_lifecycle"
REVENUE_HOURLY = "gold/agg_revenue_hourly"
REVENUE_EVENTS = "gold/revenue_events"
FUNNEL = "gold/agg_status_funnel"
GEOGRAPHY = "gold/dim_geography"
PRODUCTS = "gold/dim_product"
CUSTOMERS = "gold/dim_customer"
HEARTBEAT = "ops/cdc_heartbeat"
LATENCY = "ops/e2e_latency"
METRICS = "ops/stream_metrics"
QUARANTINE = "ops/quarantine"
MAINTENANCE = "ops/maintenance_log"

WAITING = (
    "Czekam na dane: warstwa gold jest jeszcze pusta (system dopiero wystartował "
    "albo Spark nie przetworzył pierwszych zdarzeń). Strona odświeży się sama."
)


@st.cache_resource(show_spinner=False)
def _reader(root: str, endpoint: str, access_key: str, secret_key: str) -> VersionedReader:
    return VersionedReader(Lake(LakeConfig(root, endpoint, access_key, secret_key)))


def reader() -> VersionedReader:
    """One version-aware reader per process and lake (shared by all browser sessions)."""
    c = LakeConfig.from_env()
    return _reader(c.root, c.endpoint, c.access_key, c.secret_key)


def live(fn: Callable[[], Any]) -> Callable[[], Any]:
    """Run ``fn`` as a fragment re-executed every REFRESH_S seconds (07 §3). Errors from
    the lake (e.g. MinIO restarting) are shown in place; the next refresh retries."""

    @st.fragment(run_every=REFRESH_S)
    def wrapper() -> None:
        try:
            fn()
        except Exception as exc:  # noqa: BLE001 - shown to the user, retried next run
            st.error(f"Nie udało się odczytać danych z MinIO: {type(exc).__name__}: {exc}")

    return wrapper


def waiting() -> None:
    st.info(WAITING, icon="⏳")


def now_utc() -> datetime:
    return datetime.now(UTC)


def _group(n: float, decimals: int) -> str:
    s = f"{n:,.{decimals}f}"
    return s.replace(",", " ").replace(".", ",")


def money(v: Decimal | float | None) -> str:
    if v is None:
        return "–"
    f = float(v)
    return f"{'−' if f < 0 else ''}R$ {_group(abs(f), 2)}"


def md(text: str) -> str:
    """Escape ``$`` for st.markdown/st.caption (it would start LaTeX: "R$ … R$")."""
    return text.replace("$", "\\$")


def number(v: float | int | None, decimals: int = 0) -> str:
    return "–" if v is None else _group(float(v), decimals)


def pct(v: float | None, decimals: int = 1) -> str:
    return "–" if v is None else f"{_group(float(v), decimals)} %"


def seconds(v: float | None) -> str:
    if v is None:
        return "–"
    v = float(v)
    if v < 120:
        return f"{_group(v, 1)} s"
    if v < 7200:
        return f"{_group(v / 60, 1)} min"
    return f"{_group(v / 3600, 1)} h"


def sim_time(ts: datetime | None) -> str:
    return "–" if ts is None else ts.strftime("%Y-%m-%d %H:%M")


def status(value: float | None, warn: float, crit: float) -> str:
    """good / warning / critical for 'lower is better' values (icon + label, 07 §2)."""
    if value is None:
        return "critical"
    return "good" if value < warn else "warning" if value < crit else "critical"


STATUS_LABEL = {"good": "OK", "warning": "uwaga", "critical": "problem"}


def chart(fig: Any, key: str | None = None) -> None:
    st.plotly_chart(fig, theme="streamlit", width="stretch", key=key)


def table(df: Any, **kwargs: Any) -> None:
    st.dataframe(df, hide_index=True, width="stretch", **kwargs)
