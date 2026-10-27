"""Shared Streamlit plumbing for the pages: the per-process reader, live fragments,
empty states and number formatting. Layout and charts only; logic is in ``queries``."""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import pandas as pd
import streamlit as st

from dashboard.lake import Lake, LakeConfig, VersionedReader

# 07 §1: data refresh every 10 s (only the fragment with the data reruns, 07 §3)
REFRESH_S = float(os.environ.get("DASHBOARD_REFRESH_S", "10"))

# The lake keeps Olist amounts in BRL (they reconcile with the Postgres truth views);
# the dashboard shows them in USD at one fixed rate, converted only for display.
# Default: 1 / 3.19, the average USD/BRL rate of 2017 (the bulk of the dataset).
BRL_USD = Decimal(os.environ.get("DASHBOARD_BRL_USD", "0.3135"))
CENT = Decimal("0.01")

ITEMS = "gold/fact_order_items"
LIFECYCLE = "gold/fact_orders_lifecycle"
REVENUE_HOURLY = "gold/agg_revenue_hourly"
REVENUE_EVENTS = "gold/revenue_events"
FUNNEL = "gold/agg_status_funnel"
GEOGRAPHY = "gold/dim_geography"
PRODUCTS = "gold/dim_product"
CUSTOMERS = "gold/dim_customer"
PAYMENTS = "gold/fact_payments"
REVIEWS = "gold/fact_reviews"
HEARTBEAT = "ops/cdc_heartbeat"
LATENCY = "ops/e2e_latency"
QUARANTINE = "ops/quarantine"

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
    st.info(WAITING)


def now_utc() -> datetime:
    return datetime.now(UTC)


def _group(n: float, decimals: int) -> str:
    s = f"{n:,.{decimals}f}"
    return s.replace(",", " ").replace(".", ",")


def usd(v: Decimal | None) -> Decimal | None:
    """A BRL amount from the lake in USD, rounded to cents."""
    return None if v is None else (Decimal(v) * BRL_USD).quantize(CENT, ROUND_HALF_UP)


def usd_fields(obj: Any, *fields: str) -> Any:
    """A copy of a KPI dataclass with the given BRL fields in USD."""
    return dataclasses.replace(obj, **{f: usd(getattr(obj, f)) for f in fields})


def usd_frame(df: pd.DataFrame, *cols: str) -> pd.DataFrame:
    """A copy of a chart frame with the given BRL columns in USD."""
    return df.assign(**{c: df[c] * float(BRL_USD) for c in cols})


def money_columns(*cols: str) -> dict[str, Any]:
    """``column_config`` for st.dataframe: USD columns shown with cents."""
    return {c: st.column_config.NumberColumn(f"{c} (USD)", format="%.2f") for c in cols}


def money(v: Decimal | float | None, decimals: int = 2) -> str:
    """Format an amount that is already in USD."""
    if v is None:
        return "–"
    # half-up like usd(): via the shortest repr, so 9.405 (a float sum) shows as 9,41
    d = Decimal(str(v)).quantize(Decimal(1).scaleb(-decimals), ROUND_HALF_UP)
    return f"{'−' if d < 0 else ''}$ {_group(abs(d), decimals)}"


def md(text: str) -> str:
    """Escape ``$`` for st.markdown/st.caption/help (two of them would start LaTeX)."""
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


STATUS_LABEL = {"good": "OK", "warning": "uwaga", "critical": "problem", "info": "bez sondy"}
STATUS_COLOR = {"good": "green", "warning": "orange", "critical": "red", "info": "gray"}


def status_caption(s: str) -> str:
    """Coloured label of a health tile (colour + text, no emoji)."""
    return f":{STATUS_COLOR[s]}[● {STATUS_LABEL[s]}]"


def chart(fig: Any, key: str | None = None) -> None:
    st.plotly_chart(fig, theme="streamlit", width="stretch", key=key)


def table(df: Any, **kwargs: Any) -> None:
    st.dataframe(df, hide_index=True, width="stretch", **kwargs)
