"""Zdrowie potoku: CDC freshness, end-to-end latency, micro-batches, quarantine (07 §2).

Everything comes from the ops Delta tables written by Spark; the dashboard reads no
Kafka and no Postgres (replication slot lag is checked by tests, 02 §5).
"""

from datetime import timedelta

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Zdrowie potoku")
WINDOWS = {"1 h": 1, "6 h": 6, "24 h": 24, "wszystko": None}
f1, f2 = st.columns([1, 3])
window = f1.radio("Okno (od ostatniego pomiaru)", list(WINDOWS), horizontal=True)
MAIN_QUERIES = ["kafka_to_bronze", "silver_orders", "silver_to_gold", "gold_agg_revenue"]
queries_sel = f2.multiselect(
    "Zapytania strumieniowe na wykresie",
    MAIN_QUERIES
    + ["silver_order_items", "silver_customers", "gold_transitions", "gold_agg_funnel"],
    default=MAIN_QUERIES,
)

# thresholds of the status icons (e2e thresholds of the final stage: 05 §12)
CDC_WARN_S, CDC_CRIT_S = 30, 120
LAT = {"silver": (60, 120), "gold": (120, 240)}


def tile(col, label: str, value: float | None, warn: float, crit: float, help: str) -> None:
    s = ui.status(value, warn, crit)
    col.metric(label, ui.seconds(value), help=help)
    col.caption(ui.status_caption(s))


@ui.live
def body() -> None:
    r = ui.reader()
    fresh = q.cdc_freshness(r.get(ui.HEARTBEAT), ui.now_utc()).iloc[0]
    lat = r.derive("latency_summary", [ui.LATENCY], q.latency_summary, 60)
    quarantine = r.derive("quarantine", [ui.QUARANTINE], q.quarantine_by_rule)
    by_layer = {row.layer: row for row in lat.itertuples()}

    c = st.columns(4)
    tile(
        c[0],
        "Świeżość CDC",
        fresh["freshness_s"],
        CDC_WARN_S,
        CDC_CRIT_S,
        "Wiek ostatniego heartbeatu Debezium (co 10 s) zapisanego przez Sparka w "
        "ops/cdc_heartbeat: działa cała ścieżka Postgres → Kafka → Spark → Delta.",
    )
    for col, layer in zip(c[1:3], ("silver", "gold"), strict=True):
        row = by_layer.get(layer)
        tile(
            col,
            f"Opóźnienie e2e → {layer} (p95)",
            None if row is None else row.p95_s,
            *LAT[layer],
            f"Mediana p95 paczek z ostatniej godziny pomiarów: zapis w {layer} − commit "
            "w Postgresie (ops/e2e_latency).",
        )
    c[3].metric(
        "Kwarantanna",
        ui.number(int(quarantine["rows"].sum()) if len(quarantine) else 0),
        help="Wiersze łamiące reguły jakości.",
    )
    if fresh["heartbeats"] == 0 and lat.empty:
        ui.waiting()
        return
    if fresh["last_heartbeat"] is not None and fresh["heartbeats"]:
        st.caption(f"Ostatni heartbeat CDC: {fresh['last_heartbeat']} UTC.")

    hours = WINDOWS[window]
    e2e = r.get(ui.LATENCY)
    lat_ts = r.derive("latency_ts", [ui.LATENCY], q.latency_timeseries, None)
    if hours and not lat_ts.empty:
        lat_ts = lat_ts[
            lat_ts["measured_at"] >= lat_ts["measured_at"].max() - timedelta(hours=hours)
        ]
    st.markdown("**Opóźnienie end-to-end (p95 paczki, s)**")
    if e2e.num_rows == 0 or lat_ts.empty:
        st.info("Brak pomiarów opóźnienia (potok bezczynny od startu).")
    else:
        ui.chart(
            charts.lines(lat_ts, "measured_at", "p95_s", "layer", "s", order=["silver", "gold"]),
            key="lat",
        )

    st.markdown("**Mikropaczki: czas trwania (s)**")
    sm = r.derive("stream_ts", [ui.METRICS], q.stream_timeseries, None)
    sm = sm[sm["query_name"].isin(queries_sel)]
    if hours and not sm.empty:
        sm = sm[sm["progress_at"] >= sm["progress_at"].max() - timedelta(hours=hours)]
    if sm.empty:
        st.info("Brak metryk wybranych zapytań.")
    else:
        ui.chart(
            charts.lines(sm, "progress_at", "duration_s", "query_name", "s", order=queries_sel),
            key="sm",
        )

    left, right = st.columns(2)
    with left:
        st.markdown("**Zapytania strumieniowe (ostatnie 20 paczek)**")
        ui.table(r.derive("stream_summary", [ui.METRICS], q.stream_summary, 20))
        st.markdown("**Opóźnienie per warstwa (ostatnia godzina pomiarów)**")
        ui.table(lat)
    with right:
        st.markdown("**Kwarantanna per reguła**")
        if quarantine.empty:
            st.success("Brak wierszy w kwarantannie.")
        else:
            ui.table(quarantine)
        st.markdown("**Utrzymanie tabel (OPTIMIZE / VACUUM)**")
        ui.table(r.derive("maintenance", [ui.MAINTENANCE], q.maintenance_recent, 15))


body()
