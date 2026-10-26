"""Zdrowie potoku: container status, CDC freshness, end-to-end latency and quarantine
(07 §2).

Data comes from the ops Delta tables written by Spark; the dashboard reads no Kafka and
no Postgres (replication slot lag is checked by tests, 02 §5). Container status comes
from network probes of health endpoints (``dashboard.probes``); Postgres and the
simulator are not contacted, their state is inferred from the lake.
"""

from datetime import timedelta

import streamlit as st

from dashboard import charts, probes, ui
from dashboard import queries as q

st.title("Zdrowie potoku")
WINDOWS = {"1 h": 1, "6 h": 6, "24 h": 24, "wszystko": None}
window = st.radio("Okno (od ostatniego pomiaru)", list(WINDOWS), horizontal=True)

# thresholds of the status icons (e2e thresholds of the final stage: 05 §12)
CDC_WARN_S, CDC_CRIT_S = 30, 120
LAT = {"silver": (60, 120), "gold": (120, 240)}


def containers(fresh, last_business) -> None:
    """One cell per long-running container: probed directly, or inferred (Postgres from
    the CDC heartbeat, the simulator from the last business change in the lake)."""
    status = {
        r.service: ("good", "OK", r.detail) if r.ok else ("critical", "problem", r.detail)
        for r in probes.check_all()
    }
    age = fresh["freshness_s"]
    hb = (
        f"pośrednio: heartbeat CDC sprzed {ui.seconds(age)}"
        if age is not None
        else "pośrednio: brak heartbeatu CDC"
    )
    status["postgres"] = (
        ("good", "OK", hb) if age is not None and age < CDC_CRIT_S else ("critical", "problem", hb)
    )
    since = f"{ui.seconds(last_business)} temu" if last_business is not None else "–"
    status["simulator"] = ("info", "bez sondy", f"ostatnia zmiana biznesowa w potoku: {since}")
    status["consumer"] = (
        "info",
        "na żądanie",
        "narzędzie uruchamiane ręcznie: docker compose run --rm consumer",
    )
    status["dashboard"] = ("good", "OK", "ta strona")
    st.markdown(
        "**Kontenery**",
        help="Układ jak na diagramie architektury. Kafka, Schema Registry, Connect (konektor "
        "i zadanie RUNNING), MinIO i Spark: sondy sieciowe co odświeżenie. Postgres i "
        "symulator bez sondy (dashboard nie łączy się z bazą): Postgres wg świeżości CDC. "
        "Consumer to narzędzie uruchamiane na żądanie. "
        "Szczegóły po najechaniu na prostokąt.",
    )
    ui.chart(charts.architecture(status), key="containers")


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
    lat_ts = r.derive("latency_ts", [ui.LATENCY], q.latency_timeseries, None)
    last_business = None
    if not lat_ts.empty:
        last = lat_ts["measured_at"].max().to_pydatetime()
        last_business = (ui.now_utc().replace(tzinfo=None) - last).total_seconds()
    containers(fresh, last_business)  # also while gold/ops are still empty

    if fresh["heartbeats"] == 0 and lat.empty:
        ui.waiting()
        return
    if fresh["last_heartbeat"] is not None and fresh["heartbeats"]:
        st.caption(f"Ostatni heartbeat CDC: {fresh['last_heartbeat']} UTC.")

    hours = WINDOWS[window]
    e2e = r.get(ui.LATENCY)
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

    st.markdown("**Kwarantanna per reguła**")
    if quarantine.empty:
        st.success("Brak wierszy w kwarantannie.")
    else:
        ui.table(quarantine)


body()
