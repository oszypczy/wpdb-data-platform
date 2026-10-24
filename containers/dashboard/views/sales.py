"""Sprzedaż w czasie: revenue per hour/day/week with negative corrections (07 §2)."""

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Sprzedaż w czasie")
st.caption(
    "Okna 1 h czasu symulacji (gold/agg_revenue_hourly, watermark 24 h). Korekty to ujemne "
    "kwoty po usunięciu pozycji anulowanego zamówienia. W oknie prezentacyjnym "
    "(2017-11-20 … 2017-12-10) widać szczyt Black Friday (24.11)."
)

GRAIN = {"Godzina": "hour", "Dzień": "day", "Tydzień": "week"}
f1, f2 = st.columns([1, 3])
grain = f1.radio("Ziarno", list(GRAIN), index=1, horizontal=True)
states = f2.multiselect(
    "Stany klienta (puste = wszystkie)",
    sorted(ui.reader().get(ui.GEOGRAPHY).column("state").to_pylist()),
)


@ui.live
def body() -> None:
    r = ui.reader()
    sel = tuple(sorted(states)) or None
    ts = r.derive("revenue_ts", [ui.REVENUE_HOURLY], q.revenue_timeseries, GRAIN[grain], sel)
    if ts.empty:
        ui.waiting()
        return
    peak = ts.loc[ts["sales"].idxmax()]
    c = st.columns(4)
    c[0].metric("Sprzedaż", ui.money(ts["sales"].sum()))
    c[1].metric("Korekty", ui.money(ts["corrections"].sum()))
    c[2].metric("Przychód netto", ui.money(ts["revenue"].sum()))
    c[3].metric(f"Szczyt ({grain.lower()})", ui.money(peak["sales"]))
    st.caption(f"Szczyt sprzedaży: {peak['period']} (czas symulacji).")
    ui.chart(charts.revenue_bars(ts), key="sales_ts")
    with st.expander("Tabela"):
        ui.table(ts)


body()
