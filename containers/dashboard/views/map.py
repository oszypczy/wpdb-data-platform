"""Mapa: choropleth of the 27 Brazilian states with a metric switch (07 §2, §4)."""

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Mapa")
METRICS = {"Przychód (R$)": "revenue", "Zamówienia": "orders", "% spóźnień": "late_pct"}
label = st.radio("Metryka", list(METRICS), horizontal=True)
st.caption(
    "Granice stanów: Natural Earth 1:50m admin-1 (domena publiczna), plik w repozytorium. "
    "Stan = stan klienta."
)


@ui.live
def body() -> None:
    r = ui.reader()
    df = r.derive("state_metrics", [ui.REVENUE_HOURLY, ui.LIFECYCLE, ui.GEOGRAPHY], q.state_metrics)
    if df.empty or (df["orders"].sum() == 0 and df["revenue"].sum() == 0):
        ui.waiting()
        return
    ui.chart(charts.choropleth(df, METRICS[label], label), key="map")
    with st.expander("Tabela stanów"):
        ui.table(df.sort_values(METRICS[label], ascending=False))


body()
