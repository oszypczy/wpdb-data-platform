"""Lejek statusów: status transitions before -> after over time (07 §2)."""

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Lejek statusów")
st.caption(
    "Przejścia statusów zamówień z par before → after strumienia CDC "
    "(gold/agg_status_funnel, okna 1 h czasu symulacji); (new) = utworzenie zamówienia."
)
GRAIN = {"Godzina": "hour", "Dzień": "day", "Tydzień": "week"}
f1, f2 = st.columns([1, 3])
grain = f1.radio("Ziarno", list(GRAIN), index=1, horizontal=True)
targets = f2.multiselect(
    "Przejścia do statusu",
    list(q.FUNNEL_PATH + q.FUNNEL_EXITS),
    default=["canceled", "unavailable"],
)


@ui.live
def body() -> None:
    r = ui.reader()
    stages = r.derive("funnel_stages", [ui.FUNNEL], q.funnel_stages)
    if stages["entered"].sum() == 0:
        ui.waiting()
        return
    left, right = st.columns(2)
    with left:
        st.markdown("**Główna ścieżka**")
        ui.chart(charts.funnel(stages[stages["kind"] == "path"]), key="funnel_path")
    with right:
        st.markdown("**Wyjścia ze ścieżki** (anulacje, niedostępne, …)")
        exits = stages[stages["kind"] == "exit"]
        ui.chart(charts.bars(exits, "stage", "entered", y_title="zamówienia"), key="exits")
    ts = r.derive(
        "funnel_ts", [ui.FUNNEL], q.funnel_timeseries, GRAIN[grain], tuple(sorted(targets))
    )
    st.markdown("**Przejścia w czasie**")
    if ts.empty:
        st.info("Brak przejść do wybranych statusów.")
    else:
        ui.chart(
            charts.bars(
                ts,
                "period",
                "transitions",
                color="transition",
                barmode="stack",
                y_title="przejścia",
            ),
            key="funnel_ts",
        )
    with st.expander("Wszystkie pary przejść"):
        ui.table(r.derive("funnel_pairs", [ui.FUNNEL], q.funnel_transitions))


body()
