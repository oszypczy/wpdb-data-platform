"""Lejek statusów: status transitions before -> after over time (07 §2)."""

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Lejek statusów")
GRAIN = {"Godzina": "hour", "Dzień": "day", "Tydzień": "week"}
f1, f2 = st.columns([1, 3])
grain = f1.radio("Ziarno", list(GRAIN), index=2, horizontal=True)
targets = f2.multiselect(
    "Przejścia do statusu",
    list(q.FUNNEL_PATH + q.FUNNEL_EXITS),
    default=["canceled"],
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
        st.markdown("**Ścieżka dostawy**")
        ui.chart(charts.funnel(stages[stages["kind"] == "path"]), key="funnel_path")
    with right:
        st.markdown(
            "**Statusy poza ścieżką dostawy**",
            help="Zamówienia, które zamiast pójść dalej ścieżką dostawy weszły w inny "
            "status: anulowane, niedostępne albo zatrzymane w obsłudze (processing, "
            "invoiced: w danych Olist to statusy końcowe zamówień, które nie zostały wysłane).",
        )
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
