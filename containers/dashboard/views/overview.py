"""Przegląd: headline KPIs and the simulation clock (07 §2)."""

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Przegląd")


@ui.live
def body() -> None:
    r = ui.reader()
    k = r.derive("kpis", [ui.ITEMS, ui.LIFECYCLE, ui.REVENUE_HOURLY], q.kpis)
    if k.is_empty:
        ui.waiting()
        return
    st.subheader(f"Stan na {ui.sim_time(k.sim_clock)} (czas symulacji)")
    c = st.columns(5)
    c[0].metric(
        "Przychód netto",
        ui.money(k.revenue),
        help="Suma cen pozycji zamówień (fact_order_items); pozycje anulowanych "
        "zamówień są usunięte, więc korekty są już odjęte.",
    )
    c[1].metric("Zamówienia", ui.number(k.orders), help="Wiersze fact_orders_lifecycle.")
    c[2].metric(
        "Średni koszyk",
        ui.money(k.avg_basket),
        help="Przychód / zamówienia z co najmniej jedną pozycją.",
    )
    c[3].metric(
        "Spóźnienia",
        ui.pct(k.late_pct),
        help=f"{ui.number(k.late)} z {ui.number(k.delivered)} dostarczonych zamówień "
        "dotarło po szacowanym terminie.",
    )
    c[4].metric(
        "Mediana dostawy",
        f"{ui.number(k.median_delivery_days, 1)} dni",
        help="Mediana dni od zakupu do dostawy (zamówienia dostarczone).",
    )
    st.caption(
        ui.md(
            f"Sprzedaż brutto {ui.money(k.gross_sales)}, korekty po anulacjach "
            f"{ui.money(k.corrections)} (przychód netto = brutto + korekty), fracht "
            f"{ui.money(k.freight)}; {ui.number(k.items)} pozycji w "
            f"{ui.number(k.orders_with_items)} zamówieniach, spóźnione {ui.number(k.late)} "
            f"z {ui.number(k.delivered)} dostarczonych."
        )
    )
    daily = r.derive("revenue_ts", [ui.REVENUE_HOURLY], q.revenue_timeseries, "day", None)
    st.markdown("**Przychód dzienny**")
    ui.chart(charts.revenue_bars(daily), key="overview_daily")


body()
