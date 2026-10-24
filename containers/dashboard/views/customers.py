"""Klienci: returning customers and moves from the SCD2 dimension (07 §2)."""

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Klienci")
st.caption(
    "Osoba = customer_unique_id (customer_id w Olist jest per zamówienie). gold/dim_customer "
    "to SCD typu 2: nowa wersja przy zamówieniu z nowym adresem (prawdziwe przeprowadzki "
    "z danych Olist), bieżąca wersja ma valid_to = 9999-12-31."
)


@ui.live
def body() -> None:
    r = ui.reader()
    s = r.derive("customer_summary", [ui.CUSTOMERS, ui.LIFECYCLE], q.customer_summary).iloc[0]
    if s["customers"] == 0:
        ui.waiting()
        return
    c = st.columns(4)
    c[0].metric("Klienci", ui.number(s["customers"]))
    c[1].metric("Powracający (> 1 zamówienie)", ui.number(s["returning"]))
    c[2].metric("Przeprowadzki (> 1 wersja)", ui.number(s["movers"]))
    c[3].metric("Wersje w dim_customer", ui.number(s["versions"]))
    left, right = st.columns(2)
    with left:
        st.markdown("**Zamówienia na klienta**")
        opc = r.derive("orders_per_customer", [ui.LIFECYCLE], q.orders_per_customer)
        fig = charts.bars(opc.astype({"orders": str}), "orders", "customers", y_title="klienci")
        fig.update_yaxes(type="log")
        ui.chart(fig, key="opc")
    with right:
        st.markdown("**Historia wersji klientów, którzy się przeprowadzili**")
        moves = r.derive("customer_moves", [ui.CUSTOMERS], q.customer_moves)
        if moves.empty:
            st.info("Jeszcze nikt się nie przeprowadził w oknie symulacji.")
        else:
            ui.table(moves)


body()
