"""Logistyka: delivery time distribution, % late per state, time to approval (07 §2)."""

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Logistyka")


@ui.live
def body() -> None:
    r = ui.reader()
    if r.get(ui.LIFECYCLE).num_rows == 0:
        ui.waiting()
        return
    s = r.derive("logistics", [ui.LIFECYCLE], q.logistics_summary).iloc[0]
    c = st.columns(5)
    c[0].metric("Dostarczone", ui.number(s["delivered"]))
    c[1].metric("Spóźnione", ui.pct(s["late_pct"]), help=f"{ui.number(s['late'])} zamówień.")
    c[2].metric("Mediana dostawy", f"{ui.number(s['median_d_to_deliver'], 1)} dni")
    c[3].metric("90. percentyl dostawy", f"{ui.number(s['p90_d_to_deliver'], 1)} dni")
    c[4].metric("Mediana akceptacji", f"{ui.number(s['median_h_to_approve'], 2)} h")

    dt = r.derive("delivery_times", [ui.LIFECYCLE], q.delivery_times)
    left, right = st.columns(2)
    with left:
        st.markdown("**Rozkład czasu dostawy**")
        if dt.empty:
            st.info("Brak dostarczonych zamówień.")
        else:
            d = dt.assign(termin=dt["is_late"].map({True: "spóźnione", False: "w terminie"}))
            ui.chart(
                charts.histogram(
                    d,
                    "d_to_deliver",
                    "termin",
                    "dni od zakupu",
                    order=["w terminie", "spóźnione"],
                ),
                key="dd",
            )
    with right:
        st.markdown("**% spóźnień per stan klienta**")
        late = r.derive("late_by_state", [ui.LIFECYCLE], q.late_by_state)
        if late.empty:
            st.info("Brak dostarczonych zamówień.")
        else:
            ui.chart(charts.bars(late, "state", "late_pct", y_title="% spóźnień"), key="late")
    st.markdown("**Czas do akceptacji płatności** (do 72 h)")
    ap = r.derive("approval_times", [ui.LIFECYCLE], q.approval_times)
    ap = ap[ap["h_to_approve"] <= 72]
    if ap.empty:
        st.info("Brak zaakceptowanych zamówień.")
    else:
        ui.chart(charts.histogram(ap, "h_to_approve", None, "godziny", nbins=72), key="ap")


body()
