"""Produkty: top categories (English names) by revenue and freight (07 §2)."""

import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Produkty")
n = st.slider("Liczba kategorii", 5, 30, 15)


@ui.live
def body() -> None:
    r = ui.reader()
    top = r.derive("top_categories", [ui.ITEMS, ui.PRODUCTS], q.top_categories, n)
    if top.empty:
        ui.waiting()
        return
    long = top.melt(
        id_vars="category", value_vars=["revenue", "freight"], var_name="miara", value_name="R$"
    ).replace({"miara": {"revenue": "przychód", "freight": "fracht"}})
    ui.chart(
        charts.bars(
            long,
            "category",
            "R$",
            color="miara",
            horizontal=True,
            y_title="R$",
            height=max(360, 28 * len(top)),
            order=["przychód", "fracht"],
        ),
        key="top",
    )
    ui.table(top)


body()
