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
    top = ui.usd_frame(top, "revenue", "freight")
    long = top.melt(
        id_vars="category", value_vars=["revenue", "freight"], var_name="miara", value_name="USD"
    ).replace({"miara": {"revenue": "przychód", "freight": "fracht"}})
    ui.chart(
        charts.bars(
            long,
            "category",
            "USD",
            color="miara",
            horizontal=True,
            y_title="USD",
            height=max(360, 28 * len(top)),
            order=["przychód", "fracht"],
        ),
        key="top",
    )
    ui.table(top, column_config=ui.money_columns("revenue", "freight"))


body()
