"""WPDB dashboard: entry point (``streamlit run app.py``), docs/design/07-dashboard.md.

Reads only the gold and ops Delta tables in MinIO with the read-only ``dashboard``
account; never talks to Spark, Kafka or Postgres. Pages are in ``views/``.
"""

from __future__ import annotations

import streamlit as st

from dashboard import ui

st.set_page_config(page_title="WPDB · Olist live", page_icon="📦", layout="wide")

PAGES = {
    "Biznes": [
        st.Page("views/overview.py", title="Przegląd", icon="📊", default=True),
        st.Page("views/sales.py", title="Sprzedaż w czasie", icon="📈"),
        st.Page("views/map.py", title="Mapa", icon="🗺️"),
        st.Page("views/funnel.py", title="Lejek statusów", icon="🔻"),
        st.Page("views/logistics.py", title="Logistyka", icon="🚚"),
        st.Page("views/products.py", title="Produkty", icon="🏷️"),
        st.Page("views/customers.py", title="Klienci", icon="👥"),
    ],
    "Platforma": [
        st.Page("views/pipeline.py", title="Zdrowie potoku", icon="🩺"),
        st.Page("views/time_travel.py", title="Podróż w czasie", icon="⏪"),
    ],
}

page = st.navigation(PAGES)
with st.sidebar:
    st.caption(
        f"Źródło: Delta Lake w MinIO (gold, ops), konto tylko do odczytu. "
        f"Odświeżanie co {ui.REFRESH_S:g} s, dane czytane ponownie tylko po zmianie "
        "wersji tabeli."
    )
page.run()
