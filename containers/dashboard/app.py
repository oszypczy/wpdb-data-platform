"""WPDB dashboard: entry point (``streamlit run app.py``), docs/design/07-dashboard.md.

Reads only the gold and ops Delta tables in MinIO with the read-only ``dashboard``
account; never talks to Spark, Kafka or Postgres. Pages are in ``views/``.
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="WPDB · Olist live", layout="wide")

PAGES = {
    "Biznes": [
        st.Page("views/overview.py", title="Przegląd", default=True),
        st.Page("views/sales.py", title="Sprzedaż w czasie"),
        st.Page("views/map.py", title="Mapa"),
        st.Page("views/funnel.py", title="Lejek statusów"),
        st.Page("views/logistics.py", title="Logistyka"),
        st.Page("views/products.py", title="Produkty"),
        st.Page("views/customers.py", title="Klienci"),
    ],
    "Platforma": [
        st.Page("views/pipeline.py", title="Zdrowie potoku"),
        st.Page("views/time_travel.py", title="Podróż w czasie"),
    ],
}

st.navigation(PAGES).run()
