---
name: feedback-dashboard-style
description: User's UI preferences for the Streamlit dashboard pages (no emoji, no caption paragraphs, USD display, Polish labels, compact pages)
metadata:
  type: feedback
---

Dashboard pages must follow the user's review preferences (learned 2026-10-25..27):
- No emoji anywhere; no explanatory `st.caption` paragraphs under titles or charts (user removed them all). Short `help=` tooltips on metrics are fine.
- Lake is BRL; queries return BRL; convert only in the view with `ui.usd` / `ui.usd_fields` / `ui.usd_frame`, format with `ui.money`, tables via `ui.money_columns`.
- Polish labels, including mapped category names (e.g. payment types -> Polish).
- Compact: KPI tiles + a few meaningful charts; at most one optional expander with a table, no technical tables.
- Colours only from `dashboard/charts.py` (fixed categorical slots, ordinal blues `ORDINAL`/`SCORE`, blue/red pair only for good/bad).

**Why:** the user pruned the existing pages this way; repeating it avoids another review round.
**How to apply:** every new or changed page under `containers/dashboard/views/`.
