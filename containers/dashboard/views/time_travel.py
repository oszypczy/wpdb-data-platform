"""Podróż w czasie: a gold table now vs. at version N (delta-rs time travel) and its
history (DESCRIBE HISTORY), 07 §2."""

import streamlit as st

from dashboard import contract, ui
from dashboard import queries as q

st.title("Podróż w czasie")
st.caption(
    "Każdy commit Sparka to nowa wersja tabeli Delta. Starsze wersje są czytelne, dopóki "
    "VACUUM nie usunie ich plików (retencja gold 168 h, 06 §7). Sumy kolumn są surowe, "
    "jak w tabeli, więc kwoty są tu w BRL."
)
tables = list(contract.TIME_TRAVEL_MEASURES)
table = st.selectbox("Tabela", tables, format_func=lambda t: t.removeprefix("gold/"))
current = ui.reader().lake.version(table)
if current is None:
    ui.waiting()
    st.stop()
version = st.number_input(
    f"Wersja N (0 … {current})",
    min_value=0,
    max_value=current,
    value=current // 2,
    step=1,
)


@ui.live
def body() -> None:
    r = ui.reader()
    now_v = r.versions([table])[table]
    now = r.get(table, now_v)
    try:
        then = r.at_version(table, int(version))
    except Exception as exc:  # noqa: BLE001 - vacuumed or missing log: tell the user
        st.warning(f"Wersja {version} jest niedostępna ({type(exc).__name__}): {exc}")
        return
    st.markdown(f"**Wskaźniki: teraz (wersja {now_v}) vs wersja {version}**")
    ui.table(
        q.compare_versions(table, now, then, contract.TIME_TRAVEL_MEASURES[table], int(version))
    )
    st.markdown("**Historia operacji (ostatnie 30 commitów)**")
    ui.table(q.history_frame(r.lake.history(table, 30)))


body()
