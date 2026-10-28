"""Płatności i opinie: payment types and installments, review scores, and the score of
on-time vs late deliveries (07 §2); amounts in USD."""

import pandas as pd
import streamlit as st

from dashboard import charts, ui
from dashboard import queries as q

st.title("Płatności i opinie")

# Polish names in a fixed order = fixed categorical colour slots
PAYMENT_TYPES = {
    "credit_card": "karta kredytowa",
    "boleto": "boleto",
    "voucher": "voucher",
    "debit_card": "karta debetowa",
    "not_defined": "nieokreślony",
}
ON_TIME = {True: "w terminie", False: "po terminie"}
SCORES = [f"ocena {s}" for s in q.REVIEW_SCORES]

states = ui.state_filter()


def _types(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    df = ui.usd_frame(df, "value").assign(
        typ=df["payment_type"].map(lambda t: PAYMENT_TYPES.get(t, t))
    )
    known = list(PAYMENT_TYPES.values())
    return df, known + sorted(set(df["typ"]) - set(known))


def _type_chart(df: pd.DataFrame, order: list[str], metric: str, title: str, key: str) -> None:
    fig = charts.bars(
        df, "typ", metric, color="typ", horizontal=True, y_title=title, height=240,
        order=order,
        hover={"payments": True, "value": ":,.2f", "payments_pct": ":.1f", "value_pct": ":.1f"},
        labels={"typ": "typ", "payments": "płatności", "value": "wartość (USD)",
                "payments_pct": "% płatności", "value_pct": "% wartości"},
    )  # fmt: skip
    fig.update_layout(showlegend=False)
    ui.chart(fig, key=key)


@ui.live
def body() -> None:
    ui.refresh_state_filter()
    r = ui.reader()
    sel = tuple(sorted(states)) or None
    tables = [ui.PAYMENTS, ui.REVIEWS]
    k = r.derive("payments_reviews_kpis", tables, q.payments_reviews_kpis, sel)
    if k.is_empty:
        ui.waiting()
        return
    c = st.columns(5)
    c[0].metric(
        "Średnia ocena",
        ui.number(k.avg_score, 2),
        help=f"Skala 1–5, {ui.number(k.reviews)} opinii.",
    )
    c[1].metric("Oceny 5", ui.pct(k.five_star_pct), help="Udział opinii z oceną 5.")
    c[2].metric("Opinie z komentarzem", ui.pct(k.comment_pct), help="Opinie z tytułem lub treścią.")
    c[3].metric(
        "Średnia liczba rat",
        ui.number(k.avg_credit_card_installments, 1),
        help=f"Płatności kartą kredytową: {ui.number(k.credit_card_payments)}.",
    )
    c[4].metric(
        "Karta kredytowa",
        ui.pct(k.credit_card_value_pct),
        help=ui.md(
            f"Udział w wartości płatności: {ui.money(ui.usd(k.credit_card_value))} "
            f"z {ui.money(ui.usd(k.payment_value))}."
        ),
    )

    st.subheader("Płatności")
    if k.payments == 0:
        st.info("Brak płatności.")
    else:
        types, order = _types(r.derive("payment_types", [ui.PAYMENTS], q.payment_types, sel))
        inst = r.derive("payment_installments", [ui.PAYMENTS], q.payment_installments, sel)
        inst = ui.usd_frame(inst, "value").astype({"payment_installments": str})
        left, mid, right = st.columns([1, 1, 2])
        with left:
            st.markdown("**Typy płatności: liczba**")
            _type_chart(types, order, "payments", "płatności", "types_n")
        with mid:
            st.markdown("**Typy płatności: wartość**")
            _type_chart(types, order, "value", "USD", "types_value")
        with right:
            st.markdown("**Liczba rat**")
            ui.chart(
                charts.bars(
                    inst, "payment_installments", "payments", y_title="płatności", height=240,
                    hover={"value": ":,.2f"},
                    labels={"payment_installments": "raty", "payments": "płatności",
                            "value": "wartość (USD)"},
                ),
                key="installments",
            )  # fmt: skip

    st.subheader("Opinie")
    if k.reviews == 0:
        st.info("Brak opinii.")
        return
    left, right = st.columns([1, 2])
    with left:
        st.markdown("**Rozkład ocen**")
        dist = r.derive("score_distribution", [ui.REVIEWS], q.score_distribution, sel)
        dist = dist.assign(ocena=dist["review_score"].map(lambda s: f"ocena {s}"))
        fig = charts.bars(
            dist, "ocena", "reviews", color="ocena", y_title="opinie", height=300,
            order=SCORES, colors=charts.SCORE,
            hover={"ocena": False, "share_pct": ":.1f", "orders": True},
            labels={"reviews": "opinie", "share_pct": "% opinii", "orders": "zamówienia"},
        )  # fmt: skip
        fig.update_layout(showlegend=False)
        ui.chart(fig, key="scores")
    with right:
        st.markdown("**Średnia ocena tygodniowo**")
        weekly = r.derive("weekly_scores", [ui.REVIEWS], q.weekly_scores, sel)
        fig = charts.lines(
            weekly, "week", "avg_score", None, "średnia ocena", height=300,
            hover={"reviews": True, "avg_score": ":.2f"},
            labels={"week": "tydzień", "avg_score": "średnia ocena", "reviews": "opinie"},
        )  # fmt: skip
        fig.update_yaxes(range=[0.8, 5.2])
        ui.chart(fig, key="weekly")

    st.markdown("**Ocena a terminowość dostawy (zamówienia dostarczone)**")
    by = r.derive("reviews_by_delivery", [ui.REVIEWS], q.reviews_by_delivery, sel)
    if by.empty:
        st.info("Brak opinii o dostarczonych zamówieniach.")
        return
    avg = dict(zip(by["on_time"], by["avg_score"], strict=True))
    n = dict(zip(by["on_time"], by["reviews"], strict=True))
    left, right = st.columns([1, 3])
    with left:
        for on_time in (True, False):
            delta = None
            if not on_time and True in avg and False in avg:
                d = avg[False] - avg[True]
                delta = f"{'-' if d < 0 else '+'}{ui.number(abs(d), 2)} vs w terminie"
            st.metric(
                f"Średnia ocena: {ON_TIME[on_time]}",
                ui.number(avg.get(on_time), 2),
                delta=delta,
                help=f"{ui.number(n.get(on_time, 0))} opinii.",
            )
    with right:
        shares = r.derive("score_shares", [ui.REVIEWS], q.score_shares_by_delivery, sel)
        shares = shares.assign(
            dostawa=shares["on_time"].map(lambda t: f"{ON_TIME[t]} ({ui.number(n[t])})"),
            ocena=shares["review_score"].map(lambda s: f"ocena {s}"),
        )
        fig = charts.bars(
            shares, "dostawa", "share_pct", color="ocena", horizontal=True,
            y_title="% opinii w grupie", height=240, barmode="stack", order=SCORES,
            colors=charts.SCORE, hover={"share_pct": ":.1f", "reviews": True},
            labels={"share_pct": "% opinii", "reviews": "opinie", "dostawa": "dostawa"},
        )  # fmt: skip
        fig.update_xaxes(range=[0, 100])
        ui.chart(fig, key="by_delivery")


body()
