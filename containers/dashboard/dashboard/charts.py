"""One chart style for every page (Plotly), readable in Streamlit's light and dark themes.

- Colours by job: categorical slots in a fixed order (an entity keeps its colour, never
  re-coloured by rank), one sequential hue (blue) for magnitude on the map, the blue/red
  pair for sales vs. corrections (polarity), status colours only for health tiles.
  Slots are mid-tone steps validated for both surfaces (dataviz reference palette).
- Backgrounds are transparent and text colours come from the Streamlit theme
  (``st.plotly_chart(theme="streamlit")``), so the same figure works light and dark.
- Thin marks: 2 px lines, rounded bar ends, horizontal legend above the plot,
  one y-axis per chart (no dual axes), hover on every mark.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

SERIES = [
    "#2a78d6",  # 1 blue
    "#eb6834",  # 2 orange
    "#1baf7a",  # 3 aqua
    "#eda100",  # 4 yellow
    "#e87ba4",  # 5 magenta
    "#008300",  # 6 green
    "#6250d6",  # 7 violet
    "#e34948",  # 8 red
]
POSITIVE, NEGATIVE = SERIES[0], SERIES[7]  # sales (+) vs corrections (-)
SEQUENTIAL = [
    "#cde2fb",
    "#9ec5f4",
    "#6da7ec",
    "#3987e5",
    "#256abf",
    "#184f95",
    "#0d366b",
]
# ordinal steps for funnel stages: the lightest still clears 2:1 on both surfaces
ORDINAL = ["#86b6ef", "#5598e7", "#2a78d6", "#1c5cab"]
STATUS = {"good": "#0ca30c", "warning": "#fab219", "serious": "#ec835a", "critical": "#d03b3b"}

GEOJSON_PATH = Path(__file__).resolve().parent.parent / "assets" / "brazil-states.geojson"


@lru_cache(maxsize=1)
def brazil_states() -> dict[str, Any]:
    """The vendored GeoJSON of the 27 states (feature ``id`` = state code, e.g. "SP")."""
    return json.loads(GEOJSON_PATH.read_text(encoding="utf-8"))


def finish(fig: go.Figure, height: int = 360, hover: str = "closest") -> go.Figure:
    fig.update_layout(
        height=height,
        margin={"l": 8, "r": 8, "t": 36, "b": 8},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0, "title": None},
        hovermode=hover,
        barcornerradius=4,
        bargap=0.15,
    )
    fig.update_traces(selector={"type": "scatter"}, line={"width": 2}, marker={"size": 8})
    fig.update_xaxes(showgrid=False, title=None)
    fig.update_yaxes(gridwidth=1, zeroline=True, zerolinewidth=1)
    return fig


def revenue_bars(df: pd.DataFrame) -> go.Figure:
    """Sales above zero, corrections below zero, net revenue as a line: one money axis."""
    fig = go.Figure()
    fig.add_bar(x=df["period"], y=df["sales"], name="sprzedaż", marker_color=POSITIVE)
    fig.add_bar(
        x=df["period"], y=df["corrections"], name="korekty (anulacje)", marker_color=NEGATIVE
    )
    fig.add_scatter(
        x=df["period"],
        y=df["revenue"],
        name="przychód netto",
        mode="lines",
        line={"color": SERIES[2]},
    )
    fig.update_layout(barmode="relative", yaxis_title="USD")
    return finish(fig, hover="x unified")


def _order(df: pd.DataFrame, color: str, order: list[str] | None) -> dict[str, list[str]]:
    """Fixed categorical order: an entity keeps its slot whatever the ranking or filter."""
    return {color: order if order is not None else sorted(df[color].dropna().unique())}


def lines(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str | None,
    y_title: str,
    height: int = 360,
    order: list[str] | None = None,
) -> go.Figure:
    fig = px.line(
        df,
        x=x,
        y=y,
        color=color,
        color_discrete_sequence=SERIES,
        category_orders=_order(df, color, order) if color else None,
        render_mode="svg",  # no WebGL dependency in the browser
    )
    fig.update_yaxes(title=y_title)
    return finish(fig, height=height, hover="x unified")


def bars(
    df: pd.DataFrame,
    x: str,
    y: str,
    color: str | None = None,
    horizontal: bool = False,
    y_title: str | None = None,
    height: int = 360,
    barmode: str = "group",
    order: list[str] | None = None,
) -> go.Figure:
    kwargs: dict[str, Any] = {"color_discrete_sequence": SERIES, "barmode": barmode}
    if color:
        kwargs["color"] = color
        kwargs["category_orders"] = _order(df, color, order)
    if horizontal:
        fig = px.bar(df, x=y, y=x, orientation="h", **kwargs)
        fig.update_yaxes(autorange="reversed", title=None)
        fig.update_xaxes(title=y_title)
    else:
        fig = px.bar(df, x=x, y=y, **kwargs)
        fig.update_xaxes(type="category")
        fig.update_yaxes(title=y_title)
    return finish(fig, height=height)


def histogram(
    df: pd.DataFrame,
    x: str,
    color: str | None,
    x_title: str,
    nbins: int = 40,
    order: list[str] | None = None,
) -> go.Figure:
    fig = px.histogram(
        df,
        x=x,
        color=color,
        nbins=nbins,
        barmode="stack",
        color_discrete_sequence=SERIES,
        category_orders=_order(df, color, order) if color else None,
    )
    fig.update_xaxes(title=x_title)
    fig.update_yaxes(title="zamówienia")
    return finish(fig)


def funnel(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure(
        go.Funnel(
            y=df["stage"],
            x=df["entered"],
            textinfo="value+percent initial",
            # no "percent total": a share of the sum of all stages means nothing here
            hovertemplate="%{y}: %{x}<br>%{percentInitial:.1%} utworzonych"
            "<br>%{percentPrevious:.1%} poprzedniego etapu<extra></extra>",
            marker={"color": ORDINAL[: len(df)]},
            connector={"line": {"width": 0}},
        )
    )
    return finish(fig, height=320)


def choropleth(df: pd.DataFrame, metric: str, label: str) -> go.Figure:
    fig = px.choropleth(
        df,
        geojson=brazil_states(),
        locations="state",
        featureidkey="id",
        color=metric,
        color_continuous_scale=SEQUENTIAL,
        hover_name="state_name",
        hover_data={
            "state": True,
            "revenue": ":,.2f",
            "orders": True,
            "late_pct": ":.1f",
            metric: False,
        },
        labels={
            "revenue": "przychód (USD)",
            "orders": "zamówienia",
            "late_pct": "% spóźnień",
            "state": "stan",
            metric: label,
        },
    )
    fig.update_geos(fitbounds="locations", visible=False, bgcolor="rgba(0,0,0,0)")
    fig.update_traces(marker_line_width=0.5, marker_line_color="rgba(128,128,128,0.6)")
    fig.update_layout(coloraxis_colorbar={"title": label})
    return finish(fig, height=560)


# ------------------------------------------------------------------ container map
# Boxes at the centres of docs/design/architecture.drawio (same layout as the C4
# container diagram), y grows downwards like in draw.io.
ARCH_BOX = (240, 110)
ARCH_NODES = {
    "simulator": ("Business Simulator", 420, 180),
    "postgres": ("PostgreSQL", 780, 180),
    "connect": ("Debezium (Connect)", 780, 380),
    "schema-registry": ("Schema Registry", 1140, 380),
    "dashboard": ("Dashboard", 420, 575),
    "kafka": ("Kafka", 780, 575),
    "consumer": ("Consumer (narzędzie)", 1140, 575),
    "minio": ("MinIO + Delta", 420, 780),
    "spark": ("Spark", 780, 780),
}
# (from, to, bend points between the two boxes; none = straight arrow)
ARCH_EDGES = [
    ("simulator", "postgres", ()),
    ("postgres", "connect", ()),
    ("connect", "kafka", ()),
    ("connect", "schema-registry", ()),
    ("kafka", "consumer", ()),
    ("schema-registry", "consumer", ()),
    ("kafka", "spark", ()),
    ("schema-registry", "spark", ((1310, 380), (1310, 780))),
    ("spark", "minio", ()),
    ("minio", "dashboard", ()),
]
STATUS_NEUTRAL = "#8c8c8c"  # no probe: not good, not bad


def _border(frm: tuple[float, float], to: tuple[float, float]) -> tuple[float, float]:
    """Where the segment from the centre ``frm`` towards ``to`` leaves the box."""
    (x0, y0), (x1, y1) = frm, to
    dx, dy = x1 - x0, y1 - y0
    hw, hh = ARCH_BOX[0] / 2 + 6, ARCH_BOX[1] / 2 + 6
    t = min(hw / abs(dx) if dx else float("inf"), hh / abs(dy) if dy else float("inf"))
    return x0 + dx * t, y0 + dy * t


def architecture(status: dict[str, tuple[str, str, str]]) -> go.Figure:
    """The containers laid out as on the architecture diagram, each box coloured by its
    status. ``status``: service -> (status key, label, detail shown on hover)."""
    fig = go.Figure()
    for a, b, bends in ARCH_EDGES:
        pa, pb = ARCH_NODES[a][1:], ARCH_NODES[b][1:]
        path = [pa, *bends, pb]
        (x0, y0), (x1, y1) = _border(pa, path[1]), _border(pb, path[-2])
        if bends:  # draw up to the last bend, the arrow does the last segment
            pts = [(x0, y0), *bends]
            fig.add_scatter(
                x=[p[0] for p in pts], y=[p[1] for p in pts], mode="lines",
                line={"color": "rgba(128,128,128,0.8)", "width": 1.5},
                hoverinfo="skip", showlegend=False,
            )  # fmt: skip
            x0, y0 = bends[-1]
        fig.add_annotation(
            x=x1, y=y1, ax=x0, ay=y0, xref="x", yref="y", axref="x", ayref="y",
            text="", showarrow=True, arrowhead=2, arrowsize=1.2, arrowwidth=1.5,
            arrowcolor="rgba(128,128,128,0.8)",
        )  # fmt: skip
    w, h = ARCH_BOX
    xs, ys, hover = [], [], []
    for service, (title, x, y) in ARCH_NODES.items():
        key, label, detail = status.get(service, ("info", "brak danych", ""))
        color = STATUS.get(key, STATUS_NEUTRAL)
        fig.add_shape(
            type="rect", x0=x - w / 2, x1=x + w / 2, y0=y - h / 2, y1=y + h / 2,
            line={"color": color, "width": 2}, fillcolor=color, opacity=0.22, layer="below",
        )  # fmt: skip
        fig.add_shape(
            type="rect", x0=x - w / 2, x1=x + w / 2, y0=y - h / 2, y1=y + h / 2,
            line={"color": color, "width": 2}, fillcolor="rgba(0,0,0,0)",
        )  # fmt: skip
        fig.add_annotation(
            x=x, y=y, text=f"<b>{title}</b><br><span style='color:{color}'>● {label}</span>",
            showarrow=False, font={"size": 13},
        )  # fmt: skip
        xs.append(x)
        ys.append(y)
        hover.append(f"<b>{title}</b> ({service})<br>{label}<br>{detail}")
    # invisible markers carry the hover text of each box
    fig.add_scatter(
        x=xs, y=ys, mode="markers", marker={"opacity": 0}, name="boxes",
        hovertext=hover, hoverinfo="text", showlegend=False,
    )  # fmt: skip
    fig.update_xaxes(visible=False, range=[280, 1340], fixedrange=True)
    fig.update_yaxes(visible=False, range=[850, 110], fixedrange=True)
    fig = finish(fig, height=560)
    fig.update_traces(selector={"name": "boxes"}, marker={"size": 70})  # after finish()
    fig.update_layout(margin={"l": 0, "r": 0, "t": 0, "b": 0}, dragmode=False)
    return fig
