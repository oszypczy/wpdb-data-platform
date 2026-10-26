"""Rendering dashboard pages with ``streamlit.testing.v1.AppTest`` (07 §7)."""

from __future__ import annotations

from pathlib import Path

from streamlit.testing.v1 import AppTest

APP = Path(__file__).resolve().parents[2] / "containers" / "dashboard" / "app.py"
PAGES = [
    "overview",
    "sales",
    "map",
    "funnel",
    "logistics",
    "products",
    "customers",
    "pipeline",
]


def render(page: str, timeout: float = 120) -> AppTest:
    """Run the app, switch to ``views/<page>.py`` and run it once (fragments included)."""
    at = AppTest.from_file(str(APP), default_timeout=timeout)
    at.run()
    at.switch_page(f"views/{page}.py").run()
    return at


def problems(at: AppTest) -> list[str]:
    """Uncaught exceptions and the in-place errors of ``ui.live`` (lake read failures)."""
    return [e.value for e in at.exception] + [e.value for e in at.error]


def texts(at: AppTest) -> str:
    parts = [e.value for e in (*at.title, *at.subheader, *at.info, *at.warning, *at.caption)]
    parts += [m.label + " " + m.value for m in at.metric]
    return "\n".join(str(p) for p in parts)
