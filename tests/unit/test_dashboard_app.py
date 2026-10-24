"""Every page of the app renders on small local Delta tables and on an empty lake
(07 §3 "puste stany"), without Docker. The same pages on the live MinIO data are the
integration row of 07 §7 (tests/integration/test_dashboard.py)."""

from __future__ import annotations

import pytest

from tests.helpers import dashboard_fixtures as fx
from tests.helpers.dashboard_app import PAGES, problems, render, texts

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def small_lake(tmp_path_factory):
    return fx.write_small_lake(tmp_path_factory.mktemp("lake"))


@pytest.mark.parametrize("page", PAGES)
def test_page_renders_on_small_delta_tables(page, small_lake, monkeypatch):
    monkeypatch.setenv("DASHBOARD_LAKE_ROOT", str(small_lake))
    at = render(page)
    assert problems(at) == []
    assert "Czekam na dane" not in texts(at), page


def test_overview_shows_the_kpis_and_the_simulation_clock(small_lake, monkeypatch):
    monkeypatch.setenv("DASHBOARD_LAKE_ROOT", str(small_lake))
    at = render("overview")
    metrics = {m.label: m.value for m in at.metric}
    assert metrics == {
        "Przychód netto": "R$ 180,00",
        "Zamówienia": "4",
        "Średni koszyk": "R$ 90,00",
        "Spóźnienia": "50,0 %",
        "Mediana dostawy": "6,0 dni",
    }
    assert at.subheader[0].value == "Stan na 2017-01-10 12:00 (czas symulacji)"


def test_filters_are_outside_the_live_fragment(small_lake, monkeypatch):
    """07 §3: a refresh reruns only the data fragment, so the user's filters persist;
    changing a filter recomputes the series for the new parameters."""
    monkeypatch.setenv("DASHBOARD_LAKE_ROOT", str(small_lake))
    at = render("sales")
    at.multiselect[0].set_value(["RJ"]).run()
    assert problems(at) == []
    metrics = {m.label: m.value for m in at.metric}
    assert metrics["Przychód netto"] == "R$ 30,00"
    at.radio[0].set_value("Godzina").run()
    assert at.multiselect[0].value == ["RJ"]


@pytest.mark.parametrize("page", PAGES)
def test_page_waits_for_data_on_an_empty_lake(page, tmp_path, monkeypatch):
    """07 §3: right after start gold is empty: "czekam na dane" instead of an error."""
    monkeypatch.setenv("DASHBOARD_LAKE_ROOT", str(tmp_path))
    at = render(page)
    assert problems(at) == []
    assert "Czekam na dane" in texts(at), page
