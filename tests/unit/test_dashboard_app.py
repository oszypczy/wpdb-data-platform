"""Every page of the app renders on small local Delta tables and on an empty lake
(07 §3 "puste stany"), without Docker. The same pages on the live MinIO data are the
integration row of 07 §7 (tests/integration/test_dashboard.py)."""

from __future__ import annotations

import shutil

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
    """Amounts are shown in USD at the default rate 0.3135 (lake: R$ 180 and R$ 90)."""
    monkeypatch.setenv("DASHBOARD_LAKE_ROOT", str(small_lake))
    at = render("overview")
    metrics = {m.label: m.value for m in at.metric}
    assert metrics == {
        "Przychód netto": "$ 56",
        "Zamówienia": "4",
        "Średni koszyk": "$ 28,22",
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
    assert metrics["Przychód netto"] == "$ 9,41"
    at.radio[0].set_value("Godzina").run()
    assert at.multiselect[0].value == ["RJ"]


@pytest.mark.parametrize("page", PAGES)
def test_page_waits_for_data_on_an_empty_lake(page, tmp_path, monkeypatch):
    """07 §3: right after start gold is empty: "czekam na dane" instead of an error."""
    monkeypatch.setenv("DASHBOARD_LAKE_ROOT", str(tmp_path))
    at = render(page)
    assert problems(at) == []
    assert "Czekam na dane" in texts(at), page


def test_payments_reviews_shows_the_kpis_and_the_delivery_comparison(small_lake, monkeypatch):
    """07 §2 "Płatności i opinie" on the fixtures: KPI tiles, the on time vs late
    averages (late lower, red delta), and the state filter recomputes the tiles."""
    monkeypatch.setenv("DASHBOARD_LAKE_ROOT", str(small_lake))
    at = render("payments_reviews")
    assert problems(at) == []
    metrics = {m.label: m.value for m in at.metric}
    assert metrics == {
        "Średnia ocena": "3,00",
        "Oceny 5": "25,0 %",
        "Opinie z komentarzem": "50,0 %",
        "Średnia liczba rat": "2,0",
        "Karta kredytowa": "55,0 %",
        "Średnia ocena: w terminie": "4,50",
        "Średnia ocena: po terminie": "2,00",
    }
    late = next(m for m in at.metric if m.label == "Średnia ocena: po terminie")
    assert late.delta == "-2,50 vs w terminie"
    assert len(at.get("plotly_chart")) == 6
    at.multiselect[0].set_value(["RJ"]).run()
    assert problems(at) == []
    metrics = {m.label: m.value for m in at.metric}
    assert (metrics["Średnia ocena"], metrics["Karta kredytowa"]) == ("2,00", "0,0 %")
    assert metrics["Średnia liczba rat"] == "–"  # no credit card payment in RJ


MINIO_ERROR = "Nie udało się odczytać danych z MinIO: "


@pytest.mark.parametrize("page", PAGES)
def test_page_shows_a_lake_error_in_place_and_recovers(page, small_lake, tmp_path, monkeypatch):
    """07 §3 without Docker: every lake read fails (like a stopped MinIO, before the
    reader cached anything) -> no uncaught exception, only the in-place read error, the
    filters (incl. the state filter built from gold/dim_geography) still render. Once
    reads work again the next run shows the data and the state filter gets its options."""
    from dashboard.lake import Lake

    lake = tmp_path / "lake"  # fresh root = fresh per-process reader (no cached data)
    shutil.copytree(small_lake, lake)
    monkeypatch.setenv("DASHBOARD_LAKE_ROOT", str(lake))
    real_version = Lake.version

    def down(self, table):
        raise OSError(f"Generic S3 error: GET {table}/_delta_log: dns error")

    monkeypatch.setattr(Lake, "version", down)
    at = render(page)
    assert [e.value for e in at.exception] == [], page
    errors = [e.value for e in at.error]
    assert errors and all(e.startswith(MINIO_ERROR) for e in errors), errors
    if page in ("sales", "payments_reviews"):
        assert at.multiselect[0].options == []

    monkeypatch.setattr(Lake, "version", real_version)
    at.run()
    assert problems(at) == [], page
    assert "Czekam na dane" not in texts(at), page
    if page in ("sales", "payments_reviews"):
        assert at.multiselect[0].options == ["AC", "MG", "RJ", "SP"]
