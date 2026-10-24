"""Unit-level fixtures: one local SparkSession per test session (05 §10 "Delta lokalnie").

Only tests that ask for ``spark`` start the JVM; the other unit tests stay JVM-free.
"""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from tests.helpers.spark import session

    s = session(warehouse=str(tmp_path_factory.mktemp("warehouse")))
    yield s


@pytest.fixture
def lake_dir(tmp_path):
    """A local lake root with the layer directories (file:// URIs)."""
    for layer in ("bronze", "silver", "gold", "ops", "checkpoints"):
        (tmp_path / layer).mkdir()
    return tmp_path
