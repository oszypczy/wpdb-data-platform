"""Confluent Schema Registry client (stdlib only, cached).

Spark resolves every writer schema by the id in the message header (05 §2, 03 §13:
after a DROP COLUMN the registry hands out the v1 id again, so `latest` is never used
for decoding) and the primary key from the key subject `<topic>-key` (05 §4).
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any


class SchemaRegistry:
    def __init__(self, url: str, timeout: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.timeout = timeout
        self._by_id: dict[int, str] = {}
        self._lock = threading.Lock()

    def _get(self, path: str) -> Any:
        req = urllib.request.Request(
            self.url + path, headers={"Accept": "application/vnd.schemaregistry.v1+json"}
        )
        last: Exception | None = None
        for _ in range(5):
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    return json.loads(resp.read().decode())
            except urllib.error.HTTPError:
                raise
            except OSError as exc:  # registry restarting: retry a few times
                last = exc
        raise RuntimeError(f"schema registry {self.url} unreachable: {last!r}")

    def schema(self, schema_id: int) -> str:
        """Writer schema (Avro JSON text) of an id; ids are immutable, so cached forever."""
        with self._lock:
            cached = self._by_id.get(schema_id)
        if cached is not None:
            return cached
        text = self._get(f"/schemas/ids/{schema_id}")["schema"]
        with self._lock:
            self._by_id[schema_id] = text
        return text

    def latest(self, subject: str) -> dict[str, Any]:
        return self._get(f"/subjects/{subject}/versions/latest")

    def key_fields(self, topic: str) -> list[str]:
        """Primary-key columns of a table = fields of its key schema (03 §4)."""
        schema = json.loads(self.latest(f"{topic}-key")["schema"])
        return [f["name"] for f in schema["fields"]]


def static_resolver(schemas: dict[int, str]) -> Callable[[int], str]:
    """Resolver for unit tests: id -> schema text from a dict."""

    def resolve(schema_id: int) -> str:
        return schemas[schema_id]

    return resolve
