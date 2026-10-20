"""Client factories for the host side of the stack (ports from the stack-ops skill)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import boto3
import psycopg
import requests
from botocore.config import Config
from confluent_kafka.admin import AdminClient

PG_HOST, PG_PORT, PG_DB = "localhost", 5432, "shop"
KAFKA_BOOTSTRAP_HOST = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
CONNECT_URL = "http://localhost:8083"
MINIO_URL = "http://localhost:9000"


def pg_connect(user: str, password: str, **kw: Any) -> psycopg.Connection:
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=user,
        password=password,
        connect_timeout=10,
        **kw,
    )


def kafka_admin(bootstrap: str = KAFKA_BOOTSTRAP_HOST) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000})


def minio_client(access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=MINIO_URL,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            s3={"addressing_style": "path"},
            retries={"max_attempts": 2},
            connect_timeout=5,
            read_timeout=15,
        ),
    )


@dataclass
class Http:
    """Minimal REST helper for Schema Registry and Kafka Connect."""

    base: str

    def get(self, path: str, **kw: Any) -> requests.Response:
        return requests.get(self.base + path, timeout=15, **kw)

    def json(self, path: str, **kw: Any) -> Any:
        r = self.get(path, **kw)
        r.raise_for_status()
        return r.json()

    def put(self, path: str, **kw: Any) -> requests.Response:
        return requests.put(self.base + path, timeout=15, **kw)

    def post(self, path: str, **kw: Any) -> requests.Response:
        return requests.post(self.base + path, timeout=15, **kw)

    def delete(self, path: str, **kw: Any) -> requests.Response:
        return requests.delete(self.base + path, timeout=15, **kw)
