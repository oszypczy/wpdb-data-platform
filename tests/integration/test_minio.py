"""MinIO buckets, accounts and the from-source build (06-minio-delta.md §2, §11; 08 §13)."""

from __future__ import annotations

import re
import uuid
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

from tests.helpers.compose import ComposeStack

pytestmark = pytest.mark.integration

BUCKETS = ["bronze", "silver", "gold", "ops", "checkpoints"]
DOCKERFILE = Path(__file__).resolve().parents[2] / "minio" / "Dockerfile"


def pinned(arg: str) -> str:
    m = re.search(rf"^ARG {arg}=(\S+)$", DOCKERFILE.read_text(), re.M)
    assert m, f"ARG {arg} not found in minio/Dockerfile"
    return m.group(1)


def error_code(exc: ClientError) -> str:
    return exc.response["Error"]["Code"]


def test_buckets_exist(minio):
    """06 §2: buckets bronze, silver, gold, ops, checkpoints exist (and nothing else)."""
    names = sorted(b["Name"] for b in minio("root").list_buckets()["Buckets"])
    assert names == sorted(BUCKETS)


def test_spark_account_rw_all_buckets(minio):
    """06 §2: account spark can write, read and delete in every bucket."""
    s3 = minio("spark")
    key = f"_stage0_probe/{uuid.uuid4()}.txt"
    for bucket in BUCKETS:
        s3.put_object(Bucket=bucket, Key=key, Body=b"probe")
        assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == b"probe"
        listed = [
            o["Key"]
            for o in s3.list_objects_v2(Bucket=bucket, Prefix="_stage0_probe/").get("Contents", [])
        ]
        assert key in listed
        s3.delete_object(Bucket=bucket, Key=key)


def test_dashboard_account_restricted(minio):
    """06 §11: dashboard reads gold (and ops); writing gold and reading bronze are rejected."""
    spark, dash = minio("spark"), minio("dashboard")
    key = f"_stage0_probe/{uuid.uuid4()}.txt"
    spark.put_object(Bucket="gold", Key=key, Body=b"gold")
    spark.put_object(Bucket="bronze", Key=key, Body=b"bronze")
    try:
        assert dash.get_object(Bucket="gold", Key=key)["Body"].read() == b"gold"
        assert key in [
            o["Key"]
            for o in dash.list_objects_v2(Bucket="gold", Prefix="_stage0_probe/")["Contents"]
        ]
        dash.list_objects_v2(Bucket="ops")
        with pytest.raises(ClientError) as e:
            dash.put_object(Bucket="gold", Key=key + ".w", Body=b"x")
        assert error_code(e.value) == "AccessDenied"
        with pytest.raises(ClientError) as e:
            dash.delete_object(Bucket="gold", Key=key)
        assert error_code(e.value) == "AccessDenied"
        with pytest.raises(ClientError) as e:
            dash.get_object(Bucket="bronze", Key=key)
        assert error_code(e.value) == "AccessDenied"
        with pytest.raises(ClientError) as e:
            dash.list_objects_v2(Bucket="bronze")
        assert error_code(e.value) == "AccessDenied"
        with pytest.raises(ClientError) as e:
            dash.list_objects_v2(Bucket="silver")
        assert error_code(e.value) == "AccessDenied"
    finally:
        spark.delete_object(Bucket="gold", Key=key)
        spark.delete_object(Bucket="bronze", Key=key)


def test_server_version_matches_pinned_tag(stack: ComposeStack):
    """08 §13: the image was built from the pinned source tags (minio --version / mc --version)."""
    minio_tag, mc_tag = pinned("MINIO_TAG"), pinned("MC_TAG")
    out = stack.exec("minio", "minio", "--version").stdout
    assert f"minio version {minio_tag}" in out, out
    out = stack.exec("minio", "mc", "--version").stdout
    assert f"mc version {mc_tag}" in out, out


def test_api_bound_to_localhost_only(stack: ComposeStack):
    """08 §13 (CVE-2026-40344): ports 9000/9091 are published on 127.0.0.1 only."""
    out = stack.docker("port", "minio").stdout
    bindings = [line.split(" -> ")[1] for line in out.splitlines() if line.strip()]
    assert bindings, out
    assert all(b.startswith("127.0.0.1:") for b in bindings), out
    assert {b.split(":")[1] for b in bindings} == {"9000", "9091"}, out
