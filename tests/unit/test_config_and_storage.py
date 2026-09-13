from __future__ import annotations

import json
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from fin_dq_engine.config import Settings, load_settings
from fin_dq_engine.governance.secrets import resolve_database_url
from fin_dq_engine.metrics import MetricsSink
from fin_dq_engine.storage import LocalStorage, S3Storage, get_storage, read_csv, read_parquet, write_parquet

REPO = Path(__file__).resolve().parents[2]


def test_settings_yaml_and_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FIN_DQ__DATABASE__URL", "postgresql+psycopg://u:p@h:5/x")
    monkeypatch.setenv("FIN_DQ__PIPELINE__BATCH_FAILURE_THRESHOLD_PCT", "12.5")
    s = load_settings(REPO / "config" / "settings.yaml")
    assert s.database.url == "postgresql+psycopg://u:p@h:5/x"
    assert s.pipeline.batch_failure_threshold_pct == 12.5
    assert s.is_local and s.raw_dir == s.paths.data_dir / "raw"


def test_settings_missing_file_uses_defaults(tmp_path: Path) -> None:
    s = load_settings(tmp_path / "nope.yaml", actor="me")
    assert s.actor == "me" and s.environment == "local"


def test_settings_rejects_non_mapping(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text("- 1\n- 2\n")
    with pytest.raises(ValueError):
        load_settings(p)


def test_local_storage_roundtrip(tmp_path: Path) -> None:
    st = LocalStorage(tmp_path)
    assert not st.exists("raw", "a/b.csv")
    uri = st.write_bytes("raw", "a/b.csv", b"x,y\n1,2\n")
    assert uri.startswith("file://") and st.exists("raw", "a/b.csv")
    assert read_csv(st, "raw", "a/b.csv").iloc[0].tolist() == ["1", "2"]
    assert st.list_keys("raw", "a") == ["a/b.csv"] and st.list_keys("raw", "zzz") == []
    df = pd.DataFrame({"n": [1, 2]})
    write_parquet(st, "staged", "p/x.parquet", df)
    assert read_parquet(st, "staged", "p/x.parquet").equals(df)


@mock_aws
def test_s3_storage_roundtrip() -> None:
    s = Settings(environment="aws")
    client = boto3.client("s3", region_name="us-east-1")
    for b in (s.aws.raw_bucket, s.aws.staged_bucket, s.aws.curated_bucket):
        client.create_bucket(Bucket=b)
    st = S3Storage(s, client)
    assert not st.exists("raw", "k.csv")
    assert st.write_bytes("raw", "k.csv", b"a\n1\n") == f"s3://{s.aws.raw_bucket}/k.csv"
    assert st.exists("raw", "k.csv") and st.read_bytes("raw", "k.csv") == b"a\n1\n"
    st.write_bytes("raw", "dir/z.csv", b"")
    assert st.list_keys("raw", "dir/") == ["dir/z.csv"]
    assert isinstance(get_storage(s), S3Storage)
    assert isinstance(get_storage(Settings()), LocalStorage)


@mock_aws
def test_secrets_manager_database_url() -> None:
    s = Settings(environment="aws")
    boto3.client("secretsmanager", region_name="us-east-1").create_secret(
        Name=s.aws.secret_name,
        SecretString=json.dumps(
            {"username": "app", "password": "pw", "host": "db.internal", "port": 5432, "dbname": "fin_dq"}
        ),
    )
    assert resolve_database_url(s) == "postgresql+psycopg://app:pw@db.internal:5432/fin_dq"


def test_metrics_local_sink(tmp_path: Path) -> None:
    s = Settings(paths={"out_dir": str(tmp_path)})  # type: ignore[arg-type]
    m = MetricsSink(s)
    assert m.flush() == 0
    m.put("rows_processed", 10, stage="load")
    assert m.flush() == 1
    lines = (tmp_path / "metrics.jsonl").read_text().splitlines()
    assert json.loads(lines[0])["MetricName"] == "rows_processed"


@mock_aws
def test_metrics_cloudwatch_sink() -> None:
    s = Settings(environment="aws")
    m = MetricsSink(s)
    for i in range(25):
        m.put("x", i)
    assert m.flush() == 25
    stats = boto3.client("cloudwatch", region_name="us-east-1").list_metrics(Namespace=s.aws.metrics_namespace)
    assert stats["Metrics"]
