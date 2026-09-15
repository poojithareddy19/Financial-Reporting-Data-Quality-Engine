"""Object storage abstraction: local ./data folders in local mode, S3 buckets in AWS mode."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any, Literal, Protocol

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from fin_dq_engine.config import Settings

Layer = Literal["raw", "staged", "curated"]


class Storage(Protocol):
    """Minimal object-store interface used by every stage."""

    def exists(self, layer: Layer, key: str) -> bool: ...
    def read_bytes(self, layer: Layer, key: str) -> bytes: ...
    def write_bytes(self, layer: Layer, key: str, data: bytes) -> str: ...
    def list_keys(self, layer: Layer, prefix: str) -> list[str]: ...
    def uri(self, layer: Layer, key: str) -> str: ...


class LocalStorage:
    """Files under ``<data_dir>/<layer>/<key>``."""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)

    def _path(self, layer: Layer, key: str) -> Path:
        return self.data_dir / layer / key

    def exists(self, layer: Layer, key: str) -> bool:
        """Return True if the object exists."""
        return self._path(layer, key).exists()

    def read_bytes(self, layer: Layer, key: str) -> bytes:
        """Read an object fully into memory."""
        return self._path(layer, key).read_bytes()

    def write_bytes(self, layer: Layer, key: str, data: bytes) -> str:
        """Write an object, creating parent folders. Returns its URI."""
        p = self._path(layer, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        return self.uri(layer, key)

    def list_keys(self, layer: Layer, prefix: str) -> list[str]:
        """List keys beneath a prefix (recursive)."""
        base = self._path(layer, prefix)
        root = self.data_dir / layer
        if not base.exists():
            return []
        return sorted(p.relative_to(root).as_posix() for p in base.rglob("*") if p.is_file())

    def uri(self, layer: Layer, key: str) -> str:
        """file:// URI for the object."""
        return f"file://{self._path(layer, key).resolve()}"


class S3Storage:
    """One bucket per layer."""

    def __init__(self, settings: Settings, client: Any | None = None) -> None:
        self.buckets: dict[str, str] = {
            "raw": settings.aws.raw_bucket,
            "staged": settings.aws.staged_bucket,
            "curated": settings.aws.curated_bucket,
        }
        self.client = client or boto3.client("s3", region_name=settings.aws.region)

    def exists(self, layer: Layer, key: str) -> bool:
        """Return True if the object exists."""
        try:
            self.client.head_object(Bucket=self.buckets[layer], Key=key)
            return True
        except ClientError:
            return False

    def read_bytes(self, layer: Layer, key: str) -> bytes:
        """Read an object fully into memory."""
        body = self.client.get_object(Bucket=self.buckets[layer], Key=key)["Body"].read()
        return bytes(body)

    def write_bytes(self, layer: Layer, key: str, data: bytes) -> str:
        """Upload an object. Returns its s3:// URI."""
        self.client.put_object(Bucket=self.buckets[layer], Key=key, Body=data)
        return self.uri(layer, key)

    def list_keys(self, layer: Layer, prefix: str) -> list[str]:
        """List keys beneath a prefix."""
        paginator = self.client.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=self.buckets[layer], Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return sorted(keys)

    def uri(self, layer: Layer, key: str) -> str:
        """s3:// URI for the object."""
        return f"s3://{self.buckets[layer]}/{key}"


def get_storage(settings: Settings) -> Storage:
    """Pick the storage backend from settings."""
    if settings.is_local:
        return LocalStorage(settings.paths.data_dir)
    return S3Storage(settings)


def read_csv(storage: Storage, layer: Layer, key: str, **kwargs: Any) -> pd.DataFrame:
    """Read a CSV object as strings (raw layer lands untyped)."""
    return pd.read_csv(io.BytesIO(storage.read_bytes(layer, key)), dtype=str, keep_default_na=False, **kwargs)  # type: ignore[no-any-return]


def write_parquet(storage: Storage, layer: Layer, key: str, df: pd.DataFrame) -> str:
    """Write a DataFrame as Parquet."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    return storage.write_bytes(layer, key, buf.getvalue())


def read_parquet(storage: Storage, layer: Layer, key: str) -> pd.DataFrame:
    """Read a Parquet object."""
    return pd.read_parquet(io.BytesIO(storage.read_bytes(layer, key)))
