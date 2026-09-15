"""CloudWatch custom metrics with a local JSONL sink."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3

from fin_dq_engine.config import Settings
from fin_dq_engine.logging_utils import get_logger

log = get_logger(__name__)


class MetricsSink:
    """Publish pipeline metrics to CloudWatch (aws) or ./out/metrics.jsonl (local)."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: Any = (
            boto3.client("cloudwatch", region_name=settings.aws.region) if not settings.is_local else None
        )
        self._buffer: list[dict[str, Any]] = []

    def put(self, name: str, value: float, unit: str = "Count", **dimensions: str) -> None:
        """Record a metric point; flushed in :meth:`flush`."""
        self._buffer.append(
            {
                "MetricName": name,
                "Value": float(value),
                "Unit": unit,
                "Timestamp": datetime.now(UTC),
                "Dimensions": [{"Name": k, "Value": v} for k, v in dimensions.items()],
            }
        )

    def flush(self) -> int:
        """Send buffered points. Returns the number flushed."""
        count = len(self._buffer)
        if not count:
            return 0
        if self._client is not None:
            # Drop each chunk only once it is published. Clearing the buffer up front means a failure
            # part way through the batches silently loses every point that had not been sent yet.
            while self._buffer:
                self._client.put_metric_data(
                    Namespace=self._settings.aws.metrics_namespace, MetricData=self._buffer[:20]
                )
                del self._buffer[:20]
        else:
            points, self._buffer = self._buffer, []
            out = Path(self._settings.paths.out_dir)
            out.mkdir(parents=True, exist_ok=True)
            with (out / "metrics.jsonl").open("a", encoding="utf-8") as fh:
                for p in points:
                    fh.write(json.dumps({**p, "Timestamp": p["Timestamp"].isoformat()}) + "\n")
        log.info("metrics_flushed", count=count)
        return count
