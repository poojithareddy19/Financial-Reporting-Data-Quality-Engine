"""AWS Lambda entry points: one per stage plus a dispatcher used by the Step Functions state machine.

Each handler receives ``{"run_date": "YYYY-MM-DD", "batch_id": "...", "force": false}`` and returns the merged
event so Step Functions can pass state between stages. The EventBridge schedule sends an empty event, in
which case run_date defaults to yesterday (UTC).
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, date, datetime, timedelta
from typing import Any

from fin_dq_engine.config import Settings, load_settings
from fin_dq_engine.logging_utils import configure_logging, get_logger

log = get_logger(__name__)
_settings: Settings | None = None
_engine: Any = None


def _ctx() -> tuple[Settings, Any]:
    """Lazily create settings and engine once per container."""
    global _settings, _engine
    if _settings is None:
        configure_logging()
        _settings = load_settings(environment="aws")
    if _engine is None:
        from fin_dq_engine.db import apply_migrations, get_engine

        _engine = get_engine(_settings)
        apply_migrations(_engine, _settings.paths.sql_dir / "ddl")
    return _settings, _engine


def _run_date(event: dict[str, Any]) -> date:
    raw = event.get("run_date")
    return date.fromisoformat(raw) if raw else (datetime.now(UTC) - timedelta(days=1)).date()


def _serialize(obj: Any) -> Any:
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _serialize(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list | tuple | set):
        return [_serialize(v) for v in obj]
    if isinstance(obj, date | datetime):
        return obj.isoformat()
    if hasattr(obj, "__fspath__"):
        return str(obj)
    return obj if isinstance(obj, str | int | float | bool | type(None)) else str(obj)


def stage_handler(stage: str, event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Run one stage and merge its result into the event."""
    from fin_dq_engine.orchestration.runner import run_stage

    settings, engine = _ctx()
    run_date = _run_date(event)
    out = run_stage(settings, engine, stage, run_date, event.get("batch_id"), force=event.get("force", False))
    result: dict[str, Any] = {**event, "run_date": run_date.isoformat(), "stage": stage}
    if hasattr(out, "batch_id"):
        result["batch_id"] = out.batch_id
    if hasattr(out, "scorecard"):
        result["scorecard"] = _serialize(out.scorecard)
        result["anomalies"] = len(out.anomalies)
    else:
        result[f"{stage}_result"] = _serialize(out)
    log.info("lambda_stage_finished", stage=stage, run_date=run_date.isoformat(), batch_id=result.get("batch_id"))
    return result


def ingest_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Stage 1."""
    return stage_handler("ingest", event, context)


def transform_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Stage 2."""
    return stage_handler("transform", event, context)


def validate_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Stage 3. Raises BatchAbortedError so Step Functions routes to the failure topic."""
    return stage_handler("validate", event, context)


def load_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Stage 4."""
    return stage_handler("load", event, context)


def report_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Stage 5."""
    return stage_handler("report", event, context)


def notify_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Stage 6."""
    return stage_handler("notify", event, context)


def dispatch_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """Single-image dispatcher: ``event["stage"]`` selects the stage (used when one Lambda serves all states)."""
    return stage_handler(event["stage"], event, context)
