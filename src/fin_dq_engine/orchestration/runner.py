"""Local orchestrator: run one stage, the whole daily chain, or a backfill, with timing and metrics."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import Engine

from fin_dq_engine.batchlog import latest_batch_for_date
from fin_dq_engine.config import Settings
from fin_dq_engine.db import apply_migrations, get_engine
from fin_dq_engine.ingest.ingest import ingest_date
from fin_dq_engine.load.load import load_batch
from fin_dq_engine.logging_utils import get_logger, timed_stage
from fin_dq_engine.metrics import MetricsSink
from fin_dq_engine.notify.notify import send_alert, send_summary
from fin_dq_engine.quality.engine import BatchAbortedError, validate_batch
from fin_dq_engine.reports.runner import run_reports
from fin_dq_engine.reports.summary import build_summary
from fin_dq_engine.transform.transform import transform_batch

log = get_logger(__name__)

STAGES = ["ingest", "transform", "validate", "load", "report", "notify"]


@dataclass
class DailyRunResult:
    """Outcome of one run_daily call."""

    run_date: date
    batch_id: str
    status: str
    stage_seconds: dict[str, float] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    summary_paths: dict[str, Path] = field(default_factory=dict)
    error: str | None = None


def resolve_batch_id(engine: Engine, run_date: date, batch_id: str | None) -> str:
    """Use the given batch id or look up the latest ingested batch for the date."""
    if batch_id:
        return batch_id
    with engine.connect() as conn:
        found = latest_batch_for_date(conn, run_date)
    if not found:
        raise LookupError(f"No ingested batch for {run_date}; run `fin-dq ingest --run-date {run_date}` first")
    return found


def run_stage(
    settings: Settings, engine: Engine, stage: str, run_date: date, batch_id: str | None = None, **kwargs: Any
) -> Any:
    """Run a single stage by name (used by the CLI and the Lambda handlers)."""
    if stage == "ingest":
        return ingest_date(settings, engine, run_date, force=bool(kwargs.get("force")))
    bid = resolve_batch_id(engine, run_date, batch_id)
    if stage == "transform":
        return transform_batch(settings, engine, bid, run_date)
    if stage == "validate":
        return validate_batch(settings, engine, bid, run_date)
    if stage == "load":
        return load_batch(settings, engine, bid, run_date)
    if stage == "report":
        reports = run_reports(settings, engine, bid, run_date)
        return build_summary(settings, engine, bid, run_date, reports)
    if stage == "notify":
        md_path = settings.paths.out_dir / run_date.isoformat() / "summary.md"
        if not md_path.exists():
            raise FileNotFoundError(f"{md_path} missing; run the report stage first")
        return send_summary(settings, run_date, md_path.read_text(encoding="utf-8"))
    raise ValueError(f"Unknown stage {stage!r}; expected one of {STAGES}")


def run_daily(
    settings: Settings,
    engine: Engine | None = None,
    run_date: date | None = None,
    *,
    force: bool = False,
    notify: bool = True,
) -> DailyRunResult:
    """Run the six stages for one date with structured logging, per-stage timing and custom metrics.

    A :class:`BatchAbortedError` (blocking failures above threshold) stops before load and raises a critical alert.
    Any other exception is logged, alerted, and re-raised.
    """
    run_date = run_date or (date.today() - timedelta(days=1))
    engine = engine or get_engine(settings)
    apply_migrations(engine, settings.paths.sql_dir / "ddl")
    metrics = MetricsSink(settings)
    result = DailyRunResult(run_date, "", "running")
    t0 = time.perf_counter()

    def timed(stage: str, fn: Any) -> Any:
        with timed_stage(log, stage, run_date=run_date.isoformat(), batch_id=result.batch_id) as info:
            out = fn()
            info["batch_id"] = result.batch_id
        result.stage_seconds[stage] = info["duration_seconds"]
        metrics.put("stage_duration_seconds", info["duration_seconds"], "Seconds", stage=stage)
        return out

    try:
        ingest = timed("ingest", lambda: ingest_date(settings, engine, run_date, force=force))
        result.batch_id = ingest.batch_id
        tr = timed("transform", lambda: transform_batch(settings, engine, result.batch_id, run_date))
        val = timed("validate", lambda: validate_batch(settings, engine, result.batch_id, run_date))
        timed("load", lambda: load_batch(settings, engine, result.batch_id, run_date))
        reports = timed("report", lambda: run_reports(settings, engine, result.batch_id, run_date))
        result.summary_paths = timed(
            "summary", lambda: build_summary(settings, engine, result.batch_id, run_date, reports)
        )
        result.metrics = {
            "rows_processed": float(tr.rows_out),
            "rows_quarantined": float(val.scorecard.rows_quarantined),
            "dq_pass_rate": float(val.scorecard.dq_pass_rate),
            "anomalies_flagged": float(len(val.anomalies)),
        }
        for k, v in result.metrics.items():
            metrics.put(k, v, "Percent" if k == "dq_pass_rate" else "Count", run_date=run_date.isoformat())
        if notify:
            timed(
                "notify",
                lambda: send_summary(settings, run_date, result.summary_paths["markdown"].read_text(encoding="utf-8")),
            )
        result.status = "success"
    except BatchAbortedError as exc:
        result.status = "aborted"
        result.error = str(exc)
        result.metrics = {
            "rows_quarantined": float(exc.scorecard.rows_quarantined),
            "dq_pass_rate": float(exc.scorecard.dq_pass_rate),
        }
        metrics.put("dq_pass_rate", exc.scorecard.dq_pass_rate, "Percent", run_date=run_date.isoformat())
        metrics.put("batch_aborted", 1, "Count", run_date=run_date.isoformat())
        if notify:
            send_alert(
                settings,
                run_date,
                "Pipeline aborted: data quality threshold breached",
                f"{exc}\n\nQuarantine reasons: {exc.scorecard.quarantine_reasons}\n\nSee docs/runbook.md#pipeline-aborted.",
            )
    except Exception as exc:
        result.status = "failed"
        result.error = f"{type(exc).__name__}: {exc}"
        metrics.put("stage_failure", 1, "Count", run_date=run_date.isoformat())
        if notify:
            send_alert(settings, run_date, "Pipeline stage failed", result.error)
        metrics.flush()
        raise
    finally:
        result.stage_seconds["total"] = round(time.perf_counter() - t0, 3)
        metrics.flush()
        log.info(
            "run_daily_finished",
            run_date=run_date.isoformat(),
            status=result.status,
            batch_id=result.batch_id,
            seconds=result.stage_seconds.get("total"),
            **result.metrics,
        )
    return result


def run_backfill(
    settings: Settings,
    start: date,
    end: date,
    *,
    engine: Engine | None = None,
    notify: bool = False,
    stop_on_error: bool = False,
) -> list[DailyRunResult]:
    """Run run_daily for every date in [start, end]. Dates without a source file are skipped and reported."""
    engine = engine or get_engine(settings)
    results: list[DailyRunResult] = []
    day = start
    while day <= end:
        try:
            results.append(run_daily(settings, engine, day, notify=notify))
        except FileNotFoundError as exc:
            results.append(DailyRunResult(day, "", "skipped", error=str(exc)))
        except Exception as exc:  # already logged and alerted by run_daily
            results.append(DailyRunResult(day, "", "failed", error=str(exc)))
            if stop_on_error:
                break
        day += timedelta(days=1)
    return results
