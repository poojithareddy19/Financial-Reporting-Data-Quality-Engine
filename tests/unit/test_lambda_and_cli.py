"""Lambda handlers (against the seeded database) and the Typer CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine
from typer.testing import CliRunner

from fin_dq_engine import cli
from fin_dq_engine.config import Settings
from fin_dq_engine.orchestration import lambda_handlers as lh

pytestmark = pytest.mark.integration


@pytest.fixture
def lambda_ctx(monkeypatch: pytest.MonkeyPatch, settings: Settings, pg_engine: Engine) -> None:
    monkeypatch.setattr(lh, "_settings", settings)
    monkeypatch.setattr(lh, "_engine", pg_engine)


def test_lambda_chain(lambda_ctx: None, seeded: dict[str, Any]) -> None:
    day = seeded["results"][1].run_date.isoformat()
    ev: dict[str, Any] = {"run_date": day, "force": True}  # re-land raw in case retention tests expired it
    ev = lh.ingest_handler(ev)
    assert ev["batch_id"] == seeded["results"][1].batch_id and ev["stage"] == "ingest"
    ev = lh.transform_handler(ev)
    assert ev["transform_result"]["rows_out"] > 0
    ev = lh.validate_handler(ev)
    assert 0 <= ev["scorecard"]["dq_pass_rate"] <= 1 and "anomalies" in ev
    ev = lh.load_handler(ev)
    ev = lh.report_handler(ev)
    assert "markdown" in ev["report_result"]
    ev = lh.notify_handler(ev)
    assert ev["notify_result"]["channel"] == "local"
    out = lh.dispatch_handler({"stage": "load", "run_date": day, "batch_id": ev["batch_id"]})
    assert out["stage"] == "load"
    assert json.dumps(out)  # Step Functions payloads must be JSON-serialisable


def test_lambda_defaults_to_yesterday(lambda_ctx: None) -> None:
    from datetime import UTC, datetime, timedelta

    assert lh._run_date({}) == (datetime.now(UTC) - timedelta(days=1)).date()
    serialized = lh._serialize({"p": Path("/x"), "s": {1, 2}})
    assert serialized["p"] == str(Path("/x"))  # separator differs by OS
    assert serialized["s"] == [1, 2]


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, settings: Settings, database_url: str) -> None:
    monkeypatch.setenv("FIN_DQ__DATABASE__URL", database_url)
    monkeypatch.setenv("FIN_DQ__PATHS__DATA_DIR", str(settings.paths.data_dir))
    monkeypatch.setenv("FIN_DQ__PATHS__OUT_DIR", str(settings.paths.out_dir))
    monkeypatch.setenv("FIN_DQ__PATHS__SQL_DIR", str(settings.paths.sql_dir))
    monkeypatch.setenv("FIN_DQ__PATHS__CONFIG_DIR", str(settings.paths.config_dir))
    monkeypatch.setenv("FIN_DQ_CONFIG", str(settings.paths.config_dir / "settings.yaml"))


def _run(*args: str) -> Any:
    return CliRunner().invoke(cli.app, list(args))


def test_cli_stages(cli_env: None, seeded: dict[str, Any]) -> None:
    day = seeded["results"][1].run_date.isoformat()
    assert _run("--help").exit_code == 0
    r = _run("migrate")
    assert r.exit_code == 0, r.output
    r = _run("ingest", "--run-date", day)
    assert r.exit_code == 0 and '"skipped": true' in r.output
    for stage in ("transform", "validate", "load", "report", "notify"):
        r = _run(stage, "--run-date", day)
        assert r.exit_code == 0, f"{stage}: {r.output[-800:]}"
    r = _run("run-daily", "--run-date", day, "--no-notify")
    assert r.exit_code == 0 and '"status": "success"' in r.output
    r = _run("run-daily", "--start", day, "--end", day, "--no-notify")
    assert r.exit_code == 0 and "success" in r.output
    assert _run("run-daily", "--start", day).exit_code != 0


def test_cli_governance(cli_env: None, seeded: dict[str, Any]) -> None:
    day = seeded["last_date"].isoformat()
    r = _run("lineage", "show", "--report", "ar_aging", "--run-date", day)
    assert r.exit_code == 0 and "curated.fact_transactions" in r.output
    r = _run("dq", "explain", "--rule-id", "DQ-010", "--run-date", day)
    assert r.exit_code in (0, 1)
    r = _run("dq", "explain", "--rule-id", "DQ-999", "--run-date", day)
    assert r.exit_code == 1
    r = _run("retention", "apply", "--as-of", "2030-01-01")
    assert r.exit_code == 0 and '"dry_run": true' in r.output
    r = _run("dq", "release", "--batch-id", "no-such-batch")
    assert r.exit_code == 0 and "released 0 rows" in r.output
    r = _run("seed", "--events", "0")
    assert r.exit_code == 0 and "dim_account" in r.output
