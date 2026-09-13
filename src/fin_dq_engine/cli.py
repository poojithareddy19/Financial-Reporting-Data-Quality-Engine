"""Typer CLI. Every pipeline stage is separately invokable; ``run-daily`` chains them."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

import typer

from fin_dq_engine.config import Settings, load_settings
from fin_dq_engine.logging_utils import configure_logging

app = typer.Typer(add_completion=False, help="Financial Reporting & Data Quality Engine", no_args_is_help=True)
dq_app = typer.Typer(help="Data quality commands")
lineage_app = typer.Typer(help="Lineage commands")
retention_app = typer.Typer(help="Retention commands")
app.add_typer(dq_app, name="dq")
app.add_typer(lineage_app, name="lineage")
app.add_typer(retention_app, name="retention")

RunDate = Annotated[str | None, typer.Option("--run-date", help="YYYY-MM-DD (default: yesterday)")]
LocalFlag = Annotated[
    bool, typer.Option("--local/--aws", help="Run against Docker Postgres and ./data instead of S3/RDS")
]
ConfigOpt = Annotated[Path | None, typer.Option("--config", help="settings.yaml path")]
BatchOpt = Annotated[str | None, typer.Option("--batch-id", help="Batch id (default: latest for run date)")]


def _settings(local: bool, config: Path | None) -> Settings:
    configure_logging(json_output=True)
    return load_settings(config, environment="local" if local else "aws")


def _date(value: str | None) -> date:
    return date.fromisoformat(value) if value else date.today() - timedelta(days=1)


def _engine(settings: Settings) -> Any:
    from fin_dq_engine.db import apply_migrations, get_engine

    engine = get_engine(settings)
    apply_migrations(engine, settings.paths.sql_dir / "ddl")
    return engine


def _echo(obj: Any) -> None:
    typer.echo(json.dumps(obj, indent=2, default=str))


@app.command()
def migrate(local: LocalFlag = True, config: ConfigOpt = None) -> None:
    """Apply sql/ddl migrations."""
    from fin_dq_engine.db import apply_migrations, get_engine

    s = _settings(local, config)
    _echo({"applied": apply_migrations(get_engine(s), s.paths.sql_dir / "ddl")})


@app.command()
def seed(
    local: LocalFlag = True,
    config: ConfigOpt = None,
    events: Annotated[int, typer.Option(help="Business events to generate (0 = reuse existing ./data/raw)")] = 200_000,
    months: int = 24,
    start: str = "2024-09-01",
) -> None:
    """Generate synthetic raw data (if requested) and load reference dimensions."""
    from fin_dq_engine.load.seed import seed_reference_data
    from fin_dq_engine.synthetic import GeneratorConfig, generate_dataset, write_dataset

    s = _settings(local, config)
    if events > 0:
        ds = generate_dataset(GeneratorConfig(start_date=date.fromisoformat(start), months=months, n_events=events))
        stats = write_dataset(ds, s.raw_dir)
        typer.echo(f"generated {stats['rows']:,} rows in {stats['transaction_files']} files -> {s.raw_dir}")
    _echo(seed_reference_data(s, _engine(s)))


@app.command()
def ingest(run_date: RunDate = None, force: bool = False, local: LocalFlag = True, config: ConfigOpt = None) -> None:
    """Stage 1: land raw files for the date (skips if the checksum was already processed)."""
    from fin_dq_engine.orchestration.runner import run_stage

    s = _settings(local, config)
    _echo(run_stage(s, _engine(s), "ingest", _date(run_date), force=force).__dict__)


def _simple_stage(stage: str) -> Any:
    def cmd(
        run_date: RunDate = None, batch_id: BatchOpt = None, local: LocalFlag = True, config: ConfigOpt = None
    ) -> None:
        from fin_dq_engine.orchestration.runner import run_stage

        s = _settings(local, config)
        out = run_stage(s, _engine(s), stage, _date(run_date), batch_id)
        if hasattr(out, "scorecard"):
            _echo({"scorecard": out.scorecard.__dict__, "anomalies": len(out.anomalies)})
        elif hasattr(out, "__dict__"):
            _echo(out.__dict__)
        else:
            _echo(out)

    cmd.__doc__ = {
        "transform": "Stage 2: clean, type, dedupe and FX-normalise into staging + Parquet.",
        "validate": "Stage 3: run the DQ engine; quarantine blocking failures, score the rest, detect anomalies.",
        "load": "Stage 4: upsert curated (idempotent) with SCD2 customer merge.",
        "report": "Stage 5: run sql/reports, write CSV + Parquet, build the HTML + Markdown summary.",
        "notify": "Stage 6: send the summary via SNS (local mode prints and writes ./out).",
    }[stage]
    return cmd


for _stage in ("transform", "validate", "load", "report", "notify"):
    app.command(name=_stage)(_simple_stage(_stage))


@app.command(name="run-daily")
def run_daily_cmd(
    run_date: RunDate = None,
    start: str | None = None,
    end: str | None = None,
    force: bool = False,
    no_notify: bool = False,
    local: LocalFlag = True,
    config: ConfigOpt = None,
) -> None:
    """Run all six stages for one date, or a backfill with --start/--end."""
    from fin_dq_engine.orchestration.runner import run_backfill, run_daily

    s = _settings(local, config)
    if start or end:
        if not (start and end):
            raise typer.BadParameter("--start and --end must be given together")
        results = run_backfill(
            s, date.fromisoformat(start), date.fromisoformat(end), engine=_engine(s), notify=not no_notify
        )
        _echo(
            [
                {
                    "run_date": r.run_date,
                    "status": r.status,
                    "batch_id": r.batch_id,
                    "seconds": r.stage_seconds.get("total"),
                    "error": r.error,
                }
                for r in results
            ]
        )
        return
    r = run_daily(s, _engine(s), _date(run_date), force=force, notify=not no_notify)
    _echo(
        {
            "run_date": r.run_date,
            "status": r.status,
            "batch_id": r.batch_id,
            "stage_seconds": r.stage_seconds,
            "metrics": r.metrics,
            "summary": r.summary_paths,
            "error": r.error,
        }
    )
    if r.status != "success":
        raise typer.Exit(code=2)


@dq_app.command()
def explain(
    rule_id: Annotated[str, typer.Option("--rule-id")],
    run_date: RunDate = None,
    local: LocalFlag = True,
    config: ConfigOpt = None,
) -> None:
    """Print failing samples for a rule on a run date."""
    from fin_dq_engine.quality.engine import explain_rule

    s = _settings(local, config)
    df = explain_rule(_engine(s), rule_id, _date(run_date))
    if df.empty:
        typer.echo(f"No results for {rule_id} on {_date(run_date)}")
        raise typer.Exit(code=1)
    if "rule" in df.attrs:
        r = df.attrs["rule"]
        typer.echo(
            f"{r['rule_id']} [{r['severity']}] {r['description']}: {r['rows_failed']} of {r['rows_checked']} failed"
        )
    typer.echo(df.to_string(index=False))


@dq_app.command()
def release(
    batch_id: Annotated[str, typer.Option("--batch-id")],
    rule_id: str | None = None,
    local: LocalFlag = True,
    config: ConfigOpt = None,
) -> None:
    """Release quarantined rows for a batch (audited). See docs/runbook.md."""
    from fin_dq_engine.governance.retention import release_quarantine

    s = _settings(local, config)
    typer.echo(f"released {release_quarantine(s, _engine(s), batch_id, rule_id)} rows")


@lineage_app.command()
def show(
    report: Annotated[str, typer.Option("--report")],
    run_date: RunDate = None,
    local: LocalFlag = True,
    config: ConfigOpt = None,
) -> None:
    """Show which tables, batches and rule versions produced a report."""
    from fin_dq_engine.governance.lineage import show_lineage

    s = _settings(local, config)
    with _engine(s).connect() as conn:
        df = show_lineage(conn, report, _date(run_date))
    _echo(df.to_dict(orient="records"))


@retention_app.command(name="apply")
def retention_apply(
    as_of: str | None = None,
    execute: Annotated[bool, typer.Option("--execute", help="Actually delete")] = False,
    local: LocalFlag = True,
    config: ConfigOpt = None,
) -> None:
    """Delete raw partitions and logs past their retention class. Dry-run unless --execute."""
    from fin_dq_engine.governance.retention import apply_retention

    s = _settings(local, config)
    as_of_date = date.fromisoformat(as_of) if as_of else datetime.now().date()
    _echo([a.__dict__ for a in apply_retention(s, _engine(s), as_of=as_of_date, dry_run=not execute)])


if __name__ == "__main__":
    app()
