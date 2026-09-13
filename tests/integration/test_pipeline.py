"""End-to-end: seeded 10k rows with known defects, full pipeline, exact assertions."""

from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import Engine, text
from tests.conftest import FIXTURE_CFG, FIXTURE_DAYS

from fin_dq_engine.config import Settings
from fin_dq_engine.governance.lineage import show_lineage
from fin_dq_engine.governance.retention import apply_retention, release_quarantine
from fin_dq_engine.load.load import load_batch
from fin_dq_engine.orchestration.runner import resolve_batch_id, run_backfill, run_daily, run_stage
from fin_dq_engine.quality.engine import BatchAbortedError, explain_rule, validate_batch
from fin_dq_engine.reports.runner import run_reconciliation

pytestmark = pytest.mark.integration

# defect type -> blocking rule that must quarantine it
BLOCKING_MAP = {
    "null_transaction_id": "DQ-001",
    "null_posted_date": "DQ-002",
    "null_amount": "DQ-003",
    "bad_date": "DQ-008",
    "orphan_account": "DQ-010",
    "orphan_entity": "DQ-011",
}


def _q(engine: Engine, sql: str, **params: Any) -> pd.DataFrame:
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params)


def test_all_days_succeeded(seeded: dict[str, Any]) -> None:
    assert [r.status for r in seeded["results"]] == ["success"] * FIXTURE_DAYS
    assert all(r.batch_id for r in seeded["results"])
    assert all(
        set(r.stage_seconds) >= {"ingest", "transform", "validate", "load", "report", "total"}
        for r in seeded["results"]
    )


def test_exact_quarantine_counts_per_rule(seeded: dict[str, Any], pg_engine: Engine) -> None:
    manifest = seeded["manifest"]["defects"]
    for r in seeded["results"]:
        day = r.run_date.isoformat()
        expected = manifest.get(day, {})
        results = _q(pg_engine, "SELECT rule_id, rows_failed FROM dq.rule_results WHERE batch_id = :b", b=r.batch_id)
        failed = dict(zip(results.rule_id, results.rows_failed, strict=True))
        for defect, rule in BLOCKING_MAP.items():
            assert failed[rule] == expected.get(defect, 0), f"{day} {defect}/{rule}"
        # unknown currency trips both the accepted_values rule and the FX resolution rule
        assert failed["DQ-006"] == expected.get("bad_currency", 0), day
        assert failed["DQ-015"] >= expected.get("bad_currency", 0), day
        assert failed["DQ-016"] == expected.get("sign_flip", 0), day
        assert failed["DQ-017"] == expected.get("blank_description", 0), day
        assert failed["DQ-012"] == expected.get("orphan_customer", 0), day
        # quarantine = union of blocking failures; each defect hits a distinct row
        blocking_rows = sum(expected.get(d, 0) for d in BLOCKING_MAP) + expected.get("bad_currency", 0)
        q = _q(pg_engine, "SELECT count(*) AS n FROM dq.quarantine WHERE batch_id = :b", b=r.batch_id).n[0]
        late_unpriced = failed["DQ-015"] - expected.get("bad_currency", 0)
        assert blocking_rows <= q <= blocking_rows + late_unpriced, day
        dupes = _q(
            pg_engine,
            "SELECT details->>'duplicates_removed' AS d FROM dq.batch_run_log WHERE batch_id = :b AND stage = 'transform'",
            b=r.batch_id,
        ).d[0]
        assert int(dupes) == expected.get("duplicate", 0), day


def test_reconciliation_passes_for_every_batch(seeded: dict[str, Any], pg_engine: Engine, settings: Settings) -> None:
    with pg_engine.connect() as conn:
        for r in seeded["results"]:
            rec = run_reconciliation(conn, settings.paths.sql_dir, r.batch_id)
            assert rec["reconciled"] is True, rec
            assert rec["row_gap"] == 0 and float(rec["amount_gap"]) == 0.0


def test_trial_balance_balances(seeded: dict[str, Any], pg_engine: Engine, settings: Settings) -> None:
    sql = (settings.paths.sql_dir / "reports" / "gl_trial_balance.sql").read_text()
    tb = _q(pg_engine, sql, run_date=seeded["last_date"])
    assert not tb.empty and (tb.debits_usd >= 0).all() and (tb.credits_usd >= 0).all()
    # 1. Debits equal credits across every complete double-entry pair.
    complete = _q(
        pg_engine,
        """
        WITH ev AS (SELECT event_id, SUM(amount_usd) AS net, count(*) AS legs FROM curated.fact_transactions GROUP BY 1)
        SELECT COALESCE(SUM(f.amount_usd), 0) AS net, count(*) AS legs FROM curated.fact_transactions f
        JOIN ev ON ev.event_id = f.event_id WHERE ev.legs = 2 AND ABS(ev.net) < 0.005""",
    )
    assert abs(float(complete.net[0])) < 0.01 and int(complete.legs[0]) > 0
    # 2. The report's per-account flags identify exactly the legs of unbalanced events.
    unbalanced = _q(
        pg_engine,
        """
        WITH ev AS (SELECT event_id, SUM(amount_usd) AS net, count(*) AS legs FROM curated.fact_transactions GROUP BY 1)
        SELECT f.transaction_id, f.event_id, f.entity_id, f.amount_usd, f.dq_score
        FROM curated.fact_transactions f JOIN ev ON ev.event_id = f.event_id WHERE ABS(ev.net) >= 0.005 OR ev.legs % 2 = 1""",
    )
    assert int(tb.unbalanced_legs.sum()) == len(unbalanced)
    assert (
        tb.loc[tb.unbalanced_legs > 0, "account_flag"].all()
        and not tb.loc[tb.unbalanced_legs == 0, "account_flag"].any()
    )
    # 3. Entity-month imbalance equals the sum of its unbalanced legs, and is flagged.
    per_em = tb.groupby(["entity_id", "month_start"]).agg(
        imb=("entity_imbalance_usd", "first"),
        unb=("unbalanced_usd", "sum"),
        balanced=("entity_month_balanced", "first"),
    )
    assert ((per_em.imb - per_em.unb).abs() < 0.05).all()
    assert (per_em.balanced == (per_em.imb.abs() < 0.01)).all()
    # 4. Every unbalanced event is explained by a quarantined partner leg or a sign-flip warning.
    quarantined_events = set(_q(pg_engine, "SELECT DISTINCT row_data->>'event_id' AS e FROM dq.quarantine").e.dropna())
    for event_id, grp in unbalanced.groupby("event_id"):
        assert event_id in quarantined_events or (grp.dq_score < 100).any(), event_id


def test_reports_expected_row_counts(seeded: dict[str, Any], pg_engine: Engine, settings: Settings) -> None:
    last: date = seeded["last_date"]
    curated_dir = settings.curated_dir / "reports" / last.isoformat()
    frames = {p.stem: pd.read_csv(p) for p in curated_dir.glob("*.csv")}
    assert len(frames) == 8
    n_rules = _q(pg_engine, "SELECT count(DISTINCT rule_id) AS n FROM dq.rule_results").n[0]
    assert len(frames["dq_scorecard"]) == n_rules == 17
    entity_days = _q(
        pg_engine,
        """
        SELECT count(*) AS n FROM (SELECT DISTINCT f.entity_id, f.posted_date FROM curated.fact_transactions f
        JOIN curated.dim_account a USING (account_id) WHERE a.account_type IN ('revenue','cogs')
        AND f.posted_date BETWEEN :d - 89 AND :d) x""",
        d=last,
    ).n[0]
    assert len(frames["daily_revenue_by_entity"]) == entity_days
    sources = _q(
        pg_engine,
        "SELECT count(DISTINCT source_system) AS n FROM curated.fact_transactions WHERE batch_id = :b",
        b=seeded["results"][-1].batch_id,
    ).n[0]
    assert len(frames["late_arriving_data"]) == sources
    assert len(frames["customer_concentration"]) == 10
    assert frames["customer_concentration"]["cumulative_share_pct"].is_monotonic_increasing
    assert (frames["ar_aging"]["total_open_usd"] > 0).all()
    assert frames["ar_aging"]["customer_email"].dropna().str.contains(r"\*\*\*@").all()  # masked for analysts
    for p in curated_dir.glob("*.parquet"):
        assert len(pd.read_parquet(p)) == len(frames[p.stem])


def test_lineage_pii_audit_rows_exist(seeded: dict[str, Any], pg_engine: Engine) -> None:
    last = seeded["last_date"]
    with pg_engine.connect() as conn:
        lineage = show_lineage(conn, "ar_aging", last)
    assert len(lineage) >= 1
    row = lineage.iloc[0]
    assert "curated.fact_transactions" in row.source_tables and row.batch_id == seeded["results"][-1].batch_id
    assert len(row.rule_versions) == 17
    all_lineage = _q(pg_engine, "SELECT count(*) AS n FROM governance.lineage WHERE run_date = :d", d=last).n[0]
    assert all_lineage == 8
    pii = _q(
        pg_engine,
        "SELECT report_name, masked FROM governance.pii_access_log WHERE report_name IN ('ar_aging','customer_concentration')",
    )
    assert not pii.empty and pii.masked.all()
    audit = _q(
        pg_engine, "SELECT DISTINCT target_schema || '.' || target_table AS t FROM governance.audit_log"
    ).t.tolist()
    assert {"curated.fact_transactions", "dq.quarantine", "dq.rule_results", "raw.transactions"} <= set(audit)
    registry = _q(pg_engine, "SELECT count(*) AS n FROM dq.rule_registry WHERE is_active").n[0]
    assert registry == 17


def test_idempotent_rerun_leaves_curated_unchanged(
    seeded: dict[str, Any], pg_engine: Engine, settings: Settings
) -> None:
    before = _q(pg_engine, "SELECT count(*) AS n, COALESCE(SUM(amount_usd),0) AS s FROM curated.fact_transactions")
    day = seeded["results"][2].run_date
    r = run_daily(settings, pg_engine, day, notify=False)
    assert r.status == "success" and r.batch_id == seeded["results"][2].batch_id
    after = _q(pg_engine, "SELECT count(*) AS n, COALESCE(SUM(amount_usd),0) AS s FROM curated.fact_transactions")
    assert before.n[0] == after.n[0] and float(before.s[0]) == float(after.s[0])
    skipped = _q(
        pg_engine,
        "SELECT status FROM dq.batch_run_log WHERE batch_id = :b AND stage = 'ingest' ORDER BY log_id DESC LIMIT 1",
        b=r.batch_id,
    ).status[0]
    assert skipped == "skipped"
    q = _q(pg_engine, "SELECT count(*) AS n FROM dq.quarantine WHERE batch_id = :b", b=r.batch_id).n[0]
    exp = seeded["manifest"]["defects"].get(day.isoformat(), {})
    assert q >= sum(exp.get(d, 0) for d in BLOCKING_MAP)
    # forced re-ingest also converges to the same curated state
    r2 = run_daily(settings, pg_engine, day, force=True, notify=False)
    assert r2.status == "success"
    again = _q(pg_engine, "SELECT count(*) AS n FROM curated.fact_transactions")
    assert again.n[0] == before.n[0]


def test_scd2_customer_merge(seeded: dict[str, Any], pg_engine: Engine, settings: Settings) -> None:
    changes = sorted(settings.raw_dir.glob("customers/*.csv"))
    in_window = [p for p in changes if date.fromisoformat(p.stem) <= seeded["last_date"]]
    versions = _q(
        pg_engine, "SELECT customer_id, count(*) AS n, bool_or(is_current) AS cur FROM curated.dim_customer GROUP BY 1"
    )
    assert (versions.n >= 1).all() and versions.cur.all()
    if in_window:
        changed = pd.concat(pd.read_csv(p) for p in in_window)
        multi = versions[versions.customer_id.isin(changed.customer_id)]
        assert (multi.n == 2).all(), "changed customers must have exactly two versions"
        closed = _q(
            pg_engine,
            "SELECT valid_to, is_current FROM curated.dim_customer WHERE customer_id = :c ORDER BY valid_from",
            c=changed.customer_id.iloc[0],
        )
        assert not closed.is_current.iloc[0] and closed.is_current.iloc[-1]
        assert closed.valid_to.iloc[0] < date(9999, 12, 31)
    # re-loading the same batch is a no-op
    bid = seeded["results"][-1].batch_id
    n_before = _q(pg_engine, "SELECT count(*) AS n FROM curated.dim_customer").n[0]
    load_batch(settings, pg_engine, bid, seeded["last_date"])
    assert _q(pg_engine, "SELECT count(*) AS n FROM curated.dim_customer").n[0] == n_before


def test_anomalies_flagged(seeded: dict[str, Any], pg_engine: Engine) -> None:
    flags = _q(pg_engine, "SELECT method, subject, entity_id, details FROM dq.anomaly_flags")
    assert not flags.empty
    spike = seeded["manifest"]["fx_spikes"]
    in_window = [
        s
        for s in spike
        if date.fromisoformat(s["date"]) <= seeded["last_date"]
        and date.fromisoformat(s["date"]) >= FIXTURE_CFG.start_date
    ]
    for s in in_window:
        assert ((flags.method == "fx_rate_jump") & (flags.subject == s["currency"])).any(), s


def test_batch_abort_above_threshold(seeded: dict[str, Any], pg_engine: Engine, settings: Settings) -> None:
    strict = settings.model_copy(deep=True)
    strict.pipeline.batch_failure_threshold_pct = 0.0
    r = seeded["results"][0]
    with pytest.raises(BatchAbortedError) as exc:
        validate_batch(strict, pg_engine, r.batch_id, r.run_date)
    assert exc.value.scorecard.rows_quarantined > 0
    status = _q(
        pg_engine,
        "SELECT status FROM dq.batch_run_log WHERE batch_id = :b AND stage = 'validate' ORDER BY log_id DESC LIMIT 1",
        b=r.batch_id,
    ).status[0]
    assert status == "aborted"
    # run_daily surfaces the abort without raising and writes a critical alert
    out = run_daily(strict, pg_engine, r.run_date, notify=True)
    assert out.status == "aborted" and out.error and "threshold" in out.error
    assert (
        (strict.paths.out_dir / r.run_date.isoformat() / "notification.txt")
        .read_text()
        .startswith("Subject: [fin-dq] CRITICAL")
    )
    # restore a clean validate/load for later tests
    validate_batch(settings, pg_engine, r.batch_id, r.run_date)
    load_batch(settings, pg_engine, r.batch_id, r.run_date)


def test_run_stage_and_backfill_edges(seeded: dict[str, Any], pg_engine: Engine, settings: Settings) -> None:
    last = seeded["last_date"]
    assert resolve_batch_id(pg_engine, last, None) == seeded["results"][-1].batch_id
    with pytest.raises(LookupError):
        resolve_batch_id(pg_engine, date(1990, 1, 1), None)
    with pytest.raises(ValueError):
        run_stage(settings, pg_engine, "nope", last)
    assert run_stage(settings, pg_engine, "notify", last).channel == "local"
    missing_day = FIXTURE_CFG.end_date + timedelta(days=5)
    results = run_backfill(settings, date(2025, 1, 1), date(2025, 1, 1), engine=pg_engine)
    assert results[0].status == "success"
    results = run_backfill(settings, missing_day, missing_day, engine=pg_engine)
    assert results[0].status == "skipped"


def test_summary_outputs(seeded: dict[str, Any], settings: Settings) -> None:
    last = seeded["last_date"].isoformat()
    out = settings.paths.out_dir / last
    md = (out / "summary.md").read_text()
    html = (out / "summary.html").read_text()
    assert (
        "## Headline KPIs" in md
        and "## Technical appendix" in md
        and md.index("Headline") < md.index("---") < md.index("Technical")
    )
    assert html.count("data:image/png;base64") == 3
    assert (out / "charts" / "revenue_trend.png").stat().st_size > 1000
    blocks = (
        json.loads((out / "notification.slack.json").read_text())["blocks"]
        if (out / "notification.slack.json").exists()
        else None
    )
    assert blocks is None or blocks[0]["type"] == "header"
    metrics = [json.loads(line) for line in (settings.paths.out_dir / "metrics.jsonl").read_text().splitlines()]
    names = {m["MetricName"] for m in metrics}
    assert {"rows_processed", "rows_quarantined", "dq_pass_rate", "stage_duration_seconds"} <= names


def test_golden_reports(seeded: dict[str, Any], pg_engine: Engine, settings: Settings) -> None:
    """Each report matches tests/fixtures/golden/<report>.csv. Set UPDATE_GOLDEN=1 to regenerate."""
    import os

    golden_dir = Path(__file__).resolve().parents[1] / "fixtures" / "golden"
    golden_dir.mkdir(parents=True, exist_ok=True)
    last = seeded["last_date"]
    curated_dir = settings.curated_dir / "reports" / last.isoformat()
    for csv in sorted(curated_dir.glob("*.csv")):
        actual = pd.read_csv(csv)
        golden_path = golden_dir / csv.name
        if os.environ.get("UPDATE_GOLDEN") == "1" or not golden_path.exists():
            actual.to_csv(golden_path, index=False)
        expected = pd.read_csv(golden_path)
        pd.testing.assert_frame_equal(
            actual, expected, check_dtype=False, check_exact=False, rtol=1e-4, atol=0.011, obj=csv.stem
        )


def test_zz_explain_release_retention(seeded: dict[str, Any], pg_engine: Engine, settings: Settings) -> None:
    failing = _q(
        pg_engine,
        """SELECT rule_id, run_date FROM dq.rule_results WHERE status = 'fail' AND rule_id <> 'DQ-001'
                               AND rows_failed > 0 ORDER BY run_date LIMIT 1""",
    )
    df = explain_rule(pg_engine, failing.rule_id[0], failing.run_date[0])
    assert not df.empty and "rule" in df.attrs
    assert explain_rule(pg_engine, "DQ-999", failing.run_date[0]).empty
    bid = seeded["results"][0].batch_id
    n = release_quarantine(settings, pg_engine, bid)
    assert n == _q(pg_engine, "SELECT count(*) AS n FROM dq.quarantine WHERE batch_id = :b", b=bid).n[0]
    assert release_quarantine(settings, pg_engine, bid) == 0
    actions = apply_retention(settings, pg_engine, as_of=date(2030, 1, 1))  # dry run by default locally
    assert actions and all(a.dry_run for a in actions)
    assert _q(pg_engine, "SELECT count(*) AS n FROM raw.transactions").n[0] > 0
    # Live run: expire the raw class only (ingested today) and keep operational logs.
    raw_only = settings.model_copy(deep=True)
    raw_only.governance.retention_days = {"raw": 0, "staged": 100_000, "logs": 100_000}
    live = apply_retention(raw_only, pg_engine, as_of=date.today() + timedelta(days=1), dry_run=False)
    raw_action = next(a for a in live if a.table == "raw.transactions")
    assert raw_action.rows > 0 and _q(pg_engine, "SELECT count(*) AS n FROM raw.transactions").n[0] == 0
    logged = _q(pg_engine, "SELECT count(*) AS n FROM governance.retention_log WHERE NOT dry_run").n[0]
    assert logged >= len(live)
