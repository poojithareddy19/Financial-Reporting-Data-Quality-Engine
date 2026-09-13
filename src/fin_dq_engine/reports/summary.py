"""Stage 5b: the executive summary. Business section first, technical appendix after a divider."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import markdown as md
import pandas as pd
from jinja2 import Environment, select_autoescape
from sqlalchemy import Engine

from fin_dq_engine.config import Settings
from fin_dq_engine.db import query_df
from fin_dq_engine.reports import charts
from fin_dq_engine.reports.runner import ReportResult, run_reconciliation
from fin_dq_engine.storage import Storage, get_storage


@dataclass
class SummaryData:
    """Everything the renderers need."""

    run_date: date
    batch_id: str
    kpis: dict[str, Any]
    anomalies: list[str]
    movers: pd.DataFrame
    stage_timings: pd.DataFrame
    row_counts: dict[str, Any]
    quarantine_reasons: pd.DataFrame
    reconciliation: dict[str, Any]
    rule_failures: pd.DataFrame
    chart_paths: dict[str, Path] = field(default_factory=dict)


def _money(v: Any) -> str:
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return "n/a"


def _pct(v: Any) -> str:
    return "n/a" if v is None or pd.isna(v) else f"{float(v):.1f}%"


def _num(v: Any) -> str:
    return "" if v is None or pd.isna(v) else f"{int(v):,}"


def describe_anomaly(row: pd.Series) -> str:
    """Turn one dq.anomaly_flags row into a plain-English sentence."""
    d = row["details"] if isinstance(row["details"], dict) else {}
    m = row["method"]
    if m == "revenue_zscore_mad":
        return (
            f"Revenue for {row['entity_id']} on {row['subject']} was {_money(d.get('revenue'))}, about "
            f"{d.get('ratio_to_mean', '?')}x the trailing {d.get('window_days', 28)}-day average "
            f"({_money(d.get('baseline_mean'))}). Z-score {d.get('zscore')}."
        )
    if m == "fx_rate_jump":
        return (
            f"The {row['subject']} rate moved {d.get('change_pct')}% in one day (from {d.get('prior_rate')} to "
            f"{d.get('rate')}). Treasury should confirm the rate before month-end revaluation."
        )
    if m == "benford_first_digit":
        return (
            f"Amounts for {row['entity_id']} in {row['subject']} do not follow Benford's law (p={row['score']}, "
            f"{d.get('rows')} postings); review for manual or fabricated entries."
        )
    if m == "isolation_forest":
        return (
            f"Transaction {row['subject']} ({row['entity_id']}, {_money(d.get('amount_usd'))}, {d.get('account_type')} "
            f"account, booked at hour {int(d.get('hour', 0))}) is an outlier versus the last 90 days."
        )
    return f"{m}: {row['subject']} scored {row['score']} against threshold {row['threshold']}."


def gather_summary_data(
    settings: Settings, engine: Engine, batch_id: str, run_date: date, reports: dict[str, ReportResult]
) -> SummaryData:
    """Pull KPIs, anomalies, movers and technical facts from the warehouse and report frames."""
    daily = reports["daily_revenue_by_entity"].frame
    aging = reports["ar_aging"].frame
    movers = reports["month_over_month_variance"].frame
    with engine.connect() as conn:
        anomalies = query_df(
            conn,
            """
            SELECT method, entity_id, subject, score, threshold, details FROM dq.anomaly_flags
            WHERE batch_id = :b ORDER BY CASE method WHEN 'revenue_zscore_mad' THEN 0 WHEN 'fx_rate_jump' THEN 1
                                          WHEN 'benford_first_digit' THEN 2 ELSE 3 END, score DESC""",
            {"b": batch_id},
        )
        timings = query_df(
            conn,
            """
            SELECT stage, status, rows_in, rows_out,
                   ROUND(EXTRACT(EPOCH FROM (finished_at - started_at))::numeric, 2) AS duration_seconds
            FROM dq.batch_run_log WHERE batch_id = :b AND log_id IN (
                SELECT MAX(log_id) FROM dq.batch_run_log WHERE batch_id = :b GROUP BY stage)
            ORDER BY log_id""",
            {"b": batch_id},
        )
        scorecard = query_df(
            conn,
            "SELECT details FROM dq.batch_run_log WHERE batch_id = :b AND stage = 'validate' "
            "ORDER BY log_id DESC LIMIT 1",
            {"b": batch_id},
        )
        q_reasons = query_df(
            conn,
            """
            SELECT unnest(rule_ids) AS rule_id, COUNT(*) AS rows FROM dq.quarantine WHERE batch_id = :b
            GROUP BY 1 ORDER BY 2 DESC""",
            {"b": batch_id},
        )
        rule_failures = query_df(
            conn,
            """
            SELECT r.rule_id, r.severity, r.rows_failed, r.failure_rate, g.description FROM dq.rule_results r
            JOIN dq.rule_registry g ON g.rule_id = r.rule_id AND g.rule_version = r.rule_version
            WHERE r.batch_id = :b AND r.status <> 'pass' ORDER BY r.severity, r.rows_failed DESC""",
            {"b": batch_id},
        )
        recon = run_reconciliation(conn, settings.paths.sql_dir, batch_id)
        window_flags = query_df(
            conn,
            "SELECT method, subject, details FROM dq.anomaly_flags WHERE method = 'revenue_zscore_mad' "
            "AND run_date BETWEEN :s AND :e",
            {"s": run_date - timedelta(days=89), "e": run_date},
        )
        history = query_df(
            conn,
            """
            SELECT run_date, MAX((details->'scorecard'->>'dq_pass_rate')::numeric) AS dq_pass_rate
            FROM dq.batch_run_log WHERE stage = 'validate' AND details ? 'scorecard' AND run_date <= :d
            GROUP BY run_date ORDER BY run_date DESC LIMIT 30""",
            {"d": run_date},
        )

    sc = scorecard.iloc[0]["details"].get("scorecard", {}) if not scorecard.empty else {}
    month_start = run_date.replace(day=1)
    day_rows = daily[pd.to_datetime(daily["posted_date"]).dt.date == run_date]
    mtd_rows = daily[pd.to_datetime(daily["posted_date"]).dt.date >= month_start]
    kpis = {
        "revenue_day": float(day_rows["revenue_usd"].astype(float).sum()),
        "revenue_mtd": float(mtd_rows["revenue_usd"].astype(float).sum()),
        "margin_mtd": float(mtd_rows["gross_margin_usd"].astype(float).sum()),
        "margin_pct_mtd": (
            float(mtd_rows["gross_margin_usd"].astype(float).sum())
            / float(mtd_rows["revenue_usd"].astype(float).sum())
            * 100
        )
        if float(mtd_rows["revenue_usd"].astype(float).sum())
        else 0.0,
        "ar_over_90": float(pd.to_numeric(aging["bucket_90_plus"], errors="coerce").fillna(0).sum())
        if not aging.empty
        else 0.0,
        "ar_total": float(pd.to_numeric(aging["total_open_usd"], errors="coerce").fillna(0).sum())
        if not aging.empty
        else 0.0,
        "dq_pass_rate": float(sc.get("dq_pass_rate", 1.0)) * 100,
        "mean_dq_score": sc.get("mean_dq_score"),
        "rows_quarantined": sc.get("rows_quarantined", 0),
        "rows_checked": sc.get("rows_checked", 0),
        "anomaly_count": len(anomalies),
    }
    current_month = movers[pd.to_datetime(movers["month_start"]).dt.date == month_start] if not movers.empty else movers
    top_movers = current_month.nsmallest(5, "mom_mover_rank") if not current_month.empty else current_month

    out_dir = settings.paths.out_dir / run_date.isoformat() / "charts"
    chart_paths = {
        "revenue": charts.revenue_trend_chart(daily, window_flags, out_dir / "revenue_trend.png"),
        "aging": charts.ar_aging_chart(aging, out_dir / "ar_aging.png"),
        "dq": charts.dq_pass_rate_chart(history.sort_values("run_date"), out_dir / "dq_pass_rate.png"),
    }
    return SummaryData(
        run_date,
        batch_id,
        kpis,
        [describe_anomaly(r) for _, r in anomalies.head(3).iterrows()],
        top_movers,
        timings,
        {"reconciliation": recon},
        q_reasons,
        recon,
        rule_failures,
        chart_paths,
    )


MD_TEMPLATE = """# Daily Financial Summary: {{ d.run_date }}

## Headline KPIs
| KPI | Value |
|---|---|
| Revenue on {{ d.run_date }} | {{ money(k.revenue_day) }} |
| Revenue month to date | {{ money(k.revenue_mtd) }} |
| Gross margin month to date | {{ money(k.margin_mtd) }} ({{ "%.1f"|format(k.margin_pct_mtd) }}%) |
| AR over 90 days | {{ money(k.ar_over_90) }} of {{ money(k.ar_total) }} open |
| Data quality pass rate | {{ "%.2f"|format(k.dq_pass_rate) }}% ({{ k.rows_quarantined }} of {{ k.rows_checked }} rows quarantined) |

## Top anomalies
{% if d.anomalies %}{% for a in d.anomalies %}{{ loop.index }}. {{ a }}
{% endfor %}{% else %}No anomalies were flagged for this batch.
{% endif %}
## Top 5 month-over-month movers ({{ d.run_date.strftime('%B %Y') }})
| Rank | Account | Type | This month | Prior month | Change | Change % |
|---|---|---|---|---|---|---|
{% for _, m in d.movers.iterrows() %}| {{ m.mom_mover_rank }} | {{ m.account_code }} {{ m.account_name }} | {{ m.account_type }} | {{ money(m.net_usd) }} | {{ money(m.prior_month_usd) }} | {{ money(m.mom_change_usd) }} | {{ pct(m.mom_change_pct) }} |
{% endfor %}
---

## Technical appendix
Batch `{{ d.batch_id }}`

### Stage timings and row counts
| Stage | Status | Rows in | Rows out | Seconds |
|---|---|---|---|---|
{% for _, t in d.stage_timings.iterrows() %}| {{ t.stage }} | {{ t.status }} | {{ num(t.rows_in) }} | {{ num(t.rows_out) }} | {{ t.duration_seconds }} |
{% endfor %}
### Reconciliation
raw {{ r.raw_rows }} rows = curated {{ r.curated_rows }} + quarantined {{ r.quarantined_rows }} + duplicates removed {{ r.duplicates_removed }} (gap {{ r.row_gap }}); amount gap {{ r.amount_gap }}. **Reconciled: {{ r.reconciled }}**

### Quarantine reasons
{% if d.quarantine_reasons.empty %}Nothing quarantined.{% else %}| Rule | Rows |
|---|---|
{% for _, q in d.quarantine_reasons.iterrows() %}| {{ q.rule_id }} | {{ q.rows }} |
{% endfor %}{% endif %}

### Rules that did not pass
{% if d.rule_failures.empty %}All rules passed.{% else %}| Rule | Severity | Rows failed | Rate | Description |
|---|---|---|---|---|
{% for _, f in d.rule_failures.iterrows() %}| {{ f.rule_id }} | {{ f.severity }} | {{ f.rows_failed }} | {{ "%.4f"|format(f.failure_rate) }} | {{ f.description }} |
{% endfor %}{% endif %}
"""

HTML_TEMPLATE = """<!doctype html><html><head><meta charset="utf-8"><title>Daily Financial Summary {{ run_date }}</title>
<style>body{font-family:Segoe UI,Helvetica,Arial,sans-serif;max-width:980px;margin:32px auto;color:#222;line-height:1.45}
table{border-collapse:collapse;margin:8px 0 18px}th,td{border:1px solid #ddd;padding:6px 10px;font-size:14px}th{background:#f3f6fa;text-align:left}
h1{color:#1f4e79}h2{border-bottom:2px solid #1f4e79;padding-bottom:4px;margin-top:32px}hr{margin:40px 0;border:0;border-top:3px dashed #999}
img{max-width:100%;border:1px solid #eee;margin:8px 0}code{background:#f3f3f3;padding:2px 4px}</style></head><body>
{{ business|safe }}
<h2>Charts</h2>
<img src="{{ charts.revenue }}" alt="Revenue trend"><img src="{{ charts.aging }}" alt="AR aging"><img src="{{ charts.dq }}" alt="DQ pass rate">
{{ technical|safe }}
</body></html>"""


def render_markdown(data: SummaryData) -> str:
    """Render the Markdown summary."""
    env = Environment(autoescape=select_autoescape(default=False))
    return env.from_string(MD_TEMPLATE).render(
        d=data, k=data.kpis, r=data.reconciliation, money=_money, pct=_pct, num=_num
    )


def render_html(data: SummaryData, markdown_text: str) -> str:
    """Render the HTML summary with embedded charts between the business and technical sections."""
    business_md, technical_md = markdown_text.split("\n---\n", 1)

    def conv(t: str) -> str:
        return str(md.markdown(t, extensions=["tables"]))  # type: ignore[no-untyped-call]

    env = Environment(autoescape=select_autoescape(default=False))
    return env.from_string(HTML_TEMPLATE).render(
        run_date=data.run_date,
        business=conv(business_md),
        technical=conv(technical_md),
        charts={k: charts.embed_png(p) for k, p in data.chart_paths.items()},
    )


def build_summary(
    settings: Settings,
    engine: Engine,
    batch_id: str,
    run_date: date,
    reports: list[ReportResult],
    *,
    storage: Storage | None = None,
) -> dict[str, Path]:
    """Build Markdown + HTML summaries, write them to ./out/<run_date>/ and the curated layer."""
    storage = storage or get_storage(settings)
    data = gather_summary_data(settings, engine, batch_id, run_date, {r.meta.name: r for r in reports})
    md_text = render_markdown(data)
    html_text = render_html(data, md_text)
    out_dir = settings.paths.out_dir / run_date.isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path = out_dir / "summary.md"
    html_path = out_dir / "summary.html"
    md_path.write_text(md_text, encoding="utf-8")
    html_path.write_text(html_text, encoding="utf-8")
    storage.write_bytes("curated", f"reports/{run_date.isoformat()}/summary.md", md_text.encode())
    storage.write_bytes("curated", f"reports/{run_date.isoformat()}/summary.html", html_text.encode())
    return {"markdown": md_path, "html": html_path, **data.chart_paths}
