"""Streamlit dashboard: KPIs, DQ scorecard and a quarantine explorer over the curated / dq schemas.

Run with ``make dashboard`` (uses config/settings.yaml, override the URL with FIN_DQ__DATABASE__URL).
"""

from __future__ import annotations

from datetime import timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from fin_dq_engine.config import load_settings
from fin_dq_engine.db import get_engine
from fin_dq_engine.governance.pii import log_pii_access, mask_frame, resolve_key

st.set_page_config(page_title="fin-dq engine", layout="wide")
settings = load_settings()
engine = get_engine(settings)


_PII_KEY = resolve_key(settings.governance.pii_hash_key)


@st.cache_data(ttl=60)
def q(sql: str, **params: object) -> pd.DataFrame:
    """Run a query, masking PII and logging the access before anything reaches the browser.

    Every result goes through mask_frame rather than only the queries that select PII today, so a
    future query that adds customer_name cannot quietly bypass the controls the report path enforces.
    Results are cached for 60s, so the access log gets one row per cache miss, not per page view.
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(text(sql), conn, params=params)
        masked, cols, was_masked = mask_frame(df, settings.governance.pii_columns, settings.role, _PII_KEY)
        if cols:
            with conn.begin():
                log_pii_access(
                    conn,
                    actor=settings.actor,
                    role=settings.role,
                    report_name="dashboard",
                    columns=cols,
                    masked=was_masked,
                )
    return masked


st.title("Financial Reporting & Data Quality Engine")
latest = q("SELECT max(run_date) AS d FROM dq.batch_run_log WHERE stage = 'load' AND status = 'success'").d[0]
if latest is None:
    st.warning("No completed runs yet. Run `make seed && make run` first.")
    st.stop()
run_date = st.sidebar.date_input("As of", value=latest)
window = st.sidebar.slider("Revenue window (days)", 14, 180, 60)

kpi = q(
    """
    SELECT -SUM(f.amount_usd) FILTER (WHERE a.account_type = 'revenue') AS revenue,
            SUM(f.amount_usd) FILTER (WHERE a.account_type = 'cogs') AS cogs
    FROM curated.fact_transactions f JOIN curated.dim_account a USING (account_id)
    WHERE f.posted_date BETWEEN :s AND :e""",
    s=run_date - timedelta(days=window),
    e=run_date,
)
sc = q(
    """
    SELECT (details->'scorecard'->>'dq_pass_rate')::numeric AS pass_rate,
           (details->'scorecard'->>'rows_quarantined')::int AS quarantined
    FROM dq.batch_run_log WHERE stage = 'validate' AND run_date = :d ORDER BY log_id DESC LIMIT 1""",
    d=run_date,
)
ar = q(
    """
    WITH legs AS (SELECT reference_id, MIN(posted_date) FILTER (WHERE amount_usd > 0) AS inv, SUM(amount_usd) AS open_usd
                  FROM curated.fact_transactions f JOIN curated.dim_account a USING (account_id)
                  WHERE a.account_code = '1100' AND posted_date <= :d GROUP BY reference_id HAVING SUM(amount_usd) > 0.005)
    SELECT COALESCE(SUM(open_usd) FILTER (WHERE :d - inv > 90), 0) AS over_90, COALESCE(SUM(open_usd), 0) AS total FROM legs""",
    d=run_date,
)

c1, c2, c3, c4 = st.columns(4)
rev = float(kpi.revenue[0] or 0)
margin = rev - float(kpi.cogs[0] or 0)
c1.metric(f"Revenue ({window}d)", f"${rev:,.0f}")
c2.metric("Gross margin", f"${margin:,.0f}", f"{(margin / rev * 100) if rev else 0:.1f}%")
c3.metric("AR over 90 days", f"${float(ar.over_90[0]):,.0f}", f"of ${float(ar.total[0]):,.0f} open")
c4.metric(
    "DQ pass rate",
    f"{float(sc.pass_rate[0]) * 100:.2f}%" if not sc.empty else "n/a",
    f"{int(sc.quarantined[0])} quarantined" if not sc.empty else None,
)

tab_rev, tab_dq, tab_q, tab_anom = st.tabs(["Revenue", "DQ scorecard", "Quarantine explorer", "Anomalies"])

with tab_rev:
    daily = q(
        """
        SELECT f.posted_date, e.entity_code, -SUM(f.amount_usd) AS revenue_usd
        FROM curated.fact_transactions f JOIN curated.dim_account a USING (account_id)
        JOIN curated.dim_entity e USING (entity_id)
        WHERE a.account_type = 'revenue' AND f.posted_date BETWEEN :s AND :e GROUP BY 1, 2 ORDER BY 1""",
        s=run_date - timedelta(days=window),
        e=run_date,
    )
    st.plotly_chart(
        px.line(daily, x="posted_date", y="revenue_usd", color="entity_code", title="Daily revenue by entity"),
        use_container_width=True,
    )

with tab_dq:
    scorecard = q(
        """
        SELECT r.rule_id, g.description, r.dimension, r.severity, COUNT(*) AS runs,
               COUNT(*) FILTER (WHERE r.status = 'pass') AS passes, SUM(r.rows_failed) AS rows_failed
        FROM dq.rule_results r JOIN LATERAL (SELECT description FROM dq.rule_registry g WHERE g.rule_id = r.rule_id
             ORDER BY is_active DESC, loaded_at DESC LIMIT 1) g ON TRUE
        WHERE r.run_date <= :d GROUP BY 1, 2, 3, 4 ORDER BY r.severity, rows_failed DESC""",
        d=run_date,
    )
    st.dataframe(scorecard, use_container_width=True, hide_index=True)
    history = q(
        """
        SELECT run_date, MAX((details->'scorecard'->>'dq_pass_rate')::numeric) * 100 AS pass_rate_pct
        FROM dq.batch_run_log WHERE stage = 'validate' AND details ? 'scorecard' AND run_date <= :d
        GROUP BY 1 ORDER BY 1 DESC LIMIT 30""",
        d=run_date,
    )
    fig = px.line(
        history.sort_values("run_date"),
        x="run_date",
        y="pass_rate_pct",
        title="DQ pass rate, last 30 runs",
        markers=True,
    )
    fig.add_hline(y=95, line_dash="dash", line_color="red")
    st.plotly_chart(fig, use_container_width=True)

with tab_q:
    rules = q("SELECT DISTINCT unnest(rule_ids) AS rule_id FROM dq.quarantine ORDER BY 1").rule_id.tolist()
    f1, f2, f3 = st.columns(3)
    rule = f1.selectbox("Rule", ["(all)", *rules])
    entity = f2.selectbox(
        "Entity", ["(all)", *q("SELECT entity_id FROM curated.dim_entity ORDER BY 1").entity_id.tolist()]
    )
    status = f3.selectbox("Status", ["open", "released", "all"])
    rows = q(
        """
        SELECT quarantine_id, batch_id, run_date, transaction_id, rule_ids, row_data->>'entity_id' AS entity_id,
               row_data->>'account_id' AS account_id, row_data->>'currency' AS currency, row_data->>'amount_local' AS amount_local,
               row_data->>'posted_date' AS posted_date, quarantined_at, released_at, released_by
        FROM dq.quarantine
        WHERE (:rule = '(all)' OR :rule = ANY(rule_ids))
          AND (:entity = '(all)' OR row_data->>'entity_id' = :entity)
          AND (:status = 'all' OR (:status = 'open' AND released_at IS NULL) OR (:status = 'released' AND released_at IS NOT NULL))
        ORDER BY quarantined_at DESC LIMIT 2000""",
        rule=rule,
        entity=entity,
        status=status,
    )
    st.caption(f"{len(rows):,} rows")
    st.dataframe(rows, use_container_width=True, hide_index=True)
    if not rows.empty:
        st.download_button("Download CSV", rows.to_csv(index=False).encode(), "quarantine.csv", "text/csv")

with tab_anom:
    flags = q(
        """SELECT run_date, method, entity_id, subject, score, threshold, details FROM dq.anomaly_flags
                 WHERE run_date <= :d ORDER BY run_date DESC, score DESC LIMIT 500""",
        d=run_date,
    )
    st.dataframe(flags, use_container_width=True, hide_index=True)
