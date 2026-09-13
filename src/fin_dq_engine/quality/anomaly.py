"""Statistical anomaly detection: z-score / rolling MAD, Isolation Forest, Benford, FX jumps.

The ``detect_*`` functions are pure (DataFrame in, flags out) so they are unit-testable; the
``run_anomaly_detection`` function gathers inputs from the warehouse and persists flags.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import Connection

from fin_dq_engine.config import AnomalyConfig
from fin_dq_engine.db import insert_rows, jsonb, query_df

BENFORD_EXPECTED = np.log10(1 + 1 / np.arange(1, 10))


@dataclass(frozen=True)
class AnomalyFlag:
    """One anomaly to persist in dq.anomaly_flags."""

    method: str
    entity_id: str | None
    subject: str
    score: float
    threshold: float
    details: dict[str, Any]


def detect_revenue_outliers(
    daily: pd.DataFrame,
    target_dates: set[date],
    window: int,
    z_threshold: float,
    mad_threshold: float,
) -> list[AnomalyFlag]:
    """Flag days whose revenue deviates from the trailing window by z-score or robust MAD score.

    Args:
        daily: columns ``entity_id, day, revenue`` (one row per entity per day).
        target_dates: days to score (the pipeline passes the run date).
        window: trailing days used for the baseline (the scored day is excluded); at least 75% coverage is required.
    """
    flags: list[AnomalyFlag] = []
    if daily.empty:
        return flags
    daily = daily.sort_values(["entity_id", "day"])
    for entity, grp in daily.groupby("entity_id"):
        series = grp.set_index("day")["revenue"].astype(float)
        for day in sorted(target_dates):
            if day not in series.index:
                continue
            hist = series[(series.index < day) & (series.index >= day - timedelta(days=window))]
            if len(hist) < max(7, int(window * 0.75)):  # need a real baseline, not the first week after go-live
                continue
            value = float(series[day])
            mean, std = float(hist.mean()), float(hist.std(ddof=0))
            median = float(hist.median())
            mad = float(np.median(np.abs(hist - median))) or 1e-9
            z = (value - mean) / std if std > 0 else 0.0
            robust = 0.6745 * (value - median) / mad
            if abs(z) >= z_threshold or abs(robust) >= mad_threshold:
                flags.append(
                    AnomalyFlag(
                        "revenue_zscore_mad",
                        str(entity),
                        day.isoformat(),
                        round(max(abs(z), abs(robust)), 4),
                        min(z_threshold, mad_threshold),
                        {
                            "revenue": round(value, 2),
                            "baseline_mean": round(mean, 2),
                            "baseline_median": round(median, 2),
                            "zscore": round(z, 4),
                            "mad_score": round(robust, 4),
                            "window_days": window,
                            "ratio_to_mean": round(value / mean, 2) if mean else None,
                        },
                    )
                )
    return flags


def _iforest_features(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    type_codes = {"asset": 0, "liability": 1, "equity": 2, "revenue": 3, "cogs": 4, "expense": 5}
    out = pd.DataFrame(index=df.index)
    out["log_amount"] = np.log1p(df["amount_usd"].abs().astype(float).fillna(0))
    created = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    out["hour"] = created.dt.hour.fillna(12).astype(float)
    out["account_type"] = df["account_type"].map(type_codes).fillna(-1).astype(float)
    since = pd.to_datetime(df["customer_since"], errors="coerce")
    tenure = (pd.Timestamp(run_date) - since).dt.days
    out["tenure_days"] = tenure.fillna(tenure.median() if tenure.notna().any() else 0).astype(float)
    return out


def detect_isolation_forest(
    train: pd.DataFrame,
    score: pd.DataFrame,
    run_date: date,
    contamination: float,
    min_rows: int,
    seed: int = 7,
) -> list[AnomalyFlag]:
    """Train an Isolation Forest on trailing history and flag outliers in the current batch.

    Both frames need ``transaction_id, amount_usd, created_at, account_type, customer_since, entity_id``.
    """
    if len(train) < min_rows or score.empty:
        return []
    from sklearn.ensemble import IsolationForest

    model = IsolationForest(n_estimators=100, contamination=contamination, random_state=seed)
    model.fit(_iforest_features(train, run_date))
    feats = _iforest_features(score, run_date)
    scores = -model.score_samples(feats)  # higher = more anomalous
    preds = model.predict(feats)
    threshold = float(-model.offset_)
    flags: list[AnomalyFlag] = []
    for idx in np.where(preds == -1)[0]:
        row = score.iloc[idx]
        flags.append(
            AnomalyFlag(
                "isolation_forest",
                str(row.get("entity_id")),
                str(row["transaction_id"]),
                round(float(scores[idx]), 4),
                round(threshold, 4),
                {
                    "amount_usd": float(row["amount_usd"]) if pd.notna(row["amount_usd"]) else None,
                    "hour": float(feats.iloc[idx]["hour"]),
                    "account_type": str(row.get("account_type")),
                    "tenure_days": float(feats.iloc[idx]["tenure_days"]),
                    "training_rows": len(train),
                },
            )
        )
    return flags


def benford_pvalue(amounts: pd.Series) -> tuple[float, float, np.ndarray]:
    """Chi-square test of first digits against Benford's law. Returns (statistic, p_value, observed_freq)."""
    vals = amounts.abs().astype(float)
    vals = vals[vals >= 1]
    if vals.empty:
        return 0.0, 1.0, np.zeros(9)
    first = vals.map(lambda v: int(f"{v:.6e}"[0])).astype(int)
    observed = np.bincount(first, minlength=10)[1:10].astype(float)
    expected = BENFORD_EXPECTED * observed.sum()
    stat, p = stats.chisquare(observed, expected)
    return float(stat), float(p), observed / observed.sum()


def detect_benford(monthly: pd.DataFrame, p_threshold: float, min_rows: int) -> list[AnomalyFlag]:
    """Flag entity-months whose amount first-digit distribution rejects Benford's law.

    ``monthly`` needs ``entity_id, month (YYYY-MM), amount_usd``.
    """
    flags: list[AnomalyFlag] = []
    for (entity, month), grp in monthly.groupby(["entity_id", "month"]):
        if len(grp) < min_rows:
            continue
        stat, p, observed = benford_pvalue(grp["amount_usd"])
        if p < p_threshold:
            flags.append(
                AnomalyFlag(
                    "benford_first_digit",
                    str(entity),
                    str(month),
                    round(p, 6),
                    p_threshold,
                    {
                        "chi_square": round(stat, 2),
                        "rows": len(grp),
                        "observed": [round(float(x), 4) for x in observed],
                        "expected": [round(float(x), 4) for x in BENFORD_EXPECTED],
                    },
                )
            )
    return flags


def detect_fx_jumps(rates: pd.DataFrame, run_date: date, jump_pct: float) -> list[AnomalyFlag]:
    """Flag currencies whose rate on run_date moved more than ``jump_pct`` versus the prior rate.

    ``rates`` needs ``currency, rate_date, rate_to_usd`` covering at least the prior available day.
    """
    flags: list[AnomalyFlag] = []
    rates = rates.copy()
    rates["rate_date"] = pd.to_datetime(rates["rate_date"]).dt.date
    for ccy, grp in rates.sort_values("rate_date").groupby("currency"):
        grp = grp[grp["rate_date"] <= run_date]
        if len(grp) < 2:
            continue
        today, prior = grp.iloc[-1], grp.iloc[-2]
        if today["rate_date"] != run_date:
            continue
        change = (float(today["rate_to_usd"]) / float(prior["rate_to_usd"]) - 1) * 100
        if abs(change) > jump_pct:
            flags.append(
                AnomalyFlag(
                    "fx_rate_jump",
                    None,
                    str(ccy),
                    round(abs(change), 4),
                    jump_pct,
                    {
                        "rate": float(today["rate_to_usd"]),
                        "prior_rate": float(prior["rate_to_usd"]),
                        "prior_date": prior["rate_date"].isoformat(),
                        "change_pct": round(change, 4),
                    },
                )
            )
    return flags


def run_anomaly_detection(conn: Connection, cfg: AnomalyConfig, batch_id: str, run_date: date) -> list[AnomalyFlag]:
    """Gather inputs from staging + curated, run every detector, persist and return the flags."""
    flags: list[AnomalyFlag] = []
    window = cfg.revenue_window_days

    daily = query_df(
        conn,
        """
        WITH combined AS (
            SELECT entity_id, posted_date, amount_usd FROM curated.fact_transactions f
            JOIN curated.dim_account a USING (account_id)
            WHERE a.account_type = 'revenue' AND f.posted_date BETWEEN :start AND :end AND f.batch_id <> :batch_id
            UNION ALL
            SELECT entity_id, posted_date, amount_usd FROM staging.transactions s
            JOIN curated.dim_account a USING (account_id)
            WHERE a.account_type = 'revenue' AND s.batch_id = :batch_id AND NOT s.quarantined
              AND s.posted_date BETWEEN :start AND :end
        )
        SELECT entity_id, posted_date AS day, -sum(amount_usd) AS revenue FROM combined GROUP BY 1, 2
    """,
        {"start": run_date - timedelta(days=window + 15), "end": run_date, "batch_id": batch_id},
    )
    targets = {run_date}  # score the day being closed; late-arriving days are visible in the report's moving averages
    flags += detect_revenue_outliers(daily, targets, window, cfg.zscore_threshold, cfg.mad_threshold)

    feature_sql = """
        SELECT x.transaction_id, x.entity_id, x.amount_usd, x.created_at, a.account_type, c.customer_since
        FROM {src} x
        JOIN curated.dim_account a USING (account_id)
        LEFT JOIN curated.dim_customer c ON c.customer_id = x.customer_id AND c.is_current
        WHERE {where}"""
    train = query_df(
        conn,
        feature_sql.format(
            src="curated.fact_transactions",
            where="x.posted_date BETWEEN :start AND :end AND x.batch_id <> :batch_id",
        ),
        {
            "start": run_date - timedelta(days=cfg.isolation_forest_training_days),
            "end": run_date,
            "batch_id": batch_id,
        },
    )
    score = query_df(
        conn,
        feature_sql.format(
            src="staging.transactions",
            where="x.batch_id = :batch_id AND NOT x.quarantined AND x.amount_usd IS NOT NULL",
        ),
        {"batch_id": batch_id},
    )
    if len(train) > 50_000:
        train = train.sample(50_000, random_state=1)
    flags += detect_isolation_forest(
        train, score, run_date, cfg.isolation_forest_contamination, cfg.isolation_forest_min_rows
    )

    monthly = query_df(
        conn,
        """
        SELECT entity_id, to_char(posted_date, 'YYYY-MM') AS month, amount_usd FROM (
            SELECT entity_id, posted_date, amount_usd FROM curated.fact_transactions
            WHERE posted_date >= :month_start AND posted_date <= :end AND batch_id <> :batch_id
            UNION ALL
            SELECT entity_id, posted_date, amount_usd FROM staging.transactions
            WHERE batch_id = :batch_id AND NOT quarantined AND amount_usd IS NOT NULL
              AND posted_date >= :month_start AND posted_date <= :end) u
    """,
        {"month_start": run_date.replace(day=1), "end": run_date, "batch_id": batch_id},
    )
    flags += detect_benford(monthly, cfg.benford_p_value, cfg.benford_min_rows)

    rates = query_df(
        conn,
        "SELECT currency, rate_date, rate_to_usd FROM curated.dim_fx_rate WHERE rate_date BETWEEN :start AND :end",
        {"start": run_date - timedelta(days=10), "end": run_date},
    )
    flags += detect_fx_jumps(rates, run_date, cfg.fx_jump_pct)

    insert_rows(
        conn,
        "dq.anomaly_flags",
        [
            {
                "batch_id": batch_id,
                "run_date": run_date,
                "method": f.method,
                "entity_id": f.entity_id,
                "subject": f.subject,
                "score": f.score,
                "threshold": f.threshold,
                "details": jsonb(f.details),
            }
            for f in flags
        ],
    )
    return flags


def flags_to_frame(flags: list[AnomalyFlag]) -> pd.DataFrame:
    """Convenience for tests and rendering."""
    return pd.DataFrame([asdict(f) for f in flags])
