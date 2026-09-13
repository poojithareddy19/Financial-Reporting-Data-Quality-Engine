from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd

from fin_dq_engine.quality.anomaly import (
    benford_pvalue,
    detect_benford,
    detect_fx_jumps,
    detect_isolation_forest,
    detect_revenue_outliers,
    flags_to_frame,
)


def _daily(spike: bool) -> pd.DataFrame:
    rng = np.random.default_rng(0)
    days = [date(2025, 1, 1) + timedelta(days=i) for i in range(40)]
    rev = rng.normal(10_000, 500, len(days))
    if spike:
        rev[-1] *= 4
    return pd.DataFrame({"entity_id": "ENT-01", "day": days, "revenue": rev})


def test_revenue_spike_detected() -> None:
    flags = detect_revenue_outliers(_daily(True), {date(2025, 2, 9)}, 28, 3.0, 3.5)
    assert len(flags) == 1
    assert flags[0].method == "revenue_zscore_mad"
    assert flags[0].details["ratio_to_mean"] > 3.5


def test_revenue_normal_not_flagged() -> None:
    assert detect_revenue_outliers(_daily(False), {date(2025, 2, 9)}, 28, 3.0, 3.5) == []


def test_revenue_needs_history() -> None:
    small = _daily(True).tail(15)  # fewer than 75% of the 28-day window
    assert detect_revenue_outliers(small, {date(2025, 2, 9)}, 28, 3.0, 3.5) == []


def test_benford_conforming_and_uniform() -> None:
    rng = np.random.default_rng(1)
    conforming = pd.Series(np.exp(rng.uniform(0, 12, 5000)))  # log-uniform follows Benford
    _, p_ok, _ = benford_pvalue(conforming)
    uniform = pd.Series(rng.uniform(500, 599, 5000))  # all start with 5
    _, p_bad, _ = benford_pvalue(uniform)
    assert p_ok > 0.001
    assert p_bad < 1e-6
    monthly = pd.DataFrame({"entity_id": "E", "month": "2025-01", "amount_usd": uniform})
    flags = detect_benford(monthly, 0.001, 300)
    assert len(flags) == 1 and flags[0].method == "benford_first_digit"
    assert detect_benford(monthly.head(100), 0.001, 300) == []


def test_fx_jump() -> None:
    rates = pd.DataFrame(
        {
            "currency": ["EUR", "EUR", "GBP", "GBP"],
            "rate_date": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 1), date(2025, 1, 2)],
            "rate_to_usd": [1.10, 1.25, 1.30, 1.31],
        }
    )
    flags = detect_fx_jumps(rates, date(2025, 1, 2), 5.0)
    assert [f.subject for f in flags] == ["EUR"]
    assert flags[0].details["change_pct"] > 13
    assert detect_fx_jumps(rates, date(2025, 1, 1), 5.0) == []  # no prior day


def test_isolation_forest_flags_extreme_amount() -> None:
    rng = np.random.default_rng(2)
    n = 600
    base = pd.DataFrame(
        {
            "transaction_id": [f"T{i}" for i in range(n)],
            "entity_id": "ENT-01",
            "amount_usd": rng.normal(500, 50, n),
            "created_at": pd.Timestamp("2025-01-01 10:00", tz="UTC") + pd.to_timedelta(rng.integers(0, 8, n), unit="h"),
            "account_type": "revenue",
            "customer_since": pd.Timestamp("2022-01-01"),
        }
    )
    score = base.head(20).copy()
    score.loc[score.index[0], "amount_usd"] = 5_000_000.0
    score.loc[score.index[0], "created_at"] = pd.Timestamp("2025-01-02 03:00", tz="UTC")
    flags = detect_isolation_forest(base, score, date(2025, 1, 2), 0.05, 200)
    assert any(f.subject == "T0" for f in flags)
    assert detect_isolation_forest(base.head(10), score, date(2025, 1, 2), 0.05, 200) == []
    assert not flags_to_frame(flags).empty
