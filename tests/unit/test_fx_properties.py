"""Property-based tests for FX normalisation."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
from hypothesis import given, settings
from hypothesis import strategies as st

from fin_dq_engine.transform.clean import normalize_fx

CCY = ["USD", "EUR", "GBP", "JPY"]


def _rates(start: date, days: int) -> pd.DataFrame:
    rows = []
    for d in range(days):
        for c in CCY:
            rows.append(
                {
                    "currency": c,
                    "rate_date": start + timedelta(days=d),
                    "rate_to_usd": 1.0 if c == "USD" else {"EUR": 1.1, "GBP": 1.3, "JPY": 0.007}[c] * (1 + 0.001 * d),
                }
            )
    return pd.DataFrame(rows)


amounts = st.floats(min_value=-1e7, max_value=1e7, allow_nan=False, allow_infinity=False).map(lambda x: round(x, 2))


@settings(max_examples=60, deadline=None)
@given(st.lists(st.tuples(st.sampled_from(CCY), st.integers(0, 29), amounts), min_size=1, max_size=40))
def test_usd_amount_equals_local_times_rate(rows: list[tuple[str, int, float]]) -> None:
    start = date(2025, 1, 1)
    rates = _rates(start, 30)
    df = pd.DataFrame(
        {
            "currency": [r[0] for r in rows],
            "posted_date": [start + timedelta(days=r[1]) for r in rows],
            "amount_local": [r[2] for r in rows],
        }
    )
    out = normalize_fx(df, rates)
    assert len(out) == len(df)
    assert out["fx_rate"].notna().all()
    expected = (out["amount_local"] * out["fx_rate"]).round(2)
    assert np.allclose(out["amount_usd"].astype(float), expected.astype(float), atol=0.011)
    assert (out.loc[out["currency"] == "USD", "fx_rate"] == 1.0).all()
    # sign is preserved unless the USD amount rounds to zero
    usd = out["amount_usd"].fillna(0)
    assert ((np.sign(usd) == np.sign(out["amount_local"].fillna(0))) | (usd == 0)).all()


@settings(max_examples=30, deadline=None)
@given(st.integers(1, 14))
def test_as_of_join_uses_latest_rate_on_or_before(gap_days: int) -> None:
    start = date(2025, 1, 1)
    rates = _rates(start, 5)  # rates only for Jan 1 to Jan 5
    posted = start + timedelta(days=4 + gap_days)
    out = normalize_fx(pd.DataFrame({"currency": ["EUR"], "posted_date": [posted], "amount_local": [100.0]}), rates)
    last_rate = rates[(rates.currency == "EUR") & (rates.rate_date == start + timedelta(days=4))]["rate_to_usd"].iloc[0]
    assert out["fx_rate"].iloc[0] == last_rate


def test_missing_rate_gives_null() -> None:
    rates = _rates(date(2025, 1, 10), 3)
    out = normalize_fx(
        pd.DataFrame(
            {
                "currency": ["EUR", "XXX", None],
                "posted_date": [date(2025, 1, 1), date(2025, 1, 11), None],
                "amount_local": [10.0, 5.0, 1.0],
            }
        ),
        rates,
    )
    assert out["fx_rate"].isna().all()
    assert out["amount_usd"].isna().all()


def test_order_and_index_preserved() -> None:
    rates = _rates(date(2025, 1, 1), 3)
    df = pd.DataFrame(
        {
            "currency": ["GBP", "USD", "EUR"],
            "posted_date": [date(2025, 1, 3), date(2025, 1, 1), date(2025, 1, 2)],
            "amount_local": [1.0, 2.0, 3.0],
        },
        index=[10, 20, 30],
    )
    out = normalize_fx(df, rates)
    assert list(out.index) == [10, 20, 30]
    assert list(out["currency"]) == ["GBP", "USD", "EUR"]
