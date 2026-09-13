from __future__ import annotations

from datetime import date

import pandas as pd

from fin_dq_engine.transform.clean import cast_types, deduplicate


def _raw() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transaction_id": ["TXN-1", " TXN-2 ", "", "TXN-1"],
            "event_id": ["E1", "E2", "E3", "E1"],
            "event_type": ["sale"] * 4,
            "reference_id": ["R"] * 4,
            "posted_date": ["2025-01-01", "not-a-date", "2025-01-03", "2025-01-01"],
            "created_at": ["2025-01-01 10:00:00", "", "2025-01-03 09:00:00", "2025-01-01 10:00:00"],
            "entity_id": ["ENT-01"] * 4,
            "account_id": ["ACC-1000"] * 4,
            "customer_id": ["C1", None, "", "C1"],
            "currency": ["usd", "EUR", "gbp", "usd"],
            "amount_local": ["10.0051", "abc", "-3", "10.0051"],
            "description": ["a", "b", "c", "a"],
            "source_system": ["ERP"] * 4,
        }
    )


def test_cast_types() -> None:
    df = cast_types(_raw())
    assert df["transaction_id"].tolist()[1] == "TXN-2"
    assert df["transaction_id"].tolist()[2] is None
    assert df["posted_date"].tolist()[0] == date(2025, 1, 1)
    assert df["posted_date"].tolist()[1] is None
    assert df["currency"].tolist() == ["USD", "EUR", "GBP", "USD"]
    assert df["amount_local"].tolist()[0] == 10.01  # rounded to 2dp
    assert pd.isna(df["amount_local"].tolist()[1])
    assert df["customer_id"].tolist()[2] is None
    assert df["created_at"].tolist()[1] is None


def test_deduplicate_keeps_first_and_ignores_null_keys() -> None:
    df = cast_types(_raw())
    res = deduplicate(df)
    assert res.duplicates_removed == 1
    assert len(res.frame) == 3
    assert res.frame["transaction_id"].isna().sum() == 1  # null-key row retained for the not_null rule
