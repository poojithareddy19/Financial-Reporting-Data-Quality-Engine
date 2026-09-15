"""Pure pandas transformations: typing, deduplication and FX normalisation.

These functions have no I/O so they can be property-tested (see tests/unit/test_fx_properties.py).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

TEXT_COLUMNS = [
    "transaction_id",
    "event_id",
    "event_type",
    "reference_id",
    "entity_id",
    "account_id",
    "customer_id",
    "currency",
    "description",
    "source_system",
]


@dataclass(frozen=True)
class DedupResult:
    """Deduplicated frame and how many rows were dropped."""

    frame: pd.DataFrame
    duplicates_removed: int


def cast_types(raw: pd.DataFrame) -> pd.DataFrame:
    """Cast raw TEXT columns to typed columns. Unparseable values become null (caught by rules)."""
    df = raw.copy()
    for col in TEXT_COLUMNS:
        if col in df.columns:
            s = df[col].astype("string").str.strip()
            df[col] = s.astype(object).where((s.notna() & (s != "")).to_numpy(), None)
    df["currency"] = df["currency"].map(lambda v: v.upper() if isinstance(v, str) else None)
    df["posted_date"] = pd.to_datetime(df["posted_date"], errors="coerce", format="%Y-%m-%d").dt.date
    df["posted_date"] = df["posted_date"].astype(object).where(df["posted_date"].notna(), None)
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    df["created_at"] = df["created_at"].astype(object).where(df["created_at"].notna(), None)
    df["amount_local"] = pd.to_numeric(df["amount_local"], errors="coerce").round(2)
    return df


def deduplicate(df: pd.DataFrame, key: str = "transaction_id") -> DedupResult:
    """Drop exact duplicates on the business key, keeping the first occurrence.

    Rows with a null key are never treated as duplicates of each other; they are left for the
    not_null rule to quarantine.
    """
    has_key = df[key].notna()
    keyed = df[has_key].drop_duplicates(subset=[key], keep="first")
    out = pd.concat([keyed, df[~has_key]], ignore_index=True)
    return DedupResult(out, int(len(df) - len(out)))


def normalize_fx(df: pd.DataFrame, rates: pd.DataFrame, reporting_currency: str = "USD") -> pd.DataFrame:
    """Attach ``fx_rate`` (as-of the posted_date, per currency) and compute ``amount_usd``.

    Args:
        df: Typed transactions with ``currency``, ``posted_date``, ``amount_local``.
        rates: ``currency, rate_date, rate_to_usd`` rows from dim_fx_rate.
        reporting_currency: Currency whose rate is fixed at 1.0.

    Rows whose currency has no rate on or before posted_date get a null fx_rate and amount_usd; the
    DQ-015 rule quarantines them.
    """
    out = df.copy()
    out["_row"] = np.arange(len(out))
    left = out[["_row", "currency", "posted_date"]].copy()
    left["posted_ts"] = pd.to_datetime(left["posted_date"], errors="coerce")
    r = rates.copy()
    r["rate_ts"] = pd.to_datetime(r["rate_date"])
    r["currency"] = r["currency"].astype(str)
    r = r.sort_values("rate_ts")
    left_ok = left[left["posted_ts"].notna() & left["currency"].notna()].sort_values("posted_ts")
    left_ok = left_ok.astype({"currency": str})
    if len(left_ok) and len(r):
        merged = pd.merge_asof(
            left_ok,
            r[["currency", "rate_ts", "rate_to_usd"]],
            left_on="posted_ts",
            right_on="rate_ts",
            by="currency",
            direction="backward",
        )
        rate_by_row = merged.set_index("_row")["rate_to_usd"]
    else:
        rate_by_row = pd.Series(dtype=float)
    out["fx_rate"] = out["_row"].map(rate_by_row).astype(float)
    out.loc[out["currency"] == reporting_currency, "fx_rate"] = 1.0
    out["amount_usd"] = (out["amount_local"].astype(float) * out["fx_rate"]).round(2)
    out.loc[out["fx_rate"].isna(), "amount_usd"] = np.nan
    return out.drop(columns=["_row"])
