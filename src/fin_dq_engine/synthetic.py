"""Seeded synthetic financial data with deliberately injected defects.

The generator produces double-entry GL postings (two legs per business event) so that the trial
balance genuinely balances on clean data and visibly breaks when a leg is quarantined. Roughly 1%
of rows carry a defect; every injected defect is recorded in ``_manifest.json`` so tests can assert
exact quarantine counts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

CURRENCIES = ["USD", "EUR", "GBP", "JPY"]
BASE_RATES = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067}
SEGMENTS = ["Enterprise", "Mid-Market", "SMB"]
REGIONS = ["North America", "EMEA", "APAC", "LATAM"]
ENTITIES = [
    ("ENT-01", "US01", "Northwind Holdings Inc", "US", "USD"),
    ("ENT-02", "UK01", "Northwind Trading Ltd", "GB", "GBP"),
    ("ENT-03", "DE01", "Northwind Europe GmbH", "DE", "EUR"),
    ("ENT-04", "JP01", "Northwind Japan KK", "JP", "JPY"),
    ("ENT-05", "US02", "Northwind Services LLC", "US", "USD"),
]

DEFECT_RATES: dict[str, float] = {
    "null_transaction_id": 0.0010,
    "null_posted_date": 0.0010,
    "null_amount": 0.0010,
    "sign_flip": 0.0015,
    "bad_date": 0.0010,
    "bad_currency": 0.0010,
    "orphan_account": 0.0008,
    "orphan_customer": 0.0008,
    "orphan_entity": 0.0004,
    "blank_description": 0.0010,
    "duplicate": 0.0015,
}


@dataclass
class GeneratorConfig:
    """Knobs for :func:`generate_dataset`."""

    seed: int = 42
    start_date: date = date(2024, 9, 1)
    months: int = 24
    n_events: int = 250_000
    n_customers: int = 300
    n_accounts: int = 120
    revenue_spike_days: int = 4
    fx_spike_days: int = 3
    late_arrival_share: float = 0.05
    defect_rates: dict[str, float] = field(default_factory=lambda: dict(DEFECT_RATES))

    @property
    def end_date(self) -> date:
        """Last ingestion date (inclusive)."""
        y, m = divmod(self.start_date.month - 1 + self.months, 12)
        return date(self.start_date.year + y, m + 1, 1) - timedelta(days=1)


@dataclass
class Dataset:
    """In-memory result of a generation run."""

    entities: pd.DataFrame
    accounts: pd.DataFrame
    customers: pd.DataFrame  # every version, with effective_date
    fx_rates: pd.DataFrame
    transactions: pd.DataFrame  # includes _file_date and _defect columns
    manifest: dict[str, Any]


def _build_accounts(n_accounts: int, rng: np.random.Generator) -> pd.DataFrame:
    fixed = [
        ("1000", "Cash and Cash Equivalents", "asset", "debit"),
        ("1100", "Accounts Receivable", "asset", "debit"),
        ("1200", "Inventory", "asset", "debit"),
    ]
    spec = [
        ("asset", "debit", 1300, 17, "Other Asset"),
        ("liability", "credit", 2000, 10, "Liability"),
        ("equity", "credit", 3000, 3, "Equity"),
        ("revenue", "credit", 4000, 20, "Revenue"),
        ("cogs", "debit", 5000, 10, "Cost of Goods Sold"),
        ("expense", "debit", 6000, 57, "Operating Expense"),
    ]
    rows = list(fixed)
    for acct_type, normal, base, count, label in spec:
        rows.extend((str(base + i), f"{label} {i + 1:02d}", acct_type, normal) for i in range(count))
    rows = rows[:n_accounts]
    df = pd.DataFrame(rows, columns=["account_code", "account_name", "account_type", "normal_balance"])
    df.insert(0, "account_id", [f"ACC-{c}" for c in df["account_code"]])
    return df


def _build_customers(cfg: GeneratorConfig, rng: np.random.Generator) -> pd.DataFrame:
    from faker import Faker

    fake = Faker()
    Faker.seed(cfg.seed)
    ids = [f"CUST-{i:05d}" for i in range(1, cfg.n_customers + 1)]
    since = [cfg.start_date - timedelta(days=int(d)) for d in rng.integers(30, 365 * 6, cfg.n_customers)]
    df = pd.DataFrame(
        {
            "customer_id": ids,
            "customer_name": [fake.company() for _ in ids],
            "customer_email": [fake.company_email() for _ in ids],
            "segment": rng.choice(SEGMENTS, cfg.n_customers, p=[0.2, 0.35, 0.45]),
            "region": rng.choice(REGIONS, cfg.n_customers, p=[0.4, 0.3, 0.2, 0.1]),
            "country": [fake.country_code() for _ in ids],
            "customer_since": since,
            "effective_date": cfg.start_date,
        }
    )
    # SCD2 changes: ~10% of customers change segment or region during the window.
    changers = rng.choice(cfg.n_customers, size=max(1, cfg.n_customers // 10), replace=False)
    span = (cfg.end_date - cfg.start_date).days
    changes = []
    for idx in changers:
        row = df.iloc[idx].copy()
        row["effective_date"] = cfg.start_date + timedelta(days=int(rng.integers(30, max(31, span))))
        if rng.random() < 0.5:
            row["segment"] = rng.choice([s for s in SEGMENTS if s != row["segment"]])
        else:
            row["region"] = rng.choice([r for r in REGIONS if r != row["region"]])
        changes.append(row)
    return pd.concat([df, pd.DataFrame(changes)], ignore_index=True)


def _build_fx(cfg: GeneratorConfig, rng: np.random.Generator) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    dates = pd.date_range(cfg.start_date - timedelta(days=14), cfg.end_date, freq="D")
    frames = []
    spikes: list[dict[str, Any]] = []
    for ccy in CURRENCIES:
        if ccy == "USD":
            rates = np.ones(len(dates))
        else:
            steps = rng.normal(0, 0.003, len(dates))
            rates = BASE_RATES[ccy] * np.exp(np.cumsum(steps))
            for _ in range(cfg.fx_spike_days):
                pos = int(rng.integers(30, len(dates) - 1))
                factor = float(rng.choice([0.88, 1.12, 1.15]))
                rates[pos] *= factor
                spikes.append({"currency": ccy, "date": dates[pos].date().isoformat(), "factor": factor})
        frames.append(
            pd.DataFrame(
                {
                    "currency": ccy,
                    "rate_date": dates.date,
                    "rate_to_usd": np.round(rates, 8),
                    "source": "synthetic-ecb",
                }
            )
        )
    return pd.concat(frames, ignore_index=True), spikes


def _build_transactions(
    cfg: GeneratorConfig,
    rng: np.random.Generator,
    accounts: pd.DataFrame,
    customers: pd.DataFrame,
    fx_rates: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    n = cfg.n_events
    file_dates = pd.date_range(cfg.start_date, cfg.end_date, freq="D")
    # Weekday-heavy intensity with a mild upward trend.
    weights = np.where(file_dates.dayofweek < 5, 1.0, 0.35) * np.linspace(0.9, 1.15, len(file_dates))
    weights = weights / weights.sum()
    file_idx = rng.choice(len(file_dates), size=n, p=weights)
    fdate = file_dates[file_idx]

    lag = np.where(rng.random(n) < cfg.late_arrival_share, rng.integers(1, 15, n), 0)
    posted = (fdate - pd.to_timedelta(lag, unit="D")).date

    entity_idx = rng.choice(len(ENTITIES), size=n, p=[0.35, 0.2, 0.2, 0.1, 0.15])
    entity_ids = np.array([e[0] for e in ENTITIES])[entity_idx]
    functional = np.array([e[4] for e in ENTITIES])[entity_idx]
    # 85% of postings in the entity's functional currency, else another governed currency.
    other = rng.choice(CURRENCIES, size=n)
    currency = np.where(rng.random(n) < 0.85, functional, other)

    event_type = rng.choice(["sale", "payment", "expense", "adjustment"], size=n, p=[0.5, 0.25, 0.2, 0.05])
    base_amt = np.round(np.exp(rng.normal(6.5, 1.0, n)), 2)  # lognormal, median ~665
    ccy_scale = np.where(currency == "JPY", 150.0, 1.0)
    amount = np.round(base_amt * ccy_scale, 0 if False else 2)

    revenue_accts = accounts.loc[accounts.account_type == "revenue", "account_id"].to_numpy()
    cogs_accts = accounts.loc[accounts.account_type == "cogs", "account_id"].to_numpy()
    expense_accts = accounts.loc[accounts.account_type == "expense", "account_id"].to_numpy()
    liability_accts = accounts.loc[accounts.account_type == "liability", "account_id"].to_numpy()
    cust_ids = customers.drop_duplicates("customer_id")["customer_id"].to_numpy()
    customer = rng.choice(cust_ids, size=n)

    # Revenue spikes: whole days where sale amounts are ~4x for every entity.
    spike_days = sorted(rng.choice(len(file_dates), size=cfg.revenue_spike_days, replace=False).tolist())
    spike_mask = np.isin(file_idx, spike_days) & (event_type == "sale")
    amount = np.where(spike_mask, np.round(amount * 4.2, 2), amount)

    event_id = np.array([f"EVT-{i:010d}" for i in range(n)])
    reference = np.where(
        event_type == "sale",
        np.array([f"INV-{i:08d}" for i in range(n)]),
        np.array([f"INV-{int(j):08d}" for j in rng.integers(0, max(1, n), n)]),
    )
    created = pd.to_datetime(pd.Series(list(posted))) + pd.to_timedelta(rng.integers(7 * 3600, 20 * 3600, n), unit="s")

    # Debit and credit legs per event type.
    debit_acct = np.select(
        [event_type == "sale", event_type == "payment", event_type == "expense"],
        ["ACC-1100", "ACC-1000", rng.choice(expense_accts, size=n)],
        default=rng.choice(liability_accts, size=n),
    )
    credit_acct = np.select(
        [event_type == "sale", event_type == "payment", event_type == "expense"],
        [rng.choice(revenue_accts, size=n), "ACC-1100", "ACC-1000"],
        default="ACC-1000",
    )
    desc_map = {
        "sale": "Invoice",
        "payment": "Customer payment",
        "expense": "Vendor expense",
        "adjustment": "Manual adjustment",
    }
    description = np.array([f"{desc_map[t]} {r}" for t, r in zip(event_type, reference, strict=True)])
    source = np.select([event_type == "sale", event_type == "payment"], ["BILLING", "BANK"], default="ERP")
    has_customer = np.isin(event_type, ["sale", "payment"])

    def legs(accts: np.ndarray, sign: float, suffix: str) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "event_id": event_id,
                "event_type": event_type,
                "reference_id": reference,
                "posted_date": posted,
                "created_at": created,
                "entity_id": entity_ids,
                "account_id": accts,
                "customer_id": np.where(has_customer, customer, None),  # type: ignore[call-overload]
                "currency": currency,
                "amount_local": sign * amount,
                "description": description,
                "source_system": source,
                "_file_date": fdate.date,
                "_leg": suffix,
            }
        )

    df = pd.concat([legs(debit_acct, 1.0, "D"), legs(credit_acct, -1.0, "C")], ignore_index=True)
    # COGS legs for sales: Dr COGS, Cr Inventory at ~60% of the sale.
    sale = df[(df.event_type == "sale") & (df._leg == "D")].copy()
    cogs_amt = np.round(sale["amount_local"].to_numpy() * 0.6, 2)
    cogs_d = sale.copy()
    cogs_d["account_id"] = rng.choice(cogs_accts, size=len(sale))
    cogs_d["amount_local"] = cogs_amt
    cogs_d["event_type"] = "cogs"
    cogs_d["event_id"] = cogs_d["event_id"] + "-C"
    cogs_d["description"] = "COGS " + cogs_d["reference_id"]
    cogs_d["customer_id"] = None
    cogs_c = cogs_d.copy()
    cogs_c["account_id"] = "ACC-1200"
    cogs_c["amount_local"] = -cogs_amt
    cogs_c["_leg"] = "C"
    df = pd.concat([df, cogs_d, cogs_c], ignore_index=True)
    df = df.sample(frac=1.0, random_state=cfg.seed).reset_index(drop=True)
    df.insert(0, "transaction_id", [f"TXN-{i:012d}" for i in range(1, len(df) + 1)])
    df["_defect"] = ""

    manifest: dict[str, Any] = {
        "revenue_spike_dates": [file_dates[i].date().isoformat() for i in spike_days],
        "defects": {},
    }
    df = _inject_defects(df, cfg, rng, manifest)
    return df, manifest


def _inject_defects(
    df: pd.DataFrame, cfg: GeneratorConfig, rng: np.random.Generator, manifest: dict[str, Any]
) -> pd.DataFrame:
    """Apply each defect type to a disjoint random sample of rows and record counts per file date."""
    n = len(df)
    order = rng.permutation(n)
    cursor = 0
    picks: dict[str, np.ndarray] = {}
    for name, rate in cfg.defect_rates.items():
        k = round(n * rate)
        picks[name] = order[cursor : cursor + k]
        cursor += k

    df.loc[picks["null_transaction_id"], "transaction_id"] = None
    df.loc[picks["null_posted_date"], "posted_date"] = None
    df.loc[picks["null_amount"], "amount_local"] = None
    # Sign flip: force a positive amount on a revenue credit leg of a sale (violates sign_by_account_type).
    flip_candidates = df.index[(df.event_type == "sale") & (df._leg == "C") & (df._defect == "")].to_numpy()
    flip_candidates = np.setdiff1d(flip_candidates, np.concatenate(list(picks.values())))
    flips = rng.choice(flip_candidates, size=min(len(picks["sign_flip"]), len(flip_candidates)), replace=False)
    picks["sign_flip"] = flips
    df.loc[flips, "amount_local"] = df.loc[flips, "amount_local"].abs()
    bad_dates = np.array([date(1999, 12, 31), date(2099, 1, 1)], dtype=object)
    df.loc[picks["bad_date"], "posted_date"] = rng.choice(bad_dates, size=len(picks["bad_date"]))
    df.loc[picks["bad_currency"], "currency"] = rng.choice(["XXX", "USDD", "EURO"], size=len(picks["bad_currency"]))
    df.loc[picks["orphan_account"], "account_id"] = "ACC-9999"
    df.loc[picks["orphan_customer"], "customer_id"] = "CUST-99999"
    df.loc[picks["orphan_entity"], "entity_id"] = "ENT-99"
    df.loc[picks["blank_description"], "description"] = ""
    for name, idx in picks.items():
        if name != "duplicate":
            df.loc[idx, "_defect"] = name

    dup_rows = df.loc[picks["duplicate"]].copy()
    dup_rows = dup_rows[dup_rows._defect == ""]
    dup_rows["_defect"] = "duplicate"
    df = pd.concat([df, dup_rows], ignore_index=True)

    counts = df[df._defect != ""].groupby([df["_file_date"].astype(str), "_defect"]).size().unstack(fill_value=0)
    manifest["defects"] = {d: {k: int(v) for k, v in row.items() if v} for d, row in counts.iterrows()}
    manifest["totals"] = {k: int(v) for k, v in df[df._defect != ""]["_defect"].value_counts().items()}
    manifest["row_count"] = len(df)
    return df


def generate_dataset(cfg: GeneratorConfig | None = None) -> Dataset:
    """Generate the full dataset in memory."""
    cfg = cfg or GeneratorConfig()
    rng = np.random.default_rng(cfg.seed)
    entities = pd.DataFrame(
        ENTITIES,
        columns=["entity_id", "entity_code", "entity_name", "country", "functional_currency"],
    )
    accounts = _build_accounts(cfg.n_accounts, rng)
    customers = _build_customers(cfg, rng)
    fx_rates, fx_spikes = _build_fx(cfg, rng)
    transactions, manifest = _build_transactions(cfg, rng, accounts, customers, fx_rates)
    manifest["fx_spikes"] = fx_spikes
    manifest["config"] = {
        "seed": cfg.seed,
        "start_date": cfg.start_date.isoformat(),
        "end_date": cfg.end_date.isoformat(),
        "n_events": cfg.n_events,
    }
    return Dataset(entities, accounts, customers, fx_rates, transactions, manifest)


def write_dataset(ds: Dataset, raw_dir: Path) -> dict[str, int]:
    """Write the dataset as the raw landing layout the ingest stage expects.

    Layout::

        raw/reference/{entities,accounts,customers}.csv   loaded once by `seed`
        raw/transactions/YYYY-MM-DD.csv                    one file per ingestion date
        raw/fx_rates/YYYY-MM-DD.csv
        raw/customers/YYYY-MM-DD.csv                       SCD2 change files (sparse)
        raw/_manifest.json
    """
    (raw_dir / "reference").mkdir(parents=True, exist_ok=True)
    for sub in ("transactions", "fx_rates", "customers"):
        (raw_dir / sub).mkdir(parents=True, exist_ok=True)
    ds.entities.to_csv(raw_dir / "reference" / "entities.csv", index=False)
    ds.accounts.to_csv(raw_dir / "reference" / "accounts.csv", index=False)
    initial = ds.customers[ds.customers.effective_date == ds.customers.effective_date.min()]
    initial.to_csv(raw_dir / "reference" / "customers.csv", index=False)
    for eff, grp in ds.customers[ds.customers.effective_date > initial.effective_date.min()].groupby("effective_date"):
        grp.to_csv(raw_dir / "customers" / f"{eff}.csv", index=False)

    tx_cols = [c for c in ds.transactions.columns if not c.startswith("_")]
    files = 0
    for fdate, grp in ds.transactions.groupby("_file_date"):
        grp[tx_cols].to_csv(raw_dir / "transactions" / f"{fdate}.csv", index=False)
        files += 1
    for rdate, grp in ds.fx_rates.groupby("rate_date"):
        grp.to_csv(raw_dir / "fx_rates" / f"{rdate}.csv", index=False)
    (raw_dir / "_manifest.json").write_text(json.dumps(ds.manifest, indent=2, default=str))
    return {
        "transaction_files": files,
        "rows": len(ds.transactions),
        "customers": len(ds.customers),
    }
