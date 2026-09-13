from __future__ import annotations

from datetime import date

import numpy as np

from fin_dq_engine.synthetic import GeneratorConfig, generate_dataset


def test_generator_is_deterministic_and_double_entry_balances() -> None:
    cfg = GeneratorConfig(
        seed=3,
        start_date=date(2025, 1, 1),
        months=1,
        n_events=800,
        n_customers=20,
        defect_rates=dict.fromkeys(GeneratorConfig().defect_rates, 0.0),
    )
    a = generate_dataset(cfg)
    b = generate_dataset(cfg)
    assert a.transactions.drop(columns="created_at").equals(b.transactions.drop(columns="created_at"))
    tx = a.transactions
    assert tx["_defect"].eq("").all()
    # Every event nets to zero in local currency: debits equal credits.
    net = tx.groupby("event_id")["amount_local"].sum()
    assert np.allclose(net, 0, atol=0.011)
    assert len(a.accounts) == 120 and len(a.entities) == 5
    assert set(a.fx_rates.currency) == {"USD", "EUR", "GBP", "JPY"}
    assert cfg.end_date == date(2025, 1, 31)


def test_defects_injected_disjointly() -> None:
    ds = generate_dataset(GeneratorConfig(seed=5, months=1, n_events=3000, n_customers=30))
    tx = ds.transactions
    totals = ds.manifest["totals"]
    assert totals["duplicate"] > 0 and totals["bad_currency"] > 0 and totals["orphan_entity"] > 0
    defective = tx[tx._defect != ""]
    # each defective row carries exactly one defect label, and about 1% of rows are defective
    assert 0.005 < len(defective) / len(tx) < 0.02
    assert (tx.loc[tx._defect == "bad_currency", "currency"].isin(["XXX", "USDD", "EURO"])).all()
    assert tx.loc[tx._defect == "sign_flip", "amount_local"].gt(0).all()
    assert len(ds.manifest["revenue_spike_dates"]) == 4
    dup_ids = tx.loc[tx._defect == "duplicate", "transaction_id"]
    assert tx["transaction_id"].isin(dup_ids).sum() == 2 * len(dup_ids)
