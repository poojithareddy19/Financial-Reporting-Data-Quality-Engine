"""Every curated column must be documented in config/data_dictionary.yaml (and vice versa)."""

from __future__ import annotations

from pathlib import Path

import yaml
from sqlalchemy import Engine, text

REPO = Path(__file__).resolve().parents[2]


def test_curated_columns_are_documented(pg_engine: Engine) -> None:
    dictionary = yaml.safe_load((REPO / "config" / "data_dictionary.yaml").read_text())["tables"]
    with pg_engine.connect() as conn:
        rows = conn.execute(
            text("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_schema = 'curated' ORDER BY table_name, ordinal_position""")
        ).fetchall()
    actual: dict[str, set[str]] = {}
    for table, col in rows:
        actual.setdefault(f"curated.{table}", set()).add(col)
    documented = {t: set(spec["columns"]) for t, spec in dictionary.items()}
    missing = {t: cols - documented.get(t, set()) for t, cols in actual.items() if cols - documented.get(t, set())}
    stale = {t: cols - actual.get(t, set()) for t, cols in documented.items() if cols - actual.get(t, set())}
    assert not missing, f"curated columns missing from data_dictionary.yaml: {missing}"
    assert not stale, f"data_dictionary.yaml documents columns that do not exist: {stale}"
    for table, spec in dictionary.items():
        assert spec["owner"] and spec["retention_class"], table
        for col, meta in spec["columns"].items():
            assert {"type", "description", "pii"} <= set(meta), f"{table}.{col}"
