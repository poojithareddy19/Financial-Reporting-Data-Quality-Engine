"""Schema contract enforcement.

Guards two defects that were both silent in the original build: a renamed source column arriving as a
wall of nulls instead of an incident, and the breach evidence being rolled back by the very exception
that refused the batch.
"""

from __future__ import annotations

import csv
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import Engine, text
from typer.testing import CliRunner

from fin_dq_engine import cli
from fin_dq_engine.config import Settings
from fin_dq_engine.contracts import (
    Contract,
    ContractNotFoundError,
    SchemaDriftError,
    contract_version,
    header_from_bytes,
    load_contract,
    load_contracts,
    validate_columns,
    validate_feeds,
)
from fin_dq_engine.ingest.ingest import TRANSACTION_COLUMNS, ingest_date

REPO = Path(__file__).resolve().parents[2]
CONTRACTS = REPO / "contracts"
RUN_DATE = date(2025, 6, 2)


@pytest.fixture(scope="module")
def transactions() -> Contract:
    return load_contract(CONTRACTS / "transactions.avsc")


def test_every_feed_has_a_contract_with_an_owner() -> None:
    contracts = load_contracts(CONTRACTS)
    assert set(contracts) == {"transactions", "fx_rates", "customers"}
    for contract in contracts.values():
        assert "@" in contract.owner, f"{contract.feed} must name an accountable owner"
        assert contract.producing_system and contract.delivery_window
        assert len(contract.version) == 12


def test_contract_matches_the_columns_ingest_lands(transactions: Contract) -> None:
    assert list(transactions.fields) == TRANSACTION_COLUMNS


def test_version_tracks_structure_not_documentation(transactions: Contract) -> None:
    definition = dict(transactions.definition)
    documented = {**definition, "doc": "reworded", "owner": "someone-else@example.com"}
    assert contract_version(documented) == transactions.version

    extended = {**definition, "fields": [*definition["fields"], {"name": "settlement_ref", "type": "string"}]}
    assert contract_version(extended) != transactions.version


def test_matching_header_is_compatible(transactions: Contract) -> None:
    verdict = validate_columns(transactions, TRANSACTION_COLUMNS)
    assert verdict.verdict == "compatible"
    assert not verdict.is_breaking
    assert verdict.missing == () and verdict.unknown == ()


def test_column_order_is_irrelevant(transactions: Contract) -> None:
    assert validate_columns(transactions, list(reversed(TRANSACTION_COLUMNS))).verdict == "compatible"


def test_missing_field_is_breaking(transactions: Contract) -> None:
    header = [c for c in TRANSACTION_COLUMNS if c != "amount_local"]
    verdict = validate_columns(transactions, header)
    assert verdict.is_breaking
    assert verdict.missing == ("amount_local",)
    assert "finance-ops@example.com" in verdict.message


def test_renamed_field_is_breaking_not_additive(transactions: Contract) -> None:
    """The defect: pandas reindex turned this into a NaN column and reported success."""
    header = ["amount" if c == "amount_local" else c for c in TRANSACTION_COLUMNS]
    verdict = validate_columns(transactions, header)
    assert verdict.verdict == "breaking"
    assert verdict.missing == ("amount_local",)
    assert verdict.unknown == ("amount",)


def test_duplicated_column_is_breaking(transactions: Contract) -> None:
    verdict = validate_columns(transactions, [*TRANSACTION_COLUMNS, "currency"])
    assert verdict.is_breaking
    assert verdict.duplicated == ("currency",)


def test_new_field_is_additive_and_still_lands(transactions: Contract) -> None:
    verdict = validate_columns(transactions, [*TRANSACTION_COLUMNS, "settlement_ref"])
    assert verdict.verdict == "additive"
    assert not verdict.is_breaking
    assert verdict.unknown == ("settlement_ref",)


def test_empty_file_reports_every_field_missing(transactions: Contract) -> None:
    verdict = validate_columns(transactions, header_from_bytes(b""))
    assert verdict.is_breaking
    assert verdict.missing == tuple(TRANSACTION_COLUMNS)


def test_header_parsing_tolerates_bom_and_quotes() -> None:
    assert header_from_bytes(b'\xef\xbb\xbfa,"b,c", d \nrow1\n') == ["a", "b,c", "d"]


def test_feed_without_a_contract_is_refused(transactions: Contract) -> None:
    with pytest.raises(ContractNotFoundError):
        validate_feeds({"transactions": transactions}, {"invoices": ["id"]})


def _write_transactions(data_dir: Path, run_date: date, header: list[str], rows: int = 3) -> Path:
    path = data_dir / "raw" / "transactions" / f"{run_date.isoformat()}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(header)
        for i in range(rows):
            writer.writerow([f"{col}-{i}" for col in header])
    return path


def _count_raw(engine: Engine) -> int:
    with engine.connect() as conn:
        return int(conn.execute(text("SELECT count(*) FROM raw.transactions")).scalar_one())


def test_drifted_file_aborts_ingest_and_the_evidence_outlives_it(tmp_settings: Settings, pg_engine: Engine) -> None:
    """Defect 3: the breach record must survive the rollback that the breach itself causes."""
    data_dir = tmp_settings.paths.data_dir
    _write_transactions(data_dir, RUN_DATE, list(TRANSACTION_COLUMNS))
    ok = ingest_date(tmp_settings, pg_engine, RUN_DATE)
    assert not ok.skipped and ok.rows_transactions == 3

    drift_date = date(2025, 6, 3)
    renamed = ["amount" if c == "amount_local" else c for c in TRANSACTION_COLUMNS]
    _write_transactions(data_dir, drift_date, renamed)

    before = _count_raw(pg_engine)
    with pytest.raises(SchemaDriftError) as excinfo:
        ingest_date(tmp_settings, pg_engine, drift_date)
    assert "amount_local" in str(excinfo.value)
    assert "Nothing was landed" in str(excinfo.value)
    assert _count_raw(pg_engine) == before, "a refused batch must land no rows"

    with pg_engine.connect() as conn:
        rows: list[Any] = list(
            conn.execute(
                text(
                    """
                    SELECT feed, verdict, missing_fields, unknown_fields, batch_id
                    FROM governance.contract_events
                    WHERE run_date = :d AND verdict = 'breaking'
                    """
                ),
                {"d": drift_date},
            )
        )
    assert len(rows) == 1, "the breach must be recorded even though the batch was abandoned"
    assert rows[0].feed == "transactions"
    assert list(rows[0].missing_fields) == ["amount_local"]
    assert list(rows[0].unknown_fields) == ["amount"]
    assert rows[0].batch_id is None, "no batch was ever opened"


def test_registry_records_the_current_promise(tmp_settings: Settings, pg_engine: Engine) -> None:
    _write_transactions(tmp_settings.paths.data_dir, date(2025, 6, 4), list(TRANSACTION_COLUMNS))
    ingest_date(tmp_settings, pg_engine, date(2025, 6, 4))
    with pg_engine.connect() as conn:
        row = conn.execute(
            text("SELECT owner, fields, version FROM governance.contract_registry WHERE feed = 'transactions'")
        ).one()
    assert row.owner == "finance-ops@example.com"
    assert list(row.fields) == TRANSACTION_COLUMNS
    assert row.version == load_contract(CONTRACTS / "transactions.avsc").version


@pytest.fixture
def cli_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point the CLI at an empty temp data dir and the repo's real contracts. No database involved."""
    monkeypatch.setenv("FIN_DQ_CONFIG", str(REPO / "config" / "settings.yaml"))
    monkeypatch.setenv("FIN_DQ__PATHS__DATA_DIR", str(tmp_path))
    monkeypatch.setenv("FIN_DQ__PATHS__CONTRACTS_DIR", str(CONTRACTS))
    return tmp_path


def _check(run_date: date) -> Any:
    return CliRunner().invoke(cli.app, ["contracts", "check", "--run-date", run_date.isoformat()])


def test_cli_contracts_check_exit_codes(cli_env: Path) -> None:
    """The runbook makes this an on-call's first step, so the exit codes are part of the contract."""
    _write_transactions(cli_env, RUN_DATE, list(TRANSACTION_COLUMNS))
    ok = _check(RUN_DATE)
    assert ok.exit_code == 0, ok.output
    assert '"status": "compatible"' in ok.output

    # Additive must not fail the command: the batch still loads, the field is only recorded.
    additive_date = date(2025, 6, 5)
    _write_transactions(cli_env, additive_date, [*TRANSACTION_COLUMNS, "settlement_ref"])
    added = _check(additive_date)
    assert added.exit_code == 0, added.output
    assert '"status": "additive"' in added.output and "settlement_ref" in added.output

    # Breaking is exit 2, and must name both the absent field and the accountable owner.
    drift_date = date(2025, 6, 6)
    renamed = ["amount" if c == "amount_local" else c for c in TRANSACTION_COLUMNS]
    _write_transactions(cli_env, drift_date, renamed)
    broken = _check(drift_date)
    assert broken.exit_code == 2, broken.output
    assert '"status": "breaking"' in broken.output
    assert "amount_local" in broken.output and "finance-ops@example.com" in broken.output


def test_cli_contracts_check_without_source_files(cli_env: Path) -> None:
    """A date with no drop yet is exit 1 and a plain message, not a traceback."""
    result = _check(date(1999, 1, 1))
    assert result.exit_code == 1
    assert "No source files" in result.output
