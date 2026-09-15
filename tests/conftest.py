"""Shared fixtures.

Database strategy: ``FIN_DQ_TEST_DATABASE_URL`` wins when set (CI service container, local Postgres);
otherwise testcontainers starts a Postgres container; if neither is possible, DB-backed tests are skipped.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path

import pytest
from sqlalchemy import Engine, text

from fin_dq_engine.config import Settings, load_settings
from fin_dq_engine.db import apply_migrations, get_engine
from fin_dq_engine.synthetic import GeneratorConfig, generate_dataset, write_dataset

REPO = Path(__file__).resolve().parents[1]
SCHEMAS = ["meta", "raw", "staging", "curated", "dq", "governance"]

# Small but realistic fixture dataset: ~10k posting legs across one month with the standard ~1% defect mix.
FIXTURE_CFG = GeneratorConfig(
    seed=7, start_date=date(2025, 1, 1), months=1, n_events=3400, n_customers=60, revenue_spike_days=1, fx_spike_days=1
)
FIXTURE_DAYS = 8  # run_daily is executed for the first N days of the window


def _database_url() -> tuple[str, object | None]:
    url = os.environ.get("FIN_DQ_TEST_DATABASE_URL")
    if url:
        return url, None
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:16-alpine", driver="psycopg")
        container.start()
        return container.get_connection_url(), container
    except Exception as exc:  # docker unavailable
        pytest.skip(f"No PostgreSQL available for integration tests: {exc}")


@pytest.fixture(scope="session")
def database_url() -> Iterator[str]:
    url, container = _database_url()
    yield url
    if container is not None:
        container.stop()  # type: ignore[attr-defined]


@pytest.fixture(scope="session")
def pg_engine(database_url: str) -> Iterator[Engine]:
    """Fresh schemas for the whole test session."""
    engine = get_engine(Settings(database={"url": database_url}))  # type: ignore[arg-type]
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema} CASCADE"))
    apply_migrations(engine, REPO / "sql" / "ddl")
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def fixture_data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Synthetic raw dataset written once per session."""
    data_dir = tmp_path_factory.mktemp("data")
    write_dataset(generate_dataset(FIXTURE_CFG), data_dir / "raw")
    return data_dir


def make_settings(database_url: str, data_dir: Path, out_dir: Path) -> Settings:
    """Local-mode settings pointing at temp folders and the test database."""
    return load_settings(
        REPO / "config" / "settings.yaml",
        environment="local",
        database={"url": database_url},
        # Fixed so masked PII is reproducible: without a key the HMAC falls back to a per-process
        # random one, and the golden reports containing customer_name could never match twice.
        governance={"pii_hash_key": "test-key-not-a-secret"},
        paths={
            "data_dir": str(data_dir),
            "out_dir": str(out_dir),
            "sql_dir": str(REPO / "sql"),
            "config_dir": str(REPO / "config"),
            "contracts_dir": str(REPO / "contracts"),
        },
    )


@pytest.fixture(scope="session")
def settings(database_url: str, fixture_data_dir: Path, tmp_path_factory: pytest.TempPathFactory) -> Settings:
    return make_settings(database_url, fixture_data_dir, tmp_path_factory.mktemp("out"))


@pytest.fixture(scope="session")
def seeded(settings: Settings, pg_engine: Engine) -> dict[str, object]:
    """Seed reference data and run the full pipeline for FIXTURE_DAYS days. Returns manifest + run results."""
    import json

    from fin_dq_engine.load.seed import seed_reference_data
    from fin_dq_engine.orchestration.runner import run_daily

    seed_reference_data(settings, pg_engine, date_start=date(2024, 1, 1), date_end=date(2026, 12, 31))
    manifest = json.loads((settings.raw_dir / "_manifest.json").read_text())
    results = []
    for i in range(FIXTURE_DAYS):
        results.append(run_daily(settings, pg_engine, FIXTURE_CFG.start_date + timedelta(days=i), notify=False))
    return {
        "manifest": manifest,
        "results": results,
        "last_date": FIXTURE_CFG.start_date + timedelta(days=FIXTURE_DAYS - 1),
    }


@pytest.fixture
def tmp_settings(database_url: str, tmp_path: Path) -> Settings:
    """Per-test settings with empty temp data/out folders (shares the session database)."""
    return make_settings(database_url, tmp_path / "data", tmp_path / "out")
