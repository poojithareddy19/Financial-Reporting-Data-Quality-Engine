"""Generate the seeded synthetic dataset into ./data/raw (or a custom folder).

Usage:
    python scripts/generate_synthetic_data.py --events 250000 --months 24 --out data/raw
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import typer

from fin_dq_engine.synthetic import GeneratorConfig, generate_dataset, write_dataset

app = typer.Typer(add_completion=False)


@app.command()
def main(
    out: Path = typer.Option(Path("data/raw"), help="Raw landing folder"),
    events: int = typer.Option(250_000, help="Business events; each produces 2-4 posting legs"),
    months: int = typer.Option(24),
    start: str = typer.Option("2024-09-01"),
    seed: int = typer.Option(42),
) -> None:
    """Generate transactions, FX rates, customers, accounts and entities with ~1% injected defects."""
    cfg = GeneratorConfig(seed=seed, start_date=date.fromisoformat(start), months=months, n_events=events)
    ds = generate_dataset(cfg)
    stats = write_dataset(ds, out)
    typer.echo(f"Wrote {stats['rows']:,} rows across {stats['transaction_files']} daily files to {out}")
    typer.echo(f"Defect totals: {ds.manifest['totals']}")
    typer.echo(f"Revenue spike dates: {ds.manifest['revenue_spike_dates']}")


if __name__ == "__main__":
    app()
