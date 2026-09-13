"""Matplotlib charts embedded in the HTML summary."""

from __future__ import annotations

import base64
from pathlib import Path

import matplotlib
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

BUCKETS = ["bucket_0_30", "bucket_31_60", "bucket_61_90", "bucket_90_plus"]


def _save(fig: Figure, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, dpi=110)
    plt.close(fig)
    return path


def revenue_trend_chart(daily: pd.DataFrame, anomalies: pd.DataFrame, path: Path) -> Path:
    """Total daily revenue across entities with anomaly markers on flagged days."""
    fig, ax = plt.subplots(figsize=(9, 3.6))
    if not daily.empty:
        total = daily.groupby("posted_date")["revenue_usd"].sum().astype(float)
        ma = daily.groupby("posted_date")["revenue_ma_28d"].sum().astype(float)
        ax.plot(pd.to_datetime(total.index), total.to_numpy(), color="#1f4e79", linewidth=1.6, label="Revenue (USD)")
        ax.plot(
            pd.to_datetime(ma.index), ma.to_numpy(), color="#8fb3d9", linewidth=1.2, linestyle="--", label="28-day MA"
        )
        if not anomalies.empty:
            days = pd.to_datetime(
                anomalies.loc[anomalies["method"] == "revenue_zscore_mad", "subject"], errors="coerce"
            )
            marks = total[total.index.isin(days.dt.date)]
            ax.scatter(pd.to_datetime(marks.index), marks.to_numpy(), color="#c0392b", zorder=5, s=48, label="Anomaly")
    ax.set_title("Daily revenue with anomaly markers")
    ax.set_ylabel("USD")
    ax.grid(alpha=0.3)
    ax.legend(loc="upper left", fontsize=8)
    return _save(fig, path)


def ar_aging_chart(aging: pd.DataFrame, path: Path, top_n: int = 10) -> Path:
    """Stacked aging buckets for the largest open balances."""
    fig, ax = plt.subplots(figsize=(9, 3.6))
    if not aging.empty:
        top = aging.head(top_n).copy()
        bottom = pd.Series(0.0, index=top.index)
        colors = ["#2e7d32", "#f9a825", "#ef6c00", "#c62828"]
        for bucket, color in zip(BUCKETS, colors, strict=True):
            vals = pd.to_numeric(top[bucket], errors="coerce").fillna(0).astype(float)
            ax.bar(
                top["customer_id"],
                vals,
                bottom=bottom,
                color=color,
                label=bucket.replace("bucket_", "").replace("_", "-"),
            )
            bottom = bottom + vals
        ax.tick_params(axis="x", rotation=45, labelsize=8)
    ax.set_title(f"AR aging, top {top_n} customers by open balance")
    ax.set_ylabel("USD")
    ax.legend(fontsize=8)
    return _save(fig, path)


def dq_pass_rate_chart(history: pd.DataFrame, path: Path) -> Path:
    """DQ pass rate per run (last 30 runs) with the 95% alarm line."""
    fig, ax = plt.subplots(figsize=(9, 3.2))
    if not history.empty:
        ax.plot(
            pd.to_datetime(history["run_date"]),
            history["dq_pass_rate"].astype(float) * 100,
            marker="o",
            color="#1f4e79",
            linewidth=1.4,
        )
    ax.axhline(95, color="#c0392b", linestyle="--", linewidth=1, label="Alarm threshold 95%")
    ax.set_ylim(min(90, float(history["dq_pass_rate"].min() * 100) - 1 if not history.empty else 90), 100.5)
    ax.set_title("DQ pass rate over the last 30 runs")
    ax.set_ylabel("%")
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)
    return _save(fig, path)


def embed_png(path: Path) -> str:
    """Return a data URI so the HTML summary is self-contained."""
    return "data:image/png;base64," + base64.b64encode(path.read_bytes()).decode()
