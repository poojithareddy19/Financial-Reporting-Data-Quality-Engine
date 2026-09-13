"""Row-level dq_score and batch scorecard computation."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from fin_dq_engine.quality.rules import RuleOutcome


def compute_dq_score(failed_severities: Iterable[str], warning_penalty: int = 20, info_penalty: int = 5) -> int:
    """Score one row from the severities of the rules it failed.

    Rules: any blocking failure scores 0 (the row is quarantined); otherwise start at 100 and
    subtract ``warning_penalty`` per warning and ``info_penalty`` per info failure, floored at 0.
    Penalties must be non-negative.
    """
    if warning_penalty < 0 or info_penalty < 0:
        raise ValueError("penalties must be non-negative")
    score = 100
    for sev in failed_severities:
        if sev == "blocking":
            return 0
        if sev == "warning":
            score -= warning_penalty
        elif sev == "info":
            score -= info_penalty
        else:
            raise ValueError(f"unknown severity {sev!r}")
    return max(0, min(100, score))


@dataclass
class Scorecard:
    """Per-batch data quality summary."""

    batch_id: str
    rows_checked: int
    rows_quarantined: int
    rules_passed: int
    rules_failed: int
    rules_errored: int
    blocking_failure_rate: float
    dq_pass_rate: float
    mean_dq_score: float
    by_dimension: dict[str, dict[str, int]] = field(default_factory=dict)
    by_severity: dict[str, dict[str, int]] = field(default_factory=dict)
    quarantine_reasons: dict[str, int] = field(default_factory=dict)


def build_scorecard(
    batch_id: str,
    rows_checked: int,
    outcomes: list[RuleOutcome],
    quarantined_keys: set[int],
    scores: dict[int, int],
) -> Scorecard:
    """Aggregate rule outcomes into a scorecard."""
    by_dim: dict[str, dict[str, int]] = {}
    by_sev: dict[str, dict[str, int]] = {}
    reasons: dict[str, int] = {}
    for o in outcomes:
        d = by_dim.setdefault(o.definition.dimension, {"pass": 0, "fail": 0, "error": 0})
        d[o.status] += 1
        s = by_sev.setdefault(o.definition.severity, {"pass": 0, "fail": 0, "error": 0})
        s[o.status] += 1
        if o.definition.severity == "blocking" and o.rows_failed:
            reasons[o.definition.id] = o.rows_failed
    n_q = len(quarantined_keys)
    mean_score = (sum(scores.values()) / len(scores)) if scores else 100.0
    return Scorecard(
        batch_id=batch_id,
        rows_checked=rows_checked,
        rows_quarantined=n_q,
        rules_passed=sum(1 for o in outcomes if o.status == "pass"),
        rules_failed=sum(1 for o in outcomes if o.status == "fail"),
        rules_errored=sum(1 for o in outcomes if o.status == "error"),
        blocking_failure_rate=(n_q / rows_checked) if rows_checked else 0.0,
        dq_pass_rate=(1 - n_q / rows_checked) if rows_checked else 1.0,
        mean_dq_score=round(mean_score, 2),
        by_dimension=by_dim,
        by_severity=by_sev,
        quarantine_reasons=reasons,
    )
